//! Relay usage behavior.

use super::*;

#[tokio::test]
async fn a_stream_is_relayed_with_usage() {
    let h = relay();
    h.sites.openai.reply(fixture::sse(STREAM));

    let body = h.body(chat()).await;
    let (said, sent) = (gist(&body), gist(&STREAM.concat()));
    assert_eq!(said.items(), sent.items());
    assert_eq!((said.usage, said.start), (sent.usage, sent.start));
    assert!(body.ends_with("data: [DONE]\n\n"), "{body}");
    assert_eq!(says(h.log.only()), Ok(()));
    let usage = h.usage.total();
    assert_eq!(usage.get(units::INPUT_TOKENS), Some(5));
    assert_eq!(usage.get(units::OUTPUT_TOKENS), Some(2));
}

#[tokio::test]
async fn a_unary_response_is_relayed_with_usage() {
    let fake = FakeOpenAi::start().await;
    let h = Harness::real(&fake, Vec::new(), Some("openai"));
    fake.answer(Answer::raw(UNARY));

    let body = h.body(valid_unary_chat()).await;
    let said: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
    assert_eq!(said["id"], "c2");
    assert_eq!(said["choices"][0]["message"]["content"], "hi");
    assert_eq!(said["usage"]["prompt_tokens"], 4);
    assert_eq!(said["usage"]["completion_tokens"], 1);
    assert_eq!(says(h.log.only()), Ok(()));
    assert_eq!(h.usage.total().get(units::INPUT_TOKENS), Some(4));
}

#[tokio::test]
async fn the_provider_gets_its_auth_and_the_usage_option() {
    let h = Harness::new(Vec::new(), Some("backup"));
    h.sites.backup.reply(fixture::sse(STREAM));
    let _ = h.send(chat()).await;

    let sent = &h.sites.sent.sent()[0];
    assert_eq!(sent.uri(), "http://backup.test/v1/chat/completions");
    let body: serde_json::Value = serde_json::from_slice(sent.body()).unwrap_or_default();
    assert_eq!(body["stream_options"]["include_usage"], true);

    let h = relay();
    h.sites.openai.reply(fixture::sse(STREAM));
    let _ = h.send(chat()).await;
    let auth = h.sites.sent.sent()[0]
        .headers()
        .get("authorization")
        .cloned();
    assert_eq!(auth, Some(HeaderValue::from_static("Bearer sk-test")));
}

#[tokio::test]
async fn a_client_that_did_not_ask_for_usage_does_not_get_the_usage_chunk() {
    let h = relay();
    h.sites.openai.reply(fixture::sse(STREAM));
    let request = r#"{"model":"gpt-x","stream":true,"messages":[]}"#;

    let body = h.body(call(CHAT, request)).await;

    assert!(!body.contains("usage"), "{body}");
    assert_eq!(gist(&body).text(), "Hello world");
    assert!(body.ends_with("data: [DONE]\n\n"), "{body}");
    assert_eq!(h.usage.total().get(units::OUTPUT_TOKENS), Some(2));
    let sent: serde_json::Value =
        serde_json::from_slice(h.sites.sent.sent()[0].body()).unwrap_or_default();
    assert_eq!(sent["stream_options"]["include_usage"], true);
}

#[tokio::test]
async fn the_null_usage_the_gateway_asked_for_does_not_reach_the_client() {
    let h = relay();
    let chunk = |usage: &str| {
        format!(
            "data: {{\"id\":\"c1\",\"choices\":[{{\"index\":0,\"delta\":{{\"content\":\"hi\"}}}}]{usage}}}\n\n"
        )
    };
    let stream = [chunk(",\"usage\":null"), "data: [DONE]\n\n".into()];
    let stream: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.sites.openai.reply(fixture::sse(&stream));
    let request = r#"{"model":"gpt-x","stream":true,"messages":[]}"#;

    let body = h.body(call(CHAT, request)).await;

    assert!(!body.contains("usage"), "{body}");
    assert_eq!(gist(&body).text(), "hi");
    assert!(body.ends_with("data: [DONE]\n\n"), "{body}");
}

#[tokio::test]
async fn a_chunk_that_says_more_than_null_usage_is_kept() {
    let h = relay();
    let filter = |usage: &str| {
        format!(
            "data: {{\"choices\":[],\"id\":\"\",\"prompt_filter_results\":[{{\"prompt_index\":0}}]{usage}}}\n\n"
        )
    };
    let stream = [
        filter(",\"usage\":null"),
        "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\n"
            .into(),
        "data: [DONE]\n\n".into(),
    ];
    let stream: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.sites.openai.reply(fixture::sse(&stream));
    let request = r#"{"model":"gpt-x","stream":true,"messages":[]}"#;

    let body = h.body(call(CHAT, request)).await;

    // What the chunk said beside its null usage is kept, in the chunks the
    // response is written in.
    assert!(!body.contains("usage"), "{body}");
    let first = &sent_chunks(&body)[0];
    assert_eq!(
        first["prompt_filter_results"],
        json!([{ "prompt_index": 0 }])
    );
    assert_eq!(gist(&body).text(), "hi");
}

#[tokio::test]
async fn an_edited_streaming_request_keeps_the_clients_usage_chunk() {
    let h = Harness::new(vec![Arc::new(Cool)], Some("openai"));
    h.sites.openai.reply(fixture::sse(STREAM));

    let said = gist(&h.body(chat()).await);
    assert_eq!(said.usage, gist(&STREAM.concat()).usage);
    assert_eq!(said.usage.get(units::OUTPUT_TOKENS), Some(2));
}

#[tokio::test]
async fn a_whole_message_a_filter_held_keeps_its_fields_and_usage() {
    struct Hold;

    #[async_trait]
    impl Plugin for Hold {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(HoldAll(Vec::new()))?;
            Ok(None)
        }
    }

    let tool_call = json!({"id": "c2", "model": "gpt-x", "system_fingerprint": "fp",
        "choices": [{"message": {"content": "hi", "tool_calls": [{"id": "t", "type": "function",
            "function": {"name": "f", "arguments": "{}"}}]}, "finish_reason": "tool_calls"}],
        "usage": {"prompt_tokens": 4, "completion_tokens": 1}});
    let h = Harness::new(vec![Arc::new(Hold)], Some("openai"));
    h.sites
        .openai
        .reply(fixture::json(StatusCode::OK, &tool_call.to_string()));

    let (status, _, body) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&body.unwrap_or_default()).unwrap_or_default();
    assert_eq!(body["id"], "c2");
    assert_eq!(body["model"], "gpt-x");
    assert_eq!(body["system_fingerprint"], "fp");
    let (said, sent) = (&body["choices"][0], &tool_call["choices"][0]);
    assert_eq!(said["message"]["content"], "hi");
    assert_eq!(said["message"]["tool_calls"], sent["message"]["tool_calls"]);
    assert_eq!(said["finish_reason"], "tool_calls");
    assert_eq!(body["usage"]["prompt_tokens"], 4);
    assert_eq!(body["usage"]["completion_tokens"], 1);
}

#[tokio::test]
async fn a_filter_fills_a_cache_with_the_answer_and_its_usage() {
    type Entry = Arc<Mutex<Option<(String, u64)>>>;

    /// Keeps a finished answer and what it cost, to replay later: the
    /// filter sees every reply, the usage alone at the stream's end too.
    struct Fill(String, u64, Entry);

    impl Filter<Reply> for Fill {
        fn edit(&mut self, event: Reply, out: &mut Vec<Reply>) -> Result<(), Fault> {
            if let Some(text) = text_of(&event) {
                self.0.push_str(text);
            }
            if let Reply::Usage(usage) = &event {
                self.1 += usage.get(units::OUTPUT_TOKENS).unwrap_or_default();
            }
            out.push(event);
            Ok(())
        }

        fn end(&mut self, end: &Result<(), Fault>, _out: &mut Vec<Reply>) -> Result<(), Fault> {
            if let (Ok(()), Ok(mut entry)) = (end, self.2.lock()) {
                *entry = Some((std::mem::take(&mut self.0), self.1));
            }
            Ok(())
        }
    }

    struct Cache(Entry);

    #[async_trait]
    impl Plugin for Cache {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(Fill(String::new(), 0, self.0.clone()))?;
            Ok(None)
        }
    }

    let entry = Entry::default();
    let h = Harness::new(vec![Arc::new(Cache(entry.clone()))], Some("openai"));
    h.sites.openai.reply(fixture::sse(STREAM));

    let _ = h.send(chat()).await;

    let kept = entry.lock().ok().and_then(|e| e.clone());
    assert_eq!(kept, Some(("Hello world".to_owned(), 2)));
}

#[tokio::test]
async fn usage_in_the_finish_chunk_survives_a_held_finish() {
    let stream = [
        chunk(json!({"id": "c1", "choices": [{"index": 0, "delta": {"content": "Hi"}}]})),
        chunk(
            json!({"id": "c1", "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 5, "completion_tokens": 2}}),
        ),
        "data: [DONE]\n\n".to_owned(),
    ];

    let (body, chunks) = footed(&stream).await;

    assert_eq!(
        said(&chunks),
        ("Hi [footer]".to_owned(), Some("stop"), Some(2)),
        "{body}"
    );
}

#[tokio::test]
async fn the_usage_chunk_stays_last_after_a_held_finish() {
    let stream = [
        chunk(json!({"id": "c1", "choices": [{"index": 0, "delta": {"content": "Hi"}}]})),
        chunk(json!({"id": "c1", "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]})),
        chunk(json!({"id": "c1", "choices": [],
            "usage": {"prompt_tokens": 5, "completion_tokens": 2}})),
        "data: [DONE]\n\n".to_owned(),
    ];

    let (body, chunks) = footed(&stream).await;

    assert_eq!(
        said(&chunks),
        ("Hi [footer]".to_owned(), Some("stop"), Some(2)),
        "{body}"
    );
    assert!(
        chunks
            .last()
            .is_some_and(|c| c["usage"]["completion_tokens"] == 2),
        "{body}"
    );
    assert!(body.ends_with("data: [DONE]\n\n"));
}

#[tokio::test]
async fn usage_on_every_chunk_does_not_hold_the_stream() {
    let usage = json!({"prompt_tokens": 5, "completion_tokens": 1});
    let stream = [
        chunk(
            json!({"id": "c1", "choices": [{"index": 0, "delta": {"content": "Hi"}}], "usage": usage}),
        ),
        chunk(
            json!({"id": "c1", "choices": [{"index": 0, "delta": {"content": " there"}}], "usage": usage}),
        ),
        chunk(
            json!({"id": "c1", "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}], "usage": usage}),
        ),
        "data: [DONE]\n\n".to_owned(),
    ];

    let (body, chunks) = footed(&stream).await;

    assert_eq!(
        said(&chunks),
        ("Hi there [footer]".to_owned(), Some("stop"), Some(1)),
        "{body}"
    );
}

#[tokio::test]
async fn groqs_usage_beside_the_response_is_counted() {
    let h = relay();
    let reply = json!({"id": "c2", "choices": [{"index": 0, "message": {"content": "hi"}}],
        "x_groq": {"usage": {"prompt_tokens": 7, "completion_tokens": 3}}});
    h.sites
        .openai
        .reply(fixture::json(StatusCode::OK, &reply.to_string()));

    let _ = h.send(unary_chat()).await;

    let usage = h.usage.total();
    assert_eq!(
        (
            usage.get(units::INPUT_TOKENS),
            usage.get(units::OUTPUT_TOKENS)
        ),
        (Some(7), Some(3))
    );
}

#[tokio::test]
async fn a_reframed_stream_gets_usage_only_if_it_asked() {
    let h = Harness::new(vec![Arc::new(Flip)], Some("openai"));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));
    let quiet = r#"{"model":"gpt-x","stream":true,"messages":[]}"#;

    let body = h.body(call(CHAT, quiet)).await;

    assert!(!body.contains("usage"), "{body}");
    assert!(body.ends_with("data: [DONE]\n\n"), "{body}");
    assert_eq!(h.usage.total().get(units::OUTPUT_TOKENS), Some(1));
}

#[tokio::test]
async fn every_call_a_smart_router_makes_is_billed_once_on_the_provider_edge() {
    /// Asks a judge model which provider should answer, then targets it.
    struct SmartRouter;

    #[async_trait]
    impl Plugin for SmartRouter {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let ask = Conversation::new([Message::new(Role::User, "openai or backup?")]);
            let judge = cx.call("openai").with_input(judged(ask, "judge-1"));
            let verdict = judge
                .await?
                .outputs::<Reply>()
                .try_collect::<Vec<_>>()
                .await?;
            let pick = verdict.iter().find_map(|e| match e {
                Reply::Delta { delta, .. } => Some(delta.as_str().to_owned()),
                _ => None,
            });
            cx.set_target(pick.unwrap_or_else(|| "openai".into()));
            Ok(None)
        }
    }

    let bill = Bill::default();
    let h = Harness::new(vec![Arc::new(SmartRouter), Arc::new(bill.clone())], None);
    let verdict = json!({"id": "j", "choices": [{"message": {"content": "backup"}}],
        "usage": {"prompt_tokens": 50, "completion_tokens": 1}});
    h.sites
        .openai
        .reply(fixture::json(StatusCode::OK, &verdict.to_string()));
    h.sites.backup.reply(fixture::json(StatusCode::OK, UNARY));

    let (status, _) = h.text(unary_chat()).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(bill.started(), ["openai/judge-1", "client", "backup/gpt-x"]);
    let ended = bill.ended();
    let ended: Vec<_> = ended.iter().map(|(c, o)| (c.as_str(), o.is_ok())).collect();
    assert_eq!(ended, [("backup/gpt-x", true), ("openai/judge-1", true)]);
    let billed = [
        ("backup/gpt-x".to_owned(), 4),
        ("openai/judge-1".to_owned(), 50),
    ];
    assert_eq!(metered(&h), billed);
    assert_eq!(input_tokens(&h.usage.total()), 54);
}

// Each call a request makes is metered once, on its provider edge.
#[tokio::test]
async fn each_call_a_request_makes_is_metered_once() {
    /// Reads one call through, then answers with a second.
    struct Twice;

    #[async_trait]
    impl Plugin for Twice {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let first = cx.call("openai").await?.outputs::<Reply>();
            first.try_collect::<Vec<_>>().await?;
            Ok(Some(cx.call("openai").await?))
        }
    }

    let bill = Bill::default();
    let h = Harness::new(vec![Arc::new(bill.clone()), Arc::new(Twice)], None);
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    assert_eq!(h.text(unary_chat()).await.0, StatusCode::OK);
    assert_eq!(bill.ended().len(), 2);
    let calls: Vec<u64> = metered(&h).into_iter().map(|(_, n)| n).collect();
    assert_eq!(calls, [4, 4]);
    assert_eq!(input_tokens(&h.usage.total()), 8);
}

// A call a plugin reads is metered on its own provider edge, as the
// target's is.
#[tokio::test]
async fn a_call_read_in_on_client_is_metered_like_the_targets() {
    /// Reads a call's response, then passes.
    #[derive(Clone, Default)]
    struct Read(Arc<Mutex<Option<Fault>>>);

    #[async_trait]
    impl Plugin for Read {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let read = cx.call("openai").await?;
            let fault = read.outputs::<Reply>().try_collect::<Vec<_>>().await.err();
            self.0.lock().map(|mut f| *f = fault).ok();
            Ok(None)
        }
    }

    let read = Read::default();
    let h = Harness::new(vec![Arc::new(read.clone())], Some("backup"));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));
    h.sites.backup.reply(fixture::json(StatusCode::OK, UNARY));

    assert_eq!(h.text(unary_chat()).await.0, StatusCode::OK);

    let fault = read.0.lock().ok().and_then(|mut f| f.take());
    assert!(fault.is_none());
    let calls = metered(&h);
    let calls: Vec<_> = calls.iter().map(|(c, n)| (c.as_str(), *n)).collect();
    assert_eq!(calls, [("backup/gpt-x", 4), ("openai/gpt-x", 4)]);
}

// A call's usage is metered on its provider edge, as decoded.
#[tokio::test]
async fn a_calls_usage_is_metered_on_its_provider_edge() {
    let h = Harness::new(Vec::new(), Some("openai"));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    assert_eq!(h.text(unary_chat()).await.0, StatusCode::OK);

    assert_eq!(h.usage.total().get(units::INPUT_TOKENS), Some(4));
}

// A meter registered last sees each reply first, so a filter keeping all
// back and another failing at its end hide nothing from it.
#[tokio::test]
async fn a_meter_sees_usage_though_a_filter_keeps_all_back_and_another_fails_at_its_end() {
    struct Both;

    #[async_trait]
    impl Plugin for Both {
        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            px.filter(HoldAll(Vec::new()))?;
            px.filter(FailsAtEnd)
        }
    }

    let h = Harness::new(vec![Arc::new(Both)], Some("openai"));
    h.sites.openai.reply(fixture::sse(STREAM));
    let _ = h.send(chat()).await;
    assert_eq!(code(&h.calls.all()[0]), "internal");
    assert_eq!(h.usage.total().get(units::INPUT_TOKENS), Some(5));
}

// A meter registered last sees the usage in the message a filter fails on.
#[tokio::test]
async fn a_meter_sees_usage_a_filter_fails_on() {
    // The call reported 4 input tokens.
    let h = Harness::new(vec![Arc::new(ProviderCap)], Some("openai"));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));
    let _ = h.send(unary_chat()).await;
    assert_eq!(code(&h.calls.all()[0]), "cap");
    assert_eq!(h.usage.total().get(units::INPUT_TOKENS), Some(4));
}

/// Usage in a message a provider-edge filter fails on reaches a meter
/// registered after it, unwritten.
#[tokio::test]
async fn usage_a_filter_fails_on_is_metered_not_written() {
    /// OpenAI Chat whose encoder notes the usage it is given.
    #[derive(Clone, Default)]
    struct Tally(Arc<Mutex<Usage>>);

    impl Format<Incoming, Conversation, Reply> for Tally {
        fn name(&self) -> &str {
            OpenAiChat.name()
        }
        fn matches(&self, io: &Incoming) -> bool {
            <OpenAiChat as Format<Incoming, Conversation, Reply>>::matches(&OpenAiChat, io)
        }
        fn bind(&self, io: Incoming) -> Result<Connection<Conversation, Reply>, Fault> {
            let tally = self.0.clone();
            Ok(io.bind(
                |request| OpenAiChat.read_request(request),
                move |reply, source| {
                    let mut head = reply.head();
                    match OpenAiChat.response_encoder(&mut head, reply.request()) {
                        Some(encoder) => {
                            Ok(reply.write(head, Box::new(Tallying(tally.clone(), encoder))))
                        }
                        None => Ok(reply.empty(head, source)),
                    }
                },
                |fault, request, head| OpenAiChat.refusal(fault, request, head),
            ))
        }
    }

    struct Tallying(Arc<Mutex<Usage>>, Box<dyn Encoder<Reply>>);

    impl Encoder<Reply> for Tallying {
        fn encode(&mut self, reply: &Reply, out: &mut bytes::BytesMut) -> Result<(), Fault> {
            if let Reply::Usage(usage) = reply {
                self.0.lock().map(|mut t| *t += usage).ok();
            }
            self.1.encode(reply, out)
        }

        fn end(&mut self, end: &Result<(), Fault>, out: &mut bytes::BytesMut) -> Result<(), Fault> {
            self.1.end(end, out)
        }
    }

    let (sites, calls, tally) = (Sites::default(), Calls::default(), Tally::default());
    let meter = Metered::default();
    let chain: Vec<Arc<dyn Plugin>> = vec![
        Arc::new(calls.clone()),
        Arc::new(ProviderCap),
        Arc::new(meter.clone()),
    ];
    let gw = providers([provider(&sites.openai, "openai", None)])
        .and_then(|g| g.client::<Conversation, _, _>(tally.clone()))
        .map(|g| with_plugins(g, chain))
        .and_then(|g| g.target("openai"))
        .unwrap_or_else(|e| panic!("gateway: {e}"));
    sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    let _ = read_whole(serve(&gw, unary_chat()).await.0).await;
    gw.idle().await;

    assert_eq!(code(&calls.all()[0]), "cap");
    assert_eq!(input_tokens(&meter.total()), 4);
    assert!(tally.0.lock().map(|t| t.is_empty()).unwrap_or_default());
}

// A usage edit is written as the reply says on both paths, relayed or
// translated; a meter registered after it sees what the provider reported.
#[tokio::test]
async fn r11_3_a_usage_edit_is_shown_on_both_paths_and_hidden_from_a_later_meter() {
    let sites = Sites::default();
    let list = [
        provider(&sites.openai, "openai", None),
        add_provider("claude", AnthropicMessages, sites.claude.plain()),
    ];
    let meter = Metered::default();
    let plugins: Vec<Arc<dyn Plugin>> = vec![Arc::new(Markup), Arc::new(meter.clone())];
    let gw = gateway(list, plugins, None).unwrap_or_else(|e| panic!("{e}"));
    let prompt_tokens = |target: &'static str| {
        let (gw, sites) = (gw.clone(), sites.clone());
        async move {
            struct To(&'static str);
            #[async_trait]
            impl Plugin for To {
                async fn on_client(
                    &self,
                    cx: &mut ClientContext,
                ) -> Result<Option<Response>, Fault> {
                    cx.set_target(self.0);
                    Ok(None)
                }
            }
            let gw = gw.plugin(To(target));
            let _ = &sites;
            // Anthropic needs a message, so the translated call has one.
            let ask = r#"{"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}"#;
            let (_, _, body) = read_whole(serve(&gw, call(CHAT, ask)).await.0).await;
            gw.idle().await;
            let v: serde_json::Value =
                serde_json::from_slice(&body.unwrap_or_default()).unwrap_or_default();
            v["usage"]["prompt_tokens"].clone()
        }
    };
    sites.openai.reply(fixture::json(StatusCode::OK, UNARY));
    let anthropic = r#"{"id":"m","type":"message","role":"assistant","model":"c","content":[{"type":"text","text":"hi"}],"stop_reason":"end_turn","usage":{"input_tokens":4,"output_tokens":1}}"#;
    sites.claude.reply(fixture::json(StatusCode::OK, anthropic));
    let relayed = prompt_tokens("openai").await;
    let translated = prompt_tokens("claude").await;
    // The meter saw what both calls reported.
    let counted: Vec<_> = meter
        .all()
        .iter()
        .map(|u| u.get(units::INPUT_TOKENS))
        .collect();
    assert_eq!(counted, [Some(4), Some(4)]);
    assert_eq!((&relayed, &translated), (&json!(99), &json!(99)));
}
