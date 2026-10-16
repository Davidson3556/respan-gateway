//! Relay filters behavior.

use super::*;

#[tokio::test]
async fn an_unedited_request_reaches_the_provider_as_it_said() {
    let h = relay();
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));
    let _ = h.send(unary_chat()).await;
    let sent: serde_json::Value =
        serde_json::from_slice(h.sites.sent.sent()[0].body()).unwrap_or_default();
    assert_eq!(sent, json!({ "model": "gpt-x", "messages": [] }));
}

#[tokio::test]
async fn a_request_edit_reaches_the_provider() {
    let h = Harness::new(vec![Arc::new(Cool)], Some("openai"));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));
    let _ = h.send(unary_chat()).await;
    let sent: serde_json::Value =
        serde_json::from_slice(h.sites.sent.sent()[0].body()).unwrap_or_default();
    assert_eq!(
        sent,
        json!({ "model": "gpt-x", "messages": [], "temperature": 0.0 })
    );
}

#[tokio::test]
async fn a_filter_re_encodes_only_what_changed() {
    let h = Harness::new(vec![Arc::new(Shout)], Some("openai"));
    h.sites.openai.reply(fixture::sse(STREAM));

    let said = gist(&h.body(chat()).await);
    assert_eq!(said.text(), "HELLO WORLD");
    assert_eq!(said.usage.output_tokens, 2);
    assert_eq!(said.usage.input_tokens, 5);
    assert_eq!(said.start.map(|s| s.id), Some("c1".to_owned()));
    assert_eq!(h.usage.total().output_tokens, 2);
}

#[tokio::test]
async fn a_filter_may_keep_state_that_is_not_sync() {
    /// Numbers the text outputs of each stream, in a `Cell`.
    struct Count;

    #[async_trait]
    impl Plugin for Count {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let seen = std::cell::Cell::new(0);
            cx.filter(move |e: &mut Output| {
                if let Some(text) = text_of_mut(e) {
                    seen.set(seen.get() + 1);
                    *text = format!("{}:{text}", seen.get());
                }
                Ok(())
            })?;
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Count)], Some("openai"));
    h.sites.openai.reply(fixture::sse(STREAM));

    let body = h.body(chat()).await;

    assert!(
        body.contains("1:Hello") && body.contains("2: world"),
        "{body}"
    );
}

#[tokio::test]
async fn edits_apply_the_last_added_first() {
    let transcript = Transcript::default();
    let h = Harness::new(
        vec![Arc::new(Shout), Arc::new(transcript.clone())],
        Some("openai"),
    );
    h.sites.openai.reply(fixture::sse(STREAM));

    let body = h.body(chat()).await;

    // The transcript, added after Shout, sees the text before Shout does.
    assert_eq!(transcript.text(), "Hello world");
    assert!(body.contains("HEL"), "{body}");
}

#[tokio::test]
async fn a_filter_edits_a_unary_response_and_keeps_its_other_fields() {
    let fake = FakeOpenAi::start().await;
    let h = Harness::real(&fake, vec![Arc::new(Shout)], Some("openai"));
    let rich = json!({
        "id": "c2", "object": "chat.completion", "created": 5, "model": "gpt-x",
        "system_fingerprint": "fp", "service_tier": "default",
        "choices": [{ "index": 0, "logprobs": { "content": [] },
            "message": { "role": "assistant", "content": "hi", "refusal": null },
            "finish_reason": "stop" }],
        "usage": { "prompt_tokens": 4, "completion_tokens": 1, "total_tokens": 5 },
    });
    fake.answer(Answer::raw(rich.to_string()));

    let said: serde_json::Value =
        serde_json::from_str(&h.body(valid_unary_chat()).await).unwrap_or_default();

    // Unknown fields of the same format survive, the edit is made, and the
    // rest of what it said is as it was.
    assert_eq!(said["choices"][0]["message"]["content"], "HI");
    for (field, value) in [("system_fingerprint", "fp"), ("service_tier", "default")] {
        assert_eq!(said[field], value, "{said}");
    }
    assert_eq!(said["created"], 5);
    assert_eq!(said["choices"][0]["logprobs"], json!({ "content": [] }));
    assert_eq!(said["choices"][0]["finish_reason"], "stop");
    assert_eq!(said["usage"], rich["usage"]);
}

#[tokio::test]
async fn an_edit_in_the_same_format_keeps_what_a_conversation_cannot_hold() {
    let h = Harness::new(vec![Arc::new(Cool)], Some("openai"));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));
    let request = json!({
        "model": "gpt-x",
        "messages": [
            { "role": "user", "content": "hi" },
            { "role": "developer", "content": "answer in French" },
            { "role": "user", "content": [{ "type": "image_url", "image_url": { "url": "u" } }] },
        ],
        "tools": [{ "type": "function", "function": { "name": "f" } }],
        "seed": 7,
    });

    let (status, _, _) = h.send(call(CHAT, request.to_string())).await;

    assert_eq!(status, StatusCode::OK);
    let sent: serde_json::Value =
        serde_json::from_slice(h.sites.sent.sent()[0].body()).unwrap_or_default();
    // Even the developer role is as the client spelled it.
    let mut expected = request;
    expected["temperature"] = json!(0.0);
    assert_eq!(sent, expected);
}

#[tokio::test]
async fn an_edit_to_the_start_is_written_in_the_clients_format() {
    struct Rename;

    #[async_trait]
    impl Plugin for Rename {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(|e: &mut Output| {
                if let Output::Start(start) = e {
                    start.id = "hidden".into();
                }
                Ok(())
            })?;
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Rename)], Some("openai"));
    // As OpenAI streams begin: a chunk with the role and no text.
    let opening = "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"}}]}\n\n";
    let stream = [opening, STREAM[0], STREAM[1], STREAM[2], STREAM[3]];
    h.sites.openai.reply(fixture::sse(&stream));

    let body = h.body(chat()).await;

    assert!(body.starts_with("data: {\"id\":\"hidden\""), "{body}");
    assert!(body.contains("\"content\":\"Hello\""), "{body}");
    // In every chunk, not just the first.
    assert!(!body.contains("\"c1\""), "{body}");
}

#[tokio::test]
async fn a_filter_that_changes_nothing_keeps_what_was_said() {
    struct Idle;

    #[async_trait]
    impl Plugin for Idle {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(|_: &mut Output| Ok(()))?;
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Idle)], Some("openai"));
    let spaced = "data: {\"id\": \"c1\", \"choices\": [{\"index\": 0, \"delta\": {\"content\": \"caf\\u00e9\"}}]}\n\n";
    h.sites
        .openai
        .reply(fixture::sse(&[spaced, "data: [DONE]\n\n"]));

    assert_eq!(gist(&h.body(chat()).await).text(), "café");
}

#[tokio::test]
async fn a_stream_guardrail_refuses_before_any_bytes_and_fails_the_stream_after() {
    let h = Harness::new(vec![Arc::new(Guard(|_| true))], Some("openai"));
    h.sites.openai.reply(fixture::sse(STREAM));
    let (status, _, _) = h.send(chat()).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    // Its fault has the status.
    let end = h.log.only();
    assert!(
        matches!(end, Err(f) if respan_gateway_http_faults::status(&f) == StatusCode::FORBIDDEN)
    );

    let world =
        |e: &Output| matches!(e, Output::Delta { delta, .. } if delta.as_str().contains("world"));
    let h = Harness::new(vec![Arc::new(Guard(world))], Some("openai"));
    h.sites.openai.reply(fixture::sse(STREAM));

    let body = h.body(chat()).await;

    assert!(body.contains("Hello"), "{body}");
    assert!(!body.contains("world"), "{body}");
    assert!(
        body.ends_with("\"type\":\"blocked\",\"param\":null,\"code\":\"blocked\"}}\n\n"),
        "{body}"
    );
    assert!(matches!(h.log.only(), Err(f) if fault_code(&f) == "blocked"));
}

#[tokio::test]
async fn a_filter_edits_several_events_of_one_chunk_in_its_shape() {
    struct Truncate;

    #[async_trait]
    impl Plugin for Truncate {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(move |e: &mut Output| {
                if let Some(text) = text_of_mut(e) {
                    *text = text.to_uppercase();
                }
                if let Output::Finish(finish) = e {
                    *finish = Finish::Length;
                }
                Ok(())
            })?;
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Truncate)], Some("openai"));
    let chunk = "data: {\"id\":\"x\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":\"stop\"}]}\n\n";
    h.sites
        .openai
        .reply(fixture::sse(&[chunk, "data: [DONE]\n\n"]));

    let body = h.body(chat()).await;
    let said = gist(&body);
    assert_eq!(said.text(), "HI");
    assert_eq!(said.finish, Some(Finish::Length));
    assert!(body.ends_with("data: [DONE]\n\n"), "{body}");
}

#[tokio::test]
async fn a_unary_guardrail_refuses_with_its_status() {
    let text = |e: &Output| matches!(e, Output::Delta { .. });
    let h = Harness::new(vec![Arc::new(Guard(text))], Some("openai"));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    let (status, _, body) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    let body = String::from_utf8_lossy(&body.unwrap_or_default()).into_owned();
    assert!(body.contains("\"code\":\"blocked\""), "{body}");
    // Its fault has the status.
    let end = h.log.only();
    assert!(
        matches!(end, Err(f) if respan_gateway_http_faults::status(&f) == StatusCode::FORBIDDEN)
    );
}

#[tokio::test]
async fn a_filter_restores_text_across_an_openai_stream_and_releases_the_rest() {
    let chunk = |delta: serde_json::Value, finish: serde_json::Value| {
        let chunk = json!({"id": "c1", "object": "chat.completion.chunk", "model": "gpt-x",
            "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]});
        format!("data: {chunk}\n\n")
    };
    let stream = [
        chunk(json!({"role": "assistant", "content": ""}), json!(null)),
        chunk(json!({"content": "Hi <P"}), json!(null)),
        chunk(json!({"content": "1>, bye <"}), json!(null)),
        chunk(json!({}), json!("stop")),
        "data: [DONE]\n\n".to_owned(),
    ];
    let h = Harness::new(vec![Arc::new(Unmask)], Some("openai"));
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.sites.openai.reply(fixture::sse(&chunks));

    let (status, _, body) = h.send(chat()).await;

    assert_eq!(status, StatusCode::OK, "{body:?}");
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    let chunks: Vec<serde_json::Value> = body
        .split("\n\n")
        .filter_map(|e| e.strip_prefix("data: "))
        .filter_map(|d| serde_json::from_str(d).ok())
        .collect();
    let text: String = chunks
        .iter()
        .filter_map(|c| {
            c.pointer("/choices/0/delta/content")
                .and_then(|t| t.as_str())
        })
        .collect();
    assert_eq!(text, "Hi Alice, bye <");
    // Text kept back goes out before the provider's finish, in the same stream.
    let last = &chunks[chunks.len() - 1];
    assert_eq!(
        last.pointer("/choices/0/finish_reason"),
        Some(&json!("stop"))
    );
    assert!(chunks.iter().all(|c| c["id"] == "c1"));
    assert!(body.ends_with("data: [DONE]\n\n"));
}

#[tokio::test]
async fn a_filter_releases_what_it_held_into_a_whole_openai_message() {
    let reply = json!({"id": "c2", "choices": [{"message": {"content": "Hi <P1>, bye <"}}]});
    let h = Harness::new(vec![Arc::new(Unmask)], Some("openai"));
    h.sites
        .openai
        .reply(fixture::json(StatusCode::OK, &reply.to_string()));

    let (status, _, body) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&body.unwrap_or_default()).unwrap_or_default();
    assert_eq!(
        body.pointer("/choices/0/message/content"),
        Some(&json!("Hi Alice, bye <"))
    );
}

#[tokio::test]
async fn a_delta_a_filter_splits_in_two_is_written_as_two_and_reads_back_whole() {
    /// Splits each text after its first character.
    #[derive(Default)]
    struct Split;

    impl Filter<Output> for Split {
        fn edit(&mut self, reply: Output, out: &mut Vec<Output>) -> Result<(), Fault> {
            match (text_of(&reply), &reply) {
                (Some(text), Output::Delta { block, .. }) if text.len() > 1 => {
                    let (a, b) = text.split_at(1);
                    out.extend([Output::text(*block, a), Output::text(*block, b)]);
                }
                _ => out.push(reply),
            }
            Ok(())
        }
    }

    struct Splitter;

    #[async_trait]
    impl Plugin for Splitter {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(Split)?;
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Splitter)], Some("openai"));
    h.sites.openai.reply(fixture::sse(STREAM));

    let (status, _, body) = h.send(chat()).await;

    assert_eq!(status, StatusCode::OK);
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert!(body.contains("\"content\":\"H\""), "{body}");
    assert!(body.contains("\"content\":\"ello\""), "{body}");
    assert_eq!(gist(&body).text(), "Hello world");
}

#[tokio::test]
async fn a_filter_holding_everything_of_an_own_stream_releases_it_whole() {
    let h = filtered(HoldAll(Vec::new()));
    h.sites.openai.reply(fixture::sse(STREAM));

    let (status, _, body) = h.send(chat()).await;

    assert_eq!(status, StatusCode::OK);
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    let chunks: Vec<serde_json::Value> = body
        .split("\n\n")
        .filter_map(|e| e.strip_prefix("data: "))
        .filter_map(|d| serde_json::from_str(d).ok())
        .collect();
    assert_eq!(said(&chunks).0, "Hello world", "{body}");
    assert!(body.ends_with("data: [DONE]\n\n"));
}

#[tokio::test]
async fn an_edit_to_the_start_leaves_other_choices_alone() {
    let h = filtered(|e: &mut Output| {
        if let Output::Start(start) = e {
            start.model = "alias".into();
        }
        Ok(())
    });
    let stream = [
        chunk(
            json!({"id": "c1", "model": "gpt-x", "choices": [{"index": 0, "delta": {"content": "A"}}]}),
        ),
        chunk(
            json!({"id": "c1", "model": "gpt-x", "choices": [{"index": 1, "delta": {"content": "B"}}]}),
        ),
        "data: [DONE]\n\n".to_owned(),
    ];
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.sites.openai.reply(fixture::sse(&chunks));

    let (status, _, body) = h.send(chat()).await;

    assert_eq!(status, StatusCode::OK);
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert!(
        body.contains(r#""index":1,"delta":{"content":"B"}"#),
        "{body}"
    );
    assert!(!body.contains("gpt-x"), "{body}");
}

#[tokio::test]
async fn a_whole_message_a_filter_took_the_start_of_is_sent() {
    let h = filtered(Nameless);
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    let (status, _, body) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::OK);
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert_eq!(gist(&body).text(), "hi");
    assert_eq!(gist(&body).usage.input_tokens, 4);
}

#[tokio::test]
async fn an_unreadable_event_a_filter_held_goes_out_as_it_came() {
    let h = filtered(HoldAll(Vec::new()));
    let stream = [
        chunk(json!({"id": "c1", "choices": [{"index": 0, "delta": {"content": "Hi"}}]})),
        "data: not json\n\n".to_owned(),
        "data: [DONE]\n\n".to_owned(),
    ];
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.sites.openai.reply(fixture::sse(&chunks));

    let (_, _, body) = h.send(chat()).await;

    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert!(body.contains("data: not json\n\n"), "{body}");
    assert!(!body.contains("\"choices\":\"not json\""), "{body}");
}

#[tokio::test]
async fn every_filter_shape_writes_every_openai_message_shape() {
    let call =
        json!([{"id": "t", "type": "function", "function": {"name": "f", "arguments": "{}"}}]);
    let shapes = [
        json!({"role": "assistant", "content": null, "tool_calls": call, "refusal": null}),
        json!({"role": "assistant", "content": null, "refusal": "I can't"}),
        json!({"role": "assistant", "content": "Let me check", "tool_calls": call}),
        json!({"role": "assistant", "content": "hi"}),
    ];
    for shape in shapes {
        let reply = json!({"id": "c2", "choices": [{"index": 0, "message": shape,
            "logprobs": null, "finish_reason": "stop"}]})
        .to_string();
        let text = shape["content"].is_string();
        let mut all = vec![
            filtered(Footer(7, None)),
            filtered(Silence::default()),
            filtered(HoldAll(Vec::new())),
            filtered(Disclaimer),
            filtered(Nameless),
        ];
        // A footer joins the text of a message that has some.
        if text {
            all.push(filtered(Footer(0, None)));
        }
        for h in all {
            h.sites.openai.reply(fixture::json(StatusCode::OK, &reply));
            let (status, _, body) = h.send(unary_chat()).await;
            let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
            assert_eq!(status, StatusCode::OK, "{shape}: {body}");
        }
    }
}

#[tokio::test]
async fn other_choices_a_filter_took_leave_their_chunks() {
    /// Takes other content.
    #[derive(Clone)]
    struct Plain;

    impl Filter<Output> for Plain {
        fn edit(&mut self, reply: Output, out: &mut Vec<Output>) -> Result<(), Fault> {
            if !matches!(
                reply,
                Output::Native(respan_gateway_core::NativeValue { .. })
            ) {
                out.push(reply);
            }
            Ok(())
        }
    }

    let h = filtered(Plain);
    let stream = [
        chunk(
            json!({"id": "c1", "choices": [{"index": 0, "delta": {"role": "assistant", "content": "A"}}]}),
        ),
        chunk(
            json!({"id": "c1", "choices": [{"index": 1, "delta": {"role": "assistant", "content": "B"}}]}),
        ),
        chunk(json!({"id": "c1", "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]})),
        "data: [DONE]\n\n".to_owned(),
    ];
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.sites.openai.reply(fixture::sse(&chunks));

    let (status, _, body) = h.send(chat()).await;

    assert_eq!(status, StatusCode::OK);
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert!(!body.contains("error") && !body.contains("\"B\""), "{body}");
    assert!(body.ends_with("data: [DONE]\n\n"));
    assert_eq!(says(h.log.only()), Ok(()));
}

#[tokio::test]
async fn a_block_a_filter_adds_joins_the_one_text_of_an_openai_message() {
    let h = filtered(Footer(1, None));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    let (status, _, body) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&body.unwrap_or_default()).unwrap_or_default();
    assert_eq!(
        body.pointer("/choices/0/message/content"),
        Some(&json!("hi [footer]"))
    );
}

#[tokio::test]
async fn a_filter_annotation_after_a_held_finish_waits_with_it() {
    let stream = [
        chunk(json!({"id": "c1", "choices": [{"index": 0, "delta": {"content": "Hi"}}]})),
        chunk(json!({"id": "c1", "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]})),
        chunk(json!({"id": "", "choices": [{"index": 0, "delta": {},
            "content_filter_offsets": {"check_offset": 0, "start_offset": 0, "end_offset": 2}}]})),
        "data: [DONE]\n\n".to_owned(),
    ];

    let (body, chunks) = footed(&stream).await;

    assert_eq!(
        said(&chunks),
        ("Hi [footer]".to_owned(), Some("stop"), None),
        "{body}"
    );
    let footer = body.find("[footer]").unwrap_or(usize::MAX);
    let finish = body.find("\"finish_reason\":\"stop\"").unwrap_or(0);
    assert!(footer < finish, "{body}");
}

#[tokio::test]
async fn a_guard_refuses_what_it_cannot_read() {
    struct Guard;

    #[async_trait]
    impl Plugin for Guard {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let chat = cx.input::<Input>()?;
            if !chat.messages.iter().all(Message::is_plain) {
                let why = "content the guard cannot read";
                return Err(Fault::new(why)
                    .with(ErrorCode("unreadable".into()))
                    .with(Status(StatusCode::FORBIDDEN)));
            }
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Guard)], Some("openai"));
    let image = r#"{"model":"m","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"x"}}]}]}"#;

    let (status, _, _) = h.send(call(CHAT, image)).await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(h.sites.sent.sent().is_empty());
}

#[tokio::test]
async fn provider_edge_filters_run_before_client_edge_ones_each_last_added_first() {
    /// Marks the text on both edges.
    struct Mark(&'static str);

    fn mark(tag: String) -> impl FnMut(&mut Output) -> Result<(), Fault> + Send + 'static {
        move |e| {
            if let Some(text) = text_of_mut(e) {
                text.push_str(&tag);
            }
            Ok(())
        }
    }

    #[async_trait]
    impl Plugin for Mark {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(mark(format!(" c{}", self.0)))?;
            Ok(None)
        }

        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            px.filter(mark(format!(" p{}", self.0)))
        }
    }

    let h = Harness::new(
        vec![Arc::new(Mark("1")), Arc::new(Mark("2"))],
        Some("openai"),
    );
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    let body: serde_json::Value =
        serde_json::from_str(&h.body(unary_chat()).await).unwrap_or_default();

    assert_eq!(body["choices"][0]["message"]["content"], "hi p2 p1 c2 c1");
}

#[tokio::test]
async fn a_provider_filter_edits_a_call_a_plugin_collects() {
    /// Targets the provider a judge names.
    struct Pick;

    #[async_trait]
    impl Plugin for Pick {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let ask = Input::new([Message::new(Role::User, "which?")]);
            let judge = cx.call("openai").with_input(judged(ask, "judge-1"));
            let verdict = judge
                .await?
                .outputs::<Output>()
                .try_collect::<Vec<_>>()
                .await?;
            let pick = verdict.iter().find_map(|e| match e {
                Output::Delta { delta, .. } => Some(delta.as_str().to_owned()),
                _ => None,
            });
            cx.set_target(pick.unwrap_or_default());
            Ok(None)
        }
    }

    // Overrules the judge on the provider edge.
    let overrule =
        OnProvider(
            |px| match px.input::<Input>()?.model.as_deref() == Some("judge-1") {
                true => px.filter(move |r: &mut Output| {
                    if let Some(text) = text_of_mut(r) {
                        *text = text.replace("openai", "backup");
                    }
                    Ok(())
                }),
                _ => Ok(()),
            },
        );
    let h = Harness::new(vec![Arc::new(overrule), Arc::new(Pick)], None);
    let verdict = json!({"id": "j", "choices": [{"message": {"content": "openai"}}]});
    h.sites
        .openai
        .reply(fixture::json(StatusCode::OK, &verdict.to_string()));
    h.sites.backup.reply(fixture::json(StatusCode::OK, UNARY));

    assert_eq!(h.text(unary_chat()).await.0, StatusCode::OK);
    assert!(h.sites.sent.sent()[1].uri().to_string().starts_with(BACKUP));
}

#[tokio::test]
async fn a_failed_input_filter_fails_the_input_for_every_later_reader() {
    /// Redacts, then fails a second filter and ignores its `Err`.
    struct Careless;

    #[async_trait]
    impl Plugin for Careless {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(|chat: &mut Input| {
                chat.options.temperature = Some(0.0);
                Ok(())
            })?;
            let fails = |_: &mut Input| {
                Err(Fault::new("no")
                    .with(ErrorCode("no".into()))
                    .with(Status(StatusCode::FORBIDDEN)))
            };
            let _ = cx.filter(fails);
            let read = cx.input::<Input>().err().map(|f| fault_code(&f));
            assert_eq!(read.as_deref(), Some("no"));
            Ok(None)
        }
    }

    /// Drops the input.
    struct Gone;

    impl Filter<Input> for Gone {
        fn edit(&mut self, _chat: Input, _out: &mut Vec<Input>) -> Result<(), Fault> {
            Ok(())
        }
    }

    /// Refuses by dropping the input, which only `Err` may do.
    struct Dropping;

    #[async_trait]
    impl Plugin for Dropping {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(Gone)?;
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Careless)], Some("openai"));
    // The earlier edit is not sent without the failed one: nothing is.
    assert_eq!(h.text(unary_chat()).await.0, StatusCode::FORBIDDEN);
    assert!(h.sites.sent.sent().is_empty());

    let h = Harness::new(vec![Arc::new(Dropping)], Some("openai"));
    assert_eq!(
        h.text(unary_chat()).await.0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(code(&h.log.only()), "internal");
    assert!(h.sites.sent.sent().is_empty());
}

// A marked call's own filter, added in `on_provider`, goes with each copy
// sent, each its own, and each copy is metered.
#[tokio::test]
async fn a_calls_own_filter_is_each_copys_own() {
    #[derive(Clone)]
    struct Mark;

    /// Calls twice through clones of one marked call, reading each
    /// response, and notes how many replies each call's filter had seen.
    #[derive(Clone, Default)]
    struct Decoder(Arc<Mutex<Vec<u32>>>);

    #[async_trait]
    impl Plugin for Decoder {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let call = cx.call("openai").with_extension(Mark);
            for _ in 0..2 {
                let read = call.clone().await?.outputs::<Output>();
                let _ = read.try_collect::<Vec<_>>().await?;
            }
            Err(Fault::not_found("done"))
        }

        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            if px.extensions().get::<Mark>().is_none() {
                return Ok(());
            }
            let (seen, mut count) = (self.0.clone(), 0);
            px.filter(move |_: &mut Output| {
                count += 1;
                seen.lock().push(count);
                Ok(())
            })
        }
    }

    let reader = Decoder::default();
    let h = Harness::new(vec![Arc::new(reader.clone())], None);
    h.sites
        .openai
        .reply(fixture::sse(STREAM))
        .reply(fixture::sse(STREAM));
    let _ = h.send(chat()).await;

    let seen = reader.0.lock().clone();
    let (first, second) = seen.split_at(seen.len() / 2);
    assert!(!first.is_empty() && first == second, "{seen:?}");
    assert_eq!(first[0], 1);
    assert_eq!(h.usage.total().input_tokens, 10);
}

#[tokio::test]
async fn a_filter_added_after_a_call_is_made_misses_that_whole_calls_input() {
    struct FilterAfterCall;

    #[async_trait]
    impl Plugin for FilterAfterCall {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let call = cx.call("openai");
            cx.filter(|c: &mut Input| {
                c.options.temperature = Some(0.0);
                Ok(())
            })?;
            Ok(Some(call.await?))
        }
    }

    let h = Harness::new(vec![Arc::new(FilterAfterCall)], None);
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));
    let _ = h.send(unary_chat()).await;
    let sent: serde_json::Value =
        serde_json::from_slice(h.sites.sent.sent()[0].body()).unwrap_or_default();
    assert!(sent.get("model").is_some() && sent.get("temperature").is_none());
}

/// A guard refuses a plugin's own answer that fails before its first byte,
/// whether or not the plugin's stream awaited before its first output.
#[tokio::test]
async fn r6_a_guard_refuses_a_plugins_answer_whether_or_not_it_awaited_first() {
    struct Answer(bool);

    #[async_trait]
    impl Plugin for Answer {
        async fn on_client(&self, _cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let waits = self.0;
            // Its first content, which the guard sees before anything is written.
            let answer = [Output::start("cached", "gpt-x"), Output::text(0, "bomb")];
            let answer = futures::StreamExt::then(
                futures::stream::iter(answer.map(Ok)),
                move |r| async move {
                    if waits {
                        // Such as a cache lookup or a call's bytes not yet here.
                        tokio::task::yield_now().await;
                    }
                    r
                },
            );
            Ok(Some(Response::stream(answer)))
        }
    }

    let status = |waits: bool| async move {
        let guard = Guard(|e| matches!(e, Output::Delta { delta, .. } if delta.as_str() == "bomb"));
        let h = Harness::new(vec![Arc::new(guard), Arc::new(Answer(waits))], None);
        h.send(unary_chat()).await.0
    };
    assert_eq!(status(false).await, StatusCode::FORBIDDEN);
    assert_eq!(status(true).await, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn r11_plugin_pii_redacts_every_call_and_restores_split_placeholders() {
    let h = Harness::new(vec![Arc::new(Pii)], Some("openai"));
    let split = [
        "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi <PI\"}}]}\n\n",
        "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"I_0>!\"}}]}\n\n",
        "data: [DONE]\n\n",
    ];
    h.sites.openai.reply(fixture::sse(&split));
    let req =
        r#"{"model":"m","stream":true,"messages":[{"role":"user","content":"mail bob@x.io now"}]}"#;
    let body = h.body(call(CHAT, req)).await;
    let sent: serde_json::Value =
        serde_json::from_slice(h.sites.sent.sent()[0].body()).unwrap_or_default();
    assert_eq!(sent["messages"][0]["content"], "mail <PII_0> now");
    assert!(body.contains("bob@x.io"), "{body}");
    assert!(!body.contains("PII"), "{body}");
}
