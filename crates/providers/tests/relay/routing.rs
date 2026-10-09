//! Relay routing behavior.

use super::*;

#[tokio::test]
async fn a_plugin_asks_a_model_its_own_question_and_reads_the_answer() {
    /// Refuses what a judge model calls unsafe.
    struct Judge;

    #[async_trait]
    impl Plugin for Judge {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let mut prompt = cx.input::<Input>()?.clone();
            let rule = Message::new(Role::System, "Answer SAFE or UNSAFE.");
            prompt.messages.insert(0, rule);
            prompt.model = Some("judge-1".into());
            let call = cx.call("openai").with_input(prompt);
            let verdict = call
                .await?
                .outputs::<Output>()
                .try_collect::<Vec<_>>()
                .await?;
            let unsafe_ =
                |e: &Output| matches!(e, Output::Delta { delta, .. } if delta.as_str() == "UNSAFE");
            match verdict.iter().any(unsafe_) {
                true => Err(Fault::new("the judge refused")
                    .with(ErrorCode("unsafe".into()))
                    .with(Status(StatusCode::FORBIDDEN))),
                false => Ok(None),
            }
        }
    }

    for (verdict, status) in [("SAFE", StatusCode::OK), ("UNSAFE", StatusCode::FORBIDDEN)] {
        let h = Harness::new(vec![Arc::new(Judge)], Some("openai"));
        let judged = json!({
            "id": "j",
            "choices": [{"message": {"content": verdict}}],
            "usage": {"prompt_tokens": 50, "completion_tokens": 50},
        });
        h.sites
            .openai
            .reply(fixture::json(StatusCode::OK, &judged.to_string()))
            .reply(fixture::json(StatusCode::OK, UNARY));
        let (got, _, _) = h.send(unary_chat()).await;
        assert_eq!(got, status, "{verdict}");
        // Each call counts on the provider edge, the judge's too.
        if status == StatusCode::OK {
            let usage = h.usage.total();
            let billed = (
                usage.get(units::INPUT_TOKENS),
                usage.get(units::OUTPUT_TOKENS),
            );
            assert_eq!(billed, (Some(54), Some(51)));
        }
        let sent = h.sites.sent.sent();
        let judge: serde_json::Value = serde_json::from_slice(sent[0].body()).unwrap_or_default();
        assert_eq!(judge["model"], "judge-1");
        assert_eq!(judge["messages"][0]["role"], "system");
        if status == StatusCode::OK {
            // The client's request is untouched by the judge's call.
            let own: serde_json::Value = serde_json::from_slice(sent[1].body()).unwrap_or_default();
            assert_eq!(own, json!({ "model": "gpt-x", "messages": [] }));
        }
    }
}

#[tokio::test]
async fn a_plugin_races_two_calls_and_keeps_the_first() {
    struct Hedge;

    #[async_trait]
    impl Plugin for Hedge {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let (a, b) = (cx.call("openai"), cx.call("backup"));
            let first = match futures::future::select(a, b).await {
                futures::future::Either::Left((won, _))
                | futures::future::Either::Right((won, _)) => won,
            };
            first.map(Some)
        }
    }

    let h = Harness::new(vec![Arc::new(Hedge)], None);
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));
    h.sites.backup.reply(fixture::json(StatusCode::OK, UNARY));

    let (status, _, body) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::OK);
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert_eq!(gist(&body).text(), "hi");
}

#[tokio::test]
async fn a_judges_own_prompt_carries_nothing_of_the_clients() {
    struct Judge;

    #[async_trait]
    impl Plugin for Judge {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let said = cx.input::<Input>()?.messages[0].text();
            let prompt = Input::new([
                Message::new(Role::System, "Answer SAFE or UNSAFE."),
                Message::new(Role::User, said),
            ]);
            let _verdict = cx
                .call("openai")
                .with_input(judged(prompt, "judge-1"))
                .await?
                .outputs::<Output>()
                .try_collect::<Vec<_>>()
                .await?;
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Judge)], Some("openai"));
    h.sites
        .openai
        .reply(fixture::json(StatusCode::OK, UNARY))
        .reply(fixture::json(StatusCode::OK, UNARY));
    let body = json!({"model": "gpt-x", "response_format": {"type": "json_object"},
        "tools": [{"type": "function", "function": {"name": "f"}}], "user": "u1",
        "messages": [{"role": "user", "content": "hi"}]});
    let mut req = call("/v1/chat/completions?tenant=t1", body.to_string());
    let headers = req.headers_mut();
    headers.insert("authorization", HeaderValue::from_static("Bearer client"));
    headers.insert("x-app", HeaderValue::from_static("app-1"));

    let (status, _, _) = h.send(req).await;

    assert_eq!(status, StatusCode::OK);
    let sent = h.sites.sent.sent();
    let judge: serde_json::Value = serde_json::from_slice(sent[0].body()).unwrap_or_default();
    assert!(
        judge.get("tools").is_none()
            && judge.get("response_format").is_none()
            && judge.get("user").is_none(),
        "{judge}"
    );
    assert_eq!(judge["messages"][1]["content"], "hi");
    // Nor its query or headers: the key is the transport's own.
    assert_eq!(sent[0].uri().query(), None);
    let headers = sent[0].headers();
    assert!(headers.get("x-app").is_none());
    assert_eq!(headers["authorization"], "Bearer sk-test");
    // The client's own call still carries them.
    assert_eq!(sent[1].uri().query(), None);
    assert!(sent[1].headers().get("x-app").is_none());
}

#[tokio::test]
async fn a_judge_asks_whole_while_its_client_streams() {
    struct Judge;

    #[async_trait]
    impl Plugin for Judge {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let prompt = Input::new([Message::new(Role::User, "safe?")]);
            let verdict = cx
                .call("openai")
                .with_input(prompt)
                .with_extension(respan_gateway_providers::StreamResponse(false))
                .await?
                .outputs::<Output>()
                .try_collect::<Vec<_>>()
                .await?;
            let hi =
                |e: &Output| matches!(e, Output::Delta { delta, .. } if delta.as_str() == "hi");
            assert!(verdict.iter().any(hi));
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Judge)], Some("openai"));
    h.sites
        .openai
        .reply(fixture::json(StatusCode::OK, UNARY))
        .reply(fixture::sse(STREAM));

    let (status, _, body) = h.send(chat()).await;

    assert_eq!(status, StatusCode::OK);
    let judge: serde_json::Value =
        serde_json::from_slice(h.sites.sent.sent()[0].body()).unwrap_or_default();
    assert_ne!(judge["stream"], true, "{judge}");
    assert!(judge.get("stream_options").is_none(), "{judge}");
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert_eq!(gist(&body).items(), gist(&STREAM.concat()).items());
    assert!(body.ends_with("data: [DONE]\n\n"), "{body}");
}

#[tokio::test]
async fn a_judge_reads_a_redirect_as_a_failure() {
    struct Judge;

    #[async_trait]
    impl Plugin for Judge {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let verdict = cx
                .call("openai")
                .await?
                .outputs::<Output>()
                .try_collect::<Vec<_>>()
                .await;
            // A 502 fault carrying the provider's 302.
            let fault = verdict.err();
            assert_eq!(
                fault.as_ref().map(respan_gateway_http::status),
                Some(StatusCode::BAD_GATEWAY)
            );
            let answer = fault.as_ref().and_then(|f| {
                f.get::<respan_gateway_http::ProviderError>()
                    .map(|e| &e.response)
            });
            assert_eq!(answer.map(|a| a.status()), Some(StatusCode::FOUND));
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Judge)], Some("openai"));
    let moved = http::Response::builder()
        .status(StatusCode::FOUND)
        .body(Vec::new())
        .unwrap_or_default();
    h.sites
        .openai
        .reply(moved)
        .reply(fixture::json(StatusCode::OK, UNARY));

    let (status, _, _) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn each_attempt_of_a_fallback_gets_the_provider_edge() {
    let bill = Bill::default();
    let chain: Vec<Arc<dyn Plugin>> = vec![
        Arc::new(Fallback(vec!["openai", "backup"])),
        Arc::new(bill.clone()),
    ];
    let h = Harness::new(chain, None);
    h.sites
        .openai
        .reply(fixture::json(StatusCode::SERVICE_UNAVAILABLE, "{}"));
    h.sites.backup.reply(fixture::sse(STREAM));

    let said = gist(&h.body(chat()).await);
    assert_eq!(said.items(), gist(&STREAM.concat()).items());

    // The fallback gave the answer, so the plugin after it had no client edge,
    // yet saw both calls.
    assert_eq!(bill.started(), ["openai/gpt-x", "backup/gpt-x"]);
    let ended = bill.ended();
    // The failed call's end is the provider's answer, which carries its
    // status.
    let (failed, answered) = (&ended[1].1, &ended[0].1);
    let failed = failed.as_ref().err();
    assert!(failed.is_some_and(|f| {
        f.get::<respan_gateway_http::ProviderError>()
            .map(|e| &e.response)
            .is_some()
    }));
    assert_eq!(
        failed.map(respan_gateway_http::status),
        Some(StatusCode::SERVICE_UNAVAILABLE)
    );
    assert_eq!(says(answered.clone()), Ok(()));
    let backup = metered(&h)
        .into_iter()
        .find(|(c, _)| c.starts_with("backup"));
    assert_eq!(backup.map(|(_, n)| n), Some(5));
}

#[tokio::test]
async fn a_fallback_reads_a_call_to_its_first_content() {
    /// Sends `openai`'s answer if it has content, else `backup`'s.
    struct Fallback;

    #[async_trait]
    impl Plugin for Fallback {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let mut first = cx.call("openai").await?;
            if first.buffer_until(Output::is_content).await.is_ok() {
                return Ok(Some(first));
            }
            drop(first);
            let mut backup = cx.call("backup").await?;
            backup.buffer_until(Output::is_content).await?;
            Ok(Some(backup))
        }
    }

    let ends = Ends::default();
    let h = Harness::new(vec![Arc::new(ends.clone()), Arc::new(Fallback)], None);
    // A success that fails before any content.
    let overloaded = "data: {\"error\":{\"message\":\"Overloaded\",\"code\":\"overloaded\"}}\n\n";
    h.sites.openai.reply(fixture::sse(&[overloaded]));
    h.sites.backup.reply(fixture::sse(STREAM));

    let body = h.body(chat()).await;

    // What `until_content` read is sent, with the rest of the answer.
    assert_eq!(gist(&body).items(), gist(&STREAM.concat()).items());
    assert_eq!(ends.all(), ["openai overloaded", "backup ok", "client"]);
}

#[tokio::test]
async fn a_fallback_skips_a_success_that_fails_after_only_its_start() {
    let ends = Ends::default();
    let h = Harness::new(vec![Arc::new(ends.clone()), Arc::new(FirstContent)], None);
    let role = "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"}}]}\n\n";
    let overloaded = "data: {\"error\":{\"message\":\"Overloaded\",\"code\":\"overloaded\"}}\n\n";
    h.sites.openai.reply(fixture::sse(&[role, overloaded]));
    h.sites.backup.reply(fixture::sse(STREAM));

    let body = h.body(chat()).await;

    assert_eq!(gist(&body).items(), gist(&STREAM.concat()).items());
    assert_eq!(ends.all(), ["openai overloaded", "backup ok", "client"]);
}

// The documented fallback turns every provider rate-limited into the last
// 429 with its `Retry-After`, not a 502.
#[tokio::test]
async fn r11_2_the_documented_fallback_keeps_the_last_429() {
    let h = Harness::new(vec![Arc::new(ArchFallback(vec!["openai", "backup"]))], None);
    h.sites.openai.reply(limited());
    h.sites.backup.reply(limited());
    let (status, headers, _) = h.send(unary_chat()).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert!(headers.contains_key("retry-after"));
    // It still answers with the first success.
    h.sites.openai.reply(limited());
    h.sites.backup.reply(fixture::sse(STREAM));
    assert_eq!(
        gist(&h.body(chat()).await).items(),
        gist(&STREAM.concat()).items()
    );
}

#[tokio::test]
async fn r11_plugin_fallback_moves_on_and_keeps_the_last_error_as_sent() {
    let h = Harness::new(vec![Arc::new(Fallback11(vec!["openai", "backup"]))], None);
    h.sites
        .openai
        .reply(fixture::json(StatusCode::INTERNAL_SERVER_ERROR, "{}"));
    h.sites.backup.reply(fixture::sse(STREAM));
    assert_eq!(
        gist(&h.body(chat()).await).items(),
        gist(&STREAM.concat()).items()
    );

    h.sites.openai.reply(limited());
    h.sites.backup.reply(limited());
    let (status, headers, _) = h.send(unary_chat()).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert!(headers.contains_key("retry-after"));
}

#[tokio::test]
async fn a_race_by_completion_picks_the_provider_that_finishes_first() {
    /// Races both providers, each read to its content or its end, and
    /// answers with the first.
    struct Race(bool);

    #[async_trait]
    impl Plugin for Race {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let to_end = self.0;
            let race = ["openai", "backup"].map(|p| {
                let call = cx.call(p);
                Box::pin(async move {
                    let mut resp = call.await?;
                    if to_end {
                        resp.until_end().await?;
                    } else {
                        resp.buffer_until(Output::is_content).await?;
                    }
                    Ok::<Response, Fault>(resp)
                })
            });
            let (resp, _) = futures::future::select_ok(race).await?;
            Ok(Some(resp))
        }
    }

    const SLOW_REPLY: &[&str] = &[
        "data: {\"id\":\"slow\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"first\"}}]}\n\n",
        "data: [DONE]\n\n",
    ];
    const FAST_REPLY: &[&str] = &[
        "data: {\"id\":\"fast\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"done\"}}]}\n\n",
        "data: [DONE]\n\n",
    ];
    for (to_end, winner) in [(false, "openai"), (true, "backup")] {
        let (ends, sites) = (Ends::default(), Sites::default());
        // `openai` streams content at once but ends late; `backup` starts
        // later and ends first.
        let ms = Duration::from_millis;
        let slow = Paced(sites.openai.plain(), Duration::ZERO, ms(600));
        let fast = Paced(sites.backup.plain(), ms(100), Duration::ZERO);
        let list = [
            add_provider("openai", OpenAiChat, slow),
            add_provider("backup", OpenAiChat, fast),
        ];
        let plugins: Vec<Arc<dyn Plugin>> = vec![Arc::new(ends.clone()), Arc::new(Race(to_end))];
        let gw = gateway(list, plugins, None).unwrap_or_else(|e| panic!("{e}"));
        sites.openai.reply(fixture::sse(SLOW_REPLY));
        sites.backup.reply(fixture::sse(FAST_REPLY));

        let req = post(CHAT, STREAM_REQ);
        let (_, _, body) = read_whole(serve(&gw, req).await.0).await;
        gw.idle().await;

        let body = String::from_utf8_lossy(&body.unwrap_or_default()).into_owned();
        let expected = if winner == "openai" {
            SLOW_REPLY
        } else {
            FAST_REPLY
        };
        let (said, sent) = (gist(&body), gist(&expected.concat()));
        assert_eq!(said.items(), sent.items(), "to its end: {to_end}");
        assert_eq!(said.start, sent.start, "to its end: {to_end}");
        let mut ends = ends.all();
        assert_eq!(ends.pop().as_deref(), Some("client"));
        ends.sort();
        let loser = if winner == "openai" {
            "backup"
        } else {
            "openai"
        };
        let mut expected = vec![format!("{winner} ok"), format!("{loser} abandoned")];
        expected.sort();
        assert_eq!(ends, expected, "to its end: {to_end}");
    }
}
