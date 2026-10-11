//! Relay responses behavior.

use super::*;

#[tokio::test]
async fn a_plugin_can_respond_itself() {
    let h = Harness::new(vec![Arc::new(Cached)], None);

    let body: serde_json::Value =
        serde_json::from_str(&h.body(unary_chat()).await).unwrap_or_default();
    assert_eq!(body["id"], "cached");
    assert_eq!(body["choices"][0]["message"]["content"], "hi");
    assert_eq!(body["usage"]["completion_tokens"], 1);
    // A plugin's own answer passes no provider edge, so
    // no meter there sees its usage; no call ran.
    assert_eq!(says(h.log.only()), Ok(()));
    assert!(h.usage.all().is_empty());
}

#[tokio::test]
async fn a_plugin_can_answer_by_calling_providers() {
    let h = Harness::new(vec![Arc::new(Fallback(vec!["openai", "backup"]))], None);
    let overloaded = r#"{"error":{"message":"overloaded"}}"#;
    h.sites
        .openai
        .reply(fixture::json(StatusCode::SERVICE_UNAVAILABLE, overloaded));
    h.sites.backup.reply(fixture::sse(STREAM));

    let said = gist(&h.body(chat()).await);
    assert_eq!(said.items(), gist(&STREAM.concat()).items());
    assert_eq!(h.sites.sent.sent().len(), 2);
    assert_eq!(says(h.log.only()), Ok(()));
}

#[tokio::test]
async fn a_gateway_that_cannot_work_fails_as_it_is_built() {
    let sites = Sites::default();
    let openai = || provider(&sites.openai, "openai", None);
    let built =
        |gw: Result<Gateway, Fault>| gw.err().map(|f| (fault_code(&f), f.message().to_owned()));
    let cases = [
        (
            providers([openai(), openai()]),
            "provider 'openai' is defined twice",
        ),
        (
            providers([openai()]).and_then(|g| g.target("missing")),
            "unknown provider 'missing'",
        ),
    ];
    for (gw, expected) in cases {
        let expected = Some(("config".to_owned(), expected.to_owned()));
        assert_eq!(built(gw), expected);
    }
}

#[tokio::test]
async fn plugins_run_in_registration_order_on_both_edges() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let named = |name| Named(name, log.clone());
    let sites = Sites::default();
    // Registered d, b, a, c, and they run in that order.
    let gw = providers([provider(&sites.openai, "openai", None)])
        .and_then(|g| g.client::<Input, _, _>(OpenAiChat)?.target("openai"))
        .map(|g| {
            let g = g.plugin(named("d")).plugin(named("b"));
            g.plugin(named("a")).plugin(named("c"))
        })
        .unwrap_or_else(|e| panic!("gateway: {e}"));
    sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    let (_, _, body) = read_whole(serve(&gw, unary_chat()).await.0).await;
    gw.idle().await;

    let order = ["d", "b", "a", "c"];
    let hooks = ["client", "provider", "provider_end", "client_end"];
    let expected: Vec<String> = hooks
        .iter()
        .flat_map(|hook| order.iter().map(move |name| format!("{name} {hook}")))
        .collect();
    assert_eq!(log.lock().map(|l| l.clone()).ok(), Some(expected));
    // An earlier plugin's filter wraps a later one's.
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert!(body.contains(r#""content":"d(b(a(c(hi))))""#), "{body}");
}

#[tokio::test]
async fn caller_values_reach_plugins_by_type() {
    #[derive(Clone)]
    struct Tenant(&'static str);

    struct NeedsTenant;

    #[async_trait]
    impl Plugin for NeedsTenant {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            match cx.extensions().get::<Tenant>() {
                Some(Tenant("acme")) => Ok(None),
                _ => Err(Fault::new("unknown tenant")
                    .with(ErrorCode("forbidden".into()))
                    .with(Status(StatusCode::FORBIDDEN))),
            }
        }
    }

    let h = Harness::new(vec![Arc::new(NeedsTenant)], Some("openai"));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    let (anonymous, _, _) = h.send(unary_chat()).await;
    let mut req = post(CHAT, UNARY_REQ);
    // The kernel's extensions travel in the HTTP request
    // as one value of their own.
    let mut values = respan_gateway_core::Extensions::new();
    values.insert(Tenant("acme"));
    req.extensions_mut().insert(values);
    let (acme, _, _) = h.send(req).await;

    assert_eq!((anonymous, acme), (StatusCode::FORBIDDEN, StatusCode::OK));
}

#[tokio::test]
async fn whole_untyped_bytes_answer_a_streaming_client_as_a_stream() {
    let h = relay();
    let untyped = http::Response::new(vec![Ok(bytes::Bytes::from(UNARY))]);
    h.sites.openai.reply(untyped);

    // Whole untyped bytes are rewritten as the stream the client asked for.
    let body = h.body(chat()).await;
    assert!(body.starts_with("data: {"), "{body}");
    assert!(body.contains("\"content\":\"hi\""), "{body}");
    assert!(body.ends_with("data: [DONE]\n\n"), "{body}");
    assert_eq!(says(h.log.only()), Ok(()));
}

#[test]
#[should_panic(expected = "Tokio 1.x runtime")]
fn handle_needs_a_tokio_runtime() {
    let sites = Sites::default();
    let gw = gateway(
        [provider(&sites.openai, "openai", None)],
        vec![],
        Some("openai"),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    futures::executor::block_on(async {
        let _ = serve(&gw, unary_chat()).await;
    });
}

#[tokio::test]
async fn a_router_can_set_the_target_and_let_later_plugins_run() {
    struct Pick;

    #[async_trait]
    impl Plugin for Pick {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            assert_eq!(cx.target(), Some("openai"));
            cx.set_target("backup");
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Pick), Arc::new(Cool)], Some("openai"));
    h.sites.backup.reply(fixture::json(StatusCode::OK, UNARY));
    let _ = h.send(unary_chat()).await;

    let sent = &h.sites.sent.sent()[0];
    assert!(sent.uri().to_string().starts_with(BACKUP));
    let body: serde_json::Value = serde_json::from_slice(sent.body()).unwrap_or_default();
    assert_eq!(body["temperature"], 0.0, "the plugin after the router ran");
}

#[tokio::test]
async fn a_stream_that_opens_with_no_choices_is_not_held() {
    let stream = [
        chunk(json!({"id": "", "choices": [], "prompt_filter_results": []})),
        chunk(json!({"id": "c1", "choices": [{"index": 0, "delta": {"content": "Hi"}}]})),
        chunk(json!({"id": "c1", "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]})),
        "data: [DONE]\n\n".to_owned(),
    ];

    let (body, chunks) = footed(&stream).await;

    assert_eq!(
        said(&chunks),
        ("Hi [footer]".to_owned(), Some("stop"), None),
        "{body}"
    );
}

#[tokio::test]
async fn text_before_the_start_of_a_whole_message_is_written() {
    let h = filtered(Disclaimer);
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    let (status, _, body) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&body.unwrap_or_default()).unwrap_or_default();
    assert_eq!(
        body.pointer("/choices/0/message/content"),
        Some(&json!("NOTE: hi"))
    );
}

#[tokio::test]
async fn a_tool_call_held_in_an_own_stream_is_released() {
    let h = filtered(HoldAll(Vec::new()));
    // As OpenAI streams a tool call: its role, no content, and the call.
    let call = json!([{"index": 0, "delta": {"role": "assistant", "content": null,
        "tool_calls": [{"index": 0, "id": "t", "type": "function",
        "function": {"name": "f", "arguments": "{}"}}]}}]);
    let stream = [
        chunk(json!({"id": "c1", "choices": call})),
        chunk(
            json!({"id": "c1", "choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls"}]}),
        ),
        "data: [DONE]\n\n".to_owned(),
    ];
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.sites.openai.reply(fixture::sse(&chunks));

    let (status, _, body) = h.send(chat()).await;

    assert_eq!(status, StatusCode::OK);
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert!(body.contains("\"tool_calls\""), "{body}");
    assert!(body.contains("\"finish_reason\":\"tool_calls\""), "{body}");
    assert!(body.ends_with("data: [DONE]\n\n"));
}

#[tokio::test]
async fn text_taken_beside_a_tool_call_leaves_the_call() {
    let h = filtered(Silence::default());
    h.sites.openai.reply(fixture::json(
        StatusCode::OK,
        &tool_call_message(json!("Let me check")),
    ));

    let (status, _, body) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&body.unwrap_or_default()).unwrap_or_default();
    assert_eq!(
        body.pointer("/choices/0/message/content"),
        Some(&json!(null))
    );
    assert_eq!(
        body.pointer("/choices/0/message/tool_calls/0/id"),
        Some(&json!("t"))
    );
}

#[tokio::test]
async fn a_footer_joins_a_message_of_only_a_tool_call() {
    let h = filtered(Disclaimer);
    h.sites.openai.reply(fixture::json(
        StatusCode::OK,
        &tool_call_message(json!(null)),
    ));

    let (status, _, body) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&body.unwrap_or_default()).unwrap_or_default();
    assert_eq!(
        body.pointer("/choices/0/message/content"),
        Some(&json!("NOTE: "))
    );
    assert_eq!(
        body.pointer("/choices/0/message/tool_calls/0/id"),
        Some(&json!("t"))
    );
}

#[tokio::test]
async fn several_choices_are_relayed_each_with_its_own_words() {
    let stream = [
        chunk(
            json!({"id": "c1", "choices": [{"index": 0, "delta": {"content": "a"}, "finish_reason": "stop"}]}),
        ),
        chunk(json!({"id": "c1", "choices": [{"index": 1, "delta": {"content": "b"}}]})),
        chunk(json!({"id": "c1", "choices": [{"index": 1, "delta": {}, "finish_reason": "stop"}]})),
        "data: [DONE]\n\n".to_owned(),
    ];
    let h = relay();
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.sites.openai.reply(fixture::sse(&chunks));

    let body = h.body(chat()).await;
    let said = sent_chunks(&body);
    // Each choice keeps its words, those that are not the first as they came.
    let of = |index: u64| {
        let choices = said
            .iter()
            .flat_map(|c| c["choices"].as_array().into_iter().flatten());
        let own: Vec<_> = choices.filter(|c| c["index"] == index).collect();
        let text: String = own
            .iter()
            .filter_map(|c| c["delta"]["content"].as_str())
            .collect();
        (text, own.iter().any(|c| c["finish_reason"] == "stop"))
    };
    assert_eq!(of(0), ("a".to_owned(), true), "{body}");
    assert_eq!(of(1), ("b".to_owned(), true), "{body}");
    assert!(body.ends_with("data: [DONE]\n\n"));
}

#[tokio::test]
async fn collecting_the_wrong_type_fails_even_with_nothing_said() {
    struct Wrong;

    #[async_trait]
    impl Plugin for Wrong {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let got = cx
                .call("openai")
                .await?
                .outputs::<String>()
                .try_collect::<Vec<_>>()
                .await;
            assert_eq!(
                got.err().map(|f| fault_code(&f)),
                Some("internal".to_owned())
            );
            // Its own kind reads, though empty.
            let got = cx
                .call("openai")
                .await?
                .outputs::<Output>()
                .try_collect::<Vec<_>>()
                .await;
            assert!(got.is_ok_and(|events| events.is_empty()));
            Err(BadRequest::fault("checked"))
        }
    }

    let h = Harness::new(vec![Arc::new(Wrong)], None);
    h.sites
        .openai
        .reply(fixture::sse(&["data: [DONE]\n\n"]))
        .reply(fixture::sse(&["data: [DONE]\n\n"]));

    let (status, _, _) = h.send(chat()).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn a_plugin_asking_the_provider_otherwise_leaves_the_client_its_framing() {
    let fake = FakeOpenAi::start().await;
    let h = Harness::real(&fake, vec![Arc::new(Flip)], Some("openai"));
    fake.answer(Answer::raw(UNARY));
    fake.answer(real_sse(&[&STREAM.concat()]));

    let (_, streamed, body) = h.send(chat()).await;
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert_eq!(streamed["content-type"], "text/event-stream");
    assert!(body.contains("\"content\":\"hi\""), "{body}");
    assert!(body.ends_with("data: [DONE]\n\n"), "{body}");

    let (_, whole, body) = h.send(valid_unary_chat()).await;
    let body: serde_json::Value =
        serde_json::from_slice(&body.unwrap_or_default()).unwrap_or_default();
    assert_eq!(whole["content-type"], "application/json");
    assert_eq!(
        body["choices"][0]["message"]["content"], "Hello world",
        "{body}"
    );
}

#[tokio::test]
async fn a_tool_call_is_reframed_as_the_client_asked() {
    let h = Harness::new(vec![Arc::new(Flip)], Some("openai"));
    let tool = r#"{"id":"c3","choices":[{"index":0,"message":{"role":"assistant","tool_calls":[{"id":"t","type":"function","function":{"name":"f","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}"#;
    h.sites.openai.reply(fixture::json(StatusCode::OK, tool));

    let (status, _, body) = h.send(chat()).await;

    assert_eq!(status, StatusCode::OK);
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert!(
        body.contains(r#""name":"f""#) && body.contains(r#""arguments":"{}""#),
        "{body}"
    );
    assert!(body.contains(r#""finish_reason":"tool_calls""#), "{body}");
}

#[tokio::test]
async fn a_providers_trailers_are_not_passed_on() {
    /// A provider whose whole answer ends in a trailer.
    struct Trailed;

    impl Trailed {
        pub async fn send(
            &self,
            _req: http::Request<Body>,
        ) -> Result<(http::Response<Body>, Option<Completion>), Fault> {
            let mut trailers = http::HeaderMap::new();
            trailers.insert("x-trailer", HeaderValue::from_static("1"));
            let frames = [
                Ok(http_body::Frame::data(bytes::Bytes::from_static(
                    UNARY.as_bytes(),
                ))),
                Ok(http_body::Frame::trailers(trailers)),
            ];
            let body = http_body_util::StreamBody::new(futures::stream::iter(frames));
            let mut resp = http::Response::new(Body::stream(
                http_body_util::BodyExt::into_data_stream(body),
            ));
            resp.headers_mut()
                .insert("content-type", HeaderValue::from_static("application/json"));
            Ok((resp, None))
        }
    }
    impl From<Trailed> for SendRequest {
        fn from(value: Trailed) -> Self {
            let value = std::sync::Arc::new(value);
            sender(move |call| {
                let value = value.clone();
                async move { value.send(call).await }
            })
        }
    }

    let log = Outcomes::default();
    let gw = gateway(
        [add_provider("openai", OpenAiChat, Trailed)],
        vec![Arc::new(log.clone())],
        Some("openai"),
    )
    .unwrap_or_else(|e| panic!("{e}"));

    let req = unary_chat();
    let (resp, _) = serve(&gw, req).await;
    let (_, body) = resp.into_parts();
    let collected = http_body_util::BodyExt::collect(body).await;
    gw.idle().await;

    let collected = collected.unwrap_or_else(|e| panic!("{e}"));
    assert!(collected.trailers().is_none());
    let body = String::from_utf8_lossy(&collected.to_bytes()).into_owned();
    assert_eq!(gist(&body).text(), "hi");
    assert_eq!(says(log.only()), Ok(()));
}

// A raw relay sends the client's bytes and key; to another vendor it
// would send both, so it is refused before anything is sent.
#[tokio::test]
async fn a_raw_relay_never_reaches_a_provider_of_another_format() {
    let sites = Sites::default();
    let gw = translating(&sites, explicit::Relay::RAW);
    let mut req = call(
        CHAT,
        r#"{"model":"claude-x","messages":[{"role":"user","content":"hi"}]}"#,
    );
    req.headers_mut().insert(
        http::header::AUTHORIZATION,
        HeaderValue::from_static("Bearer client-key"),
    );

    let (status, _, _) = read_whole(serve(&gw, req).await.0).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(sites.sent.sent().is_empty(), "nothing reached the provider");
}

#[tokio::test]
async fn a_plugin_answers_with_its_calls_events_read_as_they_are_sent() {
    /// Answers with one call's outputs, then another's.
    struct Merge;

    #[async_trait]
    impl Plugin for Merge {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let first = cx.call("openai").await?.outputs::<Output>();
            // The second answer's blocks follow the first's, as one response.
            let second = cx.call("backup").await?.outputs::<Output>();
            let after = |reply: Output| {
                let block = |block: u32| block + 1;
                futures::future::ready(Ok(match reply {
                    Output::Start(_) => None,
                    Output::BlockStart { block: b, kind } => {
                        Some(Output::block_start(block(b), kind))
                    }
                    Output::Delta { block: b, delta } => Some(Output::Delta {
                        block: block(b),
                        delta,
                    }),
                    Output::BlockEnd { block: b } => Some(Output::block_end(block(b))),
                    other => Some(other),
                }))
            };
            let second = futures::TryStreamExt::try_filter_map(second, after);
            let events = futures::StreamExt::chain(first, second);
            Ok(Some(Response::stream(events)))
        }
    }

    let ends = Ends::default();
    let h = Harness::new(vec![Arc::new(ends.clone()), Arc::new(Merge)], None);
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));
    h.sites.backup.reply(fixture::json(StatusCode::OK, UNARY));

    let (status, body) = h.text(unary_chat()).await;

    assert_eq!(status, StatusCode::OK);
    let body: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
    assert_eq!(body["choices"][0]["message"]["content"], "hihi");
    assert_eq!(ends.all(), ["openai ok", "backup ok", "client"]);
}

#[tokio::test]
async fn a_205_is_sent_with_no_body() {
    let h = Harness::new(vec![Arc::new(explicit::Relay::HEADS)], Some("openai"));
    // A provider that writes a body under a 205 anyway.
    h.sites
        .openai
        .reply(fixture::json(StatusCode::RESET_CONTENT, UNARY));

    let (status, _, body) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::RESET_CONTENT);
    assert_eq!(body.ok(), Some(bytes::Bytes::new()));
    assert_eq!(says(h.log.only()), Ok(()));
}
