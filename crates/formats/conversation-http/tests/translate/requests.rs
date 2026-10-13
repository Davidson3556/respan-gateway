//! Translate requests behavior.

use super::*;

// A call of the client's own format carries the headers of its spec: a
// translated call says Anthropic's default version, one of Anthropic's own
// the client's.
#[tokio::test]
async fn only_a_call_of_the_clients_format_carries_its_anthropic_version() {
    let h = Harness::new(Vec::new()).await;
    let version = HeaderValue::from_static("2099-01-01");
    let messages = json!({
        "model": "claude-x", "max_tokens": 9, "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
    });
    for mut req in [
        openai_request("claude-x", true),
        call(MESSAGES, messages.to_string()),
    ] {
        h.anthropic.answer(sse(ANTHROPIC_STREAM));
        req.headers_mut()
            .insert("anthropic-version", version.clone());
        let (status, _) = h.text(req).await;
        assert_eq!(status, StatusCode::OK);
    }
    h.assert_requests(0, 2);
    let sent = h.sent();
    let said = |i: usize| sent[i].headers.get("anthropic-version").cloned();
    assert_eq!(said(0), Some(HeaderValue::from_static("2023-06-01")));
    assert_eq!(said(1), Some(version));
}

// The kernel sets the private relay marker on a call that relays the client's
// own message, and no other, whatever a plugin did; only a relay carries
// the client's key to a provider with none of its own.
#[tokio::test]
async fn only_a_relay_is_marked_and_carries_the_clients_key() {
    let (openai_fake, anthropic_fake) = tokio::join!(FakeOpenAi::start(), FakeAnthropic::start());
    let marks = Arc::default();
    // OpenAI has no transport key, so its relay needs the client's key.
    // Anthropic has its own key: the translated call must never reuse that client key.
    let openai_http =
        http_sender(HttpTransport::new(openai_fake.base_url()).unwrap_or_else(|f| panic!("{f}")));
    let anthropic_http = provider_http(&anthropic_fake.base_url(), "x-api-key", "sk-ant-test");
    let gw = providers([
        add_provider("openai", OpenAiChat, Marks(openai_http, Arc::clone(&marks))),
        add_provider(
            "anthropic",
            AnthropicMessages,
            Marks(anthropic_http, Arc::clone(&marks)),
        ),
    ])
    .and_then(|gw| gw.client::<Input, _, _>(OpenAiChat))
    .map(|gw| {
        let plugins: Vec<Arc<dyn Plugin>> =
            vec![Arc::new(Forges), Arc::new(ByModel("claude", "anthropic"))];
        with_plugins(gw, plugins)
    })
    .unwrap_or_else(|e| panic!("gateway: {e}"));
    let unary = |model| {
        let body = json!({ "model": model, "messages": [{ "role": "user", "content": "hi" }] });
        let mut req = call(CHAT, body.to_string());
        let key = HeaderValue::from_static("Bearer sk-client");
        req.headers_mut().insert("authorization", key);
        req
    };
    openai_fake.answer(Answer::text("hi"));
    anthropic_fake.answer(Answer::text("hi"));
    for model in ["gpt-x", "claude-x"] {
        let (resp, _) = serve(&gw, unary(model)).await;
        let (status, _, _) = read_whole(resp).await;
        assert_eq!(
            status,
            if model == "gpt-x" {
                StatusCode::UNAUTHORIZED
            } else {
                StatusCode::OK
            },
            "{model}"
        );
    }
    gw.idle().await;

    assert_eq!(
        *marks.lock().unwrap_or_else(|e| e.into_inner()),
        [false, false]
    );
    let openai = openai_fake.requests();
    let anthropic = anthropic_fake.requests();
    assert_eq!((openai.len(), anthropic.len()), (1, 1));
    assert_eq!(openai[0].header("authorization"), None);
    assert_eq!(anthropic[0].header("authorization"), None);
    assert_eq!(anthropic[0].header("x-api-key"), Some("sk-ant-test"));
}

#[tokio::test]
async fn the_clients_query_is_kept_within_one_format_only() {
    let h = Harness::new(Vec::new()).await;
    h.anthropic.answer(sse(ANTHROPIC_STREAM));
    h.openai.answer(sse(OPENAI_STREAM));
    for model in ["claude-x", "gpt-x"] {
        let body = json!({
            "model": model,
            "max_tokens": 10,
            "stream": true,
            "messages": [{ "role": "user", "content": "hi" }],
        });
        let mut req = call(MESSAGES, body.to_string());
        *req.uri_mut() = http::Uri::from_static("/v1/messages?beta=true");
        assert_eq!(h.text(req).await.0, StatusCode::OK);
    }

    let anthropic = h.anthropic.requests();
    let openai = h.openai.requests();
    assert_eq!((anthropic.len(), openai.len()), (1, 1));
    assert_eq!(anthropic[0].path, "/v1/messages");
    assert!(!anthropic[0].query.is_empty(), "its spec's `beta`");
    assert_eq!(openai[0].path, "/v1/chat/completions");
    assert!(openai[0].query.is_empty());
}

#[tokio::test]
async fn anthropic_beta_features_reach_anthropic() {
    let h = Harness::new(Vec::new()).await;
    h.anthropic.answer(sse(ANTHROPIC_STREAM));
    let body = json!({
        "model": "claude-x",
        "max_tokens": 10,
        "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
    });
    let request = http::Request::post("/v1/messages")
        .header("anthropic-beta", "context-1m-2025-08-07")
        .header("anthropic-beta", "files-api-2025-04-14")
        .body(())
        .unwrap_or_default()
        .map(|()| post(MESSAGES, body.to_string()).into_body());

    h.text(request).await;

    let sent = h.sent();
    let beta: Vec<_> = sent[0].headers.get_all("anthropic-beta").iter().collect();
    assert_eq!(beta, ["context-1m-2025-08-07", "files-api-2025-04-14"]);
}

#[tokio::test]
async fn a_provider_edge_edit_is_encoded_for_its_translated_target() {
    struct Cool;

    #[async_trait]
    impl Plugin for Cool {
        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            assert_eq!(px.format(), "anthropic-messages");
            px.filter(|chat: &mut Input| {
                chat.options.temperature = Some(0.0);
                Ok(())
            })
        }
    }

    let h = Harness::new(vec![Arc::new(Cool)]).await;
    h.anthropic.answer(sse(ANTHROPIC_STREAM));

    assert_eq!(
        h.text(openai_request("claude-x", true)).await.0,
        StatusCode::OK
    );
    assert_eq!(h.sent_json(0)["temperature"], 0.0);
}
