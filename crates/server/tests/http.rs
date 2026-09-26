use axum::Router;
use axum::response::IntoResponse;
use axum::routing::post;
use respan_gateway_core::http;
use respan_gateway_core::{Gateway, Provider, Route};
use respan_gateway_formats::{AnthropicMessages, OpenAiChat};
use respan_gateway_server::{HttpTransport, router};
use tokio::net::TcpListener;

const SSE: &str =
    "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n";

/// A gateway whose route `chat` speaks OpenAI to the provider at `base`.
fn openai_gateway(base: impl Into<String>) -> Gateway {
    Gateway::new(
        HttpTransport::new().unwrap_or_else(|e| panic!("{e}")),
        [Provider::new("local", OpenAiChat, base)],
        [Route::new("chat", OpenAiChat).with_target("local")],
    )
    .unwrap_or_else(|e| panic!("gateway: {e}"))
}

async fn spawn(app: Router) -> String {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|e| panic!("bind: {e}"));
    let addr = listener
        .local_addr()
        .unwrap_or_else(|e| panic!("addr: {e}"));
    tokio::spawn(async move { axum::serve(listener, app).await });
    format!("http://{addr}")
}

#[tokio::test]
async fn relays_a_stream_over_real_http() {
    let provider = spawn(Router::new().route(
        "/v1/chat/completions",
        post(|| async { ([("content-type", "text/event-stream")], SSE).into_response() }),
    ))
    .await;
    let gateway = openai_gateway(provider);
    let base = spawn(router(gateway, [("/v1/chat/completions", "chat")])).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/v1/chat/completions"))
        .body(r#"{"model":"m","stream":true,"messages":[]}"#)
        .send()
        .await
        .unwrap_or_else(|e| panic!("send: {e}"));
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers().get("content-type").map(|v| v.as_bytes()),
        Some(&b"text/event-stream"[..])
    );
    assert_eq!(resp.text().await.unwrap_or_default(), SSE);
}

const ANTHROPIC_SSE: &str = concat!(
    "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"model\":\"claude-x\",\"usage\":{\"input_tokens\":3,\"output_tokens\":1}}}\n\n",
    "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
    "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\n",
    "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
    "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":2}}\n\n",
    "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
);

#[tokio::test]
async fn translates_an_openai_client_to_anthropic_over_real_http() {
    let provider = spawn(Router::new().route(
        "/v1/messages",
        post(|headers: http::HeaderMap| async move {
            let version = headers.get("anthropic-version").is_some();
            let body = if version {
                ANTHROPIC_SSE
            } else {
                "missing anthropic-version"
            };
            ([("content-type", "text/event-stream")], body).into_response()
        }),
    ))
    .await;
    let gateway = Gateway::new(
        HttpTransport::new().unwrap_or_else(|e| panic!("{e}")),
        [Provider::new("claude", AnthropicMessages, provider)],
        [Route::new("chat", OpenAiChat).with_target("claude")],
    )
    .unwrap_or_else(|e| panic!("gateway: {e}"));
    let base = spawn(router(gateway, [("/v1/chat/completions", "chat")])).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/v1/chat/completions"))
        .body(r#"{"model":"claude-x","stream":true,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap_or_else(|e| panic!("send: {e}"));
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap_or_default();

    assert!(body.contains(r#""delta":{"content":"hi"}"#), "{body}");
    assert!(body.contains(r#""finish_reason":"stop""#), "{body}");
    assert!(body.ends_with("data: [DONE]\n\n"), "{body}");
}

#[tokio::test]
async fn a_providers_redirect_is_passed_on_not_followed() {
    let provider = spawn(Router::new().route(
        "/v1/chat/completions",
        post(|| async {
            let moved = [("location", "http://elsewhere.invalid/")];
            (http::StatusCode::TEMPORARY_REDIRECT, moved).into_response()
        }),
    ))
    .await;
    let gateway = openai_gateway(provider);
    let base = spawn(router(gateway, [("/v1/chat/completions", "chat")])).await;

    let resp = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap_or_else(|e| panic!("{e}"))
        .post(format!("{base}/v1/chat/completions"))
        .body(r#"{"model":"m","messages":[]}"#)
        .send()
        .await
        .unwrap_or_else(|e| panic!("send: {e}"));
    assert_eq!(resp.status(), 307);
    assert_eq!(
        resp.headers().get("location").map(|v| v.as_bytes()),
        Some(&b"http://elsewhere.invalid/"[..])
    );
}

#[tokio::test]
async fn a_body_over_the_cap_is_too_large() {
    let gateway = openai_gateway("http://127.0.0.1:9");
    let app = router(gateway, [("/chat", "chat")]);
    let req = http::Request::post("/chat")
        .body(axum::body::Body::from(vec![b' '; 32 * 1024 * 1024 + 1]))
        .unwrap_or_else(|e| panic!("{e}"));

    let resp = tower::ServiceExt::oneshot(app, req)
        .await
        .unwrap_or_else(|e| match e {});

    assert_eq!(resp.status(), 413);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await;
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert!(body.contains("too_large"), "{body}");
}

#[tokio::test]
async fn a_body_that_breaks_off_is_a_bad_request() {
    let gateway = openai_gateway("http://127.0.0.1:9");
    let app = router(gateway, [("/chat", "chat")]);
    let broken = futures::stream::iter([
        Ok(bytes::Bytes::from_static(b"{\"model\"")),
        Err(std::io::Error::other("the client left")),
    ]);
    let req = http::Request::post("/chat")
        .body(axum::body::Body::from_stream(broken))
        .unwrap_or_else(|e| panic!("{e}"));

    let resp = tower::ServiceExt::oneshot(app, req)
        .await
        .unwrap_or_else(|e| match e {});

    assert_eq!(resp.status(), 400);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await;
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert!(body.contains("bad_request"), "{body}");
}

#[tokio::test]
async fn a_path_is_matched_as_written() {
    let gateway = openai_gateway("http://127.0.0.1:9");
    let paths = [("/v1/:model/*rest", "chat")];
    let status = |path: &'static str| {
        let app = router(gateway.clone(), paths);
        async move {
            let req = http::Request::post(path)
                .body(axum::body::Body::from("{}"))
                .unwrap_or_else(|e| panic!("{e}"));
            let resp = tower::ServiceExt::oneshot(app, req).await;
            resp.unwrap_or_else(|e| match e {}).status()
        }
    };

    // Axum's pattern syntax is literal here.
    assert_ne!(status("/v1/:model/*rest").await, 404);
    assert_ne!(status("/v1/:model/*rest?beta=true").await, 404);
    assert_eq!(status("/v1/gpt/chat").await, 404);

    let app = router(gateway.clone(), paths);
    let req = http::Request::post("/v1/gpt/chat")
        .body(axum::body::Body::empty())
        .unwrap_or_else(|e| panic!("{e}"));
    let resp = tower::ServiceExt::oneshot(app, req).await;
    let resp = resp.unwrap_or_else(|e| match e {});
    let plain = resp.headers().get("content-type").cloned();
    assert_eq!(
        plain.as_ref().map(|v| v.as_bytes()),
        Some(&b"text/plain; charset=utf-8"[..])
    );
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await;
    assert_eq!(body.unwrap_or_default(), "no route at '/v1/gpt/chat'");
}

#[test]
fn a_provider_url_is_one_http_reaches() {
    let build = |url: &str| {
        // An `Arc` asks the transport it holds.
        let transport = std::sync::Arc::new(HttpTransport::new().unwrap_or_else(|e| panic!("{e}")));
        Gateway::new(transport, [Provider::new("p", OpenAiChat, url)], []).map(drop)
    };
    for url in [
        "http://[::1]",
        "http://[::1]:8080",
        "https://h.test:443/v1beta",
    ] {
        assert!(build(url).is_ok(), "{url}");
    }
    let bad = [
        "ftp://h.test",
        "https://h.test:99999",
        "http://127.0.0.1:0",
        "http://1.2.3.256",
        "http://[::1%25eth0]",
    ];
    for url in bad {
        let fault = build(url).err();
        let said = fault.as_ref().map(|f| f.message());
        let expected = "provider 'p': bad base URL: an absolute one its transport reaches, with no query, fragment or `@` (credentials go in its headers)";
        assert_eq!(said, Some(expected), "{url}");
    }
}
