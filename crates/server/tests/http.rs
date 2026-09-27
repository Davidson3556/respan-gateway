use axum::Router;
use axum::response::IntoResponse;
use axum::routing::post;
use std::sync::{Arc, Mutex};

use respan_gateway_core::http;
use respan_gateway_core::{
    Context, Fault, Gateway, Outcome, Plugin, Provider, Response, async_trait,
};
use respan_gateway_formats::conversation::Reply;
use respan_gateway_formats::{AnthropicMessages, OpenAiChat};
use respan_gateway_server::{CallHeaders, HttpTransport, add_own_headers, router};
use tokio::net::TcpListener;

const SSE: &str =
    "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n";

fn http(base: impl Into<String>) -> HttpTransport {
    HttpTransport::new(base).unwrap_or_else(|e| panic!("{e}"))
}

/// A gateway serving OpenAI clients, sent to `provider`, its target.
fn gateway_to(provider: Provider) -> Gateway {
    Gateway::new([provider])
        .and_then(|g| g.serve(OpenAiChat))
        .and_then(|g| g.target("local"))
        .unwrap_or_else(|e| panic!("gateway: {e}"))
}

/// A gateway serving OpenAI clients, sent to the provider at `base`.
fn openai_gateway(base: impl Into<String>) -> Gateway {
    gateway_to(Provider::new("local", OpenAiChat, http(base)))
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
    let gateway = openai_gateway(format!("{provider}/v1"));
    let base = spawn(router(gateway)).await;

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
    let gateway = gateway_to(Provider::new("local", AnthropicMessages, http(provider)));
    let base = spawn(router(gateway)).await;

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
    let gateway = openai_gateway(format!("{provider}/v1"));
    let base = spawn(router(gateway)).await;

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
    let app = router(gateway);
    let req = http::Request::post("/v1/chat/completions")
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
    let app = router(gateway);
    let broken = futures::stream::iter([
        Ok(bytes::Bytes::from_static(b"{\"model\"")),
        Err(std::io::Error::other("the client left")),
    ]);
    let req = http::Request::post("/v1/chat/completions")
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
async fn every_path_reaches_the_gateway_which_answers_404_for_what_no_format_accepts() {
    let gateway = openai_gateway("http://127.0.0.1:9");
    let status = |method: http::Method, path: &'static str| {
        let app = router(gateway.clone());
        async move {
            let req = http::Request::builder().method(method).uri(path);
            let req = req.body(axum::body::Body::from("{}"));
            let resp = tower::ServiceExt::oneshot(app, req.unwrap_or_else(|e| panic!("{e}")));
            resp.await.unwrap_or_else(|e| match e {}).status()
        }
    };

    // OpenAI Chat accepts a POST to any path ending in its own, any query.
    assert_ne!(
        status(http::Method::POST, "/v1/chat/completions").await,
        404
    );
    assert_ne!(
        status(http::Method::POST, "/openai/chat/completions?beta=1").await,
        404
    );
    assert_eq!(status(http::Method::GET, "/v1/chat/completions").await, 404);
    assert_eq!(status(http::Method::POST, "/v1/gpt/chat").await, 404);

    let app = router(gateway.clone());
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
    assert_eq!(
        body.unwrap_or_default(),
        "no format served accepts POST /v1/gpt/chat"
    );
}

#[tokio::test]
async fn it_joins_its_base_with_the_path_and_sends_its_headers() {
    let provider = spawn(Router::new().route(
        "/v1beta/openai/chat/completions",
        post(|headers: http::HeaderMap| async move {
            let all = |name| {
                let values = headers.get_all(name).iter();
                values.filter_map(|v| v.to_str().ok()).collect::<Vec<_>>()
            };
            let right = all("authorization") == ["Bearer sk"] && all("x-team") == ["a", "b"];
            let body = if right { SSE } else { "wrong headers" };
            ([("content-type", "text/event-stream")], body).into_response()
        }),
    ))
    .await;
    let value = http::HeaderValue::from_static;
    let mut headers = http::HeaderMap::new();
    headers.insert("authorization", value("Bearer sk"));
    headers.append("x-team", value("a"));
    headers.append("x-team", value("b"));
    let transport = http(format!("{provider}/v1beta/openai/")).with_headers(headers);
    let transport = transport.unwrap_or_else(|e| panic!("{e}"));
    let gateway = gateway_to(Provider::new("local", OpenAiChat, transport));
    let base = spawn(router(gateway)).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/v1/chat/completions"))
        .header("authorization", "Bearer client")
        .header("x-team", "client")
        .body(r#"{"model":"m","stream":true,"messages":[]}"#)
        .send()
        .await
        .unwrap_or_else(|e| panic!("send: {e}"));

    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap_or_default(), SSE);
}

#[test]
fn a_base_url_needs_http_or_https_and_a_host() {
    for url in [
        "http://[::1]",
        "http://[::1]:8080",
        "https://h.test:443/v1beta",
        "https://h.test/models/m@2",
    ] {
        assert!(HttpTransport::new(url).is_ok(), "{url}");
    }
    // A base URL may hold a key, so the error does not echo it.
    let expected = "bad base URL: an http or https one with a host, and no query, fragment or credentials (those go in its headers)";
    for url in [
        "api.openai.com",
        "https://h.test?api-version=1",
        "https://h.test/#f",
        "https://user:ab/cd@h.test",
        "https://key:sec ret@h.test",
        "https://key:secret@h.test",
        "https://key@h.test",
        "https://:443",
        "ftp://h.test",
        "https://h.test:99999",
        "http://127.0.0.1:0",
        "http://1.2.3.256",
        "http://[::1%25eth0]",
    ] {
        let fault = HttpTransport::new(url).err();
        assert_eq!(fault.as_ref().map(|f| f.message()), Some(expected), "{url}");
    }
}

#[test]
fn a_connection_header_is_refused() {
    let one = |name: &str| {
        let name = http::HeaderName::from_bytes(name.as_bytes()).unwrap_or_else(|e| panic!("{e}"));
        http::HeaderMap::from_iter([(name, http::HeaderValue::from_static("4"))])
    };
    for name in [
        "host",
        "content-length",
        "connection",
        "transfer-encoding",
        "te",
        "upgrade",
        "expect",
        "proxy-authorization",
    ] {
        let fault = http("https://h.test").with_headers(one(name)).err();
        let expected = format!("header '{name}' is the connection's");
        assert_eq!(fault.as_ref().map(|f| f.message()), Some(&*expected));
    }
    assert!(http("https://h.test").with_headers(one("x-team")).is_ok());
}

/// A call's request head to `uri`, with the client's `headers`.
fn call_head(uri: &str, headers: &[(&'static str, &'static str)]) -> http::request::Parts {
    let mut req = http::Request::new(());
    *req.uri_mut() = uri.parse().unwrap_or_else(|e| panic!("{e}"));
    for (name, value) in headers {
        let value = http::HeaderValue::from_static(value);
        req.headers_mut().append(*name, value);
    }
    req.into_parts().0
}

fn headers(pairs: &[(&'static str, &'static str)]) -> http::HeaderMap {
    let pair = |(n, v): &(&'static str, &'static str)| {
        (
            http::HeaderName::from_static(n),
            http::HeaderValue::from_static(v),
        )
    };
    pairs.iter().map(pair).collect()
}

const CLIENT: [(&str, &str); 7] = [
    ("authorization", "Bearer client"),
    ("x-api-key", "client"),
    ("x-auth-token", "client"),
    ("idempotency-key", "i1"),
    ("x-idempotency-key", "i2"),
    ("openai-beta", "b"),
    ("x-tokenizer", "t"),
];

#[test]
fn without_headers_of_its_own_the_clients_credentials_pass_through() {
    let mut head = call_head("/v1/chat?key=k&beta=1", &CLIENT);
    let before = head.headers.clone();

    add_own_headers(&mut head, &http::HeaderMap::new()).unwrap_or_else(|e| panic!("{e}"));

    assert_eq!(head.headers, before);
    assert_eq!(head.uri, "/v1/chat?key=k&beta=1");
}

#[test]
fn headers_of_its_own_take_the_clients_credentials_off() {
    let query = "/v1/chat?key=a&api_key=b&access_token=c&api-version=1";
    let mut head = call_head(query, &CLIENT);

    let own = headers(&[("x-team", "t")]);
    add_own_headers(&mut head, &own).unwrap_or_else(|e| panic!("{e}"));

    let got = |name| head.headers.get(name).and_then(|v| v.to_str().ok());
    for gone in ["authorization", "x-api-key", "x-auth-token"] {
        assert_eq!(got(gone), None, "{gone}");
    }
    for (name, value) in CLIENT.iter().skip(3) {
        assert_eq!(got(*name), Some(*value), "{name}");
    }
    assert_eq!(got("x-team"), Some("t"));
    assert_eq!(head.uri, "/v1/chat?api-version=1");
}

#[test]
fn a_calls_own_headers_are_the_transports_for_that_call() {
    // With transport headers, the call's replace those of the same name.
    let mut head = call_head("/v1/chat?key=a", &CLIENT);
    let call = headers(&[("authorization", "Bearer tenant"), ("host", "evil")]);
    head.extensions.insert(CallHeaders(call));
    let own = headers(&[("authorization", "Bearer operator"), ("x-team", "t")]);

    add_own_headers(&mut head, &own).unwrap_or_else(|e| panic!("{e}"));

    let all = |name| {
        head.headers
            .get_all(name)
            .iter()
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(all("authorization"), ["Bearer tenant"]);
    assert_eq!(all("x-team"), ["t"]);
    assert!(all("host").is_empty());
    assert!(all("x-api-key").is_empty());
    assert_eq!(head.uri, "/v1/chat");

    // Without transport headers, the call's still remove the client's
    // credentials.
    let mut head = call_head("/v1/chat", &CLIENT);
    let call = headers(&[("x-api-key", "tenant")]);
    head.extensions.insert(CallHeaders(call));

    add_own_headers(&mut head, &http::HeaderMap::new()).unwrap_or_else(|e| panic!("{e}"));

    let all = |name| {
        head.headers
            .get_all(name)
            .iter()
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(all("x-api-key"), ["tenant"]);
    assert!(all("authorization").is_empty());
}

#[tokio::test]
async fn a_204_ends_complete_though_its_body_is_never_polled() {
    /// Records each request's outcome.
    #[derive(Clone, Default)]
    struct Log(Arc<Mutex<Vec<Outcome>>>);

    #[async_trait]
    impl Plugin for Log {
        async fn on_client_end(&self, _cx: &Context, outcome: &Outcome) {
            self.0.lock().map(|mut l| l.push(outcome.clone())).ok();
        }
    }

    /// Answers 204, from events its format writes as a body.
    struct NoContent;

    #[async_trait]
    impl Plugin for NoContent {
        async fn on_client(&self, _cx: &mut Context) -> Result<Option<Response>, Fault> {
            let resp = http::Response::builder()
                .status(http::StatusCode::NO_CONTENT)
                .body(futures::stream::iter([Ok(Reply::text(0, "hi"))]))?;
            Ok(Some(Response::stream(resp)))
        }
    }

    let log = Log::default();
    let gateway = Gateway::new([])
        .and_then(|g| g.serve(OpenAiChat)?.plugin("log", log.clone()))
        .and_then(|g| g.plugin("no-content", NoContent))
        .unwrap_or_else(|e| panic!("gateway: {e}"));
    let base = spawn(router(gateway.clone())).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/v1/chat/completions"))
        .body(r#"{"model":"m","messages":[]}"#)
        .send()
        .await
        .unwrap_or_else(|e| panic!("send: {e}"));
    assert_eq!(resp.status(), 204);
    assert!(resp.bytes().await.unwrap_or_default().is_empty());
    gateway.idle().await;

    let all = log.0.lock().map(|l| l.clone()).unwrap_or_default();
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].end, Ok(()));
}
