#[allow(dead_code)]
#[path = "../../../tests/support/http.rs"]
mod http_support;
use http_support::LIMIT;

use axum::Router;
use axum::response::IntoResponse;
use axum::routing::post;
use parking_lot::Mutex;
use respan_gateway_conversation::{Input, Output};
use std::sync::Arc;

use respan_gateway_conversation_http::anthropic::AnthropicMessages;
use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_core::{
    ClientContext, Extensions, Fault, Format, Gateway, Plugin, Provider, async_trait,
};
use respan_gateway_http::call::{CallHeaders, credentials};
use respan_gateway_http::http;
use respan_gateway_http::{Body, Completion, ProviderIo};
use respan_gateway_http_transport::HttpTransport;
use respan_gateway_serve::router;
use tokio::net::TcpListener;

use respan_gateway_plugin_relay::Relay;

const SSE: &str =
    "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n";

/// What a client heard of an OpenAI chat stream: the text its chunks carry and
/// whether it ended with `[DONE]`. The gateway re-encodes the provider's
/// stream, so its framing is not compared.
fn heard(body: &str) -> (String, bool) {
    let frames: Vec<&str> = body
        .split("\n\n")
        .filter_map(|frame| frame.strip_prefix("data: "))
        .collect();
    let text = frames
        .iter()
        .filter_map(|data| serde_json::from_str::<serde_json::Value>(data).ok())
        .filter_map(|chunk| {
            chunk["choices"][0]["delta"]["content"]
                .as_str()
                .map(str::to_owned)
        })
        .collect();
    (text, frames.last() == Some(&"[DONE]"))
}

fn http(base: impl Into<String>) -> HttpTransport {
    HttpTransport::new(base).unwrap_or_else(|e| panic!("{e}"))
}

/// A gateway serving OpenAI clients, sent to the provider speaking
/// `format` over `transport`, its target.
fn gateway_to<F, T>(format: F, transport: T) -> Gateway
where
    F: Format<ProviderIo, Output, Input>,
    T: Fn(Extensions) -> ProviderIo + Send + Sync + 'static,
{
    Gateway::new()
        .provider(Provider::new("local", format, transport))
        .and_then(|g| g.client::<Input, _, _>(OpenAiChat))
        .and_then(|g| g.target("local"))
        .unwrap_or_else(|e| panic!("gateway: {e}"))
}

/// A gateway relaying OpenAI clients to the provider at the origin `base`,
/// its API under `/v1`.
fn openai_gateway(base: impl Into<String>) -> Gateway {
    let base = format!("{}/v1", base.into());
    gateway_to(OpenAiChat, http(base).connect()).plugin(Relay)
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
    let gateway = openai_gateway(provider.clone());
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
    assert_eq!(
        heard(&resp.text().await.unwrap_or_default()),
        ("hi".to_owned(), true)
    );
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
    let gateway = gateway_to(AnthropicMessages, http(provider).connect());
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
    let gateway = openai_gateway(provider.clone());
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
async fn an_upgrade_the_server_does_not_switch_is_ignored() {
    /// Notes each call's head and body, and answers a unary reply.
    #[derive(Clone, Default)]
    struct Note(Arc<Mutex<Vec<(http::HeaderMap, bytes::Bytes)>>>);

    impl Note {
        pub async fn send(
            &self,
            call: http::Request<Body>,
        ) -> Result<(http::Response<Body>, Option<Completion>), Fault> {
            use respan_gateway_http::http_body_util::BodyExt;
            let (head, body) = call.into_parts();
            let body = body.collect().await?.to_bytes();
            self.0.lock().push((head.headers, body));
            let unary = r#"{"id":"c","choices":[{"message":{"content":"hi"}}]}"#;
            let mut resp = http::Response::new(Body::full(unary));
            let json = http::HeaderValue::from_static("application/json");
            resp.headers_mut().insert("content-type", json);
            Ok((resp, None))
        }
    }
    impl Note {
        fn connect(self) -> impl Fn(Extensions) -> ProviderIo + Send + Sync {
            respan_gateway_http::connect(
                move |call| {
                    let value = self.clone();
                    async move { value.send(call).await }
                },
                LIMIT,
            )
        }
    }

    let note = Note::default();
    let app = router(gateway_to(OpenAiChat, note.clone().connect()));
    let chat = r#"{"model":"m","messages":[]}"#;
    let req = http::Request::post("/v1/chat/completions")
        .header("upgrade", "h2c")
        .header("connection", "Upgrade, HTTP2-Settings")
        .header("http2-settings", "AAMAAABkAAQAAP__")
        .body(axum::body::Body::from(chat))
        .unwrap_or_else(|e| panic!("{e}"));

    let resp = tower::ServiceExt::oneshot(app, req)
        .await
        .unwrap_or_else(|e| match e {});

    assert_eq!(resp.status(), 200);
    let noted = note.0.lock().clone();
    let [(headers, body)] = &noted[..] else {
        panic!("{} calls", noted.len());
    };
    assert_eq!(&body[..], chat.as_bytes());
    for name in ["upgrade", "connection", "http2-settings"] {
        assert!(!headers.contains_key(name), "{name}");
    }
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
    let gateway = gateway_to(OpenAiChat, transport.connect());
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
    assert_eq!(
        heard(&resp.text().await.unwrap_or_default()),
        ("hi".to_owned(), true)
    );
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

/// A call of `head` with `ext`, as the kernel hands a transport.
fn exchange(
    head: http::request::Parts,
    ext: respan_gateway_core::Extensions,
) -> http::Request<Body> {
    let mut call = http::Request::from_parts(head, Body::full(""));
    call.extensions_mut().insert(ext);
    call
}

// The adapter lifts the client's credentials into an extension, which a
// transport with no keys of its own puts back when the call asks, as a
// relay's does.
#[tokio::test]
async fn without_headers_of_its_own_a_relay_passes_the_clients_credentials() {
    use respan_gateway_http::http_body_util::BodyExt;
    use respan_gateway_passthrough_http as passthrough;
    use respan_gateway_passthrough_http::Passthrough;
    let head = call_head("/v1/chat?key=k&beta=1", &CLIENT);
    let before = head.headers.clone();
    let gateway = Gateway::new()
        .client::<passthrough::Input<Passthrough>, _, _>(Passthrough)
        .expect("valid test setup")
        .provider(Provider::new(
            "relay",
            Passthrough,
            respan_gateway_http::connect(
                move |mut call| {
                    let before = before.clone();
                    async move {
                        credentials(&mut call, &http::HeaderMap::new());
                        assert_eq!(call.headers(), &before);
                        assert_eq!(call.uri(), "/v1/chat?beta=1&key=k");
                        Ok((http::Response::new(Body::full("ok")), None))
                    }
                },
                LIMIT,
            ),
        ))
        .expect("valid test setup")
        .target("relay")
        .expect("valid test setup")
        .plugin(Relay);
    let (answer, ended) = respan_gateway_http::handle(
        &gateway,
        http::Request::from_parts(head, Body::full("")),
        LIMIT,
    )
    .await;
    BodyExt::collect(answer.into_body())
        .await
        .expect("valid test setup");
    ended.await.expect("valid test setup");
}

#[test]
fn headers_of_its_own_take_the_clients_credentials_off() {
    let query = "/v1/chat?key=a&api_key=b&access_token=c&api-version=1";
    let head = call_head(query, &CLIENT);
    let mut call = exchange(head, respan_gateway_core::Extensions::new());

    let own = headers(&[("x-team", "t")]);
    credentials(&mut call, &own);
    let (head, _) = call.into_parts();

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
    let head = call_head("/v1/chat?key=a", &CLIENT);
    let mut ext = respan_gateway_core::Extensions::new();
    let call = headers(&[("authorization", "Bearer tenant"), ("host", "evil")]);
    ext.insert(CallHeaders(call));
    let own = headers(&[("authorization", "Bearer operator"), ("x-team", "t")]);
    let mut call = exchange(head, ext);

    credentials(&mut call, &own);
    let (head, _) = call.into_parts();

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
    let head = call_head("/v1/chat", &CLIENT);
    let mut ext = respan_gateway_core::Extensions::new();
    let call = headers(&[("x-api-key", "tenant")]);
    ext.insert(CallHeaders(call));
    let mut call = exchange(head, ext);

    credentials(&mut call, &http::HeaderMap::new());
    let (head, _) = call.into_parts();

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
    /// Records how each request ended.
    #[derive(Clone, Default)]
    struct Log(Arc<Mutex<Vec<Result<(), Fault>>>>);

    #[async_trait]
    impl Plugin for Log {
        async fn on_client_end(&self, _cx: &ClientContext, end: &Result<(), Fault>) {
            self.0.lock().push(end.clone());
        }
    }

    // A provider that answers 204, relayed as it came.
    let provider = spawn(Router::new().route(
        "/v1/chat/completions",
        post(|| async { http::StatusCode::NO_CONTENT }),
    ))
    .await;
    let log = Log::default();
    let gateway = Gateway::new()
        .provider(Provider::new(
            "local",
            OpenAiChat,
            http(format!("{provider}/v1")).connect(),
        ))
        .and_then(|g| g.client::<Input, _, _>(OpenAiChat))
        .map(|g| g.plugin(log.clone()).plugin(Relay))
        .and_then(|g| g.target("local"))
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

    let all = log.0.lock().clone();
    assert_eq!(all.len(), 1);
    assert!(all[0].is_ok());
}

#[tokio::test]
async fn it_strips_the_provider_origins_own_headers() {
    let provider = spawn(Router::new().route(
        "/v1/chat/completions",
        post(|| async {
            let headers = [
                ("content-type", "text/event-stream"),
                ("set-cookie", "__cf_bm=x; Domain=.openai.com"),
                ("alt-svc", "h3=\":443\""),
                ("strict-transport-security", "max-age=31536000"),
                ("x-request-id", "req_1"),
            ];
            (headers, SSE).into_response()
        }),
    ))
    .await;
    let gateway = openai_gateway(provider.clone());
    let base = spawn(router(gateway)).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/v1/chat/completions"))
        .body(r#"{"model":"m","stream":true,"messages":[]}"#)
        .send()
        .await
        .unwrap_or_else(|e| panic!("send: {e}"));

    for origin in ["set-cookie", "alt-svc", "strict-transport-security"] {
        assert!(resp.headers().get(origin).is_none(), "{origin}");
    }
    assert_eq!(resp.headers()["x-request-id"], "req_1");
}

// The client's cookies and forwarding headers are the transport's to strip;
// other end-to-end headers pass.
#[tokio::test]
async fn it_strips_the_clients_cookies_and_forwarding_headers() {
    let provider = spawn(Router::new().route(
        "/v1/chat/completions",
        post(|headers: http::HeaderMap| async move {
            let hop = ["cookie", "forwarded", "via", "x-forwarded-for", "x-real-ip"];
            let leaked: Vec<&str> = hop
                .into_iter()
                .filter(|h| headers.contains_key(*h))
                .collect();
            let body = match (leaked.is_empty(), headers.contains_key("openai-beta")) {
                (true, true) => SSE.to_owned(),
                _ => format!("leaked {leaked:?}"),
            };
            ([("content-type", "text/event-stream")], body).into_response()
        }),
    ))
    .await;
    let gateway = openai_gateway(provider.clone());
    let base = spawn(router(gateway)).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/v1/chat/completions"))
        .header("cookie", "session=1")
        .header("forwarded", "for=1.2.3.4")
        .header("via", "1.1 edge")
        .header("x-forwarded-for", "1.2.3.4")
        .header("x-real-ip", "1.2.3.4")
        .header("openai-beta", "b")
        .body(r#"{"model":"m","stream":true,"messages":[]}"#)
        .send()
        .await
        .unwrap_or_else(|e| panic!("send: {e}"));

    assert_eq!(
        heard(&resp.text().await.unwrap_or_default()),
        ("hi".to_owned(), true)
    );
}

#[tokio::test]
async fn it_sends_a_whole_body_with_its_length_and_streams_one_that_streams() {
    use respan_gateway_core::futures;
    use respan_gateway_http::bytes::Bytes;
    use respan_gateway_http::http_body_util::BodyExt;

    // Echoes the body, prefixed by its `content-length`, or `-` if none.
    let provider = spawn(Router::new().route(
        "/echo",
        post(|headers: http::HeaderMap, body: String| async move {
            let length = headers.get("content-length").and_then(|v| v.to_str().ok());
            format!("{} {body}", length.unwrap_or("-"))
        }),
    ))
    .await;
    let transport = http(&provider);
    // Full bodies carry a known size; streamed bodies remain unknown-sized.
    let call = |body: Body| {
        http::Request::builder()
            .method(http::Method::POST)
            .uri("/echo")
            .body(body)
            .expect("valid test setup")
    };
    let read = |(resp, _): (http::Response<Body>, Option<Completion>)| async move {
        let body = BodyExt::collect(resp.into_body())
            .await
            .map(|b| b.to_bytes());
        String::from_utf8_lossy(&body.unwrap_or_default()).into_owned()
    };

    let whole = transport.send(call(Body::full("ab"))).await;
    let frames = ["a", "b"].map(|f| Ok::<_, Fault>(Bytes::from(f)));
    let streamed = futures::stream::unfold(frames.into_iter(), |mut f| async move {
        f.next().map(|x| (x, f))
    });
    let streamed = transport.send(call(Body::stream(streamed))).await;

    assert_eq!(read(whole.unwrap_or_else(|e| panic!("{e}"))).await, "2 ab");
    assert_eq!(
        read(streamed.unwrap_or_else(|e| panic!("{e}"))).await,
        "- ab"
    );
}

// ===== loop 6 audit: real bodiless answers over hyper =====

/// Records how each request and each call ended.
#[derive(Clone, Default)]
struct L6Ends(Arc<Mutex<Vec<String>>>);

#[async_trait]
impl Plugin for L6Ends {
    async fn on_client_end(&self, _cx: &ClientContext, end: &Result<(), Fault>) {
        let code = end
            .as_ref()
            .err()
            .map_or("ok", respan_gateway_core::Fault::code)
            .to_owned();
        self.0.lock().push(format!("request {code}"));
    }
    async fn on_provider_end(
        &self,
        _px: &respan_gateway_core::ProviderContext,
        end: &Result<(), Fault>,
    ) {
        let code = end
            .as_ref()
            .err()
            .map_or("ok", respan_gateway_core::Fault::code)
            .to_owned();
        self.0.lock().push(format!("call {code}"));
    }
}

/// HEADs and 204s (and 304s if `with_304`) relayed through passthrough over real HTTP (axum
/// upstream, `HttpTransport`, `router`), many times, many at once: each is
/// complete, so each request and call ends `ok`, never `abandoned`.
async fn l6_bodiless(with_304: bool) {
    use axum::routing::get;
    use respan_gateway_passthrough_http::Passthrough;
    let big = "x".repeat(1 << 20);
    let provider = spawn(
        Router::new()
            .route(
                "/x",
                get(move || {
                    let big = big.clone();
                    async move { big }
                }),
            )
            .route(
                "/nm",
                get(|| async { (http::StatusCode::NOT_MODIFIED, [("etag", "\"1\"")]) }),
            )
            .route("/nc", get(|| async { http::StatusCode::NO_CONTENT })),
    )
    .await;
    let ends = L6Ends::default();
    let gateway = Gateway::new()
        .provider(Provider::new(
            "any",
            Passthrough,
            http(provider.clone()).connect(),
        ))
        .and_then(|g| {
            g.client::<respan_gateway_passthrough_http::Input<
                respan_gateway_passthrough_http::Passthrough,
            >, _, _>(Passthrough)
        })
        .map(|g| g.plugin(ends.clone()))
        .and_then(|g| g.target("any"))
        .unwrap_or_else(|e| panic!("gateway: {e}"));
    let base = spawn(router(gateway.clone())).await;
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .expect("bounded client");
    let mut tasks = Vec::new();
    // Each request opens four local sockets across the client, gateway and
    // provider. Keep the 300-request sweep below a typical 1024-fd limit,
    // including the other test running in this process.
    let slots = Arc::new(tokio::sync::Semaphore::new(64));
    for i in 0..300 {
        let (client, base) = (client.clone(), base.clone());
        let slots = slots.clone();
        tasks.push(tokio::spawn(async move {
            let _slot = slots.acquire_owned().await.expect("request slot");
            let resp = match i % 3 {
                0 => client.head(format!("{base}/x")).send().await,
                1 if with_304 => client.get(format!("{base}/nm")).send().await,
                _ => client.get(format!("{base}/nc")).send().await,
            }
            .unwrap_or_else(|e| panic!("send: {e}"));
            let status = resp.status().as_u16();
            let _ = resp.bytes().await;
            status
        }));
    }
    for t in tasks {
        let status = t.await.unwrap_or_default();
        assert!([200, 304, 204].contains(&status), "{status}");
    }
    tokio::time::timeout(std::time::Duration::from_secs(10), gateway.idle())
        .await
        .expect("bodiless requests must settle without polling their response bodies");
    let all = ends.0.lock().clone();
    let bad: Vec<_> = all.iter().filter(|e| !e.ends_with(" ok")).collect();
    assert_eq!(all.len(), 600);
    assert!(
        bad.is_empty(),
        "{} of {} ended badly: {:?}",
        bad.len(),
        all.len(),
        &bad[..bad.len().min(6)]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn l6_real_heads_and_204s_end_ok() {
    l6_bodiless(false).await;
}

/// Passthrough accepts ordinary redirects, including a bodiless 304, so
/// both the request and its provider call settle successfully.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn l6_a_real_304_ends_ok() {
    l6_bodiless(true).await;
}

#[tokio::test]
async fn body_limits_can_differ_by_route_before_decoding() {
    let provider = spawn(Router::new().fallback(|| async { r#"{"choices":[]}"# })).await;
    let app = respan_gateway_serve::Host::new(openai_gateway(provider)).limit(|head| {
        if head.uri.path().starts_with("/large/") {
            1024
        } else {
            8
        }
    });
    let app = app.router();
    for (path, expected) in [
        ("/v1/chat/completions", 413),
        ("/large/chat/completions", 200),
    ] {
        let request = http::Request::post(path)
            .body(axum::body::Body::from(r#"{"model":"m","messages":[]}"#))
            .unwrap_or_else(|error| panic!("request: {error}"));
        let response = tower::ServiceExt::oneshot(app.clone(), request)
            .await
            .unwrap_or_else(|error| match error {});
        assert_eq!(response.status(), expected, "{path}");
    }
}

#[tokio::test]
async fn an_upgrade_to_websocket_among_others_is_a_session() {
    use respan_gateway_http::http_body_util::BodyExt;
    let app = router(openai_gateway("http://127.0.0.1:9"));
    // No key: a session the WebSocket edge refuses, not a plain request.
    let req = http::Request::get("/v1/chat/completions")
        .header("upgrade", "websocket, h2c")
        .header("connection", "Upgrade")
        .body(axum::body::Body::empty())
        .unwrap_or_else(|e| panic!("{e}"));
    let resp = tower::ServiceExt::oneshot(app, req)
        .await
        .unwrap_or_else(|e| match e {});
    assert_eq!(resp.status(), 400);
    let body = resp.into_body().collect().await.map(|b| b.to_bytes());
    assert_eq!(body.ok().as_deref(), Some(&b"bad_request"[..]));
}
