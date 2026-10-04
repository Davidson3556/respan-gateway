//! Timing of a plain relay, unchanged, whole and streamed. Run with
//! `cargo bench -p respan-gateway-providers --bench relay -- --ignored --nocapture`.

use respan_gateway_sdk::conversation::Conversation;
use std::time::Instant;

use respan_gateway_http::Body;
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::http::{self, StatusCode};
use respan_gateway_http::http_body_util::BodyExt;
use respan_gateway_providers::OpenAiChat;
use respan_gateway_sdk::Gateway;
#[allow(dead_code, unused_imports)]
#[path = "../../../tests/support/http.rs"]
mod http_helpers;
use http_helpers::{LIMIT, post, read_whole, serve};

/// A chat request of about 16 KB, as an agent's history makes.
fn request(stream: bool) -> Bytes {
    let messages: Vec<_> = (0..40)
        .map(|i| {
            let role = if i % 2 == 0 { "user" } else { "assistant" };
            serde_json::json!({ "role": role, "content": "lorem ipsum dolor sit amet ".repeat(15) })
        })
        .collect();
    let body = serde_json::json!({ "model": "m", "stream": stream, "messages": messages });
    Bytes::from(body.to_string())
}

async fn time(name: &str, stream: bool, reply: &str, content_type: &'static str) {
    let reply = Bytes::from(reply.to_owned());
    let transport = move |call: http::Request<Body>| {
        let reply = reply.clone();
        async move {
            call.into_body().collect().await?;
            let mut resp = http::Response::new(Body::full(reply));
            let value = http::HeaderValue::from_static(content_type);
            resp.headers_mut().insert(http::header::CONTENT_TYPE, value);
            Ok((resp, None))
        }
    };
    let gw = Gateway::new()
        .provider::<Conversation, _, _>(
            "p",
            OpenAiChat,
            respan_gateway_http::connect(transport, LIMIT),
        )
        .and_then(|g| g.client::<Conversation, _, _>(OpenAiChat))
        .and_then(|g| g.target("p"))
        .unwrap_or_else(|f| panic!("{f}"));
    let body = request(stream);
    let run = |n: usize| {
        let (gw, body) = (gw.clone(), body.clone());
        async move {
            for _ in 0..n {
                let req = post("/v1/chat/completions", body.clone());
                let (status, _, out) = read_whole(serve(&gw, req).await.0).await;
                assert!(status == StatusCode::OK && out.is_ok());
            }
        }
    };
    run(2_000).await;
    let n = 20_000;
    let start = Instant::now();
    run(n).await;
    let each = start.elapsed() / n as u32;
    println!("{name}: {each:?} per request ({} byte body)", body.len());
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "a benchmark"]
async fn a_plain_relay_is_cheap() {
    let whole = r#"{"id":"c1","object":"chat.completion","model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":1}}"#;
    time("whole", false, whole, "application/json").await;
    let chunk = |c: &str| {
        format!(
            "data: {{\"id\":\"c1\",\"model\":\"m\",\"choices\":[{{\"index\":0,\"delta\":{{\"content\":\"{c}\"}}}}]}}\n\n"
        )
    };
    let mut sse: String = (0..20).map(|i| chunk(&format!("w{i} "))).collect();
    sse.push_str("data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n");
    sse.push_str("data: {\"id\":\"c1\",\"choices\":[],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":20}}\n\n");
    sse.push_str("data: [DONE]\n\n");
    time("streamed", true, &sse, "text/event-stream").await;
}
