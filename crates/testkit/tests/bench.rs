//! Timing of a plain relay, unchanged, whole and streamed. Run with
//! `cargo test --release -p respan-gateway-testkit --test bench -- --ignored --nocapture`.

use std::time::Instant;

use respan_gateway_core::futures::TryStreamExt;
use respan_gateway_core::{Fault, Gateway, async_trait};
use respan_gateway_formats::OpenAiChat;
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::http::{self, StatusCode};
use respan_gateway_http::http_body_util::Full;
use respan_gateway_http::{Exchange, Transport};
use respan_gateway_http::{RequestHead, ResponseHead};
use respan_gateway_testkit::{post, read_whole, serve};

/// Reads each call's body and answers with `.0`, as JSON or SSE.
struct Canned(Bytes, &'static str);

#[async_trait]
impl Transport for Canned {
    async fn send(&self, call: Exchange<RequestHead>) -> Result<Exchange<ResponseHead>, Fault> {
        let _: Vec<Bytes> = call.body.try_collect().await?;
        let mut resp = http::Response::new(Full::new(self.0.clone()));
        let value = http::HeaderValue::from_static(self.1);
        resp.headers_mut().insert(http::header::CONTENT_TYPE, value);
        Ok(respan_gateway_http::from_response(resp))
    }
}

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
    let gw = Gateway::new()
        .provider(
            "p",
            OpenAiChat,
            Canned(Bytes::from(reply.to_owned()), content_type),
        )
        .and_then(|g| g.client(OpenAiChat))
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
