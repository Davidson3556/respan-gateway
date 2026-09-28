//! Use case 14, the gateway as a library: another Rust service runs the
//! pipeline in-process, with no server and no second hop. It builds a
//! [`Gateway`], hands [`Gateway::handle`] an `http::Request`, whole, reads
//! the response and awaits the request's [`Outcome`]. With no HTTP hop,
//! it needs no adapter; `Err` is what no format could write.
//!
//! The request's `Outcome` says how it ended. The kernel counts no usage:
//! the embedder tallies it with a plugin that sums what each call's
//! provider reports (`Meter`), as billing would.
//!
//! `cargo run -p respan-gateway-examples --example embedded`

use respan_gateway_core::bytes::Bytes;
use respan_gateway_core::http_body_util::{BodyExt, Full};
use respan_gateway_core::{Fault, Gateway, Outcome, http, serde_json};
use respan_gateway_examples::Meter;
use respan_gateway_formats::OpenAiChat;
use respan_gateway_testkit::{MockTransport, mock};

const OPENAI: &str = "http://openai.test/v1";

/// The gateway, with no server. For real use, the transport is
/// `HttpTransport::new("https://api.openai.com/v1")?.with_headers(key)?`.
fn gateway(mock: &MockTransport, tally: Meter) -> Result<Gateway, Fault> {
    Gateway::new()
        .provider("openai", OpenAiChat, mock.at(OPENAI))?
        .serve(OpenAiChat)?
        .plugin("tally", tally)?
        .target("openai")
}

/// One chat request through the pipeline: the answer's status and body,
/// and the request's outcome.
async fn ask(gw: &Gateway, prompt: &str) -> Result<(http::StatusCode, Bytes, Outcome), Fault> {
    let body = serde_json::json!({
        "model": "gpt-4o-mini",
        "messages": [{"role": "user", "content": prompt}],
    });
    let req = http::Request::post("/v1/chat/completions")
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(body.to_string())))?;
    let (resp, outcome) = gw.handle(req, false).await;
    let (head, body) = resp?.into_parts();
    // Read the body first: the outcome resolves once the response ends.
    let body = body.collect().await?.to_bytes();
    Ok((head.status, body, outcome.await))
}

#[tokio::main]
async fn main() -> Result<(), Fault> {
    let mock = MockTransport::new();
    mock.reply(OPENAI, mock::json(http::StatusCode::OK, ANSWER));
    let tally = Meter::default();
    let gw = gateway(&mock, tally.clone())?;
    let (status, body, outcome) = ask(&gw, "Say hi").await?;
    // End hooks run detached; wait for them before reading the tally.
    gw.idle().await;
    println!("status: {status}, ended: {:?}", outcome.end);
    println!("body:   {}", String::from_utf8_lossy(&body));
    println!("usage:  {:?}", tally.usage());
    Ok(())
}

const ANSWER: &str = r#"{"id":"c1","object":"chat.completion","model":"gpt-4o-mini","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"hi"}}],"usage":{"prompt_tokens":3,"completion_tokens":1}}"#;

#[cfg(test)]
#[path = "tests/embedded.rs"]
mod tests;
