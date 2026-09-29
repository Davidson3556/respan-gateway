//! Use case 14, the gateway as a library: another Rust service runs the
//! pipeline in-process, with no server and no second hop. It builds a
//! [`Gateway`], hands [`Gateway::handle`] a request on the HTTP wire (an
//! [`Accepted`] `http` request head and its body, whole), reads the answer
//! and awaits how the request ended. With no HTTP hop, it needs none of the
//! adapter's rules (`respan_gateway_http::serve` applies them); no answer
//! means no format took the request, and the end says why.
//!
//! The kernel counts no usage:
//! the embedder tallies it with a plugin that sums what each call's
//! provider reports (`Meter`), as billing would.
//!
//! `cargo run -p respan-gateway-examples --example embedded`

use respan_gateway_core::futures::{StreamExt, stream};
use respan_gateway_core::{Extensions, Fault, Gateway};
use respan_gateway_examples::Meter;
use respan_gateway_formats::OpenAiChat;
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::http_body_util::BodyExt;
use respan_gateway_http::{Accepted, http};
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
/// and how the request ended.
async fn ask(
    gw: &Gateway,
    prompt: &str,
) -> Result<(http::StatusCode, Bytes, Result<(), Fault>), Fault> {
    let body = serde_json::json!({
        "model": "gpt-4o-mini",
        "messages": [{"role": "user", "content": prompt}],
    });
    let (head, ()) = http::Request::post("/v1/chat/completions")
        .header("content-type", "application/json")
        .body(())
        .map_err(respan_gateway_http::built_wrong)?
        .into_parts();
    let body = stream::iter([Ok(Bytes::from(body.to_string()))]).boxed();
    let (wire, answer) = Accepted::new(head, body, false);
    let handled = gw.handle(wire, Extensions::new()).await;
    let Some((resp, end)) = answer.await else {
        let fault = handled.err();
        return Err(fault.unwrap_or_else(|| Fault::internal("no format answered")));
    };
    // Read the body first: the end resolves once the response ends.
    let (head, body) = resp.into_parts();
    let body = body.collect().await?.to_bytes();
    Ok((head.status, body, end.await))
}

#[tokio::main]
async fn main() -> Result<(), Fault> {
    let mock = MockTransport::new();
    mock.reply(OPENAI, mock::json(http::StatusCode::OK, ANSWER));
    let tally = Meter::default();
    let gw = gateway(&mock, tally.clone())?;
    let (status, body, end) = ask(&gw, "Say hi").await?;
    // End hooks run detached; wait for them before reading the tally.
    gw.idle().await;
    println!("status: {status}, ended: {:?}", end);
    println!("body:   {}", String::from_utf8_lossy(&body));
    println!("usage:  {:?}", tally.usage());
    Ok(())
}

const ANSWER: &str = r#"{"id":"c1","object":"chat.completion","model":"gpt-4o-mini","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"hi"}}],"usage":{"prompt_tokens":3,"completion_tokens":1}}"#;

#[cfg(test)]
#[path = "tests/embedded.rs"]
mod tests;
