//! Use case 14, the gateway as a library: another Rust service runs the
//! pipeline in-process, with no server and no second hop. It builds a
//! [`Gateway`], hands [`Gateway::handle`] an `http::Request`, reads the
//! response and awaits the request's [`Outcome`].
//!
//! The request's `Outcome` says how it ended; each call's usage counts on
//! the provider edge (`on_provider_end`), so the embedder tallies it with a
//! plugin, as billing would.
//!
//! `cargo run -p respan-gateway-examples --example embedded`

use std::sync::{Arc, Mutex, PoisonError};

use respan_gateway_core::bytes::Bytes;
use respan_gateway_core::http_body_util::{BodyExt, Full};
use respan_gateway_core::{
    Fault, Gateway, Outcome, Plugin, ProviderContext, Usage, async_trait, http, serde_json,
};
use respan_gateway_formats::OpenAiChat;
use respan_gateway_testkit::{MockTransport, mock};

const OPENAI: &str = "http://openai.test/v1";

/// Adds up the usage of every call, on the provider edge where it counts.
#[derive(Clone, Default)]
struct Tally(Arc<Mutex<Usage>>);

#[async_trait]
impl Plugin for Tally {
    async fn on_provider_end(&self, _px: &ProviderContext, outcome: &Outcome) {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner) += &outcome.usage;
    }
}

impl Tally {
    fn usage(&self) -> Usage {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// The gateway, with no server. For real use, the transport is
/// `HttpTransport::new("https://api.openai.com/v1")?.with_headers(key)?`.
fn gateway(mock: &MockTransport, tally: Tally) -> Result<Gateway, Fault> {
    Gateway::new()
        .provider("openai", OpenAiChat, mock.at(OPENAI))?
        .serve(OpenAiChat)?
        .plugin("tally", tally)?
        .target("openai")
}

/// One chat request through the pipeline: the answer's body and the
/// request's outcome.
async fn ask(gw: &Gateway, prompt: &str) -> Result<(Bytes, Outcome), Fault> {
    let body = serde_json::json!({
        "model": "gpt-4o-mini",
        "messages": [{"role": "user", "content": prompt}],
    });
    let req = http::Request::post("/v1/chat/completions")
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(body.to_string())))?;
    let (resp, outcome) = gw.handle(req).await;
    // Read the body first: the outcome resolves once the response ends.
    let body = resp.into_body().collect().await?.to_bytes();
    Ok((body, outcome.await))
}

#[tokio::main]
async fn main() -> Result<(), Fault> {
    let mock = MockTransport::new();
    mock.reply(OPENAI, mock::json(http::StatusCode::OK, ANSWER));
    let tally = Tally::default();
    let gw = gateway(&mock, tally.clone())?;
    let (body, outcome) = ask(&gw, "Say hi").await?;
    // End hooks run detached; wait for them before reading the tally.
    gw.idle().await;
    let status = outcome.head.map(|h| h.status);
    println!("status: {status:?}, ended: {:?}", outcome.end);
    println!("body:   {}", String::from_utf8_lossy(&body));
    println!("usage:  {:?}", tally.usage());
    Ok(())
}

const ANSWER: &str = r#"{"id":"c1","object":"chat.completion","model":"gpt-4o-mini","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"hi"}}],"usage":{"prompt_tokens":3,"completion_tokens":1}}"#;

#[cfg(test)]
#[path = "tests/embedded.rs"]
mod tests;
