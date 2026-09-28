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
    Fault, Gateway, Outcome, Plugin, ProviderContext, Usage, async_trait, http,
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
    println!("usage:  {:?}", tally.0.lock().map(|u| u.clone()));
    Ok(())
}

const ANSWER: &str = r#"{"id":"c1","object":"chat.completion","model":"gpt-4o-mini","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"hi"}}],"usage":{"prompt_tokens":3,"completion_tokens":1}}"#;

#[cfg(test)]
mod tests {
    use super::*;
    use respan_gateway_formats::conversation::units;

    #[tokio::test]
    async fn handle_returns_the_response_and_its_outcome_with_no_server() -> Result<(), Fault> {
        let mock = MockTransport::new();
        mock.reply(OPENAI, mock::json(http::StatusCode::OK, ANSWER));
        let tally = Tally::default();
        let gw = gateway(&mock, tally.clone())?;
        let (body, outcome) = ask(&gw, "Say hi").await?;
        assert_eq!(body, ANSWER, "a relay passes the provider's bytes");
        assert!(outcome.end.is_ok());
        assert_eq!(outcome.head.map(|h| h.status), Some(http::StatusCode::OK));
        gw.idle().await;
        let usage = tally.0.lock().map(|u| u.clone()).unwrap_or_default();
        assert_eq!(usage.get(units::INPUT_TOKENS), Some(3.0));
        assert_eq!(usage.get(units::OUTPUT_TOKENS), Some(1.0));
        Ok(())
    }

    #[tokio::test]
    async fn a_provider_failure_is_an_outcome_not_a_panic() -> Result<(), Fault> {
        // Nothing queued: the mock answers 502 upstream.
        let mock = MockTransport::new();
        let gw = gateway(&mock, Tally::default())?;
        let (_, outcome) = ask(&gw, "Say hi").await?;
        assert_eq!(
            outcome.end.err().map(|f| f.code().to_owned()),
            Some("upstream".into())
        );
        Ok(())
    }
}
