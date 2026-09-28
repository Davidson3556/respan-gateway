//! Use case 12, a local proxy in front of Claude Code: no config file, one
//! Anthropic provider, and the client's own credentials (API key or
//! subscription token) passed through, since the transport sets no headers
//! of its own. Beta headers, unknown request fields and unknown response
//! events relay byte for byte: formats of one name relay, never re-encode.
//!
//! `cargo run -p respan-gateway-examples --example local_proxy` runs offline
//! against a mock; add `-- --serve 127.0.0.1:8080` to listen for real, then
//! `ANTHROPIC_BASE_URL=http://127.0.0.1:8080 claude`.

use respan_gateway_core::http::{self, HeaderValue};
use respan_gateway_core::{Fault, Gateway, Transport};
use respan_gateway_formats::AnthropicMessages;
use respan_gateway_server::{HttpTransport, router};
use respan_gateway_testkit::{MockTransport, mock, post, read_whole};

const ANTHROPIC: &str = "https://api.anthropic.com";

/// The whole setup, in code: serve Anthropic Messages, relay to Anthropic.
fn gateway(transport: impl Transport) -> Result<Gateway, Fault> {
    Gateway::new()
        .provider("anthropic", AnthropicMessages, transport)?
        .serve(AnthropicMessages)?
        .target("anthropic")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if let Some(addr) = args
        .iter()
        .position(|a| a == "--serve")
        .and_then(|i| args.get(i + 1))
    {
        // No `with_headers`: each request carries the developer's own key.
        let gw = gateway(HttpTransport::new(ANTHROPIC)?)?;
        let listener = tokio::net::TcpListener::bind(addr).await?;
        println!("listening on {addr}; ANTHROPIC_BASE_URL=http://{addr}");
        axum::serve(listener, router(gw)).await?;
        return Ok(());
    }
    let mock = MockTransport::new();
    mock.reply(ANTHROPIC, mock::sse(STREAM));
    let gw = gateway(mock.at(ANTHROPIC))?;
    let (status, _, body) = read_whole(gw.handle(request()).await.0).await;
    let sent = &mock.sent()[0];
    println!("provider got: {} {:?}", sent.uri(), sent.headers());
    println!(
        "client got:   {status}\n{}",
        String::from_utf8_lossy(&body?)
    );
    Ok(())
}

/// What Claude Code sends: a subscription token, a beta header, and fields
/// the gateway has never heard of, spaced as the client spaced them.
const REQUEST: &str = r#"{"model":"claude-sonnet-4-5", "max_tokens":1024,"stream":true,
 "messages":[{"role":"user","content":"hi"}],
 "thinking":{"type":"enabled","budget_tokens":2048}, "future_field":[1, 2.50]}"#;

fn request() -> http::Request<respan_gateway_core::Body> {
    let mut req = post("/v1/messages", REQUEST);
    let headers = req.headers_mut();
    for (name, value) in [
        ("authorization", "Bearer sk-ant-oat-subscription"),
        ("anthropic-beta", "interleaved-thinking-2025-05-14"),
        ("anthropic-version", "2023-06-01"),
        ("content-type", "application/json"),
    ] {
        headers.insert(name, HeaderValue::from_static(value));
    }
    req
}

/// A stream with an event type and a field no format knows.
const STREAM: &[&str] = &[
    "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-5\",\"content\":[],\"usage\":{\"input_tokens\":3,\"output_tokens\":0}}}\n\n",
    "event: brand_new_event\ndata: {\"type\":\"brand_new_event\",\"x\":1}\n\n",
    "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\",\"citations_v9\":[]}}\n\n",
    "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hello\"}}\n\n",
    "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
    "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\n\n",
    "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
];

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn credentials_beta_headers_and_unknown_fields_pass_untouched() -> Result<(), Fault> {
        let mock = MockTransport::new();
        mock.reply(ANTHROPIC, mock::sse(STREAM));
        let gw = gateway(mock.at(ANTHROPIC))?;
        let (status, _, body) = read_whole(gw.handle(request()).await.0).await;
        assert_eq!(status, http::StatusCode::OK);
        // The response, unknown event and field included, is the provider's bytes.
        assert_eq!(body?, STREAM.concat());
        let sent = &mock.sent()[0];
        assert_eq!(sent.uri(), "https://api.anthropic.com/v1/messages");
        assert_eq!(
            sent.body(),
            REQUEST,
            "the request body relays byte for byte"
        );
        let header = |name| sent.headers().get(name).and_then(|v| v.to_str().ok());
        assert_eq!(
            header("authorization"),
            Some("Bearer sk-ant-oat-subscription")
        );
        assert_eq!(
            header("anthropic-beta"),
            Some("interleaved-thinking-2025-05-14")
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_provider_error_reaches_the_client_as_the_provider_wrote_it() -> Result<(), Fault> {
        let error =
            r#"{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}"#;
        let mock = MockTransport::new();
        mock.reply(
            ANTHROPIC,
            mock::json(
                http::StatusCode::from_u16(529).map_err(http::Error::from)?,
                error,
            ),
        );
        let gw = gateway(mock.at(ANTHROPIC))?;
        let (status, _, body) = read_whole(gw.handle(request()).await.0).await;
        assert_eq!((status.as_u16(), body?), (529, error.into()));
        Ok(())
    }
}
