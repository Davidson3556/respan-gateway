//! Use case 12, a local proxy in front of Claude Code: no config file, one
//! Anthropic provider, and the client's own credentials (API key or
//! subscription token) passed through, since the transport sets no headers
//! of its own. Beta headers, unknown request fields and unknown response
//! events relay byte for byte: formats of one name relay, never re-encode.
//! The binary does the same with no config file, and also relays the
//! vendor's other endpoints (`count_tokens`): `respan-gateway proxy
//! --anthropic`.
//!
//! `cargo run -p respan-gateway-examples --example local_proxy` runs offline
//! against a mock; add `-- --serve 127.0.0.1:8080` to listen for real, then
//! `ANTHROPIC_BASE_URL=http://127.0.0.1:8080 claude`.

use respan_gateway_core::{Fault, Gateway};
use respan_gateway_formats::conversation::Conversation;
use respan_gateway_http::http::{self, HeaderValue};
use respan_gateway_providers::AnthropicMessages;

use respan_gateway_axum::router;
use respan_gateway_http::HttpTransport;
use respan_gateway_http::Transport;
use respan_gateway_testkit::{MockTransport, mock, post, read_whole, serve};

const ANTHROPIC: &str = "https://api.anthropic.com";

/// The whole setup, in code: serve Anthropic Messages, relay to Anthropic.
fn gateway(transport: impl Into<Transport>) -> Result<Gateway, Fault> {
    Gateway::new()
        .provider::<Conversation, _, _>(
            "anthropic",
            AnthropicMessages,
            respan_gateway_http::connect(transport),
        )?
        .client::<Conversation, _, _>(AnthropicMessages)?
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
    let (status, _, body) = read_whole(serve(&gw, request()).await.0).await;
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

fn request() -> http::Request<respan_gateway_http::Body> {
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
#[path = "tests/local_proxy.rs"]
mod tests;
