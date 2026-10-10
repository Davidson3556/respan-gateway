//! Use case 12, a local proxy in front of Claude Code: no config file, one
//! Anthropic provider, and the client's own credentials (API key or
//! subscription token) passed through, since the transport sets no headers
//! of its own. Beta headers and the request body relay byte for byte (an
//! explicit `Relay::RAW`); a response is decoded into events and written
//! again, so an unknown response event survives, not its bytes.
//! The binary does the same with no config file, and also relays the
//! vendor's other endpoints (`count_tokens`): `respan-gateway proxy
//! --anthropic`.
//!
//! `cargo run -p respan-gateway-examples --example local_proxy` runs offline
//! against a one-response fixture; add `-- --serve 127.0.0.1:8080` to listen for real, then
//! `ANTHROPIC_BASE_URL=http://127.0.0.1:8080 claude`. Set
//! `RESPAN_ANTHROPIC_UPSTREAM=http://127.0.0.1:PORT` to use a local provider
//! instead of the default Anthropic upstream.

use respan_gateway_chat::anthropic::AnthropicMessages;
use respan_gateway_conversation::Input;
use respan_gateway_core::{Fault, Gateway, futures::TryStreamExt};
use respan_gateway_examples::LIMIT;
use respan_gateway_http::Body;
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::http::{self, HeaderMap, HeaderValue, StatusCode};
use std::sync::{Arc, Mutex, PoisonError};

use respan_gateway_axum::router;
use respan_gateway_http::HttpClient;
#[allow(dead_code)]
#[path = "../../tests/support/http.rs"]
mod http_support;
use http_support::{post, read_whole, serve};

const ANTHROPIC: &str = "https://api.anthropic.com";

type Sent = Arc<Mutex<Vec<(http::Uri, HeaderMap, Bytes)>>>;

/// A single offline response for the runnable demonstration and byte-exact tests.
/// The production --serve path uses HttpClient instead.
fn fixture(
    status: StatusCode,
    body: impl Into<Bytes>,
    sent: Sent,
) -> impl Fn(respan_gateway_core::Extensions) -> respan_gateway_http::Outgoing + Send + Sync + 'static
{
    let body = body.into();
    respan_gateway_http::connect(
        move |mut call| {
            let (body, sent) = (body.clone(), Arc::clone(&sent));
            async move {
                respan_gateway_http::credentials(&mut call, &HeaderMap::new());
                let (head, body_in) = call.into_parts();
                let chunks: Vec<Bytes> = body_in.try_collect().await?;
                let mut bytes = Vec::new();
                for chunk in chunks {
                    bytes.extend_from_slice(&chunk);
                }
                sent.lock().unwrap_or_else(PoisonError::into_inner).push((
                    head.uri,
                    head.headers,
                    Bytes::from(bytes),
                ));
                let content_type = if status.is_success() {
                    "text/event-stream"
                } else {
                    "application/json"
                };
                let response = http::Response::builder()
                    .status(status)
                    .header("content-type", content_type)
                    .body(Body::full(body))
                    .map_err(|e| Fault::internal(e.to_string()))?;
                Ok((response, None))
            }
        },
        LIMIT,
    )
}

/// The whole setup, in code: serve Anthropic Messages, relay to Anthropic.
fn gateway(
    transport: impl Fn(respan_gateway_core::Extensions) -> respan_gateway_http::Outgoing
    + Send
    + Sync
    + 'static,
) -> Result<Gateway, Fault> {
    Gateway::new()
        .provider::<Input, _, _>("anthropic", AnthropicMessages, transport)?
        .client::<Input, _, _>(AnthropicMessages)?
        .target("anthropic")
        .map(|g| g.plugin(Relay::RAW))
}

fn serve_address(args: &[String]) -> Result<Option<&str>, &'static str> {
    match args {
        [] => Ok(None),
        [flag, address] if flag == "--serve" && !address.is_empty() => Ok(Some(address)),
        _ => Err("usage: local_proxy [--serve ADDRESS]"),
    }
}

fn upstream_gateway(upstream: &str) -> Result<Gateway, Fault> {
    // No `with_headers`: each request carries the developer's own key.
    gateway(HttpClient::new(upstream)?.connect())
}

async fn serve_upstream(
    listener: tokio::net::TcpListener,
    gw: Gateway,
) -> Result<(), std::io::Error> {
    axum::serve(listener, router(gw)).await
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if let Some(addr) = serve_address(&args)
        .map_err(|message| std::io::Error::new(std::io::ErrorKind::InvalidInput, message))?
    {
        let upstream =
            std::env::var("RESPAN_ANTHROPIC_UPSTREAM").unwrap_or_else(|_| ANTHROPIC.to_owned());
        let gw = upstream_gateway(&upstream)?;
        let listener = tokio::net::TcpListener::bind(addr).await?;
        let bound = listener.local_addr()?;
        println!("listening on {bound}; ANTHROPIC_BASE_URL=http://{bound}");
        serve_upstream(listener, gw).await?;
        return Ok(());
    }
    let sent = Sent::default();
    let gw = gateway(fixture(StatusCode::OK, STREAM.concat(), sent.clone()))?;
    let (status, _, body) = read_whole(serve(&gw, request()).await.0).await;
    let requests = sent.lock().unwrap_or_else(PoisonError::into_inner);
    let (uri, headers, _) = &requests[0];
    println!("provider got: {uri} {headers:?}");
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
#[path = "../tests/cases/hosting/local_proxy.rs"]
mod tests;

use respan_gateway_http::relay::Relay;
