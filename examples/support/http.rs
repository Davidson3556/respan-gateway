//! Small HTTP helpers for runnable examples using local scripted providers.

use respan_gateway_core::Fault;
use respan_gateway_http::HttpClient;
use respan_gateway_http::http::{HeaderMap, HeaderValue};

pub fn openai(base: &str) -> Result<HttpClient, Fault> {
    let mut headers = HeaderMap::new();
    headers.insert("authorization", HeaderValue::from_static("Bearer sk-test"));
    HttpClient::new(base)?.with_headers(headers)
}

pub fn anthropic(base: &str) -> Result<HttpClient, Fault> {
    let mut headers = HeaderMap::new();
    headers.insert("x-api-key", HeaderValue::from_static("sk-ant-test"));
    HttpClient::new(base)?.with_headers(headers)
}

/// Send scripted SSE bytes as individual events through mockllm's real HTTP server.
pub fn sse(chunks: &[&str]) -> mockllm::Answer {
    let events = chunks
        .iter()
        .flat_map(|chunk| chunk.split("\n\n"))
        .filter(|event| !event.is_empty())
        .map(|event| serde_json::Value::String(event.to_owned()))
        .collect();
    mockllm::Answer::events(events)
}
