//! Small HTTP helpers for runnable examples using local scripted providers.

use respan_gateway_core::Fault;
use respan_gateway_http::http::{HeaderMap, HeaderValue};
use respan_gateway_http_transport::HttpTransport;

pub fn openai(base: &str) -> Result<HttpTransport, Fault> {
    let mut headers = HeaderMap::new();
    headers.insert("authorization", HeaderValue::from_static("Bearer sk-test"));
    HttpTransport::new(base)?.with_headers(headers)
}

pub fn anthropic(base: &str) -> Result<HttpTransport, Fault> {
    let mut headers = HeaderMap::new();
    headers.insert("x-api-key", HeaderValue::from_static("sk-ant-test"));
    HttpTransport::new(base)?.with_headers(headers)
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

/// Send each of `data` as one `data:` event, as a provider streams.
pub fn data_events(data: &[&str]) -> mockllm::Answer {
    let events: Vec<String> = data.iter().map(|d| format!("data: {d}\n\n")).collect();
    sse(&events.iter().map(String::as_str).collect::<Vec<_>>())
}

/// Stream `deltas` as OpenAI chat chunks of text, then `[DONE]`.
pub fn deltas(deltas: &[&str]) -> mockllm::Answer {
    let chunk = |d: &&str| {
        serde_json::json!({ "id": "c1", "model": "gpt",
            "choices": [{ "index": 0, "delta": { "content": d } }] })
        .to_string()
    };
    let chunks: Vec<String> = deltas.iter().map(chunk).collect();
    let mut data: Vec<&str> = chunks.iter().map(String::as_str).collect();
    data.push("[DONE]");
    data_events(&data)
}
