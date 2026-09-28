//! What the SDK's tests share: a gateway on mock providers, stream
//! builders, and readers of what the client got.

#![allow(dead_code)]

use std::sync::Arc;

use respan_gateway_core::bytes::Bytes;
use respan_gateway_core::http::{self, HeaderMap, StatusCode};
use respan_gateway_core::serde_json::{self, Value, json};
use respan_gateway_core::{Body, Fault, Gateway, Outcome, Plugin};
use respan_gateway_formats::{AnthropicMessages, OpenAiChat};
use respan_gateway_testkit::{MockTransport, Outcomes, mock, post, read_whole};
use respan_safety::mcp::McpLite;

pub const OPENAI: &str = "http://openai.test/v1";
pub const BACKUP: &str = "http://backup.test/v1";
pub const JUDGE: &str = "http://judge.test/v1";
pub const ANTHROPIC: &str = "http://anthropic.test";
pub const MCP: &str = "http://mcp.test";

pub const CHAT: &str = "/v1/chat/completions";
pub const MESSAGES: &str = "/v1/messages";
pub const TOOLS: &str = "/mcp";

/// A gateway serving OpenAI, Anthropic and MCP-like clients, with
/// providers `openai`, `backup`, `judge` (OpenAI), `anthropic` and `mcp`,
/// target `openai`, and `plugins` registered in order after an outcome
/// recorder.
pub struct Harness {
    pub gw: Gateway,
    pub mock: MockTransport,
    pub log: Outcomes,
}

pub fn providers(gw: Gateway, mock: &MockTransport) -> Result<Gateway, Fault> {
    gw.provider("openai", OpenAiChat, mock.at(OPENAI))?
        .provider("backup", OpenAiChat, mock.at(BACKUP))?
        .provider("judge", OpenAiChat, mock.at(JUDGE))?
        .provider("anthropic", AnthropicMessages, mock.at(ANTHROPIC))?
        .provider("mcp", McpLite, mock.at(MCP))
}

impl Harness {
    pub fn new(plugins: Vec<(&str, Arc<dyn Plugin>)>) -> Self {
        Self::build(plugins, |gw| gw.target("openai"))
    }

    pub fn build(
        plugins: Vec<(&str, Arc<dyn Plugin>)>,
        finish: impl FnOnce(Gateway) -> Result<Gateway, Fault>,
    ) -> Self {
        let mock = MockTransport::new();
        let log = Outcomes::default();
        let gw = providers(Gateway::new(), &mock)
            .and_then(|gw| gw.serve(OpenAiChat))
            .and_then(|gw| gw.serve(AnthropicMessages))
            .and_then(|gw| gw.serve(McpLite))
            .and_then(|gw| gw.plugin("outcomes", log.clone()));
        let gw = plugins
            .into_iter()
            .fold(gw, |gw, (name, p)| gw.and_then(|gw| gw.plugin(name, p)));
        let gw = gw.and_then(finish).expect("gateway builds");
        Self { gw, mock, log }
    }

    /// Send, read the whole response, and wait for the end hooks.
    pub async fn send(
        &self,
        req: http::Request<Body>,
    ) -> (StatusCode, HeaderMap, Result<Bytes, Fault>, Outcome) {
        let (resp, ended) = self.gw.handle(req).await;
        let (status, headers, body) = read_whole(resp).await;
        let outcome = ended.await;
        self.gw.idle().await;
        (status, headers, body, outcome)
    }

    /// Like `send`, for a body that must not be aborted: status and text.
    pub async fn text(&self, req: http::Request<Body>) -> (StatusCode, String, Outcome) {
        let (status, _, body, outcome) = self.send(req).await;
        let body = body.unwrap_or_else(|f| panic!("aborted: {f}"));
        (status, String::from_utf8_lossy(&body).into_owned(), outcome)
    }

    /// The bodies sent to the provider at `base`, as JSON.
    pub fn sent_to(&self, base: &str) -> Vec<Value> {
        self.mock
            .sent()
            .into_iter()
            .filter(|r| r.uri().to_string().starts_with(base))
            .map(|r| serde_json::from_slice(r.body()).unwrap_or(Value::Null))
            .collect()
    }

    /// Every request sent to the provider at `base`.
    pub fn requests_to(&self, base: &str) -> Vec<http::Request<Bytes>> {
        self.mock
            .sent()
            .into_iter()
            .filter(|r| r.uri().to_string().starts_with(base))
            .collect()
    }

    pub fn reply(&self, base: &str, reply: mock::Reply) -> &Self {
        self.mock.reply(base, reply);
        self
    }
}

pub fn openai_request(text: &str, stream: bool) -> http::Request<Body> {
    let body = json!({
        "model": "gpt-x",
        "stream": stream,
        "messages": [{ "role": "user", "content": text }],
    });
    post(CHAT, body.to_string())
}

pub fn anthropic_request(text: &str, stream: bool) -> http::Request<Body> {
    let body = json!({
        "model": "claude-x",
        "max_tokens": 100,
        "stream": stream,
        "messages": [{ "role": "user", "content": text }],
    });
    post(MESSAGES, body.to_string())
}

pub fn mcp_request(tool: &str, arguments: Value) -> http::Request<Body> {
    let body = json!({
        "jsonrpc": "2.0", "id": 7, "method": "tools/call",
        "params": { "name": tool, "arguments": arguments },
    });
    post(TOOLS, body.to_string())
}

/// An OpenAI stream chunk of `content`.
pub fn oa_delta(content: &str) -> String {
    let v = json!({ "id": "c1", "object": "chat.completion.chunk", "model": "gpt-x",
        "choices": [{ "index": 0, "delta": { "content": content } }] });
    format!("data: {v}\n\n")
}

pub fn oa_start() -> String {
    let v = json!({ "id": "c1", "object": "chat.completion.chunk", "model": "gpt-x",
        "choices": [{ "index": 0, "delta": { "role": "assistant", "content": "" } }] });
    format!("data: {v}\n\n")
}

pub fn oa_finish() -> String {
    let v = json!({ "id": "c1", "object": "chat.completion.chunk", "model": "gpt-x",
        "choices": [{ "index": 0, "delta": {}, "finish_reason": "stop" }] });
    format!("data: {v}\n\n")
}

pub const OA_DONE: &str = "data: [DONE]\n\n";

/// A whole OpenAI stream of these text deltas.
pub fn oa_stream(deltas: &[&str]) -> mock::Reply {
    let mut chunks = vec![oa_start()];
    chunks.extend(deltas.iter().map(|d| oa_delta(d)));
    chunks.push(oa_finish());
    chunks.push(OA_DONE.to_owned());
    let refs: Vec<&str> = chunks.iter().map(String::as_str).collect();
    mock::sse(&refs)
}

/// An OpenAI stream of these deltas, then the transport breaks off.
pub fn oa_broken(deltas: &[&str]) -> mock::Reply {
    let mut reply = oa_stream(deltas);
    let chunks = reply.body_mut();
    chunks.truncate(1 + deltas.len());
    chunks.push(Err(Fault::upstream("connection reset")));
    reply
}

/// An OpenAI stream of these deltas, then the provider's own error event.
pub fn oa_error_event(deltas: &[&str]) -> mock::Reply {
    let mut chunks = vec![oa_start()];
    chunks.extend(deltas.iter().map(|d| oa_delta(d)));
    chunks.push(
        "data: {\"error\":{\"message\":\"overloaded\",\"type\":\"server_error\",\"param\":null,\"code\":null}}\n\n"
            .to_owned(),
    );
    let refs: Vec<&str> = chunks.iter().map(String::as_str).collect();
    mock::sse(&refs)
}

pub fn oa_whole(content: &str) -> mock::Reply {
    let v = json!({ "id": "c1", "object": "chat.completion", "model": "gpt-x",
        "choices": [{ "index": 0, "finish_reason": "stop",
            "message": { "role": "assistant", "content": content } }],
        "usage": { "prompt_tokens": 3, "completion_tokens": 2, "total_tokens": 5 } });
    mock::json(StatusCode::OK, &v.to_string())
}

fn an_event(name: &str, v: Value) -> String {
    format!("event: {name}\ndata: {v}\n\n")
}

/// A whole Anthropic stream of these text deltas.
pub fn an_stream(deltas: &[&str]) -> mock::Reply {
    let mut chunks = vec![
        an_event(
            "message_start",
            json!({ "type": "message_start", "message": {
            "id": "msg_1", "type": "message", "role": "assistant", "model": "claude-x",
            "content": [], "stop_reason": null, "stop_sequence": null,
            "usage": { "input_tokens": 5, "output_tokens": 1 } } }),
        ),
        an_event(
            "content_block_start",
            json!({ "type": "content_block_start", "index": 0,
            "content_block": { "type": "text", "text": "" } }),
        ),
    ];
    for d in deltas {
        chunks.push(an_event(
            "content_block_delta",
            json!({ "type": "content_block_delta",
            "index": 0, "delta": { "type": "text_delta", "text": d } }),
        ));
    }
    chunks.push(an_event(
        "content_block_stop",
        json!({ "type": "content_block_stop", "index": 0 }),
    ));
    chunks.push(an_event(
        "message_delta",
        json!({ "type": "message_delta",
        "delta": { "stop_reason": "end_turn", "stop_sequence": null },
        "usage": { "output_tokens": 2 } }),
    ));
    chunks.push(an_event("message_stop", json!({ "type": "message_stop" })));
    let refs: Vec<&str> = chunks.iter().map(String::as_str).collect();
    mock::sse(&refs)
}

pub fn mcp_result(result: Value) -> mock::Reply {
    let v = json!({ "jsonrpc": "2.0", "id": 7, "result": result });
    mock::json(StatusCode::OK, &v.to_string())
}

/// What an SSE body said, event by event: text, or an error.
#[derive(Debug, Clone, PartialEq)]
pub enum Said {
    Text(String),
    Error(Value),
}

/// Every `data:` event of an OpenAI or Anthropic SSE body, as text deltas
/// and errors, in order.
pub fn said(body: &str) -> Vec<Said> {
    let mut out = Vec::new();
    for block in body.split("\n\n") {
        let Some(data) = block.lines().find_map(|l| l.strip_prefix("data: ")) else {
            continue;
        };
        let Ok(v) = serde_json::from_str::<Value>(data) else {
            continue;
        };
        if v.get("error").is_some() {
            out.push(Said::Error(v["error"].clone()));
        } else if let Some(t) = v
            .pointer("/choices/0/delta/content")
            .and_then(Value::as_str)
        {
            if !t.is_empty() {
                out.push(Said::Text(t.to_owned()));
            }
        } else if let Some(t) = v.pointer("/delta/text").and_then(Value::as_str) {
            out.push(Said::Text(t.to_owned()));
        } else if let Some(t) = v
            .pointer("/content_block/text")
            .and_then(Value::as_str)
            .filter(|t| !t.is_empty())
        {
            out.push(Said::Text(t.to_owned()));
        }
    }
    out
}

/// All the text an SSE body said.
pub fn text_of(body: &str) -> String {
    said(body)
        .into_iter()
        .filter_map(|s| match s {
            Said::Text(t) => Some(t),
            Said::Error(_) => None,
        })
        .collect()
}

/// The error an SSE body ended with, if any.
pub fn error_of(body: &str) -> Option<Value> {
    said(body).into_iter().find_map(|s| match s {
        Said::Error(e) => Some(e),
        Said::Text(_) => None,
    })
}

/// The text of every message sent in an OpenAI or Anthropic body.
pub fn prompt_of(body: &Value) -> String {
    body.to_string()
}

pub fn plugin(p: impl Plugin) -> Arc<dyn Plugin> {
    Arc::new(p)
}
