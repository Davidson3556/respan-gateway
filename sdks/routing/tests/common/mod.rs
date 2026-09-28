//! What the SDK's tests share: a slow transport, a call log and requests.

#![allow(dead_code)]

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use respan_gateway_core::bytes::Bytes;
use respan_gateway_core::futures::{StreamExt, stream};
use respan_gateway_core::http::{self, HeaderMap, HeaderValue, StatusCode};
use respan_gateway_core::http_body_util::{BodyExt, BodyStream, StreamBody};
use respan_gateway_core::serde_json::{self, Value, json};
use respan_gateway_core::{
    Body, ClientContext, Fault, Gateway, Outcome, Plugin, ProviderContext, Transport, async_trait,
};
use respan_gateway_formats::conversation::units;
use respan_gateway_testkit::{MockAt, post, read_whole};

pub const OPENAI: &str = "http://openai.test/v1";
pub const BACKUP: &str = "http://backup.test/v1";
pub const TEXT_ONLY: &str = "http://text-only.test/v1";
pub const ANTHROPIC: &str = "http://anthropic.test";
pub const CHAT: &str = "/v1/chat/completions";
pub const MESSAGES: &str = "/v1/messages";

/// A whole OpenAI answer saying `text`, with usage 5 in, 2 out.
pub fn openai_json(text: &str) -> String {
    json!({"id": "c1", "object": "chat.completion", "model": "gpt-x",
        "choices": [{"index": 0, "finish_reason": "stop",
            "message": {"role": "assistant", "content": text}}],
        "usage": {"prompt_tokens": 5, "completion_tokens": 2, "total_tokens": 7}})
    .to_string()
}

/// A whole Anthropic answer saying `text`, with usage 5 in, 2 out.
pub fn anthropic_json(text: &str) -> String {
    json!({"id": "msg_1", "type": "message", "role": "assistant", "model": "claude-x",
        "content": [{"type": "text", "text": text}], "stop_reason": "end_turn",
        "stop_sequence": null, "usage": {"input_tokens": 5, "output_tokens": 2}})
    .to_string()
}

/// An OpenAI stream saying `words`, then usage and `[DONE]`.
pub fn openai_stream(words: &[&str]) -> Vec<String> {
    let chunk = |delta: Value, finish: Value| {
        let c = json!({"id": "c1", "object": "chat.completion.chunk", "model": "gpt-x",
            "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]});
        format!("data: {c}\n\n")
    };
    let mut out = vec![chunk(
        json!({"role": "assistant", "content": ""}),
        Value::Null,
    )];
    out.extend(
        words
            .iter()
            .map(|w| chunk(json!({"content": w}), Value::Null)),
    );
    out.push(chunk(json!({}), "stop".into()));
    let usage = json!({"id": "c1", "object": "chat.completion.chunk", "model": "gpt-x",
        "choices": [], "usage": {"prompt_tokens": 5, "completion_tokens": 2, "total_tokens": 7}});
    out.push(format!("data: {usage}\n\n"));
    out.push("data: [DONE]\n\n".into());
    out
}

/// An Anthropic stream that opens, then fails with `overloaded_error`
/// before any content: what Anthropic sends under load, with a 200.
pub fn anthropic_overloaded_stream() -> Vec<String> {
    vec![
        "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-x\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null,\"usage\":{\"input_tokens\":5,\"output_tokens\":1}}}\n\n".into(),
        "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n".into(),
    ]
}

/// A 200 event stream of `chunks`.
pub fn sse(chunks: &[String]) -> respan_gateway_testkit::mock::Reply {
    let refs: Vec<&str> = chunks.iter().map(String::as_str).collect();
    respan_gateway_testkit::mock::sse(&refs)
}

/// A JSON reply with `status`.
pub fn json_reply(status: StatusCode, body: &str) -> respan_gateway_testkit::mock::Reply {
    respan_gateway_testkit::mock::json(status, body)
}

/// A JSON reply with `status` and `Retry-After: secs`.
pub fn retry_after(status: StatusCode, secs: u64) -> respan_gateway_testkit::mock::Reply {
    let mut r = json_reply(
        status,
        r#"{"error":{"type":"rate_limit_error","message":"slow down"}}"#,
    );
    r.headers_mut().insert(
        "retry-after",
        HeaderValue::from_str(&secs.to_string()).expect("digits"),
    );
    r
}

/// An OpenAI chat request for `model`, streamed or not, saying `text`.
pub fn chat(model: &str, stream: bool, text: &str) -> http::Request<Body> {
    chat_with(json!({"model": model, "stream": stream,
        "messages": [{"role": "user", "content": text}]}))
}

/// An OpenAI chat request with this body.
pub fn chat_with(body: Value) -> http::Request<Body> {
    post(CHAT, body.to_string())
}

/// An Anthropic Messages request saying `text`.
pub fn messages(model: &str, stream: bool, text: &str) -> http::Request<Body> {
    let body = json!({"model": model, "max_tokens": 50, "stream": stream,
        "messages": [{"role": "user", "content": text}]});
    post(MESSAGES, body.to_string())
}

/// Send, read the whole response, then wait for the end hooks.
pub async fn send(
    gw: &Gateway,
    req: http::Request<Body>,
) -> (StatusCode, HeaderMap, Result<String, Fault>) {
    let (status, headers, body) = read_whole(gw.handle(req).await.0).await;
    gw.idle().await;
    let body = body.map(|b| String::from_utf8_lossy(&b).into_owned());
    (status, headers, body)
}

/// The JSON a mock provider was sent.
pub fn sent_json(req: &http::Request<Bytes>) -> Value {
    serde_json::from_slice(req.body()).unwrap_or_default()
}

/// Each `data:` of an SSE body, as JSON where it parses.
pub fn data(body: &str) -> Vec<Value> {
    body.split("\n\n")
        .filter_map(|e| e.lines().find_map(|l| l.strip_prefix("data:")))
        .map(|d| serde_json::from_str(d.trim()).unwrap_or(Value::String(d.trim().into())))
        .collect()
}

/// The text an OpenAI stream or whole answer says.
pub fn openai_text(body: &str) -> String {
    if let Ok(v) = serde_json::from_str::<Value>(body) {
        return v["choices"][0]["message"]["content"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
    }
    data(body)
        .iter()
        .filter_map(|d| {
            d["choices"][0]["delta"]["content"]
                .as_str()
                .map(str::to_owned)
        })
        .collect()
}

/// What one edge's end hook saw.
#[derive(Debug, Clone, PartialEq)]
pub struct Seen {
    /// `client`, or the provider called.
    pub edge: String,
    pub status: Option<u16>,
    /// `ok`, or the fault's code.
    pub end: String,
    pub output_tokens: f64,
}

/// Records every end hook, both edges, in the order they ran.
#[derive(Debug, Clone, Default)]
pub struct Log(pub Arc<Mutex<Vec<Seen>>>);

impl Log {
    pub fn all(&self) -> Vec<Seen> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub fn calls(&self) -> Vec<Seen> {
        self.all()
            .into_iter()
            .filter(|s| s.edge != "client")
            .collect()
    }

    pub fn client(&self) -> Seen {
        let all = self.all();
        let client: Vec<_> = all.into_iter().filter(|s| s.edge == "client").collect();
        assert_eq!(client.len(), 1, "{client:?}");
        client[0].clone()
    }

    fn push(&self, edge: &str, o: &Outcome) {
        let seen = Seen {
            edge: edge.to_owned(),
            status: o.head.as_ref().map(|h| h.status.as_u16()),
            end: match &o.end {
                Ok(()) => "ok".into(),
                Err(f) => f.code().to_owned(),
            },
            output_tokens: o.usage.get(units::OUTPUT_TOKENS).unwrap_or(0.0),
        };
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(seen);
    }
}

#[async_trait]
impl Plugin for Log {
    async fn on_client_end(&self, _cx: &ClientContext, o: &Outcome) {
        self.push("client", o);
    }

    async fn on_provider_end(&self, px: &ProviderContext, o: &Outcome) {
        self.push(px.provider(), o);
    }
}

/// A transport that waits `head` before answering, and `gap` before each
/// body chunk after the first `fast` chunks.
#[derive(Debug, Clone)]
pub struct Slow {
    pub inner: MockAt,
    pub head: Duration,
    pub gap: Duration,
    pub fast: usize,
}

impl Slow {
    pub fn head(inner: MockAt, head: Duration) -> Self {
        Self {
            inner,
            head,
            gap: Duration::ZERO,
            fast: 0,
        }
    }

    /// Answer at once, but wait `gap` before every chunk.
    pub fn first_byte(inner: MockAt, gap: Duration) -> Self {
        Self {
            inner,
            head: Duration::ZERO,
            gap,
            fast: 0,
        }
    }
}

#[async_trait]
impl Transport for Slow {
    async fn send(&self, call: http::Request<Body>) -> Result<http::Response<Body>, Fault> {
        tokio::time::sleep(self.head).await;
        let (head, body) = self.inner.send(call).await?.into_parts();
        let (gap, fast) = (self.gap, self.fast);
        let frames = BodyStream::new(body)
            .enumerate()
            .then(move |(i, f)| async move {
                if i >= fast {
                    tokio::time::sleep(gap).await;
                }
                f
            });
        Ok(http::Response::from_parts(
            head,
            StreamBody::new(frames).boxed_unsync(),
        ))
    }
}

/// A transport whose body sends `first` chunks, then goes silent forever.
#[derive(Debug, Clone)]
pub struct Stalls {
    pub inner: MockAt,
}

#[async_trait]
impl Transport for Stalls {
    async fn send(&self, call: http::Request<Body>) -> Result<http::Response<Body>, Fault> {
        let (head, body) = self.inner.send(call).await?.into_parts();
        let frames = BodyStream::new(body).chain(stream::pending());
        Ok(http::Response::from_parts(
            head,
            StreamBody::new(frames).boxed_unsync(),
        ))
    }
}
