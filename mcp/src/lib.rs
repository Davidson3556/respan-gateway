#![warn(missing_docs)]
//! The Model Context Protocol: its kind ([`ToServer`] in, [`ToClient`]
//! out), MCP over streamable HTTP as a format ([`StreamableHttp`]), and an
//! MCP server run as a child process ([`StdioTransport`]).
//!
//! Messages are JSON-RPC 2.0, kept as JSON so every field MCP adds
//! survives. What MCP carries in HTTP headers (the session, the protocol
//! version, a resumed stream's last event) is part of the kind too, so a
//! call carries it with no relay, and so is the request's query. Output is always written from the parsed
//! messages, in the framing the client's request asks for.

use std::sync::{Mutex, MutexGuard, PoisonError};

use respan_gateway_http::http::Method;
use serde_json::{Value, json};

mod session;
mod stdio;
mod streamable;

pub use stdio::{Owner, StdioTransport};
pub use streamable::{SESSION, StreamableHttp};

/// The kind's input: one HTTP request from an MCP client.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ToServer {
    /// `POST` sends messages, `GET` opens the server's stream, `DELETE`
    /// ends the session.
    pub method: Method,
    /// A `POST`'s messages, in order; none for `GET` or `DELETE`.
    pub messages: Vec<Value>,
    /// Whether they came as a JSON array (a batch).
    pub batch: bool,
    /// The session it belongs to ([`SESSION`]), none before the server
    /// assigned one.
    pub session: Option<String>,
    /// The protocol version the client and server agreed on
    /// (`MCP-Protocol-Version`), which MCP has a client send on every
    /// request after `initialize`.
    pub protocol_version: Option<String>,
    /// The SSE `id:` of the last event the client got (`Last-Event-ID`),
    /// on a `GET` that resumes a stream.
    pub last_event_id: Option<String>,
    /// The request's query as the client sent it, without the credential
    /// parameters the HTTP adapter lifted out, sent to the server as it
    /// stands.
    pub query: Option<String>,
}

impl ToServer {
    /// A request of `method` with `messages`, a batch if not exactly one,
    /// in no session.
    pub fn new(method: Method, messages: Vec<Value>) -> Self {
        Self {
            batch: method == Method::POST && messages.len() != 1,
            method,
            messages,
            ..Self::default()
        }
    }

    /// The ids of its requests, which the server must answer.
    pub fn request_ids(&self) -> impl Iterator<Item = &Value> {
        request_ids(&self.messages)
    }
}

/// The kind's output: one message of the server's, a JSON body or an
/// event. An event the kind cannot read, such as a comment, has none.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ToClient {
    /// Its JSON-RPC messages.
    pub messages: Vec<Value>,
    /// The session the server's answer named ([`SESSION`]), as on the
    /// answer to `initialize`, which assigns it. The client's format writes
    /// the first output's into the head of a `POST`'s answer.
    pub session: Option<String>,
    /// The SSE `id:` of its event, which a client resumes from
    /// (`Last-Event-ID`); written only when the client's framing is an event
    /// stream, and ignored in a JSON body.
    pub event_id: Option<String>,
    /// The SSE `event:` name of its event, written like `event_id`.
    pub event: Option<String>,
}

impl ToClient {
    /// A message of `messages`, in no session.
    pub fn new(messages: Vec<Value>) -> Self {
        Self {
            messages,
            ..Self::default()
        }
    }

    /// The ids of the responses in it.
    pub(crate) fn answers(&self) -> impl Iterator<Item = &Value> {
        let answers = self.messages.iter().filter(|m| m.get("method").is_none());
        answers.filter_map(|m| m.get("id").filter(|id| !id.is_null()))
    }
}

/// The JSON-RPC error code a format writes for a fault, as a fact
/// ([`Fault::with`](respan_gateway_sdk::Fault::with)). A fault without one
/// is written as an invalid request (-32600) if it is a `BadRequest`, else
/// as an internal error (-32603). A server's JSON-RPC error that refused a
/// call is read back into one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RpcCode(pub i64);

/// JSON-RPC 2.0 (5.1): "Invalid JSON was received by the server".
pub(crate) const PARSE_ERROR: RpcCode = RpcCode(-32700);

/// A JSON-RPC error response to `id` (`null` if unknown).
pub fn error(id: Value, code: i64, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The ids of the requests in `messages`.
fn request_ids(messages: &[Value]) -> impl Iterator<Item = &Value> {
    let requests = messages.iter().filter(|m| m.get("method").is_some());
    requests.filter_map(|m| m.get("id").filter(|id| !id.is_null()))
}

/// Messages as JSON: an array if `batch` or not exactly one, else the one
/// message.
fn body(messages: &[Value], batch: bool) -> Value {
    match (messages, batch) {
        ([one], false) => one.clone(),
        _ => Value::Array(messages.to_vec()),
    }
}

/// The messages of a JSON value, an object or an array of them, and
/// whether it was an array.
fn messages(value: Value) -> Option<(Vec<Value>, bool)> {
    match value {
        Value::Array(all) if all.iter().all(Value::is_object) => Some((all, true)),
        one @ Value::Object(_) => Some((vec![one], false)),
        _ => None,
    }
}

/// The messages in `raw`, JSON (never an SSE event), as [`messages`].
fn read(raw: &[u8]) -> Option<(Vec<Value>, bool)> {
    messages(serde_json::from_slice(raw).ok()?)
}
