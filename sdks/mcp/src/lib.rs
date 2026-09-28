#![warn(missing_docs)]
//! The Model Context Protocol: its kind ([`ToServer`] in, [`ToClient`]
//! out), MCP over streamable HTTP as a format ([`StreamableHttp`]), and an
//! MCP server run as a child process ([`StdioTransport`]).
//!
//! Messages are JSON-RPC 2.0, kept as JSON so every field MCP adds
//! survives, and each value keeps the bytes it came in, so an unedited
//! message relays byte for byte.

use std::sync::{Mutex, MutexGuard, PoisonError};

use respan_gateway_core::serde_json::{Value, json};
use respan_gateway_formats::relayed::Relayed;
use respan_gateway_http::http::Method;

mod session;
mod stdio;
mod streamable;

pub use stdio::{BUSY, NO_SESSION, Owner, StdioTransport, TOO_MANY_SESSIONS};
pub use streamable::{SESSION, StreamableHttp};

/// The code of the fault that ends a `POST` whose requests the server did
/// not answer in time ([`StdioTransport::timeout`]).
pub const TIMED_OUT: &str = "timed_out";

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
    origin: Relayed<ToServer>,
}

impl ToServer {
    /// A request of `method` with `messages`, a batch if not exactly one.
    pub fn new(method: Method, messages: Vec<Value>) -> Self {
        Self {
            batch: method == Method::POST && messages.len() != 1,
            method,
            messages,
            origin: Relayed::default(),
        }
    }

    /// The ids of its requests, which the server must answer.
    pub fn requests(&self) -> impl Iterator<Item = &Value> {
        requests(&self.messages)
    }
}

/// The kind's output: one message of the server's, a JSON body or an
/// event. An event the kind cannot read, such as a comment, has none.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ToClient {
    /// Its JSON-RPC messages.
    pub messages: Vec<Value>,
    /// Whether they came as a JSON array (a batch).
    pub batch: bool,
    origin: Relayed<ToClient>,
}

impl ToClient {
    /// A message of `messages`, a batch if not exactly one.
    pub fn new(messages: Vec<Value>) -> Self {
        Self {
            batch: messages.len() != 1,
            messages,
            origin: Relayed::default(),
        }
    }

    /// The ids of the responses in it.
    pub(crate) fn answers(&self) -> impl Iterator<Item = &Value> {
        let answers = self.messages.iter().filter(|m| m.get("method").is_none());
        answers.filter_map(|m| m.get("id").filter(|id| !id.is_null()))
    }
}

/// A JSON-RPC error response to `id` (`null` if unknown).
pub fn error(id: Value, code: i64, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Drops from `pending` the requests `outputs` answer.
fn forget_answered(pending: &mut Vec<Value>, outputs: &[ToClient]) {
    for id in outputs.iter().flat_map(ToClient::answers) {
        pending.retain(|p| p != id);
    }
}

/// The ids of the requests in `messages`.
fn requests(messages: &[Value]) -> impl Iterator<Item = &Value> {
    let requests = messages.iter().filter(|m| m.get("method").is_some());
    requests.filter_map(|m| m.get("id").filter(|id| !id.is_null()))
}

/// Messages as JSON: an array if `batch`, else the one message.
fn body(messages: &[Value], batch: bool) -> Value {
    match (messages, batch) {
        ([one], false) => one.clone(),
        _ => Value::Array(messages.to_vec()),
    }
}

/// The messages in `raw`, an object or an array of them, and whether it
/// was an array.
fn read(raw: &[u8]) -> Option<(Vec<Value>, bool)> {
    match respan_gateway_formats::json(raw)? {
        Value::Array(all) if all.iter().all(Value::is_object) => Some((all, true)),
        one @ Value::Object(_) => Some((vec![one], false)),
        _ => None,
    }
}
