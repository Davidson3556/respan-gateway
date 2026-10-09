#![warn(missing_docs)]
//! The Model Context Protocol: streamable HTTP as a format ([`StreamableHttp`]) and an
//! MCP server run as a child process ([`StdioTransport`]).
//! Typed messages live in `respan_gateway_mcp_messages`; [`HttpInput`] and
//! [`HttpOutput`] attach HTTP batch, session and SSE metadata.
//!
//! JSON-RPC requests, notifications and responses are typed. HTTP batching,
//! session and protocol headers, request queries and SSE cursors live on the
//! adapter envelopes so filters preserve their association with messages.
//! Output is encoded in the framing requested by the client.

use std::sync::{Mutex, MutexGuard, PoisonError};

mod envelope;
pub use envelope::{HttpInput, HttpOutput};
use envelope::{HttpInput as Input, HttpOutput as Output};
use serde_json::{Value, json};

mod session;
mod stdio;
mod streamable;

pub use stdio::{Owner, StdioTransport};
pub use streamable::{SESSION, StreamableHttp};

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
        Value::Array(all) if !all.is_empty() && all.iter().all(valid_message) => {
            // Keep numeric and string IDs distinct while rejecting ambiguous
            // batches; a response cannot identify two requests at once.
            let mut ids = std::collections::HashSet::new();
            let unique = request_ids(&all).all(|id| ids.insert(id.to_string()));
            unique.then_some((all, true))
        }
        one @ Value::Object(_) if valid_message(&one) => Some((vec![one], false)),
        _ => None,
    }
}

/// Validate the JSON-RPC envelope without constraining MCP's extension fields.
/// An id alone is not a response and must never settle an outstanding request.
fn valid_message(message: &Value) -> bool {
    respan_gateway_mcp_messages::Message::try_from(message.clone()).is_ok()
}

/// The messages in `raw`, JSON (never an SSE event), as [`messages`].
fn read(raw: &[u8]) -> Option<(Vec<Value>, bool)> {
    messages(serde_json::from_slice(raw).ok()?)
}
