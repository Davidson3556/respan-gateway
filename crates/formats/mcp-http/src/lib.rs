#![warn(missing_docs)]
//! The Model Context Protocol over HTTP: streamable HTTP as a format
//! ([`StreamableHttp`]) and an MCP server run as a child process
//! ([`StdioTransport`]), of the kind's `Input` and `Output`. Output is
//! encoded in the framing requested by the client.

use std::sync::{Mutex, MutexGuard, PoisonError};

use respan_gateway_mcp::{Message, RpcCode};
use serde_json::Value;

mod session;
mod stdio;
mod streamable;

pub use stdio::{Owner, StdioTransport};
pub use streamable::{SESSION, StreamableHttp};

/// JSON-RPC 2.0 (5.1): "Invalid JSON was received by the server".
const PARSE_ERROR: RpcCode = RpcCode(-32700);

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Messages as JSON: an array if `batch` or not exactly one, else the one
/// message.
fn body(messages: impl IntoIterator<Item: Into<Value>>, batch: bool) -> Value {
    let mut all: Vec<Value> = messages.into_iter().map(Into::into).collect();
    match (all.len(), batch) {
        (1, false) => all.remove(0),
        _ => Value::Array(all),
    }
}

/// The messages of a JSON value, an object or an array of them, and
/// whether it was an array. Each is a valid JSON-RPC envelope, so an id
/// alone, which is no response, never settles an outstanding request.
fn messages(value: Value) -> Option<(Vec<Message>, bool)> {
    let (all, batch) = match value {
        Value::Array(all) if !all.is_empty() => (all, true),
        one => (vec![one], false),
    };
    let all: Vec<Message> = all
        .into_iter()
        .map(Message::try_from)
        .collect::<Result<_, _>>()
        .ok()?;
    // Keep numeric and string IDs distinct while rejecting ambiguous
    // batches; a response cannot identify two requests at once.
    let mut ids = std::collections::HashSet::new();
    let unique = all
        .iter()
        .filter_map(Message::request_id)
        .all(|id| ids.insert(id.to_string()));
    unique.then_some((all, batch))
}

/// The messages in `raw`, JSON (never an SSE event), as [`messages`].
fn read(raw: &[u8]) -> Option<(Vec<Message>, bool)> {
    messages(serde_json::from_slice(raw).ok()?)
}
