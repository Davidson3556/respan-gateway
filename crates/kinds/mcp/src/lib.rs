#![warn(missing_docs)]
//! The Model Context Protocol's kind: typed JSON-RPC messages ([`Message`])
//! carried in [`Input`] and [`Output`], which attach HTTP batch, session and
//! SSE metadata.
//!
//! JSON-RPC requests, notifications and responses are typed. HTTP batching,
//! session and protocol headers, request queries and SSE cursors live on the
//! envelopes so filters preserve their association with messages.
//!
//! The crate is the kind's types alone, with no I/O; its format, streamable
//! HTTP, and a stdio server as a transport are `respan-gateway-mcp-http`'s.

use serde_json::{Value, json};

mod envelope;
mod messages;
pub use envelope::{Input, Output};
pub use messages::{Id, Message, Outcome, RpcError};

/// The JSON-RPC error code a format writes for a fault, as a fact
/// ([`Fault::with`](respan_gateway_core::Fault::with)). A fault without one
/// is written as an invalid request (-32600) if it is a `BadRequest`, else
/// as an internal error (-32603). A server's JSON-RPC error that refused a
/// call is read back into one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RpcCode(pub i64);

/// A JSON-RPC error response to `id` (`null` if unknown).
pub fn error(id: Value, code: i64, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}
