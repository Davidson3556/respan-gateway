#![warn(missing_docs)]
//! The plugin-author facade: original core contracts and shared LLM vocabulary.
//! Basic text accessors operate on typed conversation content, not opaque wire JSON.
//! HTTP bindings and wire helpers live in `respan_gateway_http` and its `formats` module.
//!
//! ```
//! use respan_gateway_sdk::{
//!     ClientContext, Conversation, Fault, Filter, Plugin, Reply, Response, Usage,
//! };
//! ```

pub use respan_gateway_core::*;

pub mod conversation;
mod fault;
pub mod relayed;
mod usage;

pub use conversation::{
    Content, Conversation, Finish, Message, Piece, Reply, Role, Start, Tool, ToolChoice, set_model,
};
pub use fault::{BadRequest, ErrorCode, RetryAfter, Truncated, Upstream};
pub use usage::Usage;

use serde_json::Value;

/// A fault's extra fields for a client's JSON error body, as a hint
/// ([`Fault::with`](respan_gateway_core::Fault::with)). Formats write these
/// into their error bodies; the kernel never reads them.
#[derive(Clone, Debug, PartialEq)]
pub struct Details(pub Value);
