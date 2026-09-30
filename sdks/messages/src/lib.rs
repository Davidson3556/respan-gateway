#![warn(missing_docs)]
//! Shared message vocabulary, independent of wire transports. Conversation
//! values retain their original bytes and decoded provenance through [`relayed`].
//! HTTP codecs and wire-format helpers live in `respan-gateway-formats`.

pub mod conversation;
pub mod relayed;
mod usage;

pub use usage::Usage;

use serde_json::Value;

/// A fault's extra fields for a client's JSON error body, as a hint
/// ([`Fault::with`](respan_gateway_core::Fault::with)). Formats write these
/// into their error bodies; the kernel never reads them.
#[derive(Clone, Debug, PartialEq)]
pub struct Details(pub Value);
