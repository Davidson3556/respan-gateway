#![warn(missing_docs)]
//! The conversation kind over HTTP: JSON requests, JSON or SSE responses.
//! A format supplies its [`Schema`], its names and its JSON both ways; the
//! HTTP around it, on either edge, is shared.
//!
//! - [`openai`]: OpenAI Chat Completions, as OpenAI serves it or in a
//!   vendor's dialect ([`openai::ChatQuirks`]).
//! - [`anthropic`]: Anthropic Messages.
//!
//! A format names no vendor but its own: a vendor's dialect is a value of
//! the format, set with the knobs it offers.

use serde_json::Value;

mod schema;
pub use schema::{Asked, Decoder, Schema, StreamResponse, merge_details, read_provider_error};

pub mod anthropic;
pub mod openai;

use anthropic::AnthropicMessages;
use openai::OpenAiChat;

/// Whether `raw`, an event of `format`, changes no answer, as that format
/// says ([`Schema::is_inert`]).
fn is_inert(format: &str, raw: &Value) -> bool {
    (format == OpenAiChat::NAME && OpenAiChat::is_inert(raw))
        || (format == AnthropicMessages::NAME && AnthropicMessages::is_inert(raw))
}
