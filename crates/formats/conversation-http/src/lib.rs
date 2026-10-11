#![warn(missing_docs)]
//! The conversation kind over HTTP: JSON requests, JSON or SSE responses.
//! A format supplies its [`Schema`], its names and its JSON both ways; the
//! HTTP around it, on either edge, is shared.
//!
//! - [`openai`]: OpenAI Chat Completions.
//! - [`anthropic`]: Anthropic Messages.
//! - [`gemini`]: Google Gemini's `generateContent`.
//!
//! A format is its spec: what a provider sends beyond it is kept native, at
//! the place it appeared, for a plugin to read (each module says where).

use serde_json::Value;

mod schema;
pub use schema::{Asked, Decoder, Schema, StreamResponse, merge_details, read_provider_error};

pub mod anthropic;
pub mod gemini;
pub mod openai;

use anthropic::AnthropicMessages;
use gemini::GeminiGenerate;
use openai::OpenAiChat;

/// Whether `raw`, an event of `format`, changes no answer, as that format
/// says ([`Schema::is_inert`]).
fn is_inert(format: &str, raw: &Value) -> bool {
    (format == OpenAiChat::NAME && OpenAiChat::is_inert(raw))
        || (format == AnthropicMessages::NAME && AnthropicMessages::is_inert(raw))
        || (format == GeminiGenerate::NAME && GeminiGenerate::is_inert(raw))
}
