#![warn(missing_docs)]
//! First-party conversation codecs. Shared message types live in
//! [`respan_gateway_sdk`]; reusable wire helpers in [`respan_gateway_http::formats`].
//!
//! Each codec decodes a request into a [`conversation::Conversation`], keeping the fields
//! the kind has no place for in its `extra`, and writes a request from the
//! value alone. A response, streamed or whole, decodes into the events of
//! [`conversation::Reply`] and is encoded from them, so what survives a relay is what the
//! events and `extra` say, not the provider's bytes; the codec modules
//! document their mappings. Only an explicit raw relay
//! ([`respan_gateway_http::relay::Relay::RAW`]) sends the client's own bytes.
//!
//! What a format has no form for stays in its own terms (an `Extra`, a
//! `Content::Other`, a `Reply::Other`) and is written only by that format;
//! translating it is refused where it could change the answer. A request
//! that asks for several choices (`n > 1`) cannot be written and is refused.

mod anthropic;
mod common;
mod errors;
mod openai;
#[cfg(test)]
mod round_trips;
mod tools;

pub use anthropic::AnthropicMessages;
pub use openai::OpenAiChat;

use respan_gateway_http::formats;
use respan_gateway_sdk::{Usage, conversation};
