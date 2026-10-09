#![warn(missing_docs)]
//! First-party conversation codecs. Shared message types live in
//! [`respan_gateway_conversation`]; reusable wire helpers in [`respan_gateway_http::formats`].
//!
//! Each codec decodes a request into a [`conversation::Input`], keeping the fields
//! the kind has no place for in its `native`, and writes a request from the
//! value alone. A response, streamed or whole, decodes into the events of
//! [`conversation::Output`] and is encoded from them, so what survives a relay is what the
//! events and `native` say, not the provider's bytes; the codec modules
//! document their mappings. Only an explicit raw relay
//! ([`respan_gateway_http::relay::Relay::RAW`]) sends the client's own bytes.
//!
//! What a format has no form for stays in its own terms (optional `NativeFields`, a
//! `Content::Native`, a `Output::Native`) and is written only by that format;
//! translating it is refused where it could change the answer. A request
//! that asks for several choices (`n > 1`) cannot be written and is refused.

mod anthropic;
mod common;
mod errors;
mod media;
mod openai;
mod options;
#[cfg(test)]
mod round_trips;
mod tools;

pub use anthropic::AnthropicMessages;
pub use openai::OpenAiChat;

use respan_gateway_conversation as conversation;
use respan_gateway_http::formats;
use respan_gateway_sdk::Usage;

/// Provider HTTP delivery preference, set in call extensions.
/// Without an override the HTTP binding follows the original client request,
/// or requests a stream when there is no original HTTP request.
/// Standalone semantic request encoding produces a finite request.
/// Client response framing remains determined by the client request.
#[derive(Debug, Clone, Copy)]
pub struct StreamResponse(pub bool);
