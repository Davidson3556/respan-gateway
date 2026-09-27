#![warn(missing_docs)]
//! Wire formats, and helpers for writing one. A format relays the
//! provider's bytes as they came, except that it rewrites the terminator and
//! stream errors and drops what it asked for itself, such as a usage chunk
//! the client did not request. OpenAI Chat Completions and Anthropic
//! Messages are formats of the [`conversation`] kind and translate to each
//! other through its types.

mod common;
pub mod sse;
pub mod whole;

mod anthropic;
pub mod conversation;
mod openai;

pub use anthropic::AnthropicMessages;
pub use common::{json_model, pass_headers, write_model};
pub use openai::OpenAiChat;

use respan_gateway_core::bytes::{Bytes, BytesMut};
use respan_gateway_core::http::{self, HeaderValue, header::CONTENT_TYPE};
use respan_gateway_core::serde_json::{self, Value};
use respan_gateway_core::{Decoder, Encoder, Fault, Part};

/// A provider's message as JSON: a whole body, or an event's `data:`.
/// `None` if neither parses.
pub fn json(raw: impl AsRef<[u8]>) -> Option<Value> {
    let raw = raw.as_ref();
    serde_json::from_slice(raw)
        .ok()
        .or_else(|| sse::event_json(raw))
}

/// Whether a client request is a `POST` to a path ending in `suffix`, such
/// as `/chat/completions`, whatever its query. The usual
/// [`Format::accepts`](respan_gateway_core::Format::accepts).
pub fn posts_to(head: &http::request::Parts, suffix: &str) -> bool {
    head.method == http::Method::POST && head.uri.path().ends_with(suffix)
}

/// Set a made response's content type to `value`, unless it has one.
pub fn default_content_type(head: &mut http::response::Parts, value: &'static str) {
    let value = HeaderValue::from_static(value);
    head.headers.entry(CONTENT_TYPE).or_insert(value);
}

/// The decoder of a format of no kind. A success becomes Frames as its
/// bytes come; a failure is read whole by [`whole::decoder`], keeping the
/// provider's message.
pub fn frames_decoder(head: &http::response::Parts) -> Box<dyn Decoder> {
    match head.status.is_success() {
        false => whole::decoder(head, |_, _| Ok(())),
        true => Box::new(FramesDecoder),
    }
}

struct FramesDecoder;

impl Decoder for FramesDecoder {
    fn decode(&mut self, bytes: Bytes, out: &mut Vec<Part>) {
        out.push(Part::Frame(bytes));
    }

    fn finish(&mut self, out: &mut Vec<Part>) {
        out.push(Part::End(Ok(())));
    }
}

/// The encoder of a format of no kind. Frames are written as they came.
/// Unless `relayed`, the head defaults to plain text and a failure is
/// written as its message; under a relayed head a failure returns `Err`, so
/// the kernel aborts the body.
pub fn frames_encoder(head: &mut http::response::Parts, relayed: bool) -> Box<dyn Encoder> {
    if !relayed {
        default_content_type(head, "text/plain; charset=utf-8");
    }
    Box::new(FramesEncoder { relayed })
}

struct FramesEncoder {
    /// Whether the head is the provider's, so a failure cannot be text.
    relayed: bool,
}

impl Encoder for FramesEncoder {
    fn encode(&mut self, part: &Part, out: &mut BytesMut) -> Result<(), Fault> {
        match part {
            Part::Frame(b) => out.extend_from_slice(b),
            Part::End(Err(f)) if self.relayed => return Err(f.clone()),
            Part::End(Err(f)) => out.extend_from_slice(f.message().as_bytes()),
            Part::Usage(_) | Part::End(Ok(())) => {}
            _ => return Err(common::unknown_part()),
        }
        Ok(())
    }
}
