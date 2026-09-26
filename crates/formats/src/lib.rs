#![warn(missing_docs)]
//! Wire formats. Each relays the provider's bytes as they came, except the
//! terminator and stream errors, which it rewrites, and what it asked for
//! itself, such as a usage chunk the client did not ask for, which it drops.
//! OpenAI Chat Completions and Anthropic Messages are formats of the
//! [`conversation`] kind and translate to each other through its types.

mod common;
pub mod sse;
pub mod whole;

mod anthropic;
pub mod conversation;
mod openai;

pub use anthropic::AnthropicMessages;
pub use common::{json_model, pass_credentials, pass_headers, write_model};
pub use openai::OpenAiChat;

use respan_gateway_core::bytes::{Bytes, BytesMut};
use respan_gateway_core::http::{self, HeaderValue, header::CONTENT_TYPE};
use respan_gateway_core::serde_json::{self, Value};
use respan_gateway_core::{Decoder, Encoder, Fault, Part};

/// A provider's message as JSON: a whole body, or an event's `data:`.
pub fn json(raw: impl AsRef<[u8]>) -> Option<Value> {
    let raw = raw.as_ref();
    serde_json::from_slice(raw)
        .ok()
        .or_else(|| sse::event_json(raw))
}

/// Set a made response's content type to `value`, unless it has one.
pub fn default_content_type(head: &mut http::response::Parts, value: &'static str) {
    let value = HeaderValue::from_static(value);
    head.headers.entry(CONTENT_TYPE).or_insert(value);
}

/// The decoder of a format of no kind, which the gateway only passes on.
/// A success becomes Frames as its bytes come. A failure is read whole, as
/// [`whole::decoder`] reads one, keeping the provider's words.
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

/// The encoder of a format of no kind. Frames are written as they came and
/// a relayed head is left as it is. A failure is written as its message in
/// plain text; under a relayed head it returns `Err`, so the kernel aborts
/// the body.
pub fn frames_encoder(head: &mut http::response::Parts, relayed: bool) -> Box<dyn Encoder> {
    if !relayed {
        default_content_type(head, "text/plain; charset=utf-8");
    }
    Box::new(FramesEncoder { relayed })
}

struct FramesEncoder {
    /// Whether the head is the provider's, under which plain text cannot
    /// tell a failure.
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
