#![warn(missing_docs)]
//! Wire formats and helpers for writing one. OpenAI Chat Completions and
//! Anthropic Messages translate through the [`conversation`] kind. Every
//! value a format decodes keeps its bytes ([`relayed`]); traffic of no kind
//! relays through [`opaque`].

mod common;
pub mod opaque;
pub mod relayed;
pub mod sse;
pub mod whole;

mod anthropic;
pub mod conversation;
mod openai;

pub use anthropic::AnthropicMessages;
pub use openai::OpenAiChat;

use respan_gateway_core::bytes::Bytes;
use respan_gateway_core::http::{self, HeaderValue, header::CONTENT_TYPE};
use respan_gateway_core::serde_json::{self, Value};
use respan_gateway_core::{Decoder, Fault};

/// A provider's message as JSON: a whole body, or an event's `data:`.
pub fn json(raw: impl AsRef<[u8]>) -> Option<Value> {
    let raw = raw.as_ref();
    serde_json::from_slice(raw)
        .ok()
        .or_else(|| sse::event_json(raw))
}

/// Whether a request is a `POST` to a path ending in `suffix`: the usual
/// [`Format::accepts`](respan_gateway_core::Format::accepts).
pub fn posts_to(head: &http::request::Parts, suffix: &str) -> bool {
    head.method == http::Method::POST && head.uri.path().ends_with(suffix)
}

/// Set `request`'s path, relative to the transport, keeping its query.
pub fn set_path(request: &mut http::Request<Bytes>, path: &str) {
    let uri = match request.uri().query() {
        Some(query) => format!("{path}?{query}"),
        None => path.to_owned(),
    };
    // A path and a query that parsed parse.
    *request.uri_mut() = uri.parse().unwrap_or_default();
}

/// Set a response's content type to `value` if it has none (RFC 9110 8.3).
pub fn default_content_type(head: &mut http::response::Parts, value: &'static str) {
    let value = HeaderValue::from_static(value);
    head.headers.entry(CONTENT_TYPE).or_insert(value);
}

/// A decoder of one value per body or data frame, made or refused by
/// `on_message`: the usual
/// [`Format::request_decoder`](respan_gateway_core::Format::request_decoder).
pub fn message_decoder<T, F>(on_message: F) -> Box<dyn Decoder<T>>
where
    T: 'static,
    F: FnMut(Bytes) -> Result<T, Fault> + Send + 'static,
{
    Box::new(Messages(on_message))
}

struct Messages<F>(F);

impl<T, F: FnMut(Bytes) -> Result<T, Fault> + Send + 'static> Decoder<T> for Messages<F> {
    fn decode(&mut self, bytes: Bytes, out: &mut Vec<T>) -> Result<(), Fault> {
        out.push((self.0)(bytes)?);
        Ok(())
    }

    fn end(&mut self, _out: &mut Vec<T>) -> Result<(), Fault> {
        Ok(())
    }
}
