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
mod usage;

pub use anthropic::AnthropicMessages;
pub use openai::OpenAiChat;
pub use usage::Usage;

use respan_gateway_core::Fault;
use respan_gateway_core::serde_json::{self, Value};
use respan_gateway_http::bytes::{Bytes, BytesMut};
use respan_gateway_http::http::{
    self, HeaderValue, header::CONTENT_ENCODING, header::CONTENT_TYPE,
};
use respan_gateway_http::{Answer, Ask};
use respan_gateway_http::{Decoder, Encoder};

/// A provider's message as JSON: a whole body, or an event's `data:`.
pub fn json(raw: impl AsRef<[u8]>) -> Option<Value> {
    let raw = raw.as_ref();
    serde_json::from_slice(raw)
        .ok()
        .or_else(|| sse::event_json(raw))
}

/// Whether a request is a `POST` to a path ending in `suffix`: the usual
/// [`Codec::accepts`](respan_gateway_http::Codec::accepts).
pub fn posts_to(head: &Ask, suffix: &str) -> bool {
    head.method == http::Method::POST && head.uri.path().ends_with(suffix)
}

/// Set a call's path, relative to the transport, keeping its query.
pub fn set_path(head: &mut Ask, path: &str) {
    let uri = match head.uri.query() {
        Some(query) => format!("{path}?{query}"),
        None => path.to_owned(),
    };
    // A path and a query that parsed parse.
    head.uri = uri.parse().unwrap_or_default();
}

/// Mark a call whose body is not the client's bytes as sent, as when a
/// value was written or its inputs stream: its `Content-Encoding`, the
/// client's, goes.
pub fn body_changed(head: &mut Ask) {
    head.headers.remove(CONTENT_ENCODING);
}

/// A call's head: the client's for a relay, else a fresh one.
pub fn call_head(client: Option<&Ask>) -> Ask {
    client
        .cloned()
        .unwrap_or_else(respan_gateway_http::fresh_ask)
}

/// A response's head: the provider's for a response in the client's
/// format, else a fresh `200`.
pub fn answer_head(answer: Option<&Answer>) -> Answer {
    answer
        .cloned()
        .unwrap_or_else(respan_gateway_http::fresh_answer)
}

/// Set a response's content type to `value` if it has none (RFC 9110 8.3).
pub fn default_content_type(head: &mut Answer, value: &'static str) {
    let value = HeaderValue::from_static(value);
    head.headers.entry(CONTENT_TYPE).or_insert(value);
}

/// A decoder of one value per body or data frame, made or refused by
/// `on_message`: the usual
/// [`Codec::request_decoder`](respan_gateway_http::Codec::request_decoder).
pub fn message_decoder<T, F>(on_message: F) -> Box<dyn Decoder<T>>
where
    T: 'static,
    F: FnMut(Bytes) -> Result<T, Fault> + Send + 'static,
{
    Box::new(Messages(on_message))
}

struct Messages<F>(F);

/// An encoder whose head is `head`, and whose body `body` writes: the usual
/// way to give a body's encoder ([`Encoder<T, ()>`]) the head a factory
/// made.
pub fn with_head<T, H>(head: H, body: Box<dyn Encoder<T, ()>>) -> Box<dyn Encoder<T, H>>
where
    T: 'static,
    H: Send + 'static,
{
    headed(move |_: Option<&T>| head, body)
}

/// An encoder whose head `head` makes from the first value it is asked
/// with ([`Encoder::head`]), and whose body `body` writes.
pub fn headed<T, H, F>(head: F, body: Box<dyn Encoder<T, ()>>) -> Box<dyn Encoder<T, H>>
where
    T: 'static,
    H: Send + 'static,
    F: FnOnce(Option<&T>) -> H + Send + 'static,
{
    Box::new(Headed {
        head: Some(head),
        body,
    })
}

struct Headed<F, T> {
    head: Option<F>,
    body: Box<dyn Encoder<T, ()>>,
}

impl<T, H, F> Encoder<T, H> for Headed<F, T>
where
    T: 'static,
    H: Send + 'static,
    F: FnOnce(Option<&T>) -> H + Send + 'static,
{
    fn head(&mut self, first: Option<&T>) -> H {
        let head = self.head.take().expect("a head is asked once");
        head(first)
    }

    fn open(&mut self, out: &mut BytesMut) -> Result<bool, Fault> {
        self.body.open(out)
    }

    fn encode(&mut self, value: &T, out: &mut BytesMut) -> Result<(), Fault> {
        self.body.encode(value, out)
    }

    fn end(&mut self, end: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault> {
        self.body.end(end, out)
    }
}

impl<T, F: FnMut(Bytes) -> Result<T, Fault> + Send + 'static> Decoder<T> for Messages<F> {
    fn decode(&mut self, bytes: Bytes, out: &mut Vec<T>) -> Result<(), Fault> {
        out.push((self.0)(bytes)?);
        Ok(())
    }

    fn end(&mut self, _out: &mut Vec<T>) -> Result<(), Fault> {
        Ok(())
    }
}
