//! One protocol's bytes on HTTP: a [`Codec`], and the [`Decoder`]s and
//! [`Encoder`]s it makes for each body.

use bytes::{Bytes, BytesMut};
use http::HeaderValue;
use http::header::CONTENT_TYPE;
use respan_gateway_core::Fault;

use crate::{RequestHead, ResponseHead};

/// One protocol's codec on HTTP, stateless and shared: each body, either
/// way, gets its own [`Decoder`] or [`Encoder`], which keeps that stream's
/// state. The factories never fail: a failure travels in the stream.
/// [`impl_format!`](crate::impl_format) makes it a
/// [`Format`](respan_gateway_core::Format), which HTTP's driver runs.
///
/// A body is a stream of messages, each decoded into one value of the kind,
/// even one the kind cannot read, which keeps its bytes: the gateway keeps
/// none of a value's, so a relay loses nothing only if the codec writes a
/// value as the bytes it came in while it says the same.
///
/// A request's inputs stream, one per data frame, if its adapter says it
/// streams ([`Transport::accept`](crate::Transport::accept)); else its body is its
/// one input, even empty. Heads reach only a codec of the name they came
/// in: the client's to a call that relays the client's message, the
/// provider's to the answer to such a call.
///
/// Its kind is its pair of value types, through which codecs of one kind
/// translate. They must differ, as the type names the direction, and should
/// belong to the kind alone: a shared type such as `serde_json::Value` lets
/// a plugin for one kind read another's traffic.
pub trait Codec: Send + Sync + 'static {
    /// What a request carries, one or a stream.
    type Input: Clone + Send + Sync + 'static;

    /// What a response carries, one or a stream.
    type Output: Clone + Send + Sync + 'static;

    /// The protocol's identity. Codecs of one name relay rather than
    /// translate, and share a kind.
    fn name(&self) -> &str;

    /// Whether a request with `head` is in this protocol; the first served
    /// that accepts takes it.
    fn accepts(&self, head: &RequestHead) -> bool;

    /// Decodes the client's request with `head`: its body when first read,
    /// or a streaming request's frames as they pass. `Err` with
    /// [`codes::BAD_REQUEST`](respan_gateway_core::codes::BAD_REQUEST) for
    /// bytes not in this protocol; what the kind has no place for stays in
    /// the input.
    fn request_decoder(&self, head: &RequestHead) -> Box<dyn Decoder<Self::Input>>;

    /// Encodes a call. `client` is the client's head if the call relays the
    /// client's message. `body` is its first bytes, the client's when
    /// relayed unread, else empty; a codec edits them in place, since a relay
    /// unread has no other place to add what the protocol needs to them. The
    /// encoder writes the head ([`Encoder::head`]), then any inputs written
    /// from their values. `streams`: a streaming request's inputs follow,
    /// one per frame, which the head may need to say; a protocol that cannot
    /// carry them fails [`Encoder::open`] with
    /// [`codes::CONFIG`](respan_gateway_core::codes::CONFIG).
    ///
    /// What this protocol cannot say is an `Err` of the encode it concerns,
    /// never dropped, with whose fault it is:
    /// [`codes::BAD_REQUEST`](respan_gateway_core::codes::BAD_REQUEST) if
    /// the input says what the client's bytes said, else
    /// [`codes::CONFIG`](respan_gateway_core::codes::CONFIG), a plugin's
    /// ([`Format::bind`](respan_gateway_core::Format::bind)).
    fn request_encoder(
        &self,
        client: Option<&RequestHead>,
        body: &mut Bytes,
        streams: bool,
    ) -> Box<dyn Encoder<Self::Input, RequestHead>>;

    /// Decodes a response with head `answer` to the call with head `call`
    /// and first bytes `sent`, which a protocol that answers each message
    /// (MCP) reads to know which it is answering. A provider's error answer ends `Err` with its
    /// status as a hint ([`Status`](crate::Status)),
    /// [`Fault::with_retry_after`] and [`Fault::with_answer`]; a body not in
    /// this protocol with
    /// [`codes::UPSTREAM`](respan_gateway_core::codes::UPSTREAM); one cut
    /// short with [`Fault::truncated`]. Hold an output that shows nothing
    /// yet, such as a start, until the next, so a failure first can still
    /// be refused.
    fn response_decoder(
        &self,
        answer: &ResponseHead,
        call: &RequestHead,
        sent: &Bytes,
    ) -> Box<dyn Decoder<Self::Output>>;

    /// Encodes a response to the client's request with head `ask` and body
    /// `body` (empty if streaming). `answer` is the provider's head on a
    /// relay, to start the head from. Each output to a streaming request is
    /// its own frame.
    ///
    /// Write an output as its bytes while it says the same, else from its
    /// value; what this protocol cannot write is an `Err`. The end always
    /// comes: write a failure in this protocol's framing.
    fn response_encoder(
        &self,
        answer: Option<&ResponseHead>,
        ask: &RequestHead,
        body: &Bytes,
    ) -> Box<dyn Encoder<Self::Output, ResponseHead>>;

    /// The words of a refusal, the response's one failure before any byte:
    /// `fault` in this protocol, as the body it returns, with any head
    /// fields of its own set in `head`, which has the fault's status and
    /// `Retry-After` already ([`error_head`](crate::error_head)). `body` is
    /// the client's request body (empty if streaming). The driver writes
    /// the provider's answer as it came instead, if this codec read it
    /// ([`Fault::answer`], [`answered`](crate::answered)), and no body where
    /// HTTP has none. By default, the message as plain text.
    fn refusal(&self, fault: &Fault, _body: &Bytes, head: &mut ResponseHead) -> Bytes {
        let text = HeaderValue::from_static("text/plain; charset=utf-8");
        head.headers.insert(CONTENT_TYPE, text);
        Bytes::from(fault.message().to_owned())
    }
}

/// Bytes to values of `T` for one body, keeping its state, such as an SSE
/// buffer.
pub trait Decoder<T>: Send + 'static {
    /// Decode what is complete into `out`, one value per message, and keep
    /// the rest. `Err` ends the body after what `out` got.
    fn decode(&mut self, bytes: Bytes, out: &mut Vec<T>) -> Result<(), Fault>;

    /// The body's bytes ended: push what is left, and return
    /// `Err(Fault::truncated())` if it had not said it was complete.
    fn end(&mut self, out: &mut Vec<T>) -> Result<(), Fault>;
}

/// Values of `T` to a head of type `H` and bytes, for one body, either way.
pub trait Encoder<T, H>: Send + 'static {
    /// The head, asked once, before any bytes. `first` is a call's first
    /// input when written from its value; none for a response.
    fn head(&mut self, first: Option<&T>) -> H;

    /// Write the body's opening bytes, such as an SSE comment for a stream
    /// that may be quiet, which take a response's head out with them; not
    /// for a refusal. `Ok(true)` if the body is done before it began, and
    /// nothing more follows. `Err` if this body cannot be written: a call
    /// fails, a response is refused.
    ///
    /// The same on either side. The opening bytes come first: on a call,
    /// before the client's bytes or the input written, whatever the call
    /// relays. Done, on a response, its head goes at once and nothing more
    /// is read, its call ending `Ok` if its decoder, told the body ended,
    /// says it is complete, else abandoned. Done, on a call, its body is the
    /// opening bytes alone: neither the client's bytes, nor an input, nor
    /// the end is written, and a streaming request's later inputs are not
    /// read.
    fn open(&mut self, _out: &mut BytesMut) -> Result<bool, Fault> {
        Ok(false)
    }

    /// Write `value`. On `Err` its partial output is dropped and the body
    /// ends with the fault.
    fn encode(&mut self, value: &T, out: &mut BytesMut) -> Result<(), Fault>;

    /// Write how the body ends. On `Err` it is written again as that
    /// failure; if that fails too, the body is aborted.
    ///
    /// A call's request body ends with the failure it has, and `end` is
    /// told it, never `Ok` after one: the client's body broke, the inputs
    /// were cut off
    /// ([`codes::ABANDONED`](respan_gateway_core::codes::ABANDONED)) or an
    /// input could not be written. What it writes for one goes before the
    /// `Err` the transport's body then ends with; nothing follows that.
    fn end(&mut self, end: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault>;
}
