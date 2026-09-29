use bytes::{Bytes, BytesMut};

use crate::Fault;

/// One protocol's codec, stateless and shared: each body, either way, gets
/// its own [`Decoder`] or [`Encoder`], which keeps that stream's state. The
/// factories never fail: a failure travels in the stream.
///
/// A body is a stream of messages, each decoded into one value of the kind,
/// even one the kind cannot read, which keeps its bytes: the kernel keeps
/// none of a value's, so a relay loses nothing only if the format writes a
/// value as the bytes it came in while it says the same.
///
/// Its wire is its pair of head types, [`Format::Ask`] and
/// [`Format::Answer`], which the kernel never reads and hands only to a
/// format of the name it came in. A request's inputs stream, one per data
/// frame, if the adapter sets [`Exchange::streams`](crate::Exchange::streams)
/// on it ([`Gateway::handle`](crate::Gateway::handle)); else its body is its
/// one input, even empty.
///
/// Its kind is its pair of value types, through which formats of one kind
/// translate. They must differ, as the type names the direction, and should
/// belong to the kind alone: a shared type such as `serde_json::Value` lets
/// a plugin for one kind read another's traffic.
pub trait Format: Send + Sync + 'static {
    /// The head of a request on this format's wire.
    type Ask: Clone + Send + Sync + 'static;

    /// The head of a response on this format's wire.
    type Answer: Send + Sync + 'static;

    /// What a request carries, one or a stream.
    type Input: Clone + PartialEq + Send + Sync + 'static;

    /// What a response carries, one or a stream.
    type Output: Clone + PartialEq + Send + Sync + 'static;

    /// The protocol's identity. Formats of one name relay rather than
    /// translate, and share a kind and a wire.
    fn name(&self) -> &str;

    /// Whether a request with `head` is in this protocol; the first format
    /// served ([`Gateway::serve`](crate::Gateway::serve)) that accepts takes it.
    fn accepts(&self, head: &Self::Ask) -> bool;

    /// Decodes the client's request with `head`: its body when first read,
    /// or a streaming request's frames as they pass. `Err` with
    /// [`codes::BAD_REQUEST`](crate::codes::BAD_REQUEST) for bytes not in
    /// this protocol; what the kind has no place for stays in the input.
    fn request_decoder(&self, head: &Self::Ask) -> Box<dyn Decoder<Self::Input>>;

    /// Encodes a call. `client` is the client's head if the call relays the
    /// client's message; `body` its first bytes, the client's when relayed
    /// unread, else empty, to edit. The encoder writes the head
    /// ([`Encoder::head`]), then any inputs written from their values.
    /// `streams`: a streaming request's inputs follow, one per frame; a
    /// protocol that cannot carry them fails [`Encoder::open`] with
    /// [`codes::CONFIG`](crate::codes::CONFIG).
    ///
    /// What this protocol cannot say is an `Err` of the encode it concerns,
    /// never dropped: the kernel makes it
    /// [`codes::BAD_REQUEST`](crate::codes::BAD_REQUEST) if the input is the
    /// client's as decoded, else [`codes::CONFIG`](crate::codes::CONFIG).
    fn request_encoder(
        &self,
        client: Option<&Self::Ask>,
        body: &mut Bytes,
        streams: bool,
    ) -> Box<dyn Encoder<Self::Input, Self::Ask>>;

    /// Decodes a response with head `answer` to the call with head `call` and
    /// first bytes `sent`. A provider's error answer ends `Err` with its
    /// status as a hint ([`Fault::with`]), [`Fault::with_retry_after`] and
    /// [`Fault::with_answer`]; a body not in this protocol with
    /// [`codes::UPSTREAM`](crate::codes::UPSTREAM); one cut short with
    /// [`Fault::truncated`]. Hold an output that shows nothing yet, such as a
    /// start, until the next, so a failure first can still be refused.
    fn response_decoder(
        &self,
        answer: &Self::Answer,
        call: &Self::Ask,
        sent: &Bytes,
    ) -> Box<dyn Decoder<Self::Output>>;

    /// Encodes a response to the client's request with head `ask` and body
    /// `body` (empty if streaming). `answer` is the provider's head for a
    /// response in this format, to start the head from. A `refusal` is the
    /// response's one failure, before any byte: write its answer as it came
    /// if this format read it ([`Fault::answer`]), else the fault in this
    /// protocol's words. Each output to a streaming request is its own frame.
    ///
    /// Write an output as its bytes while it says the same, else from its
    /// value; what this protocol cannot write is an `Err`. The end always
    /// comes: write a failure in this protocol's framing.
    fn response_encoder(
        &self,
        answer: Option<&Self::Answer>,
        ask: &Self::Ask,
        body: &Bytes,
        refusal: Option<&Fault>,
    ) -> Box<dyn Encoder<Self::Output, Self::Answer>>;

    /// Whether `output` is content, what
    /// [`Response::until_content`](crate::Response::until_content) stops at;
    /// a value that says nothing yet, such as a keep-alive, is not. By
    /// default, true.
    fn is_content(&self, _output: &Self::Output) -> bool {
        true
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
    /// were cut off ([`codes::ABANDONED`](crate::codes::ABANDONED)) or an
    /// input could not be written. What it writes for one goes before the
    /// `Err` the transport's body then ends with; nothing follows that.
    fn end(&mut self, end: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault>;
}
