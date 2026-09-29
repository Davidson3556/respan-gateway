use std::any::{Any, TypeId};
use std::sync::Arc;

use bytes::{Bytes, BytesMut};

use crate::Fault;
use crate::transport::Head;

/// One protocol's codec, stateless and shared: each body, either way, gets
/// its own [`Decoder`] or [`Encoder`], which keeps that stream's state. The
/// factories never fail: a failure travels in the stream.
///
/// A body is a stream of messages, each decoded into one value of the kind,
/// even one the kind cannot read, which keeps its bytes: the kernel keeps
/// none, so a relay loses nothing only if the format writes a value as the
/// bytes it came in while it says the same.
///
/// Its wire is its pair of head types, [`Format::Ask`] and
/// [`Format::Answer`], which the kernel never reads and hands only to a
/// format of the name it came in. A request's inputs stream, one per data
/// frame, if the adapter says so ([`Gateway::handle`](crate::Gateway::handle));
/// else its body is its one input, even empty.
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
    /// status, [`Fault::with_retry_after`] and [`Fault::with_answer`]; a body
    /// not in this protocol with [`codes::UPSTREAM`](crate::codes::UPSTREAM);
    /// one cut short with [`Fault::truncated`]. Hold an output that shows
    /// nothing yet, such as a start, until the next, so a failure first can
    /// still be refused.
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
    /// a keep-alive or usage alone is not. By default, true.
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

/// A head shared, such as the client's.
pub(crate) type SharedHead = Arc<dyn Any + Send + Sync>;

/// An input, erased and shared: copied when changed while shared.
pub(crate) type AnyInput = Arc<dyn Any + Send + Sync>;

/// Values of one kind's type, erased: a non-empty `Vec<T>`.
pub(crate) type Values = Box<dyn Any + Send>;

/// A [`Decoder`] with its values erased.
pub(crate) trait AnyDecoder: Send {
    /// The values `bytes` decode to, or those left at the end (`None`), and
    /// the end if it came.
    fn decode(&mut self, bytes: Option<Bytes>) -> (Option<Values>, Option<Result<(), Fault>>);
}

/// An [`Encoder`] with its values and head erased.
pub(crate) trait AnyEncoder: Send {
    /// The head, with the first input if it is written from its value.
    fn head(&mut self, first: Option<&AnyInput>) -> Head;
    fn open(&mut self, out: &mut BytesMut) -> Result<bool, Fault>;
    /// Write each of `values` apart, handing each one's bytes to `each`,
    /// until the first failure.
    fn encode(&mut self, values: &Values, each: &mut dyn FnMut(BytesMut)) -> Result<(), Fault>;
    fn input(&mut self, input: &AnyInput, out: &mut BytesMut) -> Result<(), Fault>;
    fn end(&mut self, end: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault>;
}

impl<T: Send + 'static> AnyDecoder for Box<dyn Decoder<T>> {
    fn decode(&mut self, bytes: Option<Bytes>) -> (Option<Values>, Option<Result<(), Fault>>) {
        let mut out = Vec::new();
        let end = match bytes {
            Some(bytes) => Decoder::decode(&mut **self, bytes, &mut out).err().map(Err),
            None => Some(Decoder::end(&mut **self, &mut out)),
        };
        ((!out.is_empty()).then(|| Box::new(out) as Values), end)
    }
}

impl<T: Send + 'static, H: Send + Sync + 'static> AnyEncoder for Box<dyn Encoder<T, H>> {
    fn head(&mut self, first: Option<&AnyInput>) -> Head {
        Box::new(Encoder::head(
            &mut **self,
            first.and_then(|i| i.downcast_ref()),
        ))
    }

    fn open(&mut self, out: &mut BytesMut) -> Result<bool, Fault> {
        Encoder::open(&mut **self, out)
    }

    fn encode(&mut self, values: &Values, each: &mut dyn FnMut(BytesMut)) -> Result<(), Fault> {
        let values = values.downcast_ref::<Vec<T>>();
        for value in values.ok_or_else(Fault::wrong_kind::<T>)? {
            let mut one = BytesMut::new();
            Encoder::encode(&mut **self, value, &mut one)?;
            each(one);
        }
        Ok(())
    }

    fn input(&mut self, input: &AnyInput, out: &mut BytesMut) -> Result<(), Fault> {
        let input = input.downcast_ref().ok_or_else(Fault::wrong_kind::<T>)?;
        Encoder::encode(&mut **self, input, out)
    }

    fn end(&mut self, end: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault> {
        Encoder::end(&mut **self, end, out)
    }
}

/// A decoder for a head of another wire than its format's: it fails.
struct OtherWire;

impl AnyDecoder for OtherWire {
    fn decode(&mut self, _: Option<Bytes>) -> (Option<Values>, Option<Result<(), Fault>>) {
        let fault = Fault::internal("a head of another wire than its format's");
        (None, Some(Err(fault)))
    }
}

/// A [`Format`] with its types erased, as the gateway keeps it. A head of
/// another wire gets a failing decoder, or no encoder.
pub(crate) trait AnyFormat: Send + Sync + 'static {
    fn name(&self) -> &str;
    fn accepts(&self, head: &dyn Any) -> bool;
    fn request_decoder(&self, head: &dyn Any) -> Box<dyn AnyDecoder>;
    fn request_encoder(
        &self,
        client: Option<&dyn Any>,
        body: &mut Bytes,
        streams: bool,
    ) -> Box<dyn AnyEncoder>;
    fn response_decoder(
        &self,
        answer: &dyn Any,
        call: &dyn Any,
        sent: &Bytes,
    ) -> Box<dyn AnyDecoder>;
    fn response_encoder(
        &self,
        answer: Option<&dyn Any>,
        ask: &dyn Any,
        body: &Bytes,
        refusal: Option<&Fault>,
    ) -> Option<Box<dyn AnyEncoder>>;
    /// A copy of `head`, one of its asks.
    fn copy_ask(&self, head: &dyn Any) -> Option<Head>;
    /// Its input and output types.
    fn kind(&self) -> (TypeId, TypeId);
    /// Its ask and answer types.
    fn wire(&self) -> (TypeId, TypeId);
    /// Whether any of `outputs` is content.
    fn any_content(&self, outputs: &Values) -> bool;
    /// Whether two inputs are equal.
    fn same(&self, a: &AnyInput, b: &AnyInput) -> bool;
    /// Inputs as values, each moved if not shared.
    fn values(&self, inputs: Vec<AnyInput>) -> Values;
    /// Values of inputs, each shared.
    fn inputs(&self, values: Values) -> Vec<AnyInput>;
}

impl<F: Format> AnyFormat for F {
    fn name(&self) -> &str {
        Format::name(self)
    }

    fn accepts(&self, head: &dyn Any) -> bool {
        head.downcast_ref()
            .is_some_and(|head| Format::accepts(self, head))
    }

    fn request_decoder(&self, head: &dyn Any) -> Box<dyn AnyDecoder> {
        match head.downcast_ref() {
            Some(head) => Box::new(Format::request_decoder(self, head)),
            None => Box::new(OtherWire),
        }
    }

    fn request_encoder(
        &self,
        client: Option<&dyn Any>,
        body: &mut Bytes,
        streams: bool,
    ) -> Box<dyn AnyEncoder> {
        let client = client.and_then(|c| c.downcast_ref());
        Box::new(Format::request_encoder(self, client, body, streams))
    }

    fn response_decoder(
        &self,
        answer: &dyn Any,
        call: &dyn Any,
        sent: &Bytes,
    ) -> Box<dyn AnyDecoder> {
        match (answer.downcast_ref(), call.downcast_ref()) {
            (Some(answer), Some(call)) => {
                Box::new(Format::response_decoder(self, answer, call, sent))
            }
            _ => Box::new(OtherWire),
        }
    }

    fn response_encoder(
        &self,
        answer: Option<&dyn Any>,
        ask: &dyn Any,
        body: &Bytes,
        refusal: Option<&Fault>,
    ) -> Option<Box<dyn AnyEncoder>> {
        let answer = answer.and_then(|a| a.downcast_ref());
        let encoder = Format::response_encoder(self, answer, ask.downcast_ref()?, body, refusal);
        Some(Box::new(encoder))
    }

    fn copy_ask(&self, head: &dyn Any) -> Option<Head> {
        Some(Box::new(head.downcast_ref::<F::Ask>()?.clone()))
    }

    fn kind(&self) -> (TypeId, TypeId) {
        (TypeId::of::<F::Input>(), TypeId::of::<F::Output>())
    }

    fn wire(&self) -> (TypeId, TypeId) {
        (TypeId::of::<F::Ask>(), TypeId::of::<F::Answer>())
    }

    fn any_content(&self, outputs: &Values) -> bool {
        let outputs = outputs.downcast_ref::<Vec<F::Output>>();
        outputs
            .into_iter()
            .flatten()
            .any(|output| Format::is_content(self, output))
    }

    fn same(&self, a: &AnyInput, b: &AnyInput) -> bool {
        let (a, b) = (a.downcast_ref::<F::Input>(), b.downcast_ref::<F::Input>());
        a.is_some() && a == b
    }

    fn values(&self, inputs: Vec<AnyInput>) -> Values {
        let owned = |input: AnyInput| match input.downcast::<F::Input>() {
            Ok(input) => Arc::try_unwrap(input).unwrap_or_else(|shared| (*shared).clone()),
            Err(_) => unreachable!("inputs are checked to be of their format's kind"),
        };
        Box::new(inputs.into_iter().map(owned).collect::<Vec<_>>())
    }

    fn inputs(&self, values: Values) -> Vec<AnyInput> {
        let inputs = values.downcast::<Vec<F::Input>>().map(|v| *v);
        let inputs = inputs.unwrap_or_default().into_iter();
        inputs.map(|input| Arc::new(input) as AnyInput).collect()
    }
}

/// Whether `T` is `format`'s input type (`true`) or output type (`false`);
/// `Err` for another.
pub(crate) fn is_input<T: Any>(format: &dyn AnyFormat) -> Result<bool, Fault> {
    match format.kind() {
        (input, _) if input == TypeId::of::<T>() => Ok(true),
        (_, output) if output == TypeId::of::<T>() => Ok(false),
        _ => Err(Fault::wrong_kind::<T>()),
    }
}
