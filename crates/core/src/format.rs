use std::any::{Any, TypeId};
use std::marker::PhantomData;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};

use crate::Fault;
use crate::transport::Head;

/// One protocol's codec: reads and writes its requests and responses, on
/// whichever side the kernel puts it. Stateless and shared: each body,
/// either way, gets its own [`Decoder`] or [`Encoder`] with that stream's
/// state.
///
/// A body is a stream of messages, each decoded into one value of the
/// format's kind: one input of a request, one output of a response. Every
/// message decodes to one value, even one the kind cannot read, which says
/// so and carries its bytes: the kernel keeps no bytes, so a relay loses
/// nothing only if the format writes a value as the bytes it came in while
/// it says the same.
///
/// Its wire is its pair of head types, [`Format::Ask`] and
/// [`Format::Answer`], such as HTTP's request and response heads, whose
/// meaning is the format's: the kernel never reads a head. It hands one,
/// like its bytes, only to a format of the name it came in, so a format
/// relays a head as it relays a body; formats of one name share a wire. A
/// request streams its inputs, one per data frame after its head, if the
/// adapter says so ([`Gateway::handle`](crate::Gateway::handle)); else its
/// body is its one input, even empty. The factories never fail: a failure
/// travels in the stream.
///
/// Its kind is its pair of types; formats of one kind translate into each
/// other through them. The two types must differ, as the type names the
/// direction, or [`Gateway::provider`](crate::Gateway::provider) and
/// [`Gateway::serve`](crate::Gateway::serve) fail. Both should belong to the
/// kind alone: a shared type such as `serde_json::Value` lets a plugin for
/// one kind read another's traffic.
pub trait Format: Send + Sync + 'static {
    /// The head of a request on this format's wire.
    type Ask: Clone + Send + Sync + 'static;

    /// The head of a response on this format's wire.
    type Answer: Send + Sync + 'static;

    /// What a request carries, one or a stream.
    type Input: Clone + PartialEq + Send + Sync + 'static;

    /// What a response carries, one or a stream.
    type Output: Clone + PartialEq + Send + Sync + 'static;

    /// The protocol's identity. Formats of one name relay to each other
    /// rather than translate, and must share a kind and a wire.
    fn name(&self) -> &str;

    /// Whether a request with `head` is in this protocol. The kernel asks
    /// the formats given to [`Gateway::serve`](crate::Gateway::serve) on
    /// the request's wire in order, and the first that accepts takes it.
    fn accepts(&self, head: &Self::Ask) -> bool;

    /// Decodes the client's request with `head`: its body once when first
    /// read, or a streaming request's data frames as they pass. `Err` with
    /// [`codes::BAD_REQUEST`](crate::codes::BAD_REQUEST) for bytes not in
    /// this protocol; note in the input what it has no place for, as a
    /// relay keeps it.
    fn request_decoder(&self, head: &Self::Ask) -> Box<dyn Decoder<Self::Input>>;

    /// Encodes a call. `client` is the client's head as the adapter handed
    /// it over, if the call relays the client's own message in this format;
    /// `body` is the call's first bytes, the client's when relayed unread,
    /// else empty, for the format to edit as its protocol needs. The
    /// encoder writes the call's head ([`Encoder::head`]), from the
    /// client's or anew, then any inputs written from their values
    /// (changed, translated or a call's own). `streams`: later inputs
    /// follow, one per data frame, from a streaming request; a protocol
    /// that cannot carry them fails [`Encoder::open`] with
    /// [`codes::CONFIG`](crate::codes::CONFIG). What a proxy must not pass
    /// on is the transport's to strip.
    ///
    /// What this protocol cannot say is an `Err` of the first encode it
    /// concerns, never dropped. The kernel makes it
    /// [`codes::BAD_REQUEST`](crate::codes::BAD_REQUEST) if the input is
    /// the client's as decoded, filters or not, else
    /// [`codes::CONFIG`](crate::codes::CONFIG).
    fn request_encoder(
        &self,
        client: Option<&Self::Ask>,
        body: &mut Bytes,
        streams: bool,
    ) -> Box<dyn Encoder<Self::Input, Self::Ask>>;

    /// Decodes a response with head `answer` to the call with head `call`
    /// and first bytes `sent`, as sent, whatever its head says. A provider's error answer ends
    /// `Err` with its status, [`Fault::with_retry_after`] and
    /// [`Fault::with_answer`], so a client of this format gets it as it
    /// came; a success not in this protocol ends `Err` with
    /// [`codes::UPSTREAM`](crate::codes::UPSTREAM), one cut short
    /// [`Fault::truncated`]. Hold an output that shows nothing yet, such as a
    /// start, until the next that does, and drop it if the response fails
    /// first, so that failure can still be refused
    /// ([`Response::until_content`](crate::Response::until_content)).
    fn response_decoder(
        &self,
        answer: &Self::Answer,
        call: &Self::Ask,
        sent: &Bytes,
    ) -> Box<dyn Decoder<Self::Output>>;

    /// Encodes a response to the client's request with head `ask` and body
    /// `body` (empty for a streaming request). `answer` is the provider's
    /// head for a response in this format, for the encoder's head
    /// ([`Encoder::head`]) to start from; else none. A `refusal` is the
    /// response's one failure, before any byte: write its answer as it came
    /// if this format read it ([`Fault::answer`]), else the fault in this
    /// protocol's words, with its status and retry. Say the content type,
    /// and what a streaming request's success needs, each of whose outputs
    /// is its own data frame.
    ///
    /// Write an output as the bytes it came in while it says the same, what
    /// changed written into them; one from elsewhere from its value. What
    /// this protocol cannot write is an `Err`. The end always comes: write a
    /// failure in this protocol's framing.
    fn response_encoder(
        &self,
        answer: Option<&Self::Answer>,
        ask: &Self::Ask,
        body: &Bytes,
        refusal: Option<&Fault>,
    ) -> Box<dyn Encoder<Self::Output, Self::Answer>>;

    /// Whether `output` is content: what
    /// [`Response::until_content`](crate::Response::until_content) commits on. A keep-alive,
    /// framing, or a message carrying only usage is not. Every output,
    /// content or not, reaches filters. By default, true.
    fn is_content(&self, _output: &Self::Output) -> bool {
        true
    }
}

/// Bytes to values of `T` for one body, either way: the state of one
/// stream, such as an SSE buffer.
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
    /// input when it is written from its value; none for a call relaying
    /// the client's bytes unread, and for a response, whose head is written
    /// before its outputs are known. A response's head goes out with its
    /// first bytes, or at once for a streaming request; until then a
    /// refusal replaces it.
    fn head(&mut self, first: Option<&T>) -> H;

    /// Write the body's opening bytes, such as an SSE comment for a stream
    /// that may be quiet; a response's head goes out with them. Not called
    /// for a refusal. `Err` if this body cannot be written at all: a call
    /// fails before it is sent; a response is refused. By default, none.
    fn open(&mut self, _out: &mut BytesMut) -> Result<(), Fault> {
        Ok(())
    }

    /// Write `value`. On `Err` its partial output is dropped and the body
    /// ends with the fault: a request's fails its call; a response's is
    /// written by [`Encoder::end`], or refused if no byte went out yet.
    fn encode(&mut self, value: &T, out: &mut BytesMut) -> Result<(), Fault>;

    /// Write how the body ends. On `Err` a response's end is written again
    /// as that failure; if that fails too, the body is aborted, or a
    /// refusal handed back to the adapter
    /// ([`Gateway::handle`](crate::Gateway::handle)).
    fn end(&mut self, end: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault>;

    /// Whether a response's body is done before it began: it has no bytes,
    /// as its head says. Its head then goes at once and nothing more is
    /// read: the response ends `Ok`; its call, its decoder told the body
    /// ended, `Ok` if that decoder says it is complete, else abandoned.
    /// Asked once, after [`Encoder::open`]. By default, false.
    fn done(&self) -> bool {
        false
    }
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
    fn open(&mut self, out: &mut BytesMut) -> Result<(), Fault>;
    /// Write each of `values` apart, handing each one's bytes to `each`,
    /// until the first failure.
    fn encode(&mut self, values: &Values, each: &mut dyn FnMut(BytesMut)) -> Result<(), Fault>;
    fn input(&mut self, input: &AnyInput, out: &mut BytesMut) -> Result<(), Fault>;
    fn end(&mut self, end: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault>;
    fn done(&self) -> bool;
}

/// A typed codec, erased.
struct Erased<C, T>(C, PhantomData<fn() -> T>);

impl<T: Send + 'static> AnyDecoder for Erased<Box<dyn Decoder<T>>, T> {
    fn decode(&mut self, bytes: Option<Bytes>) -> (Option<Values>, Option<Result<(), Fault>>) {
        let mut out = Vec::new();
        let end = match bytes {
            Some(bytes) => self.0.decode(bytes, &mut out).err().map(Err),
            None => Some(self.0.end(&mut out)),
        };
        ((!out.is_empty()).then(|| Box::new(out) as Values), end)
    }
}

impl<T: Send + 'static, H: Send + Sync + 'static> AnyEncoder for Erased<Box<dyn Encoder<T, H>>, T> {
    fn head(&mut self, first: Option<&AnyInput>) -> Head {
        let first = first.and_then(|i| i.downcast_ref::<T>());
        Box::new(self.0.head(first))
    }

    fn open(&mut self, out: &mut BytesMut) -> Result<(), Fault> {
        self.0.open(out)
    }

    fn encode(&mut self, values: &Values, each: &mut dyn FnMut(BytesMut)) -> Result<(), Fault> {
        let values = values
            .downcast_ref::<Vec<T>>()
            .ok_or_else(Fault::wrong_kind::<T>)?;
        for value in values {
            let mut one = BytesMut::new();
            self.0.encode(value, &mut one)?;
            each(one);
        }
        Ok(())
    }

    fn input(&mut self, input: &AnyInput, out: &mut BytesMut) -> Result<(), Fault> {
        let input = input
            .downcast_ref::<T>()
            .ok_or_else(Fault::wrong_kind::<T>)?;
        self.0.encode(input, out)
    }

    fn end(&mut self, end: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault> {
        self.0.end(end, out)
    }

    fn done(&self) -> bool {
        self.0.done()
    }
}

/// A codec for a head of another wire than its format's: it fails.
struct OtherWire;

fn other_wire() -> Fault {
    Fault::internal("a head of another wire than its format's")
}

impl AnyDecoder for OtherWire {
    fn decode(&mut self, _: Option<Bytes>) -> (Option<Values>, Option<Result<(), Fault>>) {
        (None, Some(Err(other_wire())))
    }
}

/// A [`Format`] with its types erased, as the gateway keeps it.
pub(crate) trait AnyFormat: Send + Sync + 'static {
    fn name(&self) -> &str;
    fn accepts(&self, head: &(dyn Any + Send + Sync)) -> bool;
    fn request_decoder(&self, head: &(dyn Any + Send + Sync)) -> Box<dyn AnyDecoder>;
    fn request_encoder(
        &self,
        client: Option<&(dyn Any + Send + Sync)>,
        body: &mut Bytes,
        streams: bool,
    ) -> Box<dyn AnyEncoder>;
    fn response_decoder(
        &self,
        answer: &(dyn Any + Send + Sync),
        call: &(dyn Any + Send + Sync),
        sent: &Bytes,
    ) -> Box<dyn AnyDecoder>;
    fn response_encoder(
        &self,
        answer: Option<&(dyn Any + Send + Sync)>,
        ask: &(dyn Any + Send + Sync),
        body: &Bytes,
        refusal: Option<&Fault>,
    ) -> Option<Box<dyn AnyEncoder>>;
    /// A copy of `head`, one of its asks.
    fn copy_ask(&self, head: &(dyn Any + Send + Sync)) -> Option<Head>;
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

    fn accepts(&self, head: &(dyn Any + Send + Sync)) -> bool {
        head.downcast_ref()
            .is_some_and(|head| Format::accepts(self, head))
    }

    fn request_decoder(&self, head: &(dyn Any + Send + Sync)) -> Box<dyn AnyDecoder> {
        match head.downcast_ref() {
            Some(head) => Box::new(Erased(Format::request_decoder(self, head), PhantomData)),
            None => Box::new(OtherWire),
        }
    }

    fn request_encoder(
        &self,
        client: Option<&(dyn Any + Send + Sync)>,
        body: &mut Bytes,
        streams: bool,
    ) -> Box<dyn AnyEncoder> {
        let client = client.and_then(|c| c.downcast_ref::<F::Ask>());
        Box::new(Erased(
            Format::request_encoder(self, client, body, streams),
            PhantomData,
        ))
    }

    fn response_decoder(
        &self,
        answer: &(dyn Any + Send + Sync),
        call: &(dyn Any + Send + Sync),
        sent: &Bytes,
    ) -> Box<dyn AnyDecoder> {
        match (answer.downcast_ref(), call.downcast_ref()) {
            (Some(answer), Some(call)) => Box::new(Erased(
                Format::response_decoder(self, answer, call, sent),
                PhantomData,
            )),
            _ => Box::new(OtherWire),
        }
    }

    fn response_encoder(
        &self,
        answer: Option<&(dyn Any + Send + Sync)>,
        ask: &(dyn Any + Send + Sync),
        body: &Bytes,
        refusal: Option<&Fault>,
    ) -> Option<Box<dyn AnyEncoder>> {
        let answer = answer.and_then(|a| a.downcast_ref::<F::Answer>());
        let encoder = Format::response_encoder(self, answer, ask.downcast_ref()?, body, refusal);
        Some(Box::new(Erased(encoder, PhantomData)))
    }

    fn copy_ask(&self, head: &(dyn Any + Send + Sync)) -> Option<Head> {
        let head = head.downcast_ref::<F::Ask>()?;
        Some(Box::new(head.clone()))
    }

    fn kind(&self) -> (TypeId, TypeId) {
        (TypeId::of::<F::Input>(), TypeId::of::<F::Output>())
    }

    fn wire(&self) -> (TypeId, TypeId) {
        (TypeId::of::<F::Ask>(), TypeId::of::<F::Answer>())
    }

    fn any_content(&self, outputs: &Values) -> bool {
        let outputs = outputs.downcast_ref::<Vec<F::Output>>();
        let mut outputs = outputs.into_iter().flatten();
        outputs.any(|output| Format::is_content(self, output))
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
