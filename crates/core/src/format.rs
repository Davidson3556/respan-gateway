use std::any::{Any, TypeId};
use std::marker::PhantomData;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};

use crate::Fault;

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
/// Heads are an envelope the kernel carries and never reads: `http` types
/// whose meaning, status and headers alike, is the format's. The kernel
/// hands a head, like its bytes, only to a format of the name it came in,
/// so a format relays a head as it relays a body. A request streams its
/// inputs, one per data frame after its head, if the adapter says so
/// ([`Gateway::handle`](crate::Gateway::handle)); else its body is its one
/// input, even empty. A codec sees the request of the exchange as
/// `http::Request<Bytes>`, its body empty for a streaming request. The
/// factories never fail: a failure travels in the stream.
///
/// Its kind is its pair of types; formats of one kind translate into each
/// other through them. The two types must differ, as the type names the
/// direction, or [`Gateway::provider`](crate::Gateway::provider) and
/// [`Gateway::serve`](crate::Gateway::serve) fail. Both should belong to the
/// kind alone: a shared type such as `serde_json::Value` lets a plugin for
/// one kind read another's traffic.
pub trait Format: Send + Sync + 'static {
    /// What a request carries, one or a stream.
    type Input: Clone + PartialEq + Send + Sync + 'static;

    /// What a response carries, one or a stream.
    type Output: Clone + PartialEq + Send + Sync + 'static;

    /// The protocol's identity. Formats of one name relay to each other
    /// rather than translate, and must share a kind.
    fn name(&self) -> &str;

    /// Whether a request with `head` is in this protocol. The kernel asks
    /// the formats given to [`Gateway::serve`](crate::Gateway::serve) in
    /// order, and the first that accepts takes the request.
    fn accepts(&self, head: &http::request::Parts) -> bool;

    /// Decodes the client's `request`: its body once when first read, or a
    /// streaming request's data frames as they pass. `Err` with
    /// [`codes::BAD_REQUEST`](crate::codes::BAD_REQUEST) for bytes not in
    /// this protocol; note in the input what it has no place for, as a
    /// relay keeps it.
    fn request_decoder(&self, request: &http::Request<Bytes>) -> Box<dyn Decoder<Self::Input>>;

    /// Encodes a call, having edited `request`, the call's head and first
    /// bytes. For the client's own message in this format, `request` is
    /// the client's as the adapter handed it over, head and all, its body
    /// the client's bytes when unread; else it is empty. Its extensions are
    /// the call's either way. Edit it as the protocol needs, such as to ask
    /// a stream for usage; what a proxy must not pass on is the
    /// transport's to strip. `first` is the first input when it is written
    /// from its value (changed, translated or a call's own); the encoder
    /// then gets it and any later inputs. `streams`: later inputs follow,
    /// one per data frame, from a streaming request; a protocol that cannot
    /// carry them fails [`Encoder::open`] with
    /// [`codes::CONFIG`](crate::codes::CONFIG).
    ///
    /// What this protocol cannot say is an `Err` of the first encode it
    /// concerns, never dropped. The kernel makes it
    /// [`codes::BAD_REQUEST`](crate::codes::BAD_REQUEST) if the input is
    /// the client's as decoded, filters or not, else
    /// [`codes::CONFIG`](crate::codes::CONFIG).
    fn request_encoder(
        &self,
        request: &mut http::Request<Bytes>,
        first: Option<&Self::Input>,
        streams: bool,
    ) -> Box<dyn Encoder<Self::Input>>;

    /// Decodes a response with head `response` to `request`, the call as
    /// sent, whatever its head says. A provider's error answer ends `Err`
    /// with its status, [`Fault::with_retry_after`] and
    /// [`Fault::with_answer`], so a client of this format gets it as it
    /// came; a success not in this protocol ends `Err` with
    /// [`codes::UPSTREAM`](crate::codes::UPSTREAM), one cut short
    /// [`Fault::truncated`]. Hold an output that shows nothing yet, such as a
    /// start, until the next that does, and drop it if the response fails
    /// first, so that failure can still be refused
    /// ([`Response::until_content`](crate::Response::until_content)).
    fn response_decoder(
        &self,
        response: &http::response::Parts,
        request: &http::Request<Bytes>,
    ) -> Box<dyn Decoder<Self::Output>>;

    /// Encodes a response to the client's `request`, having edited
    /// `response`, the head to send: the provider's for a response in this
    /// format, else an empty one. A `refusal` is the response's one
    /// failure, before any byte: write its answer as it came if this
    /// format read it ([`Fault::answer`]), else the fault in this
    /// protocol's words, with its status and retry. Set the content type,
    /// and what a streaming request's success needs, each of whose outputs
    /// is its own data frame.
    ///
    /// Write an output as the bytes it came in while it says the same, what
    /// changed written into them; one from elsewhere from its value. What
    /// this protocol cannot write is an `Err`. The end always comes: write a
    /// failure in this protocol's framing.
    fn response_encoder(
        &self,
        response: &mut http::response::Parts,
        request: &http::Request<Bytes>,
        refusal: Option<&Fault>,
    ) -> Box<dyn Encoder<Self::Output>>;

    /// Whether `output` is content: what
    /// [`Response::until_content`](crate::Response::until_content) commits on. A keep-alive,
    /// framing, or a message carrying only usage is not. Every output,
    /// content or not, reaches filters. By default, true.
    fn is_content(&self, _output: &Self::Output) -> bool {
        true
    }
}

/// Bytes to values of `T` for one body, either way: the state of one
/// stream, such as an SSE buffer. Trailers are not decoded.
pub trait Decoder<T>: Send + 'static {
    /// Decode what is complete into `out`, one value per message, and keep
    /// the rest. `Err` ends the body after what `out` got.
    fn decode(&mut self, bytes: Bytes, out: &mut Vec<T>) -> Result<(), Fault>;

    /// The body's bytes ended: push what is left, and return
    /// `Err(Fault::truncated())` if it had not said it was complete.
    fn end(&mut self, out: &mut Vec<T>) -> Result<(), Fault>;
}

/// Values of `T` to bytes for one body, either way.
pub trait Encoder<T>: Send + 'static {
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
    /// as the protocol gives a response none. Its head then goes at once
    /// and nothing more is read: the response ends `Ok`; its call, its
    /// decoder told the body ended, `Ok` if that decoder says it is
    /// complete, else abandoned. Asked once, after [`Encoder::open`]. By
    /// default, false.
    fn done(&self) -> bool {
        false
    }
}

/// A request as the codecs see it.
pub(crate) type Request = http::Request<Bytes>;

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

/// An [`Encoder`] with its values erased.
pub(crate) trait AnyEncoder: Send {
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

impl<T: Send + 'static> AnyEncoder for Erased<Box<dyn Encoder<T>>, T> {
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

/// A [`Format`] with its types erased, as the gateway keeps it.
pub(crate) trait AnyFormat: Send + Sync + 'static {
    fn name(&self) -> &str;
    fn accepts(&self, head: &http::request::Parts) -> bool;
    fn request_decoder(&self, request: &Request) -> Box<dyn AnyDecoder>;
    fn request_encoder(
        &self,
        request: &mut Request,
        first: Option<&AnyInput>,
        streams: bool,
    ) -> Box<dyn AnyEncoder>;
    fn response_decoder(&self, head: &http::response::Parts, to: &Request) -> Box<dyn AnyDecoder>;
    fn response_encoder(
        &self,
        head: &mut http::response::Parts,
        to: &Request,
        refusal: Option<&Fault>,
    ) -> Box<dyn AnyEncoder>;
    /// Its input and output types.
    fn kind(&self) -> (TypeId, TypeId);
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

    fn accepts(&self, head: &http::request::Parts) -> bool {
        Format::accepts(self, head)
    }

    fn request_decoder(&self, request: &Request) -> Box<dyn AnyDecoder> {
        Box::new(Erased(Format::request_decoder(self, request), PhantomData))
    }

    fn request_encoder(
        &self,
        request: &mut Request,
        first: Option<&AnyInput>,
        streams: bool,
    ) -> Box<dyn AnyEncoder> {
        let first = first.and_then(|i| i.downcast_ref::<F::Input>());
        Box::new(Erased(
            Format::request_encoder(self, request, first, streams),
            PhantomData,
        ))
    }

    fn response_decoder(&self, head: &http::response::Parts, to: &Request) -> Box<dyn AnyDecoder> {
        Box::new(Erased(
            Format::response_decoder(self, head, to),
            PhantomData,
        ))
    }

    fn response_encoder(
        &self,
        head: &mut http::response::Parts,
        to: &Request,
        refusal: Option<&Fault>,
    ) -> Box<dyn AnyEncoder> {
        Box::new(Erased(
            Format::response_encoder(self, head, to, refusal),
            PhantomData,
        ))
    }

    fn kind(&self) -> (TypeId, TypeId) {
        (TypeId::of::<F::Input>(), TypeId::of::<F::Output>())
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
