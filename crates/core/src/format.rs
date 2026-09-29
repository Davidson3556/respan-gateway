use std::any::{Any, TypeId};
use std::sync::Arc;

use futures::future::{BoxFuture, ready};
use futures::stream::{self, BoxStream};
use futures::{FutureExt, StreamExt, TryFutureExt};

use crate::{Extensions, Fault};

/// A stream of one kind's events; the first `Err` is its last item.
pub type Events<T> = BoxStream<'static, Result<T, Fault>>;

/// What came with a head, sealed: only its format opens it. The gateway
/// hands a client's to a provider edge only on a relay, and a provider's
/// back only then.
pub type Origin = Arc<dyn Any + Send + Sync>;

/// One direction of a two-way stream: its head's origin, and its events or
/// why there are none.
pub type Half<T> = (Option<Origin>, Result<Events<T>, Fault>);

/// Called by a client edge once its answer's end is out, with how it ended;
/// dropped uncalled, the session ends
/// [`codes::ABANDONED`](crate::codes::ABANDONED).
pub type Ended = Box<dyn FnOnce(Result<(), Fault>) + Send>;

/// A provider edge's answer, once dialed.
pub type Dialed<T> = BoxFuture<'static, Result<Half<T>, Fault>>;

/// A format: one protocol's view of a kind, and its two edges, which the
/// gateway routes events between. Serve one to clients with
/// [`Gateway::serve`](crate::Gateway::serve), call a provider in one with
/// [`Gateway::provider`](crate::Gateway::provider), and hand the gateway a
/// client's connection with [`Gateway::handle`](crate::Gateway::handle).
/// HTTP's formats implement it through `respan_gateway_http::impl_format!`
/// over a codec; a format on another wire implements it itself. A format
/// that keeps the client edge's defaults is dial-only, such as an
/// in-process model.
///
/// The gateway never sees a byte, a head or a key: a client edge
/// ([`accept`](Format::accept)) turns a connection into inputs and outputs
/// back into it, a provider edge ([`dial`](Format::dial)) the reverse, and
/// heads travel as sealed [`Origin`]s, which only a format of the name they
/// came in opens.
///
/// Its kind is its pair of value types, through which formats of one kind
/// translate. They must differ, as the type names the direction, and should
/// belong to the kind alone: a shared type such as `serde_json::Value` lets
/// a plugin for one kind read another's traffic. Formats of one name relay
/// rather than translate, and share a kind, a wire and a dialer. Every
/// message decodes to one value, even one the kind cannot read, which keeps
/// its bytes: a relay loses nothing only if the format writes a value as
/// the bytes it came in while it says the same.
///
/// # Rules
///
/// 1. [`accept`](Format::accept) resolves when the request is in: a
///    one-shot request once its body is read, which its origin keeps, no
///    inputs following (`None`); a session at once, its inputs the frames to
///    come. The plugins run once it resolves. [`opening`](Format::opening)
///    is `Some` only for a request whose inputs are `None`.
/// 2. A provider edge classifies its answer at its head: a refusal resolves
///    `Ok((origin, Err(fault)))`, which the gateway sends as a refusal and a
///    call's [`is_refused`](crate::ProviderContext::is_refused) says.
/// 3. The client edge owns its head timing: a refusal is written as one; an
///    open answer's head when the edge decides, such as with its first
///    bytes, a failure before any written as a refusal, or at once for a
///    session.
/// 4. The client edge calls [`Ended`] exactly when its answer's end is out,
///    or drops it. `Ended(Err(fault))` also ends the answer's call with
///    `fault`. Outputs dropped unread end their call as their end, if it is
///    ready, says, else abandoned. `Ended` dropped first ends the session
///    abandoned, and the outputs then end `Err` abandoned.
/// 5. What a provider edge's outputs have ready at once is one batch: the
///    call's filters see all of it before the client's see its first, as a
///    meter sees usage that shares a frame with an output a client filter
///    fails on. Outputs to be read apart are `Pending` between.
///
/// # The adapter's part
///
/// [`accept`](Format::accept) must not await its `reply`, which resolves
/// only once `accept`'s future did and the plugins ran: the `Wire` carries
/// the adapter's slot, through which the edge hands over what it writes, and
/// the adapter drives [`Gateway::handle`](crate::Gateway::handle) to its end
/// while it awaits that slot. A slot dropped unfilled means no format took
/// the wire: `handle`'s end says why, for the adapter to write plainly.
pub trait Format: Send + Sync + 'static {
    /// What a client sends, one or a stream.
    type Input: Clone + PartialEq + Send + Sync + 'static;

    /// What a provider answers, one or a stream.
    type Output: Clone + PartialEq + Send + Sync + 'static;

    /// A client connection as its adapter hands it over, with its reply
    /// slot.
    type Wire: Send + 'static;

    /// How calls reach one provider: a URL and keys, a process, a socket.
    type Dialer: Send + Sync + 'static;

    /// The protocol's identity. Formats of one name relay rather than
    /// translate, and share a kind, a wire and a dialer.
    fn name(&self) -> &str;

    /// Whether this format speaks on `wire`; the first served that does
    /// takes it. It may look inside; the gateway does not. By default,
    /// false: a format that keeps the client edge's defaults is dial-only.
    fn accepts(&self, _wire: &Self::Wire) -> bool {
        false
    }

    /// Whether `output` is content, what
    /// [`Response::until_content`](crate::Response::until_content) stops
    /// at; a keep-alive is not. By default, true.
    fn is_content(&self, _output: &Self::Output) -> bool {
        true
    }

    /// The client edge: resolves once the client's request is in, with its
    /// origin and the inputs that follow it (`None`: none do; `Err`: the
    /// request broke, and is refused before any plugin). `reply` resolves
    /// with what to send back, the answer and [`Ended`]; when to write the
    /// head is this edge's decision. By default it refuses, never awaiting
    /// `reply`, for a format nothing [`accepts`](Format::accepts).
    #[allow(clippy::type_complexity)]
    fn accept(
        &self,
        _wire: Self::Wire,
        _reply: BoxFuture<'static, (Half<Self::Output>, Ended)>,
    ) -> BoxFuture<'static, (Option<Origin>, Result<Option<Events<Self::Input>>, Fault>)> {
        let fault = Fault::not_found("this format only calls providers");
        ready((None, Err(fault))).boxed()
    }

    /// The input that came with a client's head, read from its origin: `Some`
    /// only for a request whose inputs are `None` (rule 1), as a one-shot
    /// request's, else `None`. The gateway calls it only when a value is
    /// needed, never on a relay of it unchanged. By default, `None`.
    fn opening(&self, _origin: &Origin) -> Option<Result<Self::Input, Fault>> {
        None
    }

    /// The provider edge.
    ///
    /// - `relay`: the client's origin, only on a relay.
    /// - `opening`: the first input, to write from its value; `None` on a
    ///   relay means the origin's own, as it came.
    /// - `later`: the inputs that follow; `None` if none do.
    /// - Each input's bool is whether it is the client's as decoded, which
    ///   says whose fault it is if this format cannot write it
    ///   ([`Fault::of_input`]): the format applies it where it writes the
    ///   input, out of the gateway's sight (an HTTP body's frames reach the
    ///   transport, not the gateway).
    ///
    /// `Err` now: not sent. The future's `Err`: sent, and not answered.
    /// `Ok((origin, Err(fault)))`: answered and refused (rule 2).
    /// `Ok((origin, Ok(outputs)))`: open.
    fn dial(
        &self,
        dialer: &Self::Dialer,
        extensions: Extensions,
        relay: Option<&Origin>,
        opening: Option<(&Self::Input, bool)>,
        later: Option<Events<(Self::Input, bool)>>,
    ) -> Result<Dialed<Self::Output>, Fault>;
}

/// An input, erased and shared.
pub(crate) type AnyInput = Arc<dyn Any + Send + Sync>;

/// Values of one kind's type, erased: a `Vec<T>`.
pub(crate) type Values = Box<dyn Any + Send>;

/// Events erased: each item a `Vec<T>`.
pub(crate) type AnyEvents = BoxStream<'static, Result<Values, Fault>>;

pub(crate) type AnyHalf = (Option<Origin>, Result<AnyEvents, Fault>);

pub(crate) type AnyReply = (AnyHalf, Ended);

/// A client edge's accepted request: its origin, and its inputs if any follow.
pub(crate) type AnyAccepted = (Option<Origin>, Result<Option<AnyEvents>, Fault>);

/// Inputs going to a provider, each with whether it is the client's.
pub(crate) type AnyInputs = BoxStream<'static, Result<(AnyInput, bool), Fault>>;

pub(crate) type AnyDialed = BoxFuture<'static, Result<AnyHalf, Fault>>;

/// A [`Format`] with its types erased, as the gateway keeps it.
pub(crate) trait AnyFormat: Send + Sync + 'static {
    fn name(&self) -> &str;
    /// Its input and output types.
    fn kind(&self) -> (TypeId, TypeId);
    /// What its wire is, which formats of one name share.
    fn wire(&self) -> (TypeId, TypeId);
    fn accepts(&self, wire: &dyn Any) -> bool;
    fn accept(
        &self,
        wire: Box<dyn Any + Send>,
        reply: BoxFuture<'static, AnyReply>,
    ) -> BoxFuture<'static, AnyAccepted>;
    fn opening(&self, origin: &Origin) -> Option<Result<AnyInput, Fault>>;
    fn dial(
        &self,
        dialer: &(dyn Any + Send + Sync),
        extensions: Extensions,
        relay: Option<&Origin>,
        opening: Option<(&AnyInput, bool)>,
        later: Option<AnyInputs>,
    ) -> Result<AnyDialed, Fault>;
    /// Whether any of `outputs` is content.
    fn any_content(&self, outputs: &Values) -> bool;
    /// Whether two inputs are equal.
    fn same(&self, a: &AnyInput, b: &AnyInput) -> bool;
    /// Inputs as values, each moved if not shared.
    fn values(&self, inputs: Vec<AnyInput>) -> Values;
    /// Values of inputs, each shared.
    fn inputs(&self, values: Values) -> Vec<AnyInput>;
    /// `more` outputs after `values`.
    fn append(&self, values: &mut Values, more: Values);
}

/// A format, erased, and its wire as registration checks it.
pub(crate) struct Erased<F> {
    format: F,
    wire: (TypeId, TypeId),
}

impl<F: Format> Erased<F> {
    /// `format` on its own wire and dialer.
    pub(crate) fn new(format: F) -> Self {
        let wire = (TypeId::of::<F::Wire>(), TypeId::of::<F::Dialer>());
        Self { format, wire }
    }
}

/// Typed events erased, one value per item.
fn erase<T: Send + 'static>(events: Events<T>) -> AnyEvents {
    events
        .map(|e| e.map(|v| Box::new(vec![v]) as Values))
        .boxed()
}

/// Erased events typed, each item's values in turn.
fn typed<T: Send + 'static>(events: AnyEvents) -> Events<T> {
    let each = |item: Result<Values, Fault>| {
        let values =
            item.and_then(|v| v.downcast::<Vec<T>>().map_err(|_| Fault::wrong_kind::<T>()));
        stream::iter(match values {
            Ok(values) => values.into_iter().map(Ok).collect::<Vec<_>>(),
            Err(fault) => vec![Err(fault)],
        })
    };
    events.flat_map(each).boxed()
}

/// `input` as `T`, moved if not shared.
fn owned<T: Clone + Send + Sync + 'static>(input: AnyInput) -> Result<T, Fault> {
    let input = input
        .downcast::<T>()
        .map_err(|_| Fault::wrong_kind::<T>())?;
    Ok(Arc::try_unwrap(input).unwrap_or_else(|shared| (*shared).clone()))
}

impl<F: Format> AnyFormat for Erased<F> {
    fn name(&self) -> &str {
        self.format.name()
    }

    fn kind(&self) -> (TypeId, TypeId) {
        (TypeId::of::<F::Input>(), TypeId::of::<F::Output>())
    }

    fn wire(&self) -> (TypeId, TypeId) {
        self.wire
    }

    fn accepts(&self, wire: &dyn Any) -> bool {
        wire.downcast_ref()
            .is_some_and(|wire| self.format.accepts(wire))
    }

    fn accept(
        &self,
        wire: Box<dyn Any + Send>,
        reply: BoxFuture<'static, AnyReply>,
    ) -> BoxFuture<'static, AnyAccepted> {
        let Ok(wire) = wire.downcast::<F::Wire>() else {
            let fault = Fault::internal("a wire of another type than its format's");
            return ready((None, Err(fault))).boxed();
        };
        let reply = reply
            .map(|((origin, events), ended)| ((origin, events.map(typed::<F::Output>)), ended));
        let accepted = self.format.accept(*wire, reply.boxed());
        accepted
            .map(|(origin, events)| (origin, events.map(|e| e.map(erase))))
            .boxed()
    }

    fn opening(&self, origin: &Origin) -> Option<Result<AnyInput, Fault>> {
        let opening = self.format.opening(origin)?;
        Some(opening.map(|input| Arc::new(input) as AnyInput))
    }

    fn dial(
        &self,
        dialer: &(dyn Any + Send + Sync),
        extensions: Extensions,
        relay: Option<&Origin>,
        opening: Option<(&AnyInput, bool)>,
        later: Option<AnyInputs>,
    ) -> Result<AnyDialed, Fault> {
        let dialer = dialer
            .downcast_ref()
            .ok_or_else(|| Fault::internal("a dialer of another type than its format's"))?;
        let opening = match opening {
            Some((input, clients)) => {
                let input = input.downcast_ref();
                Some((input.ok_or_else(Fault::wrong_kind::<F::Input>)?, clients))
            }
            None => None,
        };
        let later = later.map(|later| {
            later
                .map(|item| item.and_then(|(input, clients)| Ok((owned(input)?, clients))))
                .boxed()
        });
        let dialed = self
            .format
            .dial(dialer, extensions, relay, opening, later)?;
        Ok(dialed
            .map_ok(|(origin, events)| (origin, events.map(erase)))
            .boxed())
    }

    fn any_content(&self, outputs: &Values) -> bool {
        let outputs = outputs.downcast_ref::<Vec<F::Output>>();
        outputs
            .into_iter()
            .flatten()
            .any(|output| self.format.is_content(output))
    }

    fn same(&self, a: &AnyInput, b: &AnyInput) -> bool {
        let (a, b) = (a.downcast_ref::<F::Input>(), b.downcast_ref::<F::Input>());
        a.is_some() && a == b
    }

    fn values(&self, inputs: Vec<AnyInput>) -> Values {
        let inputs = inputs.into_iter().filter_map(|i| owned::<F::Input>(i).ok());
        Box::new(inputs.collect::<Vec<_>>())
    }

    fn inputs(&self, values: Values) -> Vec<AnyInput> {
        let inputs = values.downcast::<Vec<F::Input>>().map(|v| *v);
        let inputs = inputs.unwrap_or_default().into_iter();
        inputs.map(|input| Arc::new(input) as AnyInput).collect()
    }

    fn append(&self, values: &mut Values, more: Values) {
        let more = more.downcast::<Vec<F::Output>>().map(|v| *v);
        if let (Some(values), Ok(mut more)) = (values.downcast_mut::<Vec<F::Output>>(), more) {
            values.append(&mut more);
        }
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
