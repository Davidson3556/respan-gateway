use std::any::{Any, TypeId};
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::stream::{self, BoxStream};
use futures::{FutureExt, StreamExt, TryFutureExt};

use crate::{Extensions, Fault};

/// A stream of one kind's events; the first `Err` is its last item. One that
/// nothing will follow is already ended: its first poll is `Ready(None)`.
pub type Events<T> = BoxStream<'static, Result<T, Fault>>;

/// What came with a head, sealed: only its dialect opens it. The gateway
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

/// What a client edge is given to send back: the session's extensions, the
/// answer (the provider's origin on a relay, and its outputs or a refusal),
/// and [`Ended`].
pub type Reply<T> = (Extensions, Half<T>, Ended);

/// A provider edge's answer, once dialed.
pub type Dialed<T> = BoxFuture<'static, Result<Half<T>, Fault>>;

/// A dialect: one protocol's view of a kind, and its two edges, which the
/// gateway routes events between. Its name and kind are
/// [`Format`](crate::Format)'s; it is the edge contract that replaces the
/// codec (a temporary name, decision 0020). Serve one with
/// [`Gateway::serve_dialect`](crate::Gateway::serve_dialect), call one with
/// [`Gateway::provider_dialect`](crate::Gateway::provider_dialect), and hand
/// the gateway a client's connection with
/// [`Gateway::open`](crate::Gateway::open).
///
/// The gateway never sees a byte, a head or a key: a client edge
/// ([`accept`](Dialect::accept)) turns a connection into inputs and outputs
/// back into it, a provider edge ([`dial`](Dialect::dial)) the reverse, and
/// heads travel as sealed [`Origin`]s.
///
/// # Rules
///
/// 1. An [`Events`] that nothing will follow is already ended: its first
///    poll is `Ready(None)`. The gateway polls a client's inputs once as the
///    session opens, to tell whether any follow; what that poll yields is
///    kept and read first. Inputs that must not be read ahead, or that flow
///    even if none come, return `Pending` to that poll.
/// 2. [`accept`](Dialect::accept) resolves when the request is in: a
///    one-shot request once its body is read, which its origin keeps, its
///    inputs ended; a session at once, its inputs the frames to come. The
///    plugins run once it resolves.
/// 3. A provider edge classifies its answer at its head: a refusal resolves
///    `Ok((origin, Err(fault)))`, which the gateway sends as a refusal and a
///    call's [`is_head_only`](crate::ProviderContext::is_head_only) says.
/// 4. The client edge owns its head timing: a refusal is written as one; an
///    open answer's head when the edge decides, such as with its first
///    bytes, a failure before any written as a refusal, or at once for a
///    session.
/// 5. The client edge calls [`Ended`] exactly when its answer's end is out,
///    or drops it. `Ended(Err(fault))` also ends the answer's call with
///    `fault`. Outputs dropped unread end their call as their end, if it is
///    ready, says, else abandoned. `Ended` dropped first ends the session
///    abandoned, and the outputs then end `Err` abandoned.
/// 6. What a provider edge's outputs have ready at once is one batch: the
///    call's filters see all of it before the client's see its first, as a
///    meter sees usage that shares a frame with an output a client filter
///    fails on. Outputs to be read apart are `Pending` between.
///
/// # The adapter's part
///
/// [`accept`](Dialect::accept) must not await its `reply`, which resolves
/// only once `accept`'s future did and the plugins ran: the `Wire` carries
/// the adapter's slot, through which the edge hands over what it writes, and
/// the adapter drives [`Gateway::open`](crate::Gateway::open) to its end
/// while it awaits that slot. A slot dropped unfilled means no dialect took
/// the wire: `open`'s end says why, for the adapter to write plainly.
pub trait Dialect: Send + Sync + 'static {
    /// What a client sends, one or a stream.
    type Input: Clone + PartialEq + Send + Sync + 'static;

    /// What a provider answers, one or a stream.
    type Output: Clone + PartialEq + Send + Sync + 'static;

    /// A client connection as its adapter hands it over, with its reply
    /// slot.
    type Wire: Send + 'static;

    /// How calls reach one provider: a URL and keys, a process, a socket.
    type Dialer: Send + Sync + 'static;

    /// The protocol's identity. Dialects of one name relay rather than
    /// translate, and share a kind, a wire and a dialer.
    fn name(&self) -> &str;

    /// Whether this dialect speaks on `wire`; the first served that does
    /// takes it. It may look inside; the gateway does not.
    fn accepts(&self, wire: &Self::Wire) -> bool;

    /// Whether `output` is content, what
    /// [`Response::until_content`](crate::Response::until_content) stops
    /// at; a keep-alive is not. By default, true.
    fn is_content(&self, _output: &Self::Output) -> bool {
        true
    }

    /// The client edge: resolves once the client's request is in, with its
    /// origin and the inputs that follow (`Err`: the request broke, and is
    /// refused before any plugin). `reply` resolves with what to send back
    /// ([`Reply`]); when to write the head is this edge's decision.
    fn accept(
        &self,
        wire: Self::Wire,
        reply: BoxFuture<'static, Reply<Self::Output>>,
    ) -> BoxFuture<'static, Half<Self::Input>>;

    /// The input that came with a client's head, read from its origin: `Some`
    /// only if the inputs had ended as the session opened (rule 1), as a
    /// one-shot request's, else `None`. The gateway calls it only when a
    /// value is needed, never on a relay of it unchanged.
    fn opening(&self, origin: &Origin) -> Option<Result<Self::Input, Fault>>;

    /// The provider edge.
    ///
    /// - `relay`: the client's origin, only on a relay.
    /// - `opening`: the first input, to write from its value; `None` on a
    ///   relay means the origin's own, as it came.
    /// - `later`: the inputs that follow; `None` if none do.
    /// - Each input's bool is whether it is the client's as decoded, which
    ///   says whose fault it is if this dialect cannot write it
    ///   ([`Fault::of_input`]).
    ///
    /// `Err` now: not sent. The future's `Err`: sent, and not answered.
    /// `Ok((origin, Err(fault)))`: answered and refused (rule 3).
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

pub(crate) type AnyReply = (Extensions, AnyHalf, Ended);

/// Inputs going to a provider, each with whether it is the client's.
pub(crate) type AnyInputs = BoxStream<'static, Result<(AnyInput, bool), Fault>>;

pub(crate) type AnyDialed = BoxFuture<'static, Result<AnyHalf, Fault>>;

/// A [`Dialect`] with its types erased, as the gateway keeps it.
pub(crate) trait AnyDialect: Send + Sync + 'static {
    fn name(&self) -> &str;
    /// Its input and output types.
    fn kind(&self) -> (TypeId, TypeId);
    /// What its wire is, which dialects of one name share.
    fn wire(&self) -> (TypeId, TypeId);
    fn accepts(&self, wire: &dyn Any) -> bool;
    fn accept(
        &self,
        wire: Box<dyn Any + Send>,
        reply: BoxFuture<'static, AnyReply>,
    ) -> BoxFuture<'static, AnyHalf>;
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

/// A dialect, erased, and its wire as registration checks it.
pub(crate) struct Erased<D> {
    dialect: D,
    wire: (TypeId, TypeId),
}

impl<D: Dialect> Erased<D> {
    /// `dialect` on its own wire and dialer.
    pub(crate) fn new(dialect: D) -> Self {
        let wire = (TypeId::of::<D::Wire>(), TypeId::of::<D::Dialer>());
        Self { dialect, wire }
    }

    /// `dialect` on `wire`, such as an old format's pair of head types.
    pub(crate) fn on(dialect: D, wire: (TypeId, TypeId)) -> Self {
        Self { dialect, wire }
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

impl<D: Dialect> AnyDialect for Erased<D> {
    fn name(&self) -> &str {
        self.dialect.name()
    }

    fn kind(&self) -> (TypeId, TypeId) {
        (TypeId::of::<D::Input>(), TypeId::of::<D::Output>())
    }

    fn wire(&self) -> (TypeId, TypeId) {
        self.wire
    }

    fn accepts(&self, wire: &dyn Any) -> bool {
        wire.downcast_ref()
            .is_some_and(|wire| self.dialect.accepts(wire))
    }

    fn accept(
        &self,
        wire: Box<dyn Any + Send>,
        reply: BoxFuture<'static, AnyReply>,
    ) -> BoxFuture<'static, AnyHalf> {
        let Ok(wire) = wire.downcast::<D::Wire>() else {
            let fault = Fault::internal("a wire of another type than its dialect's");
            return futures::future::ready((None, Err(fault))).boxed();
        };
        let reply = reply.map(|(ext, (origin, events), ended)| {
            (ext, (origin, events.map(typed::<D::Output>)), ended)
        });
        let half = self.dialect.accept(*wire, reply.boxed());
        half.map(|(origin, events)| (origin, events.map(erase)))
            .boxed()
    }

    fn opening(&self, origin: &Origin) -> Option<Result<AnyInput, Fault>> {
        let opening = self.dialect.opening(origin)?;
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
            .ok_or_else(|| Fault::internal("a dialer of another type than its dialect's"))?;
        let opening = match opening {
            Some((input, clients)) => {
                let input = input.downcast_ref();
                Some((input.ok_or_else(Fault::wrong_kind::<D::Input>)?, clients))
            }
            None => None,
        };
        let later = later.map(|later| {
            later
                .map(|item| item.and_then(|(input, clients)| Ok((owned(input)?, clients))))
                .boxed()
        });
        let dialed = self
            .dialect
            .dial(dialer, extensions, relay, opening, later)?;
        Ok(dialed
            .map_ok(|(origin, events)| (origin, events.map(erase)))
            .boxed())
    }

    fn any_content(&self, outputs: &Values) -> bool {
        let outputs = outputs.downcast_ref::<Vec<D::Output>>();
        outputs
            .into_iter()
            .flatten()
            .any(|output| self.dialect.is_content(output))
    }

    fn same(&self, a: &AnyInput, b: &AnyInput) -> bool {
        let (a, b) = (a.downcast_ref::<D::Input>(), b.downcast_ref::<D::Input>());
        a.is_some() && a == b
    }

    fn values(&self, inputs: Vec<AnyInput>) -> Values {
        let inputs = inputs.into_iter().filter_map(|i| owned::<D::Input>(i).ok());
        Box::new(inputs.collect::<Vec<_>>())
    }

    fn inputs(&self, values: Values) -> Vec<AnyInput> {
        let inputs = values.downcast::<Vec<D::Input>>().map(|v| *v);
        let inputs = inputs.unwrap_or_default().into_iter();
        inputs.map(|input| Arc::new(input) as AnyInput).collect()
    }

    fn append(&self, values: &mut Values, more: Values) {
        let more = more.downcast::<Vec<D::Output>>().map(|v| *v);
        if let (Some(values), Ok(mut more)) = (values.downcast_mut::<Vec<D::Output>>(), more) {
            values.append(&mut more);
        }
    }
}

/// Whether `T` is `dialect`'s input type (`true`) or output type (`false`);
/// `Err` for another.
pub(crate) fn is_input<T: Any>(dialect: &dyn AnyDialect) -> Result<bool, Fault> {
    match dialect.kind() {
        (input, _) if input == TypeId::of::<T>() => Ok(true),
        (_, output) if output == TypeId::of::<T>() => Ok(false),
        _ => Err(Fault::wrong_kind::<T>()),
    }
}
