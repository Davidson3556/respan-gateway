use std::any::{Any, TypeId};
use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use futures::FutureExt;
use futures::future::BoxFuture;

use http::header::{CONNECTION, CONTENT_ENCODING, EXPECT, HOST, TE};

use crate::format::{AnyFormat, AnyInput};
use crate::gateway::{
    Edge, InFlight, Inner, PANICKED, Target, provider_for, strip_hop_by_hop, strip_named,
};
use crate::provider::{CallEdge, OpenCalls, SharedCall, lock};
use crate::{
    Body, ClientView, Decoder, Doc, Event, Fault, Filter, Item, Outcome, Plugin, Provider,
    ProviderContext, Response, Usage,
};

/// A provider's response as it arrived: its head, its body and its decoder.
pub(crate) type Sent = (http::response::Parts, Body, Box<dyn Decoder>);

pub(crate) type HeadFn = Box<dyn FnOnce(&mut http::response::Parts) + Send>;

/// A plugin's [`Filter`], over the items of its events' type.
trait ItemFilter: Send {
    /// Filter the item's events.
    fn item(&mut self, item: &mut Item) -> Result<(), Fault>;

    /// The response ends as `outcome` says: `tail`, held by earlier
    /// filters, goes through this one, then this one's held events join it.
    fn end(&mut self, outcome: &Outcome, tail: &mut Option<Item>) -> Result<(), Fault>;
}

struct Typed<T, F>(F, PhantomData<fn(T)>);

impl<T: Event, F: Filter<T>> ItemFilter for Typed<T, F> {
    fn item(&mut self, item: &mut Item) -> Result<(), Fault> {
        item.filter(|event, out| self.0.event(event, out))
    }

    fn end(&mut self, outcome: &Outcome, tail: &mut Option<Item>) -> Result<(), Fault> {
        if let Some(item) = tail {
            self.item(item)?;
        }
        let mut held = Vec::new();
        self.0.end(outcome, &mut held)?;
        match tail {
            _ if held.is_empty() => {}
            Some(item) => item.extend(held),
            None => *tail = Some(Item::new(held)),
        }
        Ok(())
    }
}

/// One edge's filters of a response, applied the last added first.
#[derive(Default)]
pub(crate) struct Filters(Vec<Box<dyn ItemFilter>>);

impl Filters {
    /// Add `filter`, if `T` is `format`'s output type.
    pub(crate) fn add<T: Event>(
        &mut self,
        format: &dyn AnyFormat,
        filter: impl Filter<T>,
    ) -> Result<(), Fault> {
        if format.kind().map(|(_, output)| output) != Some(TypeId::of::<T>()) {
            return Err(Fault::wrong_kind::<T>());
        }
        self.0.push(Box::new(Typed(filter, PhantomData)));
        Ok(())
    }

    /// Filter `item`.
    pub(crate) fn item(&mut self, item: &mut Item) -> Result<(), Fault> {
        self.0.iter_mut().rev().try_for_each(|f| f.item(item))
    }

    /// Run the filters' ends: what they held back, and the end, failed if
    /// one failed.
    pub(crate) fn end(
        &mut self,
        status: Option<http::StatusCode>,
        end: Result<(), Fault>,
        usage: &Usage,
    ) -> (Option<Item>, Result<(), Fault>) {
        if self.0.is_empty() {
            return (None, end);
        }
        let outcome = Outcome {
            status,
            end,
            usage: usage.clone(),
        };
        let mut tail = None;
        let mut ends = self.0.iter_mut().rev();
        match ends.try_for_each(|f| f.end(&outcome, &mut tail)) {
            Ok(()) => (tail, outcome.end),
            Err(f) => (None, failed(f, &outcome.end)),
        }
    }
}

/// The end after `fault` fails a response ending `end`, marked said if the
/// client already has `end`'s error.
pub(crate) fn failed(fault: Fault, end: &Result<(), Fault>) -> Result<(), Fault> {
    match end {
        Err(said) if said.is_said() => Err(fault.said()),
        _ => Err(fault.unsaid()),
    }
}

/// The client edge's response edits. The `Mutex` is reached only through
/// `&mut`, so it never locks; it makes the holder `Sync` for end hooks.
#[derive(Default)]
struct Edits {
    head: Vec<HeadFn>,
    filters: Filters,
}

pub(crate) fn unlocked<T>(edits: &mut Mutex<T>) -> &mut T {
    edits.get_mut().unwrap_or_else(PoisonError::into_inner)
}

/// One client request, as the client edge's hooks see it: head, body,
/// input, model, target, response edits and provider calls.
pub struct Context {
    /// The client's format: the first served that accepted the request.
    pub(crate) format: Arc<dyn AnyFormat>,
    head: Arc<http::request::Parts>,
    /// The head as it arrived; the input and model are read from it.
    pub(crate) arrived: Arc<http::request::Parts>,
    pub(crate) body: Doc,
    model: Option<String>,
    /// The provider that answers if every plugin passes.
    pub(crate) target: Option<Target>,
    input: Input,
    /// Set once end hooks start, and in a call's copy: no calls allowed.
    closed: bool,
    /// Its calls' end hooks still to run; its own wait for them.
    calls: Arc<InFlight>,
    /// Its calls with an open response, ended when it closes.
    pub(crate) open: Arc<OpenCalls>,
    /// The call whose response is sent, which ends as it does.
    pub(crate) committed: Option<SharedCall>,
    edits: Mutex<Edits>,
    pub(crate) gateway: Arc<Inner>,
}

impl std::fmt::Debug for Context {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Context")
            .field("format", &self.format.name())
            .finish_non_exhaustive()
    }
}

impl Edge for Context {
    fn chain(&self) -> &[Arc<dyn Plugin>] {
        &self.gateway.chain
    }

    fn end<'a>(&'a self, plugin: &'a dyn Plugin, outcome: &'a Outcome) -> BoxFuture<'a, ()> {
        plugin.on_client_end(self, outcome)
    }

    /// The committed call ends as the response did, then the other open
    /// calls are abandoned. The committed call's filters already ran their
    /// `end`, or never will if the client edge stopped reading.
    fn close(&mut self, end: &Result<(), Fault>) {
        self.closed = true;
        if let Some(call) = self.committed.take() {
            lock(&call).finish(end.clone());
        }
        self.open.abandon();
    }

    fn after(&self) -> Option<&InFlight> {
        Some(&self.calls)
    }
}

impl Context {
    pub(crate) fn new(
        format: Arc<dyn AnyFormat>,
        head: http::request::Parts,
        body: Doc,
        model: Option<String>,
        gateway: Arc<Inner>,
    ) -> Self {
        let target = gateway.target.clone();
        let head = Arc::new(head);
        Self {
            format,
            arrived: head.clone(),
            head,
            body,
            model,
            target,
            input: Input::default(),
            closed: false,
            calls: Arc::default(),
            open: Arc::default(),
            committed: None,
            edits: Mutex::default(),
            gateway,
        }
    }

    /// The name of the client's format: the first served that accepted the
    /// request.
    pub fn format(&self) -> &str {
        self.format.name()
    }

    /// The client's request line and headers. Its `extensions` hold values
    /// set by the adapter or earlier plugins.
    pub fn head(&self) -> &http::request::Parts {
        &self.head
    }

    /// Like [`Context::head`], for changing it, such as to attach values or
    /// to remove the gateway's own credential before formats pass headers
    /// on. Later calls see the edits through their format's
    /// [`prepare`](crate::Format::prepare). [`model`](crate::Format::model)
    /// and [`decode`](crate::Format::decode) read the head as it arrived.
    /// Set the model with [`Context::set_model`].
    pub fn head_mut(&mut self) -> &mut http::request::Parts {
        Arc::make_mut(&mut self.head)
    }

    /// The client's body as it arrived, in its format. `Err` is reserved for
    /// content a plugin may not see.
    pub fn body(&self) -> Result<&Doc, Fault> {
        Ok(&self.body)
    }

    /// The client's input as plugins left it, as `T`, the client's format's
    /// [`Input`](crate::Format::Input) type. `Err` with
    /// [`codes::WRONG_KIND`](crate::codes::WRONG_KIND) for another type, or
    /// [`decode`](crate::Format::decode)'s `Err` for a bad body, the same on
    /// every read. Reading changes nothing.
    pub fn input<T: Event>(&self) -> Result<&T, Fault> {
        self.input.get(&*self.format, &self.arrived, &self.body)
    }

    /// Like [`Context::input`], for changing it. Later calls get the change,
    /// encoded in their provider's format. An unchanged input reaches a
    /// provider of the client's format as the client's body.
    pub fn input_mut<T: Event>(&mut self) -> Result<&mut T, Fault> {
        self.input.get_mut(&*self.format, &self.arrived, &self.body)
    }

    /// The model sent to providers: the client's, unless a plugin set one.
    pub fn model(&self) -> Option<&str> {
        self.model.as_deref()
    }

    /// Send providers `model`. A model can be changed, not removed.
    pub fn set_model(&mut self, model: impl Into<String>) {
        self.model = Some(model.into());
    }

    /// The provider that answers if every plugin passes; initially
    /// [`Gateway::target`](crate::Gateway::target).
    pub fn target(&self) -> Option<&Target> {
        self.target.as_ref()
    }

    /// Have `provider` answer if every plugin passes. Routers set the target
    /// so later plugins still run.
    pub fn set_target(&mut self, provider: impl Into<Target>) {
        self.target = Some(provider.into());
    }

    /// A snapshot for a call to keep; it cannot call.
    fn at_call(&self) -> Self {
        Self {
            format: self.format.clone(),
            head: self.head.clone(),
            arrived: self.arrived.clone(),
            body: self.body.clone(),
            model: self.model.clone(),
            target: self.target.clone(),
            input: self.input.clone(),
            closed: true,
            calls: self.calls.clone(),
            open: self.open.clone(),
            committed: None,
            edits: Mutex::default(),
            gateway: self.gateway.clone(),
        }
    }

    /// The request, as the client's format sees it.
    pub(crate) fn view(&self) -> ClientView<'_> {
        ClientView::new(true, &self.head, &self.body, self.model())
    }

    /// The filters plugins added, taken by the committed response.
    pub(crate) fn take_filters(&mut self) -> Filters {
        std::mem::take(&mut unlocked(&mut self.edits).filters)
    }

    /// The head edits, taken by the outgoing head, a refusal's included.
    pub(crate) fn take_head_edits(&mut self) -> Vec<HeadFn> {
        std::mem::take(&mut unlocked(&mut self.edits).head)
    }

    pub(crate) fn edit_head(&mut self, edit: HeadFn) {
        unlocked(&mut self.edits).head.push(edit);
    }

    /// Edit the status or headers sent to the client, whoever answers,
    /// refusals included. Edits apply the last added first. A refusal keeps
    /// its status unless an edit makes it another error. An edit that panics
    /// changes nothing and the request is refused, the other edits applied.
    /// Connection headers are removed afterwards. `Err` is reserved for
    /// edits after the head is sent; always `Ok` in `on_client`.
    pub fn map_head(
        &mut self,
        f: impl FnOnce(&mut http::response::Parts) + Send + 'static,
    ) -> Result<(), Fault> {
        self.edit_head(Box::new(f));
        Ok(())
    }

    /// Run the response's events through `filter`, whoever answers: a
    /// closure editing each in place, or a [`Filter`]. Filters run after the
    /// provider edge's, the last added first. `Err` with
    /// [`codes::WRONG_KIND`](crate::codes::WRONG_KIND) unless `T` is the
    /// client's format's [`Output`](crate::Format::Output) type.
    pub fn filter<T: Event>(&mut self, filter: impl Filter<T>) -> Result<(), Fault> {
        unlocked(&mut self.edits).filters.add(&*self.format, filter)
    }

    /// A call to `provider`, sent when awaited, the only way to reach one.
    /// It carries the input as plugins left it, translated if the provider
    /// speaks another format of the kind. Every plugin's
    /// [`on_provider`](crate::Plugin::on_provider) runs first. An error
    /// status is still `Ok`; check the response's head.
    ///
    /// `Err` with [`codes::CONFIG`](crate::codes::CONFIG) if `provider` is
    /// unknown or shares neither format nor kind with the client, with
    /// [`codes::WRONG_KIND`](crate::codes::WRONG_KIND) for a [`Call::input`]
    /// of another type, and with [`codes::INTERNAL`](crate::codes::INTERNAL)
    /// outside `on_client`. Otherwise the `Err` of an `on_provider`, of
    /// decode, encode or `prepare`, or of the transport.
    pub fn call(&self, provider: impl Into<Target>) -> Call<'_> {
        Call {
            cx: self,
            target: provider.into(),
            model: None,
            input: None,
        }
    }

    async fn send(&self, call: Call<'_>) -> Result<Response, Fault> {
        if self.closed {
            return Err(Fault::internal(
                "providers can be called only from on_client",
            ));
        }
        // Provider details go to logs only.
        let p = provider_for(&*self.format, &self.gateway.providers, call.target.name()).map_err(
            |e| Fault::config("a provider this request cannot use was called").with_source(e),
        )?;
        let own = call.input.transpose()?;
        let relayed = own.is_none() && self.format.name() == p.format.name();
        let mut input = self.input.clone();
        input.changed = own.or(input.changed);
        let model = call.model.or_else(|| self.model.clone());
        let output = p.format.kind().map(|(_, output)| output);
        let respond = |sent, edge| Response::provider(sent, relayed, output, edge);
        // With no plugins there is no provider edge to run.
        if self.gateway.chain.is_empty() {
            let sent = self.exchange(p, &self.head, &input, model.as_deref());
            return Ok(respond(sent.await?, None));
        }
        let px = ProviderContext {
            client: self.at_call(),
            format: p.format.clone(),
            target: call.target,
            model,
            head: self.head.clone(),
            input,
            filters: Mutex::default(),
            abort: None,
            _ends: InFlight::enter(self.calls.clone()),
        };
        let mut edge = CallEdge::call(&self.gateway.in_flight, px);
        let sent = async {
            edge.open().await?;
            let px = edge.cx();
            let (head, input, model) = (&px.head, &px.input, px.model());
            let sent = self.exchange(p, head, input, model).await?;
            edge.status = Some(sent.0.status);
            Ok::<_, Fault>(sent)
        };
        // A panic ends this call as internal before unwinding, so the drop
        // during unwinding does not end it abandoned.
        match AssertUnwindSafe(sent).catch_unwind().await {
            Ok(Ok(sent)) => Ok(respond(sent, Some(edge))),
            Ok(Err(fault)) => {
                edge.finish(Err(fault.clone()));
                Err(fault)
            }
            Err(panic) => {
                edge.finish(Err(Fault::internal(PANICKED)));
                std::panic::resume_unwind(panic)
            }
        }
    }

    /// Send `input` to `p` for `model`, with `head` as the call's hooks left
    /// it; returns the response and its decoder.
    async fn exchange(
        &self,
        p: &Provider,
        head: &http::request::Parts,
        input: &Input,
        model: Option<&str>,
    ) -> Result<Sent, Fault> {
        let same = self.format.name() == p.format.name();
        let body = input.body(&*self.format, p, same, &self.arrived, &self.body)?;
        let head = end_to_end(head);
        let view = ClientView::new(same, &head, &self.body, model);
        let mut req = p.format.prepare(body, view)?;
        // The call's values reach the transport, beneath any `prepare` set.
        let mut extensions = head.extensions.clone();
        extensions.extend(std::mem::take(req.extensions_mut()));
        *req.extensions_mut() = extensions;
        let (mut sent, body) = req.into_parts();
        let bytes = body.to_bytes();
        // A body patched or encoded anew is sent plain.
        if bytes != self.body.to_bytes() {
            sent.headers.remove(CONTENT_ENCODING);
        }
        // Strip connection headers, `Host` and `Expect` (the body is sent
        // whole), keeping `te: trailers`, which gRPC needs.
        let trailers = sent.headers.get_all(TE).iter().any(|v| {
            let v = v.to_str().unwrap_or_default();
            v.split(',')
                .any(|t| t.trim().eq_ignore_ascii_case("trailers"))
        });
        strip_hop_by_hop(&mut sent.headers);
        sent.headers.remove(HOST);
        sent.headers.remove(EXPECT);
        if trailers {
            sent.headers
                .insert(TE, http::HeaderValue::from_static("trailers"));
        }
        let bytes = http::Request::from_parts(sent, crate::body(bytes));
        let (head, body) = p.transport.send(bytes).await?.into_parts();
        // The decoder follows the provider's head as it came, before edits.
        let decoder = p.format.decoder(&head, view);
        Ok((head, body, decoder))
    }
}

/// `head` without the headers its `Connection` names. Stripped here, from
/// the client's hop, so they never strip what a format sets.
fn end_to_end(head: &http::request::Parts) -> Cow<'_, http::request::Parts> {
    if !head.headers.contains_key(CONNECTION) {
        return Cow::Borrowed(head);
    }
    let mut head = head.clone();
    strip_named(&mut head.headers);
    Cow::Owned(head)
}

/// A call from [`Context::call`], sent when awaited. Its settings apply to
/// it alone.
#[must_use = "a call is sent only when awaited"]
pub struct Call<'a> {
    cx: &'a Context,
    target: Target,
    model: Option<String>,
    input: Option<Result<AnyInput, Fault>>,
}

impl std::fmt::Debug for Call<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Call")
            .field("target", &self.target)
            .field("model", &self.model)
            .finish_non_exhaustive()
    }
}

impl Call<'_> {
    /// Ask for `model` in this call only.
    pub fn model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }

    /// Send `input` in place of the client's, such as a judge's prompt. It
    /// must be the client's format's input type; another fails the call with
    /// [`codes::WRONG_KIND`](crate::codes::WRONG_KIND). To a provider of the
    /// client's format it is a patch of the client's body, keeping what the
    /// kind cannot hold. A response to it, if sent on, is written from its
    /// events.
    pub fn input<T: Event>(mut self, input: T) -> Self {
        let kind = self.cx.format.kind();
        self.input = Some(
            match kind.map(|(input, _)| input) == Some(TypeId::of::<T>()) {
                true => Ok(Arc::new(input)),
                false => Err(Fault::wrong_kind::<T>()),
            },
        );
        self
    }
}

impl<'a> IntoFuture for Call<'a> {
    type Output = Result<Response, Fault>;
    type IntoFuture = BoxFuture<'a, Result<Response, Fault>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.cx.send(self))
    }
}

/// An input as a context or call holds it: the client's, decoded lazily
/// once per request and shared, plus the holder's changed copy.
#[derive(Clone, Default)]
pub(crate) struct Input {
    base: Arc<OnceLock<Result<AnyInput, Fault>>>,
    /// Set once a plugin changes it or a call brings its own.
    changed: Option<AnyInput>,
}

impl Input {
    /// The client's input, decoded by the client's format.
    fn base(
        &self,
        format: &dyn AnyFormat,
        head: &http::request::Parts,
        body: &Doc,
    ) -> Result<&AnyInput, Fault> {
        let base = self.base.get_or_init(|| format.decode(head, body));
        base.as_ref().map_err(Clone::clone)
    }

    /// The input as `T`; another type is refused without decoding.
    pub(crate) fn get<T: Any>(
        &self,
        format: &dyn AnyFormat,
        head: &http::request::Parts,
        body: &Doc,
    ) -> Result<&T, Fault> {
        if format.kind().map(|(input, _)| input) != Some(TypeId::of::<T>()) {
            return Err(Fault::wrong_kind::<T>());
        }
        let input = match &self.changed {
            Some(changed) => changed,
            None => self.base(format, head, body)?,
        };
        input.downcast_ref().ok_or_else(Fault::wrong_kind::<T>)
    }

    /// Like [`Input::get`], for changing it; copies first if shared.
    pub(crate) fn get_mut<T: Any>(
        &mut self,
        format: &dyn AnyFormat,
        head: &http::request::Parts,
        body: &Doc,
    ) -> Result<&mut T, Fault> {
        self.get::<T>(format, head, body)?;
        let changed = match self.changed.take() {
            Some(changed) => changed,
            None => self.base(format, head, body)?.clone(),
        };
        let changed = self.changed.insert(changed);
        if Arc::get_mut(changed).is_none() {
            *changed = format.copy(changed);
        }
        Arc::get_mut(changed)
            .and_then(|input| input.downcast_mut())
            .ok_or_else(Fault::wrong_kind::<T>)
    }

    /// The body for `p` (of the client's format if `same`): the client's if
    /// the input is unchanged, else patched, or encoded anew across formats.
    fn body(
        &self,
        format: &dyn AnyFormat,
        p: &Provider,
        same: bool,
        head: &http::request::Parts,
        body: &Doc,
    ) -> Result<Doc, Fault> {
        let input = match &self.changed {
            Some(changed) => changed,
            None if same => return Ok(body.clone()),
            None => self.base(format, head, body)?,
        };
        if !same {
            return p.format.encode(input, None);
        }
        match self.base(format, head, body) {
            Ok(base) if format.same(base, input) => Ok(body.clone()),
            Ok(base) => p.format.encode(input, Some((body, base))),
            Err(_) => p.format.encode(input, None),
        }
    }
}
