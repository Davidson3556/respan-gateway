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

    /// The response ends as `outcome` says: `tail`, what filters run before
    /// this one still held, goes through it, then what it holds joins it.
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

    /// The response ends with `end`: what the filters held back, and the
    /// end once their own ends ran, failed if one failed.
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

/// The end once `fault` failed a response ending `end`: said already if
/// `end` is an error the client already has.
pub(crate) fn failed(fault: Fault, end: &Result<(), Fault>) -> Result<(), Fault> {
    match end {
        Err(said) if said.is_said() => Err(fault.said()),
        _ => Err(fault.unsaid()),
    }
}

/// The client edge's response edits. Kept in a `Mutex` reached only
/// through `&mut`, so the lock is never taken; it makes its holder `Sync`
/// for end hooks although edits are not.
#[derive(Default)]
struct Edits {
    head: Vec<HeadFn>,
    filters: Filters,
}

pub(crate) fn unlocked<T>(edits: &mut Mutex<T>) -> &mut T {
    edits.get_mut().unwrap_or_else(PoisonError::into_inner)
}

/// One client request, handed to the client edge's hooks: its head, body,
/// model, input, response edits, values attached by type, and provider
/// calls.
pub struct Context {
    /// The client's format: the first served that accepted the request.
    pub(crate) format: Arc<dyn AnyFormat>,
    head: Arc<http::request::Parts>,
    /// The head as it arrived, which the input and model are read from.
    pub(crate) arrived: Arc<http::request::Parts>,
    pub(crate) body: Doc,
    model: Option<String>,
    /// The provider that answers if every plugin passes.
    pub(crate) target: Option<Target>,
    input: Input,
    /// Set when the end hooks start, and in a call's copy: providers cannot
    /// be called.
    closed: bool,
    /// The end hooks of its calls still to run, which its own wait for.
    calls: Arc<InFlight>,
    /// Its calls whose response is still open, ended when it closes.
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

    /// The committed call ends as the response did, before the other calls
    /// still open are abandoned: its filters' `end` ran at its end, or not
    /// at all if the client edge stopped reading it first.
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

    /// The name of the client's format, the first served that accepted the
    /// request, such as for logs or a cache key.
    pub fn format(&self) -> &str {
        self.format.name()
    }

    /// The client's request line and headers, and values attached by type
    /// in its `extensions`, such as by an earlier plugin or the adapter.
    pub fn head(&self) -> &http::request::Parts {
        &self.head
    }

    /// Like [`Context::head`], for changing it: attaching values for later
    /// plugins and calls, or removing a header, such as the gateway's own
    /// credential, before formats pass the client's headers on. Edits reach
    /// calls made afterwards through their format's
    /// [`prepare`](crate::Format::prepare); within the client's format its
    /// headers and query pass on, across formats only what that format
    /// reads. [`model`](crate::Format::model) and
    /// [`decode`](crate::Format::decode) read the head as it arrived. The
    /// response is not affected. [`ProviderContext::client`] shows the head
    /// as of its call. The model is set through [`Context::set_model`].
    pub fn head_mut(&mut self) -> &mut http::request::Parts {
        Arc::make_mut(&mut self.head)
    }

    /// The client's body as it arrived, in its format; the input as plugins
    /// changed it is [`Context::input`]. `Err` for content the plugin may
    /// not see; always `Ok` today.
    pub fn body(&self) -> Result<&Doc, Fault> {
        Ok(&self.body)
    }

    /// The client's input as plugins have left it, as `T`, the client's
    /// format's [`Input`](crate::Format::Input) type. `Err` with
    /// [`codes::WRONG_KIND`](crate::codes::WRONG_KIND) if `T` is not that
    /// type; the `Err` of its [`decode`](crate::Format::decode) if the body
    /// is not a request of its format, the same each time it is read. Reading
    /// changes nothing: an unchanged input reaches a provider of the
    /// client's format as the client's body.
    pub fn input<T: Event>(&self) -> Result<&T, Fault> {
        self.input.get(&*self.format, &self.arrived, &self.body)
    }

    /// Like [`Context::input`], for changing it. Calls made afterwards get
    /// the changed input, encoded in their provider's format. If it did not
    /// actually change, a provider of the client's format gets the client's
    /// body.
    pub fn input_mut<T: Event>(&mut self) -> Result<&mut T, Fault> {
        self.input.get_mut(&*self.format, &self.arrived, &self.body)
    }

    /// The model providers are sent: the client's, unless a plugin set one.
    /// Change it here, not in the input.
    pub fn model(&self) -> Option<&str> {
        self.model.as_deref()
    }

    /// Send providers `model`, whatever the request said. A model can be
    /// changed, not removed.
    pub fn set_model(&mut self, model: impl Into<String>) {
        self.model = Some(model.into());
    }

    /// The provider that answers if every plugin passes; initially the
    /// gateway's ([`Gateway::target`](crate::Gateway::target)).
    pub fn target(&self) -> Option<&Target> {
        self.target.as_ref()
    }

    /// Have `provider` answer if every plugin passes. A router sets the
    /// target rather than calling it, so later plugins still run.
    pub fn set_target(&mut self, provider: impl Into<Target>) {
        self.target = Some(provider.into());
    }

    /// The request as it is now, for a call to keep: it cannot call.
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

    /// The filters plugins added, taken by the response that is sent.
    pub(crate) fn take_filters(&mut self) -> Filters {
        std::mem::take(&mut unlocked(&mut self.edits).filters)
    }

    /// The head edits, taken by the head that goes out, even a refusal's.
    pub(crate) fn take_head_edits(&mut self) -> Vec<HeadFn> {
        std::mem::take(&mut unlocked(&mut self.edits).head)
    }

    /// Add a head edit.
    pub(crate) fn edit_head(&mut self, edit: HeadFn) {
        unlocked(&mut self.edits).head.push(edit);
    }

    /// Change the status or headers of the response sent to the client,
    /// whoever answers, refusals included. Edits apply the last added first,
    /// unwinding in reverse plugin order. A refusal keeps its status unless
    /// an edit makes it another error. An edit that panics changes nothing,
    /// and the
    /// request is refused with the other edits applied. Connection headers,
    /// such as `Connection` and `Content-Length`, are removed after the
    /// edits. `Err` is reserved for edits once the head is sent, as in a
    /// session; `Ok` in `on_client`.
    pub fn map_head(
        &mut self,
        f: impl FnOnce(&mut http::response::Parts) + Send + 'static,
    ) -> Result<(), Fault> {
        self.edit_head(Box::new(f));
        Ok(())
    }

    /// Run the response's events through `filter`, whoever answers: a
    /// closure that changes each in place (`|r: &mut Reply| ..`), or a
    /// [`Filter`] that also holds, drops and inserts them. `Err` with
    /// [`codes::WRONG_KIND`](crate::codes::WRONG_KIND) unless `T` is the
    /// client's format's [`Output`](crate::Format::Output) type. Filters
    /// apply the last added first, each to what the next-added one passed
    /// on, after the provider edge's.
    pub fn filter<T: Event>(&mut self, filter: impl Filter<T>) -> Result<(), Fault> {
        unlocked(&mut self.edits).filters.add(&*self.format, filter)
    }

    /// A call to `provider`, sent when awaited: the input as plugins left
    /// it, translated if the provider speaks another format of the kind.
    /// Every plugin runs [`on_provider`](crate::Plugin::on_provider)
    /// first. An error status
    /// is still `Ok`; check the response's head. The only way to reach a
    /// provider.
    ///
    /// `Err` with [`codes::CONFIG`](crate::codes::CONFIG) if this request
    /// cannot use `provider` (unknown, or sharing neither the client's
    /// format nor its kind), with
    /// [`codes::WRONG_KIND`](crate::codes::WRONG_KIND) for a [`Call::input`]
    /// of another type, and with [`codes::INTERNAL`](crate::codes::INTERNAL)
    /// outside `on_client`: from end hooks and from
    /// [`ProviderContext::client`]. Otherwise the `Err` of an
    /// `on_provider`, of the formats' decode or encode, of the provider
    /// format's `prepare`, or of the transport.
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
        // A panic here, such as a hook's, ends this call as internal before
        // it unwinds, so a call dropped by the unwinding is abandoned.
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

    /// Send `input` to `p`, asking for `model`, with `head`, the client's
    /// as the call's hooks left it: the response's head and body, and its
    /// decoder.
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
        // Strip connection headers, keeping `te: trailers`, which a request
        // such as gRPC's needs, and `Host` and `Expect`, as the body is sent
        // whole.
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
        // The decoder reads the bytes the provider sent, so it follows the
        // provider's head as it came, before any edit.
        let decoder = p.format.decoder(&head, view);
        Ok((head, body, decoder))
    }
}

/// `head` without the headers its `Connection` names, which are the
/// client's hop's, so they never strip what a format or the kernel sets.
fn end_to_end(head: &http::request::Parts) -> Cow<'_, http::request::Parts> {
    if !head.headers.contains_key(CONNECTION) {
        return Cow::Borrowed(head);
    }
    let mut head = head.clone();
    strip_named(&mut head.headers);
    Cow::Owned(head)
}

/// A call to a provider from [`Context::call`], sent when awaited. Its
/// settings are its own: the input plugins left is unchanged.
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
    /// Ask for `model` in this call, such as the next one a fallback tries.
    pub fn model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }

    /// Send `input`, of the client's format's input type, instead of the
    /// client's, such as a judge's prompt; another type is an `Err` when
    /// sent with [`codes::WRONG_KIND`](crate::codes::WRONG_KIND). The model
    /// is [`Call::model`] or else [`Context::model`]. For a provider of the
    /// client's format it patches the client's body, so a copy of the
    /// client's input carries what the kind cannot hold, unless that body
    /// is not of the format: then it is encoded anew. A judge builds its
    /// own. Its response, if sent on, is written from its events, like a
    /// translated one.
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

/// An input, as a context or a call holds it. Like a [`Doc`]: the client's,
/// decoded once per request when first asked for and shared by every
/// context and call, and a changed copy of its holder's own.
#[derive(Clone, Default)]
pub(crate) struct Input {
    base: Arc<OnceLock<Result<AnyInput, Fault>>>,
    /// Set once a plugin asks to change it, or a call brings its own.
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

    /// The input as `T`, decoded only if `T` is the client's input type:
    /// another type is refused whatever the body holds.
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

    /// Like [`Input::get`], for changing it: its own copy, made first.
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

    /// The body for `p`, which speaks the client's format if `same`: the
    /// client's as it came, unless the input changed; then the client's
    /// patched, or, in another format, the input encoded anew.
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
