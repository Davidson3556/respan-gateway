use std::any::{Any, TypeId};
use std::future::IntoFuture;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use futures::future::BoxFuture;

use http::Extensions;
use http::header::{EXPECT, HOST};

use crate::gateway::{Inner, Target, provider_for, strip_hop_by_hop};
use crate::{Doc, Fault, Filter, Item, Kind, KindType, Outcome, RequestView, Response, Route};

type HeadFn = Box<dyn FnOnce(&mut http::response::Parts) + Send>;

/// A plugin's [`Filter`], over the items of its events' type.
pub(crate) trait ItemFilter: Send {
    /// Filter the item's events.
    fn item(&mut self, item: &mut Item) -> Result<(), Fault>;

    /// The response ends as `outcome` says: `tail`, what filters run before
    /// this one still held, goes through it, then what it holds joins it.
    fn end(&mut self, outcome: &Outcome, tail: &mut Option<Item>) -> Result<(), Fault>;
}

struct Typed<T, F>(F, PhantomData<fn(T)>);

impl<T: KindType, F: Filter<T>> ItemFilter for Typed<T, F> {
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

/// Response edits, each list applied the last added first.
#[derive(Default)]
struct Edits {
    head: Vec<HeadFn>,
    filters: Vec<Box<dyn ItemFilter>>,
}

/// One client request, handed to every hook: its head, body, model, typed
/// request, response edits, values attached by type, and provider calls.
pub struct Context {
    pub(crate) route: Arc<Route>,
    head: http::request::Parts,
    body: Doc,
    model: Option<String>,
    /// The provider that answers if every plugin passes.
    pub(crate) target: Option<Target>,
    /// The body as its kind's type, once asked for.
    request: Decoded,
    /// Set when the close hooks start: the request is over.
    pub(crate) closed: bool,
    /// Reached only through `&mut`, so the lock is never taken; it makes
    /// `Context` `Sync` for close hooks although edits are not.
    edits: Mutex<Edits>,
    gateway: Arc<Inner>,
}

impl std::fmt::Debug for Context {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Context")
            .field("route", &self.route.name)
            .finish_non_exhaustive()
    }
}

impl Context {
    pub(crate) fn new(
        route: Arc<Route>,
        head: http::request::Parts,
        body: Doc,
        gateway: Arc<Inner>,
    ) -> Self {
        let model = route.format.model(&head, &body);
        let request = Decoded::new(route.kind.clone());
        let target = route.target.clone();
        Self {
            route,
            head,
            body,
            model,
            target,
            request,
            closed: false,
            edits: Mutex::default(),
            gateway,
        }
    }

    /// The name of the route the request came in on, such as for logs or a
    /// cache key.
    pub fn route(&self) -> &str {
        &self.route.name
    }

    /// The client's request line and headers. Values attached by type are
    /// read through [`Context::extensions`].
    pub fn head(&self) -> &http::request::Parts {
        &self.head
    }

    /// The client's body as it arrived, in its format; the request as
    /// plugins changed it is [`Context::request`]. `Err` for content the
    /// plugin may not see; always `Ok` today.
    pub fn body(&self) -> Result<&Doc, Fault> {
        Ok(&self.body)
    }

    /// The request as plugins have left it, as `T`, its kind's request type.
    /// `Err` with [`codes::WRONG_KIND`](crate::codes::WRONG_KIND) if `T` is
    /// not the route's kind's request type; the `Err` of the kind's
    /// [`decode`](crate::Kind::new) if the body is not a request of its
    /// kind, the same on every call.
    pub fn request<T: KindType>(&self) -> Result<&T, Fault> {
        self.request.typed(&self.head, &self.body)
    }

    /// Like [`Context::request`], for changing it. Providers called
    /// afterwards get the changed request, encoded in their format. If it
    /// did not actually change, a provider of the client's format gets the
    /// client's body.
    pub fn request_mut<T: KindType>(&mut self) -> Result<&mut T, Fault> {
        self.request.typed::<T>(&self.head, &self.body)?;
        let typed = self.request.get_mut().and_then(|r| r.downcast_mut());
        typed.ok_or_else(Fault::wrong_kind::<T>)
    }

    fn decoded(&self) -> Result<&(dyn Any + Send + Sync), Fault> {
        self.request.get(&self.head, &self.body)
    }

    /// The model providers are sent: the client's, unless a plugin set one.
    /// Change it here, not in the request.
    pub fn model(&self) -> Option<&str> {
        self.model.as_deref()
    }

    /// Send providers `model`, whatever the request said. A model can be
    /// changed, not removed.
    pub fn set_model(&mut self, model: impl Into<String>) {
        self.model = Some(model.into());
    }

    /// The provider that answers if every plugin passes; initially the
    /// route's target.
    pub fn target(&self) -> Option<&Target> {
        self.target.as_ref()
    }

    /// Have `provider` answer if every plugin passes. A router sets the
    /// target rather than calling it, so later plugins still run.
    pub fn set_target(&mut self, provider: impl Into<Target>) {
        self.target = Some(provider.into());
    }

    /// The request, as the client's format sees it.
    pub(crate) fn view(&self) -> RequestView<'_> {
        RequestView::new(true, &self.head, &self.body, self.model())
    }

    fn edits(&mut self) -> &mut Edits {
        self.edits.get_mut().unwrap_or_else(PoisonError::into_inner)
    }

    /// The filters plugins added, taken by the response that is sent.
    pub(crate) fn take_filters(&mut self) -> Vec<Box<dyn ItemFilter>> {
        std::mem::take(&mut self.edits().filters)
    }

    /// The head edits, taken by the head that goes out, even a refusal's.
    pub(crate) fn take_head_edits(&mut self) -> Vec<HeadFn> {
        std::mem::take(&mut self.edits().head)
    }

    /// Change the status or headers of the response sent to the client,
    /// whoever answers, refusals included. Edits apply the last added first,
    /// unwinding in reverse chain order. `Err` once the head is sent; always
    /// `Ok` today.
    pub fn map_head(
        &mut self,
        f: impl FnOnce(&mut http::response::Parts) + Send + 'static,
    ) -> Result<(), Fault> {
        self.edits().head.push(Box::new(f));
        Ok(())
    }

    /// Run the response's events through `filter`, whoever answers: a
    /// closure that changes each in place (`|e: &mut Event| ..`), or a
    /// [`Filter`] that also holds, drops and inserts them. `Err` with
    /// [`codes::WRONG_KIND`](crate::codes::WRONG_KIND) unless `T` is the
    /// route's kind's event type. Filters apply the last added first, each
    /// to what the next-added one passed on.
    pub fn filter<T: KindType>(&mut self, filter: impl Filter<T>) -> Result<(), Fault> {
        if self.route.kind.as_ref().map(|k| k.event) != Some(TypeId::of::<T>()) {
            return Err(Fault::wrong_kind::<T>());
        }
        self.edits()
            .filters
            .push(Box::new(Typed(filter, PhantomData)));
        Ok(())
    }

    /// Values attached by type, such as by an earlier plugin or the
    /// adapter: the head's extensions.
    pub fn extensions(&self) -> &Extensions {
        &self.head.extensions
    }

    /// Attach values by type for later plugins, which read them through
    /// [`Context::extensions`].
    pub fn extensions_mut(&mut self) -> &mut Extensions {
        &mut self.head.extensions
    }

    /// A call to `provider`, sent when awaited: the request as plugins left
    /// it, translated if the provider speaks another format of the kind. An
    /// error status is still `Ok`; check the response's head. The only way
    /// to reach a provider.
    ///
    /// `Err` with [`codes::CONFIG`](crate::codes::CONFIG) if this route
    /// cannot use `provider` (unknown, or of another format and kind) or the
    /// URL of the path `prepare` gives is not valid, with
    /// [`codes::WRONG_KIND`](crate::codes::WRONG_KIND) for a
    /// [`Call::request`] of another type, and with
    /// [`codes::INTERNAL`](crate::codes::INTERNAL) from close hooks.
    /// Otherwise the `Err` of the kind's decode or encode, of the provider
    /// format's `prepare`, or of the transport.
    pub fn call(&self, provider: impl Into<Target>) -> Call<'_> {
        Call {
            cx: self,
            target: provider.into(),
            model: None,
            request: None,
        }
    }

    async fn send(&self, call: Call<'_>) -> Result<Response, Fault> {
        if self.closed {
            return Err(Fault::internal(
                "providers cannot be called once the request has ended",
            ));
        }
        let provider = call.target.name();
        // Route and provider details go to logs only.
        let p = provider_for(&self.route, &self.gateway.providers, provider).map_err(|e| {
            Fault::config("a provider this route cannot use was called").with_source(e)
        })?;
        let (from, to) = (self.route.format.name(), p.format.name());
        let same = from == to;
        // Another format gets the request encoded anew. The client's format
        // gets the client's body unless the request changed or the call
        // brings its own; then the body is patched, keeping what the kind
        // cannot hold.
        let own = call.request.transpose()?;
        let body = match &p.kind {
            Some(kind) if !same || own.is_some() || self.request.touched() => {
                let now = match &own {
                    Some(request) => &**request,
                    None => self.decoded()?,
                };
                let before = same
                    .then(|| self.request.as_decoded(&self.head, &self.body))
                    .transpose()?;
                match before {
                    Some(before) if kind.codec.same(before, now) => self.body.clone(),
                    before => kind.codec.encode(now, before.map(|b| (&self.body, b)))?,
                }
            }
            _ => self.body.clone(),
        };
        let model = call.model.as_deref().or(self.model());
        let view = RequestView::new(same, &self.head, &self.body, model);
        let mut req = p.format.prepare(body, view, &p.headers)?;
        let url = format!("{}{}", p.base_url.trim_end_matches('/'), req.uri());
        *req.uri_mut() = url.parse().map_err(|_| {
            let why = format!("provider '{provider}': bad URL {url}");
            Fault::config("a provider's URL is not valid").with_source(why)
        })?;
        // Strip connection headers a format passed on, `Host`, and `Expect`,
        // as the body is sent whole.
        let headers = req.headers_mut();
        strip_hop_by_hop(headers);
        headers.remove(HOST);
        headers.remove(EXPECT);
        headers.extend(p.headers.clone());

        let (sent, body) = req.into_parts();
        let bytes = http::Request::from_parts(sent, crate::transport::full(body.to_bytes()));
        let (head, stream) = self.gateway.transport.send(bytes).await?.into_parts();
        let decoder = p.format.decoder(&head, view);
        let event = p.kind.as_ref().map(|k| k.event);
        let answers_client = same && own.is_none();
        Ok(Response::provider(
            head,
            stream,
            decoder,
            answers_client,
            event,
        ))
    }
}

/// A call to a provider from [`Context::call`], sent when awaited. Its
/// settings are its own: the request plugins left is unchanged.
#[must_use = "a call is sent only when awaited"]
pub struct Call<'a> {
    cx: &'a Context,
    target: Target,
    model: Option<String>,
    request: Option<Result<Box<dyn Any + Send + Sync>, Fault>>,
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

    /// Send `request`, of the route's kind's request type, instead of the
    /// client's, such as a judge's prompt; another type is an `Err` when
    /// sent. The model is [`Call::model`] or else [`Context::model`]. For a
    /// provider of the client's format it patches the client's body, so a
    /// copy of the client's request carries what the kind cannot hold; a
    /// judge builds its own. Its response, if sent on, is written from its
    /// events, like a translated one.
    pub fn request<T: KindType>(mut self, request: T) -> Self {
        let kind = self.cx.route.kind.as_ref();
        self.request = Some(match kind.map(|k| k.request) == Some(TypeId::of::<T>()) {
            true => Ok(Box::new(request)),
            false => Err(Fault::wrong_kind::<T>()),
        });
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

/// The client's request as its kind's type, decoded when first asked for,
/// and a copy of it as decoded, kept once a plugin asks to change it.
pub(crate) struct Decoded {
    kind: Option<Kind>,
    cell: OnceLock<Result<Box<dyn Any + Send + Sync>, Fault>>,
    original: Option<Box<dyn Any + Send + Sync>>,
}

impl std::fmt::Debug for Decoded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Decoded").finish_non_exhaustive()
    }
}

impl Decoded {
    pub(crate) fn new(kind: Option<Kind>) -> Self {
        Self {
            kind,
            cell: OnceLock::new(),
            original: None,
        }
    }

    /// The request as `T`, decoded only if `T` is the kind's request type:
    /// another type is refused whatever the body holds.
    pub(crate) fn typed<T: Any>(
        &self,
        head: &http::request::Parts,
        body: &Doc,
    ) -> Result<&T, Fault> {
        if self
            .kind
            .as_ref()
            .is_none_or(|k| k.request != TypeId::of::<T>())
        {
            return Err(Fault::wrong_kind::<T>());
        }
        self.get(head, body)?
            .downcast_ref()
            .ok_or_else(Fault::wrong_kind::<T>)
    }

    pub(crate) fn get(
        &self,
        head: &http::request::Parts,
        body: &Doc,
    ) -> Result<&(dyn Any + Send + Sync), Fault> {
        let request = self.cell.get_or_init(|| {
            let kind = self.kind.as_ref();
            kind.ok_or_else(|| Fault::config("this route's format has no kind"))?
                .codec
                .decode(head, body)
        });
        request.as_deref().map_err(Clone::clone)
    }

    /// The request, if decoded, for changing it. The first call keeps a
    /// copy to tell later whether it changed.
    pub(crate) fn get_mut(&mut self) -> Option<&mut (dyn Any + Send + Sync)> {
        let request = self.cell.get_mut()?.as_deref_mut().ok()?;
        if self.original.is_none()
            && let Some(kind) = &self.kind
        {
            self.original = kind.codec.copy(request);
        }
        Some(request)
    }

    /// The request as it was decoded, whatever plugins changed since.
    pub(crate) fn as_decoded(
        &self,
        head: &http::request::Parts,
        body: &Doc,
    ) -> Result<&(dyn Any + Send + Sync), Fault> {
        match self.original.as_deref() {
            Some(original) => Ok(original),
            None => self.get(head, body),
        }
    }

    /// Whether a plugin asked to change the request.
    pub(crate) fn touched(&self) -> bool {
        self.original.is_some()
    }
}
