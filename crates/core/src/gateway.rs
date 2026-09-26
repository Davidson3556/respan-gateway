use std::any::TypeId;
use std::collections::HashMap;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;

use bytes::{Bytes, BytesMut};
use futures::FutureExt;
use futures::channel::oneshot;
use futures::future::Shared;
use futures::stream::{self, StreamExt, TryStreamExt};
use http::header::{
    CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, EXPECT, HOST, HeaderName, PROXY_AUTHENTICATE,
    PROXY_AUTHORIZATION, RETRY_AFTER, TE, TRAILER, TRANSFER_ENCODING, UPGRADE,
};
use http::{HeaderMap, HeaderValue, StatusCode};
use http_body::Frame;
use http_body_util::{BodyExt, StreamBody};
use tokio::sync::Notify;

use crate::context::ItemFilter;
use crate::response::Source;
use crate::{
    Body, Context, Doc, Encoder, Fault, Format, Kind, Outcome, Part, Plugin, Response, Transport,
    Usage,
};

/// The provider a request goes to, by name.
#[derive(Debug, Clone)]
pub struct Target(String);

impl Target {
    /// The provider's name.
    pub fn name(&self) -> &str {
        &self.0
    }
}

impl From<&str> for Target {
    fn from(name: &str) -> Self {
        Self(name.to_owned())
    }
}

impl From<String> for Target {
    fn from(name: String) -> Self {
        Self(name)
    }
}

/// A provider the gateway can call.
#[derive(Clone)]
pub struct Provider {
    pub(crate) name: String,
    pub(crate) format: Arc<dyn Format>,
    pub(crate) kind: Option<Kind>,
    pub(crate) base_url: String,
    pub(crate) headers: HeaderMap,
}

impl std::fmt::Debug for Provider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Provider")
            .field("name", &self.name)
            .field("format", &self.format.name())
            .field("base_url", &self.base_url)
            .finish_non_exhaustive()
    }
}

impl Provider {
    /// A provider named `name`, speaking `format`, at `base_url`, to which
    /// the format appends each request's path. The base URL holds none of
    /// that path (such as `/v1`), no query or fragment, and no credentials
    /// (`@`), which go in its headers.
    pub fn new(name: impl Into<String>, format: impl Format, base_url: impl Into<String>) -> Self {
        let format: Arc<dyn Format> = Arc::new(format);
        Self {
            name: name.into(),
            kind: format.kind(),
            format,
            base_url: base_url.into(),
            headers: HeaderMap::new(),
        }
    }

    /// Headers sent with every request, such as the API key, replacing any
    /// set before and any client header of the same name. A connection
    /// header (`Host`, `Expect`, `Content-Length`, ...) fails
    /// [`Gateway::new`].
    pub fn with_headers(mut self, headers: HeaderMap) -> Self {
        self.headers = headers;
        self
    }
}

/// A named entry point: the format clients speak, the plugin chain, and the
/// provider that answers when every plugin passes.
#[derive(Clone)]
pub struct Route {
    pub(crate) name: String,
    pub(crate) format: Arc<dyn Format>,
    pub(crate) kind: Option<Kind>,
    pub(crate) chain: Vec<Arc<dyn Plugin>>,
    pub(crate) target: Option<Target>,
}

impl std::fmt::Debug for Route {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Route")
            .field("name", &self.name)
            .field("format", &self.format.name())
            .field("chain", &self.chain.len())
            .field("target", &self.target)
            .finish()
    }
}

impl Route {
    /// A route named `name`, whose clients speak `format`, with no plugins
    /// and no target yet.
    pub fn new(name: impl Into<String>, format: impl Format) -> Self {
        let format: Arc<dyn Format> = Arc::new(format);
        Self {
            name: name.into(),
            kind: format.kind(),
            format,
            chain: Vec::new(),
            target: None,
        }
    }

    /// Add `plugin` to the end of the chain.
    pub fn with_plugin(mut self, plugin: impl Plugin) -> Self {
        self.chain.push(Arc::new(plugin));
        self
    }

    /// The provider that answers if every plugin passes and none changed
    /// the target.
    pub fn with_target(mut self, provider: impl Into<Target>) -> Self {
        self.target = Some(provider.into());
        self
    }
}

pub(crate) struct Inner {
    routes: HashMap<String, Arc<Route>>,
    pub(crate) providers: HashMap<String, Provider>,
    pub(crate) transport: Box<dyn Transport>,
    closing: Arc<HooksInFlight>,
}

/// The gateway. It has no server inside: hand it requests from an HTTP
/// adapter, an embedding program, or anything else.
#[derive(Clone)]
pub struct Gateway {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Gateway {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Gateway")
            .field("routes", &self.inner.routes.len())
            .field("providers", &self.inner.providers.len())
            .finish()
    }
}

impl Gateway {
    /// A gateway reaching `providers` through `transport`, with `routes`.
    /// `Err` with [`codes::CONFIG`](crate::codes::CONFIG) if: two providers,
    /// or two routes, share a name; a provider's base URL is not absolute,
    /// has a query, fragment or `@`, or is one `transport` does not reach; a
    /// provider header is a connection header, such as `Host` or
    /// `Content-Length`; a route has no plugins and no target; a route's
    /// target is unknown or shares neither format nor kind with it; or two
    /// formats of one name are of different kinds.
    pub fn new(
        transport: impl Transport,
        providers: impl IntoIterator<Item = Provider>,
        routes: impl IntoIterator<Item = Route>,
    ) -> Result<Gateway, Fault> {
        let twice =
            |kind: &str, name: &str| Fault::config(format!("{kind} '{name}' is defined twice"));
        let mut named_providers = HashMap::new();
        for p in providers {
            // Query, fragment and credentials are looked for in the text.
            // It may hold a key, so the error does not echo it.
            let base = p.base_url.parse::<http::Uri>().ok();
            let absolute = base
                .is_some_and(|u| u.scheme().is_some() && u.host().is_some_and(|h| !h.is_empty()));
            if !absolute || p.base_url.contains(['?', '#', '@']) || !transport.reaches(&p.base_url)
            {
                return Err(Fault::config(format!(
                    "provider '{}': bad base URL: an absolute one its transport \
                     reaches, with no query, fragment or `@` (credentials go in \
                     its headers)",
                    p.name
                )));
            }
            // Provider headers are added after connection headers are
            // stripped, so none may be one.
            let connection = |k: &&HeaderName| HOP_BY_HOP.contains(k) || [HOST, EXPECT].contains(k);
            if let Some(name) = p.headers.keys().find(connection) {
                return Err(Fault::config(format!(
                    "provider '{}': header '{name}' is the connection's",
                    p.name
                )));
            }
            if let Some(p) = named_providers.insert(p.name.clone(), p) {
                return Err(twice("provider", &p.name));
            }
        }
        let providers = named_providers;
        // Formats of one name are one protocol, so they must be of one kind.
        let mut kinds: HashMap<&str, Option<&Kind>> = HashMap::new();
        let routes: Vec<Route> = routes.into_iter().collect();
        let formats = routes.iter().map(|r| (&r.format, &r.kind));
        let formats = formats.chain(providers.values().map(|p| (&p.format, &p.kind)));
        for (format, kind) in formats {
            let seen = *kinds.entry(format.name()).or_insert(kind.as_ref());
            if !same_kind(seen, kind.as_ref()) {
                return Err(Fault::config(format!(
                    "two formats named {} are of different kinds",
                    format.name()
                )));
            }
        }
        let mut named = HashMap::new();
        for r in routes {
            if let Some(target) = &r.target {
                provider_for(&r, &providers, target.name())?;
            }
            if r.target.is_none() && r.chain.is_empty() {
                return Err(Fault::config(format!(
                    "route '{}' has no plugins and no target",
                    r.name
                )));
            }
            if let Some(r) = named.insert(r.name.clone(), Arc::new(r)) {
                return Err(twice("route", &r.name));
            }
        }
        Ok(Gateway {
            inner: Arc::new(Inner {
                routes: named,
                providers,
                transport: Box::new(transport),
                closing: Arc::default(),
            }),
        })
    }

    /// `Ok` if route `route` can send to `provider`, as a target its plugins
    /// set must: both exist and share a format or a kind; else `Err` with
    /// [`codes::CONFIG`](crate::codes::CONFIG). Not whether the provider
    /// serves any model. A route's own target is checked at build; a config
    /// uses this for targets its routers may set.
    pub fn check_target(&self, route: &str, provider: impl Into<Target>) -> Result<(), Fault> {
        let route = self
            .inner
            .routes
            .get(route)
            .ok_or_else(|| Fault::config(format!("unknown route '{route}'")))?;
        provider_for(route, &self.inner.providers, provider.into().name()).map(drop)
    }

    /// Resolves once every close hook that has started has finished. Call it
    /// on shutdown so close hooks are not lost; it waits as long as they
    /// run, so bound it with a deadline of your own.
    pub async fn idle(&self) {
        let closing = &self.inner.closing;
        loop {
            let idle = closing.idle.notified();
            if closing.count.load(Ordering::SeqCst) == 0 {
                return;
            }
            idle.await;
        }
    }

    /// Run `req` on the route named `route`. Its extensions reach plugins as
    /// [`Context::extensions`]. An unknown route gets a plain-text 404, and a
    /// format that panics before it can write a plain-text 500; the rest is
    /// in the route's format. The request body is read whole, so the caller
    /// bounds it; a body that fails while read is refused with its error's
    /// fault, in the route's format, before any plugin runs. The response
    /// body is `Send`, not `Sync`; a host that needs `Sync` wraps it.
    pub async fn handle<B>(&self, route: &str, req: http::Request<B>) -> http::Response<Body>
    where
        B: http_body::Body<Data = Bytes> + Send + 'static,
        B::Error: Into<Fault>,
    {
        let Some(route) = self.inner.routes.get(route).cloned() else {
            return closed(
                plain(StatusCode::NOT_FOUND, format!("no route '{route}'")),
                None,
            );
        };
        let (head, body) = req.into_parts();
        let (body, broken) = match body.map_err(Into::into).collect().await {
            Ok(collected) => (Doc::from(collected.to_bytes()), None),
            Err(fault) => (Doc::default(), Some(fault)),
        };
        let cx = || Context::new(route.clone(), head, body, self.inner.clone());
        // A format that panics reading the model has no response of its own.
        let Ok(cx) = std::panic::catch_unwind(AssertUnwindSafe(cx)) else {
            return closed(
                plain(StatusCode::INTERNAL_SERVER_ERROR, PANICKED.into()),
                None,
            );
        };
        let (outcome, ended) = oneshot::channel();
        let mut lifecycle = Lifecycle {
            closing: self.inner.closing.clone(),
            ran: 0,
            cx: Some(cx),
            status: None,
            usage: Usage::default(),
            outcome: Some(outcome),
        };
        let resp = match broken {
            Some(fault) => refuse(&route, fault, lifecycle).await,
            None => {
                // A plugin that panics is refused like one that fails, in
                // the client's format.
                let ran = AssertUnwindSafe(run(&route, &mut lifecycle)).catch_unwind();
                match ran.await {
                    Ok(Ok(resp)) => commit(&route, resp, lifecycle).await,
                    Ok(Err(f)) => refuse(&route, f, lifecycle).await,
                    Err(_) => refuse(&route, Fault::internal(PANICKED), lifecycle).await,
                }
            }
        };
        closed(resp, Some(ended))
    }
}

/// How a request handed to [`Gateway::handle`] ended: a future of its
/// [`Outcome`], in every response's extensions, ready once the response
/// ends, such as for billing. `None` if the request reached no route or
/// its format panicked reading the model.
#[derive(Clone)]
pub struct Closed(Shared<oneshot::Receiver<Outcome>>);

impl std::fmt::Debug for Closed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Closed").finish_non_exhaustive()
    }
}

impl Closed {
    /// The `Closed` of a response [`Gateway::handle`] returned, which every
    /// such response has; for another, one that resolves to `None`. Await
    /// it after reading the body: it resolves as the response ends, before
    /// close hooks finish ([`Gateway::idle`] waits for those).
    pub fn of<B>(response: &http::Response<B>) -> Closed {
        let none = || Closed(oneshot::channel().1.shared());
        response
            .extensions()
            .get::<Closed>()
            .cloned()
            .unwrap_or_else(none)
    }
}

impl Future for Closed {
    type Output = Option<Outcome>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx).map(Result::ok)
    }
}

/// `resp` with its [`Closed`], from `ended` or, with none, resolving to
/// `None`.
fn closed(
    mut resp: http::Response<Body>,
    ended: Option<oneshot::Receiver<Outcome>>,
) -> http::Response<Body> {
    let ended = ended.unwrap_or_else(|| oneshot::channel().1);
    resp.extensions_mut().insert(Closed(ended.shared()));
    resp
}

/// Whether formats of kinds `a` and `b`, either of which may have none, are
/// of one kind.
fn same_kind(a: Option<&Kind>, b: Option<&Kind>) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => a.same(b),
        (a, b) => a.is_none() && b.is_none(),
    }
}

const PANICKED: &str = "a plugin or format panicked";

/// A plain-text response, for what no format can write.
fn plain(status: StatusCode, text: String) -> http::Response<Body> {
    let mut head = crate::part::head(status);
    let plain = HeaderValue::from_static("text/plain; charset=utf-8");
    head.headers.insert(CONTENT_TYPE, plain);
    into_http(head, crate::transport::full(Bytes::from(text)))
}

async fn run(route: &Route, lifecycle: &mut Lifecycle) -> Result<Response, Fault> {
    for plugin in &route.chain {
        lifecycle.ran += 1;
        if let Some(resp) = plugin.on_request(lifecycle.cx()).await? {
            return Ok(resp);
        }
    }
    let cx = lifecycle.cx();
    match cx.target.clone() {
        Some(target) => cx.call(target).await,
        // Which route it was is the operator's, for logs.
        None => Err(match cx.model() {
            Some(model) => Fault::not_found(format!("nothing answers model '{model}'")),
            None => Fault::not_found("nothing answers this request"),
        }
        .with_source(format!("route '{}' set no target", route.name))),
    }
}

/// A refusal is a response that fails at once, written in the client's
/// format like any other.
async fn refuse(route: &Route, fault: Fault, lifecycle: Lifecycle) -> http::Response<Body> {
    let status = match fault.status() {
        s if s.is_client_error() || s.is_server_error() => s,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    let parts = stream::iter([Part::End(Err(fault))]);
    commit(route, Response::made(status, parts), lifecycle).await
}

/// Send `resp`. Its head waits for the first bytes: a success that fails
/// before any, such as a unary one a guardrail stops, is sent as a refusal
/// with the fault's status. Head edits apply to the head that goes out,
/// whose status the `Outcome` gets.
async fn commit(route: &Route, resp: Response, mut lifecycle: Lifecycle) -> http::Response<Body> {
    let format = route.format.as_ref();
    let client = lifecycle.cx().view();
    let mut head = resp.head;
    // A translated redirect would lose its `Location`, so it is refused;
    // a relayed one passes.
    if matches!(resp.body, Source::Bytes(_, _, false)) && head.status.is_redirection() {
        let fault = Fault::upstream(format!("the provider answered {}", head.status.as_u16()));
        return Box::pin(refuse(route, fault, lifecycle)).await;
    }
    // A provider's origin headers stay with it; a plugin's are its own.
    if matches!(resp.body, Source::Bytes(..)) {
        for name in ORIGIN {
            head.headers.remove(name);
        }
    }
    let usage = resp.usage;
    let build = || {
        let source = match resp.body {
            Source::Raw(body) => Source::Bytes(body, format.decoder(&head, client), true),
            source => source,
        };
        if let Source::Bytes(_, _, false) = source {
            // A translated response keeps only the provider's status and
            // `Retry-After`; the client's format sets the rest.
            let retry = head.headers.remove(RETRY_AFTER);
            head.headers.clear();
            head.headers.extend(retry.map(|v| (RETRY_AFTER, v)));
        }
        // Judged before the encoder touches the head: only a success is
        // refused for failing before any bytes, so a refusal never is.
        let refusable = head.status.is_success();
        // The encoder follows the head the Parts are for, before edits.
        let encoder = format.encoder(&mut head, client, source.own());
        (source, encoder, refusable)
    };
    // A format that panics building its decoder or encoder cannot write
    // even a refusal.
    let Ok((source, encoder, refusable)) = std::panic::catch_unwind(AssertUnwindSafe(build)) else {
        let status = StatusCode::INTERNAL_SERVER_ERROR;
        lifecycle.status = Some(status);
        lifecycle.finish(Err(Fault::internal(PANICKED)));
        return plain(status, PANICKED.into());
    };
    let filters = lifecycle.cx().take_filters();
    let mut pipe = Pipe {
        source,
        usage: Some(usage).filter(|u| !u.is_empty()),
        event: route.kind.as_ref().map(|k| k.event),
        filters,
        status: head.status,
        encoder,
        lifecycle,
        ended: None,
        abort: None,
    };
    let first = match pipe.first(refusable).await {
        Ok(first) => first,
        Err(fault) => return Box::pin(refuse(route, fault, pipe.lifecycle)).await,
    };
    let head_edits = pipe.lifecycle.cx().take_head_edits();
    let edit = || head_edits.into_iter().rev().for_each(|f| f(&mut head));
    // A head edit that panics is refused like a plugin that does; the head
    // has not gone out.
    if std::panic::catch_unwind(AssertUnwindSafe(edit)).is_err() {
        let fault = Fault::internal(PANICKED);
        return Box::pin(refuse(route, fault, pipe.lifecycle)).await;
    }
    pipe.lifecycle.status = Some(head.status);
    let rest = stream::unfold(pipe, |mut p| async move { p.next().await.map(|b| (b, p)) });
    let body = stream::iter(first.map(Ok)).chain(rest).map_ok(Frame::data);
    into_http(head, StreamBody::new(body).boxed_unsync())
}

/// Turns the committed response into the client's body. Filters see every
/// item. The encoder sees the client format's own Parts or, for a response
/// made anew, items without the provider's bytes, plus the usage.
struct Pipe {
    source: Source,
    /// Usage the response was given, reported first.
    usage: Option<Usage>,
    /// The event type of the route's kind, if it has one.
    event: Option<TypeId>,
    filters: Vec<Box<dyn ItemFilter>>,
    /// The response's status, before head edits.
    status: StatusCode,
    encoder: Box<dyn Encoder>,
    lifecycle: Lifecycle,
    /// Set once the last bytes are out; the close hooks start on the next
    /// poll, so a client that leaves before reading them is abandoned.
    ended: Option<Result<(), Fault>>,
    /// A failure that could not be written, to abort the body with.
    abort: Option<Fault>,
}

impl Pipe {
    /// The body's first bytes, if any, which the head waits for. `Err` if
    /// `refusable` and it failed before any: it is refused instead.
    async fn first(&mut self, refusable: bool) -> Result<Option<Bytes>, Fault> {
        loop {
            let (out, bytes_before_end) = self.step().await;
            // A failure before any bytes, its own or one writing its end,
            // is refused.
            if refusable
                && !bytes_before_end
                && let Some(Err(f)) = &self.ended
            {
                return Err(f.clone());
            }
            if !out.is_empty() || self.ended.is_some() {
                return Ok((!out.is_empty()).then(|| out.freeze()));
            }
        }
    }

    async fn next(&mut self) -> Option<Result<Bytes, Fault>> {
        loop {
            if let Some(end) = self.ended.take() {
                self.lifecycle.finish(end);
            }
            if let Some(f) = self.abort.take() {
                return Some(Err(f));
            }
            if self.lifecycle.is_finished() {
                return None;
            }
            let (out, _) = self.step().await;
            if !out.is_empty() {
                return Some(Ok(out.freeze()));
            }
        }
    }

    /// The next batch written, with its `Part::End` if it reached one, and
    /// whether any bytes came before that end.
    ///
    /// A panic in a decoder, a filter or an encoder ends the response as an
    /// internal fault: refused if nothing was sent, and aborted if it was.
    async fn step(&mut self) -> (BytesMut, bool) {
        let step = AssertUnwindSafe(async {
            let (mut out, end) = self.batch().await;
            // What filters release at a failure does not stop its refusal.
            let bytes_before_end = !out.is_empty();
            if let Some(end) = end {
                // Filters see every end, a Part's failure too.
                let end = match (self.flush(&end, &mut out), &end) {
                    (Ok(()), _) => end,
                    // After an error the client already has, nothing more
                    // can be said.
                    (Err(f), Err(said)) if said.is_relayed() => Err(f.relayed()),
                    (Err(f), _) => Err(f.unsaid()),
                };
                self.end(end, &mut out);
            }
            (out, bytes_before_end)
        });
        step.catch_unwind().await.unwrap_or_else(|_| {
            let fault = Fault::internal(PANICKED);
            self.ended = Some(Err(fault.clone()));
            self.abort = Some(fault);
            (BytesMut::new(), false)
        })
    }

    /// The next Parts written, and the `Part::End` they reached, not yet
    /// flushed or written: the response's own, or a failure of a Part.
    async fn batch(&mut self) -> (BytesMut, Option<Result<(), Fault>>) {
        let mut parts = self.source.next().await;
        if let Some(usage) = self.usage.take() {
            parts.insert(0, Part::Usage(usage));
        }
        // All usage up to the end counts, even after a Part that fails the
        // response.
        for part in &parts {
            match part {
                Part::Usage(u) => self.lifecycle.usage += u,
                Part::End(_) => break,
                _ => {}
            }
        }
        let mut out = BytesMut::new();
        let mut end = None;
        for mut part in parts {
            if let Part::End(own) = part {
                end = Some(own);
                break;
            }
            if let Err(f) = self.write(&mut part, &mut out) {
                end = Some(Err(f.unsaid()));
                break;
            }
        }
        (out, end)
    }

    /// Check, filter and write one Part.
    fn write(&mut self, part: &mut Part, out: &mut BytesMut) -> Result<(), Fault> {
        self.check(part)?;
        // Only the client's own format writes an item's bytes.
        if let Part::Item(item) = part
            && !self.source.own()
        {
            item.forget_raw();
        }
        self.edit(part)?;
        match self.for_encoder(part) {
            true => self.encoder.encode(part, out),
            false => Ok(()),
        }
    }

    /// Run the filters on an item, the last added first.
    fn edit(&mut self, part: &mut Part) -> Result<(), Fault> {
        if let Part::Item(item) = part {
            for filter in self.filters.iter_mut().rev() {
                filter.item(item)?;
            }
        }
        Ok(())
    }

    /// Write what the filters held back, before the response's `end`.
    fn flush(&mut self, end: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault> {
        if self.filters.is_empty() {
            return Ok(());
        }
        let usage = self.lifecycle.usage.clone();
        let outcome = Outcome {
            status: Some(self.status),
            end: end.clone(),
            usage,
        };
        let mut tail = None;
        for filter in self.filters.iter_mut().rev() {
            filter.end(&outcome, &mut tail)?;
        }
        match tail {
            Some(item) => self.encoder.encode(&Part::Item(item), out),
            None => Ok(()),
        }
    }

    /// The response ends with `end`, written by the encoder: if it cannot
    /// write it, the response fails with that fault instead, and if it
    /// cannot write that either, the body is aborted.
    fn end(&mut self, mut end: Result<(), Fault>, out: &mut BytesMut) {
        let said = matches!(&end, Err(f) if f.is_relayed());
        if said && self.source.own() {
            self.ended = Some(end);
            return;
        }
        if let Err(f) = self.encoder.encode(&Part::End(end.clone()), out) {
            end = Err(f.clone());
            if self.encoder.encode(&Part::End(end.clone()), out).is_err() {
                self.abort = Some(f);
            }
        }
        self.ended = Some(end);
    }

    /// Items are of the route's kind.
    fn check(&self, part: &Part) -> Result<(), Fault> {
        let Part::Item(item) = part else {
            return Ok(());
        };
        match self.event {
            Some(t) if t == item.event_type() => Ok(()),
            Some(_) => Err(Fault::another_kind(
                "a response held items of another kind".into(),
            )),
            None => Err(Fault::another_kind(
                "a response held items but the route has no kind".into(),
            )),
        }
    }

    /// Whether the encoder is handed `part`: Frames only in the client's own
    /// format, so never a plugin's.
    fn for_encoder(&self, part: &Part) -> bool {
        !matches!(part, Part::Frame(_)) || self.source.own()
    }
}

/// Runs the close hooks exactly once: when the response ends, or when the
/// request is dropped first.
struct Lifecycle {
    closing: Arc<HooksInFlight>,
    /// How many plugins' `on_request` started.
    ran: usize,
    /// Taken when the hooks start.
    cx: Option<Context>,
    status: Option<StatusCode>,
    usage: Usage,
    /// Where the `Outcome` goes, for whoever handed the request over.
    outcome: Option<oneshot::Sender<Outcome>>,
}

impl Lifecycle {
    fn cx(&mut self) -> &mut Context {
        self.cx
            .as_mut()
            .expect("the context lives until the close hooks start")
    }

    fn is_finished(&self) -> bool {
        self.cx.is_none()
    }

    fn finish(&mut self, end: Result<(), Fault>) {
        let ran = self.ran;
        let Some(mut cx) = self.cx.take() else {
            return;
        };
        cx.closed = true;
        let outcome = Outcome {
            status: self.status,
            end,
            usage: std::mem::take(&mut self.usage),
        };
        if let Some(to) = self.outcome.take() {
            let _ = to.send(outcome.clone());
        }
        if ran == 0 {
            return;
        }
        let guard = HooksInFlight::enter(self.closing.clone());
        let hooks = async move {
            for plugin in cx.route.chain.iter().take(ran) {
                // One plugin's panic must not cost the others their hook.
                let hook = AssertUnwindSafe(plugin.on_close(&cx, &outcome));
                let _ = hook.catch_unwind().await;
            }
            drop(guard);
        };
        match tokio::runtime::Handle::try_current() {
            Ok(rt) => drop(rt.spawn(hooks)),
            // Embedded without tokio: the hooks still run, on a thread.
            Err(_) => drop(std::thread::spawn(|| futures::executor::block_on(hooks))),
        }
    }
}

impl Drop for Lifecycle {
    fn drop(&mut self) {
        if self.is_finished() {
            return;
        }
        let fault = if std::thread::panicking() {
            Fault::internal(PANICKED)
        } else {
            Fault::abandoned()
        };
        self.finish(Err(fault));
    }
}

/// Lifecycle hooks still running, so shutdown can wait for them.
#[derive(Default)]
struct HooksInFlight {
    count: AtomicUsize,
    idle: Notify,
}

struct HookGuard(Arc<HooksInFlight>);

impl HooksInFlight {
    fn enter(closing: Arc<HooksInFlight>) -> HookGuard {
        closing.count.fetch_add(1, Ordering::SeqCst);
        HookGuard(closing)
    }
}

impl Drop for HookGuard {
    fn drop(&mut self) {
        if self.0.count.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.0.idle.notify_waiters();
        }
    }
}

/// The provider `name`, if `route` can send it the client's request:
/// defined, and speaking the route's format or another of its kind.
pub(crate) fn provider_for<'a>(
    route: &Route,
    providers: &'a HashMap<String, Provider>,
    name: &str,
) -> Result<&'a Provider, Fault> {
    let refuse = |why: String| Err(Fault::config(format!("route '{}': {why}", route.name)));
    let Some(p) = providers.get(name) else {
        return refuse(format!("unknown provider '{name}'"));
    };
    let (from, to) = (route.format.name(), p.format.name());
    if from != to && !(route.kind.is_some() && same_kind(route.kind.as_ref(), p.kind.as_ref())) {
        return refuse(format!(
            "it speaks {from}, provider '{name}' speaks {to}, and they share no kind"
        ));
    }
    Ok(p)
}

/// Connection-level headers, plus the length, which edits and reframing
/// change.
const HOP_BY_HOP: [HeaderName; 10] = [
    CONNECTION,
    CONTENT_LENGTH,
    HeaderName::from_static("keep-alive"),
    HeaderName::from_static("proxy-connection"),
    PROXY_AUTHENTICATE,
    PROXY_AUTHORIZATION,
    TE,
    TRAILER,
    TRANSFER_ENCODING,
    UPGRADE,
];

/// Headers of the provider's origin, such as cookies, which a relayed head
/// does not pass to clients.
const ORIGIN: [&str; 3] = ["set-cookie", "alt-svc", "strict-transport-security"];

/// Remove one connection's headers: [`HOP_BY_HOP`] and those `Connection`
/// names, keeping `te: trailers`, which a request such as gRPC's needs.
pub(crate) fn strip_hop_by_hop(headers: &mut http::HeaderMap) {
    let trailers = headers.get_all(TE).iter().any(|v| {
        let v = v.to_str().unwrap_or_default();
        v.split(',')
            .any(|t| t.trim().eq_ignore_ascii_case("trailers"))
    });
    let named: Vec<HeaderName> = headers
        .get_all(CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .filter_map(|name| HeaderName::from_bytes(name.trim().as_bytes()).ok())
        .collect();
    for name in named.into_iter().chain(HOP_BY_HOP) {
        headers.remove(name);
    }
    if trailers {
        headers.insert(TE, http::HeaderValue::from_static("trailers"));
    }
}

fn into_http(mut head: http::response::Parts, body: Body) -> http::Response<Body> {
    strip_hop_by_hop(&mut head.headers);
    // The client's connection sets its own version.
    head.version = http::Version::default();
    http::Response::from_parts(head, body)
}

/// Servers spawn `handle`, so it must stay `Send`.
#[cfg(test)]
fn _handle_is_send(g: &Gateway, req: http::Request<Body>) {
    fn send<T: Send>(_: T) {}
    send(g.handle("route", req));
}
