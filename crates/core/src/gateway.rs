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
use futures::future::{BoxFuture, Shared};
use futures::stream::{self, TryStreamExt};
use http::header::{
    CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, HeaderName, LOCATION, PROXY_AUTHENTICATE,
    PROXY_AUTHORIZATION, RETRY_AFTER, TE, TRAILER, TRANSFER_ENCODING, UPGRADE,
};
use http::{HeaderValue, StatusCode};
use http_body::Frame;
use http_body_util::{BodyExt, StreamBody};
use tokio::sync::Notify;

use crate::context::{Filters, HeadFn, failed};
use crate::format::AnyFormat;
use crate::response::Source;
use crate::{
    Body, Context, Doc, Encoder, Fault, Format, Outcome, Part, Plugin, Response, Transport, Usage,
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

/// A provider the gateway can call: a name, a format and a transport.
#[derive(Clone)]
pub struct Provider {
    pub(crate) name: String,
    pub(crate) format: Arc<dyn AnyFormat>,
    pub(crate) transport: Arc<dyn Transport>,
}

impl std::fmt::Debug for Provider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Provider")
            .field("name", &self.name)
            .field("format", &self.format.name())
            .finish_non_exhaustive()
    }
}

impl Provider {
    /// A provider named `name`, speaking `format` over `transport`.
    pub fn new(name: impl Into<String>, format: impl Format, transport: impl Transport) -> Self {
        Self {
            name: name.into(),
            format: Arc::new(format),
            transport: Arc::new(transport),
        }
    }
}

/// A registered plugin and the plugins it runs after, by index.
#[derive(Clone)]
struct Registered {
    name: String,
    plugin: Arc<dyn Plugin>,
    after: Vec<usize>,
}

#[derive(Clone)]
pub(crate) struct Inner {
    /// The formats served to clients, in the order they are tried.
    formats: Vec<Arc<dyn AnyFormat>>,
    /// In registration order.
    plugins: Vec<Registered>,
    /// The plugins in the order their hooks run, on both edges.
    pub(crate) chain: Vec<Arc<dyn Plugin>>,
    pub(crate) providers: HashMap<String, Provider>,
    /// The provider that answers when no plugin set a target.
    pub(crate) target: Option<Target>,
    /// Requests, calls and end hooks still running.
    pub(crate) in_flight: Arc<InFlight>,
}

/// The gateway. It has no server: an HTTP adapter or an embedding program
/// hands it requests.
///
/// Built by value, each setting checked as it is given:
/// `Gateway::new(providers)?.serve(format)?.plugin(name, plugin)?`, then
/// [`after`](Gateway::after) and [`target`](Gateway::target), all before
/// it is cloned or used.
#[derive(Clone)]
pub struct Gateway {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Gateway {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&str> = self.inner.plugins.iter().map(|p| &*p.name).collect();
        f.debug_struct("Gateway")
            .field("formats", &self.inner.formats.len())
            .field("plugins", &names)
            .field("providers", &self.inner.providers.len())
            .field("target", &self.inner.target)
            .finish()
    }
}

impl Gateway {
    /// A gateway with `providers` and nothing else. `Err` with
    /// [`codes::CONFIG`](crate::codes::CONFIG) if two providers share a
    /// name, or a format breaks a rule of [`Gateway::serve`].
    pub fn new(providers: impl IntoIterator<Item = Provider>) -> Result<Gateway, Fault> {
        let mut gateway = Gateway {
            inner: Arc::new(Inner {
                formats: Vec::new(),
                plugins: Vec::new(),
                chain: Vec::new(),
                providers: HashMap::new(),
                target: None,
                in_flight: Arc::default(),
            }),
        };
        for p in providers {
            gateway.check_format(&*p.format)?;
            let inner = gateway.inner_mut();
            if let Some(p) = inner.providers.insert(p.name.clone(), p) {
                return Err(Fault::config(format!(
                    "provider '{}' is defined twice",
                    p.name
                )));
            }
        }
        Ok(gateway)
    }

    /// Serve clients of `format`: a request goes to the first format served
    /// that [`accepts`](Format::accepts) it. `Err` with
    /// [`codes::CONFIG`](crate::codes::CONFIG) if the format has one type
    /// for both its input and output, or shares its name with a format of
    /// another kind.
    pub fn serve(mut self, format: impl Format) -> Result<Gateway, Fault> {
        self.check_format(&format)?;
        self.inner_mut().formats.push(Arc::new(format));
        Ok(self)
    }

    /// Register `plugin` as `name`. Its hooks run on every request and
    /// call, in the gateway's order ([`after`](Gateway::after)). `Err` with
    /// [`codes::CONFIG`](crate::codes::CONFIG) if the name is taken.
    pub fn plugin(
        mut self,
        name: impl Into<String>,
        plugin: impl Plugin,
    ) -> Result<Gateway, Fault> {
        let name = name.into();
        if self.index(&name).is_ok() {
            return Err(Fault::config(format!(
                "plugin '{name}' is registered twice"
            )));
        }
        let inner = self.inner_mut();
        let plugin = Arc::new(plugin);
        inner.plugins.push(Registered {
            name,
            plugin,
            after: Vec::new(),
        });
        inner.order();
        Ok(self)
    }

    /// Run plugin `name` after each of `before`, on both edges. Next in
    /// order is always the earliest registered plugin whose `after` plugins
    /// have all run: registered `a`, `b`, `c` with `a` after `c` run `b`,
    /// `c`, `a`. `Err` with [`codes::CONFIG`](crate::codes::CONFIG) for an
    /// unregistered name or a cycle.
    pub fn after<S: AsRef<str>>(
        mut self,
        name: &str,
        before: impl IntoIterator<Item = S>,
    ) -> Result<Gateway, Fault> {
        let at = self.index(name)?;
        for first in before {
            let first = first.as_ref();
            let first = self.index(first).map_err(|_| {
                Fault::config(format!(
                    "plugin '{name}' runs after unknown plugin '{first}'"
                ))
            })?;
            let plugins = &self.inner.plugins;
            if first == at || runs_after(plugins, first, at) {
                return Err(Fault::config(format!(
                    "plugin '{name}' cannot run after '{}', which runs after it",
                    plugins[first].name
                )));
            }
            self.inner_mut().plugins[at].after.push(first);
        }
        self.inner_mut().order();
        Ok(self)
    }

    /// The provider that answers if every plugin passes and none set a
    /// target ([`Context::set_target`]). `Err` with
    /// [`codes::CONFIG`](crate::codes::CONFIG) for an unknown provider. A
    /// request that cannot use it is refused as [`Context::call`] says.
    pub fn target(mut self, provider: impl Into<Target>) -> Result<Gateway, Fault> {
        let target = provider.into();
        self.check_target(target.name())?;
        self.inner_mut().target = Some(target);
        Ok(self)
    }

    /// `Ok` if the gateway has `provider`, else `Err` with
    /// [`codes::CONFIG`](crate::codes::CONFIG), such as to check targets a
    /// router may set. Whether a request can use it is known per request.
    pub fn check_target(&self, provider: impl Into<Target>) -> Result<(), Fault> {
        let name = provider.into();
        match self.inner.providers.contains_key(name.name()) {
            true => Ok(()),
            false => Err(Fault::config(format!("unknown provider '{}'", name.name()))),
        }
    }

    /// Resolves once every request handed to this gateway has ended, its
    /// response included, and every end hook has finished. Await it on
    /// shutdown or after a config swap so no end hook is lost, under a
    /// deadline of your own. A gateway changed after being cloned is a new
    /// one and waits only for its own requests.
    pub async fn idle(&self) {
        self.inner.in_flight.idle().await;
    }

    /// Run `req` in the first format served that accepts it
    /// ([`Format::accepts`]); plugins see its head as [`Context::head`].
    /// Remove the gateway's own credentials first, here or in a plugin
    /// ([`Context::head_mut`]), as formats may pass client headers on.
    ///
    /// A request no format accepts gets a plain-text 404, and a format
    /// panic before it can write gets a plain-text 500; everything else is
    /// in the client's format. The body is read whole, so the caller bounds
    /// it. Before any plugin runs, a body that fails while read is refused
    /// with its fault, and one whose model is read from JSON nested too
    /// deeply ([`Doc::json`]) with
    /// [`codes::BAD_REQUEST`](crate::codes::BAD_REQUEST). A 204, a 304 or
    /// an answer to `HEAD` is sent with no body. The response body is
    /// `Send`, not `Sync`.
    pub async fn handle<B>(&self, req: http::Request<B>) -> http::Response<Body>
    where
        B: http_body::Body<Data = Bytes> + Send + 'static,
        B::Error: Into<Fault>,
    {
        // In flight until its end hooks finish, for `idle`.
        let _live = InFlight::enter(self.inner.in_flight.clone());
        let (head, body) = req.into_parts();
        // A format that panics in `accepts` gets a plain refusal.
        let formats = &self.inner.formats;
        let accepts = || formats.iter().find(|f| f.accepts(&head)).cloned();
        let format = match std::panic::catch_unwind(AssertUnwindSafe(accepts)) {
            Ok(Some(format)) => format,
            Ok(None) => {
                let (method, path) = (&head.method, head.uri.path());
                let text = format!("no format served accepts {method} {path}");
                return closed(plain(StatusCode::NOT_FOUND, text), None);
            }
            Err(_) => {
                let status = StatusCode::INTERNAL_SERVER_ERROR;
                return closed(plain(status, PANICKED.into()), None);
            }
        };
        let (body, broken) = match body.map_err(Into::into).collect().await {
            Ok(collected) => (Doc::from(collected.to_bytes()), None),
            Err(fault) => (Doc::default(), Some(fault)),
        };
        // No format reads a body that broke off. A format that panics
        // reading the model gets a plain refusal.
        let model = || {
            broken
                .is_none()
                .then(|| format.model(&head, &body))
                .flatten()
        };
        let Ok(model) = std::panic::catch_unwind(AssertUnwindSafe(model)) else {
            return closed(
                plain(StatusCode::INTERNAL_SERVER_ERROR, PANICKED.into()),
                None,
            );
        };
        let deep = body.too_deep();
        let deep = deep.then(|| Fault::bad_request("the request body nests JSON too deeply"));
        let cx = Context::new(format.clone(), head, body, model, self.inner.clone());
        let format = &*format;
        let (outcome, ended) = oneshot::channel();
        let mut lifecycle = EndHooks::new(&self.inner.in_flight, cx, Some(outcome));
        let resp = match broken.or(deep) {
            Some(fault) => refuse(format, fault, lifecycle).await,
            None => {
                // A plugin panic is refused like a failure.
                let ran = AssertUnwindSafe(run(&self.inner, &mut lifecycle)).catch_unwind();
                let ran = ran.await;
                // Its plugins may not read its calls' responses from here.
                lifecycle.cx().open.answered();
                match ran {
                    Ok(Ok(resp)) => commit(format, resp, lifecycle).await,
                    Ok(Err(f)) => refuse(format, f, lifecycle).await,
                    Err(_) => refuse(format, Fault::internal(PANICKED), lifecycle).await,
                }
            }
        };
        closed(resp, Some(ended))
    }

    /// The settings, for a by-value method to change. A gateway changed
    /// after being cloned is a new one, with its own requests.
    fn inner_mut(&mut self) -> &mut Inner {
        if Arc::strong_count(&self.inner) > 1 {
            let mut inner = (*self.inner).clone();
            inner.in_flight = Arc::default();
            self.inner = Arc::new(inner);
        }
        Arc::make_mut(&mut self.inner)
    }

    /// The registration index of plugin `name`.
    fn index(&self, name: &str) -> Result<usize, Fault> {
        let plugins = &self.inner.plugins;
        plugins
            .iter()
            .position(|p| p.name == name)
            .ok_or_else(|| Fault::config(format!("unknown plugin '{name}'")))
    }

    /// `Ok` if `format`'s two types differ and formats of its name share
    /// its kind.
    fn check_format(&self, format: &dyn AnyFormat) -> Result<(), Fault> {
        let kind = format.kind();
        let name = format.name();
        if kind.is_some_and(|(input, output)| input == output) {
            return Err(Fault::config(format!(
                "the kind of format {name} has one type for both directions"
            )));
        }
        let inner = &self.inner;
        let providers = inner.providers.values().map(|p| &*p.format);
        let mut known = inner.formats.iter().map(|f| &**f).chain(providers);
        if known.any(|f| f.name() == name && f.kind() != kind) {
            return Err(Fault::config(format!(
                "two formats named {name} are of different kinds"
            )));
        }
        Ok(())
    }
}

impl Inner {
    /// Derive the chain: at each step, the earliest registered plugin whose
    /// `after` plugins are placed. The registry has no cycle.
    fn order(&mut self) {
        let plugins = &self.plugins;
        let mut placed = vec![false; plugins.len()];
        let mut chain = Vec::with_capacity(plugins.len());
        while chain.len() < plugins.len() {
            let ready = (0..plugins.len())
                .find(|&i| !placed[i] && plugins[i].after.iter().all(|&a| placed[a]));
            let Some(i) = ready else { break };
            placed[i] = true;
            chain.push(plugins[i].plugin.clone());
        }
        self.chain = chain;
    }
}

/// Whether plugin `later` must run after plugin `first`, directly or not.
fn runs_after(plugins: &[Registered], later: usize, first: usize) -> bool {
    let mut stack = vec![later];
    let mut seen = vec![false; plugins.len()];
    while let Some(i) = stack.pop() {
        if i == first {
            return true;
        }
        if !std::mem::replace(&mut seen[i], true) {
            stack.extend(&plugins[i].after);
        }
    }
    false
}

/// A future of how a request handed to [`Gateway::handle`] ended, in its
/// response's extensions. Resolves to the [`Outcome`] once the response
/// ends, or `None` if no format accepted the request or its format
/// panicked before a response.
#[derive(Clone)]
pub struct Closed(Shared<oneshot::Receiver<Outcome>>);

impl std::fmt::Debug for Closed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Closed").finish_non_exhaustive()
    }
}

impl Closed {
    /// The `Closed` of a response from [`Gateway::handle`]; for any other,
    /// one that resolves to `None`. Await it after reading the body. It
    /// resolves before end hooks finish ([`Gateway::idle`] waits for them).
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

/// `resp` with its [`Closed`]; without `ended`, it resolves to `None`.
fn closed(
    mut resp: http::Response<Body>,
    ended: Option<oneshot::Receiver<Outcome>>,
) -> http::Response<Body> {
    let ended = ended.unwrap_or_else(|| oneshot::channel().1);
    resp.extensions_mut().insert(Closed(ended.shared()));
    resp
}

pub(crate) const PANICKED: &str = "a plugin or format panicked";

/// A plain-text response, for what no format can write.
fn plain(status: StatusCode, text: String) -> http::Response<Body> {
    let mut head = crate::part::head(status);
    let plain = HeaderValue::from_static("text/plain; charset=utf-8");
    head.headers.insert(CONTENT_TYPE, plain);
    into_http(head, crate::body(text))
}

async fn run(gateway: &Inner, lifecycle: &mut EndHooks<Context>) -> Result<Response, Fault> {
    for plugin in &gateway.chain {
        lifecycle.ran += 1;
        if let Some(resp) = plugin.on_client(lifecycle.cx()).await? {
            return Ok(resp);
        }
    }
    let cx = lifecycle.cx();
    match cx.target.clone() {
        Some(target) => cx.call(target).await,
        None => Err(match cx.model() {
            Some(model) => Fault::not_found(format!("nothing answers model '{model}'")),
            None => Fault::not_found("nothing answers this request"),
        }
        .with_source("no target: the gateway has none, and no plugin set one")),
    }
}

/// A refusal: a response that fails at once, in the client's format.
async fn refuse(
    format: &dyn AnyFormat,
    fault: Fault,
    lifecycle: EndHooks<Context>,
) -> http::Response<Body> {
    let status = match fault.status() {
        s if is_error(s) => s,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    let parts = stream::iter([Part::End(Err(fault))]);
    commit(format, Response::made(status, parts), lifecycle).await
}

/// Whether `status` is an error: 4xx or 5xx.
fn is_error(status: StatusCode) -> bool {
    status.is_client_error() || status.is_server_error()
}

/// Send `resp`. The head waits for the first bytes, so a success that fails
/// before any is sent as a refusal with the fault's status. Head edits
/// apply to the outgoing head, whose status the `Outcome` gets.
async fn commit(
    format: &dyn AnyFormat,
    resp: Response,
    mut lifecycle: EndHooks<Context>,
) -> http::Response<Body> {
    let cx = lifecycle.cx();
    if let Source::Bytes { call: Some(c), .. } = &resp.body {
        // A call's response goes only to its own request, whose format and
        // end hooks it belongs to.
        if !cx.open.holds(c) {
            drop(resp);
            let fault = Fault::internal("a response to another request was sent");
            return Box::pin(refuse(format, fault, lifecycle)).await;
        }
        // The call ends as this request's response does.
        cx.committed = Some(c.clone());
    }
    let output = format.kind().map(|(_, output)| output);
    // A response of another kind fails, even with no events.
    if resp.event.is_some() && resp.event != output {
        drop(resp);
        let fault = Fault::another_kind("a response of another kind was sent".into());
        return Box::pin(refuse(format, fault, lifecycle)).await;
    }
    let client = cx.view();
    // Only a refusal is made of Parts of no kind.
    let refusal = matches!(resp.body, Source::Parts(_)) && resp.event.is_none();
    let (mut head, body, usage) = (resp.head, resp.body, resp.usage);
    let translated = matches!(body, Source::Bytes { relayed: false, .. });
    // A translated redirect would lose its `Location`, so it is refused.
    let redirect = matches!(head.status.as_u16(), 301 | 302 | 303 | 307 | 308)
        && head.headers.contains_key(LOCATION);
    if translated && redirect {
        let fault = Fault::upstream(format!("the provider answered {}", head.status.as_u16()));
        return Box::pin(refuse(format, fault, lifecycle)).await;
    }
    strip_named(&mut head.headers);
    // A provider's origin headers stay with it; a plugin's are its own.
    if matches!(body, Source::Bytes { .. }) {
        for name in ORIGIN {
            head.headers.remove(name);
        }
    }
    let build = || {
        let source = match body {
            Source::Raw(body) => Source::Bytes {
                decoder: format.decoder(&head, client),
                body,
                relayed: true,
                call: None,
            },
            source => source,
        };
        if translated {
            // Translated, only the status and `Retry-After` are kept.
            let retry = head.headers.remove(RETRY_AFTER);
            head.headers.clear();
            head.headers.extend(retry.map(|v| (RETRY_AFTER, v)));
        }
        // Judged before the encoder touches the head: only a success that
        // fails before any bytes becomes a refusal.
        let refusable = head.status.is_success();
        // The encoder follows the head the Parts are for, before the client
        // edge's edits, which change what the client is told, not the Parts.
        let encoder = format.encoder(&mut head, client, source.relayed());
        (source, encoder, refusable)
    };
    // A format that panics building its codecs cannot write a refusal.
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
        event: output,
        filters,
        status: head.status,
        encoder,
        lifecycle,
        held: None,
        ended: None,
        abort: None,
    };
    let first = match pipe.first(refusable).await {
        Ok(first) => first,
        Err(fault) => return Box::pin(refuse(format, fault, pipe.lifecycle)).await,
    };
    let before = head.clone();
    // Each edit changes a copy, so a panicking one changes nothing. The
    // head has not gone out, so the request is refused with the other
    // edits replayed onto the refusal.
    let mut panicked = false;
    for edit in pipe.lifecycle.cx().take_head_edits().into_iter().rev() {
        let mut edited = head.clone();
        match std::panic::catch_unwind(AssertUnwindSafe(|| edit(&mut edited))) {
            Ok(()) => head = edited,
            Err(_) => panicked = true,
        }
    }
    if panicked {
        pipe.lifecycle.cx().edit_head(replay(&before, head));
        let fault = Fault::internal(PANICKED);
        return Box::pin(refuse(format, fault, pipe.lifecycle)).await;
    }
    // A refusal keeps its status unless an edit makes it another error.
    if refusal && !is_error(head.status) {
        head.status = before.status;
    }
    pipe.lifecycle.status = Some(head.status);
    let head_only = pipe.lifecycle.cx().head().method == http::Method::HEAD;
    match first {
        Some(first) => pipe.held = Some(first),
        // With no bytes, it ends as its head goes out.
        None => pipe.tell_end(),
    }
    // A host never polls a bodiless response, so it is read to its end
    // here, which runs its end hooks.
    if head_only || matches!(head.status.as_u16(), 204 | 304) {
        while pipe.next().await.is_some() {}
        return into_http(head, crate::body(Bytes::new()));
    }
    let body = stream::unfold(pipe, |mut p| async move { p.next().await.map(|b| (b, p)) });
    into_http(
        head,
        StreamBody::new(body.map_ok(Frame::data)).boxed_unsync(),
    )
}

/// An edit that applies to another head what turned `before` into
/// `after`: the status and headers that changed.
fn replay(before: &http::response::Parts, after: http::response::Parts) -> HeadFn {
    let status = (after.status != before.status).then_some(after.status);
    let changed = |name: &HeaderName| {
        let (now, then) = (after.headers.get_all(name), before.headers.get_all(name));
        !now.iter().eq(then.iter())
    };
    let names: Vec<HeaderName> = after
        .headers
        .keys()
        .filter(|n| changed(n))
        .cloned()
        .collect();
    Box::new(move |head| {
        if let Some(status) = status {
            head.status = status;
        }
        for name in names {
            head.headers.remove(&name);
            for value in after.headers.get_all(&name) {
                head.headers.append(&name, value.clone());
            }
        }
    })
}

/// Turns the committed response into the client's body. Filters see every
/// item; the encoder sees the client format's own Parts, or items without
/// provider bytes, plus the usage.
struct Pipe {
    source: Source,
    /// Usage the response was given, reported first.
    usage: Option<Usage>,
    /// The output type of the client's format, if it has a kind.
    event: Option<TypeId>,
    filters: Filters,
    /// The response's status, before head edits.
    status: StatusCode,
    encoder: Box<dyn Encoder>,
    lifecycle: EndHooks<Context>,
    /// The first bytes, written before the head went out.
    held: Option<Bytes>,
    /// Set once the end is written; told as its last bytes are handed out,
    /// so a client that leaves first ends abandoned.
    ended: Option<Result<(), Fault>>,
    /// A failure that could not be written, to abort the body with.
    abort: Option<Fault>,
}

impl Pipe {
    /// The body's first bytes, if any, which the head waits for. `Err`, to
    /// refuse it, if it failed before any and is `refusable`, or panicked.
    async fn first(&mut self, refusable: bool) -> Result<Option<Bytes>, Fault> {
        loop {
            let Ok((out, bytes_before_end)) = self.step().await else {
                return Err(Fault::internal(PANICKED));
            };
            // A failure before any bytes is refused.
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
            let out = match self.held.take() {
                Some(out) => out,
                None if self.abort.is_some() => return self.abort.take().map(Err),
                None if self.lifecycle.is_finished() => return None,
                None => match self.step().await {
                    Ok((out, _)) | Err(out) => out.freeze(),
                },
            };
            self.tell_end();
            if !out.is_empty() {
                return Some(Ok(out));
            }
        }
    }

    /// Start the end hooks, if the end is written.
    fn tell_end(&mut self) {
        if let Some(end) = self.ended.take() {
            self.lifecycle.finish(end);
        }
    }

    /// The next batch written, and whether any bytes came before an end.
    /// `Err` with the bytes written before a decoder, filter or encoder
    /// panicked; the response then ends internal: refused if nothing was
    /// sent, else aborted after those bytes.
    async fn step(&mut self) -> Result<(BytesMut, bool), BytesMut> {
        let mut out = BytesMut::new();
        match AssertUnwindSafe(self.write(&mut out)).catch_unwind().await {
            Ok(bytes_before_end) => Ok((out, bytes_before_end)),
            Err(_) => {
                let fault = Fault::internal(PANICKED);
                self.ended = Some(Err(fault.clone()));
                self.abort = Some(fault);
                Err(out)
            }
        }
    }

    /// Write the next Parts to `out`, and at the end what filters held and
    /// the end. Returns whether bytes came before the end.
    async fn write(&mut self, out: &mut BytesMut) -> bool {
        let end = self.batch(out).await;
        // What filters release at a failure does not prevent its refusal.
        let bytes_before_end = !out.is_empty();
        if let Some(end) = end {
            // Filters see every end, a Part's failure too.
            let end = self.flush(end, out);
            self.end(end, out);
        }
        bytes_before_end
    }

    /// Write the next Parts to `out`, returning the end reached, if any,
    /// unwritten: the response's own or a Part's failure.
    async fn batch(&mut self, out: &mut BytesMut) -> Option<Result<(), Fault>> {
        let mut parts: Vec<Part> = self.usage.take().map(Part::Usage).into_iter().collect();
        self.source.next(&mut parts).await;
        self.lifecycle.count(&parts);
        for mut part in parts {
            if let Part::End(own) = part {
                return Some(own);
            }
            if let Err(f) = self.part(&mut part, out) {
                return Some(Err(f.unsaid()));
            }
        }
        None
    }

    /// Check, filter and write one Part.
    fn part(&mut self, part: &mut Part, out: &mut BytesMut) -> Result<(), Fault> {
        self.check(part)?;
        // Only the client's own format writes an item's bytes.
        if let Part::Item(item) = part
            && !self.source.relayed()
        {
            item.forget_raw();
        }
        if let Part::Item(item) = part {
            self.filters.item(item)?;
        }
        match self.for_encoder(part) {
            true => encode(&mut *self.encoder, part, out),
            false => Ok(()),
        }
    }

    /// Write what the filters held back before `end`; returns the end,
    /// failed if that fails.
    fn flush(&mut self, end: Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault> {
        let (tail, end) = self
            .filters
            .end(Some(self.status), end, &self.lifecycle.usage);
        match tail.map(|item| encode(&mut *self.encoder, &Part::Item(item), out)) {
            Some(Err(f)) => failed(f, &end),
            _ => end,
        }
    }

    /// Write `end`. If the encoder cannot, the response fails with its
    /// fault; if it cannot write that either, the body is aborted.
    fn end(&mut self, mut end: Result<(), Fault>, out: &mut BytesMut) {
        let said = matches!(&end, Err(f) if f.is_said());
        if said && self.source.relayed() {
            self.ended = Some(end);
            return;
        }
        let encoder = &mut *self.encoder;
        if let Err(f) = encode(encoder, &Part::End(end.clone()), out) {
            end = Err(f.clone());
            if encode(encoder, &Part::End(end.clone()), out).is_err() {
                self.abort = Some(f);
            }
        }
        self.ended = Some(end);
    }

    /// Items are of the client's kind.
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
                "a response held items but the client's format has no kind".into(),
            )),
        }
    }

    /// Whether the encoder gets `part`: Frames only when relayed.
    fn for_encoder(&self, part: &Part) -> bool {
        !matches!(part, Part::Frame(_)) || self.source.relayed()
    }
}

/// Write `part` to `out`, or nothing if the encoder fails or panics.
fn encode(encoder: &mut dyn Encoder, part: &Part, out: &mut BytesMut) -> Result<(), Fault> {
    let mut one = BytesMut::new();
    encoder.encode(part, &mut one)?;
    out.unsplit(one);
    Ok(())
}

/// The context an edge's hooks get: [`Context`] on the client edge,
/// [`ProviderContext`](crate::ProviderContext) on the provider edge.
pub(crate) trait Edge: Send + Sync + 'static {
    /// The plugins whose hooks run.
    fn chain(&self) -> &[Arc<dyn Plugin>];

    /// `plugin`'s end hook on this edge.
    fn end<'a>(&'a self, plugin: &'a dyn Plugin, outcome: &'a Outcome) -> BoxFuture<'a, ()>;

    /// The end hooks start, for an edge ending `end`.
    fn close(&mut self, _end: &Result<(), Fault>) {}

    /// End hooks that must finish before this edge's start.
    fn after(&self) -> Option<&InFlight> {
        None
    }
}

/// Runs one edge's end hooks exactly once, for every plugin whose opening
/// hook started: when told how it ended, or as abandoned if dropped first.
pub(crate) struct EndHooks<E: Edge> {
    /// Held until its end hooks finish, for [`Gateway::idle`].
    live: Option<InFlightGuard>,
    /// How many plugins' opening hook started.
    pub(crate) ran: usize,
    /// Taken when the hooks start.
    cx: Option<E>,
    pub(crate) status: Option<StatusCode>,
    pub(crate) usage: Usage,
    /// Also receives the `Outcome`, for [`Closed`].
    report: Option<oneshot::Sender<Outcome>>,
}

impl<E: Edge> EndHooks<E> {
    pub(crate) fn new(
        in_flight: &Arc<InFlight>,
        cx: E,
        report: Option<oneshot::Sender<Outcome>>,
    ) -> Self {
        Self {
            live: Some(InFlight::enter(in_flight.clone())),
            ran: 0,
            cx: Some(cx),
            status: None,
            usage: Usage::default(),
            report,
        }
    }

    pub(crate) fn cx(&mut self) -> &mut E {
        self.cx
            .as_mut()
            .expect("the context lives until the end hooks start")
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.cx.is_none()
    }

    /// Count the usage in `parts` up to their end, including after a failed
    /// Part.
    pub(crate) fn count(&mut self, parts: &[Part]) {
        for part in parts {
            match part {
                Part::Usage(u) => self.usage += u,
                Part::End(_) => break,
                _ => {}
            }
        }
    }

    /// Start the end hooks, once, detached, with an `Outcome` ending `end`.
    pub(crate) fn finish(&mut self, end: Result<(), Fault>) {
        let Some(mut cx) = self.cx.take() else {
            return;
        };
        cx.close(&end);
        let outcome = Outcome {
            status: self.status,
            end,
            usage: std::mem::take(&mut self.usage),
        };
        if let Some(to) = self.report.take() {
            let _ = to.send(outcome.clone());
        }
        let (ran, live) = (self.ran, self.live.take());
        if ran == 0 {
            return;
        }
        detach(async move {
            if let Some(first) = cx.after() {
                first.idle().await;
            }
            for plugin in cx.chain().iter().take(ran) {
                // One plugin's panic must not cost the others their hook.
                let hook = AssertUnwindSafe(cx.end(&**plugin, &outcome));
                let _ = hook.catch_unwind().await;
            }
            drop(live);
        });
    }
}

/// Dropped unfinished, it ends abandoned, even during a panic's unwinding.
impl<E: Edge> Drop for EndHooks<E> {
    fn drop(&mut self) {
        self.finish(Err(Fault::abandoned()));
    }
}

/// Run end hooks detached: on the current tokio runtime, else a thread.
fn detach(hooks: impl Future<Output = ()> + Send + 'static) {
    match tokio::runtime::Handle::try_current() {
        Ok(rt) => drop(rt.spawn(hooks)),
        Err(_) => drop(std::thread::spawn(|| futures::executor::block_on(hooks))),
    }
}

/// A count of running work, such as requests and end hooks, to wait on.
#[derive(Default)]
pub(crate) struct InFlight {
    count: AtomicUsize,
    idle: Notify,
}

/// One entry in [`InFlight`], left when dropped.
pub(crate) struct InFlightGuard(Arc<InFlight>);

impl InFlight {
    pub(crate) fn enter(in_flight: Arc<InFlight>) -> InFlightGuard {
        in_flight.count.fetch_add(1, Ordering::SeqCst);
        InFlightGuard(in_flight)
    }

    /// Resolves once none is left.
    async fn idle(&self) {
        loop {
            let idle = self.idle.notified();
            if self.count.load(Ordering::SeqCst) == 0 {
                return;
            }
            idle.await;
        }
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        if self.0.count.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.0.idle.notify_waiters();
        }
    }
}

/// The provider `name`, if it exists and speaks `format` or another format
/// of its kind.
pub(crate) fn provider_for<'a>(
    format: &dyn AnyFormat,
    providers: &'a HashMap<String, Provider>,
    name: &str,
) -> Result<&'a Provider, Fault> {
    let Some(p) = providers.get(name) else {
        return Err(Fault::config(format!("unknown provider '{name}'")));
    };
    let (from, to) = (format.name(), p.format.name());
    let kind = format.kind();
    if from != to && (kind.is_none() || kind != p.format.kind()) {
        return Err(Fault::config(format!(
            "the client speaks {from}, provider '{name}' speaks {to}, and they share no kind"
        )));
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

/// The provider origin's headers, which never reach clients.
const ORIGIN: [&str; 3] = ["set-cookie", "alt-svc", "strict-transport-security"];

/// Remove the headers `Connection` names, except `TE`, which
/// [`HOP_BY_HOP`] covers.
pub(crate) fn strip_named(headers: &mut http::HeaderMap) {
    let named: Vec<HeaderName> = headers
        .get_all(CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .filter_map(|name| HeaderName::from_bytes(name.trim().as_bytes()).ok())
        .filter(|name| name != TE)
        .collect();
    for name in named {
        headers.remove(name);
    }
}

/// Remove [`HOP_BY_HOP`].
pub(crate) fn strip_hop_by_hop(headers: &mut http::HeaderMap) {
    for name in HOP_BY_HOP {
        headers.remove(name);
    }
}

/// The response to the client, without connection headers.
fn into_http(mut head: http::response::Parts, body: Body) -> http::Response<Body> {
    strip_hop_by_hop(&mut head.headers);
    // The client's connection sets its own version.
    head.version = http::Version::default();
    http::Response::from_parts(head, body)
}

/// Servers spawn `handle`, so it must be `Send`.
#[cfg(test)]
fn _handle_is_send(g: &Gateway, req: http::Request<Body>) {
    fn send<T: Send>(_: T) {}
    send(g.handle(req));
}
