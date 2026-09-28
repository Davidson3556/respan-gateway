use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use bytes::Bytes;
use futures::FutureExt;
use futures::channel::oneshot;
use futures::future::BoxFuture;
use http_body_util::BodyExt;

use crate::deliver::{Handed, deliver, refuse};
use crate::fault::PANICKED;
use crate::format::AnyFormat;
use crate::input::Later;
use crate::lifecycle::{EndHooks, InFlight};
use crate::provider::Provider;
use crate::{Body, ClientContext, Fault, Format, Outcome, Plugin, Response, Transport};

/// A registered plugin and the plugins it runs after, by index.
#[derive(Clone)]
struct Registered {
    name: String,
    plugin: Arc<dyn Plugin>,
    after: Vec<usize>,
}

#[derive(Clone, Default)]
pub(crate) struct Inner {
    /// The formats served to clients, in the order they are tried.
    formats: Vec<Arc<dyn AnyFormat>>,
    /// In registration order.
    plugins: Vec<Registered>,
    /// The plugins in the order their hooks run, on both edges.
    pub(crate) chain: Vec<Arc<dyn Plugin>>,
    pub(crate) providers: HashMap<String, Provider>,
    /// The provider called if no plugin sets a target.
    pub(crate) target: Option<String>,
    /// Requests, calls and end hooks still running.
    pub(crate) in_flight: Arc<InFlight>,
}

/// The gateway. It has no server: an adapter or an embedding program hands
/// it requests, and owns the rules of their transport. Built by value, each setting checked as it is given:
/// `Gateway::new().provider(name, format, transport)?.serve(format)?.plugin(name, plugin)?`,
/// then [`after`](Gateway::after) and [`target`](Gateway::target).
#[derive(Clone, Default)]
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
    /// A gateway with nothing set.
    pub fn new() -> Gateway {
        Gateway::default()
    }

    /// Add provider `name`, speaking `format` over `transport`. `Err` with
    /// [`codes::CONFIG`](crate::codes::CONFIG) if the name is taken, or the
    /// format breaks [`Gateway::serve`]'s rules.
    pub fn provider(
        mut self,
        name: impl Into<String>,
        format: impl Format,
        transport: impl Transport,
    ) -> Result<Gateway, Fault> {
        let name = name.into();
        self.check_format(&format)?;
        if self.inner.providers.contains_key(&name) {
            return Err(Fault::config(format!("provider '{name}' is defined twice")));
        }
        let format = Arc::new(format);
        let transport = Arc::new(transport);
        let provider = Provider { format, transport };
        self.inner_mut().providers.insert(name, provider);
        Ok(self)
    }

    /// Serve clients of `format`: a request goes to the first format served
    /// that [`accepts`](Format::accepts) it. `Err` with
    /// [`codes::CONFIG`](crate::codes::CONFIG) if its input and output types
    /// are one, or a format of its name is of another kind.
    pub fn serve(mut self, format: impl Format) -> Result<Gateway, Fault> {
        self.check_format(&format)?;
        self.inner_mut().formats.push(Arc::new(format));
        Ok(self)
    }

    /// Register `plugin` as `name`, its hooks run on every request and call
    /// in the gateway's order. `Err` with
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

    /// The provider called if every plugin passes and none set a target
    /// ([`ClientContext::set_target`]), replacing any set before. A request
    /// it cannot take is refused with
    /// [`codes::CONFIG`](crate::codes::CONFIG), as [`ClientContext::call`]
    /// fails. `Err` with [`codes::CONFIG`](crate::codes::CONFIG) for an
    /// unknown provider.
    pub fn target(mut self, provider: impl Into<String>) -> Result<Gateway, Fault> {
        let target = provider.into();
        if !self.inner.providers.contains_key(&target) {
            return Err(Fault::config(format!("unknown provider '{target}'")));
        }
        self.inner_mut().target = Some(target);
        Ok(self)
    }

    /// Resolves once every request handed to this gateway has ended, its
    /// response included, and every end hook has finished: await it on
    /// shutdown, under a deadline of your own. A gateway changed after being
    /// cloned is a new one with its own requests.
    pub async fn idle(&self) {
        self.inner.in_flight.idle().await;
    }

    /// Run `req` in the first format served that accepts it. The adapter
    /// owns the transport's rules: it takes out what no plugin or format
    /// should see or pass on, such as the gateway's own credentials, before
    /// this, and what a hop must not carry from the response after.
    ///
    /// If `streaming`, the request's inputs stream: its body's data frames,
    /// split by the adapter, are inputs as they arrive, and its response's
    /// head goes out at once, as its client waits for it; for a WebSocket,
    /// the adapter picks each message's type from the subprotocol and turns
    /// a body's `Err` into a close code. Else its body is read in full, so
    /// the caller bounds it; one that fails while read is refused. The
    /// response body is `Send`, not `Sync`.
    ///
    /// `Err` for what no format can write, for the adapter to write in
    /// plain form: [`codes::NOT_FOUND`](crate::codes::NOT_FOUND) if no
    /// format accepts the request, or
    /// [`codes::INTERNAL`](crate::codes::INTERNAL) if a format panicked or
    /// could not write a refusal. All else is in the client's format.
    ///
    /// It comes with a future of the request's [`Outcome`], which resolves
    /// once the response ends, before end hooks finish: await it after
    /// reading the body. Call it on a tokio runtime, which runs the end
    /// hooks; it panics outside one.
    pub async fn handle<B>(
        &self,
        req: http::Request<B>,
        streaming: bool,
    ) -> (
        Result<http::Response<Body>, Fault>,
        BoxFuture<'static, Outcome>,
    )
    where
        B: http_body::Body<Data = Bytes> + Send + 'static,
        B::Error: Into<Fault>,
    {
        let runtime = tokio::runtime::Handle::current();
        let (head, body) = req.into_parts();
        // A format that panics in `accepts` is the adapter's to write.
        let formats = &self.inner.formats;
        let accepts = || formats.iter().find(|f| f.accepts(&head)).cloned();
        let format = match std::panic::catch_unwind(AssertUnwindSafe(accepts)) {
            Ok(Some(format)) => format,
            Ok(None) => {
                let (method, path) = (&head.method, head.uri.path());
                let text = format!("no format served accepts {method} {path}");
                return unwritten(Fault::not_found(text));
            }
            Err(_) => return unwritten(Fault::internal(PANICKED)),
        };
        let body = body.map_err(Into::into);
        let (body, later, broken) = match streaming {
            true => (None, Some(Later::new(body.boxed_unsync())), None),
            false => match body.collect().await {
                Ok(collected) => (Some(collected.to_bytes()), None, None),
                Err(fault) => (Some(Bytes::new()), None, Some(fault)),
            },
        };
        let inner = self.inner.clone();
        let cx = ClientContext::new(format.clone(), head, body, later, runtime, inner);
        let format = &format;
        let (outcome, ended) = oneshot::channel();
        let settling = cx.settling.clone();
        let mut hooks = EndHooks::new(&self.inner.in_flight, cx, Some(outcome));
        // The request's end hooks run after its calls'.
        hooks.after = Some(settling);
        let resp = match broken {
            Some(fault) => refuse(format, fault, hooks).await,
            None => {
                // A plugin panic is refused like a failure.
                let ran = AssertUnwindSafe(run(&self.inner, &mut hooks)).catch_unwind();
                match ran.await {
                    Ok(Ok(resp)) => deliver(format, resp, hooks).await,
                    Ok(Err(f)) => refuse(format, f, hooks).await,
                    Err(_) => refuse(format, Fault::internal(PANICKED), hooks).await,
                }
            }
        };
        let ended = ended.map(|told| told.expect("every request's end is told"));
        (resp, ended.boxed())
    }

    /// The settings, for a by-value method to change; a shared gateway is
    /// copied, with its own requests.
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

    /// `Ok` if `format`'s two types differ and formats of its name share its kind.
    fn check_format(&self, format: &dyn AnyFormat) -> Result<(), Fault> {
        let kind = format.kind();
        let name = format.name();
        if kind.0 == kind.1 {
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
    /// `after` plugins are placed.
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

/// `fault` for the adapter to write, for a request no format took.
fn unwritten(fault: Fault) -> (Handed, BoxFuture<'static, Outcome>) {
    let outcome = Outcome {
        end: Err(fault.clone()),
    };
    (Err(fault), futures::future::ready(outcome).boxed())
}

async fn run(gateway: &Inner, hooks: &mut EndHooks<ClientContext>) -> Result<Response, Fault> {
    for plugin in &gateway.chain {
        hooks.ran += 1;
        if let Some(resp) = plugin.on_client(hooks.cx()).await? {
            return Ok(resp);
        }
    }
    let cx = hooks.cx();
    match cx.target.clone() {
        Some(target) => cx.call(target).await,
        None => Err(Fault::not_found("nothing answers this request")
            .with_source("no target: the gateway has none, and no plugin set one")),
    }
}

/// Servers spawn `handle`, so it must be `Send`.
#[cfg(test)]
fn _handle_is_send(g: &Gateway, req: http::Request<Body>) {
    fn send<T: Send>(_: T) {}
    send(g.handle(req, false));
}
