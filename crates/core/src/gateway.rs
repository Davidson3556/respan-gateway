use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use futures::channel::oneshot;
use futures::future::{BoxFuture, ready};
use futures::{FutureExt, StreamExt, stream};

use crate::dialect::{AnyDialect, AnyReply, Erased};
use crate::fault::PANICKED;
use crate::leg::Later;
use crate::legacy::{self, Legacy};
use crate::lifecycle::{EndHooks, InFlight};
use crate::provider::Provider;
use crate::session;
use crate::{
    ClientContext, Dialect, Exchange, Extensions, Fault, Format, Head, Plugin, Response, Transport,
};

/// A registered plugin and its name.
#[derive(Clone)]
pub(crate) struct Registered {
    name: String,
    pub(crate) plugin: Arc<dyn Plugin>,
}

#[derive(Clone, Default)]
pub(crate) struct Inner {
    /// The dialects served to clients, in the order they are tried.
    served: Vec<Arc<dyn AnyDialect>>,
    /// In registration order, which is the order their hooks run, on both
    /// edges.
    pub(crate) plugins: Vec<Registered>,
    pub(crate) providers: HashMap<String, Provider>,
    /// The provider called if no plugin sets a target.
    pub(crate) target: Option<String>,
    /// Requests, calls and end hooks still running.
    pub(crate) in_flight: Arc<InFlight>,
}

/// The gateway. It has no server: an adapter or embedder hands it requests
/// and owns their wire's rules. Built by value, each setting checked as
/// given: `Gateway::new().provider(..)?.serve(..)?.plugin(..)?.target(..)?`.
#[derive(Clone, Default)]
pub struct Gateway {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Gateway {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&str> = self.inner.plugins.iter().map(|p| &*p.name).collect();
        f.debug_struct("Gateway")
            .field("formats", &self.inner.served.len())
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

    /// Add provider `name`, speaking `format` over `transport`, which shares
    /// its heads. `Err` with [`codes::CONFIG`](crate::codes::CONFIG) if the
    /// name is taken or the format breaks [`Gateway::serve`]'s rules.
    pub fn provider<F, T>(
        self,
        name: impl Into<String>,
        format: F,
        transport: T,
    ) -> Result<Gateway, Fault>
    where
        F: Format,
        T: Transport<Ask = F::Ask, Answer = F::Answer>,
    {
        let transport: legacy::Dialer<F> = Arc::new(transport);
        self.add_provider(name.into(), legacy_dialect(format), Arc::new(transport))
    }

    /// Serve clients of `format`: a request goes to the first format served
    /// on its wire that [`accepts`](Format::accepts) it. `Err` with
    /// [`codes::CONFIG`](crate::codes::CONFIG) if its input and output types
    /// are one, or a format of its name has another kind or wire.
    pub fn serve(self, format: impl Format) -> Result<Gateway, Fault> {
        self.add_served(legacy_dialect(format))
    }

    /// Add provider `name`, speaking `dialect`, reached through `dialer`: as
    /// [`Gateway::provider`], on the edge contract ([`Dialect`], a temporary
    /// name, decision 0020).
    pub fn provider_dialect<D: Dialect>(
        self,
        name: impl Into<String>,
        dialect: D,
        dialer: impl Into<D::Dialer>,
    ) -> Result<Gateway, Fault> {
        let dialer: Arc<dyn Any + Send + Sync> = Arc::new(dialer.into());
        self.add_provider(name.into(), Arc::new(Erased::new(dialect)), dialer)
    }

    /// Serve clients of `dialect`: a wire handed to [`Gateway::open`] goes
    /// to the first dialect served that accepts it. As [`Gateway::serve`],
    /// on the edge contract ([`Dialect`], a temporary name, decision 0020).
    pub fn serve_dialect<D: Dialect>(self, dialect: D) -> Result<Gateway, Fault> {
        self.add_served(Arc::new(Erased::new(dialect)))
    }

    /// Register `plugin` as `name`; its hooks run on every request and call,
    /// after those of the plugins registered before it, on both edges. A
    /// plugin that answers skips the ones after it, so register it after
    /// every plugin that must see each request. `Err` with
    /// [`codes::CONFIG`](crate::codes::CONFIG) if the name is taken.
    pub fn plugin(
        mut self,
        name: impl Into<String>,
        plugin: impl Plugin,
    ) -> Result<Gateway, Fault> {
        let name = name.into();
        if self.inner.plugins.iter().any(|p| p.name == name) {
            return Err(Fault::config(format!(
                "plugin '{name}' is registered twice"
            )));
        }
        let plugin = Arc::new(plugin);
        self.inner_mut().plugins.push(Registered { name, plugin });
        Ok(self)
    }

    /// The provider called if every plugin passes and none set a target
    /// ([`ClientContext::set_target`]), replacing any set before; a request
    /// it cannot take fails as [`ClientContext::call`] does. `Err` with
    /// [`codes::CONFIG`](crate::codes::CONFIG) for an unknown provider.
    pub fn target(mut self, provider: impl Into<String>) -> Result<Gateway, Fault> {
        let target = provider.into();
        if !self.inner.providers.contains_key(&target) {
            return Err(Fault::config(format!("unknown provider '{target}'")));
        }
        self.inner_mut().target = Some(target);
        Ok(self)
    }

    /// Resolves once every request handed to this gateway and every end hook
    /// has finished: await it on shutdown, under a deadline of your own. A
    /// request counts from the moment `handle` is called, while its body is
    /// still being read too. A gateway changed after being cloned, or while
    /// requests run, is a new one with its own requests: await the one that
    /// took them.
    pub async fn idle(&self) {
        self.inner.in_flight.idle().await;
    }

    /// Run `req` in the first format served that [`accepts`](Format::accepts)
    /// its head. The caller knows its wire and does the typing: it
    /// [`erase`](Exchange::erase)s its request's head and
    /// [`downcast`](Exchange::downcast)s the response's to its own, and the
    /// kernel checks neither. The adapter owns the wire's rules: it takes
    /// out what no plugin or format should see, such as the gateway's own
    /// keys, before, and what a hop must not carry after.
    ///
    /// If `req.streams`, the body's data frames are inputs as they arrive and
    /// the response's head goes out at once, as the client waits for it.
    /// Else the body is read in full, so the caller bounds it; one that
    /// fails while read is refused. `req.relayed` is ignored.
    ///
    /// `Err` for what no format can write, for the adapter to write plainly:
    /// [`codes::NOT_FOUND`](crate::codes::NOT_FOUND) if no format accepts
    /// the request; [`codes::INTERNAL`](crate::codes::INTERNAL) if the one
    /// that does panicked or could not write a refusal. A response head of
    /// another wire than the request's shows as `INTERNAL` at the caller's
    /// downcast.
    ///
    /// With it comes a future of how the request ended, as
    /// [`Plugin::on_client_end`] gets it, resolved once the response ends,
    /// before end hooks finish. Call it on a tokio
    /// runtime, which runs the end hooks; it panics outside one.
    pub async fn handle(
        &self,
        req: Exchange<Head>,
    ) -> (
        Result<Exchange<Head>, Fault>,
        BoxFuture<'static, Result<(), Fault>>,
    ) {
        legacy::handle(self, req).await
    }

    /// Serve `wire` in the first dialect served
    /// ([`Gateway::serve_dialect`]) that accepts it: resolves once its
    /// client edge has its reply, with a future of how the session ended,
    /// as [`Plugin::on_client_end`] gets it. If no dialect accepts it, the
    /// wire is dropped, unanswered, and the end is `Err` with
    /// [`codes::NOT_FOUND`](crate::codes::NOT_FOUND), or
    /// [`codes::INTERNAL`](crate::codes::INTERNAL) if one panicked, for the
    /// adapter to write plainly. The session counts for [`Gateway::idle`]
    /// from this call on. Call it on a tokio runtime, which runs the end
    /// hooks. The edge contract ([`Dialect`], a temporary name, decision
    /// 0020).
    pub async fn open<W: Send + 'static>(
        &self,
        wire: W,
        extensions: Extensions,
    ) -> BoxFuture<'static, Result<(), Fault>> {
        self.open_any(Box::new(wire), extensions).await
    }

    /// [`Gateway::open`], its wire erased.
    pub(crate) async fn open_any(
        &self,
        wire: Box<dyn Any + Send>,
        extensions: Extensions,
    ) -> BoxFuture<'static, Result<(), Fault>> {
        // A dialect that panics in `accepts` is the adapter's to write.
        let served = &self.inner.served;
        let accepts = || served.iter().find(|d| d.accepts(&*wire)).cloned();
        let dialect = match std::panic::catch_unwind(AssertUnwindSafe(accepts)) {
            Ok(Some(dialect)) => dialect,
            Ok(None) => {
                let text = "no format served accepts this request";
                return unwritten(Fault::not_found(text));
            }
            Err(_) => return unwritten(Fault::internal(PANICKED)),
        };
        // In flight while the request comes in, so `idle` waits for it.
        let reading = InFlight::enter(&self.inner.in_flight);
        let (tx, rx) = oneshot::channel::<AnyReply>();
        let reply = rx.map(|reply| reply.unwrap_or_else(|_| abandoned()));
        let accept = AssertUnwindSafe(|| dialect.accept(wire, reply.boxed()));
        let Ok(accepting) = std::panic::catch_unwind(accept) else {
            return unwritten(Fault::internal(PANICKED));
        };
        let half = AssertUnwindSafe(accepting).catch_unwind().await;
        let (origin, later) = half.unwrap_or_else(|_| (None, Err(Fault::internal(PANICKED))));
        let (later, broken) = match later {
            Ok(later) => (Later::new(later), None),
            Err(fault) => (Later::new(stream::empty().boxed()), Some(fault)),
        };
        let inner = self.inner.clone();
        let cx = ClientContext::new(dialect.clone(), origin, later, extensions, inner);
        let (outcome, ended) = oneshot::channel();
        let settling = cx.settling.clone();
        let mut hooks = EndHooks::new(&self.inner.in_flight, cx, Some(outcome));
        drop(reading);
        // The request's end hooks run after its calls'.
        hooks.after = Some(settling);
        let reply = match broken {
            Some(fault) => session::refusal(hooks.cx().extensions.clone(), fault, hooks),
            None => {
                // A plugin panic is refused like a failure.
                let ran = AssertUnwindSafe(run(&self.inner, &mut hooks)).catch_unwind();
                let answer = ran.await.unwrap_or_else(|_| Err(Fault::internal(PANICKED)));
                session::reply(&dialect, answer, hooks).await
            }
        };
        // An edge that dropped its reply ends the session abandoned.
        let _ = tx.send(reply);
        let ended = ended.map(|told| told.expect("every request's end is told"));
        ended.boxed()
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

    fn add_provider(
        mut self,
        name: String,
        dialect: Arc<dyn AnyDialect>,
        dialer: Arc<dyn Any + Send + Sync>,
    ) -> Result<Gateway, Fault> {
        self.check(&*dialect)?;
        if self.inner.providers.contains_key(&name) {
            return Err(Fault::config(format!("provider '{name}' is defined twice")));
        }
        let provider = Provider { dialect, dialer };
        self.inner_mut().providers.insert(name, provider);
        Ok(self)
    }

    fn add_served(mut self, dialect: Arc<dyn AnyDialect>) -> Result<Gateway, Fault> {
        self.check(&*dialect)?;
        self.inner_mut().served.push(dialect);
        Ok(self)
    }

    /// `Ok` if `dialect`'s two types differ and dialects of its name share
    /// its kind and wire.
    fn check(&self, dialect: &dyn AnyDialect) -> Result<(), Fault> {
        let (name, kind) = (dialect.name(), dialect.kind());
        let inner = &self.inner;
        let providers = inner.providers.values().map(|p| &*p.dialect);
        let known = inner.served.iter().map(|d| &**d).chain(providers);
        let named: Vec<_> = known.filter(|d| d.name() == name).collect();
        let fault = if kind.0 == kind.1 {
            format!("the kind of format {name} has one type for both directions")
        } else if named.iter().any(|d| d.kind() != kind) {
            format!("two formats named {name} are of different kinds")
        } else if named.iter().any(|d| d.wire() != dialect.wire()) {
            format!("two formats named {name} are on different wires")
        } else {
            return Ok(());
        };
        Err(Fault::config(fault))
    }
}

/// An old `format`, as a dialect on its pair of head types.
fn legacy_dialect<F: Format>(format: F) -> Arc<dyn AnyDialect> {
    let wire = (TypeId::of::<F::Ask>(), TypeId::of::<F::Answer>());
    Arc::new(Erased::on(Legacy::new(format), wire))
}

/// An end for a wire no dialect took, for the adapter to write plainly.
fn unwritten(fault: Fault) -> BoxFuture<'static, Result<(), Fault>> {
    ready(Err(fault)).boxed()
}

/// The reply an edge gets if the gateway dropped the session first.
fn abandoned() -> AnyReply {
    let half = (None, Err(Fault::abandoned()));
    (Extensions::new(), half, Box::new(|_| {}))
}

async fn run(gateway: &Inner, hooks: &mut EndHooks<ClientContext>) -> Result<Response, Fault> {
    for Registered { plugin, .. } in &gateway.plugins {
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

/// Servers spawn `handle` and `open`, so they must be `Send`.
#[cfg(test)]
fn _handle_is_send(g: &Gateway, req: Exchange<Head>) {
    fn send<T: Send>(_: T) {}
    send(g.handle(req));
    send(g.open(0u8, Extensions::new()));
}
