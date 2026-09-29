use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use futures::FutureExt;
use futures::channel::oneshot;

use crate::fault::PANICKED;
use crate::format::{AnyFormat, AnyReply, Erased};
use crate::leg::Later;
use crate::lifecycle::{EndHooks, InFlight};
use crate::provider::Provider;
use crate::reply;
use crate::{ClientContext, Extensions, Fault, Format, Plugin, Response};

/// A registered plugin and its name.
#[derive(Clone)]
pub(crate) struct Registered {
    name: String,
    pub(crate) plugin: Arc<dyn Plugin>,
}

#[derive(Clone, Default)]
pub(crate) struct Inner {
    /// The formats served to clients, in the order they are tried.
    served: Vec<Arc<dyn AnyFormat>>,
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
    pub(crate) inner: Arc<Inner>,
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

    /// Add provider `name`, speaking `format`, reached through `dialer`.
    /// `Err` with [`codes::CONFIG`](crate::codes::CONFIG) if the name is
    /// taken or the format breaks [`Gateway::serve`]'s rules.
    pub fn provider<F: Format>(
        self,
        name: impl Into<String>,
        format: F,
        dialer: impl Into<F::Dialer>,
    ) -> Result<Gateway, Fault> {
        let dialer: Arc<dyn Any + Send + Sync> = Arc::new(dialer.into());
        self.add_provider(name.into(), Arc::new(Erased::new(format)), dialer)
    }

    /// Serve clients of `format`: a wire handed to [`Gateway::handle`] goes
    /// to the first format served that [`accepts`](Format::accepts) it.
    /// `Err` with [`codes::CONFIG`](crate::codes::CONFIG) if its input and
    /// output types are one, or a format of its name has another kind, wire
    /// or dialer.
    pub fn serve<F: Format>(self, format: F) -> Result<Gateway, Fault> {
        self.add_served(Arc::new(Erased::new(format)))
    }

    /// Register `plugin` as `name`; its hooks run on every request and call,
    /// after those of the plugins registered before it, on both edges. A
    /// plugin that replies skips the ones after it, so register it after
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

    /// Serve `wire` in the first format served ([`Gateway::serve`]) that
    /// [`accepts`](Format::accepts) it: resolves `Ok` once its client edge
    /// has its reply, whose end the edge tells with [`Ended`](crate::Ended).
    /// The caller built `wire`, so it knows its type; a format of another
    /// wire type never accepts it.
    ///
    /// `Err` if no client edge will write a reply, for the adapter to write
    /// plainly: [`codes::NOT_FOUND`](crate::codes::NOT_FOUND) if no format
    /// accepts the wire, which is dropped;
    /// [`codes::INTERNAL`](crate::codes::INTERNAL) if one panicked; or
    /// [`codes::ABANDONED`](crate::codes::ABANDONED) if the edge dropped its
    /// reply. The adapter owns the wire's rules: it takes out what no plugin
    /// or format should see, such as the gateway's own keys, before, and
    /// what a hop must not carry after.
    ///
    /// The request counts for [`Gateway::idle`] from this call on, before
    /// the future is first polled, and the future borrows nothing: a job
    /// that hands the gateway a request calls `handle` where it starts, and
    /// awaits the future in its own task, so `idle` never misses it. Await
    /// it on a tokio runtime, which runs the end hooks; it panics outside
    /// one.
    pub fn handle<W: Send + 'static>(
        &self,
        wire: W,
        extensions: Extensions,
    ) -> impl Future<Output = Result<(), Fault>> + Send + 'static {
        let wire: Box<dyn Any + Send> = Box::new(wire);
        // A format that panics in `accepts` is the adapter's to write.
        let served = &self.inner.served;
        let accepts = || served.iter().find(|d| d.accepts(&*wire)).cloned();
        let format = match std::panic::catch_unwind(AssertUnwindSafe(accepts)) {
            Ok(Some(format)) => Ok(format),
            Ok(None) => Err(Fault::not_found("no format served accepts this request")),
            Err(_) => Err(Fault::internal(PANICKED)),
        };
        // In flight from this call on, while the request comes in too, so
        // `idle` waits for it.
        let reading = InFlight::enter(&self.inner.in_flight);
        let (tx, rx) = oneshot::channel::<AnyReply>();
        let reply = rx.map(|reply| reply.unwrap_or_else(|_| abandoned()));
        let accepting = format.and_then(|format| {
            let accept = AssertUnwindSafe(|| format.accept(wire, reply.boxed()));
            let accepting = std::panic::catch_unwind(accept);
            accepting
                .map_err(|_| Fault::internal(PANICKED))
                .map(|a| (format, a))
        });
        let inner = self.inner.clone();
        async move {
            let (format, accepting) = accepting?;
            let accepted = AssertUnwindSafe(accepting).catch_unwind().await;
            let panicked = accepted.is_err();
            let accepted = accepted.unwrap_or_else(|_| Err(Fault::internal(PANICKED)));
            let (origin, later, broken) = match accepted {
                Ok((origin, later)) => (origin, Later::new(later), None),
                Err(fault) => (None, Later::new(None), Some(fault)),
            };
            let in_flight = inner.in_flight.clone();
            let cx = ClientContext::new(format.clone(), origin, later, extensions, inner.clone());
            let settling = cx.settling.clone();
            let mut hooks = EndHooks::new(&in_flight, cx);
            drop(reading);
            // The request's end hooks run after its calls'.
            hooks.after = Some(settling);
            let reply = match broken {
                Some(fault) => reply::refusal(fault, hooks),
                None => {
                    // A plugin panic is refused like a failure.
                    let ran = AssertUnwindSafe(run(&inner, &mut hooks)).catch_unwind();
                    let resp = ran.await.unwrap_or_else(|_| Err(Fault::internal(PANICKED)));
                    reply::reply(&format, resp, hooks).await
                }
            };
            // An edge that dropped its reply ends the request abandoned, but
            // one whose future panicked took its reply with it: the request
            // ends as the refusal says, `internal`.
            let Err((_, ended)) = tx.send(reply) else {
                return Ok(());
            };
            let fault = match panicked {
                true => Fault::internal(PANICKED),
                false => Fault::abandoned(),
            };
            ended(Err(fault.clone()));
            Err(fault)
        }
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
        format: Arc<dyn AnyFormat>,
        dialer: Arc<dyn Any + Send + Sync>,
    ) -> Result<Gateway, Fault> {
        self.check(&*format)?;
        if self.inner.providers.contains_key(&name) {
            return Err(Fault::config(format!("provider '{name}' is defined twice")));
        }
        let provider = Provider { format, dialer };
        self.inner_mut().providers.insert(name, provider);
        Ok(self)
    }

    fn add_served(mut self, format: Arc<dyn AnyFormat>) -> Result<Gateway, Fault> {
        self.check(&*format)?;
        self.inner_mut().served.push(format);
        Ok(self)
    }

    /// `Ok` if `format`'s two types differ and formats of its name share
    /// its kind and wire.
    fn check(&self, format: &dyn AnyFormat) -> Result<(), Fault> {
        let (name, kind) = (format.name(), format.kind());
        let inner = &self.inner;
        let providers = inner.providers.values().map(|p| &*p.format);
        let known = inner.served.iter().map(|d| &**d).chain(providers);
        let named: Vec<_> = known.filter(|d| d.name() == name).collect();
        let fault = if kind.0 == kind.1 {
            format!("the kind of format {name} has one type for both directions")
        } else if named.iter().any(|d| d.kind() != kind) {
            format!("two formats named {name} are of different kinds")
        } else if named.iter().any(|d| d.wire() != format.wire()) {
            format!("two formats named {name} are on different wires")
        } else {
            return Ok(());
        };
        Err(Fault::config(fault))
    }
}

/// The reply an edge gets if the gateway dropped the request first.
fn abandoned() -> AnyReply {
    let headed = (None, Err(Fault::abandoned()));
    (headed, Box::new(|_| {}))
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

/// Servers spawn `handle`, so it must be `Send`.
#[cfg(test)]
fn _handle_is_send(g: &Gateway) {
    fn send<T: Send>(_: T) {}
    send(g.handle(0u8, Extensions::new()));
}
