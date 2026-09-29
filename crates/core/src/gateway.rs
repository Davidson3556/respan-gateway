use std::any::Any;
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures::channel::oneshot;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};

use crate::deliver::{Handed, deliver, refuse};
use crate::fault::PANICKED;
use crate::format::AnyFormat;
use crate::input::Later;
use crate::lifecycle::{EndHooks, InFlight};
use crate::provider::Provider;
use crate::transport::{AnyTransport, Erased, Head};
use crate::{ClientContext, Exchange, Fault, Format, Plugin, Response, Transport};

/// A registered plugin and its name.
#[derive(Clone)]
pub(crate) struct Registered {
    name: String,
    pub(crate) plugin: Arc<dyn Plugin>,
}

#[derive(Clone, Default)]
pub(crate) struct Inner {
    /// The formats served to clients, in the order they are tried.
    formats: Vec<Arc<dyn AnyFormat>>,
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

    /// Add provider `name`, speaking `format` over `transport`, which shares
    /// its heads. `Err` with [`codes::CONFIG`](crate::codes::CONFIG) if the
    /// name is taken or the format breaks [`Gateway::serve`]'s rules.
    pub fn provider<F, T>(
        mut self,
        name: impl Into<String>,
        format: F,
        transport: T,
    ) -> Result<Gateway, Fault>
    where
        F: Format,
        T: Transport<Ask = F::Ask, Answer = F::Answer>,
    {
        let name = name.into();
        self.check_format(&format)?;
        if self.inner.providers.contains_key(&name) {
            return Err(Fault::config(format!("provider '{name}' is defined twice")));
        }
        let format = Arc::new(format);
        let transport: Arc<AnyTransport> = Arc::new(Erased(transport));
        let provider = Provider { format, transport };
        self.inner_mut().providers.insert(name, provider);
        Ok(self)
    }

    /// Serve clients of `format`: a request goes to the first format served
    /// on its wire that [`accepts`](Format::accepts) it. `Err` with
    /// [`codes::CONFIG`](crate::codes::CONFIG) if its input and output types
    /// are one, or a format of its name has another kind or wire.
    pub fn serve(mut self, format: impl Format) -> Result<Gateway, Fault> {
        self.check_format(&format)?;
        self.inner_mut().formats.push(Arc::new(format));
        Ok(self)
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
        let runtime = tokio::runtime::Handle::current();
        let Exchange {
            head,
            body,
            extensions,
            streams,
            ..
        } = req;
        let head: Arc<dyn Any + Send + Sync> = Arc::from(head);
        // A format that panics in `accepts` is the adapter's to write.
        let formats = &self.inner.formats;
        let accepts = || formats.iter().find(|f| f.accepts(&*head)).cloned();
        let format = match std::panic::catch_unwind(AssertUnwindSafe(accepts)) {
            Ok(Some(format)) => format,
            Ok(None) => {
                let text = "no format served accepts this request";
                return unwritten(Fault::not_found(text));
            }
            Err(_) => return unwritten(Fault::internal(PANICKED)),
        };
        // In flight while its body is read, so `idle` waits for it.
        let reading = InFlight::enter(&self.inner.in_flight);
        let (body, later, broken) = match streams {
            true => (None, Some(Later::new(body)), None),
            false => match read_whole(body).await {
                Ok(bytes) => (Some(bytes), None, None),
                Err(fault) => (Some(Bytes::new()), None, Some(fault)),
            },
        };
        let inner = self.inner.clone();
        let cx = ClientContext::new(
            format.clone(),
            head,
            extensions,
            body,
            later,
            runtime,
            inner,
        );
        let format = &format;
        let (outcome, ended) = oneshot::channel();
        let settling = cx.settling.clone();
        let mut hooks = EndHooks::new(&self.inner.in_flight, cx, Some(outcome));
        drop(reading);
        // The request's end hooks run after its calls'.
        hooks.after = Some(settling);
        let resp = match broken {
            Some(fault) => refuse(format, fault, hooks).await,
            None => {
                // A plugin panic is refused like a failure.
                let ran = AssertUnwindSafe(run(&self.inner, &mut hooks)).catch_unwind();
                match ran.await {
                    Ok(Ok(resp)) => deliver(format, resp, None, hooks).await,
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

    /// `Ok` if `format`'s two types differ and formats of its name share its
    /// kind and wire.
    fn check_format(&self, format: &dyn AnyFormat) -> Result<(), Fault> {
        let (name, kind) = (format.name(), format.kind());
        let inner = &self.inner;
        let providers = inner.providers.values().map(|p| &*p.format);
        let known = inner.formats.iter().map(|f| &**f).chain(providers);
        let named: Vec<_> = known.filter(|f| f.name() == name).collect();
        let fault = if kind.0 == kind.1 {
            format!("the kind of format {name} has one type for both directions")
        } else if named.iter().any(|f| f.kind() != kind) {
            format!("two formats named {name} are of different kinds")
        } else if named.iter().any(|f| f.wire() != format.wire()) {
            format!("two formats named {name} are on different wires")
        } else {
            return Ok(());
        };
        Err(Fault::config(fault))
    }
}

/// `fault` for the adapter to write, for a request no format took.
fn unwritten(fault: Fault) -> (Handed, BoxFuture<'static, Result<(), Fault>>) {
    let end = Err(fault.clone());
    (Err(fault), futures::future::ready(end).boxed())
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

/// A whole body's bytes, or the failure that broke it off.
async fn read_whole(mut body: crate::Body) -> Result<Bytes, Fault> {
    let mut all = BytesMut::new();
    while let Some(bytes) = body.next().await {
        all.extend_from_slice(&bytes?);
    }
    Ok(all.freeze())
}

/// Servers spawn `handle`, so it must be `Send`.
#[cfg(test)]
fn _handle_is_send(g: &Gateway, req: Exchange<Head>) {
    fn send<T: Send>(_: T) {}
    send(g.handle(req));
}
