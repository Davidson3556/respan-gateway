use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};

use futures::FutureExt;

use crate::call::SharedCall;
use crate::fault::PANICKED;
use crate::format::{AnyFormat, Erased};
use crate::leg::locked;
use crate::lifecycle::{EndHooks, InFlight};
use crate::outflow::outflow;
use crate::provider::Provider;
use crate::response::Batch;
use crate::{ClientContext, EventStream, Extensions, Fault, Format, Plugin, Response, Result};

/// A registered plugin and its name.
#[derive(Clone)]
pub(crate) struct Registered {
    name: String,
    pub(crate) plugin: Arc<dyn Plugin>,
}

#[derive(Clone, Default)]
pub(crate) struct Registry {
    /// The client formats, in the order they are tried.
    clients: Vec<Arc<dyn AnyFormat>>,
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
/// and owns their transport's rules. Built by value, each setting checked as
/// given: `Gateway::new().provider(..)?.client(..)?.plugin(..)?.target(..)?`.
#[derive(Clone, Default)]
pub struct Gateway {
    pub(crate) inner: Arc<Registry>,
}

impl std::fmt::Debug for Gateway {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&str> = self.inner.plugins.iter().map(|p| &*p.name).collect();
        f.debug_struct("Gateway")
            .field("formats", &self.inner.clients.len())
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

    /// Add a provider speaking `format`. After its hooks run, each attempt
    /// gets a fresh transport from `factory`, with that call's extensions.
    /// The factory may return a lightweight handle backed by a shared pool.
    /// `I` is the gateway input type and `O` its output type: this binding
    /// receives `O` and sends `I`.
    pub fn provider<I, O, T>(
        self,
        name: impl Into<String>,
        format: impl Format<T, O, I>,
        factory: impl Fn(Extensions) -> T + Send + Sync + 'static,
    ) -> Result<Gateway>
    where
        T: Send + 'static,
        I: Clone + Send + Sync + 'static,
        O: Clone + Send + Sync + 'static,
    {
        let factory =
            Arc::new(move |extensions| Box::new(factory(extensions)) as Box<dyn Any + Send>);
        self.add_provider(
            name.into(),
            Arc::new(Erased::<_, T, O, I, true>::new(format)),
            factory,
        )
    }

    /// Register a client format receiving inputs `I` and sending outputs `O`
    /// over transport `T`. The first registered
    /// format whose `matches` accepts the transport handles it.
    /// Formats sharing a name must share their event types, but may use
    /// different transports. Same-name formats relay protocol metadata
    /// regardless of transport type when carrying the original input.
    pub fn client<I, O, T>(self, format: impl Format<T, I, O>) -> Result<Gateway>
    where
        T: Send + 'static,
        I: Clone + Send + Sync + 'static,
        O: Clone + Send + Sync + 'static,
    {
        self.add_client(Arc::new(Erased::<_, T, I, O, false>::new(format)))
    }

    /// Register `plugin` as `name`; its hooks run on every request and call,
    /// after those of the plugins registered before it, on both edges. A
    /// plugin that replies skips the ones after it, so register it after
    /// every plugin that must see each request. `Err` with
    /// [`codes::CONFIG`](crate::codes::CONFIG) if the name is taken.
    pub fn plugin(mut self, name: impl Into<String>, plugin: impl Plugin) -> Result<Gateway> {
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
    pub fn target(mut self, provider: impl Into<String>) -> Result<Gateway> {
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

    /// Run one application-facing edge through delivery and its completion.
    /// The first registered format matching `io` creates the edge.
    ///
    /// The adapter must consume the outgoing data concurrently with this
    /// future. An error may mean the edge refused, failed during delivery,
    /// or no format accepted the wire. Protocol adapters own how to encode it.
    ///
    /// Work counts for `idle` from this call, before polling the future.
    /// The future owns its state and requires a Tokio runtime when polled.
    pub fn handle<T: Send + 'static>(
        &self,
        io: T,
        extensions: Extensions,
    ) -> impl Future<Output = Result<()>> + Send + 'static {
        let io: Box<dyn Any + Send> = Box::new(io);
        // A format that panics in `matches` is the adapter's to write.
        let clients = &self.inner.clients;
        let matches = || clients.iter().find(|d| d.matches(&*io)).cloned();
        let format = match std::panic::catch_unwind(AssertUnwindSafe(matches)) {
            Ok(Some(format)) => Ok(format),
            Ok(None) => Err(Fault::not_found("no client format accepts this request")),
            Err(_) => Err(Fault::internal(PANICKED)),
        };
        // In flight from this call on, while the request comes in too, so
        // `idle` waits for it.
        let reading = InFlight::enter(&self.inner.in_flight);
        let accepting = format.and_then(|format| {
            let accepted = std::panic::catch_unwind(AssertUnwindSafe(|| format.bind(io)))
                .unwrap_or_else(|_| Err(Fault::internal(PANICKED)))?;
            Ok((format, accepted))
        });
        let inner = self.inner.clone();
        async move {
            // End hooks may finish on another thread while delivery resources
            // are being dropped. Keep the handle counted through that cleanup.
            let _active = reading;
            let (format, (opening, mut outgoing)) = accepting?;
            let opened = AssertUnwindSafe(opening)
                .catch_unwind()
                .await
                .unwrap_or_else(|_| Err(Fault::internal(PANICKED)));
            let incoming = opened.unwrap_or_else(EventStream::refused);
            let broken = incoming.refusal().cloned();
            let in_flight = inner.in_flight.clone();
            let cx = ClientContext::new(format.clone(), incoming, extensions, inner.clone());
            let settling = cx.settling.clone();
            let mut hooks = EndHooks::new(&in_flight, cx, Some(settling));
            let discarded = Arc::new(Mutex::new(Vec::new()));
            let (mut flow, call) = match broken {
                Some(fault) => (EventStream::refused(fault), None),
                None => {
                    let ran = AssertUnwindSafe(run(&inner, &mut hooks)).catch_unwind();
                    let resp = ran.await.unwrap_or_else(|_| Err(Fault::internal(PANICKED)));
                    outflow(&format, resp, &mut hooks, &discarded).await
                }
            };
            // This guard ends the exchange before a cancelled writer drops
            // the output stream it owns. Both halves then observe the cut.
            let mut delivery = Delivery {
                hooks,
                call,
                writing: None,
                discarded,
            };
            delivery.writing = Some(
                match std::panic::catch_unwind(AssertUnwindSafe(|| outgoing.prepare(&mut flow))) {
                    Ok(Ok(())) => crate::events::deliver(flow, outgoing),
                    result => {
                        // Release unread output before settling the delivery so
                        // its deferred provider ending observes preparation failure.
                        drop(flow);
                        let fault = match result {
                            Ok(Err(fault)) => fault,
                            _ => Fault::internal(PANICKED),
                        };
                        async move { Err(fault) }.boxed()
                    }
                },
            );
            delivery.await
        }
    }

    /// The settings, for a by-value method to change; a shared gateway is
    /// copied, with its own requests.
    fn inner_mut(&mut self) -> &mut Registry {
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
        factory: Arc<dyn Fn(Extensions) -> Box<dyn Any + Send> + Send + Sync>,
    ) -> Result<Gateway> {
        self.check(&*format)?;
        if self.inner.providers.contains_key(&name) {
            return Err(Fault::config(format!("provider '{name}' is defined twice")));
        }
        let provider = Provider { format, factory };
        self.inner_mut().providers.insert(name, provider);
        Ok(self)
    }

    fn add_client(mut self, format: Arc<dyn AnyFormat>) -> Result<Gateway> {
        self.check(&*format)?;
        self.inner_mut().clients.push(format);
        Ok(self)
    }

    /// `Ok` if `format`'s two types differ and formats of its name share
    /// its kind; transport types may differ.
    fn check(&self, format: &dyn AnyFormat) -> Result<()> {
        let (name, kind) = (format.name(), format.kind());
        let inner = &self.inner;
        let providers = inner.providers.values().map(|p| &*p.format);
        let known = inner.clients.iter().map(|d| &**d).chain(providers);
        let named: Vec<_> = known.filter(|d| d.name() == name).collect();
        let fault = if kind.0 == kind.1 {
            format!("the kind of format {name} has one type for both directions")
        } else if named.iter().any(|d| d.kind() != kind) {
            format!("two formats named {name} are of different kinds")
        } else {
            return Ok(());
        };
        Err(Fault::config(fault))
    }
}

/// Owns completion while an edge writes. Drop runs before its future is
/// dropped, so open calls and retained streams cannot outlive their owner.
struct Delivery {
    hooks: EndHooks<ClientContext>,
    call: Option<SharedCall>,
    writing: Option<futures::future::BoxFuture<'static, Result<()>>>,
    discarded: Arc<Mutex<Vec<Batch>>>,
}

impl Delivery {
    fn finish(&mut self, end: Result<()>) {
        if let (Err(fault), Some(call)) = (&end, &self.call) {
            locked(call).finish(Err(fault.clone()));
        }
        // On failure/cancellation, cut both directions before releasing the
        // pump's source. On success, an unread ready end may still settle its
        // provider normally when dropping the source performs its bounded probe.
        if end.is_err() {
            self.hooks.finish(end.clone());
        }
        drop(self.writing.take());
        // Settle discarded provider ends only after the delivery result is
        // known; a write failure takes precedence over an unread ready end.
        let discarded = std::mem::take(&mut *locked(&self.discarded));
        for mut batch in discarded {
            let _ = std::panic::catch_unwind(AssertUnwindSafe(|| batch.read()));
        }
        self.hooks.finish(end);
    }
}

impl Future for Delivery {
    type Output = Result<()>;
    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let polled = std::panic::catch_unwind(AssertUnwindSafe(|| {
            self.writing
                .as_mut()
                .expect("a delivery has a writer")
                .as_mut()
                .poll(cx)
        }));
        let end = match polled {
            Ok(std::task::Poll::Pending) => return std::task::Poll::Pending,
            Ok(std::task::Poll::Ready(end)) => end,
            Err(_) => Err(Fault::internal(PANICKED)),
        };
        self.finish(end.clone());
        std::task::Poll::Ready(end)
    }
}

impl Drop for Delivery {
    fn drop(&mut self) {
        if !self.hooks.is_finished() {
            self.finish(Err(Fault::abandoned()));
        }
    }
}

async fn run(gateway: &Registry, hooks: &mut EndHooks<ClientContext>) -> Result<Response> {
    for Registered { plugin, .. } in &gateway.plugins {
        if let Some(resp) = plugin.on_client(hooks.begin()).await? {
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
