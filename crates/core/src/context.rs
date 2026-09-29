use std::any::Any;
use std::future::ready;
use std::sync::Arc;

use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream;
use tokio::runtime::Handle;

use crate::call::OpenCalls;
use crate::format::{AnyFormat, AnyInput, Origin, is_input};
use crate::gateway::Inner;
use crate::leg::{Later, Layer, inputs, locked};
use crate::lifecycle::{Edge, InFlight, InFlightGuard};
use crate::opening::Opening;
use crate::plugin::Filters;
use crate::{Call, Events, Extensions, Fault, Filter, Plugin};

/// One client request, as the client edge's hooks see it.
pub struct ClientContext {
    /// The first format served that accepted the request.
    pub(crate) format: Arc<dyn AnyFormat>,
    /// What came with the head, sealed: only a relay hands it on.
    pub(crate) origin: Option<Origin>,
    pub(crate) extensions: Extensions,
    /// The provider called if every plugin passes.
    pub(crate) target: Option<String>,
    /// The input that came with the head; none for a streaming request.
    pub(crate) opening: Opening,
    /// The inputs that follow it.
    pub(crate) later: Arc<Later>,
    /// Idle once the request ends, as `running` is dropped; copies share it.
    pub(crate) cut: Arc<InFlight>,
    running: Option<InFlightGuard>,
    /// The runtime end hooks run on.
    pub(crate) runtime: Handle,
    /// Its calls that ended, until their end hooks finish; its own end
    /// hooks wait for them.
    pub(crate) settling: Arc<InFlight>,
    /// Its calls being sent or with an open response, ended when it ends.
    pub(crate) open: Arc<OpenCalls>,
    /// Filters of the response sent to the client; they end with it, or
    /// with the request if it ends first.
    pub(crate) outputs: Layer,
    pub(crate) gateway: Arc<Inner>,
}

impl std::fmt::Debug for ClientContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientContext")
            .field("format", &self.format.name())
            .finish_non_exhaustive()
    }
}

impl Edge for ClientContext {
    fn client(&self) -> &ClientContext {
        self
    }

    fn hook<'a>(&'a self, plugin: &'a dyn Plugin, end: &'a Result<(), Fault>) -> BoxFuture<'a, ()> {
        plugin.on_client_end(self, end)
    }

    /// Open calls end abandoned, the inputs left are cut off, and output
    /// filters still open end as the request did.
    fn close(&mut self, end: &Result<(), Fault>) {
        self.running = None;
        self.open.abandon();
        self.later.end(Err(Fault::abandoned()));
        let _ = locked(&self.outputs).end(end.clone(), &mut None);
    }
}

impl ClientContext {
    pub(crate) fn new(
        format: Arc<dyn AnyFormat>,
        origin: Option<Origin>,
        later: Later,
        extensions: Extensions,
        gateway: Arc<Inner>,
    ) -> Self {
        let cut = Arc::default();
        Self {
            opening: Opening::new(format.clone(), origin.clone()),
            format,
            origin,
            extensions,
            target: gateway.target.clone(),
            later: Arc::new(later),
            running: Some(InFlight::enter(&cut)),
            cut,
            runtime: Handle::current(),
            settling: Arc::default(),
            open: Arc::default(),
            outputs: Layer::default(),
            gateway,
        }
    }

    /// The name of the client's format.
    pub fn format(&self) -> &str {
        self.format.name()
    }

    /// The request's extensions, from the adapter, such as the client's
    /// credential, and earlier plugins; on a provider edge, the call's.
    pub fn extensions(&self) -> &Extensions {
        &self.extensions
    }

    /// Like [`ClientContext::extensions`], to change; calls made later copy
    /// them.
    pub fn extensions_mut(&mut self) -> &mut Extensions {
        &mut self.extensions
    }

    /// Whether `T` is the client's format's [`Input`](crate::Format::Input)
    /// or [`Output`](crate::Format::Output), which the typed accessors take.
    pub fn is_kind<T: Any>(&self) -> bool {
        is_input::<T>(&*self.format).is_ok()
    }

    /// The request's one input as `T`, as filters left it. `Err` with
    /// [`codes::INTERNAL`](crate::codes::INTERNAL) for another type or a
    /// streaming request; or the decoder's or a filter's `Err`. A relay
    /// sends the body unread, so a plugin that vets input refuses on `Err`.
    pub fn input<T: Any>(&self) -> Result<&T, Fault> {
        self.opening.get()
    }

    /// The request's inputs as `T`, as filters left them: a copy of a whole
    /// request's one; a streaming request's as they arrive, read once, so no
    /// call gets them, taken when first awaited and given back if none was
    /// read. Ends with the client's inputs, `Err` if the body broke, and
    /// [`codes::ABANDONED`](crate::codes::ABANDONED) if the request ended
    /// first, never as if they were complete; one `Err` with [`codes::INTERNAL`](crate::codes::INTERNAL) for another
    /// type or if another reader took them.
    pub fn take_inputs<T: Any + Clone + Send>(&mut self) -> Events<T> {
        if is_input::<T>(&*self.format) != Ok(true) {
            return stream::once(async { Err(Fault::wrong_kind::<T>()) }).boxed();
        }
        if self.later.ended {
            return stream::once(ready(self.opening.get::<T>().cloned())).boxed();
        }
        let items = inputs(self.later.clone(), self.format.clone(), None, &self.cut);
        let typed = |input: AnyInput| {
            let typed = input.downcast_ref::<T>().cloned();
            typed.ok_or_else(Fault::wrong_kind::<T>)
        };
        items.map(move |item| item.and_then(typed)).boxed()
    }

    /// The provider called if every plugin passes; initially the gateway's
    /// ([`Gateway::target`](crate::Gateway::target)).
    pub fn target(&self) -> Option<&str> {
        self.target.as_deref()
    }

    /// Call `provider` if every plugin passes, so later plugins still run.
    pub fn set_target(&mut self, provider: impl Into<String>) {
        self.target = Some(provider.into());
    }

    /// A copy of it as it is now, for a call to keep.
    pub(crate) fn copy(&self) -> Self {
        Self {
            format: self.format.clone(),
            origin: self.origin.clone(),
            extensions: self.extensions.clone(),
            target: self.target.clone(),
            opening: self.opening.clone(),
            later: self.later.clone(),
            cut: self.cut.clone(),
            running: None,
            runtime: self.runtime.clone(),
            settling: self.settling.clone(),
            open: self.open.clone(),
            outputs: Layer::default(),
            gateway: self.gateway.clone(),
        }
    }

    /// Run `filter` over the request's inputs if `T` is the client's
    /// [`Input`](crate::Format::Input), or the outputs sent to it if its
    /// [`Output`](crate::Format::Output).
    ///
    /// Input filters, the one way to edit inputs, run the first added first,
    /// before calls and [`ClientContext::take_inputs`]. A whole request's one
    /// input is edited at once and the filter ended; it stays one input. A
    /// streaming request's are edited as read; once they ended, a filter
    /// sees their end at once. Output filters run after the provider edge's,
    /// the last added first.
    ///
    /// `Err` with [`codes::INTERNAL`](crate::codes::INTERNAL) for another
    /// type or a whole input filtered to other than one; or the filter's
    /// `Err`, which reading or sending the input then gets too; or the
    /// failure the inputs ended with.
    pub fn filter<T: Clone + Send + Sync + 'static>(
        &mut self,
        filter: impl Filter<T>,
    ) -> Result<(), Fault> {
        match is_input::<T>(&*self.format)? {
            false => locked(&self.outputs).add(filter, false),
            true if self.later.ended => self.opening.filter(Filters::of(filter)),
            true => locked(&self.later.filters).add(filter, true),
        }
    }

    /// A call to `provider`, the one way to reach one: a copy of the request
    /// as it is now, sent when first awaited, carrying its inputs as filters
    /// left them, translated for another format of the kind. Every plugin's
    /// [`on_provider`](crate::Plugin::on_provider) runs first. A provider's
    /// error answer is `Ok` here and fails the response as read.
    ///
    /// `Err` with [`codes::CONFIG`](crate::codes::CONFIG) if `provider` is
    /// unknown or cannot take the inputs; with
    /// [`codes::INTERNAL`](crate::codes::INTERNAL) for a
    /// [`Call::with_input`] of another kind or once the request ended; with
    /// [`codes::ABANDONED`](crate::codes::ABANDONED) if the request ends
    /// while it is pending; or the `Err` of a hook, the codec or the
    /// transport.
    pub fn call(&mut self, provider: impl Into<String>) -> Call {
        Call::new(Arc::new(self.copy()), provider.into())
    }
}
