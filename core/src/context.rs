use std::any::Any;
use std::future::ready;
use std::sync::{Arc, Mutex};

use futures::future::BoxFuture;
use futures::{Stream, StreamExt, stream};
use tokio::runtime::Handle;

use crate::call::OpenCalls;
use crate::format::{AnyEvents, AnyFormat, AnyValue, erase, is_input, typed};
use crate::gateway::Inner;
use crate::input::Input;
use crate::leg::{Later, Layer, inputs, locked};
use crate::lifecycle::{Edge, InFlight, InFlightGuard};
use crate::plugin::Filters;
use crate::{Call, EventStream, Extensions, Fault, Filter, Gateway, Plugin, Result};

/// One client request, as the client edge's hooks see it.
///
/// Calling and reading inputs take `&mut ClientContext`, which `on_client`
/// alone gets: end hooks and a call's hooks see the request by `&`.
///
/// ```
/// # use respan_gateway_core::ClientContext;
/// fn on_client(cx: &mut ClientContext) {
///     let _ = (cx.call("p"), cx.take_inputs::<String>());
/// }
/// ```
///
/// ```compile_fail
/// # use respan_gateway_core::ClientContext;
/// fn on_client_end(cx: &ClientContext) {
///     let _ = cx.call("p");
/// }
/// ```
///
/// ```compile_fail
/// # use respan_gateway_core::ProviderContext;
/// fn on_provider(px: &mut ProviderContext) {
///     let _ = px.client().take_inputs::<String>();
/// }
/// ```
pub struct ClientContext {
    /// The first client format that accepted the request.
    pub(crate) format: Arc<dyn AnyFormat>,
    pub(crate) extensions: Extensions,
    /// The provider called if every plugin passes.
    pub(crate) target: Option<String>,
    /// The opening value; none for a streaming flow.
    pub(crate) input: Input,
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
    /// Filters of the response sent to the client added since the last
    /// [`wrap`](ClientContext::wrap), which run first; they end with it, or
    /// with the request if it ends first.
    pub(crate) outputs: Layer,
    /// Each wrap of the response sent to the client, in the order added,
    /// with the filters added before it, which run after it; taken when
    /// sent.
    pub(crate) wrapped: Mutex<Vec<(Option<Wrap>, Layer)>>,
    pub(crate) gateway: Arc<Inner>,
}

/// A plugin's wrap of the outputs sent to the client, erased.
pub(crate) type Wrap = Box<dyn FnOnce(AnyEvents) -> AnyEvents + Send>;

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

    fn hook<'a>(&'a self, plugin: &'a dyn Plugin, end: &'a Result<()>) -> BoxFuture<'a, ()> {
        plugin.on_client_end(self, end)
    }

    /// The inputs left are cut off, first, so a plugin's stream that ends
    /// as the one it wraps is cut ends after the cut; open calls end
    /// abandoned; output filters still open end as the request did.
    fn close(&mut self, end: &Result<()>) {
        self.later.end(Err(Fault::abandoned()));
        self.running = None;
        self.open.abandon();
        let wrapped = locked(&self.wrapped);
        for layer in std::iter::once(&self.outputs).chain(wrapped.iter().rev().map(|w| &w.1)) {
            let _ = locked(layer).end(end.clone(), &mut None);
        }
    }
}

impl ClientContext {
    pub(crate) fn new(
        format: Arc<dyn AnyFormat>,
        mut incoming: EventStream<AnyValue>,
        extensions: Extensions,
        gateway: Arc<Inner>,
    ) -> Self {
        let cut = Arc::default();
        let metadata = incoming.metadata_copy();
        let single = incoming.take_single();
        let later = if single.is_some() || incoming.refusal().is_some() {
            Later::new(None)
        } else {
            let input_format = format.clone();
            let events = incoming
                .boxed()
                .map(move |event| event.map(|event| input_format.values(vec![event])))
                .boxed();
            Later::new(Some(events))
        };
        Self {
            input: Input::new(format.clone(), single, metadata),
            format,
            extensions,
            target: gateway.target.clone(),
            later: Arc::new(later),
            running: Some(InFlight::enter(&cut)),
            cut,
            runtime: Handle::current(),
            settling: Arc::default(),
            open: Arc::default(),
            outputs: Layer::default(),
            wrapped: Mutex::default(),
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

    /// Whether `T` is the client's format's input type
    /// or output type, which the typed accessors take.
    pub fn is_kind<T: Any>(&self) -> bool {
        is_input::<T>(&*self.format).is_ok()
    }

    /// The request's one input as `T`, as filters left it. `Err` with
    /// [`codes::INTERNAL`](crate::codes::INTERNAL) for another type or a
    /// streaming request; or the decoder's or a filter's `Err`. A relay
    /// sends the body unread, so a plugin that vets input refuses on `Err`.
    pub fn input<T: Any>(&self) -> Result<&T> {
        self.input.get()
    }

    /// The request's inputs as `T`, as filters and wraps left them: a copy
    /// of a whole request's one; a streaming request's as they arrive, read
    /// once, so no call gets them, taken when first awaited and given back
    /// if none was read. Ends with the client's inputs, `Err` if the body
    /// broke, and [`codes::ABANDONED`](crate::codes::ABANDONED) if the
    /// request ended first, never as if they were complete; one `Err` with
    /// [`codes::INTERNAL`](crate::codes::INTERNAL) for another type or if
    /// another reader took or read them.
    pub fn take_inputs<T: Any + Clone + Send>(
        &mut self,
    ) -> futures::stream::BoxStream<'static, Result<T>> {
        if is_input::<T>(&*self.format) != Ok(true) {
            return stream::once(async { Err(Fault::wrong_kind::<T>()) }).boxed();
        }
        if !self.input.is_live() {
            return stream::once(ready(self.input.get::<T>().cloned())).boxed();
        }
        let items = inputs(self.later.clone(), self.format.clone(), None, &self.cut);
        let typed = |input: AnyValue| {
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

    /// The gateway this request came to, such as for a job that outlives
    /// the request to hand it requests of its own; [`Gateway::idle`] waits
    /// for each from the moment [`Gateway::handle`] is called, so call it
    /// here and await its future in the job's task. Move the gateway into
    /// that job, not into the plugin: the gateway holds its plugins, so a
    /// plugin that keeps it frees neither.
    ///
    /// A request handed back this way is a new one: it inherits none of
    /// this request's extensions, and every plugin runs on it again, with
    /// no limit on how deep requests nest. So a plugin that hands one back
    /// marks it with an extension of its own and passes a request it marked,
    /// or it may loop.
    pub fn gateway(&self) -> Gateway {
        Gateway {
            inner: self.gateway.clone(),
        }
    }

    /// A copy of it as it is now, for a call to keep.
    pub(crate) fn copy(&self) -> Self {
        Self {
            format: self.format.clone(),
            extensions: self.extensions.clone(),
            target: self.target.clone(),
            input: self.input.clone(),
            later: self.later.clone(),
            cut: self.cut.clone(),
            running: None,
            runtime: self.runtime.clone(),
            settling: self.settling.clone(),
            open: self.open.clone(),
            outputs: Layer::default(),
            wrapped: Mutex::default(),
            gateway: self.gateway.clone(),
        }
    }

    /// Run `filter` over the request's inputs if `T` is the client's
    /// input type, or the outputs sent to it if its
    /// output type.
    ///
    /// Input filters run the first added first, before calls and
    /// [`ClientContext::take_inputs`]. A whole request's one input is edited
    /// at once and the filter ended; it stays one input. A streaming
    /// request's are edited as read; once they ended, a filter sees their
    /// end at once. Output filters run after the provider edge's,
    /// the last added first.
    ///
    /// `Err` with [`codes::INTERNAL`](crate::codes::INTERNAL) for another
    /// type or a whole input filtered to other than one; or the filter's
    /// `Err`, which reading or sending the input then gets too; or the
    /// failure the inputs ended with.
    pub fn filter<T: Clone + Send + Sync + 'static>(
        &mut self,
        filter: impl Filter<T>,
    ) -> Result<()> {
        match is_input::<T>(&*self.format)? {
            false => locked(&self.outputs).add(filter, false),
            true if !self.input.is_live() => self.input.filter(Filters::of(filter)),
            true => locked(&self.later.filters).add(filter, true),
        }
    }

    /// Wrap the request's inputs if `T` is the client's
    /// input type, or the outputs sent to it if its
    /// output type, in a stream of `wrap`'s: for work
    /// a [filter](ClientContext::filter) cannot do, as it waits, such as a
    /// remote check of each input or a deadline that cuts the outputs.
    ///
    /// ```
    /// # use respan_gateway_core::futures::{StreamExt, TryStreamExt};
    /// # use respan_gateway_core::{ClientContext, EventStream, Result};
    /// # use tokio::time::{Duration, sleep};
    /// # #[derive(Clone)]
    /// # struct Turn;
    /// # #[derive(Clone)]
    /// # struct Event;
    /// /// A remote check of one input, such as a classifier's call.
    /// async fn check(turn: Turn) -> Result<Turn> {
    ///     Ok(turn)
    /// }
    ///
    /// fn on_client(cx: &mut ClientContext, limit: Duration) -> Result<()> {
    ///     cx.wrap(|turns: EventStream<Turn>| turns.and_then(check))?;
    ///     cx.wrap(move |events: EventStream<Event>| events.take_until(sleep(limit)))
    /// }
    /// ```
    ///
    /// `take_until` ends the stream cleanly, so the call ends `abandoned`, as
    /// if the client had left. To end it with a reason, cut with a fault:
    /// a last item `Err(Fault::new(code, message))`, as `MaxLength` in
    /// `examples/realtime/realtime.rs` does.
    ///
    /// The stream takes the place of the one it wraps, and the request stays
    /// what it was: a call of its inputs relays as before, and the provider's
    /// head still reaches the client. Wraps and filters apply in plugin
    /// order: a wrap sees inputs after the filters and wraps added before it,
    /// outputs before them. Its stream is pulled as its reader reads; the
    /// request's end cuts what it wraps and what it gives with
    /// [`codes::ABANDONED`](crate::codes::ABANDONED), never a clean end.
    /// Inputs that fail end the call that reads them with that fault, so
    /// the client edge can say it. A call made before the wrap keeps the
    /// inputs as they were.
    ///
    /// `Err` with [`codes::INTERNAL`](crate::codes::INTERNAL) for another
    /// type, or for the inputs of a whole request, whose one input came with
    /// its head: vet it here and edit it with a filter.
    pub fn wrap<T, S>(
        &mut self,
        wrap: impl FnOnce(EventStream<T>) -> S + Send + 'static,
    ) -> Result<()>
    where
        T: Any + Clone + Send + Sync,
        S: Stream<Item = Result<T>> + Send + 'static,
    {
        if !is_input::<T>(&*self.format)? {
            let wrap: Wrap =
                Box::new(|outputs| erase(wrap(EventStream::stream(typed(outputs))).boxed()));
            let before = std::mem::take(&mut self.outputs);
            locked(&self.wrapped).push((Some(wrap), before));
            return Ok(());
        }
        if !self.input.is_live() {
            let whole = "a whole request's one input came with its head: edit it with a filter";
            return Err(Fault::internal(whole));
        }
        let inputs = erase(wrap(EventStream::stream(self.take_inputs::<T>())).boxed());
        self.later = Arc::new(Later::wrapping(inputs, self.later.clone()));
        Ok(())
    }

    /// A call to `provider`, the one way to reach one: a copy of the request
    /// as it is now, sent when first awaited, carrying its inputs as filters
    /// and wraps left them, translated for another format of the kind.
    /// Every plugin's [`on_provider`](crate::Plugin::on_provider) runs
    /// first. A provider's
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
