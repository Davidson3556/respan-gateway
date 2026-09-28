use std::any::Any;
use std::future::ready;
use std::sync::{Arc, Mutex, PoisonError};

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use tokio::runtime::Handle;

use crate::call::OpenCalls;
use crate::format::{AnyFormat, SharedHead, is_input};
use crate::gateway::Inner;
use crate::input::{Input, Later, locked, stream};
use crate::lifecycle::{Edge, InFlight, InFlightGuard};
use crate::plugin::Filters;
use crate::transport::Head;
use crate::{Call, Exchange, Extensions, Fault, Filter, Outcome, Plugin, Response};

pub(crate) type HeadEdit = Box<dyn FnOnce(&mut Head) -> Result<(), Fault> + Send>;

/// The client edge's response edits. The `Mutex` is always reached through
/// `&mut`, so it never locks; it makes the holder `Sync` for end hooks.
#[derive(Default)]
pub(crate) struct Edits {
    /// Taken by the outgoing head, a refusal's included.
    pub(crate) head: Vec<HeadEdit>,
    /// Filters of the response sent to the client; they end with it, or
    /// with the request if it ends first.
    pub(crate) outputs: Filters,
}

pub(crate) fn unlocked<T>(edits: &mut Mutex<T>) -> &mut T {
    edits.get_mut().unwrap_or_else(PoisonError::into_inner)
}

/// One client request, as the client edge's hooks see it: body, inputs,
/// extensions, target, response edits and provider calls.
pub struct ClientContext {
    /// The first format served that accepted the request.
    pub(crate) format: Arc<dyn AnyFormat>,
    /// The head as the adapter handed it over: the formats' alone to read.
    pub(crate) head: SharedHead,
    pub(crate) extensions: Extensions,
    /// The provider called if every plugin passes.
    pub(crate) target: Option<String>,
    /// The input that came with the head; none for a streaming request.
    pub(crate) input: Input,
    /// A streaming request's later inputs.
    pub(crate) later: Option<Arc<Later>>,
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
    pub(crate) edits: Mutex<Edits>,
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

    fn hook<'a>(&'a self, plugin: &'a dyn Plugin, outcome: &'a Outcome) -> BoxFuture<'a, ()> {
        plugin.on_client_end(self, outcome)
    }

    /// Open calls end abandoned, the inputs left are cut off, and output
    /// filters still open end as the request did.
    fn close(&mut self, end: &Result<(), Fault>) {
        self.running = None;
        self.open.abandon();
        if let Some(later) = &self.later {
            later.end(Err(Fault::abandoned()));
        }
        let edits = unlocked(&mut self.edits);
        let _ = edits.outputs.end(end.clone(), &mut None);
    }
}

impl ClientContext {
    pub(crate) fn new(
        format: Arc<dyn AnyFormat>,
        head: SharedHead,
        extensions: Extensions,
        body: Option<Bytes>,
        later: Option<Later>,
        runtime: Handle,
        gateway: Arc<Inner>,
    ) -> Self {
        let target = gateway.target.clone();
        let cut = Arc::default();
        Self {
            input: Input::new(format.clone(), head.clone(), body),
            format,
            head,
            extensions,
            target,
            later: later.map(Arc::new),
            running: Some(InFlight::enter(&cut)),
            cut,
            runtime,
            settling: Arc::default(),
            open: Arc::default(),
            edits: Mutex::default(),
            gateway,
        }
    }

    /// The name of the client's format: the first served that accepted the
    /// request.
    pub fn format(&self) -> &str {
        self.format.name()
    }

    /// The request's extensions: values the adapter set, such as the
    /// client's credentials or a view of its head, or earlier plugins; on
    /// a provider edge, the call's
    /// ([`ProviderContext::extensions`](crate::ProviderContext::extensions)).
    /// The kernel reads none of them.
    pub fn extensions(&self) -> &Extensions {
        &self.extensions
    }

    /// Like [`ClientContext::extensions`], for changing them, such as to
    /// mark the request for later plugins or remove a credential no call
    /// should carry. Calls made later copy them.
    pub fn extensions_mut(&mut self) -> &mut Extensions {
        &mut self.extensions
    }

    /// The client's body as it arrived; empty for a streaming request.
    pub fn body(&self) -> &Bytes {
        self.input.raw()
    }

    /// Whether `T` is the client's format's
    /// [`Input`](crate::Format::Input) or [`Output`](crate::Format::Output)
    /// type, which the typed accessors take.
    pub fn is_kind<T: Any>(&self) -> bool {
        is_input::<T>(&*self.format).is_ok()
    }

    /// The input that came with the head as `T`, the client's
    /// [`Input`](crate::Format::Input) type, as plugins' filters left it.
    /// `Err` with [`codes::INTERNAL`](crate::codes::INTERNAL) for another
    /// type or a streaming request, whose inputs come after its head; or
    /// the decoder's or a filter's `Err`, the same on
    /// every read. A relay sends the client's body unread, so a plugin that
    /// must vet the input refuses what it cannot read.
    pub fn input<T: Any>(&self) -> Result<&T, Fault> {
        self.input.get()
    }

    /// The request's inputs as `T`, the client's
    /// [`Input`](crate::Format::Input) type, as plugins' filters left them:
    /// a copy of a whole request's one input, which calls still carry; a
    /// streaming request's as they arrive, read once, so no call gets what
    /// this takes. They are taken when first awaited, and given back if
    /// none was read. It ends when the client stops sending or the request
    /// ends, with an `Err` if the body broke; one `Err` with
    /// [`codes::INTERNAL`](crate::codes::INTERNAL) for another type, or if
    /// another reader took them.
    pub fn take_inputs<T: Any + Clone + Send>(&mut self) -> BoxStream<'static, Result<T, Fault>> {
        if is_input::<T>(&*self.format) != Ok(true) {
            return stream::once(async { Err(Fault::wrong_kind::<T>()) }).boxed();
        }
        match &self.later {
            None => stream::once(ready(self.input.get::<T>().cloned())).boxed(),
            Some(later) => {
                let input = self.input.clone();
                let frames = stream(later.clone(), input, Arc::default(), &self.cut);
                let value = |frame: crate::input::Frame| frame.input.downcast_ref::<T>().cloned();
                frames.try_filter_map(move |f| ready(Ok(value(f)))).boxed()
            }
        }
    }

    /// The provider called if every plugin passes; initially the gateway's
    /// ([`Gateway::target`](crate::Gateway::target)).
    pub fn target(&self) -> Option<&str> {
        self.target.as_deref()
    }

    /// Call `provider` if every plugin passes. Routers set the target so
    /// later plugins still run.
    pub fn set_target(&mut self, provider: impl Into<String>) {
        self.target = Some(provider.into());
    }

    /// A copy of it as it is now, for a call to keep.
    pub(crate) fn copy(&self) -> Self {
        Self {
            format: self.format.clone(),
            head: self.head.clone(),
            extensions: self.extensions.clone(),
            target: self.target.clone(),
            input: self.input.clone(),
            later: self.later.clone(),
            cut: self.cut.clone(),
            running: None,
            runtime: self.runtime.clone(),
            settling: self.settling.clone(),
            open: self.open.clone(),
            edits: Mutex::default(),
            gateway: self.gateway.clone(),
        }
    }

    pub(crate) fn edits(&mut self) -> &mut Edits {
        unlocked(&mut self.edits)
    }

    /// Edit the head sent to the client, refusals included, after the
    /// client's format wrote it: `H` is the client's format's
    /// [`Answer`](crate::Format::Answer), or the edit fails with
    /// [`codes::INTERNAL`](crate::codes::INTERNAL). Edits apply the last
    /// added first, as output filters do. An edit's `Err` or panic refuses
    /// the request with no edits. The adapter removes what a hop must not
    /// carry afterwards.
    pub fn edit_response_head<H: Any>(
        &mut self,
        f: impl FnOnce(&mut H) -> Result<(), Fault> + Send + 'static,
    ) {
        self.edits()
            .head
            .push(Box::new(move |head: &mut Head| match head.downcast_mut() {
                Some(head) => f(head),
                None => Err(Fault::internal(
                    "a head edit of another wire than the client's",
                )),
            }));
    }

    /// Run `filter`, a closure editing each value in place or a
    /// [`Filter`], over the request's inputs if `T` is the client's
    /// [`Input`](crate::Format::Input) type, or over the outputs sent to the
    /// client if it is its [`Output`](crate::Format::Output) type.
    ///
    /// Input filters are the one way to edit inputs, the first added first,
    /// before calls and [`ClientContext::take_inputs`] get them. A whole
    /// request's one input is edited at once and the filter ended; it must
    /// stay one input, as refusing is `Err`'s job. A streaming request's
    /// inputs are edited as they are read; once they ended, a filter sees
    /// their end at once. Output filters run after the provider edge's, the
    /// last added first.
    ///
    /// `Err` with [`codes::INTERNAL`](crate::codes::INTERNAL) for another
    /// type or a whole input filtered to none or several; or the filter's
    /// `Err` on that input, which reading or sending it then gets too; or
    /// the failure the inputs ended with.
    pub fn filter<T: Clone + PartialEq + Send + Sync + 'static>(
        &mut self,
        filter: impl Filter<T>,
    ) -> Result<(), Fault> {
        match is_input::<T>(&*self.format)? {
            false => unlocked(&mut self.edits).outputs.add(filter, false),
            true => match &self.later {
                Some(later) => locked(&later.filters).add(filter, true),
                None => self.input.filter(Filters::of(filter)),
            },
        }
    }

    /// A call to `provider`, the one way to reach one: a copy of the
    /// request as it is now, sent when first awaited, until the request
    /// ends. It carries the request's inputs as plugins left them,
    /// translated if the provider speaks another format of the kind: a
    /// whole request's one, or a streaming request's later ones no other
    /// reader took. Every plugin's [`on_provider`](crate::Plugin::on_provider)
    /// runs first. A provider's error answer is still `Ok` here: it fails
    /// the response as read ([`Response::until_content`]).
    ///
    /// `Err` with [`codes::CONFIG`](crate::codes::CONFIG) if `provider` is
    /// unknown or cannot take the client's inputs (neither format nor kind
    /// shared, or a streaming request's to a format that cannot carry
    /// them); with [`codes::INTERNAL`](crate::codes::INTERNAL) for a
    /// [`Call::with_input`] of another kind, or once the request ended; with
    /// [`codes::ABANDONED`](crate::codes::ABANDONED) if the request ends
    /// while it is pending; or the `Err` of an `on_provider`, an
    /// [`edit_call`](crate::ProviderContext::edit_call), the format's
    /// request codec or the transport.
    pub fn call(&mut self, provider: impl Into<String>) -> Call {
        Call::new(Arc::new(self.copy()), provider.into())
    }

    /// Relay a plugin's own response in the client's format, such as a
    /// cached one. The client's format decodes it as a provider's, so it
    /// can be read ([`Response::until_content`], [`Response::outputs`]), and writes
    /// it as it came until edited. It is read on no provider edge, so a
    /// plugin that meters calls there never sees it: to bill it, carry
    /// what it cost in an extension.
    /// `H` is the client's format's [`Answer`](crate::Format::Answer), else
    /// the response fails with [`codes::INTERNAL`](crate::codes::INTERNAL).
    pub fn relay<H: Any + Send + Sync>(&self, response: Exchange<H>) -> Response {
        let Exchange { head, body, .. } = response;
        let response = Exchange::new(Box::new(head) as Head, body);
        let streams = self.later.is_some();
        let request = (&*self.head, self.body());
        Response::decoded(&self.format, request, response, true, streams)
    }
}
