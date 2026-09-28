use std::any::Any;
use std::future::ready;
use std::sync::{Arc, Mutex, PoisonError};

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use http_body_util::BodyExt;
use tokio::runtime::Handle;

use crate::call::OpenCalls;
use crate::format::{AnyFormat, Request, is_input};
use crate::gateway::Inner;
use crate::input::{Input, Later, locked, stream};
use crate::lifecycle::{Edge, InFlight, InFlightGuard};
use crate::plugin::Filters;
use crate::{Call, Fault, Filter, Outcome, Plugin, Response};

pub(crate) type HeadEdit = Box<dyn FnOnce(&mut http::response::Parts) -> Result<(), Fault> + Send>;

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

/// One client request, as the client edge's hooks see it: head, body,
/// inputs, target, response edits and provider calls.
pub struct ClientContext {
    /// The first format served that accepted the request.
    pub(crate) format: Arc<dyn AnyFormat>,
    pub(crate) head: Arc<http::request::Parts>,
    /// The provider called if every plugin passes.
    pub(crate) target: Option<String>,
    /// The input that came with the head; none for an upgrading request.
    pub(crate) input: Input,
    /// An upgrading request's later inputs.
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
        head: http::request::Parts,
        body: Option<Bytes>,
        later: Option<Later>,
        runtime: Handle,
        gateway: Arc<Inner>,
    ) -> Self {
        let target = gateway.target.clone();
        let head = Arc::new(head);
        let cut = Arc::default();
        Self {
            input: Input::new(format.clone(), head.clone(), body),
            format,
            head,
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

    /// The client's request line and headers, without those its
    /// `Connection` names. Its `extensions` carry values set by the adapter
    /// or earlier plugins, and on a provider edge the call's
    /// ([`ProviderContext::extensions`](crate::ProviderContext::extensions)).
    pub fn head(&self) -> &http::request::Parts {
        &self.head
    }

    /// Like [`ClientContext::head`], for changing it, such as to remove the
    /// gateway's own credential before a relay passes headers on. Later
    /// calls are filled from it; the
    /// [`request_decoder`](crate::Format::request_decoder) reads the head as
    /// it arrived.
    pub fn head_mut(&mut self) -> &mut http::request::Parts {
        Arc::make_mut(&mut self.head)
    }

    /// The client's body as it arrived; empty for an upgrading request.
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
    /// type or an upgrading request, whose inputs come after its head (it
    /// has `Upgrade`); or the decoder's or a filter's `Err`, the same on
    /// every read. A relay sends the client's body unread, so a plugin that
    /// must vet the input refuses what it cannot read.
    pub fn input<T: Any>(&self) -> Result<&T, Fault> {
        self.input.get()
    }

    /// The request's inputs as `T`, the client's
    /// [`Input`](crate::Format::Input) type, as plugins' filters left them:
    /// a copy of a whole request's one input, which calls still carry; an
    /// upgrading request's as they arrive, read once, so no call gets what
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

    /// The client's request, its head as plugins left it and its body as
    /// it arrived.
    pub(crate) fn request(&self) -> Request {
        http::Request::from_parts((*self.head).clone(), self.body().clone())
    }

    pub(crate) fn edits(&mut self) -> &mut Edits {
        unlocked(&mut self.edits)
    }

    /// Edit the status or headers sent to the client, refusals included.
    /// Edits apply the last added first, as output filters do. An edit's
    /// `Err` or panic, or a 101 to a request that did not ask to switch
    /// protocols, refuses the request with no edits. Connection headers are
    /// removed afterwards. The client's format writes for the status before
    /// these edits ([`Response::with_status`]).
    pub fn edit_response_head(
        &mut self,
        f: impl FnOnce(&mut http::response::Parts) -> Result<(), Fault> + Send + 'static,
    ) {
        self.edits().head.push(Box::new(f));
    }

    /// Run `filter`, a closure editing each value in place or a
    /// [`Filter`], over the request's inputs if `T` is the client's
    /// [`Input`](crate::Format::Input) type, or over the outputs sent to the
    /// client if it is its [`Output`](crate::Format::Output) type.
    ///
    /// Input filters are the one way to edit inputs, the first added first,
    /// before calls and [`ClientContext::take_inputs`] get them. A whole
    /// request's one input is edited at once and the filter ended; it must
    /// stay one input, as refusing is `Err`'s job. An upgrading request's
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
    /// whole request's one, or an upgrading request's later ones no other
    /// reader took. Every plugin's [`on_provider`](crate::Plugin::on_provider)
    /// runs first. An error status is still `Ok`.
    ///
    /// `Err` with [`codes::CONFIG`](crate::codes::CONFIG) if `provider` is
    /// unknown or cannot take the client's inputs (neither format nor kind
    /// shared, or an upgrading request's to a call not switching
    /// protocols); with [`codes::INTERNAL`](crate::codes::INTERNAL) for a
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
    pub fn relay<B>(&self, response: http::Response<B>) -> Response
    where
        B: http_body::Body<Data = Bytes> + Send + 'static,
        B::Error: Into<Fault>,
    {
        let response = response.map(|body| body.map_err(Into::into).boxed_unsync());
        Response::decoded(&self.format, &self.request(), response, true)
    }
}
