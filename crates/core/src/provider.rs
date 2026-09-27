use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

use std::panic::AssertUnwindSafe;

use futures::future::BoxFuture;
use futures::stream::AbortHandle;
use http::Extensions;

use crate::context::{Filters, Input, unlocked};
use crate::format::AnyFormat;
use crate::gateway::{Edge, EndHooks, InFlight, InFlightGuard, PANICKED};
use crate::{Context, Event, Fault, Filter, Item, Outcome, Part, Plugin, Target};

/// One call to a provider, inside its request, handed to the provider
/// edge's hooks ([`Plugin::on_provider`](crate::Plugin::on_provider)):
/// this call's target, format and model, its own copies of the input and
/// the extensions, and filters of its response.
pub struct ProviderContext {
    /// The request as it was when the call was made.
    pub(crate) client: Context,
    /// The target's format.
    pub(crate) format: Arc<dyn AnyFormat>,
    pub(crate) target: Target,
    pub(crate) model: Option<String>,
    /// The client's head with this call's extensions.
    pub(crate) head: Arc<http::request::Parts>,
    pub(crate) input: Input,
    /// Kept in a `Mutex` for the reason `Context`'s edits are.
    pub(crate) filters: Mutex<Filters>,
    /// Cuts off its response's body, so a reader still waiting wakes.
    pub(crate) abort: Option<AbortHandle>,
    /// Held until this call's end hooks finish, which the request's wait
    /// for.
    pub(crate) _ends: InFlightGuard,
}

impl std::fmt::Debug for ProviderContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProviderContext")
            .field("target", &self.target)
            .field("model", &self.model)
            .finish_non_exhaustive()
    }
}

impl Edge for ProviderContext {
    fn chain(&self) -> &[Arc<dyn Plugin>] {
        &self.client.gateway.chain
    }

    fn end<'a>(&'a self, plugin: &'a dyn Plugin, outcome: &'a Outcome) -> BoxFuture<'a, ()> {
        plugin.on_provider_end(self, outcome)
    }

    /// Nothing reads its response past its end, so its body is cut off.
    fn close(&mut self, _end: &Result<(), Fault>) {
        if let Some(abort) = self.abort.take() {
            abort.abort();
        }
    }
}

impl ProviderContext {
    /// The request this call is in, as it was when the call was made: its
    /// format, head, body, input as plugins had left it, model, target and
    /// values set once per request (the head's `extensions`). Its
    /// [`Context::model`] is the request's; this call's is
    /// [`ProviderContext::model`]. Its [`Context::call`] is an `Err` with
    /// [`codes::INTERNAL`](crate::codes::INTERNAL).
    pub fn client(&self) -> &Context {
        &self.client
    }

    /// The provider this call goes to.
    pub fn target(&self) -> &Target {
        &self.target
    }

    /// The name of this call's target's format, such as to convert the
    /// input for it.
    pub fn format(&self) -> &str {
        self.format.name()
    }

    /// The model this call asks for: its own, else the request's.
    pub fn model(&self) -> Option<&str> {
        self.model.as_deref()
    }

    /// Ask for `model` in this call only.
    pub fn set_model(&mut self, model: impl Into<String>) {
        self.model = Some(model.into());
    }

    /// This call's input, as `T`, the client's format's input type: its
    /// own ([`Call::input`](crate::Call::input)), else the request's as
    /// plugins left it. `Err` as [`Context::input`]; reading changes
    /// nothing.
    pub fn input<T: Event>(&self) -> Result<&T, Fault> {
        let cx = &self.client;
        self.input.get(&*cx.format, &cx.arrived, &cx.body)
    }

    /// Like [`ProviderContext::input`], for changing this call's input
    /// only; if it did not actually change, a provider of the client's
    /// format still gets the client's body.
    pub fn input_mut<T: Event>(&mut self) -> Result<&mut T, Fault> {
        let cx = &self.client;
        self.input.get_mut(&*cx.format, &cx.arrived, &cx.body)
    }

    /// Run this call's response events through `filter`, before the client
    /// edge's filters, whoever reads it; the last added first. `Err` as
    /// [`Context::filter`].
    pub fn filter<T: Event>(&mut self, filter: impl Filter<T>) -> Result<(), Fault> {
        unlocked(&mut self.filters).add(&*self.client.format, filter)
    }

    /// This call's copy of the request's extensions, as its `on_provider`
    /// hooks left it; the request's own are `px.client().head().extensions`.
    pub fn extensions(&self) -> &Extensions {
        &self.head.extensions
    }

    /// This call's own copy of the extensions, such as for `on_provider`
    /// to hand a reservation to `on_provider_end`, or a value to the
    /// transport, such as a tenant's key, which gets them in its request
    /// ([`Transport::send`](crate::Transport::send)).
    pub fn extensions_mut(&mut self) -> &mut Extensions {
        &mut Arc::make_mut(&mut self.head).extensions
    }
}

/// One call's provider edge: its hooks, and its end hooks, run exactly
/// once, when its response ends or is dropped, or its request closes.
pub(crate) type CallEdge = EndHooks<ProviderContext>;

/// A call edge its response and its request share, so either can end it.
pub(crate) type SharedCall = Arc<Mutex<CallEdge>>;

/// Lock `call`, even if a panic poisoned it: its state stays whole.
pub(crate) fn lock(call: &Mutex<CallEdge>) -> MutexGuard<'_, CallEdge> {
    call.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A request's calls whose response may still be open, so that it can end
/// them when it closes, whoever holds their response.
#[derive(Default)]
pub(crate) struct OpenCalls {
    calls: Mutex<Vec<Weak<Mutex<CallEdge>>>>,
    /// Set once its plugins have answered: its calls' responses are no
    /// longer theirs to read.
    answered: AtomicBool,
}

impl OpenCalls {
    fn open(&self) -> MutexGuard<'_, Vec<Weak<Mutex<CallEdge>>>> {
        self.calls.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Its plugins have answered.
    pub(crate) fn answered(&self) {
        self.answered.store(true, Ordering::SeqCst);
    }

    /// Whether `call` is one of this request's.
    pub(crate) fn holds(&self, call: &SharedCall) -> bool {
        let call = Arc::downgrade(call);
        self.open().iter().any(|c| c.ptr_eq(&call))
    }

    /// End every call still open, as
    /// [`codes::ABANDONED`](crate::codes::ABANDONED), starting its end
    /// hooks, which the request's wait for.
    pub(crate) fn abandon(&self) {
        let open = std::mem::take(&mut *self.open());
        for call in open.iter().filter_map(Weak::upgrade) {
            lock(&call).abandon();
        }
    }
}

impl CallEdge {
    pub(crate) fn call(in_flight: &Arc<InFlight>, px: ProviderContext) -> Self {
        EndHooks::new(in_flight, px, None)
    }

    /// Whether its request's plugins have answered, so its response is not
    /// theirs to read; `false` once it ended.
    pub(crate) fn answered(&mut self) -> bool {
        !self.is_finished() && self.cx().client.open.answered.load(Ordering::SeqCst)
    }

    /// Share it with its response, whose body it cuts off at its end, and
    /// with its request.
    pub(crate) fn share(mut self, abort: AbortHandle) -> SharedCall {
        let px = self.cx();
        px.abort = Some(abort);
        let open = px.client.open.clone();
        let call = Arc::new(Mutex::new(self));
        let mut calls = open.open();
        calls.retain(|c| c.strong_count() > 0);
        calls.push(Arc::downgrade(&call));
        call
    }

    /// Run `on_provider` for each plugin in chain order until one refuses.
    pub(crate) async fn open(&mut self) -> Result<(), Fault> {
        let gateway = self.cx().client.gateway.clone();
        for plugin in &gateway.chain {
            self.ran += 1;
            plugin.on_provider(self.cx()).await?;
        }
        Ok(())
    }

    /// `parts`, all of this call's response, as the reader gets them: usage
    /// counted, items filtered, and at the end, which a filter's failure
    /// brings, what the filters held and the end. Nothing after it is kept.
    /// Its end hooks start once its reader is done with it
    /// ([`Source::end`](crate::response::Source::end), or its request's
    /// close for a response sent). Past its end, which its request closing
    /// brings, just that it was abandoned.
    pub(crate) fn parts(&mut self, parts: &mut Vec<Part>) {
        if self.is_finished() {
            parts.clear();
            return parts.push(Part::End(Err(Fault::abandoned())));
        }
        self.count(parts);
        let (mut end, mut fault) = (None, None);
        let filters = unlocked(&mut self.cx().filters);
        parts.retain_mut(|part| match part {
            _ if end.is_some() => false,
            Part::End(e) => {
                end = Some(std::mem::replace(e, Ok(())));
                false
            }
            Part::Usage(_) => true,
            _ if fault.is_some() => false,
            Part::Item(item) => match filters.item(item) {
                Ok(()) => true,
                Err(f) => {
                    fault = Some(f.unsaid());
                    false
                }
            },
            _ => true,
        });
        let end = match (fault, end) {
            (Some(f), _) => Err(f),
            (None, Some(end)) => end,
            (None, None) => return,
        };
        let (tail, end) = self.end_filters(end);
        parts.extend(tail.map(Part::Item));
        parts.push(Part::End(end));
    }

    /// Its request closed with its response still open: its filters get
    /// their `end`, what they held is dropped, and its end hooks start.
    fn abandon(&mut self) {
        if self.is_finished() {
            return;
        }
        // A filter that panics here must not stop the request's close.
        let end = AssertUnwindSafe(|| self.end_filters(Err(Fault::abandoned())).1);
        let end = std::panic::catch_unwind(end).unwrap_or(Err(Fault::internal(PANICKED)));
        self.finish(end);
    }

    /// The filters' ends, for a response ending `end`: what they held, and
    /// the end. They are taken, so each ends once.
    fn end_filters(&mut self, end: Result<(), Fault>) -> (Option<Item>, Result<(), Fault>) {
        let (status, usage) = (self.status, self.usage.clone());
        std::mem::take(unlocked(&mut self.cx().filters)).end(status, end, &usage)
    }
}
