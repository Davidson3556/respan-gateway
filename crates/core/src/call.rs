use std::any::Any;
use std::future::{Future, ready};
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Poll, Waker};

use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt, stream};

use crate::fault::{PANICKED, guarded};
use crate::format::{AnyEvents, AnyInput, Origin};
use crate::gateway::Registered;
use crate::leg::{self, inputs, locked};
use crate::lifecycle::{EndHooks, InFlight};
use crate::provider::Provider;
use crate::response::Batch;
use crate::{ClientContext, Extensions, Fault, ProviderContext, Response};

/// A call from [`ClientContext::call`], sent when first awaited; it may
/// outlive the hook. A clone is an unsent copy.
#[must_use = "a call is sent only when awaited"]
pub struct Call {
    request: Arc<ClientContext>,
    provider: String,
    /// Its own input.
    input: Option<AnyInput>,
    extensions: Extensions,
    /// The call once sent, shared with its request, which drops it at its end.
    sent: Option<Arc<Sending>>,
}

impl std::fmt::Debug for Call {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Call")
            .field("provider", &self.provider)
            .finish_non_exhaustive()
    }
}

/// An unsent copy.
impl Clone for Call {
    fn clone(&self) -> Self {
        Self {
            request: self.request.clone(),
            provider: self.provider.clone(),
            input: self.input.clone(),
            extensions: self.extensions.clone(),
            sent: None,
        }
    }
}

impl Call {
    pub(crate) fn new(request: Arc<ClientContext>, provider: String) -> Self {
        Self {
            request,
            provider,
            input: None,
            extensions: Extensions::new(),
            sent: None,
        }
    }

    /// Send `input`, such as a judge's prompt, in place of the client's. It
    /// must be the provider's input type, of any kind, or the call fails
    /// with [`codes::INTERNAL`](crate::codes::INTERNAL).
    pub fn with_input<T: Any + Send + Sync>(mut self, input: T) -> Self {
        self.input = Some(Arc::new(input));
        self
    }

    /// Put `value` in this call's extensions, for its hooks and transport,
    /// such as a mark `on_provider` reads to filter this call alone.
    pub fn with_extension<T: Clone + Send + Sync + 'static>(mut self, value: T) -> Self {
        self.extensions.insert(value);
        self
    }
}

impl Future for Call {
    type Output = Result<Response, Fault>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let call = &mut *self;
        if call.sent.is_none() {
            let sent = send(
                call.request.clone(),
                call.provider.clone(),
                call.input.clone(),
                call.extensions.clone(),
            );
            let sent = Arc::new(Mutex::new((Some(sent.boxed()), None)));
            if !call.request.open.add(&sent) {
                let ended = "providers can be called until the request ends";
                return Poll::Ready(Err(Fault::internal(ended)));
            }
            call.sent = Some(sent);
        }
        let mut sending = locked(call.sent.as_ref().expect("set above"));
        let Some(sent) = &mut sending.0 else {
            return Poll::Ready(Err(Fault::abandoned()));
        };
        let sent = sent.poll_unpin(cx);
        match sent {
            Poll::Ready(_) => sending.0 = None,
            Poll::Pending => sending.1 = Some(cx.waker().clone()),
        }
        sent
    }
}

/// A sent call until it resolves, and who awaits it.
///
/// Why a call is a slot its request can empty, not a future driven in place:
/// a call polled once and then parked, never polled again, must still end
/// abandoned when its request ends. Only the request's end runs then, so it
/// needs a synchronous handle to drop the future ([`Open::abandon`]); wrapping
/// `Call::poll` in `take_until` cannot, as that runs only when polled. It was
/// tried and reverted; the test
/// `a_pending_call_left_unpolled_still_ends_with_its_request`
/// (`crates/testkit/tests/streams.rs`) pins it.
type Sending = Mutex<(
    Option<BoxFuture<'static, Result<Response, Fault>>>,
    Option<Waker>,
)>;

async fn send(
    cx: Arc<ClientContext>,
    provider: String,
    own: Option<AnyInput>,
    extensions: Extensions,
) -> Result<Response, Fault> {
    // An own input sends nothing of the client's, so only its type must
    // match; provider details go to logs.
    let p = match cx.gateway.providers.get(&provider) {
        None => Err(format!("unknown provider '{provider}'")),
        // Relayed within one name, else translated within one kind.
        Some(p)
            if own.is_none()
                && cx.format.name() != p.format.name()
                && cx.format.kind() != p.format.kind() =>
        {
            Err(format!(
                "the client speaks {}, provider '{provider}' speaks {}, and they share no kind",
                cx.format.name(),
                p.format.name()
            ))
        }
        Some(p) => Ok(p),
    };
    let p = p.map_err(|e| {
        Fault::config("a provider this request cannot use was called").with_source(e)
    })?;
    let mut opening = cx.opening.clone();
    if let Some(own) = own {
        if p.format.kind().0 != (*own).type_id() {
            return Err(Fault::another_kind(format!(
                "a call's own input is not the input type of provider '{provider}'"
            )));
        }
        opening.bring(p.format.clone(), own);
    }
    let mut client = cx.copy();
    client.extensions.extend(extensions);
    let cut = Arc::default();
    let px = ProviderContext {
        client,
        format: p.format.clone(),
        provider,
        // A call's own input is none of the client's: nothing follows it.
        flowing: opening.own.is_none() && !cx.later.ended,
        opening,
        outputs: Arc::default(),
        inputs: Arc::default(),
        running: InFlight::enter(&cut),
        cut,
        sent: false,
        refused: false,
    };
    let mut edge = EndHooks::new(&cx.gateway.in_flight, px, None);
    let sent = async {
        edge.start().await?;
        dial(&cx, p, &mut edge).await
    };
    // A panic ends this call as internal before unwinding, so the drop
    // during unwinding does not end it abandoned.
    match AssertUnwindSafe(sent).catch_unwind().await {
        Ok(Ok((origin, events))) => Ok(Response::of_call(&p.format, origin, events, edge)),
        Ok(Err(fault)) => {
            edge.finish(Err(fault.clone()));
            Err(fault)
        }
        Err(panic) => {
            edge.finish(Err(Fault::internal(PANICKED)));
            std::panic::resume_unwind(panic)
        }
    }
}

/// The one way a client's origin reaches a provider: a call that carries
/// the session's own inputs (no input of its own, which no plugin can
/// forge) to a format of the client's name.
fn relay_origin(cx: &ClientContext, own: bool, p: &Provider) -> Option<Origin> {
    (!own && cx.format.name() == p.format.name())
        .then(|| cx.origin.clone())
        .flatten()
}

/// Dial `p` with the call's opening and later inputs: the provider's origin
/// (on a relay alone) and events, a refusal at the head as one `Err`.
async fn dial(
    cx: &ClientContext,
    p: &Provider,
    edge: &mut CallEdge,
) -> Result<(Option<Origin>, AnyEvents), Fault> {
    let px = edge.cx();
    let relay = relay_origin(cx, px.opening.own.is_some(), p);
    let opening = px.opening.sent(relay.is_some())?;
    let later = px.flowing.then(|| {
        let call = Some(px.inputs.clone());
        inputs(cx.later.clone(), cx.format.clone(), call, &px.cut)
    });
    let (dialer, ext) = (&*p.dialer, px.client.extensions.clone());
    let dialed = guarded(|| {
        p.format
            .dial(dialer, ext, relay.as_ref(), opening.as_ref(), later)
    })?;
    px.sent = true;
    let (origin, events) = dialed.await?;
    // A provider's head goes only to a client of the name it came in.
    let origin = relay.and(origin);
    Ok(match events {
        Ok(events) => (origin, events),
        Err(fault) => {
            edge.cx().refused = true;
            (origin, stream::once(ready(Err(fault))).boxed())
        }
    })
}

/// One call's provider edge, whose end hooks run exactly once.
pub(crate) type CallEdge = EndHooks<ProviderContext>;

/// A call edge shared by its response and request, so either can end it.
pub(crate) type SharedCall = Arc<Mutex<CallEdge>>;

/// What a request ends when it ends: a call being sent, or an open response.
/// The request holds each weakly in [`OpenCalls`] and ends them by `&self`, so
/// no one has to poll them (see [`Sending`]).
trait Open: Send + Sync {
    fn abandon(&self);
}

/// Dropped, its end hooks run abandoned; whoever awaits it wakes.
impl Open for Sending {
    fn abandon(&self) {
        let (sent, waker) = std::mem::take(&mut *locked(self));
        drop(sent);
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl Open for Mutex<CallEdge> {
    fn abandon(&self) {
        locked(self).finish(Err(Fault::abandoned()));
    }
}

/// A request's calls being sent or with a response open; `None` once it ended.
/// Its `abandon` is the synchronous handle [`Sending`] explains.
pub(crate) struct OpenCalls(Mutex<Option<Vec<Weak<dyn Open>>>>);

impl Default for OpenCalls {
    fn default() -> Self {
        Self(Mutex::new(Some(Vec::new())))
    }
}

impl OpenCalls {
    /// Whether `call` is one of this request's.
    pub(crate) fn holds(&self, call: &SharedCall) -> bool {
        let calls = locked(&self.0);
        let call = Arc::as_ptr(call);
        calls
            .iter()
            .flatten()
            .any(|c| std::ptr::addr_eq(c.as_ptr(), call))
    }

    /// Add `call`; `false` once the request has ended.
    fn add<T: Open + 'static>(&self, call: &Arc<T>) -> bool {
        let Some(calls) = &mut *locked(&self.0) else {
            return false;
        };
        calls.retain(|c| c.strong_count() > 0);
        calls.push(Arc::downgrade(call) as Weak<dyn Open>);
        true
    }

    /// End every call [`codes::ABANDONED`](crate::codes::ABANDONED).
    pub(crate) fn abandon(&self) {
        let calls = locked(&self.0).take().unwrap_or_default();
        for call in calls.iter().filter_map(Weak::upgrade) {
            call.abandon();
        }
    }
}

impl CallEdge {
    /// Share it with its request and its response.
    pub(crate) fn share(mut self) -> SharedCall {
        let open = self.cx().client.open.clone();
        let call = Arc::new(Mutex::new(self));
        if !open.add(&call) {
            call.abandon();
        }
        call
    }

    /// Run each plugin's `on_provider` until one refuses.
    pub(crate) async fn start(&mut self) -> Result<(), Fault> {
        let gateway = self.cx().client.gateway.clone();
        for Registered { plugin, .. } in &gateway.plugins {
            self.ran += 1;
            plugin.on_provider(self.cx()).await?;
        }
        Ok(())
    }

    /// Turn `batch` of this call's response into what its reader gets, as
    /// the provider edge gave it: its outputs filtered. Whether it holds an end, a failed
    /// filter's too, for [`CallEdge::settle`] once read. Once the call
    /// ended, just an abandoned end.
    pub(crate) fn events(&mut self, batch: &mut Batch) -> bool {
        if self.is_finished() {
            *batch = Batch::end(Err(Fault::abandoned()));
            return false;
        }
        if let Some(values) = &mut batch.values
            && let Err(f) = leg::run(&[&self.cx().outputs], values)
        {
            batch.end = Some(Err(f));
        }
        batch.end.is_some()
    }

    /// At the end in `batch`, as it is read: what filters kept back, and the
    /// call's end. Once the call ended, just an abandoned end.
    pub(crate) fn settle(&mut self, batch: &mut Batch) {
        if self.is_finished() {
            *batch = Batch::end(Err(Fault::abandoned()));
            return;
        }
        let Some(end) = batch.end.take() else {
            return;
        };
        let end = leg::end(&[&self.cx().outputs], end, &mut batch.values);
        batch.end = Some(end.clone());
        self.finish(end);
    }
}

#[cfg(test)]
mod tests {
    /// A call can be kept in shared state, such as an extension or a
    /// plugin's `Arc`, without a `Mutex`.
    #[test]
    fn a_call_is_send_and_sync() {
        fn shareable<T: Send + Sync>() {}
        shareable::<super::Call>();
    }
}
