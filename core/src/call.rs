use std::any::Any;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Poll, Waker};

use futures::channel::oneshot;
use futures::future::{BoxFuture, pending};
use futures::{FutureExt, StreamExt, stream};

use crate::events::Metadata;
use crate::fault::{PANICKED, guarded};
use crate::gateway::Registered;
use crate::input::Input;
use crate::leg::{Later, cut, inputs, locked};
use crate::lifecycle::{EndHooks, Latch};
use crate::provider::Provider;
use crate::values::AnyEvents;
use crate::{ClientContext, EventStream, Extensions, Fault, ProviderContext, Response, Result};

/// A call from [`ClientContext::call`], sent when first awaited; it may
/// outlive the hook. A clone is an unsent copy.
#[must_use = "a call is sent only when awaited"]
pub struct Call {
    request: Arc<ClientContext>,
    provider: String,
    /// Its own input, if any.
    own: Option<Arc<dyn Any + Send + Sync>>,
    extensions: Extensions,
    /// The call once sent, shared with its request, which drops it at its end.
    sent: Option<Arc<Mutex<CallSlot>>>,
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
            own: self.own.clone(),
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
            own: None,
            extensions: Extensions::new(),
            sent: None,
        }
    }

    /// Send `input`, such as a judge's prompt, in place of the client's. It
    /// must be the provider's input type, of any kind, or the call fails
    /// with [`codes::INTERNAL`](crate::codes::INTERNAL).
    pub fn with_input<T: Any + Send + Sync>(mut self, input: T) -> Self {
        self.own = Some(Arc::new(input));
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
    type Output = Result<Response>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let call = &mut *self;
        if call.sent.is_none() {
            let sent = send(
                call.request.clone(),
                call.provider.clone(),
                call.own.clone(),
                call.extensions.clone(),
            );
            let slot = Arc::new(Mutex::new(CallSlot {
                sending: Some(sent.boxed()),
                waker: None,
            }));
            if !call.request.open.add(&slot) {
                let ended = "providers can be called until the request ends";
                return Poll::Ready(Err(Fault::internal(ended)));
            }
            call.sent = Some(slot);
        }
        let mut slot = locked(call.sent.as_ref().expect("set above"));
        let Some(sending) = &mut slot.sending else {
            return Poll::Ready(Err(Fault::abandoned()));
        };
        let sent = sending.poll_unpin(cx);
        match sent {
            Poll::Ready(_) => slot.sending = None,
            Poll::Pending => slot.waker = Some(cx.waker().clone()),
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
/// `Call::poll` in `take_until` cannot do this, because it only runs when
/// the call is polled again.
struct CallSlot {
    /// Emptied when it resolves or its request ends.
    sending: Option<BoxFuture<'static, Result<Response>>>,
    waker: Option<Waker>,
}

async fn send(
    cx: Arc<ClientContext>,
    provider: String,
    own: Option<Arc<dyn Any + Send + Sync>>,
    extensions: Extensions,
) -> Result<Response> {
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
    let (input, later) = match own {
        None => {
            let later = cx.input.is_live().then(|| cx.later.clone());
            (cx.input.clone(), later)
        }
        Some(input) => {
            if p.format.kind().0 != (*input).type_id() {
                return Err(Fault::another_kind(format!(
                    "a call's own input is not the input type of provider '{provider}'"
                )));
            }
            (Input::own(p.format.clone(), p.format.input(&*input)?), None)
        }
    };
    // The call keeps its snapshot for its unsent clones; its hooks get their
    // own copy, with the call's extensions.
    let mut client = cx.copy();
    client.extensions.extend(extensions);
    let px = ProviderContext {
        client,
        format: p.format.clone(),
        provider,
        live: later.is_some(),
        input,
        outputs: Arc::default(),
        inputs: Arc::default(),
        ended: Latch::default(),
        settling: None,
        sent: false,
        refused: false,
    };
    let mut edge = EndHooks::new(&cx.gateway.in_flight, px, None);
    let sent = async {
        edge.start().await?;
        dial(p, later, &mut edge).await
    };
    // A panic ends this call as internal before unwinding, so the drop
    // during unwinding does not end it abandoned.
    match AssertUnwindSafe(sent).catch_unwind().await {
        Ok(Ok((metadata, events))) => Ok(Response::of_call(metadata, events, edge)),
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

/// Open receiving and close the sending sink concurrently. A call counts
/// as sent only after the sink has accepted its input sequence. `later` are
/// the client edge's live inputs, if the call carries them.
async fn dial(
    p: &Provider,
    later: Option<Arc<Later>>,
    edge: &mut CallEdge,
) -> Result<(Metadata, AnyEvents)> {
    let px = edge.cx();
    let relay = !px.input.own && px.client.format.name() == p.format.name();
    let (fail, mut failed) = oneshot::channel();
    let mut outgoing = match later {
        Some(later) => {
            let call = Some((px.inputs.clone(), fail));
            EventStream::stream(inputs(later, call, &px.ended))
        }
        None => px.input.events(relay)?,
    };
    if relay {
        outgoing.set_metadata(px.client.input.metadata_copy());
    }
    let (opening, mut sink) = guarded(|| {
        let io = (p.factory)(px.client.extensions.clone());
        p.format.bind(io)
    })?;
    // Preparation is synchronous: a failure here has not sent the call.
    // The pump progresses alongside opening and response reads.
    guarded(|| sink.prepare(&mut outgoing))?;
    px.sent = true;
    let writing = crate::events::deliver(outgoing, sink);
    let opened = match futures::future::select(writing, opening).await {
        futures::future::Either::Left((written, opening)) => match written {
            Ok(()) => opening.await.map(|incoming| (incoming, None)),
            Err(fault) => Err(fault),
        },
        futures::future::Either::Right((opened, writing)) => {
            opened.map(|incoming| (incoming, Some(writing)))
        }
    };
    let (mut incoming, writing) = match opened {
        Ok(opened) => opened,
        Err(fault) => return Err(failed.try_recv().ok().flatten().unwrap_or(fault)),
    };
    px.refused = incoming.refusal().is_some();
    // A refusal never consumes the outgoing half. Release it before the
    // response is handed to routing, which may retain this attempt.
    let writing = if px.refused { None } else { writing };
    if !relay {
        incoming.clear_metadata();
    }
    let metadata = incoming.metadata_copy();
    let events = drive_writer(incoming.boxed(), writing);
    let failed = async {
        match failed.await {
            Ok(fault) => fault,
            Err(_) => pending().await,
        }
    };
    Ok((metadata, cut(events, failed.boxed())))
}

/// Keep the other half progressing as the response is read. Dropping the
/// response also drops any unfinished writer, cancelling that half.
fn drive_writer(events: AnyEvents, writing: Option<BoxFuture<'static, Result<()>>>) -> AnyEvents {
    stream::unfold(
        (events, writing, false),
        |(mut events, mut writing, done)| async move {
            if done {
                return None;
            }
            loop {
                let Some(write) = writing.take() else {
                    let event = events.next().await?;
                    let done = event.is_err();
                    return Some((event, (events, None, done)));
                };
                match futures::future::select(write, events.next()).await {
                    futures::future::Either::Left((Ok(()), _)) => continue,
                    futures::future::Either::Left((Err(fault), _)) => {
                        return Some((Err(fault), (events, None, true)));
                    }
                    futures::future::Either::Right((Some(event), write)) => {
                        let done = event.is_err();
                        return Some((event, (events, (!done).then_some(write), done)));
                    }
                    futures::future::Either::Right((None, write)) => {
                        // The receiving direction can finish before the
                        // outgoing direction flushes its last data.
                        return match write.await {
                            Ok(()) => None,
                            Err(fault) => Some((Err(fault), (events, None, true))),
                        };
                    }
                }
            }
        },
    )
    .boxed()
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
impl Open for Mutex<CallSlot> {
    fn abandon(&self) {
        let mut slot = locked(self);
        let (sending, waker) = (slot.sending.take(), slot.waker.take());
        drop(slot);
        drop(sending);
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
/// Its `abandon` is the synchronous handle [`CallSlot`] explains.
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
    pub(crate) async fn start(&mut self) -> Result<()> {
        let gateway = self.cx().client.gateway.clone();
        for Registered { plugin, .. } in &gateway.plugins {
            plugin.on_provider(self.begin()).await?;
        }
        Ok(())
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
