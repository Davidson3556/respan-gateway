use std::any::Any;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::task::{Poll, Waker};

use bytes::{Bytes, BytesMut};
use futures::future::BoxFuture;
use futures::stream::{BoxStream, Stream};
use futures::{FutureExt, StreamExt, stream};

use crate::context::unlocked;
use crate::fault::PANICKED;
use crate::format::{AnyEncoder, AnyInput};
use crate::gateway::Registered;
use crate::input::{Frame as InputFrame, Input, locked, stream as inputs};
use crate::lifecycle::{EndHooks, InFlight};
use crate::provider::Provider;
use crate::response::Batch;
use crate::transport::Head;
use crate::{ClientContext, Exchange, Extensions, Fault, ProviderContext, Response, Transport};

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
    let mut input = cx.input.clone();
    if let Some(own) = own {
        if p.format.kind().0 != (*own).type_id() {
            return Err(Fault::another_kind(format!(
                "a call's own input is not the input type of provider '{provider}'"
            )));
        }
        input.bring(p.format.clone(), own);
    }
    // A call's own input is none of the client's, so never relayed.
    let relay = input.own_format.is_none() && cx.format.name() == p.format.name();
    let mut client = cx.copy();
    client.extensions.extend(extensions);
    let cut = Arc::default();
    let px = ProviderContext {
        client,
        format: p.format.clone(),
        provider,
        input,
        outputs: Mutex::default(),
        inputs: Arc::default(),
        call_edits: Mutex::default(),
        running: InFlight::enter(&cut),
        cut,
        sent: false,
        answered: false,
        began: false,
        failed: Arc::default(),
    };
    let mut edge = EndHooks::new(&cx.gateway.in_flight, px, None);
    let sent = async {
        edge.start().await?;
        send_call(&cx, p, relay, &mut edge).await
    };
    // A panic ends this call as internal before unwinding, so the drop
    // during unwinding does not end it abandoned.
    match AssertUnwindSafe(sent).catch_unwind().await {
        Ok(Ok(resp)) => Ok(resp.of_call(edge)),
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

/// Send `px`'s call to `p`, relaying the client's inputs if `relay`.
async fn send_call(
    cx: &ClientContext,
    p: &Provider,
    relay: bool,
    edge: &mut CallEdge,
) -> Result<Response, Fault> {
    let px = edge.cx();
    // A streaming request's later inputs follow, unless the call has its own.
    let later = cx.later.as_ref().filter(|_| px.input.own_format.is_none());
    let client = relay.then(|| &*px.client.head as &dyn Any);
    let (mut head, mut first, encoder, done) = first_frame(&px.input, p, client, later.is_some())?;
    // A body done as it opens has no later inputs.
    let later = later.filter(|_| !done);
    for edit in std::mem::take(unlocked(&mut px.call_edits)) {
        edit(&mut head, &mut first)?;
    }
    let extensions = px.client.extensions.clone();
    // The decoder reads the provider's head as it came, for the call as sent.
    let sent = p
        .format
        .copy_ask(&*head)
        .ok_or_else(|| Fault::internal("a call's head of another wire than its format's"))?;
    // An empty first frame is none, as an HTTP body of no bytes has none.
    let first_frame = stream::iter((!first.is_empty()).then(|| Ok(first.clone())));
    let body = match later {
        None => first_frame.boxed(),
        Some(later) => {
            let (input, filters) = (px.input.clone(), px.inputs.clone());
            let frames = inputs(later.clone(), input, filters, &px.cut);
            let rest = later_frames(frames, encoder, px.failed.clone());
            first_frame.chain(rest).boxed()
        }
    };
    let mut call = Exchange::new(head, body);
    call.extensions = extensions;
    call.relayed = relay;
    call.streams = later.is_some();
    px.sent = true;
    let resp = p.transport.send(call).await?;
    edge.cx().answered = true;
    Ok(Response::decoded(
        &p.format,
        &*sent,
        &first,
        resp,
        relay,
        later.is_some(),
    ))
}

/// The call's head as its format writes it, from the `client`'s head if it
/// relays the client's message; its first frame: the encoder's opening bytes,
/// then the client's bytes if relayed unread, else the input written from
/// its value, then the end unless it `streams`; the encoder; and whether the
/// body is done as it opens, when the first frame is the opening bytes
/// alone. Content that cannot be written is whose it is (`Whose`).
fn first_frame(
    input: &Input,
    p: &Provider,
    client: Option<&dyn Any>,
    streams: bool,
) -> Result<(Head, Bytes, Box<dyn AnyEncoder>, bool), Fault> {
    let ((mut body, first), whose) = input.sent(client.is_some())?;
    let mut encoder = p.format.request_encoder(client, &mut body, streams);
    let head = encoder.head(first.as_ref());
    // What cannot be opened is the format's to say, whoever's input it is.
    let mut opening = BytesMut::new();
    if encoder.open(&mut opening)? {
        return Ok((head, opening.freeze(), encoder, true));
    }
    let mut more = BytesMut::new();
    let mut write = || {
        if let Some(first) = &first {
            encoder.input(first, &mut more)?;
        }
        match streams {
            true => Ok(()),
            false => encoder.end(&Ok(()), &mut more),
        }
    };
    write().map_err(|fault| whose.fault(fault))?;
    if !opening.is_empty() || !more.is_empty() {
        body = [&opening[..], &body[..], &more[..]].concat().into();
    }
    Ok((head, body, encoder, false))
}

/// Where a streamed call's request encoder panicked, if it did.
type Failure = Arc<OnceLock<Fault>>;

/// A streamed call's frames after its first, as its body: each of `frames`
/// written by `encoder`, then, if it has bytes, the end. A frame that cannot
/// be written is whose it is (`Whose`).
///
/// The body ends with the first failure, its own or the frames' (the
/// client's body breaking, or the inputs cut off: `abandoned`), as an
/// `Err`, and only after the encoder was told that failure as its end, so
/// no clean end follows one. A panic in the encoder ends it `internal`, in
/// the body and, as the transport may read the body where nothing unwinds
/// to the call, in `failed`.
fn later_frames(
    frames: BoxStream<'static, Result<InputFrame, Fault>>,
    encoder: Box<dyn AnyEncoder>,
    failed: Failure,
) -> impl Stream<Item = Result<Bytes, Fault>> {
    let writing = Writing {
        frames,
        encoder,
        failed,
        done: false,
        abort: None,
    };
    stream::unfold(
        writing,
        |mut w| async move { w.next().await.map(|b| (b, w)) },
    )
}

struct Writing {
    frames: BoxStream<'static, Result<InputFrame, Fault>>,
    encoder: Box<dyn AnyEncoder>,
    failed: Failure,
    /// Whether the frames ended, or the body failed.
    done: bool,
    /// The failure to end the body with, once its last bytes are out.
    abort: Option<Fault>,
}

impl Writing {
    async fn next(&mut self) -> Option<Result<Bytes, Fault>> {
        loop {
            if let Some(fault) = self.abort.take() {
                return Some(Err(fault));
            }
            if self.done {
                return None;
            }
            let frame = self.frames.next().await;
            let mut out = BytesMut::new();
            let encoder = &mut self.encoder;
            let step = std::panic::catch_unwind(AssertUnwindSafe(|| match &frame {
                Some(Ok(frame)) => {
                    let written = encoder.input(&frame.input, &mut out);
                    written.map_err(|f| frame.whose.fault(f)).map(|()| true)
                }
                None => encoder.end(&Ok(()), &mut out).map(|()| false),
                Some(Err(fault)) => Err(fault.clone()),
            }));
            match step {
                Err(_) => return Some(Err(self.panicked())),
                Ok(Ok(more)) => {
                    self.done = !more;
                    // A frame is one, an empty one too; the end, if it has bytes.
                    if more || !out.is_empty() {
                        return Some(Ok(out.freeze()));
                    }
                }
                Ok(Err(fault)) => {
                    self.done = true;
                    // Its partial output is dropped; a failed end was told.
                    out.clear();
                    if frame.is_some() {
                        let ended =
                            AssertUnwindSafe(|| self.encoder.end(&Err(fault.clone()), &mut out));
                        match std::panic::catch_unwind(ended) {
                            Ok(Ok(())) => {}
                            Ok(Err(_)) => out.clear(),
                            Err(_) => {
                                self.panicked();
                                out.clear();
                            }
                        }
                    }
                    self.abort = Some(fault);
                    if !out.is_empty() {
                        return Some(Ok(out.freeze()));
                    }
                }
            }
        }
    }

    /// The encoder panicked: the body and the call end `internal`.
    fn panicked(&mut self) -> Fault {
        self.done = true;
        let fault = Fault::internal(PANICKED);
        let _ = self.failed.set(fault.clone());
        fault
    }
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

    /// How the request encoder failed, if it panicked and the call has not
    /// ended.
    pub(crate) fn failure(&mut self) -> Option<Fault> {
        match self.is_finished() {
            true => None,
            false => self.cx().failed.get().cloned(),
        }
    }

    /// Turn `batch` of this call's response into what its reader gets, as
    /// decoded: its outputs filtered. Whether it holds an end, a failed
    /// filter's too, for [`CallEdge::settle`] once read. Once the call
    /// ended, just an abandoned end.
    pub(crate) fn events(&mut self, batch: &mut Batch) -> bool {
        if self.is_finished() {
            *batch = Batch::end(Err(Fault::abandoned()));
            return false;
        }
        let px = self.cx();
        px.began |= batch.values.is_some() || batch.broke;
        let outputs = unlocked(&mut px.outputs);
        if let Some(values) = &mut batch.values
            && let Err(f) = outputs.run(values)
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
        let end = unlocked(&mut self.cx().outputs).end(end, &mut batch.values);
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
