use std::any::Any;
use std::future::{Future, ready};
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Poll, Waker};

use bytes::{Bytes, BytesMut};
use futures::future::BoxFuture;
use futures::stream::{BoxStream, Stream};
use futures::{FutureExt, StreamExt, stream};
use http::Extensions;
use http_body::Frame;
use http_body_util::{BodyExt, StreamBody};

use crate::context::unlocked;
use crate::fault::PANICKED;
use crate::format::{AnyEncoder, AnyInput};
use crate::input::{Frame as InputFrame, Input, locked, stream as inputs};
use crate::lifecycle::{EndHooks, InFlight};
use crate::provider::{Provider, provider_for};
use crate::response::Batch;
use crate::transport::one_frame;
use crate::{ClientContext, Fault, ProviderContext, Response};

/// A call from [`ClientContext::call`]: a copy of the request as it was
/// then, sent when first awaited, which may outlive the hook. A clone is an
/// unsent copy. Its settings apply to it alone.
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

    /// Send `input`, such as a judge's prompt, in place of the client's
    /// inputs, written from its value. The provider may be of another kind;
    /// `input` must be its format's input type, or the call fails with
    /// [`codes::INTERNAL`](crate::codes::INTERNAL).
    pub fn with_input<T: Any + Send + Sync>(mut self, input: T) -> Self {
        self.input = Some(Arc::new(input));
        self
    }

    /// Put `value` in this call's extensions
    /// ([`ProviderContext::extensions`]), for its hooks and transport, such
    /// as a mark a plugin's `on_provider` reads to filter this call alone.
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
    let providers = &cx.gateway.providers;
    let p = match &own {
        Some(_) => providers
            .get(&provider)
            .ok_or_else(|| Fault::config(format!("unknown provider '{provider}'"))),
        None => provider_for(&*cx.format, providers, &provider),
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
    let client = relay.then(|| &*px.client.head);
    let extensions = px.client.extensions.clone();
    let (mut call, encoder) = first_frame(&px.input, p, client, extensions, later.is_some())?;
    for edit in std::mem::take(unlocked(&mut px.call_edits)) {
        edit(&mut call)?;
    }
    let (head, first) = call.into_parts();
    let sent = head.clone();
    let body = match later {
        None => one_frame(first.clone()),
        Some(later) => {
            let (input, filters) = (px.input.clone(), px.inputs.clone());
            let frames = inputs(later.clone(), input, filters, &px.cut);
            let first = (!first.is_empty()).then(|| Ok(first.clone()));
            let frames = stream::iter(first).chain(later_frames(frames, encoder));
            StreamBody::new(frames.map(|bytes| bytes.map(Frame::data))).boxed_unsync()
        }
    };
    let call = http::Request::from_parts(head, body);
    let resp = p.transport.send(call).await?;
    // The decoder reads the provider's head as it came, for the call as sent.
    let sent = http::Request::from_parts(sent, first);
    Ok(Response::decoded(
        &p.format,
        &sent,
        resp,
        relay,
        later.is_some(),
    ))
}

/// The call as its format writes it, from the `client`'s head if it relays
/// the client's message, with the call's `extensions` and its first frame
/// as its body (the client's bytes if relayed unread, else the encoder's
/// opening bytes, the input, and the end unless it `streams`), and the
/// encoder. Content that cannot be written is whose it is (`Whose`).
fn first_frame(
    input: &Input,
    p: &Provider,
    client: Option<&http::request::Parts>,
    extensions: Extensions,
    streams: bool,
) -> Result<(http::Request<Bytes>, Box<dyn AnyEncoder>), Fault> {
    let ((body, first), whose) = input.sent(client.is_some())?;
    let mut call = match client {
        Some(head) => http::Request::from_parts(head.clone(), body),
        None => http::Request::new(body),
    };
    *call.extensions_mut() = extensions;
    let mut encoder = p.format.request_encoder(&mut call, first.as_ref(), streams);
    // What cannot be opened is the format's to say, whoever's input it is.
    let mut more = BytesMut::new();
    encoder.open(&mut more)?;
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
    if !more.is_empty() {
        let body = call.body_mut();
        let mut all = BytesMut::from(&body[..]);
        all.unsplit(more);
        *body = all.freeze();
    }
    Ok((call, encoder))
}

/// A streamed call's frames after its first: each of `frames` written by
/// `encoder`, then the end, if it has bytes. A frame that cannot be written
/// is whose it is (`Whose`); a failure aborts the body.
fn later_frames(
    frames: BoxStream<'static, Result<InputFrame, Fault>>,
    mut encoder: Box<dyn AnyEncoder>,
) -> impl Stream<Item = Result<Bytes, Fault>> {
    let frames = frames.map(|frame| frame.map(Some));
    let end = stream::iter([Ok(None)]);
    frames.chain(end).filter_map(move |frame| {
        let mut out = BytesMut::new();
        let written = frame.and_then(|frame| match frame {
            Some(frame) => {
                let written = encoder.input(&frame.input, &mut out);
                written.map_err(|f| frame.whose.fault(f))?;
                Ok(Some(out.freeze()))
            }
            None => {
                encoder.end(&Ok(()), &mut out)?;
                Ok((!out.is_empty()).then(|| out.freeze()))
            }
        });
        ready(written.transpose())
    })
}

/// One call's provider edge, whose end hooks run exactly once.
pub(crate) type CallEdge = EndHooks<ProviderContext>;

/// A call edge shared by its response and request, so either can end it.
pub(crate) type SharedCall = Arc<Mutex<CallEdge>>;

/// What a request ends when it ends: a call being sent, or an open response.
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
        for plugin in &gateway.chain {
            self.ran += 1;
            plugin.on_provider(self.cx()).await?;
        }
        Ok(())
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
        let outputs = unlocked(&mut self.cx().outputs);
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
