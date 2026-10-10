//! Client input and its one response handoff.

use super::writer::Writer;
use super::{PANICKED, Written};
use crate::{
    Body, ClientHead, Completion, Credential, Encoder, Frames, RawReply, Status, Unwritten, Wire,
    bodiless, error_head, plain_response,
};
use bytes::{Bytes, BytesMut};
use futures::channel::oneshot;
use futures::{FutureExt, Sink, StreamExt, TryStreamExt};
use http::StatusCode;
use http::response::Parts as ResponseHead;
use respan_gateway_core::{Connection, ErrorCode, EventSink, EventStream, Extensions, Fault};
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, ready};

type ErrorWriter = dyn Fn(&Fault, &http::Request<Bytes>, &mut ResponseHead) -> Bytes + Send + Sync;
type Slot = Arc<Mutex<Handoff>>;
struct Handoff {
    sender: Option<oneshot::Sender<Written>>,
    staged: Option<Written>,
    committed: bool,
    failed: Option<Fault>,
    /// Whether the response has no body, so its source goes unread.
    unread: bool,
}
impl Handoff {
    fn publish(&mut self, response: Written) {
        if !self.committed {
            self.staged = Some(response);
            return;
        }
        if self
            .sender
            .take()
            .is_none_or(|sender| sender.send(response).is_err())
        {
            self.failed = Some(Fault::abandoned());
        }
    }
    fn commit(&mut self) {
        self.committed = true;
        if let Some(response) = self.staged.take() {
            self.publish(response);
        }
    }
}

/// How an accepted request's body carries its messages.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Framing {
    /// One message, the whole body, collected before it is decoded: an
    /// ordinary HTTP request, however the network chunked it.
    Whole,
    /// One message per body frame, decoded as each comes: a session's, such
    /// as a WebSocket's after its upgrade. Its response opens at once, so a
    /// duplex peer can send without waiting for its own first reply.
    Live,
}

/// One accepted request. It is consumed once, rather than shared and runtime-locked.
///
/// An accepted request cannot be cloned:
/// ```compile_fail
/// use respan_gateway_http::{Body, Framing, ClientIo, http::Request};
/// let (incoming, _) = ClientIo::accept(Request::new(Body::default()), Framing::Whole, 1024);
/// let duplicate = incoming.clone();
/// ```
/// Binding consumes it, preventing a second binding:
/// ```compile_fail
/// use respan_gateway_http::{self as h, formats::opaque::Passthrough};
/// use respan_gateway_core::Format;
/// let request = h::http::Request::new(h::Body::default());
/// let (incoming, _) = h::ClientIo::accept(request, h::Framing::Whole, 1024);
/// let first = Passthrough.bind(incoming);
/// let second = Passthrough.bind(incoming);
/// ```
pub struct ClientIo {
    request: http::Request<Body>,
    live: bool,
    limit: usize,
    wire: Wire,
    head: ClientHead,
    credential: Option<Credential>,
    reply: oneshot::Sender<Written>,
}
impl ClientIo {
    /// Accept an HTTP request, its credentials lifted out of its head into a
    /// [`Credential`], its body framed as `framing`. `limit` is the most
    /// bytes of one message it takes, a whole body or a live frame: past it,
    /// the request fails 413 (`too_large`). Hand the gateway its
    /// [`extensions`](Self::extensions), and drive the returned answer
    /// together with `Gateway::handle`.
    pub fn accept(
        request: http::Request<Body>,
        framing: Framing,
        limit: usize,
    ) -> (Self, futures::future::BoxFuture<'static, Option<Written>>) {
        let live = framing == Framing::Live;
        let (mut head, body) = request.into_parts();
        let credential = Credential::lift(&mut head);
        let wire = Wire::unread(live);
        if live {
            wire.fill(http::Request::from_parts(head.clone(), Bytes::new()));
        }
        let (reply, answer) = oneshot::channel();
        (
            Self {
                request: http::Request::from_parts(head, body),
                live,
                limit,
                wire,
                head: ClientHead::default(),
                credential,
                reply,
            },
            async move { answer.await.ok() }.boxed(),
        )
    }
    /// What the gateway is handed with this request: its [`Wire`], its
    /// [`ClientHead`] and the client's [`Credential`], if it sent one.
    pub fn extensions(&self) -> Extensions {
        let mut extensions = Extensions::new();
        extensions.insert(self.wire.clone());
        extensions.insert(self.head.clone());
        if let Some(credential) = &self.credential {
            extensions.insert(credential.clone());
        }
        extensions
    }
    /// The request, available before binding for protocol recognition.
    pub fn request(&self) -> &http::Request<Body> {
        &self.request
    }

    /// Bind a request's decoder, its response's encoder and the format's
    /// error writer to core events. `decode` reads the request: its whole
    /// body, collected up to the limit, or its live frames ([`messages`]
    /// decodes either). `encode` makes the response's sink from a [`Reply`].
    /// `error` writes a refusal's body, and may edit its head, whose status
    /// and `Retry-After` the fault's are ([`status`], [`RetryAfter`]); an
    /// explicit [`RawReply`] is written instead, as it came.
    ///
    /// [`messages`]: crate::messages
    /// [`status`]: crate::status
    /// [`RetryAfter`]: respan_gateway_core::RetryAfter
    pub fn bind<I, O, D, E, F>(self, decode: D, encode: E, error: F) -> Connection<I, O>
    where
        I: Clone + Send + Sync + 'static,
        O: Clone + Send + Sync + 'static,
        D: FnOnce(http::Request<Body>) -> EventStream<I> + Send + 'static,
        E: FnOnce(Reply) -> Result<EventSink<O>, Fault> + Send + 'static,
        F: Fn(&Fault, &http::Request<Bytes>, &mut ResponseHead) -> Bytes + Send + Sync + 'static,
    {
        let Self {
            request,
            live,
            limit,
            reply,
            wire,
            head: client_head,
            ..
        } = self;
        let (head, body) = request.into_parts();
        let filled = wire.clone();
        let incoming = async move {
            if live {
                let frames = body.map(move |frame| match frame {
                    Ok(bytes) if bytes.len() > limit => Err(too_large()),
                    frame => frame,
                });
                return Ok(decode(http::Request::from_parts(
                    head,
                    Body::stream(frames),
                )));
            }
            let whole = read_whole(body, limit).await;
            let raw = whole.as_ref().cloned().unwrap_or_default();
            filled.fill(http::Request::from_parts(head.clone(), raw));
            Ok(decode(http::Request::from_parts(head, Body::full(whole?))))
        }
        .boxed();
        let outgoing = EventSink::preparing(move |events: &mut EventStream<O>| {
            // The kernel prepares a response once its request opened, which
            // filled the wire.
            let unread = "a response was prepared before its request was read";
            let request = wire.get().cloned().ok_or_else(|| Fault::internal(unread))?;
            if reply.is_canceled() {
                return Err(Fault::abandoned());
            }
            let reply = Reply {
                head: client_head,
                request: Arc::new(request),
                slot: Arc::new(Mutex::new(Handoff {
                    sender: Some(reply),
                    staged: None,
                    committed: false,
                    failed: None,
                    unread: false,
                })),
                error: Arc::new(error),
                live,
            };
            let fallback = reply.fallback();
            let slot = fallback.slot.clone();
            let sink = if let Some(fault) = events.refusal().cloned() {
                events.stop();
                reply.failure(fault, false)
            } else {
                match std::panic::catch_unwind(AssertUnwindSafe(|| encode(reply))) {
                    Ok(Ok(sink)) => {
                        if slot.lock().unwrap_or_else(|e| e.into_inner()).unread {
                            events.stop();
                        }
                        sink
                    }
                    result => {
                        events.stop();
                        slot.lock().unwrap_or_else(|e| e.into_inner()).staged.take();
                        match result {
                            Ok(Err(fault)) => fallback.failure(fault, false),
                            Err(_) => fallback.failure(Fault::internal(PANICKED), true),
                            Ok(Ok(_)) => unreachable!(),
                        }
                    }
                }
            };
            slot.lock().unwrap_or_else(|e| e.into_inner()).commit();
            Ok(sink)
        });
        (incoming, outgoing)
    }
}

/// Owns a client's response handoff and original request. Its methods construct
/// real event sinks; there is no intermediate response plan.
///
/// A response HTTP gives no body ([`bodiless`]: to a `HEAD`, or a 204, 205
/// or 304) is written with none whatever its encoder: its head goes at
/// once, its encoder is never called, and its source, if the sink is made
/// while the response is prepared, is stopped unread.
pub struct Reply {
    head: ClientHead,
    request: Arc<http::Request<Bytes>>,
    slot: Slot,
    error: Arc<ErrorWriter>,
    live: bool,
}
impl Reply {
    fn fallback(&self) -> Self {
        Self {
            head: self.head.clone(),
            request: self.request.clone(),
            slot: self.slot.clone(),
            error: self.error.clone(),
            live: self.live,
        }
    }
    /// The explicitly selected client head, or a fresh success head.
    pub fn head(&self) -> ResponseHead {
        self.head_or(http::Response::new(()).into_parts().0)
    }
    /// The explicitly selected client head, or `head`, such as a typed
    /// event's.
    pub fn head_or(&self, head: ResponseHead) -> ResponseHead {
        self.head.get().unwrap_or(head)
    }
    /// The client's request as it came, its [`Wire`]'s, independently of
    /// any rewritten provider call.
    pub fn request(&self) -> &http::Request<Bytes> {
        &self.request
    }
    /// Encode a response, delaying its head until bytes or completion, but
    /// for [`Framing::Live`] ingress, whose head goes at once.
    pub fn write<T: 'static>(
        self,
        head: ResponseHead,
        encoder: Box<dyn Encoder<T>>,
    ) -> EventSink<T> {
        let open = self.live;
        self.writer(head, encoder, Bytes::new(), open)
    }
    /// Open a quiet response immediately, optionally with initial bytes.
    pub fn open<T: 'static>(
        self,
        head: ResponseHead,
        prefix: Bytes,
        encoder: Box<dyn Encoder<T>>,
    ) -> EventSink<T> {
        self.writer(head, encoder, prefix, true)
    }
    fn writer<T: 'static>(
        self,
        head: ResponseHead,
        encoder: Box<dyn Encoder<T>>,
        prefix: Bytes,
        open: bool,
    ) -> EventSink<T> {
        if bodiless(self.request.method(), head.status) {
            self.slot.lock().unwrap_or_else(|e| e.into_inner()).unread = true;
            let mut client = Client::new(self, head, None);
            client.writer.end(Ok(()), None);
            client.ended = true;
            client.open();
            return EventSink::new(client);
        }
        let mut client = Client::new(self, head, Some(encoder));
        if !prefix.is_empty() {
            client.writer.send(prefix);
        }
        if open {
            client.open();
        }
        EventSink::new(client)
    }
    fn failure<T: 'static>(self, fault: Fault, plain: bool) -> EventSink<T> {
        let mut client = Client::new(self, http::Response::new(()).into_parts().0, None);
        if plain {
            client.unwritable(fault);
        } else {
            client.refuse(fault);
        }
        EventSink::new(client)
    }
}

/// The fault of a client's message over its adapter's limit: a 413.
fn too_large() -> Fault {
    Fault::new("request body too large")
        .with(ErrorCode("too_large".into()))
        .with(Status(StatusCode::PAYLOAD_TOO_LARGE))
}

/// A whole body of up to `limit` bytes.
async fn read_whole(body: Body, limit: usize) -> Result<Bytes, Fault> {
    if let Some(bytes) = body.full_bytes() {
        return match bytes.len() > limit {
            true => Err(too_large()),
            false => Ok(bytes.clone()),
        };
    }
    // Release each frame before polling again: a producer may recycle its buffer.
    body.try_fold(BytesMut::new(), |mut all, frame| {
        let kept = all.len() + frame.len() <= limit;
        if kept {
            all.extend_from_slice(&frame);
        }
        futures::future::ready(kept.then_some(all).ok_or_else(too_large))
    })
    .await
    .map(BytesMut::freeze)
}

struct Client<T> {
    reply: Reply,
    head: Option<ResponseHead>,
    frames: Option<Frames>,
    completion: Option<Completion>,
    encoder: Option<Box<dyn Encoder<T>>>,
    writer: Writer,
    ended: bool,
}
impl<T> Unpin for Client<T> {}
impl<T: 'static> Client<T> {
    fn new(reply: Reply, head: ResponseHead, encoder: Option<Box<dyn Encoder<T>>>) -> Self {
        let (writer, frames, completion) = Writer::new();
        Self {
            reply,
            head: Some(head),
            frames: Some(frames),
            completion: Some(completion),
            encoder,
            writer,
            ended: false,
        }
    }
    fn open(&mut self) {
        if let Some(head) = self.head.take() {
            let body = Body::stream(self.frames.take().expect("response handed once"));
            let completion = self.completion.take().expect("response handed once");
            self.reply
                .slot
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .publish((http::Response::from_parts(head, body), completion));
        }
    }
    fn unwritable(&mut self, fault: Fault) {
        self.ended = true;
        self.writer.end(Err(fault.clone()), None);
        let mut response = plain_response(&fault);
        response.extensions_mut().insert(Unwritten);
        self.head.take();
        let end = self.completion.take().expect("response handed once");
        self.reply
            .slot
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .publish((response, end));
    }
    /// Write `fault` as the response: an explicit [`RawReply`] as it came,
    /// else its status and `Retry-After` and the format's error body.
    fn refuse(&mut self, fault: Fault) {
        let mut head = http::Response::new(()).into_parts().0;
        let bytes = match fault.get::<RawReply>() {
            Some(RawReply(raw)) => {
                let (raw, bytes) = raw.clone().into_parts();
                head = raw;
                bytes
            }
            None => {
                error_head(&mut head, &fault);
                let error = &self.reply.error;
                let request = &self.reply.request;
                let written = AssertUnwindSafe(|| error(&fault, request, &mut head));
                let Ok(bytes) = std::panic::catch_unwind(written) else {
                    self.unwritable(Fault::internal(PANICKED));
                    return;
                };
                bytes
            }
        };
        if !bytes.is_empty() && !bodiless(self.reply.request.method(), head.status) {
            self.writer.send(bytes);
        }
        self.head = Some(head);
        self.writer.end(Err(fault), None);
        self.ended = true;
        self.open();
    }
    fn finish(&mut self, mut end: Result<(), Fault>) {
        if self.ended {
            return;
        }
        let mut out = BytesMut::new();
        let mut abort = None;
        if let Some(encoder) = &mut self.encoder {
            let encoded = std::panic::catch_unwind(AssertUnwindSafe(|| {
                if let Err(fault) = encoder.end(&end, &mut out) {
                    end = Err(fault.clone());
                    out.clear();
                    if encoder.end(&end, &mut out).is_err() {
                        out.clear();
                        abort = Some(fault);
                    }
                }
            }));
            if encoded.is_err() {
                let fault = Fault::internal(PANICKED);
                out.clear();
                end = Err(fault.clone());
                abort = Some(fault);
            }
        }
        if self.head.is_some()
            && let Err(fault) = end
        {
            self.refuse(fault);
            return;
        }
        if !out.is_empty() {
            self.writer.send(out.freeze());
        }
        self.writer.end(end, abort);
        self.ended = true;
        self.open();
    }
}
impl<T> Drop for Client<T> {
    fn drop(&mut self) {
        let mut slot = self.reply.slot.lock().unwrap_or_else(|e| e.into_inner());
        if self.head.is_some() && slot.committed && slot.sender.is_some() {
            let fault = Fault::abandoned();
            let end = futures::future::ready(Err(fault.clone())).boxed();
            slot.publish((plain_response(&fault), end));
        }
    }
}
impl<T: 'static> Sink<Result<T, Fault>> for Client<T> {
    type Error = Fault;
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Fault>> {
        if let Some(fault) = self
            .reply
            .slot
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .failed
            .clone()
        {
            return Poll::Ready(Err(fault));
        }
        if self.ended {
            self.writer.poll_close(cx)
        } else {
            self.writer.poll_ready(cx, false)
        }
    }
    fn start_send(mut self: Pin<&mut Self>, item: Result<T, Fault>) -> Result<(), Fault> {
        if self.ended {
            return Ok(());
        }
        let mut out = BytesMut::new();
        let encoded = match item {
            Ok(value) => std::panic::catch_unwind(AssertUnwindSafe(|| {
                self.encoder
                    .as_mut()
                    .expect("response encoder")
                    .encode(&value, &mut out)
            }))
            .unwrap_or_else(|_| Err(Fault::internal(PANICKED))),
            Err(fault) => Err(fault),
        };
        match encoded {
            Ok(()) if self.reply.live || !out.is_empty() => {
                self.writer.send(out.freeze());
                self.open();
            }
            Ok(()) => {}
            Err(fault) => self.finish(Err(fault)),
        }
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Fault>> {
        self.poll_ready(cx)
    }
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Fault>> {
        if !self.ended {
            ready!(self.writer.poll_ready(cx, false))?;
            self.finish(Ok(()));
        }
        if let Some(fault) = self
            .reply
            .slot
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .failed
            .clone()
        {
            return Poll::Ready(Err(fault));
        }
        self.writer.poll_close(cx)
    }
}

impl std::fmt::Debug for ClientIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientIo")
            .field("request", &self.request)
            .field("live", &self.live)
            .finish_non_exhaustive()
    }
}
impl std::fmt::Debug for Reply {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reply")
            .field("request", &self.request)
            .finish_non_exhaustive()
    }
}
