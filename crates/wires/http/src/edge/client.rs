//! Client input and its one response handoff.

use super::writer::Writer;
use super::{PANICKED, Written};
use crate::rules::{bodiless, error_head, outgoing, plain_response, strip_hops};
use crate::{Body, ClientHead, Completion, Credential, Encoder, Frames, RawReply, Status, Wire};
use bytes::{Bytes, BytesMut};
use futures::channel::oneshot;
use futures::future::{BoxFuture, ready};
use futures::{FutureExt, Sink, StreamExt, TryStreamExt};
use http::Method;
use http::StatusCode;
use http::header::CONTENT_LENGTH;
use http::response::Parts as ResponseHead;
use http_body_util::BodyExt;
use respan_gateway_core::{Connection, ErrorCode, EventSink, EventStream, Extensions, Fault};
use respan_gateway_core::{Gateway, NotFound};
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
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
/// use respan_gateway_http::{self as h, passthrough::Passthrough};
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
        let mut out = Vec::new();
        let encoded = match item {
            Ok(value) => std::panic::catch_unwind(AssertUnwindSafe(|| {
                self.encoder
                    .as_mut()
                    .expect("response encoder")
                    .encode_messages(&value, &mut out)
            }))
            .unwrap_or_else(|_| Err(Fault::internal(PANICKED))),
            Err(fault) => Err(fault),
        };
        match encoded {
            Ok(()) => {
                // A live message may be empty; an empty chunk of a body is nothing.
                let live = self.reply.live;
                out.retain(|message| live || !message.is_empty());
                if !out.is_empty() {
                    out.into_iter()
                        .for_each(|message| self.writer.send(message));
                    self.open();
                }
            }
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

/// Hand `req` to `gateway` as an HTTP adapter does, as an [`ClientIo`]
/// handle, to [`Gateway::handle`], its body framed as `framing` with `limit`
/// the most bytes of one message it takes ([`ClientIo::accept`]). Before:
/// the hop's headers go (RFC 9110 7.6.1), and `Upgrade` too unless the
/// body is [`Framing::Live`], as only a session switches protocols: a whole
/// request's `Upgrade`, such as `h2c`, is no format's to pass on. The
/// [`ClientIo`]'s [extensions](ClientIo::extensions) join those `req`
/// carries. After: what no format can write, or a 101 to a whole request,
/// goes as text/plain; the response loses a hop's headers; a [`bodiless`]
/// one goes with none, the gateway's body read apart.
///
/// Like [`Gateway::handle`], the request counts for [`Gateway::idle`] from
/// this call, not from the future's first poll, and the future borrows
/// nothing: a job handed over from an end hook calls this where it starts
/// and awaits the future in its own task. Await it on a tokio runtime.
pub fn handle<B>(
    gateway: &Gateway,
    req: http::Request<B>,
    framing: Framing,
    limit: usize,
) -> impl Future<Output = (http::Response<Body>, BoxFuture<'static, Result<(), Fault>>)> + Send + 'static
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Fault>,
{
    let (mut head, body) = req.into_parts();
    let mut extensions = head.extensions.remove::<Extensions>().unwrap_or_default();
    head.extensions = http::Extensions::new();
    strip_hops(&mut head.headers, framing == Framing::Live);
    let to = (head.method.clone(), head.uri.path().to_owned());
    let body = Body::stream(body.map_err(Into::into).into_data_stream());
    let (incoming, answer) =
        ClientIo::accept(http::Request::from_parts(head, body), framing, limit);
    extensions.extend(incoming.extensions());
    let handled = gateway.handle(incoming, extensions);
    async move {
        let (resp, ended) = match super::drive(handled.boxed(), answer).await {
            Ok((resp, ended)) => (Ok(resp), ended),
            Err(fault) => (Err(fault.clone()), ready(Err(fault)).boxed()),
        };
        (depart(&to, framing, resp), ended)
    }
}

/// Marks the plain text of a refusal no codec could write, which goes as
/// it is: HTTP's rules for a body do not apply to it.
#[derive(Clone, Copy)]
pub(crate) struct Unwritten;

/// The answer to a request of `method` to `path` as it departs the adapter
/// ([`handle`]): what no format can write, or a 101 to a whole request, as
/// text/plain; a hop's headers gone; a [`bodiless`] one with none, the
/// gateway's body read apart.
fn depart(
    (method, path): &(Method, String),
    framing: Framing,
    resp: Result<http::Response<Body>, Fault>,
) -> http::Response<Body> {
    // A refusal no codec could write goes as the client edge wrote it.
    let resp = match resp {
        Ok(mut resp) => match resp.extensions_mut().remove::<Unwritten>() {
            Some(Unwritten) => return resp,
            None => Ok(resp),
        },
        Err(fault) => Err(fault),
    };
    match resp {
        Err(fault) if fault.is::<NotFound>() => {
            let text = format!("no format served accepts {method} {path}");
            plain_response(&Fault::not_found(text))
        }
        Err(fault) => plain_response(&fault),
        Ok(resp)
            if resp.status() == StatusCode::SWITCHING_PROTOCOLS && framing == Framing::Whole =>
        {
            // RFC 9110 15.2.2: a 101 only to a request that asked.
            let asked = "a 101 to a request that did not ask to switch protocols";
            plain_response(&Fault::internal(asked))
        }
        Ok(resp) => {
            let (mut head, mut body) = resp.into_parts();
            let length = head.headers.get(CONTENT_LENGTH).cloned();
            outgoing(&mut head);
            if bodiless(method, head.status) {
                // A HEAD's or a 304's `Content-Length` is the provider's
                // for the body it would send (RFC 9110 8.6); a 204 has none.
                let kept = method == Method::HEAD || head.status == StatusCode::NOT_MODIFIED;
                if let Some(length) = length.filter(|_| kept) {
                    head.headers.insert(CONTENT_LENGTH, length);
                }
                // A hop drops a bodiless answer's body once its head is out,
                // so that is when the client leaves. Read what is ready now,
                // so an end that is ready is told, then drop the rest: the
                // request ends with its client, never when a provider's
                // stream does.
                drain_ready(&mut body);
                body = Body::default();
            }
            http::Response::from_parts(head, body)
        }
    }
}

/// Read what `body` has ready now, dropping its frames: a bodiless answer's
/// are content it cannot send, read only to reach its end, so outputs dropped
/// unread end their call as that end says if it is ready, else `abandoned`.
///
/// The drain stops at the body's end or error, at a `Pending` nothing woke
/// (the body is waiting; the gateway's own body wakes itself between frames
/// sent apart, which counts as ready), when tokio's cooperative budget is
/// spent, or after `DRAIN_POLLS` polls, which bounds a task under
/// `tokio::task::unconstrained` and a body that wakes itself without giving
/// a frame.
fn drain_ready(body: &mut Body) {
    /// The most polls one drain makes.
    const DRAIN_POLLS: usize = 1 << 16;
    struct Woken(AtomicBool);
    impl futures::task::ArcWake for Woken {
        fn wake_by_ref(this: &Arc<Self>) {
            this.0.store(true, Ordering::SeqCst);
        }
    }
    let woken = Arc::new(Woken(AtomicBool::new(false)));
    let waker = futures::task::waker(woken.clone());
    let mut cx = std::task::Context::from_waker(&waker);
    let mut left = DRAIN_POLLS;
    while left > 0 {
        let mut budget = std::pin::pin!(tokio::task::consume_budget());
        if budget.as_mut().poll(&mut cx).is_pending() {
            return;
        }
        left -= 1;
        woken.0.store(false, Ordering::SeqCst);
        match http_body::Body::poll_frame(std::pin::Pin::new(&mut *body), &mut cx) {
            std::task::Poll::Pending if woken.0.load(Ordering::SeqCst) => {}
            std::task::Poll::Ready(Some(Ok(_))) => {}
            _ => return,
        }
    }
}
