//! Client input and its one response handoff.

use super::writer::Writer;
use super::{PANICKED, Written, guarded, locked};
use crate::codec::{self, decode_one};
use crate::rules::{bodiless, error_head, outgoing, plain_response, strip_hops};
use crate::{
    Body, Completion, Credential, Decoder, Encoder, Frames, raw::ClientHead, raw::Original,
    raw::RawReply,
};
use bytes::Bytes;
use futures::channel::oneshot;
use futures::future::{BoxFuture, ready};
use futures::{FutureExt, Sink, StreamExt};
use http::Method;
use http::StatusCode;
use http::header::CONTENT_LENGTH;
use http::request::Parts as RequestHead;
use http::response::Parts as ResponseHead;
use http_body_util::BodyExt;
use respan_gateway_core::Gateway;
use respan_gateway_core::{Connection, ErrorCode, EventSink, EventStream, Extensions, Fault};
use respan_gateway_http_faults::Status;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, ready};

type ErrorWriter = dyn Fn(&Fault, &http::Request<Bytes>, &mut ResponseHead) -> Bytes + Send + Sync;
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

/// One accepted request. It is consumed once, rather than shared and runtime-locked.
///
/// An accepted request cannot be cloned:
/// ```compile_fail
/// use respan_gateway_http::{Body, ClientIo, http::Request};
/// let (incoming, _) = ClientIo::accept(Request::new(Body::default()), 1024);
/// let duplicate = incoming.clone();
/// ```
/// Binding consumes it, preventing a second binding:
/// ```compile_fail
/// use respan_gateway_http as h;
/// use respan_gateway_core::{Fault, EventSink};
/// use h::bytes::Bytes;
/// let request = h::http::Request::new(h::Body::default());
/// let (incoming, _) = h::ClientIo::accept(request, 1024);
/// fn bind(io: h::ClientIo) {
///     let _ = io.bind::<Bytes, Bytes>(
///         |_| Box::new(|raw: Bytes, out: &mut Vec<Bytes>| Ok(out.push(raw))),
///         |_| Err::<EventSink<Bytes>, _>(Fault::new("unused")),
///     );
/// }
/// bind(incoming);
/// bind(incoming);
/// ```
pub struct ClientIo {
    request: http::Request<Body>,
    live: bool,
    limit: usize,
    original: Original,
    client_head: ClientHead,
    credential: Option<Credential>,
    respond: oneshot::Sender<Written>,
    error: Arc<ErrorWriter>,
}
impl ClientIo {
    /// Accept an HTTP request whose body is one message, collected whole
    /// however the network chunked it, its credentials lifted out of its
    /// head into a [`Credential`]. `limit` is the most bytes of the body it
    /// takes: past it, the request fails 413 (`too_large`). Hand the gateway
    /// its [`extensions`](Self::extensions), and drive the returned answer
    /// together with `Gateway::handle`.
    pub fn accept(
        request: http::Request<Body>,
        limit: usize,
    ) -> (Self, BoxFuture<'static, Option<Written>>) {
        Self::accepted(request, false, limit)
    }

    /// Accept a session, such as a WebSocket's after its upgrade, as
    /// [`accept`](Self::accept) does, but each frame of its body one message
    /// decoded as it comes, `limit` the most bytes of one. Its response
    /// opens at once, so a duplex peer can send without waiting for its own
    /// first reply.
    pub fn accept_live(
        request: http::Request<Body>,
        limit: usize,
    ) -> (Self, BoxFuture<'static, Option<Written>>) {
        Self::accepted(request, true, limit)
    }

    fn accepted(
        request: http::Request<Body>,
        live: bool,
        limit: usize,
    ) -> (Self, BoxFuture<'static, Option<Written>>) {
        let (mut head, body) = request.into_parts();
        let credential = Credential::lift(&mut head);
        let original = Original::unread();
        if live {
            original.fill(http::Request::from_parts(head.clone(), Bytes::new()));
        }
        let (respond, answer) = oneshot::channel();
        (
            Self {
                request: http::Request::from_parts(head, body),
                live,
                limit,
                original,
                client_head: ClientHead::default(),
                credential,
                respond,
                error: Arc::new(crate::rules::plain),
            },
            async move { answer.await.ok() }.boxed(),
        )
    }
    /// What the gateway is handed with this request: its [`Original`], the
    /// client's [`Credential`], if it sent one, and where a relayed answer
    /// puts its head ([`Relayed`](crate::raw::Relayed)).
    pub fn extensions(&self) -> Extensions {
        let mut extensions = Extensions::new();
        extensions.insert(self.original.clone());
        extensions.insert(self.client_head.clone());
        if let Some(credential) = &self.credential {
            extensions.insert(credential.clone());
        }
        extensions
    }
    /// The request, available before binding for protocol recognition.
    pub fn request(&self) -> &http::Request<Body> {
        &self.request
    }

    /// Write a refusal's body with `error`, which may edit its head, whose
    /// status and `Retry-After` the fault's are ([`status`], [`RetryAfter`]),
    /// in place of the default, the fault's message as plain text. An explicit
    /// [`RawReply`] is written instead, as it came.
    ///
    /// [`status`]: respan_gateway_http_faults::status
    /// [`RetryAfter`]: respan_gateway_core::RetryAfter
    pub fn with_error(
        mut self,
        error: impl Fn(&Fault, &http::Request<Bytes>, &mut ResponseHead) -> Bytes
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.error = Arc::new(error);
        self
    }

    /// Bind the request's decoder and the response's encoder to core events.
    /// `decode` gives the request's [`Decoder`], given its head: it reads
    /// the whole body, collected up to the limit, as one input, or each
    /// live frame. `encode` makes the response's sink from a [`Reply`].
    pub fn bind<I, O>(
        self,
        decode: impl FnOnce(&RequestHead) -> Box<dyn Decoder<I>> + Send + 'static,
        encode: impl FnOnce(Reply) -> Result<EventSink<O>, Fault> + Send + 'static,
    ) -> Connection<I, O>
    where
        I: Clone + Send + Sync + 'static,
        O: Clone + Send + Sync + 'static,
    {
        let Self {
            request,
            live,
            limit,
            respond,
            original,
            client_head,
            error,
            ..
        } = self;
        let (head, body) = request.into_parts();
        let filled = original.clone();
        let incoming = async move {
            if live {
                let frames = body.map(move |frame| match frame {
                    Ok(bytes) if bytes.len() > limit => Err(too_large()),
                    frame => frame,
                });
                let decoder = decode(&head);
                return Ok(codec::decode(Body::stream(frames), decoder, None));
            }
            let whole = codec::collect(body, limit, too_large).await;
            let raw = whole.as_ref().cloned().unwrap_or_default();
            filled.fill(http::Request::from_parts(head.clone(), raw));
            let decoder = decode(&head);
            Ok(decode_one(whole?, decoder))
        }
        .boxed();
        let outgoing = EventSink::preparing(move |events: &mut EventStream<O>| {
            // The kernel prepares a response once its request opened, which
            // filled its original.
            let unread = "a response was prepared before its request was read";
            let request = original
                .get()
                .cloned()
                .ok_or_else(|| Fault::internal(unread))?;
            if respond.is_canceled() {
                return Err(Fault::abandoned());
            }
            let reply = Reply {
                head: client_head,
                request: Arc::new(request),
                slot: Arc::new(Mutex::new(Handoff {
                    sender: Some(respond),
                    staged: None,
                    committed: false,
                    failed: None,
                    unread: false,
                })),
                error,
                live,
            };
            let fallback = reply.fallback();
            let slot = fallback.slot.clone();
            let sink = match events.refusal().cloned() {
                Some(fault) => {
                    events.stop();
                    reply.refusal(fault)
                }
                None => {
                    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| encode(reply))) {
                        Ok(Ok(sink)) => {
                            if locked(&slot).unread {
                                events.stop();
                            }
                            sink
                        }
                        failed => {
                            events.stop();
                            locked(&slot).staged.take();
                            match failed.ok().and_then(Result::err) {
                                Some(fault) => fallback.refusal(fault),
                                // A panicking format may have left its error
                                // writer unusable too: it goes as plain text.
                                None => fallback.unwritable(Fault::internal(PANICKED)),
                            }
                        }
                    }
                }
            };
            locked(&slot).commit();
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
    slot: Arc<Mutex<Handoff>>,
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
    /// The client's request as it came, its [`Original`]'s, independently of
    /// any rewritten provider call.
    pub fn request(&self) -> &http::Request<Bytes> {
        &self.request
    }
    /// Encode a response, delaying its head until bytes or completion, but
    /// for a live session's ([`ClientIo::accept_live`]), whose head goes at once.
    pub fn write<T: 'static>(
        self,
        head: ResponseHead,
        encoder: Box<dyn Encoder<T>>,
    ) -> EventSink<T> {
        let live = self.live;
        let mut client = self.client(head, encoder);
        if live {
            client.open();
        }
        EventSink::new(client)
    }
    /// Open a quiet response at once, with what its encoder
    /// [starts](Encoder::start) with.
    pub fn open<T: 'static>(
        self,
        head: ResponseHead,
        encoder: Box<dyn Encoder<T>>,
    ) -> EventSink<T> {
        let mut client = self.client(head, encoder);
        client.open();
        EventSink::new(client)
    }
    /// The sink of `head` and `encoder`, started.
    fn client<T: 'static>(self, head: ResponseHead, encoder: Box<dyn Encoder<T>>) -> Client<T> {
        if bodiless(self.request.method(), head.status) {
            locked(&self.slot).unread = true;
            let mut client = Client::new(self, head, None);
            client.writer.end(Ok(()), None);
            client.open();
            return client;
        }
        let mut client = Client::new(self, head, Some(encoder));
        match client.writer.start() {
            Ok(frames) => _ = client.send(frames),
            Err(fault) => client.finish(Err(fault)),
        }
        client
    }
    /// The sink of a refusal: `fault` written as the format writes errors.
    fn refusal<T: 'static>(self, fault: Fault) -> EventSink<T> {
        let mut client = Client::new(self, http::Response::new(()).into_parts().0, None);
        client.refuse(fault);
        EventSink::new(client)
    }
    /// The sink of a refusal no format can write: `fault` as plain text.
    fn unwritable<T: 'static>(self, fault: Fault) -> EventSink<T> {
        let mut client = Client::new(self, http::Response::new(()).into_parts().0, None);
        client.unwritable(fault);
        EventSink::new(client)
    }
}

/// The fault of a client's message over its adapter's limit: a 413.
fn too_large() -> Fault {
    Fault::new("request body too large")
        .with(ErrorCode("too_large".into()))
        .with(Status(StatusCode::PAYLOAD_TOO_LARGE))
}

/// A response's sink: its writer, and its head, body and completion,
/// `unopened` until handed to the adapter.
struct Client<T> {
    reply: Reply,
    unopened: Option<(ResponseHead, Frames, Completion)>,
    writer: Writer<T>,
}
impl<T> Unpin for Client<T> {}
impl<T: 'static> Client<T> {
    fn new(reply: Reply, head: ResponseHead, encoder: Option<Box<dyn Encoder<T>>>) -> Self {
        let (writer, frames, completion) = Writer::new(encoder);
        Self {
            reply,
            unopened: Some((head, frames, completion)),
            writer,
        }
    }
    fn open(&mut self) {
        if let Some((head, frames, completion)) = self.unopened.take() {
            let response = http::Response::from_parts(head, Body::stream(frames));
            locked(&self.reply.slot).publish((response, completion));
        }
    }
    /// Send `frames`, but an empty chunk of a body, which is nothing (a live
    /// message may be empty). Whether any went.
    fn send(&mut self, frames: Vec<Bytes>) -> bool {
        let live = self.reply.live;
        let mut sent = false;
        for frame in frames.into_iter().filter(|frame| live || !frame.is_empty()) {
            self.writer.send(frame);
            sent = true;
        }
        sent
    }
    fn unwritable(&mut self, fault: Fault) {
        self.writer.drop_encoder();
        self.writer.end(Err(fault.clone()), None);
        let mut response = plain_response(&fault);
        response.extensions_mut().insert(Unwritten);
        if let Some((_, _, completion)) = self.unopened.take() {
            locked(&self.reply.slot).publish((response, completion));
        }
    }
    /// Write `fault` as the response: an explicit [`RawReply`] as it came,
    /// else its status and `Retry-After` and the format's error body.
    fn refuse(&mut self, fault: Fault) {
        self.writer.drop_encoder();
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
                let Ok(bytes) = guarded(|| Ok(error(&fault, request, &mut head))) else {
                    self.unwritable(Fault::internal(PANICKED));
                    return;
                };
                bytes
            }
        };
        if !bytes.is_empty() && !bodiless(self.reply.request.method(), head.status) {
            self.writer.send(bytes);
        }
        if let Some(unopened) = &mut self.unopened {
            unopened.0 = head;
        }
        self.writer.end(Err(fault), None);
        self.open();
    }
    fn finish(&mut self, end: Result<(), Fault>) {
        let mut out = Vec::new();
        let Some((end, abort)) = self.writer.end_encoder(end, &mut out) else {
            return;
        };
        if self.unopened.is_some()
            && let Err(fault) = end
        {
            self.refuse(fault);
            return;
        }
        self.send(out);
        self.writer.end(end, abort);
        self.open();
    }
}
impl<T> Drop for Client<T> {
    fn drop(&mut self) {
        let mut slot = locked(&self.reply.slot);
        if self.unopened.is_some() && slot.committed && slot.sender.is_some() {
            let fault = Fault::abandoned();
            let end = futures::future::ready(Err(fault.clone())).boxed();
            slot.publish((plain_response(&fault), end));
        }
    }
}
impl<T: 'static> Sink<Result<T, Fault>> for Client<T> {
    type Error = Fault;
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Fault>> {
        if let Some(fault) = locked(&self.reply.slot).failed.clone() {
            return Poll::Ready(Err(fault));
        }
        match self.writer.is_open() {
            true => self.writer.poll_ready(cx, false),
            false => self.writer.poll_close(cx),
        }
    }
    fn start_send(mut self: Pin<&mut Self>, item: Result<T, Fault>) -> Result<(), Fault> {
        match self.writer.encode(item) {
            Ok(frames) => {
                if self.send(frames) {
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
        if self.writer.is_open() {
            ready!(self.writer.poll_ready(cx, false))?;
            self.finish(Ok(()));
        }
        if let Some(fault) = locked(&self.reply.slot).failed.clone() {
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
/// handle, to [`Gateway::handle`], its body one message of at most `limit`
/// bytes ([`ClientIo::accept`]). Before:
/// the hop's headers go (RFC 9110 7.6.1), and `Upgrade` too, as only a
/// session switches protocols ([`handle_live`]): a whole
/// request's `Upgrade`, such as `h2c`, is no format's to pass on. The
/// [`ClientIo`]'s [extensions](ClientIo::extensions) join the gateway
/// [`Extensions`] `req` carries; its other `http::Extensions`, such as a
/// server's connection data, are dropped, as no format or plugin reads
/// them: a server puts what plugins should see in that [`Extensions`]. After: what no format can write, or a 101 to a whole request,
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
    limit: usize,
) -> impl Future<Output = Written> + Send + 'static
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Fault>,
{
    handled(gateway, req, false, limit)
}

/// [`handle`] for a session, such as a WebSocket's after its upgrade: each
/// frame of its body one message of at most `limit` bytes
/// ([`ClientIo::accept_live`]), its `Upgrade` kept.
pub fn handle_live<B>(
    gateway: &Gateway,
    req: http::Request<B>,
    limit: usize,
) -> impl Future<Output = Written> + Send + 'static
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Fault>,
{
    handled(gateway, req, true, limit)
}

fn handled<B>(
    gateway: &Gateway,
    req: http::Request<B>,
    live: bool,
    limit: usize,
) -> impl Future<Output = Written> + Send + 'static
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Fault>,
{
    let (mut head, body) = req.into_parts();
    let mut extensions = head.extensions.remove::<Extensions>().unwrap_or_default();
    head.extensions = http::Extensions::new();
    strip_hops(&mut head.headers, live);
    let to = (head.method.clone(), head.uri.path().to_owned());
    let body = Body::stream(body.map_err(Into::into).into_data_stream());
    let (incoming, answer) = ClientIo::accepted(http::Request::from_parts(head, body), live, limit);
    extensions.extend(incoming.extensions());
    let handled = gateway.handle(incoming, extensions);
    async move {
        let (resp, ended) = match super::drive(handled.boxed(), answer).await {
            Ok((resp, ended)) => (Ok(resp), ended),
            Err(fault) => (Err(fault.clone()), ready(Err(fault)).boxed()),
        };
        (depart(&to, live, resp), ended)
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
    live: bool,
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
        Err(fault) if fault.code() == "not_found" => {
            let text = format!("no format served accepts {method} {path}");
            plain_response(&Fault::not_found(text))
        }
        Err(fault) => plain_response(&fault),
        Ok(resp) if resp.status() == StatusCode::SWITCHING_PROTOCOLS && !live => {
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
