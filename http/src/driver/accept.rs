//! Client input and its one response handoff.

use super::writer::Writer;
use super::{End, PANICKED, RequestMetadata, Written};
use crate::{
    Body, Encoder, Frames, ResponseHead, Unwritten, answered, bodiless, fresh_response_head, plain,
};
use bytes::{Bytes, BytesMut};
use futures::channel::oneshot;
use futures::{FutureExt, Sink, StreamExt};
use respan_gateway_sdk::{Connection, EventSink, EventStream, Fault};
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll, ready};

type Refuse = dyn Fn(&Fault, &http::Request<Bytes>, &mut ResponseHead) -> Bytes + Send + Sync;
type Slot = Arc<Mutex<Handoff>>;
struct Handoff {
    sender: Option<oneshot::Sender<Written>>,
    staged: Option<Written>,
    committed: bool,
    failed: Option<Fault>,
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
/// use respan_gateway_http::{Body, Incoming, http::Request};
/// let (incoming, _) = Incoming::accept(Request::new(Body::default()), false);
/// let duplicate = incoming.clone();
/// ```
/// Binding consumes it, preventing a second binding:
/// ```compile_fail
/// use respan_gateway_http::{self as h, formats::opaque::Passthrough};
/// use respan_gateway_sdk::Format;
/// let (incoming, _) = h::Incoming::accept(h::http::Request::new(h::Body::default()), false);
/// let first = Passthrough.bind(incoming);
/// let second = Passthrough.bind(incoming);
/// ```
pub struct Incoming {
    request: http::Request<Body>,
    live: bool,
    reply: oneshot::Sender<Written>,
}
impl Incoming {
    /// Accept an HTTP request. `live` explicitly means one semantic message per
    /// body frame; ordinary network chunks are collected before decoding.
    /// Drive the returned answer together with `Gateway::handle`.
    pub fn accept(
        request: http::Request<Body>,
        live: bool,
    ) -> (Self, futures::future::BoxFuture<'static, Option<Written>>) {
        let (reply, answer) = oneshot::channel();
        (
            Self {
                request,
                live,
                reply,
            },
            async move { answer.await.ok() }.boxed(),
        )
    }
    /// The request, available before binding for protocol recognition.
    pub fn request(&self) -> &http::Request<Body> {
        &self.request
    }

    /// Bind ordinary reading and response preparation functions to core events.
    pub fn bind<I, O, R, W, F>(self, name: &str, read: R, write: W, refuse: F) -> Connection<I, O>
    where
        I: Clone + Send + Sync + 'static,
        O: Clone + Send + Sync + 'static,
        R: FnOnce(http::Request<Body>) -> Result<EventStream<I>, Fault> + Send + 'static,
        W: FnOnce(Reply, &mut EventStream<O>) -> Result<EventSink<O>, Fault> + Send + 'static,
        F: Fn(&Fault, &http::Request<Bytes>, &mut ResponseHead) -> Bytes + Send + Sync + 'static,
    {
        let Self {
            request,
            live,
            reply,
        } = self;
        let (head, body) = request.into_parts();
        let head = Arc::new(head);
        let original = Arc::new(OnceLock::new());
        let saved = original.clone();
        let ask = head.clone();
        let format = name.to_owned();
        let incoming = {
            let format = name.to_owned();
            async move {
                let (raw, body) = if live {
                    (None, Body::stream(body))
                } else {
                    let result = read_whole(body).await;
                    let raw = result.as_ref().cloned().unwrap_or_default();
                    let _ = saved.set(raw.clone());
                    result?;
                    (Some(raw.clone()), Body::full(raw))
                };
                if live {
                    let _ = saved.set(Bytes::new());
                }
                let came = RequestMetadata {
                    head: head.clone(),
                    body: raw,
                };
                Ok(read(http::Request::from_parts((*head).clone(), body))?
                    .with_metadata(&format, came))
            }
            .boxed()
        };
        let outgoing = EventSink::preparing(move |events: &mut EventStream<O>| {
            let raw = original
                .get()
                .cloned()
                .ok_or_else(|| Fault::internal(PANICKED))?;
            if reply.is_canceled() {
                return Err(Fault::abandoned());
            }
            let reply = Reply {
                name: format,
                request: Arc::new(http::Request::from_parts((*ask).clone(), raw)),
                slot: Arc::new(Mutex::new(Handoff {
                    sender: Some(reply),
                    staged: None,
                    committed: false,
                    failed: None,
                })),
                refuse: Arc::new(refuse),
                live,
            };
            let fallback = reply.fallback();
            let slot = fallback.slot.clone();
            let sink = if let Some(fault) = events.refusal().cloned() {
                events.stop();
                reply.failure(fault, false)
            } else {
                match std::panic::catch_unwind(AssertUnwindSafe(|| write(reply, events))) {
                    Ok(Ok(sink)) => sink,
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
pub struct Reply {
    name: String,
    request: Arc<http::Request<Bytes>>,
    slot: Slot,
    refuse: Arc<Refuse>,
    live: bool,
}
impl Reply {
    fn fallback(&self) -> Self {
        Self {
            name: self.name.clone(),
            request: self.request.clone(),
            slot: self.slot.clone(),
            refuse: self.refuse.clone(),
            live: self.live,
        }
    }
    /// The original request, independently of any rewritten provider call.
    pub fn request(&self) -> &http::Request<Bytes> {
        &self.request
    }
    /// Encode a response, delaying its head until bytes or completion.
    pub fn write<T: 'static>(
        self,
        head: ResponseHead,
        encoder: Box<dyn Encoder<T>>,
    ) -> EventSink<T> {
        let open = self.live;
        self.writer(head, Some(encoder), Bytes::new(), open)
    }
    /// Open a quiet response immediately, optionally with initial bytes.
    pub fn open<T: 'static>(
        self,
        head: ResponseHead,
        prefix: Bytes,
        encoder: Box<dyn Encoder<T>>,
    ) -> EventSink<T> {
        self.writer(head, Some(encoder), prefix, true)
    }
    /// Write no body and retain the unread source until delivery settles.
    pub fn empty<T: 'static>(
        self,
        head: ResponseHead,
        source: &mut EventStream<T>,
    ) -> EventSink<T> {
        source.stop();
        self.writer(head, None, Bytes::new(), true)
    }
    fn writer<T: 'static>(
        self,
        head: ResponseHead,
        encoder: Option<Box<dyn Encoder<T>>>,
        prefix: Bytes,
        open: bool,
    ) -> EventSink<T> {
        let mut client = Client::new(self, head, encoder);
        if client.encoder.is_none() {
            client.writer.end(Ok(()), None);
            client.ended = true;
        } else if !prefix.is_empty() {
            client.writer.send(prefix);
        }
        if open {
            client.open();
        }
        EventSink::new(client)
    }
    fn failure<T: 'static>(self, fault: Fault, plain: bool) -> EventSink<T> {
        let mut client = Client::new(self, fresh_response_head(), None);
        if plain {
            client.unwritable(fault);
        } else {
            client.refuse(fault);
        }
        EventSink::new(client)
    }
}

async fn read_whole(mut body: Body) -> Result<Bytes, Fault> {
    if let Some(bytes) = body.full_bytes() {
        return Ok(bytes.clone());
    }
    let mut all = BytesMut::new();
    while let Some(bytes) = body.next().await {
        all.extend_from_slice(&bytes?);
    }
    Ok(all.freeze())
}

struct Client<T> {
    reply: Reply,
    head: Option<ResponseHead>,
    frames: Option<Frames>,
    completion: Option<End>,
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
        let mut response = plain(&fault);
        response.extensions_mut().insert(Unwritten);
        self.head.take();
        let end = self.completion.take().expect("response handed once");
        self.reply
            .slot
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .publish((response, end));
    }
    fn refuse(&mut self, fault: Fault) {
        let mut head = fresh_response_head();
        let answer = answered(&self.reply.name, &fault, &mut head);
        let bytes = std::panic::catch_unwind(AssertUnwindSafe(|| {
            answer.unwrap_or_else(|| (self.reply.refuse)(&fault, &self.reply.request, &mut head))
        }));
        let Ok(bytes) = bytes else {
            self.unwritable(Fault::internal(PANICKED));
            return;
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
            slot.publish((plain(&fault), end));
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
            Ok(()) if !out.is_empty() => {
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

impl std::fmt::Debug for Incoming {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Incoming")
            .field("request", &self.request)
            .field("live", &self.live)
            .finish_non_exhaustive()
    }
}
impl std::fmt::Debug for Reply {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reply")
            .field("name", &self.name)
            .field("request", &self.request)
            .finish_non_exhaustive()
    }
}
