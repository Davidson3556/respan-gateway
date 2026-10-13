//! Native HTTP sending bound to the kernel's symmetric event interface.

use super::reader::observed;
use super::upload::{self, Upload};

use crate::codec;
use crate::{Body, Completion, Decoder, Encoder};
use bytes::Bytes;
use futures::channel::oneshot;
use futures::future::BoxFuture;
use futures::{Future, FutureExt, SinkExt, Stream, StreamExt, TryStreamExt, stream};
use http::{request::Parts as RequestHead, response::Parts as ResponseHead};
use respan_gateway_core::{Connection, EventSink, EventStream, Extensions, Fault};
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

/// Make a cloneable provider factory from an ordinary async sending function.
/// `Some(completion)` remains authoritative even after response EOF; `None`
/// means only the outgoing handoff is complete, not later socket writes.
/// `limit` is the most bytes a reader holds of one answer at once
/// ([`Body::limit`](crate::Body::limit)): a whole body, a refusal's, or one
/// event; past it, the answer fails `upstream`.
pub fn connect<F, Fut>(
    send: F,
    limit: usize,
) -> impl Fn(Extensions) -> crate::ProviderIo + Clone + Send + Sync + 'static
where
    F: Fn(http::Request<crate::Body>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(http::Response<crate::Body>, Option<Completion>), Fault>>
        + Send
        + 'static,
{
    use futures::FutureExt;
    let send: std::sync::Arc<Sending> = std::sync::Arc::new(move |request| send(request).boxed());
    move |extensions| crate::ProviderIo {
        send: send.clone(),
        extensions,
        limit,
        success: None,
        refusal: None,
    }
}

pub(crate) type Sending = dyn Fn(
        http::Request<Body>,
    ) -> BoxFuture<'static, Result<(http::Response<Body>, Option<Completion>), Fault>>
    + Send
    + Sync;

type Success = Box<dyn FnOnce(&ResponseHead, &RequestHead) -> bool + Send>;
type Refusal = Box<dyn FnOnce(http::Response<Bytes>) -> Fault + Send>;

/// One provider attempt, owning its extensions and sharing only the sender.
pub struct ProviderIo {
    pub(crate) send: Arc<Sending>,
    pub(crate) extensions: Extensions,
    /// The most bytes a reader holds of its answer at once.
    pub(crate) limit: usize,
    /// What it takes for a success, if not HTTP's.
    pub(crate) success: Option<Success>,
    /// How a refused answer reads as a fault, if not as
    /// [`provider_fault`](respan_gateway_http_faults::provider_fault).
    pub(crate) refusal: Option<Refusal>,
}
impl ProviderIo {
    /// Per-call transport state supplied by the gateway and its plugins.
    pub fn extensions(&self) -> &Extensions {
        &self.extensions
    }

    /// Take an answer for a success if `success` says so, instead of HTTP's
    /// own rule: a 2xx, or a 101 to a call that asked to switch protocols.
    /// A WebSocket format, say, takes only its handshake's 101.
    pub fn with_success(
        mut self,
        success: impl FnOnce(&ResponseHead, &RequestHead) -> bool + Send + 'static,
    ) -> Self {
        self.success = Some(Box::new(success));
        self
    }

    /// Read a refused answer, its body whole up to the transport's limit,
    /// into its fault with `refusal`, such as from the error body a format
    /// writes, instead of [`provider_fault`], which reads none of it.
    ///
    /// [`provider_fault`]: respan_gateway_http_faults::provider_fault
    pub fn with_refusal(
        mut self,
        refusal: impl FnOnce(http::Response<Bytes>) -> Fault + Send + 'static,
    ) -> Self {
        self.refusal = Some(Box::new(refusal));
        self
    }

    /// Bind the call's encoder and its answer's decoder to core events.
    ///
    /// `encode` writes the call's head, given as `GET /` with no headers, and
    /// returns the encoder of its body. It is given the call's one input, or
    /// `None` for live input, one message per frame, which a format that
    /// takes one value per call refuses. One input is encoded whole now,
    /// before the call is sent; live input is encoded as it comes, as the
    /// provider reads it. Whatever its transport, the call
    /// loses what a proxy does not pass on: a hop's headers but `Upgrade`,
    /// which gets its `Connection: upgrade` (RFC 9110 7.6.1, 7.8); `Host`;
    /// `Expect`; conditions and ranges (13, 14); method overrides;
    /// `Accept-Encoding`, as the gateway decodes what it reads; and the
    /// gateway origin's `Cookie`, `Forwarded`, `Via`, `X-Forwarded-*` and
    /// `X-Real-IP`, which plugins still see ([`Original`](crate::raw::Original)) and a
    /// call's [`CallHeaders`](crate::call::CallHeaders) may set.
    ///
    /// An answer that is a success ([`with_success`](Self::with_success))
    /// opens at its head: `decode` gives its body's [`Decoder`], given its
    /// head and the call as sent, or `None` for no body to read; a fault its
    /// body says later fails it. Any other answer is refused, read into its
    /// fault ([`with_refusal`](Self::with_refusal)). A call
    /// [`Relayed`](crate::raw::Relayed) gives the client its answer's head
    /// and error answer.
    pub fn bind<I, O>(
        self,
        encode: impl FnOnce(&mut RequestHead, Option<&I>) -> Result<Box<dyn Encoder<I>>, Fault>
        + Send
        + 'static,
        decode: impl FnOnce(&ResponseHead, &http::Request<Bytes>) -> Option<Box<dyn Decoder<O>>>
        + Send
        + 'static,
    ) -> Connection<O, I>
    where
        I: Clone + Send + Sync + 'static,
        O: Clone + Send + Sync + 'static,
    {
        let (answer, answered) = oneshot::channel();
        let incoming = async move { answered.await.map_err(|_| Fault::abandoned())? }.boxed();

        let outgoing = EventSink::preparing(move |events: &mut EventStream<I>| {
            if let Some(fault) = events.refusal() {
                return Err(fault.clone());
            }
            let mut head = http::Request::new(()).into_parts().0;
            let (mut body, sink) = match events.is_single() {
                true => {
                    let none = || Fault::internal("a whole request has no input");
                    let input = events.value().transpose()?.ok_or_else(none)?;
                    let mut encoder = encode(&mut head, Some(input))?;
                    let bytes = encoder.encode_all(std::slice::from_ref(input))?;
                    events.stop();
                    (Body::full(bytes), drained())
                }
                false => {
                    let (sink, body) = upload::encoding(encode(&mut head, None)?);
                    (body, sink)
                }
            };
            crate::call::proxy_call(&mut head);
            crate::call::strip_client_origin_headers(&mut head.headers);
            let streaming = body.is_streaming();
            let sent = http::Request::from_parts(
                head.clone(),
                body.full_bytes().cloned().unwrap_or_default(),
            );
            // Only an encoding body streams, and it brings its handoff.
            let handoff = body.upload.take();
            let failed = handoff
                .as_ref()
                .map(|h| h.failed.clone())
                .unwrap_or_default();
            let mut request = http::Request::from_parts(head, body);
            let relayed = self.extensions.get::<crate::raw::Relayed>().copied();
            let client_head = relayed
                .and_then(|_| self.extensions.get::<crate::raw::ClientHead>())
                .cloned();
            let success = self.success;
            let refusal = self.refusal;
            let limit = self.limit;
            request.extensions_mut().insert(self.extensions);
            let failure = failed.clone();
            let settled = handoff.as_ref().map(|h| h.settled.clone());
            let delivery = async move {
                let (ended, ending) = oneshot::channel::<()>();
                let response = async move {
                    let (response, completion) = (self.send)(request).await?;
                    let (head, body) = response.into_parts();
                    let call = sent.clone().into_parts().0;
                    let succeeded = match success {
                        Some(success) => success(&head, &call),
                        None => crate::rules::success(&head, &call),
                    };
                    if let Some(slot) = client_head.filter(|_| succeeded) {
                        slot.set(head.clone());
                    }
                    let body = observed(body, failure, limit);
                    let applied = move |fault| match &relayed {
                        Some(raw) => raw.apply(fault),
                        None => fault,
                    };
                    if !succeeded {
                        let fault = match codec::bytes(body).await {
                            Ok(bytes) => {
                                let answer = http::Response::from_parts(head, bytes);
                                match refusal {
                                    Some(refusal) => refusal(answer),
                                    None => respan_gateway_http_faults::provider_fault(answer),
                                }
                            }
                            Err(fault) => fault,
                        };
                        return Ok((EventStream::refused(applied(fault)), completion));
                    }
                    let mut events = match decode(&head, &sent) {
                        Some(decoder) => codec::decode(body, decoder),
                        None => EventStream::empty(),
                    }
                    .map_err(applied);
                    let response = match streaming && completion.is_none() {
                        true => {
                            let mut ended = Some(ended);
                            EventStream::stream(stream::poll_fn(move |cx| {
                                let item = events.poll_next_unpin(cx);
                                if matches!(item, Poll::Ready(None | Some(Err(_)))) {
                                    ended.take();
                                }
                                item
                            }))
                        }
                        false => EventStream::stream(events),
                    };
                    Ok::<_, Fault>((response, completion))
                }
                .await;
                match response {
                    Ok((response, completion)) => {
                        let _ = answer.send(Ok(response));
                        match completion {
                            Some(completion) => completion.await,
                            None => {
                                if let Some(handoff) = handoff {
                                    let _ = futures::future::select(handoff.draining, ending).await;
                                }
                                Ok(())
                            }
                        }
                    }
                    Err(fault) => {
                        let _ = answer.send(Err(fault.clone()));
                        Err(fault)
                    }
                }
            };
            let delivery = async move {
                let end = delivery.await;
                if let Some(settled) = settled {
                    settled.store(true, std::sync::atomic::Ordering::Release);
                }
                end
            }
            .boxed()
            .shared();
            if streaming {
                let mut completion = delivery.clone();
                let mut source = std::mem::replace(events, EventStream::empty());
                *events = EventStream::stream(stream::poll_fn(move |cx| {
                    if let Poll::Ready(end) = Pin::new(&mut completion).poll(cx) {
                        return Poll::Ready(end.err().map(Err));
                    }
                    Pin::new(&mut source).poll_next(cx)
                }));
            }
            Ok(Upload::new(sink, delivery, failed))
        });
        (incoming, outgoing)
    }
}

/// The sink of a call whose input was encoded whole, or went unread: there
/// is nothing left to send.
fn drained<T: Send + 'static>() -> EventSink<T> {
    EventSink::new(futures::sink::drain().sink_map_err(|never| match never {}))
}

impl std::fmt::Debug for ProviderIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProviderIo").finish_non_exhaustive()
    }
}
