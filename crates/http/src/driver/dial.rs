//! Native HTTP sending bound to the kernel's symmetric event interface.

use super::reader::observed;
use super::upload::Upload;

use crate::{Body, Completion};
use bytes::Bytes;
use futures::channel::oneshot;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{Future, FutureExt, SinkExt, Stream, StreamExt, TryStreamExt, stream};
use http::{request::Parts as RequestHead, response::Parts as ResponseHead};
use respan_gateway_sdk::{Connection, EventSink, EventStream, Extensions, Fault, Upstream};
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

pub(crate) type Sending = dyn Fn(
        http::Request<Body>,
    ) -> BoxFuture<'static, Result<(http::Response<Body>, Option<Completion>), Fault>>
    + Send
    + Sync;

/// One provider attempt, owning its extensions and sharing only the sender.
pub struct Outgoing {
    pub(crate) send: Arc<Sending>,
    pub(crate) extensions: Extensions,
    /// The most bytes a reader holds of its answer at once.
    pub(crate) limit: usize,
}
impl Outgoing {
    /// Per-call transport state supplied by the gateway and its plugins.
    pub fn extensions(&self) -> &Extensions {
        &self.extensions
    }

    /// Bind synchronous request preparation and a standard response stream.
    /// The answer is refused, with the first fault `read` gives, if
    /// `success` says it is no success; else it opens, and a fault that
    /// comes later fails it. A call's [`RawErrors`](crate::RawErrors) give
    /// each fault of its answer the provider's error answer to write.
    pub fn bind<I, O, P, R, S>(self, prepare: P, read: R, success: S) -> Connection<O, I>
    where
        I: Clone + Send + Sync + 'static,
        O: Clone + Send + Sync + 'static,
        P: FnOnce(&mut EventStream<I>) -> Result<(http::Request<Body>, EventSink<I>), Fault>
            + Send
            + 'static,
        R: FnOnce(
                http::Response<Body>,
                http::Request<Bytes>,
            ) -> BoxStream<'static, Result<O, Fault>>
            + Send
            + 'static,
        S: FnOnce(&ResponseHead, &RequestHead) -> bool + Send + 'static,
    {
        let (answer, answered) = oneshot::channel();
        let incoming = async move { answered.await.map_err(|_| Fault::abandoned())? }.boxed();

        let outgoing = EventSink::preparing(move |events: &mut EventStream<I>| {
            if let Some(fault) = events.refusal() {
                return Err(fault.clone());
            }
            let (mut request, mut sink) = match self.extensions.get::<crate::SendRaw>() {
                Some(crate::SendRaw(wire)) => {
                    let raw = wire
                        .get()
                        .ok_or_else(|| Fault::internal("raw request unavailable"))?
                        .clone();
                    let (head, bytes) = raw.into_parts();
                    if wire.is_live() {
                        let (encoded, sink) = prepare(events)?;
                        (http::Request::from_parts(head, encoded.into_body()), sink)
                    } else {
                        events.stop();
                        (
                            http::Request::from_parts(head, Body::full(bytes)),
                            EventSink::new(
                                futures::sink::drain().sink_map_err(|never| match never {}),
                            ),
                        )
                    }
                }
                None => prepare(events)?,
            };
            sink.prepare(events)?;
            let streaming = request.body().is_streaming();
            if !streaming {
                events.stop();
            }
            let (head, mut body) = request.into_parts();
            let initial = body.full_bytes().cloned().unwrap_or_default();
            let sent = http::Request::from_parts(head.clone(), initial);
            let mut handoff = body.upload.take();
            if streaming && handoff.is_none() {
                let (drained, draining) = oneshot::channel::<()>();
                let mut drained = Some(drained);
                let failed = super::reader::Failure::default();
                let failure = failed.clone();
                body = Body::stream(stream::poll_fn(move |cx| {
                    let item = Pin::new(&mut body).poll_next(cx);
                    if let Poll::Ready(Some(Err(fault))) = &item {
                        let _ = failure.set(fault.clone());
                    }
                    if matches!(item, Poll::Ready(None | Some(Err(_)))) {
                        drained.take();
                    }
                    item
                }));
                handoff = Some(super::upload::Handoff {
                    draining: draining.map(|_| Ok(())).boxed(),
                    failed,
                    settled: Default::default(),
                });
            }
            let failed = handoff
                .as_ref()
                .map(|h| h.failed.clone())
                .unwrap_or_default();
            request = http::Request::from_parts(head, body);
            let provider_head = self.extensions.get::<crate::ProviderHead>().cloned();
            let raw_errors = self.extensions.get::<crate::RawErrors>().cloned();
            let limit = self.limit;
            request.extensions_mut().insert(self.extensions);
            let failure = failed.clone();
            let settled = handoff.as_ref().map(|h| h.settled.clone());
            let delivery = async move {
                let (ended, ending) = oneshot::channel::<()>();
                let response = async move {
                    let (response, completion) = (self.send)(request).await?;
                    let (head, body) = response.into_parts();
                    if let Some(slot) = provider_head {
                        slot.set(Some(head.clone()));
                    }
                    let refused = !success(&head, &sent.clone().into_parts().0);
                    let status = head.status;
                    let body = observed(body, failure, limit);
                    let stream = read(http::Response::from_parts(head, body), sent);
                    let mut stream = match raw_errors {
                        Some(raw) => stream.map_err(move |fault| raw.apply(fault)).boxed(),
                        None => stream,
                    };
                    let response = if refused {
                        let fault = match stream.next().await {
                            Some(Err(fault)) => fault,
                            _ => Upstream::fault(crate::formats::whole::answered(status)),
                        };
                        EventStream::refused(fault)
                    } else if streaming && completion.is_none() {
                        let mut ended = Some(ended);
                        EventStream::stream(stream::poll_fn(move |cx| {
                            let item = stream.as_mut().poll_next(cx);
                            if matches!(item, Poll::Ready(None | Some(Err(_)))) {
                                ended.take();
                            }
                            item
                        }))
                    } else {
                        EventStream::stream(stream)
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

impl std::fmt::Debug for Outgoing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Outgoing").finish_non_exhaustive()
    }
}
