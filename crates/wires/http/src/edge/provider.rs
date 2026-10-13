//! Native HTTP sending bound to the kernel's symmetric event interface.

use super::reader::{Failure, observed};
use super::upload::{self, Upload};

use crate::codec;
use crate::{Body, Completion, Decoder, Encoder};
use bytes::Bytes;
use futures::channel::oneshot;
use futures::future::BoxFuture;
use futures::{Future, FutureExt, Stream, StreamExt, TryStreamExt, stream};
use http::{request::Parts as RequestHead, response::Parts as ResponseHead};
use respan_gateway_core::{Connection, EventSink, EventStream, Extensions, Fault, RetryAfter};
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

/// Make a cloneable provider factory from an ordinary async sending function.
/// `Some(completion)` remains authoritative even after response EOF; `None`
/// means only the outgoing handoff is complete, not later socket writes.
/// `limit` is the most bytes a reader holds of one answer at once: a whole
/// body, a refusal's, or one event; past it, the answer fails `upstream`.
pub fn connect<F, Fut>(
    send: F,
    limit: usize,
) -> impl Fn(Extensions) -> ProviderIo + Clone + Send + Sync + 'static
where
    F: Fn(http::Request<Body>) -> Fut + Send + Sync + 'static,
    Fut:
        Future<Output = Result<(http::Response<Body>, Option<Completion>), Fault>> + Send + 'static,
{
    let send: Arc<Sending> = Arc::new(move |request| send(request).boxed());
    move |extensions| ProviderIo {
        send: send.clone(),
        extensions,
        limit,
        success: Box::new(crate::rules::success),
        refusal: Box::new(respan_gateway_http_faults::provider_fault),
    }
}

pub(crate) type Sending = dyn Fn(
        http::Request<Body>,
    ) -> BoxFuture<'static, Result<(http::Response<Body>, Option<Completion>), Fault>>
    + Send
    + Sync;

/// What it takes for an answer to succeed ([`ProviderIo::with_success`]).
type Success = Box<dyn FnOnce(&ResponseHead, &RequestHead) -> bool + Send>;

/// One provider attempt, owning its extensions and sharing only the sender.
pub struct ProviderIo {
    send: Arc<Sending>,
    extensions: Extensions,
    /// The most bytes a reader holds of its answer at once.
    limit: usize,
    /// What it takes for a success.
    success: Success,
    /// How a refused answer reads as a fault.
    refusal: Box<dyn FnOnce(http::Response<Bytes>) -> Fault + Send>,
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
        self.success = Box::new(success);
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
        self.refusal = Box::new(refusal);
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
    /// head and the call as sent (its body the whole bytes of one input, or
    /// empty for live input, which streamed), or `None` for no body to read; a fault its
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
            // A whole call has one input, encoded now; live input streams,
            // encoded as the transport reads it, and drains when it did.
            let failed = Failure::default();
            let (body, encoding, draining) = match events.is_single() {
                true => {
                    let none = || Fault::internal("a whole request has no input");
                    let input = events.value().transpose()?.ok_or_else(none)?;
                    let mut encoder = encode(&mut head, Some(input))?;
                    let bytes = encoder.encode_all(std::slice::from_ref(input))?;
                    events.stop();
                    (Body::full(bytes), None, None)
                }
                false => {
                    let (encoding, body, draining) =
                        upload::encoding(encode(&mut head, None)?, failed.clone());
                    (body, Some(encoding), Some(draining))
                }
            };
            crate::call::proxy_call(&mut head);
            let streaming = encoding.is_some();
            let sent = http::Request::from_parts(
                head.clone(),
                body.full_bytes().cloned().unwrap_or_default(),
            );
            let mut request = http::Request::from_parts(head, body);
            let relayed = self.extensions.get::<crate::raw::Relayed>().copied();
            let client_head = relayed
                .and_then(|_| self.extensions.get::<crate::raw::ClientHead>())
                .cloned();
            let success = self.success;
            let refusal = self.refusal;
            let limit = self.limit;
            request.extensions_mut().insert(self.extensions);
            let upload = failed.clone();
            // The call's delivery, which the sink drives and every poll of
            // the upload's source watches, runs in this order:
            //
            // 1. Send the call; a transport error fails the answer and the
            //    delivery alike.
            // 2. Hand the answer over (`answer`): refused, read whole into its
            //    fault, or opened with its decoder.
            // 3. Wait for the upload to settle: the transport's `completion`
            //    if it gave one; else, for a streamed upload, until the byte
            //    reader drained it (`draining`) or the answer ended
            //    (`ending`: the answer's stream drops `ended` at its end, as
            //    a provider that answered in full reads no more input). A
            //    refusal drops `ended` at once: a refused call's upload is
            //    cancelled, not awaited. A whole call has nothing to wait for.
            //
            // A streamed upload's source ends with the delivery: done, the
            // caller's input stops being read; failed, it reads the fault.
            let delivery = async move {
                let (ended, ending) = oneshot::channel::<()>();
                let response = async move {
                    let (response, completion) = (self.send)(request).await?;
                    let (head, body) = response.into_parts();
                    let call = sent.clone().into_parts().0;
                    let succeeded = success(&head, &call);
                    if let Some(slot) = client_head.filter(|_| succeeded) {
                        slot.set(head.clone());
                    }
                    let body = observed(body, upload.clone(), limit);
                    let applied = move |fault| match &relayed {
                        Some(raw) => raw.apply(fault),
                        None => fault,
                    };
                    if !succeeded {
                        let fault = match codec::collect(body, limit, || {
                            respan_gateway_sse::over_limit(limit)
                        })
                        .await
                        {
                            Ok(bytes) => refusal(http::Response::from_parts(head, bytes)),
                            // A failed upload says why the call failed.
                            Err(fault) if upload.get().is_some() => fault,
                            // An answer whose body is over the limit or broke
                            // off is the gateway's 502, as it cannot be read
                            // or relayed, but its `Retry-After` still says
                            // when to try again.
                            Err(fault) => {
                                let answer = http::Response::from_parts(head, Bytes::new());
                                let head = respan_gateway_http_faults::provider_fault(answer);
                                match head.get::<RetryAfter>() {
                                    Some(wait) => fault.with(*wait),
                                    None => fault,
                                }
                            }
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
                                if let Some(draining) = draining {
                                    let _ = futures::future::select(draining, ending).await;
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
            let delivery = delivery.boxed().shared();
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
            Ok(Upload::new(encoding, delivery, failed))
        });
        (incoming, outgoing)
    }
}

impl std::fmt::Debug for ProviderIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProviderIo").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Decoder;
    use futures::SinkExt;
    use respan_gateway_http_faults::status;

    /// A refusal whose body is over the limit is the gateway's 502, but
    /// keeps the provider's `Retry-After`.
    #[tokio::test]
    async fn a_refusal_over_the_limit_keeps_its_retry_after() {
        let send = |_| async {
            let answer = http::Response::builder()
                .status(429)
                .header("retry-after", "7")
                .body(Body::full("far more than four bytes"));
            Ok((answer.unwrap_or_default(), None))
        };
        let io = connect(send, 4)(Extensions::new());
        let (incoming, mut outgoing) = io.bind(
            |_, _| {
                Ok(Box::new(|value: &Bytes, out: &mut Vec<Bytes>| {
                    out.push(value.clone());
                    Ok(())
                }))
            },
            |_, _| None::<Box<dyn Decoder<Bytes>>>,
        );
        let mut input = EventStream::single(Bytes::from_static(b"q"));
        assert!(outgoing.prepare(&mut input).is_ok());
        let (answer, _) = futures::join!(incoming, outgoing.close());
        let refused = answer.map(|events| events.refusal().cloned());
        let Ok(Some(fault)) = refused else {
            panic!("refused")
        };
        assert_eq!(status(&fault), http::StatusCode::BAD_GATEWAY);
        assert_eq!(
            fault.get(),
            Some(&RetryAfter(std::time::Duration::from_secs(7)))
        );
    }
}
