//! HTTP implementation of the core's stream/sink boundary.
//! A client receives inputs and sends outputs; a provider receives outputs
//! and sends inputs. An opening future yields the incoming event stream,
//! and the outgoing sink accepts a stream and closes after delivery.
//!
//! This driver owns protocol details: headers, bytes, lazy decoding,
//! refusal classification, and response-head timing. An ordinary HTTP
//! response opens with its first encoded bytes; a WebSocket upgrade opens
//! before its first event, so either peer can initiate the conversation.
//! Non-success provider responses are decoded until their codec decides
//! whether they are refusals or event streams.
//!
//! Private event-stream metadata retains request and response heads for an
//! eligible relay. Unedited lazy inputs preserve their original bytes;
//! editing inputs preserves relay provenance but encodes the new values.

mod accept;
mod dial;
mod reader;
mod writer;

use std::sync::Arc;

use bytes::Bytes;
use futures::FutureExt;
use futures::channel::oneshot;
use futures::future::BoxFuture;
use respan_gateway_core::{Connection, Extensions, Fault};

#[cfg(doc)]
use respan_gateway_core::Gateway;

pub use accept::accept;
pub use dial::dial;

use crate::{Body, Exchange, Frames, RequestHead, ResponseHead};
#[cfg(doc)]
use crate::{Codec, Encoder};

/// The message a panic in a codec or plugin ends with.
const PANICKED: &str = "a plugin or format panicked";

/// Completion of writing the client-facing body.
pub(crate) type End = BoxFuture<'static, Result<(), Fault>>;

/// Private acknowledgement owned by the body encoder. Dropping it reports
/// abandonment through the receiver held by the edge's writer future.
pub(crate) struct Completion(oneshot::Sender<Result<(), Fault>>);

impl Completion {
    pub(crate) fn finish(self, end: Result<(), Fault>) {
        let _ = self.0.send(end);
    }
}

/// What the client edge wrote, and how the request ended.
type Written = (http::Response<Body>, End);

/// What the client edge hands its adapter, through the wire's slot.
type Writing = BoxFuture<'static, Written>;

/// A transport handle consumed by a format: an accepted exchange or an
/// outbound sender. Clones share the sender; an accepted exchange binds once.
#[derive(Clone)]
pub struct Transport {
    state: Arc<TransportState>,
    extensions: Extensions,
}

type Sending = dyn Fn(Exchange<RequestHead>) -> BoxFuture<'static, Result<Exchange<ResponseHead>, Fault>>
    + Send
    + Sync;

enum TransportState {
    Accepted {
        head: Arc<RequestHead>,
        incoming: std::sync::Mutex<Option<(Frames, bool, oneshot::Sender<Writing>)>>,
    },
    Outbound(Box<Sending>),
}

impl Transport {
    /// Create a reusable outbound sender. Construction performs no I/O.
    /// Return the response once its head arrives; ongoing writes belong in
    /// [`Exchange::write_completion`](crate::Exchange::write_completion).
    /// Send streaming bodies as frames arrive and abort on body errors.
    /// Return redirects unchanged. Apply [`crate::proxy_call`] and
    /// [`crate::credentials`] before sending HTTP calls to providers.
    pub fn new<F>(send: impl Fn(Exchange<RequestHead>) -> F + Send + Sync + 'static) -> Self
    where
        F: std::future::Future<Output = Result<Exchange<ResponseHead>, Fault>> + Send + 'static,
    {
        Self {
            state: Arc::new(TransportState::Outbound(Box::new(move |call| {
                send(call).boxed()
            }))),
            extensions: Extensions::new(),
        }
    }

    /// Accept a head and body, returning the handle and its eventual reply.
    /// Drive the reply concurrently with [`Gateway::handle`], then keep
    /// driving `handle` while consuming the response. [`crate::handle`]
    /// performs this coordination. `None` means no format wrote a reply.
    /// Non-streaming bodies are read whole; the adapter must bound their size.
    pub fn accept(
        head: RequestHead,
        body: Frames,
        streams: bool,
    ) -> (Self, BoxFuture<'static, Option<Written>>) {
        let (slot, writing) = oneshot::channel::<Writing>();
        let transport = Self {
            state: Arc::new(TransportState::Accepted {
                head: Arc::new(head),
                incoming: std::sync::Mutex::new(Some((body, streams, slot))),
            }),
            extensions: Extensions::new(),
        };
        let answer = async move {
            let writing = writing.await.ok()?;
            Some(writing.await)
        };
        (transport, answer.boxed())
    }

    /// The accepted head, available for format matching.
    pub fn head(&self) -> Option<&RequestHead> {
        match self.state.as_ref() {
            TransportState::Accepted { head, .. } => Some(head),
            TransportState::Outbound(_) => None,
        }
    }

    /// Send an encoded call through an outbound handle.
    /// Accepted handles cannot initiate calls.
    pub fn send(
        &self,
        call: Exchange<RequestHead>,
    ) -> BoxFuture<'static, Result<Exchange<ResponseHead>, Fault>> {
        match self.state.as_ref() {
            TransportState::Outbound(send) => send(call),
            TransportState::Accepted { .. } => async {
                Err(Fault::config(
                    "an accepted transport cannot initiate a call",
                ))
            }
            .boxed(),
        }
    }

    pub(crate) fn with_extensions(&self, extensions: Extensions) -> Self {
        Self {
            state: self.state.clone(),
            extensions,
        }
    }
}

impl std::fmt::Debug for Transport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Transport")
            .field("head", &self.head())
            .finish_non_exhaustive()
    }
}

/// Protocol state retained privately on an incoming event stream for exact relay.
/// The body is present only for a single, lazily decoded input.
struct RequestMetadata {
    head: Arc<RequestHead>,
    body: Option<Bytes>,
}

/// A response head is format-owned relay state, never directly exposed as
/// a public metadata payload.
struct ResponseMetadata(ResponseHead);

/// Implements both receive/send orientations of [`Format`](respan_gateway_core::Format) for a
/// [`Codec`](crate::Codec) through HTTP's driver. Both directions consume
/// the same HTTP-owned handle. The codec must be `Clone`, as its streams
/// outlive the call that made them.
///
/// ```
/// # use respan_gateway_http::{ResponseHead, RequestHead, Codec, Decoder, Encoder};
/// # use bytes::Bytes;
/// #[derive(Clone)]
/// struct Echo;
/// impl Codec for Echo {
///     type Input = String;
///     type Output = Vec<u8>;
///     fn name(&self) -> &str { "echo" }
///     fn accepts(&self, _: &RequestHead) -> bool { true }
///     # fn request_decoder(&self, _: &RequestHead) -> Box<dyn Decoder<String>> { unimplemented!() }
///     # fn request_encoder(&self, _: Option<&RequestHead>, _: &mut Bytes, _: bool)
///     #     -> Box<dyn Encoder<String, RequestHead>> { unimplemented!() }
///     # fn response_decoder(&self, _: &ResponseHead, _: &RequestHead, _: &Bytes)
///     #     -> Box<dyn Decoder<Vec<u8>>> { unimplemented!() }
///     # fn response_encoder(&self, _: Option<&ResponseHead>, _: &RequestHead, _: &Bytes)
///     #     -> Box<dyn Encoder<Vec<u8>, ResponseHead>> { unimplemented!() }
///     // ...
/// }
/// respan_gateway_http::impl_format!(Echo);
/// let gateway = respan_gateway_core::Gateway::new().client::<String, Vec<u8>, _>(Echo);
/// assert!(gateway.is_ok());
/// ```
#[macro_export]
macro_rules! impl_format {
    ($t:ty) => {
        impl
            $crate::__private::Format<
                $crate::Transport,
                <$t as $crate::Codec>::Input,
                <$t as $crate::Codec>::Output,
            > for $t
        {
            fn name(&self) -> &str {
                $crate::Codec::name(self)
            }

            fn matches(&self, io: &$crate::Transport) -> bool {
                io.head()
                    .is_some_and(|head| $crate::Codec::accepts(self, head))
            }

            fn bind(
                &self,
                io: $crate::Transport,
            ) -> $crate::__private::Result<
                $crate::__private::Connection<
                    <$t as $crate::Codec>::Input,
                    <$t as $crate::Codec>::Output,
                >,
            > {
                $crate::__private::accept(self, io)
            }
        }

        impl
            $crate::__private::Format<
                $crate::Transport,
                <$t as $crate::Codec>::Output,
                <$t as $crate::Codec>::Input,
            > for $t
        {
            fn name(&self) -> &str {
                $crate::Codec::name(self)
            }

            fn bind(
                &self,
                io: $crate::Transport,
            ) -> $crate::__private::Result<
                $crate::__private::Connection<
                    <$t as $crate::Codec>::Output,
                    <$t as $crate::Codec>::Input,
                >,
            > {
                $crate::__private::dial(self, io)
            }
        }
    };
}

/// Drive the gateway alongside an HTTP reply, then let its body own the
/// remaining work. Cancelling either stage cancels the gateway exchange.
#[doc(hidden)]
pub async fn drive(
    handled: BoxFuture<'static, Result<(), Fault>>,
    answer: BoxFuture<'static, Option<Written>>,
) -> Result<Written, Fault> {
    use futures::future::{Either, select};
    let (answer, pending) = match select(handled, answer).await {
        Either::Left((end, answer)) => (answer.await, Err(end)),
        Either::Right((answer, handled)) => (answer, Ok(handled)),
    };
    match answer {
        Some((response, ended)) => {
            let mut handled = pending.ok();
            // An empty response may already have completed while its head
            // was encoded. Settle that work even if no body is polled.
            futures::future::poll_fn(|cx| {
                poll_handle(&mut handled, cx);
                std::task::Poll::Ready(())
            })
            .await;
            let response = response.map(|body| {
                use http_body_util::BodyExt;
                Pump { handled, body }.boxed_unsync()
            });
            Ok((response, ended))
        }
        None => match pending {
            Err(Err(fault)) => Err(fault),
            Ok(handled) => {
                handled.await?;
                Err(Fault::internal("no format answered"))
            }
            Err(Ok(())) => Err(Fault::internal("no format answered")),
        },
    }
}

/// The gateway future drops first, ending its calls before their output
/// streams are dropped by the protocol body.
struct Pump {
    handled: Option<BoxFuture<'static, Result<(), Fault>>>,
    body: Body,
}

fn poll_handle(
    handled: &mut Option<BoxFuture<'static, Result<(), Fault>>>,
    cx: &mut std::task::Context<'_>,
) {
    if let Some(work) = handled
        && work.as_mut().poll(cx).is_ready()
    {
        *handled = None;
    }
}

impl http_body::Body for Pump {
    type Data = Bytes;
    type Error = Fault;

    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Bytes>, Fault>>> {
        let this = self.get_mut();
        poll_handle(&mut this.handled, cx);
        let frame = std::pin::Pin::new(&mut this.body).poll_frame(cx);
        // Reading the final frame can complete the writer. Observe it now,
        // before a consumer drops the body immediately after that frame.
        poll_handle(&mut this.handled, cx);
        if matches!(frame, std::task::Poll::Ready(None)) && this.handled.is_some() {
            return std::task::Poll::Pending;
        }
        frame
    }

    fn is_end_stream(&self) -> bool {
        self.handled.is_none() && self.body.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.body.size_hint()
    }
}
