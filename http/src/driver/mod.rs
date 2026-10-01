//! Pull-driven HTTP binding and delivery coordination.

mod accept;
mod dial;
mod reader;
pub(crate) mod upload;
mod writer;

use crate::{Body, RequestHead, ResponseHead};
use bytes::Bytes;
use futures::FutureExt;
use futures::future::BoxFuture;
use respan_gateway_sdk::Fault;
use std::sync::Arc;

pub use accept::{Incoming, Reply};
pub use dial::Outgoing;
pub(crate) use dial::Sending;
const PANICKED: &str = "a plugin or format panicked";
pub(crate) type End = BoxFuture<'static, Result<(), Fault>>;
pub(crate) type Written = (http::Response<Body>, End);

pub(crate) struct RequestMetadata {
    pub(crate) head: Arc<RequestHead>,
    pub(crate) body: Option<Bytes>,
}
pub(crate) struct ResponseMetadata(pub(crate) ResponseHead);

/// Drive the gateway alongside an HTTP reply, then let its body own the
/// remaining work. Cancelling either stage cancels the gateway exchange.
#[doc(hidden)]
pub async fn drive(
    handled: BoxFuture<'static, Result<(), Fault>>,
    answer: BoxFuture<'static, Option<Written>>,
) -> Result<Written, Fault> {
    use futures::future::{Either, select};
    let (answer, pending) = match select(handled, answer).await {
        Either::Left((end, answer)) => {
            // Preserve an already prepared reply, but do not wait for a
            // sender retained by an unused clone after handling has ended.
            (answer.now_or_never().flatten(), Err(end))
        }
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
            let response =
                response.map(|body| Body::stream(crate::body_of(Pump { handled, body })));
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
