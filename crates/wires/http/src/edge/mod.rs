//! How HTTP meets the kernel: the client's edge ([`ClientIo`]), the provider's
//! ([`ProviderIo`]), and the bodies and encoders between them.

mod body;
mod client;
mod encoder;
mod provider;
mod reader;
mod upload;
mod writer;

use bytes::Bytes;
use futures::FutureExt;
use futures::future::BoxFuture;
use http_body_util::BodyExt;
use respan_gateway_core::Fault;

pub use body::Body;
pub use client::{ClientIo, Framing, Reply, handle};
pub use encoder::{Encoder, encode_all, messages};
pub use provider::{ProviderIo, connect};

const PANICKED: &str = "a plugin or format panicked";
pub(crate) type Written = (http::Response<Body>, crate::Completion);

/// Drive the gateway alongside an HTTP reply, then let its body own the
/// remaining work. Cancelling either stage cancels the gateway exchange.
pub(crate) async fn drive(
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
                response.map(|body| Body::stream(Pump { handled, body }.into_data_stream()));
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::passthrough::Passthrough;
    use respan_gateway_core::{EventStream, Format, Upstream};

    /// A reply prepared before handling ends is kept, however it ended.
    #[tokio::test]
    async fn a_completed_handler_keeps_an_already_ready_reply() {
        let request = http::Request::post("/x").body(Body::full("q"));
        let request = request.unwrap_or_default();
        let (incoming, answer) = ClientIo::accept(request, Framing::Whole, 1024);
        let Ok((opening, mut outgoing)) = Passthrough.bind(incoming) else {
            panic!("bound")
        };
        assert!(opening.await.is_ok());
        let mut refused = EventStream::refused(Upstream::fault("refused"));
        assert!(outgoing.prepare(&mut refused).is_ok());
        let handled = futures::future::ready(Err(Upstream::fault("handling failed"))).boxed();
        let response = drive(handled, answer)
            .await
            .map(|(response, _)| response.status());
        assert_eq!(response.ok(), Some(http::StatusCode::BAD_GATEWAY));
    }
}
