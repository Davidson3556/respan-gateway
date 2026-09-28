//! A stall bound on a provider's body.

use std::time::Duration;

use respan_gateway_core::futures::StreamExt;
use respan_gateway_core::http_body_util::{BodyExt, BodyStream, StreamBody};
use respan_gateway_core::{Body, Fault, Transport, async_trait, http};

/// A transport that fails a response whose body is silent for `idle`,
/// between any two frames, so a stalled stream ends as an error in the
/// client's framing rather than hanging. The core has no stall bound yet;
/// a transport can add one because it owns the body.
#[derive(Debug, Clone)]
pub struct Watchdog<T> {
    inner: T,
    idle: Duration,
}

impl<T: Transport> Watchdog<T> {
    /// Bound `inner`'s silences by `idle`.
    pub fn new(inner: T, idle: Duration) -> Self {
        Self { inner, idle }
    }
}

#[async_trait]
impl<T: Transport> Transport for Watchdog<T> {
    async fn send(&self, call: http::Request<Body>) -> Result<http::Response<Body>, Fault> {
        let idle = self.idle;
        let (head, body) = self.inner.send(call).await?.into_parts();
        let frames = BodyStream::new(body);
        let bounded = respan_gateway_core::futures::stream::unfold(
            Some(Box::pin(frames)),
            move |frames| async move {
                let mut frames = frames?;
                match tokio::time::timeout(idle, frames.next()).await {
                    Ok(Some(frame)) => Some((frame, Some(frames))),
                    Ok(None) => None,
                    Err(_) => Some((Err(Fault::upstream("the provider stalled")), None)),
                }
            },
        );
        Ok(http::Response::from_parts(
            head,
            StreamBody::new(bounded).boxed_unsync(),
        ))
    }
}
