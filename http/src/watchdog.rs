//! Bounds an outbound sender's head and polled body-frame waits.

use crate::{Body, Completion};
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt, stream};
use respan_gateway_sdk::{Fault, Upstream};
use std::sync::Arc;
use std::time::Duration;

/// Decorate an async sending function with head and per-polled-frame timeouts.
/// Native response headers/extensions and independent write completion survive.
/// Idle time is measured only while the response body is being polled.
// Keep the native callback signature rather than introducing a sender facade.
#[allow(clippy::type_complexity)]
pub fn watchdog<F, Fut>(
    inner: F,
    head: Duration,
    idle: Duration,
) -> impl Fn(
    http::Request<Body>,
) -> BoxFuture<'static, Result<(http::Response<Body>, Option<Completion>), Fault>>
+ Clone
+ Send
+ Sync
+ 'static
where
    F: Fn(http::Request<Body>) -> Fut + Send + Sync + 'static,
    Fut:
        Future<Output = Result<(http::Response<Body>, Option<Completion>), Fault>> + Send + 'static,
{
    let inner = Arc::new(inner);
    move |call| {
        let inner = inner.clone();
        async move {
            let stalled =
                |d: Duration| Upstream::fault(format!("the provider sent nothing for {d:?}"));
            let (response, completion) = tokio::time::timeout(head, inner(call))
                .await
                .map_err(|_| stalled(head))??;
            let response = response.map(|body| {
                Body::stream(stream::unfold(Some(body), move |body| async move {
                    let mut body = body?;
                    match tokio::time::timeout(idle, body.next()).await {
                        Ok(item) => Some((item?, Some(body))),
                        Err(_) => Some((Err(stalled(idle)), None)),
                    }
                }))
            });
            Ok((response, completion))
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }
    fn request() -> http::Request<Body> {
        http::Request::new(Body::default())
    }

    #[tokio::test(start_paused = true)]
    async fn a_silent_head_times_out() {
        let send = watchdog(|_| async { std::future::pending().await }, secs(5), secs(1));
        let start = tokio::time::Instant::now();
        let fault = send(request()).await.err().expect("head timeout");
        assert!(fault.is::<Upstream>());
        assert_eq!(fault.message(), "the provider sent nothing for 5s");
        assert_eq!(start.elapsed(), secs(5));
    }
    #[tokio::test(start_paused = true)]
    async fn idle_is_measured_between_polled_frames() {
        let send = watchdog(
            |_| async {
                Ok((
                    http::Response::new(Body::stream(
                        stream::iter([Ok(Bytes::from_static(b"first"))]).chain(stream::pending()),
                    )),
                    None,
                ))
            },
            secs(5),
            secs(1),
        );
        let (mut resp, _) = send(request()).await.expect("head");
        assert_eq!(
            resp.body_mut()
                .next()
                .await
                .expect("expected value")
                .expect("expected value"),
            "first"
        );
        tokio::time::sleep(secs(10)).await;
        let start = tokio::time::Instant::now();
        let fault = resp
            .body_mut()
            .next()
            .await
            .expect("expected value")
            .expect_err("expected failure");
        assert!(fault.is::<Upstream>());
        assert_eq!(fault.message(), "the provider sent nothing for 1s");
        assert_eq!(start.elapsed(), secs(1));
        assert!(resp.body_mut().next().await.is_none());
    }
    #[tokio::test(start_paused = true)]
    async fn a_whole_response_can_take_longer_than_idle_to_open() {
        let send = watchdog(
            |_| async {
                tokio::time::sleep(secs(3)).await;
                Ok((http::Response::new(Body::full("whole")), None))
            },
            secs(5),
            secs(1),
        );
        let start = tokio::time::Instant::now();
        let (mut resp, _) = send(request()).await.expect("head before deadline");
        assert_eq!(start.elapsed(), secs(3));
        assert_eq!(
            resp.body_mut()
                .next()
                .await
                .expect("expected value")
                .expect("expected value"),
            "whole"
        );
        assert!(resp.body_mut().next().await.is_none());
    }
    #[tokio::test]
    async fn response_exchange_fields_and_write_completion_survive() {
        #[derive(Clone, Debug, PartialEq, Eq)]
        struct Marker(&'static str);
        let send = watchdog(
            |_| async {
                let mut response = http::Response::new(Body::full("data"));
                response
                    .headers_mut()
                    .insert("x-provider", "there".parse().expect("expected value"));
                response.extensions_mut().insert(Marker("preserved"));
                let completion: Completion =
                    async { Err(Upstream::fault("late write failure")) }.boxed();
                Ok((response, Some(completion)))
            },
            secs(5),
            secs(1),
        );
        let (mut response, completion) = send(request()).await.expect("head");
        assert_eq!(response.headers()["x-provider"], "there");
        assert_eq!(
            response.extensions().get::<Marker>(),
            Some(&Marker("preserved"))
        );
        assert_eq!(
            response
                .body_mut()
                .next()
                .await
                .expect("expected value")
                .expect("expected value"),
            "data"
        );
        assert!(response.body_mut().next().await.is_none());
        assert_eq!(
            completion
                .expect("expected value")
                .await
                .expect_err("expected failure")
                .message(),
            "late write failure"
        );
    }
}
