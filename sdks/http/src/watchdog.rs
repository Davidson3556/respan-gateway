//! Bounds an outbound HTTP transport's head and per-frame idle waits.

use std::time::Duration;

use futures::StreamExt;
use futures::stream;
use respan_gateway_core::Fault;

use crate::{Exchange, RequestHead, Transport};

/// Bound a provider's wait for its head by `head`, and each gap between
/// polled body frames by `idle`. A timeout is an `upstream` fault, eligible
/// for retries when the caller selects an appropriate classifier.
///
/// A whole (non-streaming) answer sends its head only once it is done, so
/// `head` is the longest answer to wait for; `idle` bounds a stream's gaps.
/// Response heads, extensions, relay and streaming flags, and outgoing
/// `write_completion` are preserved unchanged.
pub fn watchdog(inner: impl Into<Transport>, head: Duration, idle: Duration) -> Transport {
    let inner = inner.into();
    Transport::new(move |call: Exchange<RequestHead>| {
        let inner = inner.clone();
        async move {
            let stalled =
                |d: Duration| Fault::upstream(format!("the provider sent nothing for {d:?}"));
            let resp = tokio::time::timeout(head, inner.send(call)).await;
            let mut resp = resp.map_err(|_| stalled(head))??;
            let bounded = stream::unfold(Some(resp.body), move |frames| async move {
                let mut frames = frames?;
                match tokio::time::timeout(idle, frames.next()).await {
                    Ok(frame) => Some((frame?, Some(frames))),
                    Err(_) => Some((Err(stalled(idle)), None)),
                }
            });
            resp.body = bounded.boxed();
            Ok(resp)
        }
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::Bytes;
    use futures::stream;
    use futures::{FutureExt, StreamExt};
    use respan_gateway_core::{Fault, codes};

    use super::watchdog;
    use crate::{Exchange, Transport, fresh_request_head, fresh_response_head};

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    fn request() -> Exchange<crate::RequestHead> {
        Exchange::new(fresh_request_head(), stream::empty().boxed())
    }

    #[tokio::test(start_paused = true)]
    async fn a_silent_head_times_out() {
        let inner = Transport::new(|_| async { std::future::pending().await });
        let transport = watchdog(inner, secs(5), secs(1));
        let start = tokio::time::Instant::now();
        let fault = transport.send(request()).await.expect_err("head timeout");
        assert_eq!(fault.code(), codes::UPSTREAM);
        assert_eq!(fault.message(), "the provider sent nothing for 5s");
        assert_eq!(start.elapsed(), secs(5));
    }

    #[tokio::test(start_paused = true)]
    async fn idle_is_measured_between_polled_frames() {
        let inner = Transport::new(|_| async {
            let frames = stream::iter([Ok(Bytes::from_static(b"first"))])
                .chain(stream::pending())
                .boxed();
            Ok(Exchange::new(fresh_response_head(), frames))
        });
        let transport = watchdog(inner, secs(5), secs(1));
        let mut resp = transport.send(request()).await.expect("head");
        assert_eq!(
            resp.body.next().await.expect("first frame").expect("data"),
            "first"
        );
        tokio::time::sleep(secs(10)).await;
        let start = tokio::time::Instant::now();
        let fault = resp
            .body
            .next()
            .await
            .expect("idle fault frame")
            .expect_err("idle timeout");
        assert_eq!(fault.code(), codes::UPSTREAM);
        assert_eq!(fault.message(), "the provider sent nothing for 1s");
        assert_eq!(start.elapsed(), secs(1));
        assert!(resp.body.next().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn a_whole_response_can_take_longer_than_idle_to_open() {
        let inner = Transport::new(|_| async {
            tokio::time::sleep(secs(3)).await;
            Ok(Exchange::new(
                fresh_response_head(),
                stream::iter([Ok(Bytes::from_static(b"whole"))]).boxed(),
            ))
        });
        let start = tokio::time::Instant::now();
        let mut resp = watchdog(inner, secs(5), secs(1))
            .send(request())
            .await
            .expect("head before deadline");
        assert_eq!(start.elapsed(), secs(3));
        assert_eq!(
            resp.body.next().await.expect("whole frame").expect("data"),
            "whole"
        );
        assert!(resp.body.next().await.is_none());
    }

    #[tokio::test]
    async fn response_exchange_fields_and_write_completion_survive() {
        #[derive(Debug, Clone, PartialEq, Eq)]
        struct Marker(&'static str);

        let inner = Transport::new(|_| async {
            let mut resp = Exchange::new(
                fresh_response_head(),
                stream::iter([Ok(Bytes::from_static(b"data"))]).boxed(),
            );
            resp.head
                .headers
                .insert("x-provider", "there".parse().expect("header value"));
            resp.extensions.insert(Marker("preserved"));
            resp.relayed = true;
            resp.streams = true;
            resp.write_completion =
                Some(async { Err(Fault::upstream("late write failure")) }.boxed());
            Ok(resp)
        });
        let mut resp = watchdog(inner, secs(5), secs(1))
            .send(request())
            .await
            .expect("head");
        assert_eq!(resp.head.headers["x-provider"], "there");
        assert_eq!(resp.extensions.get::<Marker>(), Some(&Marker("preserved")));
        assert!(resp.relayed && resp.streams);
        assert_eq!(
            resp.body.next().await.expect("body frame").expect("data"),
            "data"
        );
        assert!(resp.body.next().await.is_none());
        let fault = resp
            .write_completion
            .take()
            .expect("write completion retained")
            .await
            .expect_err("late write failure retained");
        assert_eq!(fault.message(), "late write failure");
    }
}
