//! Generic read-ahead preserves replay and lets each caller choose its predicate.
#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use respan_gateway_core::futures::{StreamExt, stream};
use respan_gateway_core::{Fault, Response};

#[tokio::test]
async fn different_predicates_recheck_the_buffer_and_replay_every_event() {
    let reads = Arc::new(AtomicUsize::new(0));
    let mut response = Response::stream(stream::iter(0..5u32).map({
        let reads = reads.clone();
        move |value| {
            reads.fetch_add(1, Ordering::SeqCst);
            Ok(value)
        }
    }));
    response
        .buffer_until(|value: &u32| *value == 1)
        .await
        .unwrap();
    assert_eq!(reads.load(Ordering::SeqCst), 2);
    response
        .buffer_until(|value: &u32| *value == 0)
        .await
        .unwrap();
    assert_eq!(reads.load(Ordering::SeqCst), 2);
    response
        .buffer_until(|value: &u32| *value == 3)
        .await
        .unwrap();
    assert_eq!(reads.load(Ordering::SeqCst), 4);
    assert_eq!(
        response.outputs::<u32>().collect::<Vec<_>>().await,
        (0..5).map(Ok).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn a_match_before_fault_keeps_the_fault_for_replay() {
    let failure = Fault::upstream("cut");
    let mut response = Response::stream(stream::iter([Ok(0u32), Err(failure.clone())]));
    assert_eq!(response.buffer_until(|_: &u32| true).await, Ok(()));
    assert_eq!(
        response.outputs::<u32>().collect::<Vec<_>>().await,
        vec![Ok(0), Err(failure)]
    );
}

#[tokio::test]
async fn a_fault_before_a_match_retains_values_and_end_after_failure() {
    let failure = Fault::upstream("cut");
    let mut response = Response::stream(stream::iter([Ok(0u32), Err(failure.clone())]));
    assert_eq!(
        response.buffer_until(|value: &u32| *value > 0).await,
        Err(failure.clone())
    );
    assert_eq!(response.until_end().await, Err(failure.clone()));
    assert_eq!(
        response.outputs::<u32>().collect::<Vec<_>>().await,
        vec![Ok(0), Err(failure)]
    );
}

#[tokio::test]
async fn a_clean_end_without_a_match_preserves_all_events() {
    let mut response = Response::stream(stream::iter([Ok(0u32), Ok(1)]));
    response.buffer_until(|_: &u32| false).await.unwrap();
    assert_eq!(
        response.outputs::<u32>().collect::<Vec<_>>().await,
        [Ok(0), Ok(1)]
    );
}

#[tokio::test]
async fn a_wrong_event_type_is_rejected_without_polling_the_source() {
    let mut response = Response::stream(stream::poll_fn(|_| {
        panic!("a type mismatch must not read events");
        #[allow(unreachable_code)]
        std::task::Poll::Ready(Some(Ok::<u32, Fault>(0)))
    }));
    assert!(response.buffer_until(|_: &String| true).await.is_err());
}

#[tokio::test]
async fn hot_synthetic_buffers_yield_and_release_the_source_on_cancellation() {
    use respan_gateway_core::futures::FutureExt;
    for to_end in [false, true] {
        let reads = Arc::new(AtomicUsize::new(0));
        let mut response = Response::stream(stream::iter(0..100_000u32).map({
            let reads = reads.clone();
            move |value| {
                reads.fetch_add(1, Ordering::SeqCst);
                Ok(value)
            }
        }));
        let mut buffering = if to_end {
            response.until_end().boxed()
        } else {
            response.buffer_until(|_: &u32| false).boxed()
        };
        assert!(buffering.as_mut().now_or_never().is_none());
        assert!(reads.load(Ordering::SeqCst) <= 128);
        drop(buffering);
        drop(response);
        assert_eq!(Arc::strong_count(&reads), 1, "cancelled source is released");
    }
}

#[tokio::test]
async fn buffering_checks_each_new_value_once() {
    let mut response = Response::stream(stream::iter((0..1024_u32).map(Ok)));
    let mut checked = 0;
    response
        .buffer_until(|_: &u32| {
            checked += 1;
            false
        })
        .await
        .unwrap();
    assert_eq!(checked, 1024, "buffering must not rescan earlier values");
    assert_eq!(response.buffered_end(), Some(&Ok(())));
    assert_eq!(
        response.outputs::<u32>().collect::<Vec<_>>().await,
        (0..1024).map(Ok).collect::<Vec<_>>()
    );
}
