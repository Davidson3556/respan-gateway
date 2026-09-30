//! Generic read-ahead preserves replay and lets each caller choose its predicate.
#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use respan_gateway_core::futures::{StreamExt, stream};
use respan_gateway_core::{Fault, Response};

#[tokio::test]
async fn different_predicates_recheck_the_buffer_and_replay_every_event() {
    let reads = Arc::new(AtomicUsize::new(0));
    let response = Response::stream(stream::iter(0..5u32).map({
        let reads = reads.clone();
        move |value| {
            reads.fetch_add(1, Ordering::SeqCst);
            Ok(value)
        }
    }));
    let response = response
        .buffer_until(|value: &u32| *value == 1)
        .await
        .unwrap();
    assert_eq!(reads.load(Ordering::SeqCst), 2);
    let response = response
        .buffer_until(|value: &u32| *value == 0)
        .await
        .unwrap();
    assert_eq!(reads.load(Ordering::SeqCst), 2);
    let response = response
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
async fn the_same_event_can_commit_one_policy_and_fail_another() {
    let failure = Fault::upstream("cut");
    let make = || Response::stream(stream::iter([Ok(0u32), Err(failure.clone())]));
    let response = make().buffer_until(|_: &u32| true).await.unwrap();
    assert_eq!(
        response.outputs::<u32>().collect::<Vec<_>>().await,
        vec![Ok(0), Err(failure.clone())]
    );
    assert_eq!(
        make()
            .buffer_until(|value: &u32| *value > 0)
            .await
            .unwrap_err(),
        failure
    );
}

#[tokio::test]
async fn a_clean_end_without_a_match_preserves_all_events() {
    let response = Response::stream(stream::iter([Ok(0u32), Ok(1)]))
        .buffer_until(|_: &u32| false)
        .await
        .unwrap();
    assert_eq!(
        response.outputs::<u32>().collect::<Vec<_>>().await,
        [Ok(0), Ok(1)]
    );
}

#[tokio::test]
async fn a_wrong_event_type_is_rejected_without_polling_the_source() {
    let response = Response::stream(stream::poll_fn(|_| {
        panic!("a type mismatch must not read events");
        #[allow(unreachable_code)]
        std::task::Poll::Ready(Some(Ok::<u32, Fault>(0)))
    }));
    assert!(response.buffer_until(|_: &String| true).await.is_err());
}
