//! Probes of the round-10 core: a filter's panic is its `Err` (T10-5), and
//! the client edge counts usage that follows a failure in the same batch
//! (T10-3); but core's closure filter loses a usage increment on a panic
//! (N10-1).

mod common;

const PLENTY: i64 = 10_000_000;

use std::sync::Arc;

use common::*;
use respan_gateway_core::bytes::Bytes;
use respan_gateway_core::http::{self, StatusCode};
use respan_gateway_core::{
    Body, ClientContext, Doc, Fault, Filter, Plugin, ProviderContext, Response, Usage, async_trait,
};
use respan_gateway_formats::conversation::{Reply, units};
use respan_gateway_testkit::{Calls, mock};
use respan_money::Basis;

/// Fails the response's text containing "world", on the client edge, with
/// an `Err`: `PanicFilter`'s twin.
struct ErrFilter;

#[async_trait]
impl Plugin for ErrFilter {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.filter(|e: &mut Reply| match e {
            Reply::Text { text, .. } if text.contains("world") => {
                Err(Fault::internal("a plugin or format panicked"))
            }
            _ => Ok(()),
        })?;
        Ok(None)
    }
}

async fn failing_filter(filter: Arc<dyn Plugin>) -> ((StatusCode, String), Vec<(Basis, i64)>) {
    let bank = bank(PLENTY);
    let h = H::new(
        bank.clone(),
        vec![Arc::new(money(&bank)), filter],
        Some("openai"),
    );
    h.mock.reply(OPENAI, mock::sse(STREAM));
    let sent = h.send(chat(STREAM_REQ)).await;
    (sent, h.bases())
}

/// T10-5: a panicking client-edge filter and one returning the same fault
/// give the client the same bytes and the budget the same charge.
#[tokio::test]
async fn a_filter_that_panics_ends_the_stream_as_its_err_would() {
    let panicked = failing_filter(Arc::new(PanicFilter)).await;
    let failed = failing_filter(Arc::new(ErrFilter)).await;
    assert_eq!(panicked, failed);
    // "Hello" passed before the failure in the same batch, so the head
    // went out with it and the failure is written in band.
    assert_eq!(panicked.0.0, StatusCode::OK);
    assert!(panicked.0.1.contains("Hello") && panicked.0.1.contains("internal"));
}

/// A cache that relays stored SSE bytes: its usage comes from no call, so
/// it counts on the client edge. " world" and the usage share one frame.
struct RelayedCache;

const ONE_FRAME: &str = concat!(
    "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello\"}}]}\n\n",
    "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"}}]}\n\n",
    "data: {\"id\":\"c1\",\"choices\":[],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2}}\n\n",
    "data: [DONE]\n\n",
);

#[async_trait]
impl Plugin for RelayedCache {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let resp = http::Response::builder()
            .header("content-type", "text/event-stream")
            .body(Body::from(Doc::from(Bytes::from_static(
                ONE_FRAME.as_bytes(),
            ))))?;
        Ok(Some(cx.relay(resp)))
    }
}

/// T10-3: an output filter fails on " world"; the usage after it in the
/// same batch still counts on the client edge, so the answer is charged in
/// full, and the usage is not written.
#[tokio::test]
async fn usage_after_a_failure_in_a_relayed_answer_is_charged() {
    let bank = bank(PLENTY);
    let h = H::new(
        bank.clone(),
        vec![
            Arc::new(money(&bank).bill_answers(true)),
            Arc::new(RelayedCache),
            Arc::new(ErrFilter),
        ],
        None,
    );
    let (status, body) = h.send(chat(STREAM_REQ)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(!body.contains("prompt_tokens"), "{body}");
    assert_eq!(h.bases(), [(Basis::Answer, STREAM_COST)]);
}

/// Passes each usage increment on, then fails: with a panic if `panics`,
/// else with an `Err`.
struct PushThenFail(bool);

impl Filter<Usage> for PushThenFail {
    fn edit(&mut self, value: Usage, out: &mut Vec<Usage>) -> Result<(), Fault> {
        out.push(value);
        match self.0 {
            true => panic!("a buggy usage filter"),
            false => Err(Fault::internal("usage filter failed")),
        }
    }
}

struct UsageFails(bool);

#[async_trait]
impl Plugin for UsageFails {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        px.filter(PushThenFail(self.0))
    }
}

async fn usage_filter(panics: bool) -> (StatusCode, Option<f64>, Vec<(Basis, i64)>) {
    let bank = bank(PLENTY);
    let calls = Calls::default();
    let h = H::new(
        bank.clone(),
        vec![
            Arc::new(UsageFails(panics)),
            Arc::new(money(&bank)),
            Arc::new(calls.clone()),
        ],
        Some("openai"),
    );
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));
    let (status, _) = h.send(chat(UNARY_REQ)).await;
    let input = calls.usage().get(units::INPUT_TOKENS);
    (status, input, h.bases())
}

/// A usage filter that fails after pushing the increment on counts it
/// (K9-1) whether it fails with `Err` or a panic (T10-5).
#[tokio::test]
async fn usage_a_filter_pushed_counts_whether_it_fails_or_panics() {
    let (status, input, bases) = usage_filter(false).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(input, Some(4.0));
    assert!(bases[0].1 >= UNARY_COST, "{bases:?}");
    assert_eq!(usage_filter(true).await, (status, input, bases));
}

/// The closure form of a usage filter, failing each increment with `Err`
/// or with a panic.
struct ClosureFails(bool);

#[async_trait]
impl Plugin for ClosureFails {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        match self.0 {
            true => {
                px.filter(|_: &mut Usage| -> Result<(), Fault> { panic!("a buggy usage filter") })
            }
            false => px.filter(|_: &mut Usage| Err(Fault::internal("usage filter failed"))),
        }
    }
}

async fn closure_filter(panics: bool) -> (StatusCode, Option<f64>, Vec<(Basis, i64)>) {
    let bank = bank(PLENTY);
    let calls = Calls::default();
    let h = H::new(
        bank.clone(),
        vec![
            Arc::new(ClosureFails(panics)),
            Arc::new(money(&bank)),
            Arc::new(calls.clone()),
        ],
        Some("openai"),
    );
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));
    let (status, _) = h.send(chat(UNARY_REQ)).await;
    (status, calls.usage().get(units::INPUT_TOKENS), h.bases())
}

/// N10-1: core's closure `Filter` pushes the value after the closure
/// returns, so an `Err` passes the increment on and it counts, while a
/// panic passes nothing: the call's usage is lost and the budget charges
/// its estimate (50) where the `Err` twin charges the reported usage (80).
/// "A panic in either method is an internal fault, as its `Err`" does not
/// hold for the one `Filter` impl core provides.
#[tokio::test]
#[ignore = "N10-1: core's closure Filter drops the value on a panic, so its usage is lost"]
async fn a_closure_usage_filter_that_panics_counts_as_one_that_fails() {
    assert_eq!(closure_filter(true).await, closure_filter(false).await);
}

/// Pins N10-1 as it is today.
#[tokio::test]
async fn today_a_closure_usage_filter_that_panics_loses_the_usage() {
    let (status, input, bases) = closure_filter(false).await;
    assert_eq!(
        (status, input),
        (StatusCode::INTERNAL_SERVER_ERROR, Some(4.0))
    );
    assert!(bases[0].1 >= UNARY_COST, "{bases:?}");
    let (status, input, bases) = closure_filter(true).await;
    assert_eq!((status, input), (StatusCode::INTERNAL_SERVER_ERROR, None));
    assert_eq!(bases, [(Basis::Estimated, 50)]);
}
