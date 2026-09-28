//! Probes of the round-9 core: usage a failing filter pushes counts once and
//! is never written (K9-1, K9-7), and a filter's end cannot rewrite how the
//! call ended (K9-2).

mod common;

use std::sync::Arc;

use common::*;
use respan_gateway_core::http::StatusCode;
use respan_gateway_core::{Fault, Filter, Plugin, ProviderContext, Usage, async_trait};
use respan_gateway_formats::conversation::{Reply, units};
use respan_gateway_testkit::{Calls, mock};
use respan_money::Basis;

/// A usage filter that passes every increment on, then fails.
fn failing_usage(px: &mut ProviderContext) -> Result<(), Fault> {
    px.filter(|_: &mut Usage| Err(Fault::internal("usage filter failed")))
}

/// Keeps usage back, releases it at the end, then fails there.
#[derive(Default)]
struct KeepBackThenFail(Usage);

impl Filter<Usage> for KeepBackThenFail {
    fn edit(&mut self, value: Usage, _out: &mut Vec<Usage>) -> Result<(), Fault> {
        self.0 += value;
        Ok(())
    }

    fn end(&mut self, _: &Result<(), Fault>, out: &mut Vec<Usage>) -> Result<(), Fault> {
        out.push(std::mem::take(&mut self.0));
        Err(Fault::internal("usage end failed"))
    }
}

struct KeepBack;

#[async_trait]
impl Plugin for KeepBack {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        px.filter(KeepBackThenFail::default())
    }
}

/// An output filter that fails at the end, whatever the end.
struct FailAtEnd;

impl Filter<Reply> for FailAtEnd {
    fn edit(&mut self, value: Reply, out: &mut Vec<Reply>) -> Result<(), Fault> {
        out.push(value);
        Ok(())
    }

    fn end(&mut self, _: &Result<(), Fault>, _: &mut Vec<Reply>) -> Result<(), Fault> {
        Err(Fault::internal("output end failed"))
    }
}

struct OutputEndFails;

#[async_trait]
impl Plugin for OutputEndFails {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        px.filter(FailAtEnd)
    }
}

fn with_calls(bank: &respan_money::Bank, first: Arc<dyn Plugin>) -> (H, Calls) {
    let calls = Calls::default();
    let h = H::new(
        bank.clone(),
        vec![first, Arc::new(money(bank)), Arc::new(calls.clone())],
        Some("openai"),
    );
    (h, calls)
}

/// K9-1: a usage filter that fails after passing its increment on. The
/// usage counts once on the provider edge, and the budget charges it once.
#[tokio::test]
async fn usage_a_failing_filter_passed_on_is_charged_once() {
    let bank = bank(10_000_000);
    let (h, calls) = with_calls(&bank, Arc::new(OnProvider(failing_usage)));
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));

    let (status, body) = h.send(chat(UNARY_REQ)).await;

    // The failure is the response's: nothing of the usage filter's is written.
    assert_ne!(status, StatusCode::OK, "{body}");
    assert_eq!(calls.usage().get(units::INPUT_TOKENS), Some(4.0));
    assert_eq!(calls.usage().get(units::OUTPUT_TOKENS), Some(1.0));
    let bases = h.bases();
    assert_eq!(bases.len(), 1);
    assert!(matches!(bases[0].0, Basis::Reported | Basis::ToppedUp));
    assert!(
        bases[0].1 >= UNARY_COST && bases[0].1 < 2 * UNARY_COST,
        "{bases:?}"
    );
}

/// K9-7: usage a filter keeps back and releases at a failing end counts
/// once, unwritten.
#[tokio::test]
async fn usage_released_by_a_failing_end_is_charged_once() {
    let bank = bank(10_000_000);
    let (h, calls) = with_calls(&bank, Arc::new(KeepBack));
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));

    let (status, body) = h.send(chat(UNARY_REQ)).await;

    assert_ne!(status, StatusCode::OK, "{body}");
    assert_eq!(calls.usage().get(units::INPUT_TOKENS), Some(4.0));
    let bases = h.bases();
    assert_eq!(bases.len(), 1);
    assert!(
        bases[0].1 >= UNARY_COST && bases[0].1 < 2 * UNARY_COST,
        "{bases:?}"
    );
}

/// K9-2 / O8-1: the provider answered in full, then an output filter's end
/// fails. What does the call's Outcome say, and what is charged?
#[tokio::test]
async fn an_output_filter_failing_at_the_end_after_a_full_answer() {
    let bank = bank(10_000_000);
    let (h, calls) = with_calls(&bank, Arc::new(OutputEndFails));
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));

    let (status, _) = h.send(chat(UNARY_REQ)).await;

    let call = &calls.all()[0];
    // The client is refused: the filter's end failed before any byte.
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    // Pinned: the call's Outcome says it failed, with the filter's fault,
    // though the provider's answer was read to its end.
    let end = call.end.as_ref().map_err(|f| f.message().to_owned());
    assert_eq!(end, Err("output end failed".to_owned()));
    // The budget sees a partial end and may top up, never under-charge.
    let bases = h.bases();
    assert!(bases[0].1 >= UNARY_COST, "{bases:?}");
}
