//! Each test here pins a finding of FINDINGS.md. An `#[ignore]`d one
//! asserts what a correct budget needs and fails on today's core; the
//! others pass and show the ambiguity or clumsiness as it is.

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::*;
use respan_gateway_core::http::StatusCode;
use respan_gateway_core::{Fault, Outcome, Plugin, ProviderContext, async_trait, codes};
use respan_gateway_formats::conversation::set_model;
use respan_gateway_testkit::mock;
use respan_money::Basis;

/// Whether `edit_call` ran for a call: it was handed to the transport.
#[derive(Clone, Default)]
struct Handed(Arc<AtomicBool>);

/// Records each call's provider-edge outcome by target, and whether it was
/// handed to the transport.
#[derive(Clone, Default)]
struct Ends(Arc<Mutex<Vec<(String, Outcome, bool)>>>);

impl Ends {
    fn of(&self, target: &str) -> Vec<(Outcome, bool)> {
        let all = self.0.lock().map(|a| a.clone()).unwrap_or_default();
        all.into_iter()
            .filter(|(t, ..)| t == target)
            .map(|(_, o, h)| (o, h))
            .collect()
    }
}

#[async_trait]
impl Plugin for Ends {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        let handed = Handed::default();
        px.extensions_mut().insert(handed.clone());
        px.edit_call(move |_| {
            handed.0.store(true, Ordering::Relaxed);
            Ok(())
        });
        Ok(())
    }

    async fn on_provider_end(&self, px: &ProviderContext, outcome: &Outcome) {
        let handed = px.extensions().get::<Handed>();
        let handed = handed.is_some_and(|h| h.0.load(Ordering::Relaxed));
        let row = (px.provider().to_owned(), outcome.clone(), handed);
        self.0.lock().map(|mut a| a.push(row)).ok();
    }
}

/// F1: a call's end hooks are detached, so a retry's `on_provider` runs
/// before the failed attempt's `on_provider_end` released its hold.
#[tokio::test]
async fn f1_a_retry_waits_for_the_failed_attempts_settlement() {
    let bank = bank(50_000);
    let money = money(&bank);
    let fallback = DroppingFallback(vec!["openai", "backup"]);
    let h = H::new(
        bank.clone(),
        vec![Arc::new(money), Arc::new(fallback)],
        None,
    );
    h.mock
        .reply(OPENAI, mock::json(StatusCode::SERVICE_UNAVAILABLE, "{}"))
        .reply(BACKUP, mock::json(StatusCode::OK, UNARY));

    // Fixed by D1: the retry's `on_provider` waits for the failed
    // attempt's `on_provider_end`.
    assert_eq!(h.send(chat(UNARY_REQ)).await.0, StatusCode::OK);
}

/// F1b: the fallback of docs/ARCHITECTURE.md keeps each failed response
/// alive until the next attempt returns (`last = cx.call(..).await` drops
/// the old value after the new one is made), so the failed call has not
/// even ended when the retry is priced.
#[tokio::test]
async fn f1b_the_documented_fallback_releases_a_failed_attempt_before_retrying() {
    let bank = bank(50_000);
    let fallback = Fallback(vec!["openai", "backup"]);
    let h = H::new(
        bank.clone(),
        vec![Arc::new(money(&bank)), Arc::new(fallback)],
        None,
    );
    h.mock
        .reply(OPENAI, mock::json(StatusCode::SERVICE_UNAVAILABLE, "{}"))
        .reply(BACKUP, mock::json(StatusCode::OK, UNARY));

    // Fixed: the documented idiom drops each failed response before the
    // next attempt, so D1 waits for its settlement.
    assert_eq!(h.send(chat(UNARY_REQ)).await.0, StatusCode::OK);
}

/// F2: a hedge loser the provider received and one never sent end alike in
/// their `Outcome`. A `edit_call` used as a tap says the call was handed to
/// the transport (the fix F2 asked for), but `cold` is handed over too and
/// dropped while connecting: only the transport knows whether the provider
/// got it.
#[tokio::test]
async fn f2_a_sent_loser_and_an_unsent_one_end_alike() {
    let ends = Ends::default();
    let bank = bank(10_000_000);
    let hedges = [("slow", SLOW), ("cold", COLD)];
    for (loser, base) in hedges {
        let hedge = Hedge {
            first: loser,
            second: "openai",
            delay: Duration::from_millis(20),
        };
        let h = H::new(
            bank.clone(),
            vec![Arc::new(ends.clone()), Arc::new(hedge)],
            None,
        );
        h.mock
            .reply(base, mock::json(StatusCode::OK, UNARY))
            .reply(OPENAI, mock::json(StatusCode::OK, UNARY));
        assert_eq!(h.send(chat(UNARY_REQ)).await.0, StatusCode::OK);
        let received = h
            .mock
            .sent()
            .iter()
            .any(|r| r.uri().to_string().starts_with(base));
        assert_eq!(received, loser == "slow", "{loser}");
    }

    let ((sent, handed), (unsent, not_handed)) = (&ends.of("slow")[0], &ends.of("cold")[0]);
    // The Outcomes are the same facts for both...
    assert!(sent.head.is_none() && unsent.head.is_none());
    assert_eq!(sent.usage, unsent.usage);
    let code = |o: &Outcome| o.end.as_ref().err().map(|f| f.code().to_owned());
    assert_eq!(code(sent), code(unsent));
    assert_eq!(code(sent).as_deref(), Some(codes::ABANDONED));
    // ...and the tap says both reached the transport.
    assert!(*handed && *not_handed);
}

/// F3: once a request made any call, usage a plugin answered with cannot
/// be told from a committed call's.
#[tokio::test]
async fn f3_a_cached_answer_after_a_judge_is_charged() {
    let bank = bank(10_000_000);
    let money = money(&bank).bill_answers(true);
    let h = H::new(
        bank.clone(),
        vec![Arc::new(money), Arc::new(JudgedCache)],
        None,
    );
    let verdict = r#"{"id":"j","choices":[{"message":{"content":"yes"}}],"usage":{"prompt_tokens":50,"completion_tokens":1}}"#;
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, verdict));

    assert_eq!(h.send(chat(UNARY_REQ)).await.0, StatusCode::OK);

    // Fixed by usage that never overlaps: the client edge sees only the
    // answer's usage, the judge's counts on its provider edge.
    assert_eq!(
        h.bases(),
        [
            (Basis::Reported, 50 + 2),
            (Basis::Answer, 100 * 10 + 10 * 30)
        ]
    );
}

/// F4: one order for both edges. Registered once and first, as its client
/// edge needs, the budget prices a call before later plugins change it.
#[tokio::test]
#[ignore = "F4: one plugin order for both edges (required plugins are promised)"]
async fn f4_a_budget_registered_once_prices_the_model_that_is_called() {
    let bank = bank(100_000);
    let upgrade = OnProvider(|px| px.filter(set_model("gpt-big")));
    let h = H::new(
        bank.clone(),
        vec![Arc::new(money(&bank)), Arc::new(upgrade)],
        Some("openai"),
    );
    let big = r#"{"id":"c","choices":[{"message":{"content":"long"}}],"usage":{"prompt_tokens":4,"completion_tokens":1000}}"#;
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, big));

    // Today: held at gpt-x (30,050), sent, and settled at gpt-big for
    // 300,400: the $0.10 wallet is overdrawn by $0.20.
    assert_eq!(
        h.send(chat(UNARY_REQ)).await.0,
        StatusCode::PAYMENT_REQUIRED
    );
    assert!(bank.available(KEY) >= 0);
}

/// F5: a hold lives in the call's extensions, a type map any later plugin
/// can clear; the hold is then never settled.
#[tokio::test]
#[ignore = "F5: per-call state is the shared extensions map (rejected in the plan)"]
async fn f5_another_plugin_cannot_lose_a_hold() {
    let bank = bank(10_000_000);
    let tidy = OnProvider(|px| {
        px.extensions_mut().clear();
        Ok(())
    });
    let h = H::new(
        bank.clone(),
        vec![Arc::new(money(&bank)), Arc::new(tidy)],
        Some("openai"),
    );
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));

    assert_eq!(h.send(chat(UNARY_REQ)).await.0, StatusCode::OK);

    // Today: the hold of 30,050 stays open forever and nothing is charged.
    assert_eq!(bank.open_holds(), 0);
    assert_eq!(bank.spent(KEY), UNARY_COST);
}

/// F8: a hedge that drops a call cancels the plugins' `on_provider` at an
/// await, yet their `on_provider_end` still runs, with whatever the
/// cancelled hook had done.
#[tokio::test]
async fn f8_a_cancelled_on_provider_still_gets_its_end_hook() {
    #[derive(Clone, Default)]
    struct SlowReserve(Arc<Mutex<Vec<String>>>);

    impl SlowReserve {
        fn log(&self, s: String) {
            self.0.lock().map(|mut l| l.push(s)).ok();
        }
    }

    #[async_trait]
    impl Plugin for SlowReserve {
        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            let name = px.provider().to_owned();
            self.log(format!("start {name}"));
            if name == "backup" {
                // A remote store's reserve: committed there, not yet here.
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            self.log(format!("reserved {name}"));
            Ok(())
        }

        async fn on_provider_end(&self, px: &ProviderContext, _o: &Outcome) {
            self.log(format!("end {}", px.provider()));
        }
    }

    let slow = SlowReserve::default();
    let hedge = Hedge {
        first: "backup",
        second: "openai",
        delay: Duration::from_millis(20),
    };
    let h = H::new(bank(1), vec![Arc::new(slow.clone()), Arc::new(hedge)], None);
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));

    assert_eq!(h.send(chat(UNARY_REQ)).await.0, StatusCode::OK);

    let mut log = slow.0.lock().map(|l| l.clone()).unwrap_or_default();
    log.sort();
    assert_eq!(
        log,
        [
            "end backup",
            "end openai",
            "reserved openai",
            "start backup",
            "start openai"
        ]
    );
}

/// F6: the provider edge sees the provider's head, such as its request id
/// to reconcile an invoice.
#[tokio::test]
async fn f6_the_provider_edge_sees_the_providers_head() {
    let ends = Ends::default();
    let h = H::new(bank(1), vec![Arc::new(ends.clone())], Some("openai"));
    let mut reply = mock::json(StatusCode::OK, UNARY);
    reply.headers_mut().insert(
        "x-request-id",
        "req_42".parse().unwrap_or_else(|e| panic!("{e}")),
    );
    h.mock.reply(OPENAI, reply);

    assert_eq!(h.send(chat(UNARY_REQ)).await.0, StatusCode::OK);

    let (outcome, _) = &ends.of("openai")[0];
    let id = outcome
        .head
        .as_ref()
        .and_then(|h| h.headers.get("x-request-id"));
    assert_eq!(id.and_then(|v| v.to_str().ok()), Some("req_42"));
}

/// N1 (moot: `Outcome.sent` is deleted): a call whose response the kernel
/// refused in its place (a redirect it cannot translate) ends as that
/// refusal on its provider edge, and the budget releases it.
#[tokio::test]
async fn n1_a_response_refused_in_its_place_ends_refused() {
    let ends = Ends::default();
    let bank = bank(10_000_000);
    let h = H::new(
        bank.clone(),
        vec![Arc::new(ends.clone()), Arc::new(money(&bank))],
        Some("claude"),
    );
    let mut reply = mock::json(StatusCode::TEMPORARY_REDIRECT, "{}");
    reply.headers_mut().insert(
        "location",
        "http://elsewhere.test/"
            .parse()
            .unwrap_or_else(|e| panic!("{e}")),
    );
    h.mock.reply(CLAUDE, reply);

    let (status, _) = h.send(chat(UNARY_REQ)).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);

    let (outcome, _) = &ends.of("claude")[0];
    let code = outcome.end.as_ref().err().map(|f| f.code().to_owned());
    assert_eq!(code.as_deref(), Some(codes::UPSTREAM), "{outcome:?}");
    assert_eq!(h.bases(), [(Basis::Released, 0)]);
}

/// F7: a check on the call as sent, such as pricing its final bytes, can
/// refuse it from `edit_call`; nothing reaches the provider, and the hold
/// taken on the provider edge is released.
#[tokio::test]
async fn f7_a_check_on_the_final_bytes_refuses_the_call() {
    let bank = bank(10_000_000);
    let too_long = OnProvider(|px| {
        px.edit_call(|call| match call.body().to_bytes().len() > 10 {
            true => Err(Fault::new(
                StatusCode::PAYMENT_REQUIRED,
                "too_long",
                "priced out",
            )),
            false => Ok(()),
        });
        Ok(())
    });
    let h = H::new(
        bank.clone(),
        vec![Arc::new(money(&bank)), Arc::new(too_long)],
        Some("openai"),
    );

    assert_eq!(
        h.send(chat(UNARY_REQ)).await.0,
        StatusCode::PAYMENT_REQUIRED
    );
    assert!(h.mock.sent().is_empty());
    assert_eq!(h.bases(), [(Basis::Released, 0)]);
}

/// M-5: `with_usage` now says usage read through `outputs()` counts on the
/// client edge of the request the response came from, "and so nowhere for
/// a `Response::stream`". The promise is narrowed, not kept: a cache that
/// reads its own answer before sending it must give the usage again.
struct RereadCache {
    again: bool,
}

#[async_trait]
impl Plugin for RereadCache {
    async fn on_client(
        &self,
        _cx: &mut respan_gateway_core::ClientContext,
    ) -> Result<Option<respan_gateway_core::Response>, Fault> {
        use respan_gateway_core::futures::TryStreamExt;
        let read: Vec<respan_gateway_formats::conversation::Reply> =
            cached().outputs().try_collect().await?;
        let again = respan_gateway_core::futures::stream::iter(read.into_iter().map(Ok));
        let resp = respan_gateway_core::Response::stream(again);
        Ok(Some(match self.again {
            true => resp.with_usage(cached_usage()),
            false => resp,
        }))
    }
}

#[tokio::test]
async fn m5_a_cached_answer_read_before_it_is_sent_bills_only_the_usage_given_again() {
    for (again, billed) in [(false, vec![]), (true, vec![(Basis::Answer, 1300)])] {
        let bank = bank(10_000_000);
        let money = money(&bank).bill_answers(true);
        let h = H::new(
            bank.clone(),
            vec![Arc::new(money), Arc::new(RereadCache { again })],
            None,
        );

        assert_eq!(h.send(chat(UNARY_REQ)).await.0, StatusCode::OK);

        assert_eq!(h.bases(), billed);
    }
}
