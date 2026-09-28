//! The budget SDK against every hard case it must survive.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use respan_gateway_core::futures;
use respan_gateway_core::http::StatusCode;
use respan_gateway_core::http_body_util::BodyExt;
use respan_gateway_core::serde_json::{self, Value};
use respan_gateway_core::{Plugin, codes};
use respan_gateway_formats::conversation::set_model;
use respan_gateway_testkit::mock;
use respan_money::{Abandoned, Basis, Money};

const PLENTY: i64 = 10_000_000;

fn plugins(list: Vec<Arc<dyn Plugin>>) -> Vec<Arc<dyn Plugin>> {
    list
}

// Plain calls.

#[tokio::test]
async fn a_call_is_charged_its_reported_usage_and_its_hold_released() {
    let bank = bank(PLENTY);
    let h = H::new(
        bank.clone(),
        plugins(vec![Arc::new(money(&bank))]),
        Some("openai"),
    );
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));

    assert_eq!(h.send(chat(UNARY_REQ)).await.0, StatusCode::OK);

    assert_eq!(h.bases(), [(Basis::Reported, UNARY_COST)]);
    for ledger in [KEY, "t1", "acme"] {
        assert_eq!(bank.spent(ledger), UNARY_COST, "{ledger}");
        assert_eq!(bank.available(ledger), PLENTY - UNARY_COST, "{ledger}");
    }
}

#[tokio::test]
async fn a_stream_is_charged_from_its_usage_frame() {
    let bank = bank(PLENTY);
    let h = H::new(
        bank.clone(),
        plugins(vec![Arc::new(money(&bank))]),
        Some("openai"),
    );
    h.mock.reply(OPENAI, mock::sse(STREAM));

    let (status, body) = h.send(chat(STREAM_REQ)).await;

    assert_eq!(status, StatusCode::OK);
    assert!(
        body.contains("Hel") && body.ends_with("data: [DONE]\n\n"),
        "{body}"
    );
    assert_eq!(h.bases(), [(Basis::Reported, STREAM_COST)]);
}

#[tokio::test]
async fn a_request_with_no_bound_on_output_is_capped() {
    let bank = bank(PLENTY);
    let h = H::new(
        bank.clone(),
        plugins(vec![Arc::new(money(&bank))]),
        Some("openai"),
    );
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));

    h.send(chat(UNARY_REQ)).await;

    let sent: Value = serde_json::from_slice(h.mock.sent()[0].body()).unwrap_or_default();
    assert_eq!(sent["max_completion_tokens"], CAP);
    assert_eq!(
        h.settled()[0].held,
        input_of(&h.settled()[0]) * 10 + CAP as i64 * 30
    );
}

#[tokio::test]
async fn a_bound_within_the_cap_is_kept_and_held_as_it_is() {
    let bank = bank(PLENTY);
    let h = H::new(
        bank.clone(),
        plugins(vec![Arc::new(money(&bank))]),
        Some("openai"),
    );
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));
    let req = r#"{"model":"gpt-x","max_tokens":10,"messages":[{"role":"user","content":"hi"}]}"#;

    h.send(chat(req)).await;

    // Unchanged, the client's body goes byte for byte.
    assert_eq!(h.mock.sent()[0].body().as_ref(), req.as_bytes());
    let entry = &h.settled()[0];
    assert_eq!(entry.held % 10, 0);
    assert_eq!(entry.held - 10 * 30, (entry.held - 300) / 10 * 10);
}

#[tokio::test]
async fn usage_the_provider_never_reported_is_estimated() {
    let bank = bank(PLENTY);
    let h = H::new(
        bank.clone(),
        plugins(vec![Arc::new(money(&bank))]),
        Some("openai"),
    );
    h.mock
        .reply(OPENAI, mock::json(StatusCode::OK, UNARY_NO_USAGE));

    assert_eq!(h.send(chat(UNARY_REQ)).await.0, StatusCode::OK);

    let entry = &h.settled()[0];
    // "hello world" is 11 characters: 3 tokens.
    assert_eq!(entry.basis, Basis::Estimated);
    assert_eq!(entry.charged, input_of(entry) * 10 + 3 * 30);
}

#[tokio::test]
async fn a_provider_error_costs_nothing() {
    let bank = bank(PLENTY);
    let h = H::new(
        bank.clone(),
        plugins(vec![Arc::new(money(&bank))]),
        Some("openai"),
    );
    h.mock
        .reply(OPENAI, mock::json(StatusCode::SERVICE_UNAVAILABLE, "{}"));

    assert_eq!(
        h.send(chat(UNARY_REQ)).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );

    assert_eq!(h.bases(), [(Basis::Released, 0)]);
}

#[tokio::test]
async fn an_unreachable_provider_costs_nothing() {
    let bank = bank(PLENTY);
    let h = H::new(
        bank.clone(),
        plugins(vec![Arc::new(money(&bank))]),
        Some("openai"),
    );

    assert_eq!(h.send(chat(UNARY_REQ)).await.0, StatusCode::BAD_GATEWAY);

    assert_eq!(h.bases(), [(Basis::Released, 0)]);
}

#[tokio::test]
async fn a_stream_cut_before_its_usage_is_estimated_not_free() {
    let bank = bank(PLENTY);
    let h = H::new(
        bank.clone(),
        plugins(vec![Arc::new(money(&bank))]),
        Some("openai"),
    );
    h.mock.reply(OPENAI, mock::sse(&STREAM[..2]));

    let (status, body) = h.send(chat(STREAM_REQ)).await;

    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("truncated"), "{body}");
    let entry = &h.settled()[0];
    assert_eq!(entry.basis, Basis::Estimated);
    assert_eq!(entry.charged, input_of(entry) * 10 + 3 * 30);
}

#[tokio::test]
async fn a_partial_report_is_topped_up() {
    let bank = bank(PLENTY);
    let h = H::new(
        bank.clone(),
        plugins(vec![Arc::new(money(&bank))]),
        Some("openai"),
    );
    // Input reported on the first chunk, then cut before the output count.
    let first = "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello world\"}}],\"usage\":{\"prompt_tokens\":7}}\n\n";
    h.mock.reply(OPENAI, mock::sse(&[first]));

    h.send(chat(STREAM_REQ)).await;

    assert_eq!(h.bases(), [(Basis::ToppedUp, 7 * 10 + 3 * 30)]);
}

// Refusals.

#[tokio::test]
async fn an_empty_wallet_is_refused_402_in_the_clients_format_before_any_call() {
    let bank = bank(PLENTY);
    bank.open(KEY, Some("t1"), 0);
    let h = H::new(
        bank.clone(),
        plugins(vec![Arc::new(money(&bank))]),
        Some("openai"),
    );

    let (status, body) = h.send(chat(UNARY_REQ)).await;

    assert_eq!(status, StatusCode::PAYMENT_REQUIRED);
    assert!(body.contains("insufficient_funds"), "{body}");
    assert!(h.mock.sent().is_empty());
    assert!(h.settled().is_empty());
}

#[tokio::test]
async fn a_call_the_wallet_cannot_cover_is_refused_before_it_is_sent() {
    let bank = bank(20_000);
    let h = H::new(
        bank.clone(),
        plugins(vec![Arc::new(money(&bank))]),
        Some("openai"),
    );

    let (status, body) = h.send(chat(UNARY_REQ)).await;

    assert_eq!(status, StatusCode::PAYMENT_REQUIRED);
    assert!(body.contains("available_micros"), "{body}");
    assert!(h.mock.sent().is_empty());
    assert!(h.settled().is_empty());
}

#[tokio::test]
async fn a_team_budget_binds_a_key_with_plenty() {
    let bank = bank(PLENTY);
    bank.open("t1", Some("acme"), 20_000);
    let h = H::new(
        bank.clone(),
        plugins(vec![Arc::new(money(&bank))]),
        Some("openai"),
    );

    let (status, body) = h.send(chat(UNARY_REQ)).await;

    assert_eq!(status, StatusCode::PAYMENT_REQUIRED);
    assert!(body.contains(r#""ledger":"t1""#), "{body}");
}

#[tokio::test]
async fn an_unknown_key_is_refused_401() {
    let bank = bank(PLENTY);
    let h = H::new(
        bank.clone(),
        plugins(vec![Arc::new(money(&bank))]),
        Some("openai"),
    );
    let mut req = chat(UNARY_REQ);
    req.headers_mut().insert(
        "authorization",
        "Bearer sk-nobody".parse().unwrap_or_else(|e| panic!("{e}")),
    );

    assert_eq!(h.send(req).await.0, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_request_refused_before_any_call_touches_no_ledger() {
    let bank = bank(PLENTY);
    let h = H::new(
        bank.clone(),
        plugins(vec![Arc::new(money(&bank)), Arc::new(Guard)]),
        Some("openai"),
    );

    assert_eq!(h.send(chat(UNARY_REQ)).await.0, StatusCode::FORBIDDEN);

    assert!(h.settled().is_empty());
    assert_eq!(bank.available(KEY), PLENTY);
}

#[tokio::test]
async fn a_format_of_no_kind_has_no_cost_bound_and_is_refused() {
    let bank = bank(PLENTY);
    let h = H::new(
        bank.clone(),
        plugins(vec![Arc::new(money(&bank))]),
        Some("opaque"),
    );

    let (status, body) = h.send(opaque()).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("no cost bound"), "{body}");
    assert!(h.mock.sent().is_empty());
}

#[tokio::test]
async fn an_unpriced_model_is_refused() {
    let bank = bank(PLENTY);
    let h = H::new(
        bank.clone(),
        plugins(vec![Arc::new(money(&bank))]),
        Some("openai"),
    );
    let req = r#"{"model":"mystery","messages":[]}"#;

    assert_eq!(h.send(chat(req)).await.0, StatusCode::BAD_REQUEST);
    assert!(h.mock.sent().is_empty());
}

// Money under concurrency.

#[tokio::test]
async fn fifty_parallel_requests_never_overspend_one_dollar() {
    let bank = bank(1_000_000);
    let h = H::new(
        bank.clone(),
        plugins(vec![Arc::new(money(&bank))]),
        Some("openai"),
    );
    for _ in 0..50 {
        h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));
    }

    // Every head first, so every admitted call still holds.
    let resps = futures::future::join_all((0..50).map(|_| h.gw.handle(chat(UNARY_REQ)))).await;
    let admitted = resps
        .iter()
        .filter(|r| r.0.status() == StatusCode::OK)
        .count();
    let refused = resps
        .iter()
        .filter(|r| r.0.status() == StatusCode::PAYMENT_REQUIRED)
        .count();
    assert!(bank.available("acme") >= 0);
    for r in resps {
        let _ = r.0.into_body().collect().await;
    }
    h.gw.idle().await;

    let hold = h.settled()[0].held;
    assert_eq!(admitted as i64, 1_000_000 / hold);
    assert_eq!(admitted + refused, 50);
    assert_eq!(bank.spent("acme"), admitted as i64 * UNARY_COST);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_requests_on_threads_never_overspend() {
    let bank = bank(1_000_000);
    let h = Arc::new(H::new(
        bank.clone(),
        plugins(vec![Arc::new(money(&bank))]),
        Some("openai"),
    ));
    for _ in 0..50 {
        h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));
    }

    let tasks: Vec<_> = (0..50)
        .map(|_| {
            let h = h.clone();
            tokio::spawn(async move { h.gw.handle(chat(UNARY_REQ)).await })
        })
        .collect();
    let mut resps = Vec::new();
    for t in tasks {
        resps.push(t.await.unwrap_or_else(|e| panic!("{e}")));
    }
    assert!(bank.available("acme") >= 0);
    for r in resps {
        let _ = r.0.into_body().collect().await;
    }
    h.gw.idle().await;

    h.settled();
    assert!(bank.spent("acme") <= 1_000_000);
    assert!(bank.available("acme") >= 0);
}

// Many calls per request.

#[tokio::test]
async fn a_fallbacks_failed_attempt_is_released_and_its_retry_charged() {
    let bank = bank(PLENTY);
    let h = H::new(
        bank.clone(),
        plugins(vec![
            Arc::new(money(&bank)),
            Arc::new(Fallback(vec!["openai", "backup"])),
        ]),
        None,
    );
    h.mock
        .reply(OPENAI, mock::json(StatusCode::SERVICE_UNAVAILABLE, "{}"))
        .reply(BACKUP, mock::json(StatusCode::OK, UNARY));

    assert_eq!(h.send(chat(UNARY_REQ)).await.0, StatusCode::OK);

    let mut bases = h.bases();
    bases.sort_by_key(|b| b.1);
    assert_eq!(bases, [(Basis::Released, 0), (Basis::Reported, UNARY_COST)]);
}

#[tokio::test]
async fn a_tight_wallet_still_affords_a_fallbacks_retry() {
    // Room for one hold, not two: the failed attempt's must go first.
    let bank = bank(50_000);
    let fallback = DroppingFallback(vec!["openai", "backup"]);
    let h = H::new(
        bank.clone(),
        plugins(vec![Arc::new(money(&bank)), Arc::new(fallback)]),
        None,
    );
    h.mock
        .reply(OPENAI, mock::json(StatusCode::SERVICE_UNAVAILABLE, "{}"))
        .reply(BACKUP, mock::json(StatusCode::OK, UNARY));

    // The fallback drops the failed attempt first; D1 makes the retry
    // wait for its release.
    assert_eq!(h.send(chat(UNARY_REQ)).await.0, StatusCode::OK);
    assert_eq!(bank.spent(KEY), UNARY_COST);
}

#[tokio::test]
async fn a_smart_routers_judge_and_its_pick_are_each_charged_their_own_usage() {
    let bank = bank(PLENTY);
    let h = H::new(
        bank.clone(),
        plugins(vec![Arc::new(money(&bank)), Arc::new(SmartRouter)]),
        None,
    );
    let verdict = r#"{"id":"j","choices":[{"message":{"content":"backup"}}],"usage":{"prompt_tokens":50,"completion_tokens":1}}"#;
    h.mock
        .reply(OPENAI, mock::json(StatusCode::OK, verdict))
        .reply(BACKUP, mock::json(StatusCode::OK, UNARY));

    assert_eq!(h.send(chat(UNARY_REQ)).await.0, StatusCode::OK);

    // The judge at judge-1 prices (1 in, 2 out), the answer at gpt-x.
    assert_eq!(
        h.bases(),
        [(Basis::Reported, 50 + 2), (Basis::Reported, UNARY_COST)]
    );
    // The judge's hold was priced on its own prompt and model.
    assert!(h.settled()[0].held < h.settled()[1].held);
    // Every call's usage counts on its provider edge only; the client edge
    // saw none of its own.
    assert!(h.log.only().usage.is_empty());
}

#[tokio::test]
async fn a_hedges_winner_is_charged_and_its_loser_settled_once() {
    let bank = bank(PLENTY);
    let hedge = Hedge {
        first: "slow",
        second: "openai",
        delay: Duration::from_millis(20),
    };
    let h = H::new(
        bank.clone(),
        plugins(vec![Arc::new(money(&bank)), Arc::new(hedge)]),
        None,
    );
    h.mock
        .reply(SLOW, mock::json(StatusCode::OK, UNARY))
        .reply(OPENAI, mock::json(StatusCode::OK, UNARY));

    assert_eq!(h.send(chat(UNARY_REQ)).await.0, StatusCode::OK);

    // The loser was sent (the slow provider got it) but gave no head: its
    // `edit_call` tap says it was handed over, so its input is estimated.
    assert_eq!(h.mock.sent().len(), 2);
    let journal = h.settled();
    let loser = journal.iter().find(|e| e.basis == Basis::Estimated);
    assert!(
        loser.is_some_and(|e| e.charged == input_of(e) * 10),
        "{journal:?}"
    );
    assert!(
        journal
            .iter()
            .any(|e| e.basis == Basis::Reported && e.charged == UNARY_COST)
    );
}

#[tokio::test]
async fn a_hedge_loser_whose_head_arrived_is_charged_its_hold_under_the_reserved_policy() {
    let bank = bank(PLENTY);
    let money = money(&bank).abandoned(Abandoned::Reserved);
    let h = H::new(
        bank.clone(),
        plugins(vec![
            Arc::new(money),
            Arc::new(KeepFirst("openai", "backup")),
        ]),
        None,
    );
    h.mock
        .reply(OPENAI, mock::json(StatusCode::OK, UNARY))
        .reply(BACKUP, mock::sse(STREAM));

    assert_eq!(h.send(chat(UNARY_REQ)).await.0, StatusCode::OK);

    let journal = h.settled();
    let loser = journal.iter().find(|e| e.basis == Basis::Reserved);
    assert!(loser.is_some_and(|e| e.charged == e.held), "{journal:?}");
    assert!(
        journal
            .iter()
            .any(|e| e.basis == Basis::Reported && e.charged == UNARY_COST)
    );
}

// Plugins that answer.

#[tokio::test]
async fn usage_a_plugin_answers_with_is_charged_once_on_the_client_edge() {
    let bank = bank(PLENTY);
    let money = money(&bank).bill_answers(true);
    let h = H::new(
        bank.clone(),
        plugins(vec![Arc::new(money), Arc::new(Cache)]),
        Some("openai"),
    );

    let (status, body) = h.send(chat(UNARY_REQ)).await;

    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("from cache"), "{body}");
    assert!(h.mock.sent().is_empty());
    assert_eq!(h.bases(), [(Basis::Answer, 100 * 10 + 10 * 30)]);
}

#[tokio::test]
async fn usage_a_plugin_answers_with_is_not_charged_unless_asked() {
    let bank = bank(PLENTY);
    let h = H::new(
        bank.clone(),
        plugins(vec![Arc::new(money(&bank)), Arc::new(Cache)]),
        Some("openai"),
    );

    assert_eq!(h.send(chat(UNARY_REQ)).await.0, StatusCode::OK);

    assert!(h.settled().is_empty());
}

#[tokio::test]
async fn usage_a_plugin_adds_to_a_calls_response_is_told_from_the_calls() {
    struct Surcharge;

    #[respan_gateway_core::async_trait]
    impl Plugin for Surcharge {
        async fn on_client(
            &self,
            cx: &mut respan_gateway_core::ClientContext,
        ) -> Result<Option<respan_gateway_core::Response>, respan_gateway_core::Fault> {
            let mut extra = respan_gateway_core::Usage::default();
            extra.add("output_tokens", 10.0);
            Ok(Some(cx.call("openai").await?.with_usage(extra)))
        }
    }

    let bank = bank(PLENTY);
    let money = money(&bank).bill_answers(true);
    let h = H::new(
        bank.clone(),
        plugins(vec![Arc::new(money), Arc::new(Surcharge)]),
        None,
    );
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));

    assert_eq!(h.send(chat(UNARY_REQ)).await.0, StatusCode::OK);

    // The call on the provider edge; the plugin's 10 on the client edge,
    // the request's usage less the sent call's.
    assert_eq!(
        h.bases(),
        [(Basis::Reported, UNARY_COST), (Basis::Answer, 10 * 30)]
    );
}

// Clients that leave, and plugins that panic.

#[tokio::test]
async fn a_client_that_leaves_mid_stream_is_charged_for_what_it_was_sent() {
    let bank = bank(PLENTY);
    let h = H::new(
        bank.clone(),
        plugins(vec![Arc::new(money(&bank))]),
        Some("openai"),
    );
    h.mock.reply(OPENAI, mock::sse(STREAM));

    let (_, mut body) = h.gw.handle(chat(STREAM_REQ)).await.0.into_parts();
    let _first = body.frame().await;
    drop(body);
    h.gw.idle().await;

    let entry = &h.settled()[0];
    // Both deltas were decoded before the first bytes went out.
    assert_eq!(entry.basis, Basis::Estimated);
    assert_eq!(entry.charged, input_of(entry) * 10 + 3 * 30);
    assert!(matches!(h.log.only().end, Err(f) if f.code() == codes::ABANDONED));
}

#[tokio::test]
async fn a_panic_on_the_provider_edge_after_the_hold_releases_it() {
    let bank = bank(PLENTY);
    let panics = OnProvider(|_| panic!("a buggy plugin"));
    let h = H::new(
        bank.clone(),
        plugins(vec![Arc::new(money(&bank)), Arc::new(panics)]),
        Some("openai"),
    );

    assert_eq!(
        h.send(chat(UNARY_REQ)).await.0,
        StatusCode::INTERNAL_SERVER_ERROR
    );

    assert!(h.mock.sent().is_empty());
    assert_eq!(h.bases(), [(Basis::Released, 0)]);
}

#[tokio::test]
async fn a_refusal_on_the_provider_edge_after_the_hold_releases_it() {
    let bank = bank(PLENTY);
    let deny = OnProvider(|_| {
        Err(respan_gateway_core::Fault::new(
            StatusCode::TOO_MANY_REQUESTS,
            "limited",
            "slow down",
        ))
    });
    let h = H::new(
        bank.clone(),
        plugins(vec![Arc::new(money(&bank)), Arc::new(deny)]),
        Some("openai"),
    );

    assert_eq!(
        h.send(chat(UNARY_REQ)).await.0,
        StatusCode::TOO_MANY_REQUESTS
    );

    assert_eq!(h.bases(), [(Basis::Released, 0)]);
}

#[tokio::test]
async fn a_client_filter_that_panics_mid_stream_leaves_the_call_estimated() {
    let bank = bank(PLENTY);
    let h = H::new(
        bank.clone(),
        plugins(vec![Arc::new(money(&bank)), Arc::new(PanicFilter)]),
        Some("openai"),
    );
    h.mock.reply(OPENAI, mock::sse(STREAM));

    // A panic is the filter's `Err` (T10-5): "Hello" passed before it in
    // the same batch, so the head goes out with it and the failure follows
    // in band.
    let (status, body) = h.send(chat(STREAM_REQ)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.contains("Hello") && body.contains("internal"),
        "{body}"
    );

    // The provider edge's filter counted the deltas before the client
    // edge's panicked.
    let entry = &h.settled()[0];
    assert_eq!(entry.basis, Basis::Estimated);
    assert_eq!(entry.charged, input_of(entry) * 10 + 3 * 30);
}

#[tokio::test]
async fn a_plugin_that_panics_holding_a_response_leaves_its_call_charged() {
    let bank = bank(PLENTY);
    let h = H::new(
        bank.clone(),
        plugins(vec![Arc::new(money(&bank)), Arc::new(PanicHolding)]),
        None,
    );
    h.mock.reply(OPENAI, mock::sse(STREAM));

    assert_eq!(
        h.send(chat(STREAM_REQ)).await.0,
        StatusCode::INTERNAL_SERVER_ERROR
    );

    let entry = &h.settled()[0];
    assert_eq!(entry.basis, Basis::Estimated);
    assert_eq!(entry.charged, input_of(entry) * 10);
}

#[tokio::test]
async fn another_plugins_panicking_end_hook_does_not_skip_the_settlement() {
    let bank = bank(PLENTY);
    let h = H::new(
        bank.clone(),
        plugins(vec![Arc::new(PanicAtEnd), Arc::new(money(&bank))]),
        Some("openai"),
    );
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));

    assert_eq!(h.send(chat(UNARY_REQ)).await.0, StatusCode::OK);

    assert_eq!(h.bases(), [(Basis::Reported, UNARY_COST)]);
}

// Ordering, and several budgets.

#[tokio::test]
async fn split_its_hold_prices_the_model_a_later_plugin_chose() {
    let bank = bank(100_000);
    let (client, provider) = money(&bank).split();
    let upgrade = OnProvider(|px| px.filter(set_model("gpt-big")));
    let h = H::new(
        bank.clone(),
        plugins(vec![
            Arc::new(client),
            Arc::new(upgrade),
            Arc::new(provider),
        ]),
        Some("openai"),
    );

    // 1000 output tokens at gpt-big is 300,000: more than the wallet.
    let (status, _) = h.send(chat(UNARY_REQ)).await;

    assert_eq!(status, StatusCode::PAYMENT_REQUIRED);
    assert!(h.mock.sent().is_empty());
}

#[tokio::test]
async fn two_budgets_on_one_gateway_keep_their_own_holds() {
    let (keys, orgs) = (bank(PLENTY), bank(PLENTY));
    let h = H::new(
        keys.clone(),
        plugins(vec![
            Arc::new(money(&keys)),
            Arc::new(Money::new(orgs.clone(), prices()).cap(CAP)),
        ]),
        Some("openai"),
    );
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));

    assert_eq!(h.send(chat(UNARY_REQ)).await.0, StatusCode::OK);

    assert_eq!(h.bases(), [(Basis::Reported, UNARY_COST)]);
    assert_eq!(orgs.journal().len(), 1);
    assert_eq!(orgs.spent("acme"), UNARY_COST);
    assert_eq!(orgs.open_holds(), 0);
}

#[tokio::test]
async fn a_key_an_auth_plugin_resolved_is_read_from_the_extensions() {
    #[derive(Clone)]
    struct Account(String);

    struct Auth;

    #[respan_gateway_core::async_trait]
    impl Plugin for Auth {
        async fn on_client(
            &self,
            cx: &mut respan_gateway_core::ClientContext,
        ) -> Result<Option<respan_gateway_core::Response>, respan_gateway_core::Fault> {
            // Resolve, then strip the gateway's key before formats pass it on.
            let head = cx.head_mut();
            head.headers.remove("authorization");
            head.extensions.insert(Account(KEY.into()));
            Ok(None)
        }
    }

    let bank = bank(PLENTY);
    let money = money(&bank).who(|cx| cx.head().extensions.get::<Account>().map(|a| a.0.clone()));
    let h = H::new(
        bank.clone(),
        plugins(vec![Arc::new(Auth), Arc::new(money)]),
        Some("openai"),
    );
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));

    assert_eq!(h.send(chat(UNARY_REQ)).await.0, StatusCode::OK);
    assert_eq!(bank.spent(KEY), UNARY_COST);
}
