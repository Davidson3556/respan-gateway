//! Probes of the round-8 core: the model is no longer the kernel's. It is a
//! field of the conversation input, changed by a filter
//! (`conversation::set_model`), and the budget prices what the input asks.

mod common;

use std::sync::Arc;

use common::*;
use respan_gateway_core::http::StatusCode;
use respan_gateway_core::serde_json::{self, Value};
use respan_gateway_core::{ClientContext, Fault, Plugin, Response, async_trait};
use respan_gateway_formats::conversation::set_model;
use respan_gateway_testkit::mock;
use respan_money::Basis;

fn sent_model(h: &H, i: usize) -> Value {
    let sent = h.mock.sent();
    let body: Value = serde_json::from_slice(sent[i].body()).unwrap_or_default();
    body["model"].clone()
}

/// Sends one call to `openai` with a filter that asks for `gpt-big`.
struct BigCall;

#[async_trait]
impl Plugin for BigCall {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let call = cx.call("openai").with_filter(set_model("gpt-big"));
        call.await.map(Some)
    }
}

/// A model one call asks for through its own filter is the one it is held
/// and priced at: 1000 output tokens at gpt-big are more than the wallet.
#[tokio::test]
async fn a_calls_own_model_filter_is_priced_on_its_provider_edge() {
    let bank = bank(100_000);
    let h = H::new(
        bank.clone(),
        vec![Arc::new(money(&bank)), Arc::new(BigCall)],
        None,
    );

    assert_eq!(
        h.send(chat(UNARY_REQ)).await.0,
        StatusCode::PAYMENT_REQUIRED
    );
    assert!(h.mock.sent().is_empty());
}

/// Asks every call for `gpt-big` from the client edge.
struct BigRequest;

#[async_trait]
impl Plugin for BigRequest {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.filter(set_model("gpt-big"))?;
        Ok(None)
    }
}

/// A request filter that changes the model: the call carries it, the
/// budget prices it, and what the kind has no place for is kept.
#[tokio::test]
async fn a_request_model_filter_is_sent_and_charged() {
    let bank = bank(10_000_000);
    let h = H::new(
        bank.clone(),
        vec![Arc::new(BigRequest), Arc::new(money(&bank))],
        Some("openai"),
    );
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));
    let req =
        r#"{"model":"gpt-x","brand_new":{"a":1},"messages":[{"role":"user","content":"hi"}]}"#;

    assert_eq!(h.send(chat(req)).await.0, StatusCode::OK);

    assert_eq!(sent_model(&h, 0), "gpt-big");
    let sent: Value = serde_json::from_slice(h.mock.sent()[0].body()).unwrap_or_default();
    assert_eq!(sent["brand_new"]["a"], 1);
    assert_eq!(h.bases(), [(Basis::Reported, 4 * 100 + 300)]);
}

/// A client body that names no model, given one by a plugin: priced, and
/// the model is written into the body sent.
#[tokio::test]
async fn a_body_with_no_model_is_priced_at_the_model_a_filter_gives_it() {
    let bank = bank(10_000_000);
    let h = H::new(
        bank.clone(),
        vec![Arc::new(BigRequest), Arc::new(money(&bank))],
        Some("openai"),
    );
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));
    let req = r#"{"messages":[{"role":"user","content":"hi"}]}"#;

    assert_eq!(h.send(chat(req)).await.0, StatusCode::OK);

    assert_eq!(sent_model(&h, 0), "gpt-big");
    assert_eq!(h.bases(), [(Basis::Reported, 4 * 100 + 300)]);
}

/// With the model in the input, a body that does not decode has no model
/// either: the budget fails closed with the decoder's fault, before any
/// hold or send.
#[tokio::test]
async fn a_body_that_does_not_decode_is_refused_before_any_hold() {
    let bank = bank(10_000_000);
    let h = H::new(bank.clone(), vec![Arc::new(money(&bank))], Some("openai"));

    let (status, _) = h.send(chat(r#"{"model":"gpt-x","#)).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(h.mock.sent().is_empty());
    assert!(h.bank.journal().is_empty());
}

/// Translated: the model a filter set reaches an Anthropic body too.
#[tokio::test]
async fn a_model_filter_reaches_a_translated_call() {
    let bank = bank(10_000_000);
    let h = H::new(
        bank.clone(),
        vec![Arc::new(BigRequest), Arc::new(money(&bank))],
        Some("claude"),
    );
    let answer = r#"{"id":"m","type":"message","role":"assistant","model":"gpt-big","content":[{"type":"text","text":"hi"}],"stop_reason":"end_turn","usage":{"input_tokens":4,"output_tokens":1}}"#;
    h.mock.reply(CLAUDE, mock::json(StatusCode::OK, answer));

    assert_eq!(h.send(chat(UNARY_REQ)).await.0, StatusCode::OK);

    assert_eq!(sent_model(&h, 0), "gpt-big");
    assert_eq!(h.bases(), [(Basis::Reported, 4 * 100 + 300)]);
}
