//! Use case 9, money under concurrency: prepaid wallets nested key within
//! team within customer. Every call holds its worst case on the key, its
//! team and its customer at once, or is refused 402 before it is sent, and
//! settles once from its reported usage. Fifty parallel requests against
//! a $1 wallet never spend more than $1.
//!
//! `cargo run -p respan-gateway-examples --example wallet`

use respan_gateway_core::{BadRequest, ErrorCode};
use std::collections::HashMap;

use mockllm::{Answer, FakeOpenAi};
use respan_gateway_core::{Fault, Gateway, ProviderContext, futures};
use respan_gateway_examples::Price;
use respan_gateway_examples::chat_bound::{answers, input_bound};
use respan_gateway_http::Credential;
use respan_gateway_http::Status;
use respan_gateway_http::http::{HeaderValue, StatusCode};
#[path = "../support/billing/ledger.rs"]
mod ledger;
use ledger::MemoryLedger;
use respan_gateway_conversation::{Input, Output};
#[path = "../support/billing/accounting.rs"]
mod accounting;
use accounting::{Accounting, Quote, Tally};
use respan_gateway_conversation_http::openai::OpenAiChat;
#[allow(dead_code)]
#[path = "../../tests/support/http.rs"]
mod http_support;
#[allow(dead_code)]
#[path = "../support/http.rs"]
mod provider_support;
use http_support::{post, read_whole, serve};

/// Your table; a model not in it has no cost bound.
fn prices() -> HashMap<String, Price> {
    HashMap::from([("gpt-x".to_owned(), Price::new(2_000_000, 8_000_000))])
}

/// Who pays and the most the call can cost; a filter prices each usage
/// increment the provider reports into the call's tally. A call with no
/// price, no `max_tokens` or non-text input has no bound, so it is refused;
/// `n` answers may each be `max_tokens` long, as the provider reads it.
fn quote(
    px: &mut ProviderContext,
    tally: Tally,
    prices: &HashMap<String, Price>,
) -> Result<Quote, Fault> {
    let key = px
        .client()
        .extensions()
        .get::<Credential>()
        .and_then(Credential::bearer);
    let key = key.ok_or_else(|| {
        Fault::new("no key")
            .with(ErrorCode("no_key".into()))
            .with(Status(StatusCode::UNAUTHORIZED))
    })?;
    let chat = px.input::<Input>()?;
    let model = chat.model.as_deref().unwrap_or_default();
    let Some(&price) = prices.get(model) else {
        return Err(BadRequest::fault(format!("no price for model '{model}'")));
    };
    let max_output = chat
        .options
        .max_output_tokens
        .ok_or_else(|| BadRequest::fault("set max_tokens"))?;
    let reserve = price.cost(
        input_bound(chat)?,
        max_output.saturating_mul(answers(chat)?),
    );
    let key = key.to_owned();
    // Accounting runs last, so this filter sees each reply first.
    px.filter(move |reply: &mut Output| {
        if let Output::Usage(usage) = reply {
            tally.add(price.of(usage));
        }
        Ok(())
    })?;
    // The ledger holds the key's team and customer with it.
    Ok(Quote::new([key], reserve))
}

/// acme ($1) has team-a ($0.30, keys sk-a1 and sk-a2) and team-b (sk-b1).
fn wallets() -> MemoryLedger {
    let ledger = MemoryLedger::new();
    ledger
        .open("acme", None, 1_000_000)
        .open("team-a", Some("acme"), 300_000)
        .open("team-b", Some("acme"), 1_000_000)
        .open("sk-a1", Some("team-a"), 1_000_000)
        .open("sk-a2", Some("team-a"), 1_000_000)
        .open("sk-b1", Some("team-b"), 1_000_000);
    ledger
}

fn gateway(
    transport: impl Fn(respan_gateway_core::Extensions) -> respan_gateway_http::ProviderIo
    + Send
    + Sync
    + 'static,
    ledger: MemoryLedger,
) -> Result<Gateway, Fault> {
    let prices = prices();
    let accounting = Accounting::new(ledger, move |px, tally| quote(px, tally, &prices));
    Gateway::new()
        .provider::<Input, _, _>("openai", OpenAiChat::OPENAI, transport)?
        .client::<Input, _, _>(OpenAiChat::OPENAI)?
        // Register it last, so it prices the call as other plugins left it.
        .plugin(accounting)
        .target("openai")
}

/// A reply reporting 10 input and 10,000 output tokens: 80,020 micro-dollars.
fn answer() -> Answer {
    let body = r#"{"id":"c1","object":"chat.completion","model":"gpt-x","choices":[{"index":0,"finish_reason":"length","message":{"role":"assistant","content":"..."}}],"usage":{"prompt_tokens":10,"completion_tokens":10000}}"#;
    Answer::raw(body)
}

/// `key` sends `body`.
async fn send(gw: &Gateway, key: &str, body: &'static str) -> Result<StatusCode, Fault> {
    let mut req = post("/v1/chat/completions", body);
    let auth = HeaderValue::from_str(&format!("Bearer {key}"))
        .map_err(|e| respan_gateway_http::built_wrong(e.into()))?;
    req.headers_mut().insert("authorization", auth);
    let (status, _, _) = read_whole(serve(gw, req).await.0).await;
    gw.idle().await;
    Ok(status)
}

/// `key` asks for up to 12,000 tokens: about $0.1 held per call.
async fn ask(gw: &Gateway, key: &str) -> Result<StatusCode, Fault> {
    let body =
        r#"{"model":"gpt-x","max_tokens":12000,"messages":[{"role":"user","content":"hi"}]}"#;
    send(gw, key, body).await
}

/// Fifty requests at once from `key`: how many were served.
async fn fifty(gw: &Gateway, fake: &FakeOpenAi, key: &'static str) -> usize {
    let calls = (0..50).map(|_| {
        fake.answer(answer());
        let gw = gw.clone();
        tokio::spawn(async move { ask(&gw, key).await })
    });
    let statuses = futures::future::join_all(calls).await;
    let served = statuses
        .into_iter()
        .filter(|s| matches!(s, Ok(Ok(s)) if s.is_success()));
    served.count()
}

#[tokio::main]
async fn main() -> Result<(), Fault> {
    let (fake, ledger) = (FakeOpenAi::start().await, wallets());
    let gw = gateway(
        provider_support::openai(&fake.base_url())?.connect(),
        ledger.clone(),
    )?;
    let served = fifty(&gw, &fake, "sk-b1").await;
    println!(
        "served {served} of 50; acme spent {} of 1,000,000",
        ledger.spent("acme")
    );
    println!("sk-a1 now gets {}", ask(&gw, "sk-a1").await?);
    Ok(())
}

#[cfg(test)]
#[path = "../tests/cases/billing/wallet.rs"]
mod tests;

#[cfg(test)]
#[path = "../tests/cases/billing/ledger.rs"]
mod ledger_tests;

#[cfg(test)]
#[path = "../tests/cases/billing/accounting.rs"]
mod accounting_tests;

#[cfg(test)]
#[path = "../tests/cases/billing/accounting_wire.rs"]
mod accounting_wire_tests;
