//! Use case 9, money under concurrency: prepaid wallets nested key within
//! team within customer. Every call holds its worst case on the key, its
//! team and its customer at once, or is refused 402 before it is sent, and
//! settles once from its reported usage. Fifty parallel requests against
//! a $1 wallet never spend more than $1.
//!
//! `cargo run -p respan-gateway-examples --example wallet`

use std::collections::HashMap;

use respan_gateway_core::{Fault, Gateway, ProviderContext, futures};
use respan_gateway_examples::Price;
use respan_gateway_examples::chat_bound::{answers, input_bound};
use respan_gateway_formats::conversation::{Conversation, Reply};
use respan_gateway_http::Credential;
use respan_gateway_http::Status;
use respan_gateway_http::http::{HeaderValue, StatusCode};
use respan_gateway_ledger::{Accounting, MemoryLedger, Quote, Tally};
use respan_gateway_providers::OpenAiChat;
use respan_gateway_testkit::{MockTransport, mock, post, read_whole, serve};

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
    let key = key.ok_or_else(|| Fault::new("no_key", "no key").with(Status(401)))?;
    let chat = px.input::<Conversation>()?;
    let model = chat.model.as_deref().unwrap_or_default();
    let Some(&price) = prices.get(model) else {
        return Err(Fault::bad_request(format!("no price for model '{model}'")));
    };
    let max_output = chat
        .max_tokens
        .ok_or_else(|| Fault::bad_request("set max_tokens"))?;
    let reserve = price.cost(
        input_bound(chat)?,
        max_output.saturating_mul(answers(chat)?),
    );
    let key = key.to_owned();
    // Accounting runs last, so this filter sees each reply first.
    px.filter(move |reply: &mut Reply| {
        if !reply.usage.is_empty() {
            tally.add(price.of(&reply.usage));
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

const OPENAI: &str = "http://openai.test/v1";

fn gateway(mock: &MockTransport, ledger: MemoryLedger) -> Result<Gateway, Fault> {
    let prices = prices();
    let accounting = Accounting::new(ledger, move |px, tally| quote(px, tally, &prices));
    Gateway::new()
        .provider::<Conversation, _, _>(
            "openai",
            OpenAiChat,
            respan_gateway_http::connect(mock.at(OPENAI)),
        )?
        .client::<Conversation, _, _>(OpenAiChat)?
        // Register it last, so it prices the call as other plugins left it.
        .plugin("accounting", accounting)?
        .target("openai")
}

/// A reply reporting 10 input and 10,000 output tokens: 80,020 micro-dollars.
fn answer() -> mock::Reply {
    let body = r#"{"id":"c1","object":"chat.completion","model":"gpt-x","choices":[{"index":0,"finish_reason":"length","message":{"role":"assistant","content":"..."}}],"usage":{"prompt_tokens":10,"completion_tokens":10000}}"#;
    mock::json(StatusCode::OK, body)
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
async fn fifty(gw: &Gateway, mock: &MockTransport, key: &'static str) -> usize {
    let calls = (0..50).map(|_| {
        mock.reply(OPENAI, answer());
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
    let (mock, ledger) = (MockTransport::new(), wallets());
    let gw = gateway(&mock, ledger.clone())?;
    let served = fifty(&gw, &mock, "sk-b1").await;
    println!(
        "served {served} of 50; acme spent {} of 1,000,000",
        ledger.spent("acme")
    );
    println!("sk-a1 now gets {}", ask(&gw, "sk-a1").await?);
    Ok(())
}

#[cfg(test)]
#[path = "tests/wallet.rs"]
mod tests;
