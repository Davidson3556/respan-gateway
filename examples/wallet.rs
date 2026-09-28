//! Use case 9, money under concurrency: prepaid wallets nested key within
//! team within customer. Every call holds its worst case on the key, its
//! team and its customer at once, or is refused 402 before it is sent, and
//! settles once from its reported usage. Fifty parallel requests against
//! a $1 wallet never spend more than $1.
//!
//! `cargo run -p respan-gateway-examples --example wallet`

use std::collections::HashMap;

use respan_gateway_core::http::{HeaderValue, StatusCode};
use respan_gateway_core::serde_json::Map;
use respan_gateway_core::{Fault, Gateway, ProviderContext, Usage, futures, http, serde_json};
use respan_gateway_formats::OpenAiChat;
use respan_gateway_formats::conversation::{Content, Conversation, units};
use respan_gateway_ledger::{Accounting, Amount, MemoryLedger, Quote};
use respan_gateway_testkit::{MockTransport, mock, post, read_whole};

/// A model's price per million tokens, in micro-dollars.
#[derive(Clone, Copy)]
struct Price {
    input: Amount,
    output: Amount,
}

impl Price {
    /// What `input` and `output` tokens cost, rounded up.
    fn cost(&self, input: u64, output: u64) -> Amount {
        let per_million = input as Amount * self.input + output as Amount * self.output;
        (per_million + 999_999) / 1_000_000
    }
}

/// Your table; a model not in it has no cost bound.
fn prices() -> HashMap<String, Price> {
    let gpt_x = Price {
        input: 2_000_000,
        output: 8_000_000,
    };
    HashMap::from([("gpt-x".to_owned(), gpt_x)])
}

/// The most input tokens `chat` can be. A token is at least a byte, so
/// bytes bound them: its text, the JSON of what the kind has no place for
/// (tools, tool calls), and 8 a message for framing. Content with no byte
/// bound, such as an image, is refused. Copied in `cost_cap.rs`: it is
/// chat-specific, so it stays out of the neutral ledger.
fn input_bound(chat: &Conversation) -> Result<u64, Fault> {
    let json = |m: &Map<_, _>| match m.is_empty() {
        true => 0,
        false => serde_json::to_string(m).map_or(0, |s| s.len()),
    };
    let mut bytes = json(&chat.unsupported);
    for m in &chat.messages {
        if !m.content.iter().all(|c| matches!(c, Content::Text { .. })) {
            return Err(Fault::bad_request("no cost bound for non-text input"));
        }
        bytes += m.text().len() + json(&m.unsupported) + 8;
    }
    Ok(bytes as u64)
}

/// Who pays, the most the call can cost, and its price. A call with no
/// price, no `max_tokens` or non-text input has no bound, so it is refused.
fn quote(px: &ProviderContext, prices: &HashMap<String, Price>) -> Result<Quote, Fault> {
    let auth = px.client().head().headers.get("authorization");
    let key = auth.and_then(|v| v.to_str().ok()?.strip_prefix("Bearer "));
    let key = key.ok_or_else(|| Fault::new(StatusCode::UNAUTHORIZED, "no_key", "no key"))?;
    let chat = px.input::<Conversation>()?;
    let model = chat.model.as_deref().unwrap_or_default();
    let Some(&price) = prices.get(model) else {
        return Err(Fault::bad_request(format!("no price for model '{model}'")));
    };
    let max_output = chat
        .max_tokens
        .ok_or_else(|| Fault::bad_request("set max_tokens"))?;
    let reserve = price.cost(input_bound(chat)?, max_output);
    let n = |u: &Usage, unit| u.get(unit).unwrap_or(0.0).max(0.0).ceil() as u64;
    let price = move |u: &Usage| price.cost(n(u, units::INPUT_TOKENS), n(u, units::OUTPUT_TOKENS));
    // The ledger holds the key's team and customer with it.
    Ok(Quote::new([key], reserve, price))
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
    let accounting = Accounting::new(ledger, move |px| quote(px, &prices));
    Gateway::new()
        .provider("openai", OpenAiChat, mock.at(OPENAI))?
        .serve(OpenAiChat)?
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
    let auth = HeaderValue::from_str(&format!("Bearer {key}")).map_err(http::Error::from)?;
    req.headers_mut().insert("authorization", auth);
    let (status, _, _) = read_whole(gw.handle(req).await.0).await;
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
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn fifty_parallel_requests_never_overspend_a_one_dollar_wallet() -> Result<(), Fault> {
        let (mock, ledger) = (MockTransport::new(), wallets());
        let gw = gateway(&mock, ledger.clone())?;
        let served = fifty(&gw, &mock, "sk-b1").await;
        let spent = ledger.spent("acme");
        assert!(spent <= 1_000_000, "overspent: {spent}");
        assert_eq!(spent, served as Amount * 80_020, "each settled once");
        assert!(served >= 10, "held about $0.1 each: {served}");
        assert_eq!(ledger.spent("team-b"), spent);
        Ok(())
    }

    #[tokio::test]
    async fn a_spent_team_stops_its_keys_and_not_the_others() -> Result<(), Fault> {
        let (mock, ledger) = (MockTransport::new(), wallets());
        let gw = gateway(&mock, ledger.clone())?;
        for _ in 0..4 {
            mock.reply(OPENAI, answer());
        }
        // team-a's $0.30 covers three calls; the fourth is refused, whichever key.
        for _ in 0..3 {
            assert!(ask(&gw, "sk-a1").await?.is_success());
        }
        assert_eq!(ask(&gw, "sk-a2").await?, StatusCode::PAYMENT_REQUIRED);
        assert!(ask(&gw, "sk-b1").await?.is_success());
        assert_eq!(ledger.spent("team-a"), 3 * 80_020);
        assert_eq!(ledger.available("sk-a2"), 1_000_000, "nothing held");
        Ok(())
    }

    #[tokio::test]
    async fn a_request_with_no_cost_bound_is_refused() -> Result<(), Fault> {
        let (mock, ledger) = (MockTransport::new(), wallets());
        let gw = gateway(&mock, ledger)?;
        let unbounded = r#"{"model":"gpt-x","messages":[]}"#;
        assert_eq!(
            send(&gw, "sk-b1", unbounded).await?,
            StatusCode::BAD_REQUEST
        );
        assert!(mock.sent().is_empty());
        Ok(())
    }

    /// An image's tokens have no byte bound: 600k of them would cost $1.20.
    #[tokio::test]
    async fn an_image_cannot_overspend_the_wallet() -> Result<(), Fault> {
        let (mock, ledger) = (MockTransport::new(), wallets());
        let gw = gateway(&mock, ledger.clone())?;
        let image = r#"{"model":"gpt-x","max_tokens":100,"messages":[{"role":"user","content":[{"type":"text","text":"what?"},{"type":"image_url","image_url":{"url":"data:image/png;base64,AAAA"}}]}]}"#;
        assert_eq!(send(&gw, "sk-b1", image).await?, StatusCode::BAD_REQUEST);
        assert!(mock.sent().is_empty());
        assert_eq!(ledger.spent("acme"), 0);
        Ok(())
    }
}
