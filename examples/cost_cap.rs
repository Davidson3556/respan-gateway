//! A cost cap without a meter. Providers report output usage at a stream's
//! end, too late to stop it, so the cap works in three steps:
//!
//! 1. a `Conversation` filter asks every call for at most `CAP` tokens an
//!    answer, which bounds its cost, and `Accounting` holds that bound;
//! 2. a `Reply` filter on the provider edge estimates output from its text
//!    and tool calls, and cuts a stream that runs past the bound, for a
//!    provider that ignores `max_tokens`;
//! 3. the call settles from its reported usage, or, cut before the usage
//!    came, from the estimate, never less than its hold: a stream cut
//!    before any text (the model was thinking, the client left) may still
//!    have cost it all.
//!
//! Register `Cap` before `Accounting`: Cap stores the call's `Seen`, which
//! Accounting's quote reads, and a call without one is refused.
//!
//! `cargo run -p respan-gateway-examples --example cost_cap`

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use respan_gateway_core::http::StatusCode;
use respan_gateway_core::{Fault, Gateway, Plugin, ProviderContext, Usage, async_trait};
use respan_gateway_examples::Price;
use respan_gateway_examples::chat_bound::{answers, input_bound};
use respan_gateway_formats::OpenAiChat;
use respan_gateway_formats::conversation::{Conversation, Piece, Reply, units};
use respan_gateway_ledger::{Accounting, MemoryLedger, Quote};
use respan_gateway_testkit::{MockTransport, mock, post, read_whole};

/// The most output tokens an answer may have.
const CAP: u64 = 100;
/// The model's price.
const PRICE: Price = Price::new(1_000_000, 4_000_000);

/// One call's output bound, its output as seen, and whether it was cut.
#[derive(Clone, Default)]
struct Seen {
    cap: u64,
    chars: Arc<AtomicUsize>,
    cut: Arc<AtomicBool>,
}

impl Seen {
    /// About four characters a token.
    fn tokens(&self) -> u64 {
        self.chars.load(Ordering::Relaxed).div_ceil(4) as u64
    }
}

/// Steps 1 and 2. Register it before `Accounting`, which reads its `Seen`.
struct Cap;

#[async_trait]
impl Plugin for Cap {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        if !px.is_kind::<Conversation>() {
            return Ok(()); // no quote: Accounting refuses it
        }
        let seen = Seen {
            cap: CAP * answers(px.input::<Conversation>()?)?,
            ..Seen::default()
        };
        px.extensions_mut().insert(seen.clone());
        let limit = px
            .input::<Conversation>()?
            .max_tokens
            .map_or(CAP, |n| n.min(CAP));
        px.filter(move |chat: &mut Conversation| {
            chat.max_tokens = Some(limit);
            Ok(())
        })?;
        px.filter(move |reply: &mut Reply| {
            // A tool call counts as its whole JSON: an overestimate, which
            // errs toward cutting.
            let chars = |piece: &Piece| match piece {
                Piece::Text { text, .. } => text.len(),
                Piece::Other { raw, .. } => raw.to_string().len(),
                _ => 0,
            };
            let chars: usize = reply.content.iter().map(chars).sum();
            seen.chars.fetch_add(chars, Ordering::Relaxed);
            if seen.tokens() <= seen.cap {
                return Ok(());
            }
            seen.cut.store(true, Ordering::Relaxed);
            let msg = "the answer ran past its cap";
            Err(Fault::new(StatusCode::PAYMENT_REQUIRED, "cost_cap", msg))
        })
    }
}

/// Step 3's price: reported usage, else the estimate; a cut call's usage
/// may be partial, so never less than was seen. `Accounting` charges an
/// unfinished call at least its reserve.
fn quote(px: &ProviderContext) -> Result<Quote, Fault> {
    let seen = px.extensions().get::<Seen>().cloned();
    let seen = seen.ok_or_else(|| Fault::internal("register Cap before Accounting"))?;
    let input = input_bound(px.input::<Conversation>()?)?;
    let reserve = PRICE.cost(input, seen.cap);
    let price = move |u: &Usage| {
        let n = |unit| u.get(unit).map(|n| n.max(0.0).ceil() as u64);
        let reported = n(units::OUTPUT_TOKENS);
        let mut output = reported.unwrap_or(0);
        if reported.is_none() || seen.cut.load(Ordering::Relaxed) {
            output = output.max(seen.tokens());
        }
        PRICE.cost(n(units::INPUT_TOKENS).unwrap_or(input), output)
    };
    Ok(Quote::new(["team"], reserve, price))
}

const OPENAI: &str = "http://openai.test/v1";

fn gateway(mock: &MockTransport, ledger: MemoryLedger) -> Result<Gateway, Fault> {
    let accounting = Accounting::new(ledger, quote);
    Gateway::new()
        .provider("openai", OpenAiChat, mock.at(OPENAI))?
        .serve(OpenAiChat)?
        .plugin("cap", Cap)? // before accounting
        .plugin("accounting", accounting)?
        .target("openai")
}

/// A stream of these events.
fn sse(data: &[&str]) -> mock::Reply {
    let events: Vec<String> = data.iter().map(|d| format!("data: {d}\n\n")).collect();
    mock::sse(&events.iter().map(String::as_str).collect::<Vec<_>>())
}

/// A provider ignoring `max_tokens`: 30 deltas of 20 characters, about 150
/// tokens, with its usage at the end.
fn runaway() -> mock::Reply {
    let say = r#"{"id":"c1","model":"m","choices":[{"index":0,"delta":{"content":"twenty characters.. "}}]}"#;
    let usage = r#"{"id":"c1","choices":[],"usage":{"prompt_tokens":3,"completion_tokens":150}}"#;
    let mut data = vec![say; 30];
    data.extend([usage, "[DONE]"]);
    sse(&data)
}

/// Asks for 5,000 tokens: the body the client got, or how it broke off.
async fn ask(gw: &Gateway, stream: bool) -> String {
    let body = format!(
        r#"{{"model":"m","max_tokens":5000,"stream":{stream},"messages":[{{"role":"user","content":"hi"}}]}}"#
    );
    let (_, _, body) = read_whole(gw.handle(post("/v1/chat/completions", body)).await.0).await;
    gw.idle().await;
    match body {
        Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
        Err(fault) => format!("broken off: {fault}"),
    }
}

fn team() -> MemoryLedger {
    let ledger = MemoryLedger::new();
    ledger.open("team", None, 10_000);
    ledger
}

#[tokio::main]
async fn main() -> Result<(), Fault> {
    let (mock, ledger) = (MockTransport::new(), team());
    mock.reply(OPENAI, runaway());
    let gw = gateway(&mock, ledger.clone())?;
    let body = ask(&gw, true).await;
    println!(
        "client got {} bytes, ending: {:?}",
        body.len(),
        body.lines().rev().find(|l| !l.is_empty())
    );
    println!(
        "charged {} micro-dollars for a runaway stream",
        ledger.spent("team")
    );
    Ok(())
}

#[cfg(test)]
#[path = "tests/cost_cap.rs"]
mod tests;
