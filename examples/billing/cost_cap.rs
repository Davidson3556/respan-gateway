//! A cost cap without a meter. Providers report output usage at a stream's
//! end, too late to stop it, so the cap works in three steps:
//!
//! 1. a `Input` filter asks every call for at most `CAP` tokens an
//!    answer, which bounds its cost, and `Accounting` holds that bound;
//! 2. an `Output` filter on the provider edge estimates output from its text
//!    and tool calls, and cuts a stream that runs past the bound, for a
//!    provider that ignores `max_tokens`;
//! 3. the call settles from its reported usage, or, cut before the usage
//!    came, from the estimate, never less than its hold: a stream cut
//!    before any text (the model was thinking, the client left) may still
//!    have cost it all. The quote's `Settle` filter sums the usage the
//!    provider reports and prices it into the call's tally at its end.
//!
//! Register `Cap` before `Accounting`: Cap stores the call's `Seen`, which
//! Accounting's quote reads, and a call without one is refused. Accounting
//! runs last, so `Settle` is the call's last filter and sees each reply
//! before Cap's filter can fail on it.
//!
//! `cargo run -p respan-gateway-examples --example cost_cap`

use respan_gateway_core::ErrorCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use mockllm::{Answer, FakeOpenAi};
use respan_gateway_core::{Fault, Filter, Gateway, Plugin, ProviderContext, async_trait};
use respan_gateway_examples::Price;
use respan_gateway_examples::chat_bound::{answers, input_bound};
use respan_gateway_http::Status;
#[path = "../support/billing/ledger.rs"]
mod ledger;
use ledger::MemoryLedger;
use respan_gateway_conversation::Usage;
use respan_gateway_conversation::{Block, Input, Output};
#[path = "../support/billing/accounting.rs"]
mod accounting;
use accounting::{Accounting, Quote, Tally};
use respan_gateway_conversation::openai::OpenAiChat;
#[allow(dead_code)]
#[path = "../../tests/support/http.rs"]
mod http_support;
#[allow(dead_code)]
#[path = "../support/http.rs"]
mod provider_support;
use http_support::{post, read_whole, serve};

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
        if !px.is_kind::<Input>() {
            return Ok(()); // no quote: Accounting refuses it
        }
        let chat = px.input::<Input>()?;
        let limit = chat.options.max_output_tokens.map_or(CAP, |n| n.min(CAP));
        let seen = Seen {
            cap: CAP.saturating_mul(answers(chat)?),
            ..Seen::default()
        };
        px.extensions_mut().insert(seen.clone());
        px.filter(move |chat: &mut Input| {
            chat.options.max_output_tokens = Some(limit);
            Ok(())
        })?;
        px.filter(move |reply: &mut Output| {
            // A delta counts its text or arguments, a tool call its name;
            // other events their whole JSON, an overestimate, which errs
            // toward cutting.
            let chars = match reply {
                Output::Delta { delta, .. } => delta.as_str().len(),
                Output::BlockStart {
                    kind: Block::ToolCall { name, .. },
                    ..
                } => name.len(),
                Output::Native(respan_gateway_core::NativeValue { value: raw, .. }) => {
                    raw.to_string().len()
                }
                _ => 0,
            };
            seen.chars.fetch_add(chars, Ordering::Relaxed);
            if seen.tokens() <= seen.cap {
                return Ok(());
            }
            seen.cut.store(true, Ordering::Relaxed);
            let msg = "the answer ran past its cap";
            Err(Fault::new(msg)
                .with(ErrorCode("cost_cap".into()))
                .with(Status(
                    respan_gateway_http::http::StatusCode::PAYMENT_REQUIRED,
                )))
        })
    }
}

/// Step 3: sums the usage the provider reports, and at the call's end
/// prices it into the tally: reported usage, else the estimate; a cut
/// call's usage may be partial, so never less than was seen. A call that
/// reported nothing and was not cut adds nothing, so `Accounting` charges
/// it at least its reserve, as it does any unfinished call.
struct Settle {
    usage: Usage,
    seen: Seen,
    /// The input's bound, for when no usage came.
    input: u64,
    tally: Tally,
}

impl Filter<Output> for Settle {
    fn edit(&mut self, reply: Output, out: &mut Vec<Output>) -> Result<(), Fault> {
        if let Output::Usage(usage) = &reply {
            self.usage += usage;
        }
        out.push(reply);
        Ok(())
    }

    fn end(&mut self, _end: &Result<(), Fault>, _out: &mut Vec<Output>) -> Result<(), Fault> {
        let cut = self.seen.cut.load(Ordering::Relaxed);
        if self.usage.is_empty() && !cut {
            return Ok(());
        }
        // A count of 0 is one the provider did not report.
        let mut output = self.usage.output_tokens;
        if output == 0 || cut {
            output = output.max(self.seen.tokens());
        }
        let input = match self.usage.input_tokens {
            0 => self.input,
            n => n,
        };
        self.tally.add(PRICE.cost(input, output));
        Ok(())
    }
}

/// Who pays and the call's bound, with `Settle` to price it.
fn quote(px: &mut ProviderContext, tally: Tally) -> Result<Quote, Fault> {
    let seen = px.extensions().get::<Seen>().cloned();
    let seen = seen.ok_or_else(|| Fault::internal("register Cap before Accounting"))?;
    let input = input_bound(px.input::<Input>()?)?;
    let reserve = PRICE.cost(input, seen.cap);
    let usage = Usage::default();
    px.filter(Settle {
        usage,
        seen,
        input,
        tally,
    })?;
    Ok(Quote::new(["team"], reserve))
}

fn gateway(
    transport: impl Fn(respan_gateway_core::Extensions) -> respan_gateway_http::ProviderIo
    + Send
    + Sync
    + 'static,
    ledger: MemoryLedger,
) -> Result<Gateway, Fault> {
    let accounting = Accounting::new(ledger, quote);
    Gateway::new()
        .provider::<Input, _, _>("openai", OpenAiChat::OPENAI, transport)?
        .client::<Input, _, _>(OpenAiChat::OPENAI)?
        .plugin(Cap) // before accounting
        .plugin(accounting)
        .target("openai")
}

/// A stream of these events.
fn sse(data: &[&str]) -> Answer {
    let events: Vec<String> = data.iter().map(|d| format!("data: {d}\n\n")).collect();
    provider_support::sse(&events.iter().map(String::as_str).collect::<Vec<_>>())
}

/// A provider ignoring `max_tokens`: 30 deltas of 20 characters, about 150
/// tokens, with its usage at the end.
fn runaway() -> Answer {
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
    let (_, _, body) = read_whole(serve(gw, post("/v1/chat/completions", body)).await.0).await;
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
    let (fake, ledger) = (FakeOpenAi::start().await, team());
    fake.answer(runaway());
    let gw = gateway(
        provider_support::openai(&fake.base_url())?.connect(),
        ledger.clone(),
    )?;
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
#[path = "../tests/cases/billing/cost_cap.rs"]
mod tests;
