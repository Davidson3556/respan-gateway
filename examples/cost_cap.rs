//! A cost cap without a meter. Providers report output usage at a stream's
//! end, too late to stop it, so the cap works in three steps:
//!
//! 1. a `Conversation` filter asks every call for at most `CAP` tokens,
//!    which bounds its cost, and `Accounting` holds that bound;
//! 2. a `Reply` filter on the provider edge estimates output from its text
//!    and tool calls, and cuts a stream that runs past `CAP`, for a
//!    provider that ignores `max_tokens`;
//! 3. the call settles from its reported usage, or, cut before the usage
//!    came, from the estimate.
//!
//! `cargo run -p respan-gateway-examples --example cost_cap`

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use respan_gateway_core::http::StatusCode;
use respan_gateway_core::serde_json::Map;
use respan_gateway_core::{
    Fault, Gateway, Plugin, ProviderContext, Usage, async_trait, serde_json,
};
use respan_gateway_formats::OpenAiChat;
use respan_gateway_formats::conversation::{Content, Conversation, Piece, Reply, units};
use respan_gateway_ledger::{Accounting, Amount, MemoryLedger, Quote, Unreported};
use respan_gateway_testkit::{MockTransport, mock, post, read_whole};

/// The most output tokens a call may have.
const CAP: u64 = 100;
/// The model's price.
const PRICE: Price = Price {
    input: 1_000_000,
    output: 4_000_000,
};

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

/// The most input tokens `chat` can be. A token is at least a byte, so
/// bytes bound them: its text, the JSON of what the kind has no place for
/// (tools, tool calls), and 8 a message for framing. Content with no byte
/// bound, such as an image, is refused. Copied in `wallet.rs`: it is
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

/// One call's output as seen, and whether it was cut.
#[derive(Clone, Default)]
struct Seen {
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
        px.filter(|chat: &mut Conversation| {
            chat.max_tokens = Some(chat.max_tokens.map_or(CAP, |n| n.min(CAP)));
            Ok(())
        })?;
        let seen = Seen::default();
        px.extensions_mut().insert(seen.clone());
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
            if seen.tokens() <= CAP {
                return Ok(());
            }
            seen.cut.store(true, Ordering::Relaxed);
            let msg = "the answer ran past its cap";
            Err(Fault::new(StatusCode::PAYMENT_REQUIRED, "cost_cap", msg))
        })
    }
}

/// Step 3's price: reported usage, else the estimate; a cut call's usage
/// may be partial, so never less than was seen.
fn quote(px: &ProviderContext) -> Result<Quote, Fault> {
    let chat = px.input::<Conversation>()?;
    let seen = px.extensions().get::<Seen>().cloned();
    let seen = seen.ok_or_else(|| Fault::internal("register Cap before Accounting"))?;
    let input = input_bound(chat)?;
    let reserve = PRICE.cost(input, CAP);
    let price = move |u: &Usage| {
        let n = |unit| u.get(unit).map(|n| n.max(0.0).ceil() as u64);
        let output = match (n(units::OUTPUT_TOKENS), seen.cut.load(Ordering::Relaxed)) {
            (Some(n), false) => n,
            (n, _) => n.unwrap_or(0).max(seen.tokens()),
        };
        PRICE.cost(n(units::INPUT_TOKENS).unwrap_or(input), output)
    };
    Ok(Quote::new(["team"], reserve, price))
}

const OPENAI: &str = "http://openai.test/v1";

fn gateway(mock: &MockTransport, ledger: MemoryLedger) -> Result<Gateway, Fault> {
    // A cut stream's usage is unreported; its price estimates it.
    let accounting = Accounting::new(ledger, quote).unreported(Unreported::Price);
    Gateway::new()
        .provider("openai", OpenAiChat, mock.at(OPENAI))?
        .serve(OpenAiChat)?
        .plugin("cap", Cap)?
        .plugin("accounting", accounting)?
        .target("openai")
}

/// A provider ignoring `max_tokens`: 30 deltas of 20 characters, about 150
/// tokens, with its usage at the end.
fn runaway() -> mock::Reply {
    let delta = r#"data: {"id":"c1","model":"m","choices":[{"index":0,"delta":{"content":"twenty characters.. "}}]}"#;
    let mut chunks = vec![format!("{delta}\n\n"); 30];
    chunks.push(
        r#"data: {"id":"c1","choices":[],"usage":{"prompt_tokens":3,"completion_tokens":150}}"#
            .to_owned()
            + "\n\n",
    );
    chunks.push("data: [DONE]\n\n".to_owned());
    mock::sse(&chunks.iter().map(String::as_str).collect::<Vec<_>>())
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
mod tests {
    use super::*;

    #[tokio::test]
    async fn every_call_asks_for_at_most_cap_tokens() -> Result<(), Fault> {
        let mock = MockTransport::new();
        let gw = gateway(&mock, team())?;
        ask(&gw, false).await;
        let sent: serde_json::Value = serde_json::from_slice(mock.sent()[0].body())
            .map_err(|e| Fault::internal("json").with_source(e))?;
        assert_eq!(sent["max_tokens"], CAP);
        Ok(())
    }

    #[tokio::test]
    async fn a_runaway_stream_is_cut_and_charged_its_estimate() -> Result<(), Fault> {
        let (mock, ledger) = (MockTransport::new(), team());
        mock.reply(OPENAI, runaway());
        let gw = gateway(&mock, ledger.clone())?;
        let body = ask(&gw, true).await;
        assert!(!body.contains("[DONE]"), "cut: {body}");
        // Cut on the delta that passed 100 tokens: 21 × 20 characters = 105.
        assert_eq!(ledger.spent("team"), PRICE.cost(10, 105));
        Ok(())
    }

    #[tokio::test]
    async fn a_call_that_ends_settles_from_its_reported_usage() -> Result<(), Fault> {
        let (mock, ledger) = (MockTransport::new(), team());
        let answer = r#"{"id":"c1","object":"chat.completion","model":"m","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"hi"}}],"usage":{"prompt_tokens":3,"completion_tokens":7}}"#;
        mock.reply(OPENAI, mock::json(StatusCode::OK, answer));
        let gw = gateway(&mock, ledger.clone())?;
        ask(&gw, false).await;
        assert_eq!(ledger.spent("team"), PRICE.cost(3, 7));
        Ok(())
    }

    #[tokio::test]
    async fn a_runaway_tool_call_is_cut_and_charged_its_estimate() -> Result<(), Fault> {
        let (mock, ledger) = (MockTransport::new(), team());
        // 300 deltas of 20 characters of arguments, about 1,500 tokens.
        let first = r#"data: {"id":"c1","model":"m","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"t","type":"function","function":{"name":"f","arguments":""}}]}}]}"#;
        let delta = r#"data: {"id":"c1","model":"m","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"twenty characters.. "}}]}}]}"#;
        let mut chunks = vec![format!("{first}\n\n")];
        chunks.extend(vec![format!("{delta}\n\n"); 300]);
        chunks.push("data: [DONE]\n\n".to_owned());
        mock.reply(
            OPENAI,
            mock::sse(&chunks.iter().map(String::as_str).collect::<Vec<_>>()),
        );
        let gw = gateway(&mock, ledger.clone())?;
        let body = ask(&gw, true).await;
        assert!(!body.contains("[DONE]"), "cut: {body}");
        // No usage came: charged the input bound and at least the cap.
        assert!(
            ledger.spent("team") > PRICE.cost(10, CAP),
            "{}",
            ledger.spent("team")
        );
        Ok(())
    }

    #[tokio::test]
    async fn non_text_input_is_refused_before_it_is_sent() -> Result<(), Fault> {
        let (mock, ledger) = (MockTransport::new(), team());
        let gw = gateway(&mock, ledger.clone())?;
        let image = r#"{"model":"m","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"https://x.test/cat.png"}}]}]}"#;
        let (status, _, _) =
            read_whole(gw.handle(post("/v1/chat/completions", image)).await.0).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(mock.sent().is_empty());
        assert_eq!(ledger.spent("team"), 0);
        Ok(())
    }
}
