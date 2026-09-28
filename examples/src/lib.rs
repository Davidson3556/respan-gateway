//! Helpers the examples share. The runnable examples live next to this crate
//! as `examples/<name>.rs`; run one with
//! `cargo run -p respan-gateway-examples --example <name>`. Each has tests that
//! `cargo test` runs; see `examples/README.md` for the list.

use respan_gateway_core::Usage;
use respan_gateway_formats::conversation::units;

/// A model's price per million tokens, in micro-dollars. Add fields for
/// what else you price, such as cached input. Used by `billing.rs`,
/// `wallet.rs` and `cost_cap.rs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Price {
    /// Per million input tokens.
    pub input: i64,
    /// Per million output tokens.
    pub output: i64,
}

impl Price {
    /// `input` and `output` per million tokens.
    pub const fn new(input: i64, output: i64) -> Self {
        Self { input, output }
    }

    /// What `input` and `output` tokens cost, rounded up. Saturates rather
    /// than wraps, so a client's absurd count prices high, never low.
    pub fn cost(&self, input: u64, output: u64) -> i64 {
        let part = |n: u64, per: i64| i64::try_from(n).unwrap_or(i64::MAX).saturating_mul(per);
        let per_million = part(input, self.input).saturating_add(part(output, self.output));
        per_million.saturating_add(999_999) / 1_000_000
    }

    /// What `usage`'s input and output tokens cost; a missing count is 0.
    pub fn of(&self, usage: &Usage) -> i64 {
        let n = |unit| usage.get(unit).unwrap_or(0.0).max(0.0).ceil() as u64;
        self.cost(n(units::INPUT_TOKENS), n(units::OUTPUT_TOKENS))
    }
}

pub mod chat_bound {
    //! Bounds on what a chat call can cost, before it is sent. Used by
    //! `wallet.rs` and `cost_cap.rs`.

    use respan_gateway_core::Fault;
    use respan_gateway_core::serde_json::{self, Map, Value};
    use respan_gateway_formats::conversation::{Content, Conversation};

    /// The most input tokens `chat` can be. A token is at least a byte, so
    /// bytes bound them: its text, the JSON of what the kind has no place for
    /// (tools, tool calls), and 8 a message for framing. Content with no byte
    /// bound, such as an image, is refused.
    pub fn input_bound(chat: &Conversation) -> Result<u64, Fault> {
        let json = |m: &Map<_, _>| serde_json::to_string(m).map_or(0, |s| s.len());
        let mut bytes = json(&chat.unsupported);
        for m in &chat.messages {
            if !m.content.iter().all(|c| matches!(c, Content::Text { .. })) {
                return Err(Fault::bad_request("no cost bound for non-text input"));
            }
            bytes += m.text().len() + json(&m.unsupported) + 8;
        }
        Ok(bytes as u64)
    }

    /// How many answers `chat` asks for: its `n`, 1 if unset.
    pub fn answers(chat: &Conversation) -> Result<u64, Fault> {
        let n = chat.unsupported.get("n").map_or(Some(1), Value::as_u64);
        n.map(|n| n.max(1))
            .ok_or_else(|| Fault::bad_request("n must be a whole number"))
    }
}
