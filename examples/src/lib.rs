//! Shared helpers for the topic-grouped examples in `examples/README.md`.
//! Run one with
//! `cargo run -p respan-gateway-examples --example <name>`. Each has tests that
//! `cargo test` runs; see `examples/README.md` for the list.

pub mod admission;
pub mod rest;
pub mod store;

/// The most bytes of one message the examples' adapters and transports
/// hold: 32 MiB, as the gateway binary's, the largest request the vendors'
/// JSON APIs take (Anthropic's Messages API).
pub const LIMIT: usize = 32 << 20;

use std::sync::{Arc, Mutex, PoisonError};

use respan_gateway_conversation::{Input, Output, units};
use respan_gateway_sdk::Usage;
use respan_gateway_sdk::{Fault, Plugin, ProviderContext, async_trait};

/// A chat call's usage: every [`Output::usage`] increment its provider
/// reported, summed by the filter [`meter`] adds. Clones share it.
#[derive(Debug, Clone, Default)]
pub struct CallUsage(Arc<Mutex<Usage>>);

impl CallUsage {
    /// The usage summed so far.
    pub fn get(&self) -> Usage {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// The usage of the call `px`, if [`meter`] ran for it; read it in
    /// `on_provider_end`, once the call's filters have ended.
    pub fn of(px: &ProviderContext) -> Option<Usage> {
        px.client()
            .extensions()
            .get::<CallUsage>()
            .map(CallUsage::get)
    }
}

/// In `on_provider`: sum the usage of this chat call's replies into a
/// [`CallUsage`] in its extensions, the one already there if another
/// plugin metered the call. A call of another kind is not metered.
///
/// The kernel counts no usage: this filter sees a reply only if no filter
/// before it failed on it or held it back. Output filters run the last
/// added first, so meter from the plugin that runs last: register it after
/// every plugin that filters outputs.
pub fn meter(px: &mut ProviderContext) -> Result<(), Fault> {
    if !px.is_kind::<Input>() || px.extensions().get::<CallUsage>().is_some() {
        return Ok(());
    }
    let usage = CallUsage::default();
    px.extensions_mut().insert(usage.clone());
    px.filter(move |reply: &mut Output| {
        if let Output::Usage(reported) = reply {
            *usage.0.lock().unwrap_or_else(PoisonError::into_inner) += &*reported;
        }
        Ok(())
    })
}

/// A plugin that meters every chat call ([`meter`]) and adds up their
/// usage, such as an embedder's tally. Register it last. Clones share it.
#[derive(Debug, Clone, Default)]
pub struct Meter(Arc<Mutex<Usage>>);

impl Meter {
    /// The usage of every call that ended, added up.
    pub fn usage(&self) -> Usage {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

#[async_trait]
impl Plugin for Meter {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        meter(px)
    }

    async fn on_provider_end(&self, px: &ProviderContext, _end: &Result<(), Fault>) {
        if let Some(usage) = CallUsage::of(px) {
            *self.0.lock().unwrap_or_else(PoisonError::into_inner) += &usage;
        }
    }
}

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
        let n = |unit| usage.get(unit).unwrap_or(0);
        self.cost(n(units::INPUT_TOKENS), n(units::OUTPUT_TOKENS))
    }
}

pub mod chat_bound {
    //! Bounds on what a chat call can cost, before it is sent. Used by
    //! `wallet.rs` and `cost_cap.rs`.

    use respan_gateway_conversation::{Content, Input, ResponseFormat};
    use respan_gateway_sdk::{BadRequest, Fault};
    use serde_json::{self, Map, Value};

    /// The most input tokens `chat` can be. A token is at least a byte, so
    /// bytes bound them: its text, tool calls and results, tools, the JSON
    /// of what the kind has no place for (`native`), and 8 a message for
    /// framing. Structured-output schemas and descriptions also count.
    /// Content with no byte bound, such as an image, is refused.
    pub fn input_bound(chat: &Input) -> Result<u64, Fault> {
        let json = |m: &Map<_, _>, spellings: &[&str]| {
            let kept: Map<_, _> = m
                .iter()
                .filter(|(key, _)| !spellings.contains(&key.as_str()))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            serde_json::to_string(&kept).map_or(0, |s| s.len())
        };
        let mut bytes = chat.native.as_ref().map_or(0, |n| json(&n.fields, &[]));
        if let Some(ResponseFormat::JsonSchema {
            name,
            description,
            schema,
            ..
        }) = &chat.options.response_format
        {
            bytes += name.len()
                + description.as_deref().map_or(0, str::len)
                + serde_json::to_string(schema).map_or(0, |s| s.len());
        }
        for tool in &chat.tools {
            let description = tool.description.as_deref().map_or(0, str::len);
            let parameters = serde_json::to_string(&tool.parameters).map_or(0, |s| s.len());
            bytes += tool.name.len()
                + description
                + parameters
                + tool.native.as_ref().map_or(0, |n| json(&n.fields, &[]));
        }
        for m in &chat.messages {
            bytes += content_bytes(&m.content)?
                + m.native.as_ref().map_or(0, |n| json(&n.fields, &[]))
                + 8;
        }
        Ok(bytes as u64)
    }

    /// The bytes of `parts`: text, tool calls and results.
    fn content_bytes(parts: &[Content]) -> Result<usize, Fault> {
        let mut bytes = 0;
        for part in parts {
            bytes += match part {
                Content::Text { text, .. } => text.len(),
                Content::ToolCall {
                    id,
                    name,
                    arguments,
                    ..
                } => id.len() + name.len() + arguments.len(),
                Content::ToolResult { id, content, .. } => id.len() + content_bytes(content)?,
                _ => return Err(BadRequest::fault("no cost bound for non-text input")),
            };
        }
        Ok(bytes)
    }

    /// How many answers `chat` asks for: its `n`, 1 if unset.
    pub fn answers(chat: &Input) -> Result<u64, Fault> {
        let n = chat
            .native
            .as_ref()
            .and_then(|n| n.fields.get("n"))
            .map_or(Some(1), Value::as_u64);
        n.map(|n| n.max(1))
            .ok_or_else(|| BadRequest::fault("n must be a whole number"))
    }
}

#[cfg(test)]
mod model_tests {
    use super::chat_bound;
    use respan_gateway_conversation::{Input, ResponseFormat};
    use serde_json::json;

    #[test]
    fn structured_output_schema_still_contributes_to_the_cost_bound() {
        let mut input = Input::default();
        let schema = json!({"type":"object","description":"x".repeat(4096)});
        input.options.response_format = Some(ResponseFormat::JsonSchema {
            name: "result".into(),
            description: None,
            schema: schema.clone(),
            strict: Some(true),
        });
        assert!(
            chat_bound::input_bound(&input).expect("bounded schema")
                >= schema.to_string().len() as u64
        );
    }
}
