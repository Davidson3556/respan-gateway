//! Prices per model, and turning usage and estimates into money.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use respan_gateway_core::{Fault, Usage};
use respan_gateway_formats::conversation::{Content, Conversation, Reply, units};

use crate::Micros;

/// A model's price, in micros per token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Price {
    /// Per uncached input token.
    pub input: Micros,
    /// Per input token read from a cache.
    pub cached_input: Micros,
    /// Per output token.
    pub output: Micros,
}

impl Price {
    /// A price with cached input at the input price.
    pub fn new(input: Micros, output: Micros) -> Self {
        Self {
            input,
            cached_input: input,
            output,
        }
    }

    /// What `usage` of the conversation kind costs. Amounts that are not
    /// finite or are negative count as zero; fractions round up.
    pub fn of(&self, usage: &Usage) -> Micros {
        let n = |unit| tokens(usage.get(unit).unwrap_or(0.0));
        let (input, cached) = (n(units::INPUT_TOKENS), n(units::CACHED_INPUT_TOKENS));
        let cached = cached.min(input);
        (input - cached) * self.input
            + cached * self.cached_input
            + n(units::OUTPUT_TOKENS) * self.output
    }

    /// What `input` and `output` tokens cost.
    pub fn tokens(&self, input: u64, output: u64) -> Micros {
        let (input, output) = (to_micros(input), to_micros(output));
        input * self.input + output * self.output
    }
}

fn tokens(amount: f64) -> Micros {
    match amount.is_finite() && amount > 0.0 {
        true => amount.ceil().min(i64::MAX as f64) as Micros,
        false => 0,
    }
}

fn to_micros(n: u64) -> Micros {
    Micros::try_from(n).unwrap_or(Micros::MAX)
}

/// Prices by model name. A model with no price has no cost bound, so its
/// calls are refused.
#[derive(Debug, Clone, Default)]
pub struct Prices(HashMap<String, Price>);

impl Prices {
    /// No prices.
    pub fn new() -> Self {
        Self::default()
    }

    /// Price `model` at `price`.
    pub fn model(mut self, model: impl Into<String>, price: Price) -> Self {
        self.0.insert(model.into(), price);
        self
    }

    /// The price of `model`.
    pub fn get(&self, model: &str) -> Option<Price> {
        self.0.get(model).copied()
    }
}

/// Roughly four characters a token, plus four a message for its framing.
pub fn estimate_input(chat: &Conversation) -> u64 {
    let chars: usize = chat
        .messages
        .iter()
        .map(|m| {
            let content: usize = m
                .content
                .iter()
                .map(|c| match c {
                    Content::Text { text, .. } => text.len(),
                    Content::Other { raw, .. } => raw.to_string().len(),
                    _ => 0,
                })
                .sum();
            content + json_len(&m.unsupported) + 16
        })
        .sum();
    let chars = chars + json_len(&chat.unsupported);
    (chars as u64).div_ceil(4)
}

fn json_len(
    map: &respan_gateway_core::serde_json::Map<String, respan_gateway_core::serde_json::Value>,
) -> usize {
    match map.is_empty() {
        true => 0,
        false => respan_gateway_core::serde_json::Value::Object(map.clone())
            .to_string()
            .len(),
    }
}

/// Characters of output seen on one call's response, shared between its
/// filter and its end hook.
#[derive(Debug, Clone, Default)]
pub struct Tally(Arc<AtomicU64>);

impl Tally {
    /// A filter that counts and changes nothing.
    pub fn filter(&self) -> impl FnMut(&mut Reply) -> Result<(), Fault> + Send + 'static {
        let count = self.0.clone();
        move |event: &mut Reply| {
            let n = match event {
                Reply::Text { text, .. } => text.len(),
                Reply::Other { raw, .. } => raw.to_string().len(),
                _ => 0,
            };
            count.fetch_add(n as u64, Ordering::Relaxed);
            Ok(())
        }
    }

    /// Output tokens estimated from what was seen.
    pub fn tokens(&self) -> u64 {
        self.0.load(Ordering::Relaxed).div_ceil(4)
    }
}
