//! What a model costs, and what a use of it cost.

use serde::{Deserialize, Serialize};

/// A model's price, in US dollars per million tokens.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Price {
    /// Of each input token.
    pub input: f64,
    /// Of each output token.
    pub output: f64,
    /// Of each input token read from a cache; `input` if unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cached_input: Option<f64>,
    /// Who says it: the config's prices, or the provider's list.
    #[serde(skip_deserializing)]
    pub from: PriceFrom,
}

/// Who says a [`Price`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PriceFrom {
    /// The host's config, which overrides a provider's.
    #[default]
    Config,
    /// The provider's model list.
    Provider,
}

/// Tokens a use of a model took.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Tokens {
    /// Read, the cached among them.
    pub input: u64,
    /// Written.
    pub output: u64,
    /// Of `input`, those read from a cache.
    pub cached_input: u64,
}

impl Price {
    /// A provider's price of `input` and `output`.
    pub fn provider(input: f64, output: f64, cached_input: Option<f64>) -> Self {
        Self {
            input,
            output,
            cached_input,
            from: PriceFrom::Provider,
        }
    }

    /// What `tokens` cost at this price, in US dollars.
    pub fn charge(&self, tokens: &Tokens) -> f64 {
        let cached = tokens.cached_input.min(tokens.input);
        let cached_rate = self.cached_input.unwrap_or(self.input);
        let dollars = (tokens.input - cached) as f64 * self.input
            + cached as f64 * cached_rate
            + tokens.output as f64 * self.output;
        dollars / 1e6
    }

    /// What a typical call weighs it at, to rank models by: input, and
    /// output three times, as calls write less than they read.
    pub fn weight(&self) -> f64 {
        self.input + 3.0 * self.output
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cached_input_costs_its_own_rate() {
        let price = Price::provider(2.0, 8.0, Some(0.5));
        let tokens = Tokens {
            input: 1_000_000,
            output: 500_000,
            cached_input: 400_000,
        };
        assert!((price.charge(&tokens) - (0.6 * 2.0 + 0.4 * 0.5 + 0.5 * 8.0)).abs() < 1e-9);
        let plain = Price {
            cached_input: None,
            ..price
        };
        assert!((plain.charge(&tokens) - 6.0).abs() < 1e-9);
    }
}
