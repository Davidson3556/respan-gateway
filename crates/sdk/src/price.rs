//! What a model costs.

use serde::{Deserialize, Serialize};

/// A model's price, in US dollars per million tokens, and per second of
/// audio for a model billed by its length.
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
    /// Of each input token written to a cache; `input` if unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_write: Option<f64>,
    /// Of each second of audio, as a transcription model may bill.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub per_second: Option<f64>,
}

impl Price {
    /// A price of `input` and `output`, and of `cached_input` if it has
    /// its own.
    pub fn new(input: f64, output: f64, cached_input: Option<f64>) -> Self {
        Self {
            input,
            output,
            cached_input,
            cache_write: None,
            per_second: None,
        }
    }

    /// What a typical call weighs it at, to rank models by: input, and
    /// output three times, as calls write less than they read.
    pub fn weight(&self) -> f64 {
        self.input + 3.0 * self.output
    }
}
