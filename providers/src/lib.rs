#![warn(missing_docs)]
//! First-party conversation codecs. Shared message types and reusable format
//! helpers live in [`respan_gateway_formats`].

mod anthropic;
mod common;
mod openai;
mod tools;

pub use anthropic::AnthropicMessages;
pub use openai::OpenAiChat;

use formats::{Usage, conversation};
use respan_gateway_formats as formats;
