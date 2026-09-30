#![warn(missing_docs)]
//! First-party conversation codecs. Shared message types live in
//! [`respan_gateway_messages`]; reusable wire helpers in [`respan_gateway_formats`].

mod anthropic;
mod common;
mod openai;
mod tools;

pub use anthropic::AnthropicMessages;
pub use openai::OpenAiChat;

use respan_gateway_formats as formats;
use respan_gateway_messages::{Usage, conversation};
