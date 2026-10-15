//! DeepSeek: Chat Completions.

use std::borrow::Cow;

use respan_gateway_conversation::{Input, Output};
use respan_gateway_conversation_http::dialect::{Call, Dialect, Dialected};
use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_core::{Fault, Filter};

use respan_gateway_vendor_kit::{Vendor, chat};

/// Where its API is.
const BASE_URL: &str = "https://api.deepseek.com";

/// DeepSeek.
pub const VENDOR: Vendor = Vendor {
    id: "deepseek",
    key_env: &["DEEPSEEK_API_KEY"],
    formats: &[(&Dialected::new(OpenAiChat, DeepSeek), Some(BASE_URL))],
    ..Vendor::DEFAULT
};

/// DeepSeek's Chat Completions: reasoning is `reasoning_content`.
#[derive(Debug, Clone, Copy)]
pub struct DeepSeek;

impl Dialect for DeepSeek {
    fn input<'a>(&self, input: &'a Input, _: &Call) -> Result<Cow<'a, Input>, Fault> {
        Ok(chat::reasoning_written(input))
    }

    fn output(&self, call: &Call) -> Option<Box<dyn Filter<Output>>> {
        chat::reasoning_read(call)
    }
}
