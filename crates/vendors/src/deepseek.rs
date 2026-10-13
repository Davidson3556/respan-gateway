//! DeepSeek: Chat Completions.

use respan_gateway_core::{Plugin, ProviderContext, Result, async_trait};

use respan_gateway_conversation_http::openai::OpenAiChat;

use crate::{Quirk, Speaks, Vendor, chat};

/// DeepSeek.
pub const VENDOR: Vendor = Vendor {
    id: "deepseek",
    base_url: Some("https://api.deepseek.com"),
    key_env: &["DEEPSEEK_API_KEY"],
    formats: &[&Speaks::http(OpenAiChat)],
    quirks: &[Quirk {
        name: "reasoning_content",
        description: "Reasoning is `reasoning_content`, in a request's messages and a \
                      response's message or delta.",
    }],
    plugin: Some(&DeepSeekQuirks),
    ..Vendor::DEFAULT
};

/// DeepSeek's quirks: reasoning is `reasoning_content`.
#[derive(Debug)]
pub struct DeepSeekQuirks;

#[async_trait]
impl Plugin for DeepSeekQuirks {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<()> {
        chat::reasoning_content(px)
    }
}
