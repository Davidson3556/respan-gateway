//! DeepSeek: Chat Completions.

use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_core::{Plugin, ProviderContext, Result, async_trait};

use crate::{Format, Provider, Quirk, Speaks, chat};

/// DeepSeek.
pub const PROVIDER: Provider = Provider {
    id: "deepseek",
    base_url: Some("https://api.deepseek.com"),
    key_env: &["DEEPSEEK_API_KEY"],
    prefixes: &["deepseek"],
    example_model: "deepseek-chat",
    formats: &[Speaks::new(Format::OpenAiChat(OpenAiChat))],
    model_id_prefix: None,
    quirks: &[Quirk {
        name: "reasoning_content",
        description: "Reasoning is `reasoning_content`, in a request's messages and a \
                      response's message or delta.",
    }],
    plugin: Some(&DeepSeekQuirks),
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
