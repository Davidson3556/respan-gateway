//! OpenRouter: Chat Completions, for many vendors' models.

use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_core::{Plugin, ProviderContext, Result, async_trait};

use crate::{Format, Provider, Quirk, Speaks, chat};

/// OpenRouter. Its models' names are `<vendor>/<model>`: a configured
/// provider named `<vendor>` takes the name first.
pub const PROVIDER: Provider = Provider {
    id: "openrouter",
    base_url: Some("https://openrouter.ai/api/v1"),
    key_env: &["OPENROUTER_API_KEY"],
    prefixes: &[
        "openai/",
        "anthropic/",
        "google/",
        "meta-llama/",
        "mistralai/",
        "qwen/",
        "deepseek/",
        "x-ai/",
        "moonshotai/",
        "cohere/",
        "nvidia/",
        "microsoft/",
        "amazon/",
        "perplexity/",
    ],
    example_model: "openai/gpt-4o-mini",
    formats: &[Speaks::new(Format::OpenAiChat(OpenAiChat))],
    model_id_prefix: None,
    quirks: &[Quirk {
        name: "inert_choice",
        description: "A choice's `native_finish_reason`, the upstream provider's own reason, \
                      describes its finish.",
    }],
    plugin: Some(&OpenRouterQuirks),
};

/// OpenRouter's quirks: a choice's `native_finish_reason` only describes
/// it.
#[derive(Debug)]
pub struct OpenRouterQuirks;

#[async_trait]
impl Plugin for OpenRouterQuirks {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<()> {
        chat::inert_choice(px, &["native_finish_reason"])
    }
}
