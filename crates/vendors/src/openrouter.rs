//! OpenRouter: Chat Completions, for many vendors' models.

use respan_gateway_core::{Plugin, ProviderContext, Result, async_trait};

use respan_gateway_conversation_http::openai::OpenAiChat;

use crate::{Quirk, Speaks, Vendor, chat};

/// OpenRouter. Its models' names are `<vendor>/<model>`: a configured
/// provider named `<vendor>` takes the name first.
pub const VENDOR: Vendor = Vendor {
    id: "openrouter",
    base_url: Some("https://openrouter.ai/api/v1"),
    key_env: &["OPENROUTER_API_KEY"],
    formats: &[&Speaks::http(OpenAiChat)],
    quirks: &[Quirk {
        name: "inert_choice",
        description: "A choice's `native_finish_reason`, the upstream provider's own reason, \
                      describes its finish.",
    }],
    plugin: Some(&OpenRouterQuirks),
    ..Vendor::DEFAULT
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
