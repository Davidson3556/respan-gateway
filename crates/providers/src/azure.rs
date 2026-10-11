//! Azure OpenAI: Chat Completions, at a resource's own URL
//! (`https://<resource>.openai.azure.com/openai/v1`).

use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_core::{Plugin, ProviderContext, Result, async_trait};

use crate::{Format, Provider, Quirk, Speaks, chat};

/// Azure OpenAI. It has no base URL of its own: a resource has one.
pub const PROVIDER: Provider = Provider {
    id: "azure",
    base_url: None,
    key_env: &["AZURE_OPENAI_API_KEY"],
    prefixes: &[],
    example_model: "gpt-4o-mini",
    formats: &[Speaks::new(Format::OpenAiChat(OpenAiChat)).key_in("api-key")],
    model_id_prefix: None,
    quirks: &[Quirk {
        name: "inert_choice",
        description: "A choice's `content_filter_results` and `content_filter_offsets` \
                      describe the moderation it passed.",
    }],
    plugin: Some(&AzureQuirks),
};

/// Azure's quirks: a choice's moderation results only describe it.
#[derive(Debug)]
pub struct AzureQuirks;

#[async_trait]
impl Plugin for AzureQuirks {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<()> {
        chat::inert_choice(px, &["content_filter_results", "content_filter_offsets"])
    }
}
