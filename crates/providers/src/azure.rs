//! Azure OpenAI: Chat Completions in its dialect, at a resource's own URL
//! (`https://<resource>.openai.azure.com/openai/v1`).

use respan_gateway_conversation_http::openai::{ChatQuirks, OpenAiChat};

use crate::{Format, Provider, Quirk, Speaks};

/// Azure OpenAI. It has no base URL of its own: a resource has one.
pub const PROVIDER: Provider = Provider {
    id: "azure",
    base_url: None,
    key_env: &["AZURE_OPENAI_API_KEY"],
    prefixes: &[],
    example_model: "gpt-4o-mini",
    formats: &[Speaks::new(Format::OpenAiChat(CHAT)).key_in("api-key")],
    model_id_prefix: None,
    quirks: &[Quirk {
        name: "inert_choice",
        description: "A choice's `content_filter_results` and `content_filter_offsets` \
                      describe the moderation it passed.",
    }],
};

/// Azure OpenAI's Chat Completions, `azure-chat`.
pub const CHAT: OpenAiChat = OpenAiChat::new(
    "azure-chat",
    ChatQuirks {
        inert_choice: &["content_filter_results", "content_filter_offsets"],
        ..ChatQuirks::NONE
    },
);
