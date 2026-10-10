//! OpenRouter: Chat Completions in its dialect, for many vendors' models.

use respan_gateway_conversation_http::openai::{ChatQuirks, OpenAiChat};

use crate::{Format, Provider, Quirk, Speaks};

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
    formats: &[Speaks::new(Format::OpenAiChat(CHAT))],
    model_id_prefix: None,
    quirks: &[Quirk {
        name: "inert_choice",
        description: "A choice's `native_finish_reason`, the upstream provider's own reason, \
                      describes its finish.",
    }],
};

/// OpenRouter's Chat Completions, `openrouter-chat`.
pub const CHAT: OpenAiChat = OpenAiChat::new(
    "openrouter-chat",
    ChatQuirks {
        inert_choice: &["native_finish_reason"],
        ..ChatQuirks::NONE
    },
);
