//! DeepSeek: Chat Completions in its dialect.

use respan_gateway_conversation_http::openai::{ChatQuirks, OpenAiChat};

use crate::{Format, Provider, Quirk, Speaks};

/// DeepSeek.
pub const PROVIDER: Provider = Provider {
    id: "deepseek",
    base_url: Some("https://api.deepseek.com"),
    key_env: &["DEEPSEEK_API_KEY"],
    prefixes: &["deepseek"],
    example_model: "deepseek-chat",
    formats: &[Speaks::new(Format::OpenAiChat(CHAT))],
    model_id_prefix: None,
    quirks: &[Quirk {
        name: "reasoning_content",
        description: "Reasoning is `reasoning_content`, in a request's messages and a \
                      response's message or delta.",
    }],
};

/// DeepSeek's Chat Completions, `deepseek-chat`.
pub const CHAT: OpenAiChat = OpenAiChat::new(
    "deepseek-chat",
    ChatQuirks {
        reasoning_content: true,
        ..ChatQuirks::NONE
    },
);
