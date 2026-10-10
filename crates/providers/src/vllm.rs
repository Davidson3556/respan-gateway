//! vLLM: Chat Completions in its dialect, as a server one runs serves it.

use respan_gateway_conversation_http::openai::{ChatQuirks, OpenAiChat};

use crate::{Format, Provider, Quirk, Speaks};

/// vLLM. It has no base URL of its own, and takes a key only if its server
/// was started with one.
pub const PROVIDER: Provider = Provider {
    id: "vllm",
    base_url: None,
    key_env: &[],
    prefixes: &[],
    example_model: "Qwen/Qwen3-8B",
    formats: &[Speaks::new(Format::OpenAiChat(CHAT))],
    model_id_prefix: None,
    quirks: &[
        Quirk {
            name: "reasoning_content",
            description: "Reasoning is `reasoning_content`, in a request's messages and a \
                          response's message or delta.",
        },
        Quirk {
            name: "inert_choice",
            description: "A choice's `stop_reason`, the stop token or string that ended it, \
                          describes its finish.",
        },
    ],
};

/// vLLM's Chat Completions, `vllm-chat`.
pub const CHAT: OpenAiChat = OpenAiChat::new(
    "vllm-chat",
    ChatQuirks {
        reasoning_content: true,
        inert_choice: &["stop_reason"],
        ..ChatQuirks::NONE
    },
);
