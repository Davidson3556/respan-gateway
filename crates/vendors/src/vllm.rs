//! vLLM: Chat Completions, as a server one runs serves it.

use respan_gateway_core::{Plugin, ProviderContext, Result, async_trait};

use respan_gateway_conversation_http::openai::OpenAiChat;

use crate::{Quirk, Speaks, Vendor, chat};

/// vLLM. It has no base URL of its own, and takes a key only if its server
/// was started with one.
pub const VENDOR: Vendor = Vendor {
    id: "vllm",
    base_url: None,
    key_env: &[],
    formats: &[&Speaks::http(OpenAiChat)],
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
    plugin: Some(&VllmQuirks),
    ..Vendor::DEFAULT
};

/// vLLM's quirks: reasoning is `reasoning_content`, and a choice's
/// `stop_reason` only describes it.
#[derive(Debug)]
pub struct VllmQuirks;

#[async_trait]
impl Plugin for VllmQuirks {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<()> {
        chat::reasoning_content(px)?;
        chat::inert_choice(px, &["stop_reason"])
    }
}
