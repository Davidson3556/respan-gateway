//! Mistral: Chat Completions and embeddings, as OpenAI serves them.

use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_media_http::openai::OpenAiEmbeddings;

use crate::{Format, Speaks, Vendor};

/// Mistral.
pub const VENDOR: Vendor = Vendor {
    id: "mistral",
    base_url: Some("https://api.mistral.ai/v1"),
    key_env: &["MISTRAL_API_KEY"],
    example_model: "mistral-small-latest",
    formats: &[
        Speaks::new(Format::OpenAiChat(OpenAiChat)),
        Speaks::new(Format::OpenAiEmbeddings(OpenAiEmbeddings)),
    ],
    model_id_prefix: None,
    quirks: &[],
    plugin: None,
};
