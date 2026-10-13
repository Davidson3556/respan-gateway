//! Mistral: Chat Completions and embeddings, as OpenAI serves them.

use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_media_http::openai::OpenAiEmbeddings;

use crate::{Speaks, Vendor};

/// Mistral.
pub const VENDOR: Vendor = Vendor {
    id: "mistral",
    base_url: Some("https://api.mistral.ai/v1"),
    key_env: &["MISTRAL_API_KEY"],
    formats: &[&Speaks::http(OpenAiChat), &Speaks::http(OpenAiEmbeddings)],
    ..Vendor::DEFAULT
};
