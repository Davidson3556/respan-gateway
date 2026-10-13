//! Fireworks: Chat Completions and embeddings, as OpenAI serves them.

use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_media_http::openai::OpenAiEmbeddings;

use crate::{Speaks, Vendor};

/// Fireworks.
pub const VENDOR: Vendor = Vendor {
    id: "fireworks",
    base_url: Some("https://api.fireworks.ai/inference/v1"),
    key_env: &["FIREWORKS_API_KEY"],
    formats: &[&Speaks::http(OpenAiChat), &Speaks::http(OpenAiEmbeddings)],
    ..Vendor::DEFAULT
};
