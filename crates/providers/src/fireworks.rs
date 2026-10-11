//! Fireworks: Chat Completions and embeddings, as OpenAI serves them.

use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_media_http::openai::OpenAiEmbeddings;

use crate::{Format, Provider, Speaks};

/// Fireworks.
pub const PROVIDER: Provider = Provider {
    id: "fireworks",
    base_url: Some("https://api.fireworks.ai/inference/v1"),
    key_env: &["FIREWORKS_API_KEY"],
    prefixes: &["accounts/"],
    example_model: "accounts/fireworks/models/llama-v3p3-70b-instruct",
    formats: &[
        Speaks::new(Format::OpenAiChat(OpenAiChat)),
        Speaks::new(Format::OpenAiEmbeddings(OpenAiEmbeddings)),
    ],
    model_id_prefix: None,
    quirks: &[],
    plugin: None,
};
