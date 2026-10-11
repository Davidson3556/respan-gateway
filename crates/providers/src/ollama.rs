//! Ollama: Chat Completions and embeddings, as OpenAI serves them, from a local server.

use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_media_http::openai::OpenAiEmbeddings;

use crate::{Format, Provider, Speaks};

/// Ollama, on this machine. It needs no key; `OLLAMA_HOST` says where it is.
pub const PROVIDER: Provider = Provider {
    id: "ollama",
    base_url: Some("http://localhost:11434/v1"),
    key_env: &[],
    prefixes: &[],
    example_model: "llama3.2",
    formats: &[
        Speaks::new(Format::OpenAiChat(OpenAiChat)),
        Speaks::new(Format::OpenAiEmbeddings(OpenAiEmbeddings)),
    ],
    model_id_prefix: None,
    quirks: &[],
    plugin: None,
};
