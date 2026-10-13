//! Ollama: Chat Completions and embeddings, as OpenAI serves them, from a local server.

use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_media_http::openai::OpenAiEmbeddings;

use crate::{Speaks, Vendor};

/// Ollama, on this machine. It needs no key; `OLLAMA_HOST` says where it is.
pub const VENDOR: Vendor = Vendor {
    id: "ollama",
    base_url: Some("http://localhost:11434/v1"),
    key_env: &[],
    formats: &[&Speaks::http(OpenAiChat), &Speaks::http(OpenAiEmbeddings)],
    ..Vendor::DEFAULT
};
