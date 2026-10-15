//! Ollama: Chat Completions and embeddings, as OpenAI serves them, from a local server.

use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_media_http::openai::OpenAiEmbeddings;

use respan_gateway_vendor_kit::Vendor;

/// Where its API is.
const BASE_URL: &str = "http://localhost:11434/v1";

/// Ollama, on this machine. It needs no key; `OLLAMA_HOST` says where it is.
pub const VENDOR: Vendor = Vendor {
    id: "ollama",
    key_env: &[],
    formats: &[
        (&OpenAiChat, Some(BASE_URL)),
        (&OpenAiEmbeddings, Some(BASE_URL)),
    ],
    ..Vendor::DEFAULT
};
