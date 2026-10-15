//! Mistral: Chat Completions and embeddings, as OpenAI serves them.

use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_media_http::openai::OpenAiEmbeddings;

use respan_gateway_vendor_kit::Vendor;

/// Where its API is.
const BASE_URL: &str = "https://api.mistral.ai/v1";

/// Mistral.
pub const VENDOR: Vendor = Vendor {
    id: "mistral",
    key_env: &["MISTRAL_API_KEY"],
    formats: &[
        (&OpenAiChat, Some(BASE_URL)),
        (&OpenAiEmbeddings, Some(BASE_URL)),
    ],
    ..Vendor::DEFAULT
};
