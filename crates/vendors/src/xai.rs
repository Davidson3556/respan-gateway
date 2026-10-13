//! xAI: Chat Completions and images, as OpenAI serves them.

use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_media_http::openai::OpenAiImages;

use crate::{Speaks, Vendor};

/// xAI.
pub const VENDOR: Vendor = Vendor {
    id: "xai",
    base_url: Some("https://api.x.ai/v1"),
    key_env: &["XAI_API_KEY"],
    formats: &[&Speaks::http(OpenAiChat), &Speaks::http(OpenAiImages)],
    ..Vendor::DEFAULT
};
