//! xAI: Chat Completions and images, as OpenAI serves them.

use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_media_http::openai::OpenAiImages;

use respan_gateway_vendor_kit::Vendor;

/// Where its API is.
const BASE_URL: &str = "https://api.x.ai/v1";

/// xAI.
pub const VENDOR: Vendor = Vendor {
    id: "xai",
    key_env: &["XAI_API_KEY"],
    formats: &[
        (&OpenAiChat, Some(BASE_URL)),
        (&OpenAiImages, Some(BASE_URL)),
    ],
    ..Vendor::DEFAULT
};
