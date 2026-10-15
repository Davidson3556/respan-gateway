//! Together: Chat Completions, images and embeddings, as OpenAI serves them.

use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_media_http::openai::{OpenAiEmbeddings, OpenAiImages};

use respan_gateway_vendor_kit::Vendor;

/// Where its API is.
const BASE_URL: &str = "https://api.together.xyz/v1";

/// Together.
pub const VENDOR: Vendor = Vendor {
    id: "together",
    key_env: &["TOGETHER_API_KEY"],
    formats: &[
        (&OpenAiChat, Some(BASE_URL)),
        (&OpenAiImages, Some(BASE_URL)),
        (&OpenAiEmbeddings, Some(BASE_URL)),
    ],
    ..Vendor::DEFAULT
};
