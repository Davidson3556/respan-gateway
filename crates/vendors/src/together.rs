//! Together: Chat Completions, images and embeddings, as OpenAI serves them.

use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_media_http::openai::{OpenAiEmbeddings, OpenAiImages};

use crate::{Speaks, Vendor};

/// Together.
pub const VENDOR: Vendor = Vendor {
    id: "together",
    base_url: Some("https://api.together.xyz/v1"),
    key_env: &["TOGETHER_API_KEY"],
    formats: &[
        &Speaks::http(OpenAiChat),
        &Speaks::http(OpenAiImages),
        &Speaks::http(OpenAiEmbeddings),
    ],
    ..Vendor::DEFAULT
};
