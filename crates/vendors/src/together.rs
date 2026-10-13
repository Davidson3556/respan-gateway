//! Together: Chat Completions, images and embeddings, as OpenAI serves them.

use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_media_http::openai::{OpenAiEmbeddings, OpenAiImages};

use crate::{Format, Speaks, Vendor};

/// Together.
pub const VENDOR: Vendor = Vendor {
    id: "together",
    base_url: Some("https://api.together.xyz/v1"),
    key_env: &["TOGETHER_API_KEY"],
    example_model: "meta-llama/Llama-3.3-70B-Instruct-Turbo",
    formats: &[
        Speaks::new(Format::OpenAiChat(OpenAiChat)),
        Speaks::new(Format::OpenAiImages(OpenAiImages)),
        Speaks::new(Format::OpenAiEmbeddings(OpenAiEmbeddings)),
    ],
    model_id_prefix: None,
    quirks: &[],
    plugin: None,
};
