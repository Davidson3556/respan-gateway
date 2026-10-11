//! OpenAI: Chat Completions, Realtime and Responses over WebSocket, and its
//! media endpoints (images, transcriptions, speech, videos, embeddings).

use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_media_http::openai::{
    OpenAiEmbeddings, OpenAiImages, OpenAiSpeech, OpenAiTranscriptions, OpenAiVideos,
};
use respan_gateway_realtime_ws::JsonEvents;

use crate::{Format, Provider, Speaks};

/// OpenAI.
pub const PROVIDER: Provider = Provider {
    id: "openai",
    base_url: Some("https://api.openai.com/v1"),
    key_env: &["OPENAI_API_KEY"],
    prefixes: &[
        "gpt",
        "o1",
        "o3",
        "o4",
        "chatgpt",
        "codex",
        "dall-e",
        "gpt-image",
        "whisper-1",
        "tts-",
        "sora",
        "text-embedding",
    ],
    example_model: "gpt-4o-mini",
    formats: &[
        Speaks::new(Format::OpenAiChat(OpenAiChat)),
        Speaks::new(Format::JsonEvents(JsonEvents::OPENAI_REALTIME)),
        Speaks::new(Format::JsonEvents(JsonEvents::RESPONSES_WS)),
        Speaks::new(Format::OpenAiImages(OpenAiImages)),
        Speaks::new(Format::OpenAiTranscriptions(OpenAiTranscriptions)),
        Speaks::new(Format::OpenAiSpeech(OpenAiSpeech)),
        Speaks::new(Format::OpenAiVideos(OpenAiVideos)),
        Speaks::new(Format::OpenAiEmbeddings(OpenAiEmbeddings)),
    ],
    model_id_prefix: None,
    quirks: &[],
    plugin: None,
};
