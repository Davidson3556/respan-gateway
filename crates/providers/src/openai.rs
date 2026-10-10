//! OpenAI: Chat Completions, Realtime and Responses over WebSocket, and its
//! media endpoints (images, transcriptions, speech, videos, embeddings).

use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_media_http::openai::{
    OpenAiEmbeddings, OpenAiImages, OpenAiSpeech, OpenAiTranscriptions, OpenAiVideos,
};
use respan_gateway_realtime_ws::{JsonEvents, SessionQuirks};

use crate::{Format, Provider, Quirk, Speaks};

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
        Speaks::new(Format::OpenAiChat(OpenAiChat::OPENAI)),
        Speaks::new(Format::JsonEvents(REALTIME)),
        Speaks::new(Format::JsonEvents(RESPONSES_WS)),
        Speaks::new(Format::OpenAiImages(OpenAiImages)),
        Speaks::new(Format::OpenAiTranscriptions(OpenAiTranscriptions)),
        Speaks::new(Format::OpenAiSpeech(OpenAiSpeech)),
        Speaks::new(Format::OpenAiVideos(OpenAiVideos)),
        Speaks::new(Format::OpenAiEmbeddings(OpenAiEmbeddings)),
    ],
    model_id_prefix: None,
    quirks: &[Quirk {
        name: "protocol",
        description: "Realtime speaks the subprotocol `realtime`, which its browser clients \
                      offer beside their key; a browser that offered one refuses a `101` that \
                      names none.",
    }],
};

/// OpenAI Realtime, `openai-realtime`: at `/realtime`, after the base URL
/// its SDKs take (`wss://api.openai.com/v1`).
pub const REALTIME: JsonEvents = JsonEvents::from_static(
    "openai-realtime",
    "/realtime",
    SessionQuirks {
        protocol: Some("realtime"),
        ..SessionQuirks::NONE
    },
);

/// Responses over WebSocket, `responses-ws`: at `/responses`, as for HTTP.
/// Only an upgrade is this format's: a `POST` there is no session.
pub const RESPONSES_WS: JsonEvents =
    JsonEvents::from_static("responses-ws", "/responses", SessionQuirks::NONE);
