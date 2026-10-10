//! Google's Gemini API: Chat Completions, images and embeddings at its
//! OpenAI-compatible base, and its own API beside it: Gemini Live and Veo.

use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_media_http::gemini::GeminiVeo;
use respan_gateway_media_http::openai::{OpenAiEmbeddings, OpenAiImages};
use respan_gateway_realtime_ws::{JsonEvents, SessionQuirks};

use crate::{Base, Format, Provider, Quirk, Speaks};

/// The header its own API takes the key in.
const KEY: &str = "x-goog-api-key";

/// Google's Gemini API. Its own API is its OpenAI-compatible base without
/// `/openai`; Live's endpoint is at its origin.
pub const PROVIDER: Provider = Provider {
    id: "gemini",
    base_url: Some("https://generativelanguage.googleapis.com/v1beta/openai"),
    key_env: &["GEMINI_API_KEY", "GOOGLE_API_KEY"],
    prefixes: &["gemini", "imagen", "veo"],
    example_model: "gemini-2.5-flash",
    formats: &[
        Speaks::new(Format::OpenAiChat(OpenAiChat::OPENAI)),
        Speaks::new(Format::JsonEvents(LIVE))
            .key_in(KEY)
            .at(Base::Origin),
        Speaks::new(Format::OpenAiImages(OpenAiImages)),
        Speaks::new(Format::GeminiVeo(GeminiVeo))
            .key_in(KEY)
            .at(Base::Without("/openai")),
        Speaks::new(Format::OpenAiEmbeddings(OpenAiEmbeddings)),
    ],
    model_id_prefix: Some("models/"),
    quirks: &[
        Quirk {
            name: "binary_text",
            description: "Live sends each JSON event in a binary frame, as Google's SDK reads \
                          frames as bytes.",
        },
        Quirk {
            name: "model_id_prefix",
            description: "Its OpenAI-compatible model list names each model `models/<id>`.",
        },
    ],
};

/// Gemini Live, `gemini-live`: at the Live API's endpoint, after the origin
/// Google's SDK takes (`wss://generativelanguage.googleapis.com`).
pub const LIVE: JsonEvents = JsonEvents::from_static(
    "gemini-live",
    "/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent",
    SessionQuirks {
        binary_text: true,
        ..SessionQuirks::NONE
    },
);
