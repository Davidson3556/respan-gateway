//! Groq: Chat Completions in its dialect, transcriptions and speech.

use respan_gateway_conversation_http::openai::{ChatQuirks, OpenAiChat};
use respan_gateway_media_http::openai::{OpenAiSpeech, OpenAiTranscriptions};

use crate::{Format, Provider, Quirk, Speaks};

/// Groq.
pub const PROVIDER: Provider = Provider {
    id: "groq",
    base_url: Some("https://api.groq.com/openai/v1"),
    key_env: &["GROQ_API_KEY"],
    prefixes: &[
        "whisper-large",
        "distil-whisper",
        "playai-tts",
        "canopylabs/orpheus",
    ],
    example_model: "llama-3.3-70b-versatile",
    formats: &[
        Speaks::new(Format::OpenAiChat(CHAT)),
        Speaks::new(Format::OpenAiTranscriptions(OpenAiTranscriptions)),
        Speaks::new(Format::OpenAiSpeech(OpenAiSpeech)),
    ],
    model_id_prefix: None,
    quirks: &[Quirk {
        name: "usage_under",
        description: "A stream reports usage under `x_groq`, beside the response; the \
                      object's other fields stay native.",
    }],
};

/// Groq's Chat Completions, `groq-chat`.
pub const CHAT: OpenAiChat = OpenAiChat::new(
    "groq-chat",
    ChatQuirks {
        usage_under: Some("x_groq"),
        ..ChatQuirks::NONE
    },
);
