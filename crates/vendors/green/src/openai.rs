//! OpenAI: Chat Completions, Responses, Realtime and Responses over WebSocket, and its
//! media endpoints (images, transcriptions, speech, embeddings). Its Videos
//! API and Sora models were removed on 2026-09-24, with no replacement; the
//! format stays, as what clients send for any provider of video.

use respan_gateway_conversation::Effort;

use respan_gateway_conversation_http::Schema;
use respan_gateway_conversation_http::either::Either;
use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_conversation_http::responses::OpenAiResponses;
use respan_gateway_media_http::openai::{
    OpenAiEmbeddings, OpenAiImages, OpenAiSpeech, OpenAiTranscriptions,
};
use respan_gateway_realtime_ws::{OpenAiRealtime, ResponsesWs};

use respan_gateway_vendor_kit::Vendor;

/// Where its API is.
const BASE_URL: &str = "https://api.openai.com/v1";

/// OpenAI.
pub const VENDOR: Vendor = Vendor {
    id: "openai",
    key_env: &["OPENAI_API_KEY"],
    formats: &[
        (
            &Either::new(OpenAiChat, OpenAiResponses, chat_refuses),
            Some(BASE_URL),
        ),
        (&OpenAiResponses, Some(BASE_URL)),
        (&OpenAiRealtime, Some(BASE_URL)),
        (&ResponsesWs, Some(BASE_URL)),
        (&OpenAiImages, Some(BASE_URL)),
        (&OpenAiTranscriptions, Some(BASE_URL)),
        (&OpenAiSpeech, Some(BASE_URL)),
        (&OpenAiEmbeddings, Some(BASE_URL)),
    ],
    ..Vendor::DEFAULT
};

/// The models only Responses serves, by a part of their names.
const RESPONSES_ONLY: &[&str] = &["-pro", "codex", "deep-research", "computer-use"];

/// Whether a model reasons when no effort is asked for, as Chat Completions
/// counts it against function tools: gpt-5.6 and later.
fn reasons_by_default(model: &str) -> bool {
    let Some(version) = model.strip_prefix("gpt-") else {
        return false;
    };
    let version = version.split('-').next().unwrap_or_default();
    let (major, minor) = version.split_once('.').unwrap_or((version, "0"));
    match (major.parse::<u32>(), minor.parse::<u32>()) {
        (Ok(major), Ok(minor)) => (major, minor) >= (5, 6),
        _ => false,
    }
}

/// Whether a call of `input` goes to Responses: one Chat Completions
/// refuses (function tools with reasoning, which gpt-5.6 and later do
/// unless it is turned off, or a model only
/// Responses serves) and Responses can write, which a field only Chat
/// Completions has (`n`, `logprobs`) keeps from it.
pub fn chat_refuses(input: &respan_gateway_conversation::Input) -> bool {
    let model = input.model().unwrap_or_default();
    let reasoning = match input.options.reasoning.as_ref() {
        Some(r) if r.budget_tokens.is_some() => true,
        Some(r) if r.effort.is_some() => r.effort != Some(Effort::Off),
        // None asked for: what the model does unless told.
        _ => reasons_by_default(model),
    };
    let only = RESPONSES_ONLY.iter().any(|p| model.contains(p));
    let refused = (reasoning && !input.tools.is_empty()) || only;
    refused && OpenAiResponses.encode_request(input).is_ok()
}
