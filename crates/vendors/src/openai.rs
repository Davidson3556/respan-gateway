//! OpenAI: Chat Completions, Responses, Realtime and Responses over WebSocket, and its
//! media endpoints (images, transcriptions, speech, embeddings). Its Videos
//! API and Sora models were removed on 2026-09-24, with no replacement; the
//! format stays, as what clients send for any provider of video.

use respan_gateway_conversation::Effort;
use respan_gateway_core::{NativeFields, Plugin, ProviderContext, Result, async_trait};
use respan_gateway_realtime::Input;
use serde_json::{Value, json};

use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_conversation_http::responses::OpenAiResponses;
use respan_gateway_media_http::openai::{
    OpenAiEmbeddings, OpenAiImages, OpenAiSpeech, OpenAiTranscriptions,
};
use respan_gateway_realtime_ws::{OpenAiRealtime, ResponsesWs};

use crate::{Either, Quirk, Speaks, Vendor};

/// The model it transcribes a session's input with, unless the session
/// names one.
const TRANSCRIPTION_MODEL: &str = "gpt-4o-mini-transcribe";

/// OpenAI.
pub const VENDOR: Vendor = Vendor {
    id: "openai",
    base_url: Some("https://api.openai.com/v1"),
    key_env: &["OPENAI_API_KEY"],
    formats: &[
        &Speaks::http(Either::new(OpenAiChat, OpenAiResponses, chat_refuses)),
        &Speaks::http(OpenAiResponses),
        &Speaks::ws(OpenAiRealtime, |base| OpenAiRealtime.transport(base)),
        &Speaks::ws(ResponsesWs, |base| ResponsesWs.transport(base)),
        &Speaks::http(OpenAiImages),
        &Speaks::http(OpenAiTranscriptions),
        &Speaks::http(OpenAiSpeech),
        &Speaks::http(OpenAiEmbeddings),
    ],
    quirks: &[
        Quirk {
            name: "openai_transcription_model",
            description: "Realtime transcribes the user's audio only with a named model: a \
                          session that turns transcription on without one asks for \
                          `gpt-4o-mini-transcribe`.",
        },
        Quirk {
            name: "openai_responses_only",
            description: "Chat Completions refuses function tools with a reasoning effort on \
                          its newer models, and its pro, codex, deep research and computer \
                          use models altogether: such a call, whoever made it, goes to \
                          Responses instead.",
        },
    ],
    plugin: Some(&OpenAiQuirks),
    ..Vendor::DEFAULT
};

/// The models only Responses serves, by a part of their names.
const RESPONSES_ONLY: &[&str] = &["-pro", "codex", "deep-research", "computer-use"];

/// Whether Chat Completions would refuse a call of `input`, which then goes
/// to Responses: one with function tools and a reasoning effort, or for a
/// model only Responses serves; but not one with a field only Chat
/// Completions has.
pub fn chat_refuses(input: &respan_gateway_conversation::Input) -> bool {
    // A field only Chat Completions has (`n`, `logprobs`) would be refused
    // in Responses: such a chat stays, as it was.
    // A tool's `strict` is Responses' too.
    let foreign = |native: &Option<respan_gateway_core::NativeFields>, carried: &[&str]| {
        let carried = |k: &String| k == "inert" || carried.contains(&k.as_str());
        native
            .as_ref()
            .is_some_and(|n| !n.fields.keys().all(carried))
    };
    let tools = input.tools.iter().any(|t| foreign(&t.native, &["strict"]));
    if foreign(&input.native, &[]) || tools {
        return false;
    }
    let reasoning = input.options.reasoning.as_ref().is_some_and(|r| {
        r.effort.as_ref().is_some_and(|e| *e != Effort::Off) || r.budget_tokens.is_some()
    });
    let only = input
        .model()
        .is_some_and(|m| RESPONSES_ONLY.iter().any(|p| m.contains(p)));
    (reasoning && !input.tools.is_empty()) || only
}

/// OpenAI's quirks: a realtime session's input transcription names its
/// model.
#[derive(Debug)]
pub struct OpenAiQuirks;

#[async_trait]
impl Plugin for OpenAiQuirks {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<()> {
        match px.is_kind::<Input>() {
            true => px.filter(transcription_model),
            false => Ok(()),
        }
    }
}

/// A session that turns input transcription on, asking for no model, asks
/// for [`TRANSCRIPTION_MODEL`]. Another format's native fields, which OpenAI
/// drops, go first (and one it refuses is refused here), as they are where
/// the model is asked for.
fn transcription_model(input: &mut Input) -> Result<()> {
    if !matches!(input, Input::Session(s) if s.input_transcription == Some(true)) {
        return Ok(());
    }
    *input = OpenAiRealtime.admit(input)?;
    let Input::Session(session) = input else {
        return Ok(());
    };
    let native = session
        .native
        .get_or_insert_with(|| NativeFields::new(OpenAiRealtime::NAME, Default::default()));
    let mut fields = Value::Object(std::mem::take(&mut native.fields));
    let transcription = &mut fields["session"]["audio"]["input"]["transcription"];
    if transcription.get("model").is_none() {
        transcription["model"] = json!(TRANSCRIPTION_MODEL);
    }
    if let Value::Object(fields) = fields {
        native.fields = fields;
    }
    Ok(())
}
