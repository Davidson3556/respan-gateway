//! OpenAI: Chat Completions, Responses, Realtime and Responses over WebSocket, and its
//! media endpoints (images, transcriptions, speech, videos, embeddings).

use respan_gateway_core::{NativeFields, Plugin, ProviderContext, Result, async_trait};
use respan_gateway_realtime::Input;
use serde_json::{Value, json};

use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_conversation_http::responses::OpenAiResponses;
use respan_gateway_media_http::openai::{
    OpenAiEmbeddings, OpenAiImages, OpenAiSpeech, OpenAiTranscriptions, OpenAiVideos,
};
use respan_gateway_realtime_ws::{OpenAiRealtime, ResponsesWs};

use crate::{Quirk, Speaks, Vendor};

/// The model it transcribes a session's input with, unless the session
/// names one.
const TRANSCRIPTION_MODEL: &str = "gpt-4o-mini-transcribe";

/// OpenAI.
pub const VENDOR: Vendor = Vendor {
    id: "openai",
    base_url: Some("https://api.openai.com/v1"),
    key_env: &["OPENAI_API_KEY"],
    formats: &[
        &Speaks::http(OpenAiChat),
        &Speaks::http(OpenAiResponses),
        &Speaks::ws(OpenAiRealtime, |base| OpenAiRealtime.transport(base)),
        &Speaks::ws(ResponsesWs, |base| ResponsesWs.transport(base)),
        &Speaks::http(OpenAiImages),
        &Speaks::http(OpenAiTranscriptions),
        &Speaks::http(OpenAiSpeech),
        &Speaks::http(OpenAiVideos),
        &Speaks::http(OpenAiEmbeddings),
    ],
    quirks: &[Quirk {
        name: "openai_transcription_model",
        description: "Realtime transcribes the user's audio only with a named model: a session \
                      that turns transcription on without one asks for \
                      `gpt-4o-mini-transcribe`.",
    }],
    plugin: Some(&OpenAiQuirks),
    ..Vendor::DEFAULT
};

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
