//! A strict fake of Gemini Live over WebSocket, at its endpoint after
//! `ws://{addr}`. It writes every message in a binary frame, as the API does.
//!
//! It refuses an upgrade without a key (`x-goog-api-key` or `?key=`) or
//! with another query parameter, and closes the session 1007, as the API
//! does, for:
//! - a first message that is no `setup`, or a second `setup`; a model without
//!   `models/`; a message of other than one of `setup`, `clientContent`,
//!   `realtimeInput` and `toolResponse`, or a field the API has none of;
//! - audio whose `mimeType` is not `audio/pcm` with a rate, or whose data is
//!   not base64 of whole 16-bit samples;
//! - `activityStart` or `activityEnd` with automatic activity detection on,
//!   and `audioStreamEnd` with it off;
//! - a function response naming no pending call.
//!
//! A turn ends with `turnComplete` in a complete `clientContent`, with
//! `realtimeInput.text`, with `activityEnd`, or, with automatic detection
//! on, at [`Fake::user_stops`]; a tool's answer continues it.
//! [`Fake::user_speaks`] in a held [`Turn`] interrupts it: `interrupted`,
//! then `toolCallCancellation` for its pending calls. Transcripts are sent
//! only if the setup asks for them. [`Fake::go_away`] sends `goAway`.

use std::collections::HashSet;

use serde_json::{Value, json};
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::http::StatusCode;

use super::{Command, Connection, Event, Fake, Turn, Usage, base64, serve, unbase64, words};

/// Where the Live API is served.
pub const PATH: &str =
    "/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent";

/// The fields of a setup.
const SETUP_FIELDS: &[&str] = &[
    "model",
    "generationConfig",
    "systemInstruction",
    "tools",
    "realtimeInputConfig",
    "sessionResumption",
    "contextWindowCompression",
    "inputAudioTranscription",
    "outputAudioTranscription",
    "proactivity",
];

/// The fields of a `realtimeInput`.
const INPUT_FIELDS: &[&str] = &[
    "mediaChunks",
    "audio",
    "video",
    "text",
    "activityStart",
    "activityEnd",
    "audioStreamEnd",
];

/// Bytes of model audio in one message: 100 ms of 24 kHz PCM.
const CHUNK: usize = 4_800;

/// A fake of Gemini Live; its base URL is `ws://{addr}`.
pub async fn start() -> Fake {
    serve("", check, |connection| Box::pin(run(connection))).await
}

/// An upgrade to the Live API's endpoint with a key and no other query.
fn check(request: &Request, _: &mut Response) -> Result<(), (StatusCode, &'static str)> {
    let uri = request.uri();
    if uri.path() != PATH {
        return Err((StatusCode::NOT_FOUND, "Not Found"));
    }
    let query = uri.query().unwrap_or_default();
    let params: Vec<&str> = query.split('&').filter(|p| !p.is_empty()).collect();
    if params.iter().any(|p| !p.starts_with("key=")) {
        return Err((
            StatusCode::BAD_REQUEST,
            "Invalid JSON payload received. Unknown name",
        ));
    }
    let header = request.headers().get("x-goog-api-key");
    if header.is_none_or(|v| v.is_empty()) && params.is_empty() {
        return Err((StatusCode::UNAUTHORIZED, "API key not valid"));
    }
    Ok(())
}

/// A session's state.
struct Session {
    connection: Connection,
    setup: Option<Value>,
    /// Calls the model made that no answer named yet.
    pending: HashSet<String>,
    /// The turn being answered, held open.
    held: Option<Turn>,
}

/// Why a session is closed: the reason of a close 1007.
type Invalid = String;

async fn run(connection: Connection) -> Connection {
    let mut session = Session {
        connection,
        setup: None,
        pending: HashSet::new(),
        held: None,
    };
    loop {
        let result = match session.connection.next().await {
            Event::Frame(message) => session.read(&message).await,
            Event::Command(Command::Speaks) => {
                session.speaks().await;
                Ok(())
            }
            Event::Command(Command::Stops) => {
                if session.automatic() && session.setup.is_some() {
                    session.respond().await;
                }
                Ok(())
            }
            Event::Command(Command::GoAway) => {
                session.send(json!({"goAway": {"timeLeft": "10s"}})).await;
                Ok(())
            }
            Event::Closed => break,
        };
        if let Err(reason) = result {
            session.connection.close(1007, &reason).await;
            break;
        }
    }
    session.connection
}

impl Session {
    async fn send(&mut self, message: Value) {
        self.connection.send(message, true).await;
    }

    /// Whether the server detects the user's activity.
    fn automatic(&self) -> bool {
        let setup = self.setup.as_ref().unwrap_or(&Value::Null);
        setup["realtimeInputConfig"]["automaticActivityDetection"]["disabled"] != true
    }

    /// Whether the setup asks for `transcription`.
    fn transcribes(&self, transcription: &str) -> bool {
        self.setup
            .as_ref()
            .is_some_and(|s| s.get(transcription).is_some())
    }

    async fn read(&mut self, message: &Value) -> Result<(), Invalid> {
        let Some(fields) = message.as_object().filter(|f| f.len() == 1) else {
            return Err("a message holds exactly one of setup, clientContent, realtimeInput and toolResponse".into());
        };
        let (name, body) = fields.iter().next().expect("one field");
        match (name.as_str(), &self.setup) {
            ("setup", None) => self.configure(body).await,
            ("setup", Some(_)) => Err("setup is sent once, first".into()),
            (_, None) => Err("the first message must be setup".into()),
            ("clientContent", _) => {
                if body["turnComplete"] == true {
                    self.respond().await;
                }
                Ok(())
            }
            ("realtimeInput", _) => self.input(body).await,
            ("toolResponse", _) => self.answered(body).await,
            (name, _) => Err(format!(
                "Invalid JSON payload received. Unknown name \"{name}\""
            )),
        }
    }

    async fn configure(&mut self, setup: &Value) -> Result<(), Invalid> {
        let fields = setup.as_object().ok_or("setup must be an object")?;
        if let Some(unknown) = fields.keys().find(|k| !SETUP_FIELDS.contains(&k.as_str())) {
            return Err(format!(
                "Invalid JSON payload received. Unknown name \"{unknown}\" at 'setup'"
            ));
        }
        let model = setup["model"].as_str().unwrap_or_default();
        if !model.starts_with("models/") || model.len() == 7 {
            return Err("setup.model must be models/{model}".into());
        }
        self.setup = Some(setup.clone());
        self.send(json!({"setupComplete": {}})).await;
        Ok(())
    }

    async fn input(&mut self, input: &Value) -> Result<(), Invalid> {
        let fields = input.as_object().ok_or("realtimeInput must be an object")?;
        if let Some(unknown) = fields.keys().find(|k| !INPUT_FIELDS.contains(&k.as_str())) {
            return Err(format!(
                "Invalid JSON payload received. Unknown name \"{unknown}\" at 'realtime_input'"
            ));
        }
        let chunks = input["mediaChunks"].as_array().into_iter().flatten();
        for audio in chunks.chain(input.get("audio")) {
            audio_chunk(audio)?;
        }
        let automatic = self.automatic();
        let activity = fields.contains_key("activityStart") || fields.contains_key("activityEnd");
        if activity && automatic {
            return Err(
                "activityStart and activityEnd need automatic activity detection disabled".into(),
            );
        }
        if fields.contains_key("audioStreamEnd") && !automatic {
            return Err("audioStreamEnd needs automatic activity detection enabled".into());
        }
        if fields.contains_key("activityStart") {
            self.interrupt().await;
        }
        if fields.contains_key("text") || fields.contains_key("activityEnd") {
            self.respond().await;
        }
        Ok(())
    }

    async fn answered(&mut self, response: &Value) -> Result<(), Invalid> {
        let answers = response["functionResponses"]
            .as_array()
            .ok_or("functionResponses must be a list")?;
        for answer in answers {
            let id = answer["id"].as_str().unwrap_or_default();
            if !self.pending.remove(id) {
                return Err(format!("no pending function call has the id {id:?}"));
            }
        }
        if self.pending.is_empty() {
            self.respond().await;
        }
        Ok(())
    }

    /// Voice detection hears speech over a held turn: it is interrupted.
    async fn speaks(&mut self) {
        if self.automatic() {
            self.interrupt().await;
        }
    }

    /// The turn being answered is cut, and its pending calls cancelled.
    async fn interrupt(&mut self) {
        if self.held.take().is_none() && self.pending.is_empty() {
            return;
        }
        self.send(json!({"serverContent": {"interrupted": true}}))
            .await;
        if !self.pending.is_empty() {
            let mut ids: Vec<String> = self.pending.drain().collect();
            ids.sort();
            self.send(json!({"toolCallCancellation": {"ids": ids}}))
                .await;
        }
    }

    /// The model answers with the next turn.
    async fn respond(&mut self) {
        let turn = self.connection.turn();
        if let (true, Some(heard)) = (self.transcribes("inputAudioTranscription"), &turn.heard) {
            self.send(json!({"serverContent": {"inputTranscription": {"text": heard}}}))
                .await;
        }
        let setup = self.setup.clone().unwrap_or_default();
        let modalities = &setup["generationConfig"]["responseModalities"];
        let text = modalities[0] == "TEXT";
        if text {
            for part in turn.text.iter().flat_map(|t| words(t)) {
                self.send(json!({"serverContent": {"modelTurn": {"parts": [{"text": part}]}}}))
                    .await;
            }
        } else {
            for chunk in turn.audio.iter().flat_map(|a| a.chunks(CHUNK)) {
                let data = json!({"mimeType": "audio/pcm;rate=24000", "data": base64(chunk)});
                self.send(
                    json!({"serverContent": {"modelTurn": {"parts": [{"inlineData": data}]}}}),
                )
                .await;
            }
            if self.transcribes("outputAudioTranscription") {
                for part in turn.transcript.iter().flat_map(|t| words(t)) {
                    self.send(json!({"serverContent": {"outputTranscription": {"text": part}}}))
                        .await;
                }
            }
        }
        if let Some(call) = &turn.tool_call {
            let id = self.connection.id("call");
            self.pending.insert(id.clone());
            let call = json!({"id": id, "name": call.name, "args": call.arguments});
            self.send(json!({"toolCall": {"functionCalls": [call]}}))
                .await;
            // The turn goes on once the call is answered.
            return;
        }
        if turn.held {
            self.held = Some(turn);
            return;
        }
        self.send(json!({"serverContent": {"generationComplete": true}}))
            .await;
        let done =
            json!({"serverContent": {"turnComplete": true}, "usageMetadata": usage(turn.usage)});
        self.send(done).await;
        if self
            .setup
            .as_ref()
            .is_some_and(|s| s.get("sessionResumption").is_some())
        {
            let handle = self.connection.id("handle");
            self.send(json!({"sessionResumptionUpdate": {"newHandle": handle, "resumable": true}}))
                .await;
        }
    }
}

/// A chunk of audio as the API takes it: `audio/pcm` at a declared rate,
/// whole 16-bit samples in base64.
fn audio_chunk(blob: &Value) -> Result<(), Invalid> {
    let mime = blob["mimeType"].as_str().unwrap_or_default();
    let rate = mime
        .strip_prefix("audio/pcm;rate=")
        .and_then(|r| r.parse::<u32>().ok());
    if rate.is_none_or(|r| r == 0) {
        return Err(format!(
            "unsupported audio mimeType {mime:?}: audio/pcm;rate=N"
        ));
    }
    let data = blob["data"]
        .as_str()
        .and_then(unbase64)
        .ok_or("audio data must be base64")?;
    if data.len() % 2 != 0 {
        return Err("audio must be whole 16-bit samples".into());
    }
    Ok(())
}

/// A turn's usage, as the API writes it.
fn usage(usage: Usage) -> Value {
    let details = |total: u64, audio: u64| json!([{"modality": "TEXT", "tokenCount": total.saturating_sub(audio)}, {"modality": "AUDIO", "tokenCount": audio}]);
    json!({
        "promptTokenCount": usage.input_tokens,
        "cachedContentTokenCount": usage.cached_tokens,
        "responseTokenCount": usage.output_tokens,
        "totalTokenCount": usage.input_tokens + usage.output_tokens,
        "promptTokensDetails": details(usage.input_tokens, usage.input_audio_tokens),
        "responseTokensDetails": details(usage.output_tokens, usage.output_audio_tokens)
    })
}
