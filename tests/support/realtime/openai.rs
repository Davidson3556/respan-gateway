//! A strict fake of OpenAI Realtime (GA) over WebSocket, at
//! `/v1/realtime?model=..`.
//!
//! It refuses an upgrade without a model or a key, and answers, as the API
//! does, with an `error` event and the session going on:
//! - an event the GA API has no client event of (a beta one included), and
//!   a session field it has none of (`modalities`, `voice`, ...);
//! - a `session.update` without `session.type`, or one that changes the
//!   model; a transcription without its model, and a tool whose schema
//!   names a type JSON Schema has not, such as Gemini's `OBJECT`;
//! - PCM input or output at a rate but 24 kHz;
//! - audio that is not base64, or not whole samples of the session's input
//!   format; a commit of less than 100 ms of it;
//! - a `response.create` while a response is active, a `response.cancel`
//!   while none is;
//! - a `function_call_output` naming no call it made, a `truncate` naming no
//!   assistant message, and an `output_audio_buffer.clear`, which is WebRTC's.
//!
//! Its voice detection is the test's: [`Fake::user_speaks`] starts speech,
//! cancelling an active response if `interrupt_response` is on (a held
//! [`Turn`]); [`Fake::user_stops`] commits the audio and answers if
//! `create_response` is on. Neither does anything with turn detection off.
//! [`Fake::go_away`] closes the session, 1001.

use std::collections::HashSet;

use serde_json::{Value, json};
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::http::{HeaderValue, StatusCode};

use super::{Command, Connection, Event, Fake, Turn, Usage, base64, serve, unbase64, words};

/// The fields of a GA session.
const SESSION_FIELDS: &[&str] = &[
    "type",
    "model",
    "instructions",
    "output_modalities",
    "audio",
    "tools",
    "tool_choice",
    "max_output_tokens",
    "tracing",
    "truncation",
    "prompt",
    "include",
];

/// The audio formats of a GA session.
const FORMATS: &[&str] = &["audio/pcm", "audio/pcmu", "audio/pcma"];

/// Bytes of model audio in one delta: 100 ms of 24 kHz PCM.
const CHUNK: usize = 4_800;

/// A fake of OpenAI Realtime; its base URL is `ws://{addr}/v1`.
pub async fn start() -> Fake {
    serve("/v1", check, |connection| Box::pin(run(connection))).await
}

/// An upgrade to `/v1/realtime` with a model and a key, given the
/// subprotocol `realtime` if it offered it.
fn check(request: &Request, response: &mut Response) -> Result<(), (StatusCode, &'static str)> {
    let uri = request.uri();
    if uri.path() != "/v1/realtime" {
        return Err((StatusCode::NOT_FOUND, "Invalid URL"));
    }
    let query = uri.query().unwrap_or_default();
    if !query
        .split('&')
        .any(|p| p.starts_with("model=") && p.len() > 6)
    {
        return Err((StatusCode::BAD_REQUEST, "missing_model"));
    }
    let headers = request.headers();
    let offers: Vec<&str> = headers
        .get_all("sec-websocket-protocol")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(str::trim)
        .collect();
    let bearer = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("Bearer ") && v.len() > 7);
    let keyed = offers
        .iter()
        .any(|o| o.starts_with("openai-insecure-api-key."));
    if !bearer && !keyed {
        return Err((StatusCode::UNAUTHORIZED, "invalid_api_key"));
    }
    if offers.contains(&"realtime") {
        let protocol = HeaderValue::from_static("realtime");
        response
            .headers_mut()
            .insert("sec-websocket-protocol", protocol);
    }
    Ok(())
}

/// A response being made.
struct Active {
    id: String,
    turn: Turn,
    /// Its message item, if it says anything.
    item: Option<String>,
}

/// A session's state.
struct Session {
    connection: Connection,
    model: String,
    config: Value,
    /// Uncommitted input audio.
    buffer: Vec<u8>,
    /// The input timeline, in ms.
    audio_ms: u64,
    /// The conversation's items, in order.
    items: Vec<Value>,
    /// The ids of the calls the model made.
    calls: HashSet<String>,
    /// The item the user's speech becomes, once voice detection heard it.
    speech: Option<String>,
    active: Option<Active>,
}

async fn run(connection: Connection) -> Connection {
    let handshake = connection.handshake();
    let model = handshake.query("model").unwrap_or_default().to_owned();
    let mut session = Session {
        connection,
        config: initial(&model),
        model,
        buffer: Vec::new(),
        audio_ms: 0,
        items: Vec::new(),
        calls: HashSet::new(),
        speech: None,
        active: None,
    };
    let created = json!({"type": "session.created", "session": session.config});
    session.emit(created).await;
    loop {
        match session.connection.next().await {
            Event::Frame(event) => session.read(event).await,
            Event::Command(Command::Speaks) => session.speaks().await,
            Event::Command(Command::Stops) => session.stops().await,
            Event::Command(Command::GoAway) => {
                session.connection.close(1001, "going away").await;
                break;
            }
            Event::Closed => break,
        }
    }
    session.connection
}

/// A session as the API opens it, of `model`.
fn initial(model: &str) -> Value {
    let format = json!({"type": "audio/pcm", "rate": 24_000});
    json!({
        "type": "realtime",
        "object": "realtime.session",
        "id": "sess_fake",
        "model": model,
        "output_modalities": ["audio"],
        "instructions": "",
        "tools": [],
        "tool_choice": "auto",
        "max_output_tokens": "inf",
        "tracing": null,
        "truncation": "auto",
        "prompt": null,
        "include": null,
        "audio": {
            "input": {
                "format": format,
                "transcription": null,
                "noise_reduction": null,
                "turn_detection": {
                    "type": "server_vad",
                    "threshold": 0.5,
                    "prefix_padding_ms": 300,
                    "silence_duration_ms": 200,
                    "idle_timeout_ms": null,
                    "create_response": true,
                    "interrupt_response": true
                }
            },
            "output": {"format": format, "voice": "alloy", "speed": 1.0}
        }
    })
}

/// `patch` merged into `target`: objects field by field, anything else
/// replaced.
fn merge(target: &mut Value, patch: &Value) {
    match (target, patch) {
        (Value::Object(target), Value::Object(patch)) => {
            for (key, value) in patch {
                merge(target.entry(key.clone()).or_insert(Value::Null), value);
            }
        }
        (target, patch) => *target = patch.clone(),
    }
}

/// A type a schema names that JSON Schema has not, such as Gemini's
/// `OBJECT`.
fn upper_type(schema: &Value) -> Option<String> {
    match schema {
        Value::Object(fields) => fields.iter().find_map(|(key, value)| match value {
            Value::String(t) if key == "type" && t.chars().any(|c| c.is_ascii_uppercase()) => {
                Some(t.clone())
            }
            value => upper_type(value),
        }),
        Value::Array(values) => values.iter().find_map(upper_type),
        _ => None,
    }
}

/// A refusal: the API's `error` event's `code` and `message`.
type Refusal = (&'static str, String);

impl Session {
    /// Write `event` with an event id.
    async fn emit(&mut self, mut event: Value) {
        event["event_id"] = self.connection.id("event").into();
        self.connection.send(event, false).await;
    }

    async fn read(&mut self, event: Value) {
        let client_id = event.get("event_id").cloned().unwrap_or(Value::Null);
        if let Err((code, message)) = self.handle(&event).await {
            let error = json!({
                "type": "error",
                "error": {
                    "type": "invalid_request_error",
                    "code": code,
                    "message": message,
                    "param": null,
                    "event_id": client_id,
                }
            });
            self.emit(error).await;
        }
    }

    async fn handle(&mut self, event: &Value) -> Result<(), Refusal> {
        let kind = event["type"].as_str().unwrap_or_default();
        match kind {
            "session.update" => self.update(&event["session"]).await,
            "input_audio_buffer.append" => self.append(&event["audio"]),
            "input_audio_buffer.commit" => self.commit().await,
            "input_audio_buffer.clear" => {
                self.buffer.clear();
                self.emit(json!({"type": "input_audio_buffer.cleared"}))
                    .await;
                Ok(())
            }
            "conversation.item.create" => self.create(&event["item"]).await,
            "conversation.item.truncate" => self.truncate(event).await,
            "conversation.item.delete" => {
                let id = self.item(&event["item_id"])?["id"].clone();
                self.items.retain(|item| item["id"] != id);
                self.emit(json!({"type": "conversation.item.deleted", "item_id": id}))
                    .await;
                Ok(())
            }
            "conversation.item.retrieve" => {
                let item = self.item(&event["item_id"])?.clone();
                self.emit(json!({"type": "conversation.item.retrieved", "item": item}))
                    .await;
                Ok(())
            }
            "response.create" => match self.active {
                Some(_) => Err((
                    "conversation_already_has_active_response",
                    "Conversation already has an active response in progress".to_owned(),
                )),
                None => {
                    self.respond().await;
                    Ok(())
                }
            },
            "response.cancel" => match self.active {
                Some(_) => {
                    self.finish("cancelled", Some("client_cancelled")).await;
                    Ok(())
                }
                None => Err((
                    "response_cancel_not_active",
                    "Cancellation failed: no active response found".to_owned(),
                )),
            },
            "output_audio_buffer.clear" => Err((
                "invalid_value",
                "output_audio_buffer.clear is only supported for WebRTC and SIP".to_owned(),
            )),
            _ => Err((
                "invalid_value",
                format!("Invalid value: '{kind}'. Supported values are GA client events."),
            )),
        }
    }

    async fn update(&mut self, patch: &Value) -> Result<(), Refusal> {
        let Some(fields) = patch.as_object() else {
            return Err(("invalid_type", "session must be an object".to_owned()));
        };
        if patch["type"] != "realtime" {
            return Err((
                "missing_required_parameter",
                "Missing required parameter: 'session.type'.".to_owned(),
            ));
        }
        if let Some(unknown) = fields
            .keys()
            .find(|k| !SESSION_FIELDS.contains(&k.as_str()))
        {
            return Err((
                "unknown_parameter",
                format!("Unknown parameter: 'session.{unknown}'."),
            ));
        }
        if fields.get("model").is_some_and(|m| *m != *self.model) {
            return Err((
                "invalid_value",
                "session.update cannot change the model".to_owned(),
            ));
        }
        for side in ["input", "output"] {
            let format = &patch["audio"][side]["format"];
            if format.is_null() {
                continue;
            }
            let kind = format["type"].as_str().unwrap_or_default();
            let rate = format.get("rate").and_then(Value::as_u64);
            if !FORMATS.contains(&kind)
                || (kind == "audio/pcm" && rate.is_some_and(|r| r != 24_000))
            {
                return Err((
                    "invalid_value",
                    format!("Invalid audio.{side}.format: only a 24kHz sample rate is supported"),
                ));
            }
        }
        let transcription = &patch["audio"]["input"]["transcription"];
        if transcription.is_object() && transcription.get("model").is_none() {
            return Err((
                "missing_required_parameter",
                "Missing required parameter: 'session.audio.input.transcription.model'.".to_owned(),
            ));
        }
        for tool in patch["tools"].as_array().into_iter().flatten() {
            if let Some(t) = upper_type(&tool["parameters"]) {
                let name = tool["name"].as_str().unwrap_or_default();
                return Err((
                    "invalid_function_parameters",
                    format!(
                        "Invalid schema for function '{name}': '{t}' is not valid under any of the given schemas."
                    ),
                ));
            }
        }
        merge(&mut self.config, patch);
        let updated = json!({"type": "session.updated", "session": self.config});
        self.emit(updated).await;
        Ok(())
    }

    /// The bytes of a sample of the session's input format.
    fn sample_bytes(&self) -> usize {
        match self.config["audio"]["input"]["format"]["type"] == "audio/pcm" {
            true => 2,
            false => 1,
        }
    }

    /// How many ms `bytes` of the session's input format play.
    fn ms(&self, bytes: usize) -> u64 {
        // 24 kHz PCM is 48 bytes a ms; 8 kHz G.711, 8.
        let per_ms = match self.sample_bytes() {
            2 => 48,
            _ => 8,
        };
        bytes as u64 / per_ms
    }

    fn append(&mut self, audio: &Value) -> Result<(), Refusal> {
        let invalid = || ("invalid_value", "audio must be base64".to_owned());
        let bytes = unbase64(audio.as_str().ok_or_else(invalid)?).ok_or_else(invalid)?;
        if bytes.len() % self.sample_bytes() != 0 {
            return Err((
                "invalid_value",
                "audio must be whole samples of the session's input format".to_owned(),
            ));
        }
        self.buffer.extend(bytes);
        Ok(())
    }

    fn turn_detection(&self) -> &Value {
        &self.config["audio"]["input"]["turn_detection"]
    }

    async fn commit(&mut self) -> Result<(), Refusal> {
        let ms = self.ms(self.buffer.len());
        if ms < 100 {
            return Err((
                "input_audio_buffer_commit_empty",
                format!(
                    "buffer too small. Expected at least 100ms of audio, but buffer only has {ms}ms of audio."
                ),
            ));
        }
        self.committed().await;
        Ok(())
    }

    /// The buffer as a user's item, transcribed if the session says so.
    async fn committed(&mut self) {
        self.audio_ms += self.ms(self.buffer.len());
        self.buffer.clear();
        let id = match self.speech.take() {
            Some(id) => id,
            None => self.connection.id("item"),
        };
        let previous = self.items.last().map(|i| i["id"].clone());
        self.emit(json!({"type": "input_audio_buffer.committed", "previous_item_id": previous, "item_id": id}))
            .await;
        let item = json!({
            "id": id, "object": "realtime.item", "type": "message", "status": "completed",
            "role": "user", "content": [{"type": "input_audio", "transcript": null}]
        });
        self.add(item).await;
        let transcribed = !self.config["audio"]["input"]["transcription"].is_null();
        if let (true, Some(heard)) = (transcribed, self.connection.heard()) {
            for delta in words(&heard) {
                let delta = json!({"type": "conversation.item.input_audio_transcription.delta",
                    "item_id": id, "content_index": 0, "delta": delta});
                self.emit(delta).await;
            }
            let done = json!({"type": "conversation.item.input_audio_transcription.completed",
                "item_id": id, "content_index": 0, "transcript": heard});
            self.emit(done).await;
        }
    }

    /// Add `item` to the conversation.
    async fn add(&mut self, item: Value) {
        let previous = self.items.last().map(|i| i["id"].clone());
        self.items.push(item.clone());
        let added =
            json!({"type": "conversation.item.added", "previous_item_id": previous, "item": item});
        self.emit(added).await;
        self.emit(json!({"type": "conversation.item.done", "item": item}))
            .await;
    }

    fn item(&self, id: &Value) -> Result<&Value, Refusal> {
        let found = self.items.iter().find(|item| item["id"] == *id);
        found.ok_or((
            "item_not_found",
            format!("Item with item_id not found: {id}"),
        ))
    }

    async fn create(&mut self, item: &Value) -> Result<(), Refusal> {
        let mut item = item.clone();
        match item["type"].as_str().unwrap_or_default() {
            "message" => {
                let allowed: &[&str] = match item["role"].as_str().unwrap_or_default() {
                    "user" => &["input_text", "input_audio", "input_image"],
                    "system" => &["input_text"],
                    "assistant" => &["output_text", "output_audio"],
                    _ => return Err(("invalid_value", "Invalid value for 'item.role'".to_owned())),
                };
                let content = item["content"].as_array().cloned().unwrap_or_default();
                if content
                    .iter()
                    .any(|part| !allowed.contains(&part["type"].as_str().unwrap_or_default()))
                {
                    return Err((
                        "invalid_value",
                        "Invalid content type for the role".to_owned(),
                    ));
                }
            }
            "function_call_output" => {
                let call = item["call_id"].as_str().unwrap_or_default();
                if !self.calls.contains(call) {
                    return Err((
                        "invalid_value",
                        format!("No tool call found for function call output with call_id {call}."),
                    ));
                }
            }
            "function_call" => {
                let call = item["call_id"].as_str().unwrap_or_default().to_owned();
                self.calls.insert(call);
            }
            _ => return Err(("invalid_value", "Invalid value for 'item.type'".to_owned())),
        }
        if item.get("id").is_none() {
            item["id"] = self.connection.id("item").into();
        }
        item["object"] = "realtime.item".into();
        item["status"] = "completed".into();
        self.add(item).await;
        Ok(())
    }

    async fn truncate(&mut self, event: &Value) -> Result<(), Refusal> {
        let item = self.item(&event["item_id"])?;
        if item["type"] != "message" || item["role"] != "assistant" {
            return Err((
                "invalid_value",
                "Only assistant messages can be truncated".to_owned(),
            ));
        }
        let truncated = json!({"type": "conversation.item.truncated", "item_id": event["item_id"],
            "content_index": event["content_index"], "audio_end_ms": event["audio_end_ms"]});
        self.emit(truncated).await;
        Ok(())
    }

    /// Voice detection hears speech: an active response is cancelled if the
    /// session says so.
    async fn speaks(&mut self) {
        let detection = self.turn_detection().clone();
        if detection.is_null() {
            return;
        }
        let item = self.connection.id("item");
        self.speech = Some(item.clone());
        let started = json!({"type": "input_audio_buffer.speech_started",
            "audio_start_ms": self.audio_ms, "item_id": item});
        self.emit(started).await;
        if self.active.is_some() && detection["interrupt_response"] != false {
            self.finish("cancelled", Some("turn_detected")).await;
        }
    }

    /// Voice detection hears the speech end: the audio is the user's turn,
    /// answered if the session says so.
    async fn stops(&mut self) {
        let detection = self.turn_detection().clone();
        if detection.is_null() {
            return;
        }
        let end = self.audio_ms + self.ms(self.buffer.len());
        let item = match &self.speech {
            Some(item) => item.clone(),
            None => self.connection.id("item"),
        };
        self.speech = Some(item.clone());
        let stopped = json!({"type": "input_audio_buffer.speech_stopped",
            "audio_end_ms": end, "item_id": item});
        self.emit(stopped).await;
        self.committed().await;
        if detection["create_response"] != false && self.active.is_none() {
            self.respond().await;
        }
    }

    /// A response saying the next turn.
    async fn respond(&mut self) {
        let id = self.connection.id("resp");
        let turn = self.connection.turn();
        let response = json!({"object": "realtime.response", "id": id, "status": "in_progress",
            "status_details": null, "output": [], "usage": null});
        self.emit(json!({"type": "response.created", "response": response}))
            .await;
        let audio = self.config["output_modalities"][0] == "audio";
        let says = match audio {
            true => turn.audio.is_some() || turn.transcript.is_some(),
            false => turn.text.is_some(),
        };
        let mut item = None;
        if says {
            let item_id = self.connection.id("item");
            let message = json!({"id": item_id, "object": "realtime.item", "type": "message",
                "status": "in_progress", "role": "assistant", "content": []});
            self.emit(
                json!({"type": "response.output_item.added", "response_id": id,
                "output_index": 0, "item": message}),
            )
            .await;
            let previous = self.items.last().map(|i| i["id"].clone());
            self.items.push(message.clone());
            self.emit(json!({"type": "conversation.item.added", "previous_item_id": previous, "item": message}))
                .await;
            let part = match audio {
                true => json!({"type": "audio", "transcript": ""}),
                false => json!({"type": "text", "text": ""}),
            };
            let at = json!({"response_id": id, "item_id": item_id, "output_index": 0, "content_index": 0});
            let event = |kind: &str, fields: Value| {
                let mut event = at.clone();
                event["type"] = kind.into();
                merge(&mut event, &fields);
                event
            };
            self.emit(event("response.content_part.added", json!({"part": part})))
                .await;
            if audio {
                for chunk in turn.audio.iter().flat_map(|a| a.chunks(CHUNK)) {
                    let delta = event(
                        "response.output_audio.delta",
                        json!({"delta": base64(chunk)}),
                    );
                    self.emit(delta).await;
                }
                for delta in turn.transcript.iter().flat_map(|t| words(t)) {
                    let delta = event(
                        "response.output_audio_transcript.delta",
                        json!({"delta": delta}),
                    );
                    self.emit(delta).await;
                }
            } else {
                for delta in turn.text.iter().flat_map(|t| words(t)) {
                    let delta = event("response.output_text.delta", json!({"delta": delta}));
                    self.emit(delta).await;
                }
            }
            item = Some(item_id);
        }
        let held = turn.held;
        self.active = Some(Active { id, turn, item });
        if !held {
            self.finish("completed", None).await;
        }
    }

    /// End the active response: its message's closing events, its call's,
    /// then `response.done` with `status` and why.
    async fn finish(&mut self, status: &str, reason: Option<&str>) {
        let Some(Active { id, turn, item }) = self.active.take() else {
            return;
        };
        let completed = status == "completed";
        let mut output = Vec::new();
        if let Some(item_id) = item {
            let audio = self.config["output_modalities"][0] == "audio";
            let at = json!({"response_id": id, "item_id": item_id, "output_index": 0, "content_index": 0});
            let event = |kind: &str, fields: Value| {
                let mut event = at.clone();
                event["type"] = kind.into();
                merge(&mut event, &fields);
                event
            };
            let (part, content) = match audio {
                true => {
                    let transcript = turn.transcript.clone().unwrap_or_default();
                    self.emit(event("response.output_audio.done", json!({})))
                        .await;
                    self.emit(event(
                        "response.output_audio_transcript.done",
                        json!({"transcript": transcript}),
                    ))
                    .await;
                    (
                        json!({"type": "audio", "transcript": transcript}),
                        json!({"type": "output_audio", "transcript": transcript}),
                    )
                }
                false => {
                    let text = turn.text.clone().unwrap_or_default();
                    self.emit(event("response.output_text.done", json!({"text": text})))
                        .await;
                    (
                        json!({"type": "text", "text": text}),
                        json!({"type": "output_text", "text": text}),
                    )
                }
            };
            self.emit(event("response.content_part.done", json!({"part": part})))
                .await;
            let done = json!({"id": item_id, "object": "realtime.item", "type": "message",
                "status": if completed { "completed" } else { "incomplete" },
                "role": "assistant", "content": [content]});
            if let Some(kept) = self.items.iter_mut().find(|i| i["id"] == item_id) {
                *kept = done.clone();
            }
            self.emit(json!({"type": "conversation.item.done", "item": done}))
                .await;
            self.emit(
                json!({"type": "response.output_item.done", "response_id": id,
                "output_index": 0, "item": done}),
            )
            .await;
            output.push(done);
        }
        if let (true, Some(call)) = (completed, &turn.tool_call) {
            output.push(
                self.call(&id, output.len(), &call.name, &call.arguments)
                    .await,
            );
        }
        let details = reason.map(|reason| json!({"type": status, "reason": reason}));
        let response = json!({"object": "realtime.response", "id": id, "status": status,
            "status_details": details, "output": output, "usage": usage(turn.usage)});
        self.emit(json!({"type": "response.done", "response": response}))
            .await;
        let limits = json!({"type": "rate_limits.updated", "rate_limits": [
            {"name": "requests", "limit": 1000, "remaining": 999, "reset_seconds": 60.0},
            {"name": "tokens", "limit": 100_000, "remaining": 99_000, "reset_seconds": 60.0}]});
        self.emit(limits).await;
    }

    /// The model calls `name`: its item's events, its arguments in two
    /// deltas; the item.
    async fn call(&mut self, response: &str, index: usize, name: &str, arguments: &Value) -> Value {
        let item_id = self.connection.id("item");
        let call_id = self.connection.id("call");
        self.calls.insert(call_id.clone());
        let arguments = arguments.to_string();
        let item = |status: &str, arguments: &str| {
            json!({"id": item_id, "object": "realtime.item", "type": "function_call",
                "status": status, "name": name, "call_id": call_id, "arguments": arguments})
        };
        let open = item("in_progress", "");
        self.emit(
            json!({"type": "response.output_item.added", "response_id": response,
            "output_index": index, "item": open}),
        )
        .await;
        let previous = self.items.last().map(|i| i["id"].clone());
        self.emit(
            json!({"type": "conversation.item.added", "previous_item_id": previous, "item": open}),
        )
        .await;
        let half = arguments.len() / 2;
        let half = (0..=half)
            .rev()
            .find(|i| arguments.is_char_boundary(*i))
            .unwrap_or(0);
        for delta in [&arguments[..half], &arguments[half..]] {
            self.emit(
                json!({"type": "response.function_call_arguments.delta", "response_id": response,
                "item_id": item_id, "output_index": index, "call_id": call_id, "delta": delta}),
            )
            .await;
        }
        self.emit(
            json!({"type": "response.function_call_arguments.done", "response_id": response,
            "item_id": item_id, "output_index": index, "call_id": call_id, "name": name,
            "arguments": arguments}),
        )
        .await;
        let done = item("completed", &arguments);
        self.items.push(done.clone());
        self.emit(json!({"type": "conversation.item.done", "item": done}))
            .await;
        self.emit(
            json!({"type": "response.output_item.done", "response_id": response,
            "output_index": index, "item": done}),
        )
        .await;
        done
    }
}

/// A response's usage, as the API writes it.
fn usage(usage: Usage) -> Value {
    json!({
        "total_tokens": usage.input_tokens + usage.output_tokens,
        "input_tokens": usage.input_tokens,
        "output_tokens": usage.output_tokens,
        "input_token_details": {
            "cached_tokens": usage.cached_tokens,
            "text_tokens": usage.input_tokens.saturating_sub(usage.input_audio_tokens),
            "audio_tokens": usage.input_audio_tokens
        },
        "output_token_details": {
            "text_tokens": usage.output_tokens.saturating_sub(usage.output_audio_tokens),
            "audio_tokens": usage.output_audio_tokens
        }
    })
}
