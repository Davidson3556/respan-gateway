//! What the client says: the Live API's client messages as the kind's
//! [`Input`], read at the client's edge and written at the provider's.

use std::sync::Arc;

use respan_gateway_conversation::{Content, Message, Role};
use respan_gateway_core::{BadRequest, Fault, NativeFields, NativeValue};
use respan_gateway_http::bytes::Bytes;
use respan_gateway_realtime::{Audio, AudioFormat, Encoding, Input, ToolResult};
use respan_gateway_ws::CloseCode;
use serde_json::{Map, Value, json};

use super::{Applied, NAME, setup};
use crate::json::{self, merge, own, untranslatable_input};

/// A client's messages as events: its `setup` first and once, and each
/// message one of `setup`, `clientContent`, `realtimeInput` and
/// `toolResponse`, as the Live API takes them; any other is the client's
/// fault, closed 1007.
#[derive(Debug, Default)]
pub(super) struct Client {
    setup: bool,
}

impl Client {
    pub(super) fn read(&mut self, raw: Bytes, out: &mut Vec<Input>) -> Result<(), Fault> {
        let v: Value =
            serde_json::from_slice(&raw).map_err(|_| invalid("a client's message is not JSON"))?;
        let field = v
            .as_object()
            .filter(|m| m.len() == 1)
            .and_then(|m| m.keys().next());
        let field = field.ok_or_else(|| {
            invalid("a message holds one of setup, clientContent, realtimeInput and toolResponse")
        })?;
        match (field.as_str(), self.setup) {
            ("setup", false) => {
                self.setup = true;
                out.push(Input::Session(setup::decode(v)));
            }
            ("setup", true) => return Err(invalid("setup is sent once")),
            (_, false) => return Err(invalid("a session's first message is its setup")),
            _ => out.extend(decode(&v).unwrap_or_else(|| vec![Input::native(NAME, v)])),
        }
        Ok(())
    }
}

/// A message the Live API refuses, as it closes the session.
fn invalid(message: &str) -> Fault {
    BadRequest::fault(message).with(CloseCode(1007))
}

/// The events of a message after the setup; `None` for one the kind has no
/// form for, or one its events would not write back as it came.
fn decode(v: &Value) -> Option<Vec<Input>> {
    let events = match v.as_object()?.iter().next()? {
        (name, body) if name == "realtimeInput" => {
            let (field, value) = single(body)?;
            match field {
                "audio" => return audio(value).map(|a| vec![a]),
                "text" => vec![user(value.as_str()?), Input::Respond { native: None }],
                "activityEnd" if *value == json!({}) => vec![
                    Input::Commit { native: None },
                    Input::Respond { native: None },
                ],
                _ => return None,
            }
        }
        (name, body) if name == "clientContent" => {
            let turns = match body.get("turns") {
                Some(turns) => turns.as_array()?.as_slice(),
                None => &[],
            };
            let mut events = Vec::new();
            for turn in turns {
                events.push(Input::Message {
                    message: message(turn)?,
                    native: None,
                });
            }
            // The last event of the message keeps its shape, and ends it.
            let shape = json::native(NAME, json!({"clientContent": {}}));
            match (body["turnComplete"] == true, events.last_mut()) {
                (true, _) => events.push(Input::Respond { native: shape }),
                (false, Some(Input::Message { native, .. })) => *native = shape,
                (false, _) => return None,
            }
            events
        }
        (name, body) if name == "toolResponse" => {
            let answers = body["functionResponses"].as_array()?;
            let mut events = answers.iter().map(answer).collect::<Option<Vec<_>>>()?;
            events.push(Input::Respond { native: None });
            events
        }
        _ => return None,
    };
    let mut provider = Provider::opened();
    let mut written = Vec::new();
    for event in &events {
        provider.encode(event, &mut written).ok()?;
    }
    provider.flush(&mut written).ok()?;
    (written.as_slice() == std::slice::from_ref(v)).then_some(events)
}

/// The one field of `v`, an object.
fn single(v: &Value) -> Option<(&str, &Value)> {
    match v.as_object()? {
        fields if fields.len() == 1 => fields.iter().next().map(|(k, v)| (k.as_str(), v)),
        _ => None,
    }
}

/// A chunk of the user's audio: PCM at the rate its type declares.
fn audio(blob: &Value) -> Option<Input> {
    let mut rest = blob.clone();
    let rate = rest["mimeType"]
        .as_str()?
        .strip_prefix("audio/pcm;rate=")?
        .parse()
        .ok()?;
    let data = json::take_audio(&mut rest, &["data"])?;
    rest.as_object_mut()?.shift_remove("mimeType");
    let native = match rest.as_object()?.is_empty() {
        true => None,
        false => json::native(NAME, json!({"realtimeInput": {"audio": rest}})),
    };
    Some(Input::Audio {
        audio: Audio {
            data,
            format: AudioFormat::pcm16(rate),
        },
        native,
    })
}

/// The user's text, as a message.
fn user(text: &str) -> Input {
    Input::Message {
        message: Message::new(Role::User, text),
        native: None,
    }
}

/// A turn of `clientContent`: a role and parts; a part of text alone is the
/// kind's, any other stays the Live API's.
fn message(turn: &Value) -> Option<Message> {
    let mut rest = turn.as_object()?.clone();
    let role = match rest.shift_remove("role")?.as_str()? {
        "user" => Role::User,
        "model" => Role::Assistant,
        _ => return None,
    };
    let parts = rest.shift_remove("parts")?;
    let part = |p: &Value| match p.as_object().map(|o| (o.len(), o.get("text"))) {
        Some((1, Some(Value::String(text)))) => Content::from(text.as_str()),
        _ => Content::native(NAME, p.clone()),
    };
    let mut message = Message::new(role, "");
    message.content = parts.as_array()?.iter().map(part).collect();
    message.native = json::native(NAME, Value::Object(rest));
    Some(message)
}

fn message_json(m: &Message, refuse: fn(&str) -> Fault) -> Result<Value, Fault> {
    let role = match m.role {
        Role::User => "user",
        Role::Assistant => "model",
        _ => return Err(refuse("a message of this role")),
    };
    let part = |c: &Content| match c {
        Content::Text { text, native, .. } if native.is_none() => Ok(json!({"text": text})),
        Content::Native(NativeValue { format, value }) if **format == *NAME => {
            Ok(Value::clone(value))
        }
        _ => Err(refuse("a message part of this kind")),
    };
    let parts = m.content.iter().map(part).collect::<Result<Vec<_>, _>>()?;
    let mut v = json!({"role": role, "parts": parts});
    merge(
        &mut v,
        own(NAME, &m.native, || refuse("another format's message"))?,
    );
    Ok(v)
}

/// A function's answer: its response, as JSON text, and its other fields,
/// such as its name, kept.
fn answer(v: &Value) -> Option<Input> {
    let mut rest = v.as_object()?.clone();
    let id = rest.shift_remove("id")?.as_str()?.to_owned();
    let response = rest.shift_remove("response")?;
    let mut result = ToolResult::new(id, response.to_string());
    result.native = json::native(NAME, Value::Object(rest));
    Some(Input::ToolResult(result))
}

/// A tool's answer as a function response: its output as the response if
/// it is a JSON object, else the object `{"output": ..}`.
fn answer_json(result: &ToolResult, refuse: fn(&str) -> Fault) -> Result<Value, Fault> {
    let response = match serde_json::from_str::<Value>(&result.output) {
        Ok(object @ Value::Object(_)) => object,
        _ => json!({"output": result.output}),
    };
    let mut v = json!({"id": result.id, "response": response});
    merge(
        &mut v,
        own(NAME, &result.native, || refuse("another format's answer"))?,
    );
    Ok(v)
}

/// What the client says that the next [`Input::Respond`] answers.
#[derive(Debug)]
enum Held {
    /// Messages, which the Live API adds to the conversation together.
    Messages(Vec<Input>),
    /// Tools' answers, which it takes together.
    Results(Vec<ToolResult>),
}

/// The client's events written to the provider: its setup first, then
/// messages and tools' answers held until what answers them, so that a
/// message writes them together.
#[derive(Debug)]
pub(super) struct Provider {
    /// The setup written, which the provider's events say took effect.
    applied: Applied,
    /// Whether the setup was written.
    opened: bool,
    held: Option<Held>,
    /// Whether the last message already makes the model answer.
    answered: bool,
}

impl Provider {
    pub(super) fn new(applied: Applied) -> Self {
        Self {
            applied,
            opened: false,
            held: None,
            answered: false,
        }
    }

    /// A writer past its setup, which writes a message as a session would.
    fn opened() -> Self {
        let mut provider = Self::new(Arc::default());
        provider.opened = true;
        provider
    }

    pub(super) fn write(&mut self, event: &Input, out: &mut Vec<Bytes>) -> Result<(), Fault> {
        let mut messages = Vec::new();
        self.encode(event, &mut messages)?;
        for message in &messages {
            out.push(json::frame(message)?);
        }
        Ok(())
    }

    fn encode(&mut self, event: &Input, out: &mut Vec<Value>) -> Result<(), Fault> {
        let refuse = |what: &str| untranslatable_input(NAME, what);
        let answered = std::mem::take(&mut self.answered);
        let (mut v, native) = match event {
            Input::Session(s) if !self.opened => {
                self.opened = true;
                *super::lock(&self.applied) = Some(s.clone());
                out.push(setup::encode(s, refuse)?);
                return Ok(());
            }
            Input::Session(_) => {
                let fixed = "Gemini Live fixes a session's configuration at its setup";
                return Err(BadRequest::fault(fixed).with(CloseCode(1008)));
            }
            _ if !self.opened => {
                return Err(Fault::internal(
                    "a Gemini Live session begins with its setup",
                ));
            }
            Input::Message { native, .. } => {
                match &mut self.held {
                    Some(Held::Messages(held)) => held.push(event.clone()),
                    _ => {
                        self.flush(out)?;
                        self.held = Some(Held::Messages(vec![event.clone()]));
                    }
                }
                if shaped(native) {
                    self.flush(out)?;
                }
                return Ok(());
            }
            Input::ToolResult(result) => {
                match &mut self.held {
                    Some(Held::Results(held)) => held.push(result.clone()),
                    _ => {
                        self.flush(out)?;
                        self.held = Some(Held::Results(vec![result.clone()]));
                    }
                }
                return Ok(());
            }
            Input::Respond { native } if answered && self.held.is_none() => {
                own(NAME, native, || refuse("another format's event"))?;
                return Ok(());
            }
            Input::Respond { native } => (self.respond(native)?, native),
            _ => {
                self.flush(out)?;
                match event {
                    Input::Audio { audio, native } => {
                        if audio.format.encoding != Encoding::Pcm16 {
                            return Err(refuse("audio other than PCM"));
                        }
                        let mime = format!("audio/pcm;rate={}", audio.format.rate);
                        let blob = json!({"mimeType": mime, "data": json::base64(&audio.data)});
                        (json!({"realtimeInput": {"audio": blob}}), native)
                    }
                    Input::Commit { native } => {
                        self.answered = true;
                        (json!({"realtimeInput": {"activityEnd": {}}}), native)
                    }
                    Input::Native(NativeValue { format, value }) if **format == *NAME => {
                        out.push(Value::clone(value));
                        return Ok(());
                    }
                    Input::Cancel { .. } => return Err(refuse("a cancel")),
                    _ => return Err(refuse("another format's event")),
                }
            }
        };
        merge(
            &mut v,
            own(NAME, native, || refuse("another format's event"))?,
        );
        out.push(v);
        Ok(())
    }

    /// The message that asks for an answer to what is held: the user's
    /// text alone, unless `native` says it came as `clientContent`;
    /// messages; tools' answers; or a complete turn of nothing more.
    fn respond(&mut self, native: &Option<NativeFields>) -> Result<Value, Fault> {
        let refuse = |what: &str| untranslatable_input(NAME, what);
        let content = shaped(native);
        match self.held.take() {
            None => Ok(json!({"clientContent": {"turnComplete": true}})),
            Some(Held::Results(results)) => self.results(&results),
            Some(Held::Messages(messages)) => match messages.as_slice() {
                [Input::Message { message, native }]
                    if !content
                        && native.is_none()
                        && message.native.is_none()
                        && message.role == Role::User
                        && message
                            .content
                            .iter()
                            .all(|c| matches!(c, Content::Text { native: None, .. })) =>
                {
                    Ok(json!({"realtimeInput": {"text": message.text()}}))
                }
                _ => {
                    let mut v = messages_json(&messages, refuse)?;
                    v["clientContent"]["turnComplete"] = true.into();
                    Ok(v)
                }
            },
        }
    }

    fn results(&self, results: &[ToolResult]) -> Result<Value, Fault> {
        let refuse = |what: &str| untranslatable_input(NAME, what);
        let answers = results.iter().map(|r| answer_json(r, refuse));
        let answers = answers.collect::<Result<Vec<_>, _>>()?;
        Ok(json!({"toolResponse": {"functionResponses": answers}}))
    }

    /// Write what is held, which nothing answers.
    fn flush(&mut self, out: &mut Vec<Value>) -> Result<(), Fault> {
        let refuse = |what: &str| untranslatable_input(NAME, what);
        match self.held.take() {
            None => {}
            Some(Held::Messages(messages)) => out.push(messages_json(&messages, refuse)?),
            Some(Held::Results(results)) => out.push(self.results(&results)?),
        }
        Ok(())
    }
}

/// Whether an event's `native` says it came as `clientContent`.
fn shaped(native: &Option<NativeFields>) -> bool {
    native
        .as_ref()
        .is_some_and(|n| n.fields.contains_key("clientContent"))
}

/// Messages as one `clientContent`, with what each event kept.
fn messages_json(messages: &[Input], refuse: fn(&str) -> Fault) -> Result<Value, Fault> {
    let mut turns = Vec::new();
    let mut kept = Map::new();
    for event in messages {
        if let Input::Message { message, native } = event {
            turns.push(message_json(message, refuse)?);
            if let Some(fields) = own(NAME, native, || refuse("another format's event"))? {
                kept.extend(fields.clone());
            }
        }
    }
    let mut v = json!({"clientContent": {"turns": turns}});
    merge(&mut v, Some(&kept));
    Ok(v)
}
