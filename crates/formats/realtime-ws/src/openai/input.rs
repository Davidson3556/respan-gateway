//! What the client says: OpenAI Realtime's client events as the kind's
//! [`Input`], read at the client's edge and written at the provider's.

use respan_gateway_conversation::{Content, Message, Role};
use respan_gateway_core::{BadRequest, Fault, NativeValue};
use respan_gateway_http::bytes::Bytes;
use respan_gateway_realtime::{Audio, AudioFormat, Input, Session, ToolResult};
use serde_json::{Value, json};

use super::{NAME, session};
use crate::audio::Convert;
use crate::foreign::Drops;
use crate::json::{self, merge, own, string, take, text, untranslatable_input};

/// What of another format's natives OpenAI Realtime drops: Gemini Live's
/// transcription settings, context compression and voice detection's
/// sensitivities, and the shape of the message a client's event came in.
const DROPS: Drops = Drops {
    format: NAME,
    session: &[
        "setup.contextWindowCompression",
        "setup.inputAudioTranscription",
        "setup.outputAudioTranscription",
    ],
    vad: &[
        "startOfSpeechSensitivity",
        "endOfSpeechSensitivity",
        "disabled",
    ],
    message: &[],
    event: &["clientContent", "realtimeInput.activityStart", "name"],
};

/// A client's frames as events, beginning with the session the handshake
/// asked for, its model the one at `?model=`: the client's first
/// `session.update`, or, if its first frame is another event, a session
/// of that model alone before it.
#[derive(Debug)]
pub(super) struct Client {
    /// The model the handshake asked for, until the first frame.
    opening: Option<Option<String>>,
    /// The format of the client's audio, as its session says.
    input: AudioFormat,
}

impl Client {
    pub(super) fn new(model: Option<String>) -> Self {
        Self {
            opening: Some(model),
            input: AudioFormat::pcm16(24_000),
        }
    }

    pub(super) fn read(&mut self, raw: Bytes, out: &mut Vec<Input>) -> Result<(), Fault> {
        let v: Value = serde_json::from_slice(&raw)
            .map_err(|_| BadRequest::fault("a client's frame is not a JSON event"))?;
        let mut event = decode(v, self.input);
        if let Some(model) = self.opening.take() {
            match &mut event {
                Input::Session(s) if s.model.is_none() => s.model = model,
                Input::Session(_) => {}
                _ => {
                    let mut opening = Session::default();
                    opening.model = model;
                    out.push(Input::Session(opening));
                }
            }
        }
        if let Input::Session(Session {
            input_audio: Some(format),
            ..
        }) = &event
        {
            self.input = *format;
        }
        out.push(event);
        Ok(())
    }
}

/// A client event as the kind's, its audio in `input`; one the kind has no
/// form for, or one it could not write back as it came, is native.
fn decode(v: Value, input: AudioFormat) -> Input {
    let mut rest = v.clone();
    let kind = take(&mut rest, &["type"], string, text);
    let event = match kind.as_deref() {
        Some("session.update") => Some(Input::Session(session::decode(rest))),
        Some("input_audio_buffer.append") => audio(rest, input),
        Some("input_audio_buffer.commit") => Some(Input::Commit {
            native: json::native(NAME, rest),
        }),
        Some("conversation.item.create") => item(rest),
        Some("response.create") => Some(Input::Respond {
            native: json::native(NAME, rest),
        }),
        Some("response.cancel") => Some(Input::Cancel {
            native: json::native(NAME, rest),
        }),
        _ => None,
    };
    event.unwrap_or_else(|| Input::native(NAME, v))
}

fn audio(mut rest: Value, format: AudioFormat) -> Option<Input> {
    let data = json::take_audio(&mut rest, &["audio"])?;
    Some(Input::Audio {
        audio: Audio { data, format },
        native: json::native(NAME, rest),
    })
}

/// An item the client adds: a message, or a tool's answer; any other
/// item, such as a call in the history, is native.
fn item(mut rest: Value) -> Option<Input> {
    match rest["item"]["type"].as_str()? {
        "message" => {
            let message = message(&rest["item"])?;
            rest.as_object_mut()?.shift_remove("item");
            Some(Input::Message {
                message,
                native: json::native(NAME, rest),
            })
        }
        "function_call_output" => {
            let kind = |t: &Value| (t == "function_call_output").then_some(());
            take(&mut rest, &["item", "type"], kind, |_| {
                Some("function_call_output".into())
            })?;
            let id = take(&mut rest, &["item", "call_id"], string, text)?;
            let output = take(&mut rest, &["item", "output"], string, text)?;
            let mut result = ToolResult::new(id, output);
            result.native = json::native(NAME, rest);
            Some(Input::ToolResult(result))
        }
        _ => None,
    }
}

/// A message item, if it writes back as it came.
fn message(item: &Value) -> Option<Message> {
    let mut rest = item.clone();
    let rest = &mut rest;
    take(
        rest,
        &["type"],
        |t| (t == "message").then_some(()),
        |_| Some("message".into()),
    )?;
    let role = match take(rest, &["role"], string, text)?.as_str() {
        "system" => Role::System,
        "user" => Role::User,
        "assistant" => Role::Assistant,
        _ => return None,
    };
    let parts = take(
        rest,
        &["content"],
        |c| c.as_array().cloned(),
        |c| Some(Value::Array(c.clone())),
    )?;
    let mut message = Message::new(role, "");
    message.content = parts.iter().map(part).collect();
    message.native = json::native(NAME, std::mem::take(rest));
    let written = message_json(&message, |_| Fault::internal("unwritable"));
    (written.ok().as_ref() == Some(item)).then_some(message)
}

/// A text part as the kind's; any other part, such as audio, stays native.
fn part(p: &Value) -> Content {
    let is_text = matches!(p["type"].as_str(), Some("input_text" | "output_text"));
    match (p.as_object().map(|o| o.len()), p["text"].as_str()) {
        (Some(2), Some(text)) if is_text => Content::from(text),
        _ => Content::native(NAME, p.clone()),
    }
}

/// A message as an item: its text typed as its role's.
fn message_json(m: &Message, refuse: fn(&str) -> Fault) -> Result<Value, Fault> {
    let (role, text) = match m.role {
        Role::System => ("system", "input_text"),
        Role::User => ("user", "input_text"),
        Role::Assistant => ("assistant", "output_text"),
        _ => return Err(refuse("a message of this role")),
    };
    let part = |c: &Content| match c {
        Content::Text {
            text: t, native, ..
        } if native.is_none() => Ok(json!({"type": text, "text": t})),
        Content::Native(NativeValue { format, value }) if **format == *NAME => {
            Ok(Value::clone(value))
        }
        _ => Err(refuse("a message part of this kind")),
    };
    let content = m.content.iter().map(part).collect::<Result<Vec<_>, _>>()?;
    let mut v = json!({"type": "message", "role": role, "content": content});
    merge(
        &mut v,
        own(NAME, &m.native, || refuse("another format's message"))?,
    );
    Ok(v)
}

/// The client's events written to the provider.
#[derive(Debug)]
pub(super) struct Provider {
    /// The model the session was opened with, at its URL.
    model: Option<String>,
    /// Whether its first session was written.
    opened: bool,
    /// The format of the audio it takes, as the sessions written say.
    input: AudioFormat,
    /// The client's audio in that format.
    convert: Convert,
}

impl Provider {
    /// A writer to a session opened with `model`.
    pub(super) fn new(model: Option<String>) -> Self {
        Self {
            model,
            opened: false,
            input: AudioFormat::pcm16(24_000),
            convert: Convert::default(),
        }
    }

    pub(super) fn write(&mut self, event: &Input, out: &mut Vec<Bytes>) -> Result<(), Fault> {
        let event = DROPS.admit(event)?;
        if let Some(v) = self.encode(&event)? {
            out.push(json::frame(&v)?);
        }
        Ok(())
    }

    fn encode(&mut self, event: &Input) -> Result<Option<Value>, Fault> {
        let refuse = |what: &str| untranslatable_input(NAME, what);
        let (mut v, native) = match event {
            Input::Session(s) => {
                // OpenAI takes its model at connect: the first session's is
                // the one at its URL, and a later one that names it again
                // changes nothing.
                let opening = !std::mem::replace(&mut self.opened, true);
                let mut s = s.clone();
                if opening || s.model == self.model {
                    s.model = None;
                }
                if s == Session::default() {
                    return Ok(None);
                }
                if let Some(format) = s.input_audio {
                    self.input = format;
                }
                let session = session::encode(&s, refuse)?;
                let mut v = json!({"type": "session.update", "session": session});
                merge(
                    &mut v,
                    own(NAME, &s.native, || refuse("another format's session"))?,
                );
                if v["session"].get("type").is_none() {
                    v["session"]["type"] = "realtime".into();
                }
                return Ok(Some(v));
            }
            Input::Audio { audio, native } => {
                let data = self.convert.to(audio, self.input);
                let data = data.ok_or_else(|| refuse("audio of this format"))?;
                let v = json!({"type": "input_audio_buffer.append", "audio": json::base64(&data)});
                (v, native)
            }
            Input::Commit { native } => (json!({"type": "input_audio_buffer.commit"}), native),
            Input::Message { message, native } => {
                let item = message_json(message, refuse)?;
                (
                    json!({"type": "conversation.item.create", "item": item}),
                    native,
                )
            }
            Input::ToolResult(result) => {
                let item = json!({"type": "function_call_output", "call_id": result.id,
                    "output": result.output});
                let v = json!({"type": "conversation.item.create", "item": item});
                (v, &result.native)
            }
            Input::Respond { native } => (json!({"type": "response.create"}), native),
            Input::Cancel { native } => (json!({"type": "response.cancel"}), native),
            Input::Native(NativeValue { format, value }) if **format == *NAME => {
                return Ok(Some(Value::clone(value)));
            }
            Input::Native(NativeValue { value, .. }) => return dropped(value).map(|()| None),
            _ => return Err(refuse("another format's event")),
        };
        merge(
            &mut v,
            own(NAME, native, || refuse("another format's event"))?,
        );
        Ok(Some(v))
    }
}

/// Whether another format's event, which OpenAI has none of, is dropped:
/// Gemini Live's start of the user's activity, which OpenAI's voice
/// detection or the next commit implies, and the end of its audio stream;
/// any other is refused, by its field.
fn dropped(event: &Value) -> Result<(), Fault> {
    let input = event.get("realtimeInput").and_then(Value::as_object);
    let field = |fields: &serde_json::Map<String, Value>| fields.keys().next().cloned();
    match input.and_then(field).as_deref() {
        Some("activityStart" | "audioStreamEnd") => Ok(()),
        Some(field) => Err(untranslatable_input(
            NAME,
            &format!("`realtimeInput.{field}`"),
        )),
        None => {
            let field = event.as_object().and_then(field).unwrap_or_default();
            Err(untranslatable_input(NAME, &format!("`{field}`")))
        }
    }
}
