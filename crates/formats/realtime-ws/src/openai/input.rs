//! What the client says: OpenAI Realtime's client events as the kind's
//! [`Input`], read at the client's edge and written at the provider's.

use respan_gateway_conversation::{Content, Message, Role};
use respan_gateway_core::{BadRequest, Fault, NativeValue};
use respan_gateway_http::bytes::Bytes;
use respan_gateway_realtime::{Audio, AudioFormat, Input, Session, ToolResult};
use serde_json::{Value, json};

use super::{NAME, session};
use crate::json::{self, merge, own, string, take, text, untranslatable_input};

/// A client's frames as events. Its first is preceded by the session the
/// handshake asked for, its model the one at `?model=`, so a session's
/// events always begin with an [`Input::Session`].
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
        if let Some(model) = self.opening.take() {
            let mut opening = Session::default();
            opening.model = model;
            out.push(Input::Session(opening));
        }
        let event = decode(v, self.input);
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
    /// The format of the audio it takes, as the sessions written say.
    input: AudioFormat,
}

impl Default for Provider {
    fn default() -> Self {
        Self {
            input: AudioFormat::pcm16(24_000),
        }
    }
}

impl Provider {
    pub(super) fn write(&mut self, event: &Input, out: &mut Vec<Bytes>) -> Result<(), Fault> {
        if let Some(v) = self.encode(event)? {
            out.push(json::frame(&v)?);
        }
        Ok(())
    }

    fn encode(&mut self, event: &Input) -> Result<Option<Value>, Fault> {
        let refuse = |what: &str| untranslatable_input(NAME, what);
        let (mut v, native) = match event {
            Input::Session(s) => {
                // OpenAI takes its model at connect: a session that says
                // nothing more changes nothing.
                let mut rest = s.clone();
                rest.model = None;
                if rest == Session::default() {
                    return Ok(None);
                }
                if let Some(format) = s.input_audio {
                    self.input = format;
                }
                let session = session::encode(s, refuse)?;
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
                if audio.format != self.input {
                    return Err(refuse("audio in another format than the session's"));
                }
                let v = json!({"type": "input_audio_buffer.append", "audio": json::base64(&audio.data)});
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
            _ => return Err(refuse("another format's event")),
        };
        merge(
            &mut v,
            own(NAME, native, || refuse("another format's event"))?,
        );
        Ok(Some(v))
    }
}
