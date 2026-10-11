//! What the model says: OpenAI Realtime's server events as the kind's
//! [`Output`], read at the provider's edge and written at the client's.

use std::collections::HashMap;

use respan_gateway_core::{Fault, NativeFields, NativeValue, Upstream};
use respan_gateway_http::bytes::Bytes;
use respan_gateway_realtime::{Audio, AudioFormat, Output, Status, Usage};
use serde_json::{Value, json};

use super::{NAME, session};
use crate::json::{
    self, at, count, merge, number, own, set, string, take, text, untranslatable_output,
};

/// A count of [`Usage`].
type Counter = fn(&mut Usage) -> &mut u64;

/// Where a response's usage counts are, and which of [`Usage`] each is.
const USAGE: &[(&[&str], Counter)] = &[
    (&["response", "usage", "input_tokens"], |u| {
        &mut u.tokens.input_tokens
    }),
    (&["response", "usage", "output_tokens"], |u| {
        &mut u.tokens.output_tokens
    }),
    (
        &["response", "usage", "input_token_details", "cached_tokens"],
        |u| &mut u.tokens.cached_input_tokens,
    ),
    (
        &["response", "usage", "input_token_details", "audio_tokens"],
        |u| &mut u.input_audio_tokens,
    ),
    (
        &["response", "usage", "output_token_details", "audio_tokens"],
        |u| &mut u.output_audio_tokens,
    ),
];

/// A provider's frames as events.
#[derive(Debug)]
pub(super) struct Provider {
    /// The format of the model's audio, as the session says.
    output: AudioFormat,
    /// The tools' names by the id of their calls, for a call's end that
    /// does not name its tool.
    calls: HashMap<String, String>,
}

impl Default for Provider {
    fn default() -> Self {
        Self {
            output: AudioFormat::pcm16(24_000),
            calls: HashMap::new(),
        }
    }
}

impl Provider {
    pub(super) fn read(&mut self, raw: Bytes, out: &mut Vec<Output>) -> Result<(), Fault> {
        let v: Value = serde_json::from_slice(&raw)
            .map_err(|_| Upstream::fault("a provider's frame is not a JSON event"))?;
        let mut rest = v.clone();
        match take(&mut rest, &["type"], string, text) {
            Some(kind) if self.decode(&kind, rest, out).is_some() => {}
            _ => out.push(Output::native(NAME, v)),
        }
        Ok(())
    }

    /// The events of a server event of type `kind`, whose other fields are
    /// `rest`; `None`, writing none, for one the kind has no form for or
    /// one it could not write back as it came.
    fn decode(&mut self, kind: &str, mut rest: Value, out: &mut Vec<Output>) -> Option<()> {
        let v = &mut rest;
        let delta = |v: &mut Value| take(v, &["delta"], string, text);
        let event = match kind {
            "session.created" | "session.updated" => {
                let session = session::decode(rest);
                self.output = session.output_audio.unwrap_or(self.output);
                match kind {
                    "session.created" => Output::Opened(session),
                    _ => Output::Updated(session),
                }
            }
            "input_audio_buffer.speech_started" => Output::SpeechStarted {
                at_ms: take(v, &["audio_start_ms"], |n| n.as_u64(), number),
                native: json::native(NAME, rest),
            },
            "input_audio_buffer.speech_stopped" => Output::SpeechStopped {
                at_ms: take(v, &["audio_end_ms"], |n| n.as_u64(), number),
                native: json::native(NAME, rest),
            },
            "conversation.item.input_audio_transcription.delta" => Output::InputTranscript {
                text: delta(v)?,
                complete: false,
                native: json::native(NAME, rest),
            },
            "conversation.item.input_audio_transcription.completed" => Output::InputTranscript {
                text: take(v, &["transcript"], string, text)?,
                complete: true,
                native: json::native(NAME, rest),
            },
            // A response's start is OpenAI's even if it says nothing more:
            // its items are the provider's.
            "response.created" => Output::ResponseStart {
                id: take(v, &["response", "id"], string, text),
                native: Some(NativeFields::new(NAME, rest.as_object()?.clone())),
            },
            "response.output_audio.delta" => Output::Audio {
                audio: Audio {
                    data: json::take_audio(v, &["delta"])?,
                    format: self.output,
                },
                native: json::native(NAME, rest),
            },
            "response.output_text.delta" => Output::Text {
                text: delta(v)?,
                native: json::native(NAME, rest),
            },
            "response.output_audio_transcript.delta" => Output::Transcript {
                text: delta(v)?,
                native: json::native(NAME, rest),
            },
            "response.output_item.added" => {
                let item = &v["item"];
                if let (Some("function_call"), Some(id), Some(name)) = (
                    item["type"].as_str(),
                    item["call_id"].as_str(),
                    item["name"].as_str(),
                ) {
                    self.calls.insert(id.to_owned(), name.to_owned());
                }
                return None;
            }
            "response.function_call_arguments.done" => {
                let id = take(v, &["call_id"], string, text)?;
                let name =
                    take(v, &["name"], string, text).or_else(|| self.calls.get(&id).cloned())?;
                Output::ToolCall {
                    arguments: take(v, &["arguments"], string, text)?,
                    id,
                    name,
                    native: json::native(NAME, rest),
                }
            }
            "response.done" => {
                let status = status(v)?;
                if at(v, &["response", "usage"]).is_some_and(Value::is_object) {
                    let mut usage = Usage::default();
                    for (path, counter) in USAGE {
                        *counter(&mut usage) = take(v, path, count, number).unwrap_or(0);
                    }
                    out.push(Output::Usage(usage));
                }
                Output::ResponseDone {
                    status,
                    native: json::native(NAME, rest),
                }
            }
            "error" => Output::Error {
                message: take(v, &["error", "message"], string, text)?,
                code: take(v, &["error", "type"], string, text),
                native: json::native(NAME, rest),
            },
            _ => return None,
        };
        out.push(event);
        Some(())
    }
}

/// How a response ended, by its status and the reason its details give,
/// taken from `v` where they write back as they came.
fn status(v: &mut Value) -> Option<Status> {
    let said = string(at(v, &["response", "status"])?)?;
    let details = at(v, &["response", "status_details"]);
    let typed = details
        .and_then(|d| d.get("type"))
        .is_none_or(|t| *t == *said);
    let reason = details.and_then(|d| d["reason"].as_str());
    let known = [
        Status::Interrupted,
        Status::Cancelled,
        Status::Length,
        Status::Filtered,
    ];
    let named = known
        .into_iter()
        .find(|s| typed && spelled(s) == Some((said.as_str(), reason)));
    let status = match (said.as_str(), named) {
        (_, Some(status)) => {
            take(v, &["response", "status_details", "type"], string, text);
            take(v, &["response", "status_details", "reason"], string, text);
            status
        }
        ("completed", None) => Status::Completed,
        ("failed", None) => Status::Failed,
        (_, None) => Status::Native {
            format: NAME.into(),
            reason: said.clone(),
            fallback: Box::new(match said.as_str() {
                "cancelled" => Status::Cancelled,
                _ => Status::Failed,
            }),
        },
    };
    take(v, &["response", "status"], string, text);
    Some(status)
}

/// A status as GA writes it: the response's status, and its details'
/// reason if it has one.
fn spelled(status: &Status) -> Option<(&str, Option<&str>)> {
    Some(match status {
        Status::Completed => ("completed", None),
        Status::Interrupted => ("cancelled", Some("turn_detected")),
        Status::Cancelled => ("cancelled", Some("client_cancelled")),
        Status::Length => ("incomplete", Some("max_output_tokens")),
        Status::Filtered => ("incomplete", Some("content_filter")),
        Status::Failed => ("failed", None),
        Status::Native { format, reason, .. } if **format == *NAME => (reason.as_str(), None),
        Status::Native { fallback, .. } => return spelled(fallback),
        _ => return None,
    })
}

/// The events written to the client.
#[derive(Debug)]
pub(super) struct Client {
    /// The format of the audio it takes, as its session says.
    output: AudioFormat,
    /// Usage of the response being made, written in its `response.done`.
    usage: Option<Usage>,
}

impl Default for Client {
    fn default() -> Self {
        Self {
            output: AudioFormat::pcm16(24_000),
            usage: None,
        }
    }
}

impl Client {
    pub(super) fn write(&mut self, event: &Output, out: &mut Vec<Bytes>) -> Result<(), Fault> {
        if let Some(v) = self.encode(event)? {
            out.push(json::frame(&v)?);
        }
        Ok(())
    }

    fn encode(&mut self, event: &Output) -> Result<Option<Value>, Fault> {
        let refuse = |what: &str| untranslatable_output(NAME, what);
        let (mut v, native) = match event {
            Output::Opened(s) | Output::Updated(s) => {
                self.output = s.output_audio.unwrap_or(self.output);
                let kind = match event {
                    Output::Opened(_) => "session.created",
                    _ => "session.updated",
                };
                let session = session::encode(s, refuse)?;
                (json!({"type": kind, "session": session}), &s.native)
            }
            Output::SpeechStarted { at_ms, native } => {
                let mut v = json!({"type": "input_audio_buffer.speech_started"});
                if let Some(ms) = at_ms {
                    v["audio_start_ms"] = (*ms).into();
                }
                (v, native)
            }
            Output::SpeechStopped { at_ms, native } => {
                let mut v = json!({"type": "input_audio_buffer.speech_stopped"});
                if let Some(ms) = at_ms {
                    v["audio_end_ms"] = (*ms).into();
                }
                (v, native)
            }
            Output::InputTranscript {
                text,
                complete,
                native,
            } => {
                let kind = "conversation.item.input_audio_transcription";
                let v = match complete {
                    false => json!({"type": format!("{kind}.delta"), "delta": text}),
                    true => json!({"type": format!("{kind}.completed"), "transcript": text}),
                };
                (v, native)
            }
            Output::ResponseStart { id, native } => {
                // A response that began in another format has none of the
                // items OpenAI's scaffolding names.
                if !matches!(native, Some(n) if *n.format == *NAME) {
                    return Err(refuse("a response outside OpenAI's items"));
                }
                let mut v = json!({"type": "response.created", "response": {}});
                if let Some(id) = id {
                    v["response"]["id"] = id.as_str().into();
                }
                (v, native)
            }
            Output::Audio { audio, native } => {
                if audio.format != self.output {
                    return Err(refuse("audio in another format than the session's"));
                }
                let delta = json::base64(&audio.data);
                (
                    json!({"type": "response.output_audio.delta", "delta": delta}),
                    native,
                )
            }
            Output::Text { text, native } => (
                json!({"type": "response.output_text.delta", "delta": text}),
                native,
            ),
            Output::Transcript { text, native } => {
                let v = json!({"type": "response.output_audio_transcript.delta", "delta": text});
                (v, native)
            }
            Output::ToolCall {
                id,
                name,
                arguments,
                native,
            } => {
                let v = json!({"type": "response.function_call_arguments.done", "call_id": id,
                    "name": name, "arguments": arguments});
                (v, native)
            }
            Output::Usage(usage) => {
                let held = self.usage.get_or_insert_default();
                held.tokens += &usage.tokens;
                held.input_audio_tokens += usage.input_audio_tokens;
                held.output_audio_tokens += usage.output_audio_tokens;
                return Ok(None);
            }
            Output::ResponseDone { status, native } => {
                let (said, reason) = spelled(status).ok_or_else(|| refuse("this status"))?;
                let mut v = json!({"type": "response.done", "response": {"status": said}});
                if let Some(reason) = reason {
                    v["response"]["status_details"] = json!({"type": said, "reason": reason});
                }
                merge(
                    &mut v,
                    own(NAME, native, || refuse("another format's event"))?,
                );
                for (path, counter) in USAGE {
                    let mut usage = self.usage.unwrap_or_default();
                    let n = *counter(&mut usage);
                    if n > 0 {
                        set(&mut v, path, n.into());
                    }
                }
                self.usage = None;
                return Ok(Some(v));
            }
            Output::Error {
                code,
                message,
                native,
            } => {
                let mut v = json!({"type": "error", "error": {"message": message}});
                if let Some(code) = code {
                    v["error"]["type"] = code.as_str().into();
                }
                (v, native)
            }
            Output::Native(NativeValue { format, value }) if **format == *NAME => {
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
