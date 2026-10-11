//! Audio validation and a conservative token bound for PCM24k and G.711.
//! Rates: https://developers.openai.com/api/docs/guides/voice-latency-cost
use base64::{Engine, engine::general_purpose::STANDARD};
use respan_gateway_core::BadRequest;
use respan_gateway_core::ErrorCode;
use respan_gateway_core::futures::{StreamExt, stream};
use respan_gateway_core::{ClientContext, EventStream, Fault, Plugin, Response, async_trait};
use respan_gateway_realtime::{AudioFormat, Input, Session, Turns};
use respan_gateway_ws::CloseCode;
use serde_json::Value;

pub(super) fn contains_audio(value: &Value) -> bool {
    match value {
        Value::Object(fields) => {
            fields.get("audio").is_some_and(Value::is_string) || fields.values().any(contains_audio)
        }
        Value::Array(items) => items.iter().any(contains_audio),
        _ => false,
    }
}

pub(super) fn tokens(encoded: &str) -> Result<i64, Fault> {
    let bytes = STANDARD
        .decode(encoded)
        .map_err(|_| BadRequest::fault("audio must be valid base64").with(CloseCode(1008)))?;
    Ok(bound(bytes.len()))
}

/// A bound on the tokens of `bytes` of audio. The fastest supported encoding
/// is 8kHz G.711: 400 bytes per output token. PCM24k needs more bytes per
/// token. Using the faster rate bounds both directions.
pub(super) fn bound(bytes: usize) -> i64 {
    i64::try_from(bytes.div_ceil(400)).unwrap_or(i64::MAX)
}

fn refused(message: &str) -> Fault {
    Fault::new(message.to_owned())
        .with(ErrorCode("audio_policy".into()))
        .with(CloseCode(1008))
}

/// A session this policy accepts: no separately billed transcription, and
/// audio only in the formats the bounds know. What the kind has no form for
/// (beta spellings, a malformed session) is read from its native fields.
fn check(session: &Session) -> Result<(), Fault> {
    if session.input_transcription == Some(true) {
        return Err(refused(
            "capped sessions disable separately billed transcription",
        ));
    }
    let allowed = [
        AudioFormat::pcm16(24_000),
        AudioFormat::MULAW,
        AudioFormat::ALAW,
    ];
    for format in [session.input_audio, session.output_audio]
        .into_iter()
        .flatten()
    {
        if !allowed.contains(&format) {
            return Err(refused("unsupported audio encoding in capped session"));
        }
    }
    let Some(native) = &session.native else {
        return Ok(());
    };
    let native = Value::Object(native.fields.clone());
    let Some(session) = native.get("session") else {
        return Ok(());
    };
    if !session.is_object() {
        return Err(BadRequest::fault("session must be an object").with(CloseCode(1008)));
    }
    for path in ["/audio", "/audio/input", "/audio/output"] {
        if session.pointer(path).is_some_and(|v| !v.is_object()) {
            return Err(
                BadRequest::fault("audio configuration must be an object").with(CloseCode(1008))
            );
        }
    }
    for path in ["/input_audio_transcription", "/audio/input/transcription"] {
        if session.pointer(path).is_some_and(|v| !v.is_null()) {
            return Err(refused(
                "capped sessions disable separately billed transcription",
            ));
        }
    }
    for path in [
        "/input_audio_format",
        "/output_audio_format",
        "/audio/input/format",
        "/audio/output/format",
    ] {
        let Some(format) = session.pointer(path) else {
            continue;
        };
        let valid = match format.as_str() {
            Some("pcm16" | "g711_ulaw" | "g711_alaw") => true,
            Some(_) => false,
            // The kind took the type of a format it knows.
            None if format.get("type").is_none() => true,
            None => match format["type"].as_str() {
                Some("audio/pcm") => format["rate"].as_u64() == Some(24_000),
                Some("audio/pcmu" | "audio/pcma") => true,
                _ => false,
            },
        };
        if !valid {
            return Err(refused("unsupported audio encoding in capped session"));
        }
    }
    Ok(())
}

/// Reject separately billed transcription and unknown encodings in capped
/// sessions; with `Validate(true)`, turns are the client's (`Turns::Manual`).
pub(super) struct Validate(pub bool);
#[async_trait]
impl Plugin for Validate {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let manual = self.0;
        if manual {
            cx.wrap(|events: EventStream<Input>| {
                let mut opening = Session::default();
                opening.turns = Some(Turns::Manual);
                stream::once(async { Ok(Input::Session(opening)) }).chain(events)
            })?;
        }
        cx.filter(move |event: &mut Input| {
            let Input::Session(session) = event else {
                return Ok(());
            };
            check(session)?;
            // The client's own turn detection is replaced by the opening's.
            if manual && session.turns.is_some() {
                session.turns = Some(Turns::Manual);
                // A beta spelling of turn detection, which the kind has no form for.
            }
            if manual {
                let beta = session
                    .native
                    .as_mut()
                    .and_then(|n| n.fields.get_mut("session"));
                if let Some(turns) = beta.and_then(|s| s.get_mut("turn_detection")) {
                    *turns = Value::Null;
                }
            }
            Ok(())
        })?;
        Ok(None)
    }
}
