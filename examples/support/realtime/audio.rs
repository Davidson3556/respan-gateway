//! Audio validation and a conservative token bound for PCM24k and G.711.
//! Rates: https://developers.openai.com/api/docs/guides/voice-latency-cost
use base64::{Engine, engine::general_purpose::STANDARD};
use respan_gateway_core::futures::{StreamExt, stream};
use respan_gateway_core::{BadRequest, ErrorCode};
use respan_gateway_core::{ClientContext, EventStream, Fault, Plugin, Response, async_trait};
use respan_gateway_realtime::Input;
use respan_gateway_ws::CloseCode;
use serde_json::{Value, json};

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
    // The fastest supported encoding is 8kHz G.711: 400 bytes per output token.
    // PCM24k needs more bytes per token. Using the faster rate bounds both directions.
    Ok(i64::try_from(bytes.len().div_ceil(400)).unwrap_or(i64::MAX))
}

/// Reject separately billed transcription and unknown encodings in capped sessions.
pub(super) struct Validate(pub bool);
#[async_trait]
impl Plugin for Validate {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let manual = self.0;
        if manual {
            cx.wrap(|events: EventStream<Input>| {
                let manual = Input::native(
                    "openai-realtime",
                    json!({
                        "type": "session.update", "session": {"type": "realtime",
                        "audio": {"input": {"turn_detection": null}}}
                    }),
                );
                stream::once(async { Ok(manual) }).chain(events)
            })?;
        }
        cx.filter(move |event: &mut Input| {
            let Input::Native(event) = event else {
                return Ok(());
            };
            if event.value["type"] != "session.update" {
                return Ok(());
            }
            let session = &event.value["session"];
            if !session.is_object() {
                return Err(BadRequest::fault("session must be an object").with(CloseCode(1008)));
            }
            for path in ["/audio", "/audio/input", "/audio/output"] {
                if session
                    .pointer(path)
                    .is_some_and(|value| !value.is_object())
                {
                    return Err(BadRequest::fault("audio configuration must be an object")
                        .with(CloseCode(1008)));
                }
            }
            for path in ["/input_audio_transcription", "/audio/input/transcription"] {
                if session.pointer(path).is_some_and(|v| !v.is_null()) {
                    return Err(Fault::new(
                        "capped sessions disable separately billed transcription",
                    )
                    .with(ErrorCode("audio_policy".into()))
                    .with(CloseCode(1008)));
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
                    None => match format["type"].as_str() {
                        Some("audio/pcm") => format["rate"].as_u64() == Some(24_000),
                        Some("audio/pcmu" | "audio/pcma") => true,
                        _ => false,
                    },
                };
                if !valid {
                    return Err(Fault::new("unsupported audio encoding in capped session")
                        .with(ErrorCode("audio_policy".into()))
                        .with(CloseCode(1008)));
                }
            }
            if manual {
                let session = &mut std::sync::Arc::make_mut(&mut event.value)["session"];
                session["audio"]["input"]["turn_detection"] = Value::Null;
                if session.get("turn_detection").is_some() {
                    session["turn_detection"] = Value::Null;
                }
            }
            Ok(())
        })?;
        Ok(None)
    }
}
