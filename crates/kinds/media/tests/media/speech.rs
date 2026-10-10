//! Text to speech.

use respan_gateway_core::{ClientContext, Fault, Plugin, Response, async_trait};
use respan_gateway_http::http::StatusCode;
use respan_gateway_media::openai::OpenAiSpeech;
use respan_gateway_media::speech;
use serde_json::{Value, json};

use crate::support::{
    Fake, content_type, events_of, gateway, json_of, json_request, send, send_frames, spoken_audio,
    unb64,
};

#[tokio::test]
async fn audio_streams_through_intact_chunk_by_chunk() {
    let fake = Fake::openai().await;
    let gateway = gateway(OpenAiSpeech, &fake);
    let asked = json!({"model": "tts-1", "input": "Hello there.", "voice": "alloy", "speed": 1.25});
    let (status, headers, frames) =
        send_frames(&gateway, json_request("/v1/audio/speech", asked.clone())).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(content_type(&headers), "audio/mpeg");
    assert!(frames.len() > 1, "the audio came in {} frame", frames.len());
    assert_eq!(frames.concat(), spoken_audio().concat());
    assert_eq!(Value::Object(fake.seen()[0].fields.clone()), asked);
}

#[tokio::test]
async fn audio_is_typed_as_its_format() {
    let fake = Fake::openai().await;
    let gateway = gateway(OpenAiSpeech, &fake);
    let asked =
        json!({"model": "tts-1-hd", "input": "Hi", "voice": "nova", "response_format": "wav"});
    let (status, headers, body) = send(&gateway, json_request("/v1/audio/speech", asked)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(content_type(&headers), "audio/wav");
    assert_eq!(body.as_ref(), spoken_audio().concat());
}

#[tokio::test]
async fn audio_events_stream_through_with_their_usage() {
    let fake = Fake::openai().await;
    let gateway = gateway(OpenAiSpeech, &fake);
    let asked = json!({
        "model": "gpt-4o-mini-tts",
        "input": "Hello there.",
        "voice": "coral",
        "instructions": "Cheerfully.",
        "stream_format": "sse",
    });
    let (status, headers, body) =
        send(&gateway, json_request("/v1/audio/speech", asked.clone())).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert_eq!(content_type(&headers), "text/event-stream");
    let events = events_of(&body);
    let (done, deltas) = events.split_last().expect("events");
    let audio: Vec<u8> = deltas
        .iter()
        .inspect(|e| assert_eq!(e["type"], "speech.audio.delta"))
        .flat_map(|e| unb64(&e["audio"]))
        .collect();
    assert_eq!(audio, spoken_audio().concat());
    assert_eq!(
        *done,
        json!({
            "type": "speech.audio.done",
            "usage": {"input_tokens": 5, "output_tokens": 40, "total_tokens": 45},
        })
    );
    assert_eq!(Value::Object(fake.seen()[0].fields.clone()), asked);
}

#[tokio::test]
async fn speech_without_input_is_refused_before_any_call() {
    let fake = Fake::openai().await;
    let gateway = gateway(OpenAiSpeech, &fake);
    let asked = json!({"model": "tts-1", "voice": "alloy"});
    let (status, _, body) = send(&gateway, json_request("/v1/audio/speech", asked)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(json_of(&body)["error"]["param"], "input");
    assert!(fake.seen().is_empty());
}

#[tokio::test]
async fn a_plugin_reads_and_edits_the_typed_input() {
    struct Polite;

    #[async_trait]
    impl Plugin for Polite {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(|speech: &mut speech::Input| {
                speech.text = format!("Please: {}", speech.text);
                Ok(())
            })?;
            Ok(None)
        }
    }

    let fake = Fake::openai().await;
    let gateway = gateway(OpenAiSpeech, &fake).plugin(Polite);
    let asked = json!({"model": "tts-1", "input": "go", "voice": "alloy"});
    let (status, _, _) = send(&gateway, json_request("/v1/audio/speech", asked)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(fake.seen()[0].fields["input"], "Please: go");
}
