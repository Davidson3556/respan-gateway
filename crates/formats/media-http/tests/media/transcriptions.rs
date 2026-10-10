//! Transcriptions and translations.

use respan_gateway_http::http::StatusCode;
use respan_gateway_media_http::openai::OpenAiTranscriptions;
use serde_json::{Value, json};

use crate::support::{
    Fake, Form, Rules, SPOKEN, SUBRIP, content_type, events_of, gateway, json_of, send,
};

const WAV: &[u8] = b"RIFF\x24\0\0\0WAVEfmt fake audio";

fn audio(form: Form) -> Form {
    form.file("file", "speech.wav", "audio/wav", WAV)
}

#[tokio::test]
async fn a_json_transcript_keeps_its_token_usage_and_details() {
    let fake = Fake::openai().await;
    let gateway = gateway(OpenAiTranscriptions, &fake);
    let form = audio(Form::default())
        .text("model", "gpt-4o-transcribe")
        .text("language", "en")
        .text("temperature", "0.2")
        .text("include[]", "logprobs");
    let (status, headers, body) = send(&gateway, form.to("/v1/audio/transcriptions")).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert_eq!(content_type(&headers), "application/json");
    assert_eq!(
        json_of(&body),
        json!({
            "text": SPOKEN,
            "usage": {
                "type": "tokens", "input_tokens": 14, "output_tokens": 7, "total_tokens": 21,
                "input_token_details": {"text_tokens": 0, "audio_tokens": 14},
            },
        })
    );
    let call = &fake.seen()[0];
    assert_eq!(call.path, "/v1/audio/transcriptions");
    assert_eq!(call.files.len(), 1);
    assert_eq!(call.files[0].bytes.as_ref(), WAV);
    assert_eq!(call.files[0].filename.as_deref(), Some("speech.wav"));
    assert_eq!(call.files[0].content_type.as_deref(), Some("audio/wav"));
    assert_eq!(
        Value::Object(call.fields.clone()),
        json!({
            "model": "gpt-4o-transcribe",
            "language": "en",
            "temperature": "0.2",
            "include[]": ["logprobs"],
        })
    );
}

#[tokio::test]
async fn a_verbose_transcript_keeps_its_segments_words_and_vendor_fields() {
    let mut extra_answer = serde_json::Map::new();
    extra_answer.insert("x_groq".into(), json!({"id": "req_01"}));
    let fake = Fake::start(Rules {
        extra_answer,
        no_streamed_transcription: true,
        ..Rules::default()
    })
    .await;
    let gateway = gateway(OpenAiTranscriptions, &fake);
    let form = audio(Form::default())
        .text("model", "whisper-large-v3")
        .text("response_format", "verbose_json")
        .text("timestamp_granularities[]", "word")
        .text("timestamp_granularities[]", "segment");
    let (status, _, body) = send(&gateway, form.to("/openai/v1/audio/transcriptions")).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let v = json_of(&body);
    assert_eq!(v["task"], "transcribe");
    assert_eq!(v["language"], "english");
    assert_eq!(v["duration"], 3.0);
    assert_eq!(v["text"], SPOKEN);
    assert_eq!(v["usage"], json!({"type": "duration", "seconds": 3}));
    assert_eq!(v["x_groq"], json!({"id": "req_01"}));
    assert_eq!(
        v["words"],
        json!([{"word": "Hello", "start": 0.0, "end": 0.5}])
    );
    let segments = v["segments"].as_array().expect("segments");
    assert_eq!(segments.len(), 2);
    assert_eq!(
        segments[1],
        json!({
            "start": 1.5, "end": 3.0, "text": " General Kenobi.", "id": 1, "seek": 0,
            "tokens": [50364, 2425], "temperature": 0.0, "avg_logprob": -0.25,
            "compression_ratio": 0.8, "no_speech_prob": 0.01,
        })
    );
    let sent = &fake.seen()[0].fields;
    assert_eq!(
        sent["timestamp_granularities[]"],
        json!(["word", "segment"])
    );
}

#[tokio::test]
async fn rendered_subrip_goes_back_as_the_provider_wrote_it() {
    let fake = Fake::openai().await;
    let gateway = gateway(OpenAiTranscriptions, &fake);
    let form = audio(Form::default())
        .text("model", "whisper-1")
        .text("response_format", "srt");
    let (status, headers, body) = send(&gateway, form.to("/v1/audio/transcriptions")).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert_eq!(content_type(&headers), "text/plain; charset=utf-8");
    assert_eq!(body.as_ref(), SUBRIP.as_bytes());
}

#[tokio::test]
async fn a_streamed_transcript_streams_its_deltas_and_ends_with_its_usage() {
    let fake = Fake::openai().await;
    let gateway = gateway(OpenAiTranscriptions, &fake);
    let form = audio(Form::default())
        .text("model", "gpt-4o-mini-transcribe")
        .text("stream", "true");
    let (status, headers, body) = send(&gateway, form.to("/v1/audio/transcriptions")).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert_eq!(content_type(&headers), "text/event-stream");
    let events = events_of(&body);
    let deltas: Vec<_> = events
        .iter()
        .filter(|e| e["type"] == "transcript.text.delta")
        .map(|e| e["delta"].as_str().unwrap_or_default())
        .collect();
    assert_eq!(deltas, ["Hello", " there.", " General", " Kenobi."]);
    let done = events.last().expect("an end");
    assert_eq!(done["type"], "transcript.text.done");
    assert_eq!(done["text"], SPOKEN);
    assert_eq!(done["usage"]["input_token_details"]["audio_tokens"], 14);
    assert_eq!(fake.seen()[0].fields["stream"], "true");
}

#[tokio::test]
async fn a_translation_goes_to_its_endpoint() {
    let fake = Fake::openai().await;
    let gateway = gateway(OpenAiTranscriptions, &fake);
    let form = audio(Form::default())
        .text("model", "whisper-1")
        .text("response_format", "text");
    let (status, headers, body) = send(&gateway, form.to("/v1/audio/translations")).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert_eq!(content_type(&headers), "text/plain; charset=utf-8");
    assert_eq!(body.as_ref(), format!("{SPOKEN}\n").as_bytes());
    assert_eq!(fake.seen()[0].path, "/v1/audio/translations");
}

#[tokio::test]
async fn audio_without_a_file_is_refused_before_any_call() {
    let fake = Fake::openai().await;
    let gateway = gateway(OpenAiTranscriptions, &fake);
    let form = Form::default().text("model", "whisper-1");
    let (status, _, body) = send(&gateway, form.to("/v1/audio/transcriptions")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(json_of(&body)["error"]["param"], "file");
    assert!(fake.seen().is_empty());
}

#[tokio::test]
async fn a_vendor_that_cannot_stream_answers_in_openais_schema() {
    let fake = Fake::start(Rules {
        no_streamed_transcription: true,
        ..Rules::default()
    })
    .await;
    let gateway = gateway(OpenAiTranscriptions, &fake);
    let form = audio(Form::default())
        .text("model", "gpt-4o-transcribe")
        .text("stream", "true");
    let (status, headers, body) = send(&gateway, form.to("/v1/audio/transcriptions")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(content_type(&headers), "application/json");
    let error = &json_of(&body)["error"];
    assert_eq!(error["param"], "stream");
    assert_eq!(error["code"], "invalid_value");
    assert_eq!(
        error["message"],
        "Streaming is not supported for this model."
    );
}
