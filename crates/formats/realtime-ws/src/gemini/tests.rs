use std::sync::Arc;

use respan_gateway_core::{BadRequest, Fault};
use respan_gateway_http::bytes::Bytes;
use respan_gateway_realtime::{AudioFormat, Input, Modality, Output, Status, Turns};
use respan_gateway_ws::CloseCode;
use serde_json::{Value, json};

use super::{Applied, input, lock, output};

fn inputs(messages: &[Value]) -> Result<Vec<Input>, Fault> {
    let mut client = input::Client::default();
    let mut events = Vec::new();
    for message in messages {
        client.read(Bytes::from(message.to_string()), &mut events)?;
    }
    Ok(events)
}

fn sent(applied: &Applied, events: &[Input]) -> Result<Vec<Value>, Fault> {
    let mut provider = input::Provider::new(applied.clone());
    let mut messages = Vec::new();
    for event in events {
        provider.write(event, &mut messages)?;
    }
    Ok(messages
        .iter()
        .map(|m| serde_json::from_slice(m).expect("JSON"))
        .collect())
}

fn outputs(applied: &Applied, messages: &[Value]) -> Vec<Output> {
    let mut provider = output::Provider::new(applied.clone());
    let mut events = Vec::new();
    for message in messages {
        provider
            .read(Bytes::from(message.to_string()), &mut events)
            .expect("read");
    }
    events
}

fn heard(events: &[Output]) -> Result<Vec<Value>, Fault> {
    let mut client = output::Client::default();
    let mut messages = Vec::new();
    for event in events {
        client.write(event, &mut messages)?;
    }
    Ok(messages
        .iter()
        .map(|m| serde_json::from_slice(m).expect("JSON"))
        .collect())
}

const SETUP: &str = r#"{"setup": {"model": "models/gemini-live",
    "generationConfig": {"responseModalities": ["AUDIO"], "temperature": 0.4,
        "speechConfig": {"voiceConfig": {"prebuiltVoiceConfig": {"voiceName": "Kore"}}},
        "maxOutputTokens": 900},
    "systemInstruction": {"parts": [{"text": "Be brief."}]},
    "tools": [{"functionDeclarations": [{"name": "weather", "description": "The weather.",
        "parameters": {"type": "OBJECT"}, "behavior": "NON_BLOCKING"}]}],
    "realtimeInputConfig": {"automaticActivityDetection": {"silenceDurationMs": 500,
        "startOfSpeechSensitivity": "START_SENSITIVITY_LOW"},
        "activityHandling": "NO_INTERRUPTION", "turnCoverage": "TURN_INCLUDES_ALL_INPUT"},
    "inputAudioTranscription": {}, "outputAudioTranscription": {},
    "sessionResumption": {"handle": "h1"}}}"#;

#[test]
fn every_client_message_is_written_back_as_it_came() {
    let messages = [
        serde_json::from_str(SETUP).expect("JSON"),
        json!({"realtimeInput": {"audio": {"mimeType": "audio/pcm;rate=16000", "data": "AAEC"}}}),
        json!({"realtimeInput": {"text": "Hi there"}}),
        json!({"clientContent": {"turns": [{"role": "user", "parts": [{"text": "a"}]},
            {"role": "model", "parts": [{"text": "b"}, {"inlineData": {"mimeType": "image/png",
                "data": "AA=="}}]}], "turnComplete": true}}),
        json!({"clientContent": {"turns": [{"role": "user", "parts": [{"text": "seed"}]}]}}),
        json!({"clientContent": {"turns": [{"role": "user", "parts": [{"text": "one"}]}],
            "turnComplete": true}}),
        json!({"clientContent": {"turnComplete": true}}),
        json!({"realtimeInput": {"activityStart": {}}}),
        json!({"realtimeInput": {"activityEnd": {}}}),
        json!({"toolResponse": {"functionResponses": [
            {"id": "c1", "name": "weather", "response": {"celsius": 21}},
            {"id": "c2", "response": {"ok": true}, "scheduling": "SILENT"}]}}),
        json!({"realtimeInput": {"audioStreamEnd": true}}),
        json!({"clientContent": {"turnComplete": false}}),
        json!({"toolResponse": {"functionResponses": [{"id": "c3", "response": "text"}]}}),
    ];
    let events = inputs(&messages).expect("written");
    let kinds: Vec<&str> = events.iter().map(kind).collect();
    assert_eq!(
        kinds,
        [
            "session",
            "audio",
            "message",
            "respond",
            "message",
            "message",
            "respond",
            "message",
            "message",
            "respond",
            "respond",
            "native",
            "commit",
            "respond",
            "tool_result",
            "tool_result",
            "respond",
            "native",
            "native",
            "native"
        ]
    );
    let applied = Applied::default();
    assert_eq!(sent(&applied, &events).expect("written"), messages);
    let Some(session) = lock(&applied).clone() else {
        panic!("no setup")
    };
    assert_eq!(session.model.as_deref(), Some("gemini-live"));
    assert_eq!(session.output, Some(Modality::Audio));
    assert_eq!(session.voice.as_deref(), Some("Kore"));
    assert_eq!(session.instructions.as_deref(), Some("Be brief."));
    assert_eq!(session.max_output_tokens, Some(900));
    assert_eq!(session.tools.as_ref().map(Vec::len), Some(1));
    let Some(Turns::Server(vad)) = &session.turns else {
        panic!("{session:?}")
    };
    assert_eq!((vad.silence_ms, vad.interrupt), (Some(500), Some(false)));
    assert_eq!(
        (session.input_transcription, session.output_transcription),
        (Some(true), Some(true))
    );
    assert_eq!(session.output_audio, Some(AudioFormat::pcm16(24_000)));
    let Input::Audio { audio, .. } = &events[1] else {
        panic!("{events:?}")
    };
    assert_eq!(audio.format, AudioFormat::pcm16(16_000));
}

fn kind(event: &Input) -> &'static str {
    match event {
        Input::Session(_) => "session",
        Input::Audio { .. } => "audio",
        Input::Commit { .. } => "commit",
        Input::Message { .. } => "message",
        Input::ToolResult(_) => "tool_result",
        Input::Respond { .. } => "respond",
        _ => "native",
    }
}

#[test]
fn a_client_must_set_up_first_and_once_one_thing_a_message() {
    let setup: Value = json!({"setup": {"model": "models/m"}});
    let text = json!({"realtimeInput": {"text": "hi"}});
    for (messages, refused) in [
        (vec![text.clone()], "first"),
        (vec![setup.clone(), setup.clone()], "once"),
        (
            vec![
                setup.clone(),
                json!({"realtimeInput": {}, "toolResponse": {}}),
            ],
            "one",
        ),
        (vec![json!("setup")], "one"),
    ] {
        let fault = inputs(&messages).expect_err("refused");
        assert!(fault.is::<BadRequest>(), "{fault}");
        assert_eq!(
            fault.get::<CloseCode>(),
            Some(&CloseCode(1007)),
            "{refused}"
        );
    }
}

#[test]
fn a_session_is_set_up_once_and_first() {
    let applied = Applied::default();
    let respond = Input::Respond { native: None };
    assert!(sent(&applied, &[respond]).is_err());
    let events = inputs(&[json!({"setup": {"model": "models/m"}})]).expect("written");
    let twice = [events[0].clone(), events[0].clone()];
    let fault = sent(&applied, &twice).expect_err("refused");
    assert_eq!(fault.get::<CloseCode>(), Some(&CloseCode(1008)));
}

#[test]
fn every_server_message_is_written_back_as_it_came() {
    let applied: Applied = Arc::default();
    let usage = json!({"promptTokenCount": 40, "responseTokenCount": 12,
        "cachedContentTokenCount": 0, "totalTokenCount": 52,
        "promptTokensDetails": [{"modality": "TEXT", "tokenCount": 10},
            {"modality": "AUDIO", "tokenCount": 30}],
        "responseTokensDetails": [{"modality": "AUDIO", "tokenCount": 12}]});
    let messages = [
        json!({"setupComplete": {}}),
        json!({"serverContent": {"inputTranscription": {"text": "what time"}}}),
        json!({"serverContent": {"modelTurn": {"parts": [{"inlineData":
            {"mimeType": "audio/pcm;rate=24000", "data": "AAEC"}}]}}}),
        json!({"serverContent": {"outputTranscription": {"text": "Noon."}}}),
        json!({"serverContent": {"generationComplete": true}}),
        json!({"serverContent": {"turnComplete": true}, "usageMetadata": usage}),
        json!({"voiceActivity": {"voiceActivityType": "ACTIVITY_START"}}),
        json!({"serverContent": {"modelTurn": {"parts": [{"text": "Once"}]}}}),
        json!({"serverContent": {"interrupted": true}}),
        json!({"serverContent": {"turnComplete": true}}),
        json!({"serverContent": {"modelTurn": {"parts": [{"text": "Let me check."}]}}}),
        json!({"toolCall": {"functionCalls": [{"id": "c1", "name": "weather",
            "args": {"city": "Paris"}}, {"id": "c2", "name": "time", "args": {}}]}}),
        json!({"toolCallCancellation": {"ids": ["c2"]}}),
        json!({"serverContent": {"modelTurn": {"parts": [{"text": "21."}]},
            "turnComplete": true, "turnCompleteReason": "MALFORMED_FUNCTION_CALL"}}),
        json!({"usageMetadata": {"promptTokenCount": 3}}),
        json!({"serverContent": {"interrupted": true}}),
        json!({"goAway": {"timeLeft": "10s"}}),
        json!({"sessionResumptionUpdate": {"newHandle": "h2", "resumable": true}}),
    ];
    let events = outputs(&applied, &messages);
    let statuses: Vec<&Status> = events
        .iter()
        .filter_map(|e| match e {
            Output::ResponseDone { status, .. } => Some(status),
            _ => None,
        })
        .collect();
    assert_eq!(
        statuses,
        [
            &Status::Completed,
            &Status::Interrupted,
            &Status::Completed,
            &Status::Failed
        ]
    );
    let used = events.iter().find_map(|e| match e {
        Output::Usage(used) => Some(*used),
        _ => None,
    });
    let used = used.expect("usage");
    assert_eq!(
        (used.tokens.input_tokens, used.tokens.output_tokens),
        (40, 12)
    );
    assert_eq!(
        (used.input_audio_tokens, used.output_audio_tokens),
        (30, 12)
    );
    let started = events
        .iter()
        .filter(|e| matches!(e, Output::SpeechStarted { .. }))
        .count();
    assert_eq!(
        started, 2,
        "the voice activity, and the interruption after no response"
    );
    let mut written = heard(&events).expect("written");
    // The turn of a part and its end is written as two messages, saying the same.
    let ended = written.remove(14);
    assert_eq!(
        ended,
        json!({"serverContent": {"turnComplete": true,
        "turnCompleteReason": "MALFORMED_FUNCTION_CALL"}})
    );
    written[13]["serverContent"]["turnComplete"] = true.into();
    written[13]["serverContent"]["turnCompleteReason"] = "MALFORMED_FUNCTION_CALL".into();
    assert_eq!(written, messages);
}

#[test]
fn the_setup_written_is_the_session_its_completion_says() {
    let applied = Applied::default();
    let events = inputs(&[json!({"setup": {"model": "models/m"}})]).expect("written");
    sent(&applied, &events).expect("written");
    let events = outputs(&applied, &[json!({"setupComplete": {}})]);
    let [Output::Updated(session)] = events.as_slice() else {
        panic!("{events:?}")
    };
    assert_eq!(session.model.as_deref(), Some("m"));
}

#[test]
fn a_refusal_closes_a_live_client() {
    let error = Output::Error {
        code: Some("invalid_request_error".into()),
        message: "no".into(),
        native: None,
    };
    let fault = heard(&[error]).expect_err("refused");
    assert_eq!(fault.get::<CloseCode>(), Some(&CloseCode(1007)));
    let foreign = Output::native("openai-realtime", json!({"type": "x"}));
    assert!(heard(&[foreign]).is_err());
}
