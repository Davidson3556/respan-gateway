use respan_gateway_core::{BadRequest, NativeFields, Upstream};
use respan_gateway_http::bytes::Bytes;
use respan_gateway_realtime::{AudioFormat, Input, Output, Session, Status, ToolResult};
use serde_json::{Value, json};

use super::{input, output};

/// The events of `frames`, a client's, after the session its handshake opens.
fn inputs(frames: &[Value]) -> Vec<Input> {
    let mut client = input::Client::new(Some("gpt-realtime".into()));
    let mut events = Vec::new();
    for frame in frames {
        client
            .read(Bytes::from(frame.to_string()), &mut events)
            .expect("read");
    }
    events
}

/// What `events` are written as, to a provider.
fn sent(events: &[Input]) -> Result<Vec<Value>, respan_gateway_core::Fault> {
    let mut provider = input::Provider::default();
    let mut frames = Vec::new();
    for event in events {
        provider.write(event, &mut frames)?;
    }
    Ok(frames
        .iter()
        .map(|f| serde_json::from_slice(f).expect("JSON"))
        .collect())
}

fn outputs(frames: &[Value]) -> Vec<Output> {
    let mut provider = output::Provider::default();
    let mut events = Vec::new();
    for frame in frames {
        provider
            .read(Bytes::from(frame.to_string()), &mut events)
            .expect("read");
    }
    events
}

fn heard(events: &[Output]) -> Result<Vec<Value>, respan_gateway_core::Fault> {
    let mut client = output::Client::default();
    let mut frames = Vec::new();
    for event in events {
        client.write(event, &mut frames)?;
    }
    Ok(frames
        .iter()
        .map(|f| serde_json::from_slice(f).expect("JSON"))
        .collect())
}

#[test]
fn every_client_event_is_written_back_as_it_came() {
    let frames = [
        json!({"type": "session.update", "event_id": "e1", "session": {"type": "realtime",
            "instructions": "Be brief.", "output_modalities": ["audio"],
            "audio": {"input": {"format": {"type": "audio/pcm", "rate": 24000},
                "transcription": {"model": "gpt-4o-mini-transcribe", "language": "en"},
                "noise_reduction": {"type": "far_field"},
                "turn_detection": {"type": "semantic_vad", "eagerness": "low",
                    "create_response": true, "interrupt_response": false}},
                "output": {"format": {"type": "audio/pcmu"}, "voice": "marin", "speed": 1.1}},
            "tools": [{"type": "function", "name": "f", "parameters": {"type": "object"}}],
            "tool_choice": {"type": "function", "name": "f"}, "max_output_tokens": "inf"}}),
        json!({"type": "input_audio_buffer.append", "event_id": "e2", "audio": "AAEC"}),
        json!({"type": "input_audio_buffer.commit"}),
        json!({"type": "conversation.item.create", "previous_item_id": "root",
            "item": {"id": "i1", "type": "message", "role": "user",
                "content": [{"type": "input_text", "text": "hi"},
                    {"type": "input_image", "image_url": "data:image/png;base64,AA=="}]}}),
        json!({"type": "conversation.item.create", "item": {"type": "message",
            "role": "assistant", "content": [{"type": "output_text", "text": "hello"}]}}),
        json!({"type": "conversation.item.create", "item": {"type": "function_call_output",
            "call_id": "c1", "output": "{\"ok\":true}"}}),
        json!({"type": "response.create", "response": {"instructions": "short",
            "max_output_tokens": 200}}),
        json!({"type": "response.cancel", "event_id": "e3"}),
        json!({"type": "conversation.item.truncate", "item_id": "i2", "content_index": 0,
            "audio_end_ms": 10}),
        json!({"type": "input_audio_buffer.append", "audio": "not base64!"}),
        json!({"type": "session.update", "session": {"type": "realtime", "modalities": ["text"]}}),
        json!("not an event"),
    ];
    let events = inputs(&frames);
    let kinds: Vec<&str> = events.iter().map(kind).collect();
    assert_eq!(
        kinds,
        [
            "session",
            "session",
            "audio",
            "commit",
            "message",
            "message",
            "tool_result",
            "respond",
            "cancel",
            "native",
            "native",
            "session",
            "native"
        ]
    );
    assert_eq!(sent(&events).expect("written"), frames);
}

fn kind(event: &Input) -> &'static str {
    match event {
        Input::Session(_) => "session",
        Input::Audio { .. } => "audio",
        Input::Commit { .. } => "commit",
        Input::Message { .. } => "message",
        Input::ToolResult(_) => "tool_result",
        Input::Respond { .. } => "respond",
        Input::Cancel { .. } => "cancel",
        _ => "native",
    }
}

#[test]
fn a_client_event_is_read_as_the_kinds() {
    let frames = [
        json!({"type": "session.update", "session": {"type": "realtime",
            "audio": {"input": {"format": {"type": "audio/pcma"}}}}}),
        json!({"type": "input_audio_buffer.append", "audio": "AAE"}),
    ];
    let events = inputs(&frames);
    let Input::Session(opening) = &events[0] else {
        panic!("{events:?}")
    };
    assert_eq!(opening.model.as_deref(), Some("gpt-realtime"));
    let Input::Session(session) = &events[1] else {
        panic!("{events:?}")
    };
    assert_eq!(session.input_audio, Some(AudioFormat::ALAW));
    let Input::Audio { audio, native } = &events[2] else {
        panic!("{events:?}")
    };
    assert_eq!(
        (audio.format, &audio.data[..]),
        (AudioFormat::ALAW, &[0, 1][..])
    );
    assert_eq!(*native, None);
    // Written padded: the same audio.
    assert_eq!(sent(&events).expect("written")[1]["audio"], "AAE=");
}

#[test]
fn a_session_of_nothing_but_its_model_is_no_frame() {
    let mut opening = Session::default();
    opening.model = Some("gpt-realtime".into());
    let session = Input::Session(opening);
    let respond = Input::Respond { native: None };
    // A session a plugin opens with says something: it is written, typed.
    let mut capped = Session::default();
    capped.max_output_tokens = Some(100);
    let written = sent(&[Input::Session(capped), session, respond]).expect("written");
    assert_eq!(
        written,
        [
            json!({"type": "session.update", "session": {"type": "realtime",
                "max_output_tokens": 100}}),
            json!({"type": "response.create"})
        ]
    );
    let result = Input::ToolResult(ToolResult::new("c1", "{}"));
    assert_eq!(
        sent(&[result]).expect("written"),
        [json!({"type": "conversation.item.create",
            "item": {"type": "function_call_output", "call_id": "c1", "output": "{}"}})]
    );
}

#[test]
fn another_formats_input_is_refused() {
    let foreign = || Some(NativeFields::new("gemini-live", Default::default()));
    for event in [
        Input::Respond { native: foreign() },
        Input::native("gemini-live", json!({"clientContent": {}})),
    ] {
        let refused = sent(&[event]).expect_err("refused");
        assert!(refused.is::<BadRequest>(), "{refused}");
        assert!(refused.message().contains("cannot be translated"));
    }
    let mut session = Session::default();
    session.instructions = Some("x".into());
    let audio = respan_gateway_realtime::Audio {
        data: Bytes::from_static(&[0; 4]),
        format: AudioFormat::pcm16(16_000),
    };
    let audio = Input::Audio {
        audio,
        native: None,
    };
    assert!(sent(&[Input::Session(session), audio]).is_err());
}

#[test]
fn every_server_event_is_written_back_as_it_came() {
    let usage = json!({"total_tokens": 7, "input_tokens": 5, "output_tokens": 2,
        "input_token_details": {"cached_tokens": 0, "text_tokens": 1, "audio_tokens": 4},
        "output_token_details": {"text_tokens": 2, "audio_tokens": 0}});
    let frames = [
        json!({"type": "session.created", "event_id": "s1", "session": {"type": "realtime",
            "object": "realtime.session", "id": "sess_1", "model": "gpt-realtime",
            "output_modalities": ["audio"], "max_output_tokens": "inf", "tools": [],
            "tool_choice": "auto", "instructions": "",
            "audio": {"input": {"format": {"type": "audio/pcm", "rate": 24000},
                "transcription": null, "turn_detection": {"type": "server_vad",
                    "threshold": 0.5, "silence_duration_ms": 200, "idle_timeout_ms": null}},
                "output": {"format": {"type": "audio/pcmu", "rate": 24000}, "voice": "alloy"}}}}),
        json!({"type": "input_audio_buffer.speech_started", "audio_start_ms": 0,
            "item_id": "i1", "event_id": "s2"}),
        json!({"type": "input_audio_buffer.speech_stopped", "audio_end_ms": 900,
            "item_id": "i1"}),
        json!({"type": "conversation.item.input_audio_transcription.delta", "item_id": "i1",
            "content_index": 0, "delta": "hel"}),
        json!({"type": "conversation.item.input_audio_transcription.completed",
            "item_id": "i1", "content_index": 0, "transcript": "hello",
            "usage": {"type": "duration", "seconds": 1}}),
        json!({"type": "response.created", "response": {"id": "r1", "status": "in_progress",
            "output": [], "usage": null}}),
        json!({"type": "response.output_item.added", "response_id": "r1", "output_index": 1,
            "item": {"id": "i3", "type": "function_call", "call_id": "c1", "name": "f"}}),
        json!({"type": "response.output_audio.delta", "response_id": "r1", "item_id": "i2",
            "output_index": 0, "content_index": 0, "delta": "AAEC"}),
        json!({"type": "response.output_text.delta", "item_id": "i2", "delta": "hi"}),
        json!({"type": "response.output_audio_transcript.delta", "item_id": "i2", "delta": "hi"}),
        json!({"type": "response.function_call_arguments.delta", "call_id": "c1", "delta": "{"}),
        json!({"type": "response.function_call_arguments.done", "call_id": "c1",
            "item_id": "i3", "name": "f", "arguments": "{}"}),
        json!({"type": "response.done", "response": {"id": "r1", "status": "completed",
            "status_details": null, "usage": usage}}),
        json!({"type": "response.done", "response": {"status": "cancelled",
            "status_details": {"type": "cancelled", "reason": "turn_detected"}}}),
        json!({"type": "response.done", "response": {"status": "failed",
            "status_details": {"type": "failed", "error": {"code": "x"}}}}),
        json!({"type": "response.done", "response": {"status": "incomplete",
            "status_details": {"type": "incomplete", "reason": "something_new"}}}),
        json!({"type": "error", "event_id": "s3", "error": {"type": "invalid_request_error",
            "code": "invalid_value", "message": "no", "param": null, "event_id": "e9"}}),
        json!({"type": "rate_limits.updated", "rate_limits": []}),
    ];
    let events = outputs(&frames);
    let statuses: Vec<&Status> = events
        .iter()
        .filter_map(|e| match e {
            Output::ResponseDone { status, .. } => Some(status),
            _ => None,
        })
        .collect();
    assert_eq!(
        statuses[..3],
        [&Status::Completed, &Status::Interrupted, &Status::Failed]
    );
    assert!(matches!(statuses[3], Status::Native { reason, .. } if reason == "incomplete"));
    let Output::Usage(used) = &events[12] else {
        panic!("{events:?}")
    };
    assert_eq!((used.tokens.input_tokens, used.input_audio_tokens), (5, 4));
    let Output::Audio { audio, .. } = &events[7] else {
        panic!("{events:?}")
    };
    assert_eq!(audio.format, AudioFormat::MULAW);
    assert_eq!(heard(&events).expect("written"), frames);
}

#[test]
fn a_call_that_does_not_name_its_tool_is_named_by_its_item() {
    let frames = [
        json!({"type": "response.output_item.added",
            "item": {"type": "function_call", "call_id": "c1", "name": "weather"}}),
        json!({"type": "response.function_call_arguments.done", "call_id": "c1",
            "arguments": "{}"}),
    ];
    let events = outputs(&frames);
    assert!(matches!(&events[1], Output::ToolCall { name, .. } if name == "weather"));
}

#[test]
fn usage_a_filter_edits_is_written_in_the_response_done() {
    let frames = [
        json!({"type": "response.done", "response": {"status": "completed",
        "usage": {"input_tokens": 5, "output_tokens": 2, "total_tokens": 7}}}),
    ];
    let mut events = outputs(&frames);
    if let Output::Usage(usage) = &mut events[0] {
        usage.tokens.output_tokens = 3;
        usage.output_audio_tokens = 1;
    }
    let written = heard(&events).expect("written");
    assert_eq!(
        written[0]["response"]["usage"],
        json!({"input_tokens": 5, "output_tokens": 3, "total_tokens": 7,
            "output_token_details": {"audio_tokens": 1}})
    );
}

#[test]
fn a_response_outside_openais_items_is_refused_to_its_client() {
    let foreign = Output::ResponseStart {
        id: None,
        native: None,
    };
    let refused = heard(&[foreign]).expect_err("refused");
    assert!(refused.is::<Upstream>(), "{refused}");
    let error = Output::Error {
        code: Some("invalid_request_error".into()),
        message: "no".into(),
        native: None,
    };
    assert_eq!(
        heard(&[error]).expect("written"),
        [json!({"type": "error", "error": {"type": "invalid_request_error", "message": "no"}})]
    );
}
