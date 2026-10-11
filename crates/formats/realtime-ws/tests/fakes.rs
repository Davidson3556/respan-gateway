//! The strict realtime fakes reject what their APIs reject, and answer as
//! they do: spoken to directly, with no gateway between.
#![allow(clippy::unwrap_used)]

#[allow(dead_code)]
#[path = "../../../../tests/support/realtime/mod.rs"]
mod realtime;

use std::time::Duration;

use realtime::{Fake, ToolCall, Turn, Usage, gemini, openai};
use respan_gateway_core::futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderName;
use tokio_tungstenite::tungstenite::{Error, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

const KEY: &[(&str, &str)] = &[("authorization", "Bearer sk-test")];
const GOOGLE_KEY: &[(&str, &str)] = &[("x-goog-api-key", "AIza-test")];

/// A session at `url`, or the status its upgrade was refused with.
async fn open(url: &str, headers: &[(&str, &str)]) -> Result<Ws, u16> {
    let mut request = url.into_client_request().unwrap();
    for (name, value) in headers {
        let name = HeaderName::from_bytes(name.as_bytes()).unwrap();
        request.headers_mut().insert(name, value.parse().unwrap());
    }
    match tokio_tungstenite::connect_async(request).await {
        Ok((ws, _)) => Ok(ws),
        Err(Error::Http(response)) => Err(response.status().as_u16()),
        Err(e) => panic!("connect: {e}"),
    }
}

/// What a session hears next: an event and whether it came in a binary
/// frame, or how the session closed.
#[derive(Debug, PartialEq)]
enum Heard {
    Event(Value, bool),
    Closed(u16, String),
}

async fn hear(ws: &mut Ws) -> Heard {
    loop {
        let next = tokio::time::timeout(Duration::from_secs(3), ws.next());
        match next.await.expect("a frame in time") {
            Some(Ok(Message::Text(text))) => {
                return Heard::Event(serde_json::from_str(&text).unwrap(), false);
            }
            Some(Ok(Message::Binary(bytes))) => {
                return Heard::Event(serde_json::from_slice(&bytes).unwrap(), true);
            }
            Some(Ok(Message::Close(Some(frame)))) => {
                return Heard::Closed(frame.code.into(), frame.reason.to_string());
            }
            Some(Ok(Message::Close(None))) | None => return Heard::Closed(1005, String::new()),
            Some(Ok(_)) => {}
            Some(Err(e)) => panic!("read: {e}"),
        }
    }
}

/// The next event.
async fn event(ws: &mut Ws) -> Value {
    match hear(ws).await {
        Heard::Event(event, _) => event,
        closed => panic!("expected an event, heard {closed:?}"),
    }
}

/// The type of each event up to one of type `last`, that one included.
async fn until(ws: &mut Ws, last: &str) -> Vec<Value> {
    let mut events = Vec::new();
    loop {
        let event = event(ws).await;
        let done = event["type"] == last;
        events.push(event);
        if done {
            return events;
        }
    }
}

fn types(events: &[Value]) -> Vec<&str> {
    events.iter().map(|e| e["type"].as_str().unwrap()).collect()
}

/// How a session closed.
async fn closed(ws: &mut Ws) -> u16 {
    loop {
        if let Heard::Closed(code, _) = hear(ws).await {
            return code;
        }
    }
}

async fn say(ws: &mut Ws, event: Value) {
    ws.send(Message::Text(event.to_string().into()))
        .await
        .unwrap();
}

fn pcm(ms: usize) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(vec![0u8; ms * 48])
}

/// An OpenAI session, past its `session.created`.
async fn openai_session(fake: &Fake) -> Ws {
    let url = format!("{}/realtime?model=gpt-realtime", fake.base_url());
    let mut ws = open(&url, KEY).await.unwrap();
    let created = event(&mut ws).await;
    assert_eq!(created["type"], "session.created");
    assert_eq!(created["session"]["model"], "gpt-realtime");
    ws
}

/// The error the next event is: its code, and the client's event it names.
async fn refused(ws: &mut Ws) -> (String, Value) {
    let error = event(ws).await;
    assert_eq!(error["type"], "error", "{error}");
    let code = error["error"]["code"].as_str().unwrap().to_owned();
    (code, error["error"]["event_id"].clone())
}

#[tokio::test]
async fn openai_takes_an_upgrade_with_a_model_and_a_key() {
    let fake = openai::start().await;
    let base = fake.base_url();
    assert_eq!(
        open(&format!("{base}/realtime"), KEY).await.err(),
        Some(400)
    );
    let url = format!("{base}/realtime?model=gpt-realtime");
    assert_eq!(open(&url, &[]).await.err(), Some(401));
    let browser = [(
        "sec-websocket-protocol",
        "realtime, openai-insecure-api-key.k",
    )];
    let (_, response) = {
        let mut request = url.as_str().into_client_request().unwrap();
        request
            .headers_mut()
            .insert(browser[0].0, browser[0].1.parse().unwrap());
        tokio_tungstenite::connect_async(request).await.unwrap()
    };
    assert_eq!(response.headers()["sec-websocket-protocol"], "realtime");
    let handshakes = fake.handshakes();
    assert_eq!(handshakes.len(), 3);
    assert_eq!(handshakes[2].query("model"), Some("gpt-realtime"));
}

#[tokio::test]
async fn openai_takes_ga_events_alone_and_goes_on() {
    let fake = openai::start().await;
    let mut ws = openai_session(&fake).await;
    say(
        &mut ws,
        json!({"type": "transcription_session.update", "event_id": "e1"}),
    )
    .await;
    assert_eq!(
        refused(&mut ws).await,
        ("invalid_value".into(), json!("e1"))
    );
    let cases = [
        (json!({"instructions": "x"}), "missing_required_parameter"),
        (
            json!({"type": "realtime", "modalities": ["text"]}),
            "unknown_parameter",
        ),
        (
            json!({"type": "realtime", "voice": "alloy"}),
            "unknown_parameter",
        ),
        (
            json!({"type": "realtime", "model": "gpt-other"}),
            "invalid_value",
        ),
        (
            json!({"type": "realtime", "audio": {"input": {"format": {"type": "audio/pcm", "rate": 16000}}}}),
            "invalid_value",
        ),
    ];
    for (session, code) in cases {
        say(
            &mut ws,
            json!({"type": "session.update", "session": session}),
        )
        .await;
        assert_eq!(refused(&mut ws).await.0, code, "{session}");
    }
    say(&mut ws, json!({"type": "output_audio_buffer.clear"})).await;
    assert_eq!(refused(&mut ws).await.0, "invalid_value");
    let update = json!({"type": "realtime", "instructions": "Be brief.", "audio": {"output": {"voice": "marin"}}});
    say(
        &mut ws,
        json!({"type": "session.update", "session": update}),
    )
    .await;
    let updated = event(&mut ws).await;
    assert_eq!(updated["type"], "session.updated");
    assert_eq!(updated["session"]["instructions"], "Be brief.");
    assert_eq!(updated["session"]["audio"]["output"]["voice"], "marin");
    assert_eq!(
        updated["session"]["audio"]["output"]["format"]["rate"],
        24000
    );
    assert_eq!(fake.received(8).await.len(), 8);
}

#[tokio::test]
async fn openai_takes_whole_samples_of_base64_and_commits_100_ms() {
    let fake = openai::start().await;
    let mut ws = openai_session(&fake).await;
    say(
        &mut ws,
        json!({"type": "input_audio_buffer.append", "audio": "not base64!"}),
    )
    .await;
    assert_eq!(refused(&mut ws).await.0, "invalid_value");
    say(
        &mut ws,
        json!({"type": "input_audio_buffer.append", "audio": "AAAA"}),
    )
    .await;
    assert_eq!(refused(&mut ws).await.0, "invalid_value", "3 bytes of PCM");
    say(
        &mut ws,
        json!({"type": "input_audio_buffer.append", "audio": pcm(50)}),
    )
    .await;
    say(&mut ws, json!({"type": "input_audio_buffer.commit"})).await;
    assert_eq!(refused(&mut ws).await.0, "input_audio_buffer_commit_empty");
    say(
        &mut ws,
        json!({"type": "input_audio_buffer.append", "audio": pcm(50)}),
    )
    .await;
    say(&mut ws, json!({"type": "input_audio_buffer.commit"})).await;
    let events = until(&mut ws, "conversation.item.done").await;
    assert_eq!(
        types(&events),
        [
            "input_audio_buffer.committed",
            "conversation.item.added",
            "conversation.item.done"
        ]
    );
    // G.711 takes any number of bytes.
    let mulaw = json!({"type": "realtime", "audio": {"input": {"format": {"type": "audio/pcmu"}}}});
    say(&mut ws, json!({"type": "session.update", "session": mulaw})).await;
    assert_eq!(event(&mut ws).await["type"], "session.updated");
    say(
        &mut ws,
        json!({"type": "input_audio_buffer.append", "audio": "AAAA"}),
    )
    .await;
    say(&mut ws, json!({"type": "input_audio_buffer.clear"})).await;
    assert_eq!(event(&mut ws).await["type"], "input_audio_buffer.cleared");
}

#[tokio::test]
async fn openai_says_a_turn_in_the_documented_order() {
    let fake = openai::start().await;
    let mut ws = openai_session(&fake).await;
    let usage = Usage {
        input_tokens: 10,
        output_tokens: 20,
        input_audio_tokens: 4,
        output_audio_tokens: 15,
        cached_tokens: 2,
    };
    fake.answer(Turn {
        audio: Some(vec![1; 9_600]),
        transcript: Some("hi there".into()),
        usage,
        ..Turn::default()
    });
    say(&mut ws, json!({"type": "response.create"})).await;
    let events = until(&mut ws, "rate_limits.updated").await;
    assert_eq!(
        types(&events),
        [
            "response.created",
            "response.output_item.added",
            "conversation.item.added",
            "response.content_part.added",
            "response.output_audio.delta",
            "response.output_audio.delta",
            "response.output_audio_transcript.delta",
            "response.output_audio_transcript.delta",
            "response.output_audio.done",
            "response.output_audio_transcript.done",
            "response.content_part.done",
            "conversation.item.done",
            "response.output_item.done",
            "response.done",
            "rate_limits.updated",
        ]
    );
    let done = &events[13]["response"];
    assert_eq!(done["status"], "completed");
    assert_eq!(done["output"][0]["content"][0]["transcript"], "hi there");
    assert_eq!(done["usage"]["total_tokens"], 30);
    assert_eq!(done["usage"]["input_token_details"]["audio_tokens"], 4);
    assert_eq!(done["usage"]["input_token_details"]["cached_tokens"], 2);
    assert_eq!(done["usage"]["output_token_details"]["audio_tokens"], 15);
    assert!(events.iter().all(|e| e["event_id"].is_string()));
    assert_eq!(fake.sent().len(), 1 + events.len());
}

#[tokio::test]
async fn openai_makes_one_response_at_a_time_and_cancels_only_an_active_one() {
    let fake = openai::start().await;
    let mut ws = openai_session(&fake).await;
    let text = json!({"type": "realtime", "output_modalities": ["text"]});
    say(&mut ws, json!({"type": "session.update", "session": text})).await;
    event(&mut ws).await;
    fake.answer(Turn {
        text: Some("one two".into()),
        held: true,
        ..Turn::default()
    });
    say(&mut ws, json!({"type": "response.create"})).await;
    let events = until(&mut ws, "response.output_text.delta").await;
    assert_eq!(types(&events)[0], "response.created");
    event(&mut ws).await;
    say(
        &mut ws,
        json!({"type": "response.create", "event_id": "again"}),
    )
    .await;
    let (code, named) = refused(&mut ws).await;
    assert_eq!(
        (code.as_str(), named),
        ("conversation_already_has_active_response", json!("again"))
    );
    say(&mut ws, json!({"type": "response.cancel"})).await;
    let events = until(&mut ws, "response.done").await;
    let done = &events.last().unwrap()["response"];
    assert_eq!(done["status"], "cancelled");
    assert_eq!(done["status_details"]["reason"], "client_cancelled");
    assert_eq!(done["output"][0]["status"], "incomplete");
    assert_eq!(event(&mut ws).await["type"], "rate_limits.updated");
    say(&mut ws, json!({"type": "response.cancel"})).await;
    assert_eq!(refused(&mut ws).await.0, "response_cancel_not_active");
}

#[tokio::test]
async fn openai_takes_outputs_and_truncates_of_what_the_model_made() {
    let fake = openai::start().await;
    let mut ws = openai_session(&fake).await;
    let output = json!({"type": "function_call_output", "call_id": "call_none", "output": "{}"});
    say(
        &mut ws,
        json!({"type": "conversation.item.create", "item": output}),
    )
    .await;
    assert_eq!(refused(&mut ws).await.0, "invalid_value");
    let user = json!({"type": "message", "role": "user", "content": [{"type": "input_text", "text": "weather?"}]});
    say(
        &mut ws,
        json!({"type": "conversation.item.create", "item": user}),
    )
    .await;
    let added = until(&mut ws, "conversation.item.done").await;
    let user_id = added[0]["item"]["id"].clone();
    say(
        &mut ws,
        json!({"type": "conversation.item.truncate", "item_id": user_id,
        "content_index": 0, "audio_end_ms": 10}),
    )
    .await;
    assert_eq!(refused(&mut ws).await.0, "invalid_value");
    fake.answer(Turn {
        transcript: Some("Let me look.".into()),
        tool_call: Some(ToolCall {
            name: "weather".into(),
            arguments: json!({"city": "Paris"}),
        }),
        ..Turn::default()
    });
    say(&mut ws, json!({"type": "response.create"})).await;
    let events = until(&mut ws, "response.done").await;
    let deltas = events
        .iter()
        .filter(|e| e["type"] == "response.function_call_arguments.delta");
    let arguments: String = deltas.map(|e| e["delta"].as_str().unwrap()).collect();
    assert_eq!(arguments, r#"{"city":"Paris"}"#);
    let output = &events.last().unwrap()["response"]["output"];
    let (message, call) = (output[0]["id"].clone(), output[1].clone());
    assert_eq!(
        (call["type"].as_str(), call["name"].as_str()),
        (Some("function_call"), Some("weather"))
    );
    event(&mut ws).await;
    let answer =
        json!({"type": "function_call_output", "call_id": call["call_id"], "output": "{\"c\":20}"});
    say(
        &mut ws,
        json!({"type": "conversation.item.create", "item": answer}),
    )
    .await;
    assert_eq!(event(&mut ws).await["type"], "conversation.item.added");
    event(&mut ws).await;
    say(
        &mut ws,
        json!({"type": "conversation.item.truncate", "item_id": message,
        "content_index": 0, "audio_end_ms": 10}),
    )
    .await;
    assert_eq!(event(&mut ws).await["type"], "conversation.item.truncated");
}

#[tokio::test]
async fn openai_voice_detection_answers_a_turn_and_barges_in() {
    let fake = openai::start().await;
    let mut ws = openai_session(&fake).await;
    let transcribed = json!({"type": "realtime", "audio": {"input": {"transcription": {"model": "gpt-4o-mini-transcribe"}}}});
    say(
        &mut ws,
        json!({"type": "session.update", "session": transcribed}),
    )
    .await;
    event(&mut ws).await;
    say(
        &mut ws,
        json!({"type": "input_audio_buffer.append", "audio": pcm(200)}),
    )
    .await;
    fake.received(2).await;
    fake.answer(Turn {
        heard: Some("hello there".into()),
        audio: Some(vec![0; 4_800]),
        held: true,
        ..Turn::default()
    });
    fake.user_speaks();
    let started = event(&mut ws).await;
    assert_eq!(started["type"], "input_audio_buffer.speech_started");
    fake.user_stops();
    let events = until(&mut ws, "response.output_audio.delta").await;
    assert_eq!(
        types(&events)[..7],
        [
            "input_audio_buffer.speech_stopped",
            "input_audio_buffer.committed",
            "conversation.item.added",
            "conversation.item.done",
            "conversation.item.input_audio_transcription.delta",
            "conversation.item.input_audio_transcription.delta",
            "conversation.item.input_audio_transcription.completed",
        ]
    );
    assert_eq!(events[1]["item_id"], started["item_id"]);
    assert_eq!(events[6]["transcript"], "hello there");
    assert_eq!(events[7]["type"], "response.created");
    // The user speaks over the held response: it is cancelled.
    fake.user_speaks();
    assert_eq!(
        event(&mut ws).await["type"],
        "input_audio_buffer.speech_started"
    );
    let events = until(&mut ws, "response.done").await;
    let done = &events.last().unwrap()["response"];
    assert_eq!(
        (
            done["status"].as_str(),
            done["status_details"]["reason"].as_str()
        ),
        (Some("cancelled"), Some("turn_detected"))
    );
    // With detection off, the test's voice does nothing.
    event(&mut ws).await;
    let manual = json!({"type": "realtime", "audio": {"input": {"turn_detection": null}}});
    say(
        &mut ws,
        json!({"type": "session.update", "session": manual}),
    )
    .await;
    event(&mut ws).await;
    fake.user_speaks();
    fake.go_away();
    assert_eq!(closed(&mut ws).await, 1001);
}

/// A Gemini session at the fake, keyed in a header, its `?query` if any.
async fn gemini_open(fake: &Fake, query: &str) -> Ws {
    let url = format!("{}{}{query}", fake.base_url(), gemini::PATH);
    open(&url, GOOGLE_KEY).await.unwrap()
}

/// A Gemini session past its `setupComplete`, set up with `setup`'s fields
/// beside its model.
async fn gemini_session(fake: &Fake, setup: Value) -> Ws {
    let mut ws = gemini_open(fake, "").await;
    let mut setup = setup;
    setup["model"] = "models/gemini-live".into();
    say(&mut ws, json!({ "setup": setup })).await;
    assert_eq!(
        hear(&mut ws).await,
        Heard::Event(json!({"setupComplete": {}}), true)
    );
    ws
}

#[tokio::test]
async fn gemini_takes_an_upgrade_with_a_key_and_no_other_query() {
    let fake = gemini::start().await;
    let url = format!("{}{}", fake.base_url(), gemini::PATH);
    assert_eq!(open(&url, &[]).await.err(), Some(401));
    assert_eq!(
        open(&format!("{url}?model=x"), GOOGLE_KEY).await.err(),
        Some(400)
    );
    assert_eq!(
        open(&format!("{url}?key=k&alt=json"), &[]).await.err(),
        Some(400)
    );
    let mut ws = open(&format!("{url}?key=k"), &[]).await.unwrap();
    ws.close(None).await.unwrap();
    gemini_open(&fake, "").await;
}

#[tokio::test]
async fn gemini_takes_setup_first_and_once_and_one_field_a_message() {
    let fake = gemini::start().await;
    let audio = json!({"realtimeInput": {"audio": {"mimeType": "audio/pcm;rate=16000", "data": "AAAAAA=="}}});
    let model = json!({"model": "models/gemini-live"});
    for messages in [
        vec![audio.clone()],
        vec![json!({"setup": model}), json!({"setup": model})],
        vec![json!({"setup": {"model": "gemini-live"}})],
        vec![json!({"setup": {"model": "models/gemini-live", "voice": "Kore"}})],
        vec![json!({"setup": model, "clientContent": {}})],
        vec![
            json!({"setup": model}),
            json!({"realtimeInput": {"audio": {"mimeType": "audio/pcm", "data": "AAAA"}}}),
        ],
        vec![
            json!({"setup": model}),
            json!({"realtimeInput": {"audio": {"mimeType": "audio/pcm;rate=16000", "data": "AA=="}}}),
        ],
        vec![
            json!({"setup": model}),
            json!({"realtimeInput": {"activityStart": {}}}),
        ],
        vec![
            json!({"setup": {"model": "models/gemini-live", "realtimeInputConfig": {"automaticActivityDetection": {"disabled": true}}}}),
            json!({"realtimeInput": {"audioStreamEnd": true}}),
        ],
        vec![
            json!({"setup": model}),
            json!({"toolResponse": {"functionResponses": [{"id": "call_9", "name": "f", "response": {}}]}}),
        ],
    ] {
        let mut ws = gemini_open(&fake, "").await;
        for message in &messages {
            say(&mut ws, message.clone()).await;
        }
        assert_eq!(closed(&mut ws).await, 1007, "{messages:?}");
    }
    // Whole samples at a declared rate are taken, and answer nothing.
    let mut ws = gemini_session(&fake, json!({})).await;
    say(&mut ws, audio).await;
    say(&mut ws, json!({"realtimeInput": {"audioStreamEnd": true}})).await;
    say(&mut ws, json!({"clientContent": {"turnComplete": true}})).await;
    assert_eq!(
        event(&mut ws).await["serverContent"]["generationComplete"],
        true
    );
}

#[tokio::test]
async fn gemini_says_a_turn_in_binary_frames_with_transcripts_if_asked() {
    let fake = gemini::start().await;
    let usage = Usage {
        input_tokens: 10,
        output_tokens: 20,
        output_audio_tokens: 15,
        ..Usage::default()
    };
    let turn = Turn {
        heard: Some("hello".into()),
        audio: Some(vec![1; 9_600]),
        transcript: Some("hi there".into()),
        usage,
        ..Turn::default()
    };
    let complete = json!({"clientContent": {"turns": [{"role": "user", "parts": [{"text": "hi"}]}], "turnComplete": true}});
    for transcribed in [false, true] {
        let setup = match transcribed {
            true => json!({"inputAudioTranscription": {}, "outputAudioTranscription": {}}),
            false => json!({}),
        };
        let mut ws = gemini_session(&fake, setup).await;
        fake.answer(turn.clone());
        say(&mut ws, complete.clone()).await;
        let mut heard = Vec::new();
        loop {
            let Heard::Event(message, binary) = hear(&mut ws).await else {
                panic!("closed");
            };
            assert!(binary, "{message}");
            let done = message["serverContent"]["turnComplete"] == true;
            heard.push(message);
            if done {
                break;
            }
        }
        let kinds: Vec<String> = heard
            .iter()
            .map(|m| {
                let content = &m["serverContent"];
                let kind = content.as_object().unwrap().keys().next().unwrap();
                kind.clone()
            })
            .collect();
        let expected: &[&str] = match transcribed {
            true => &[
                "inputTranscription",
                "modelTurn",
                "modelTurn",
                "outputTranscription",
                "outputTranscription",
                "generationComplete",
                "turnComplete",
            ],
            false => &[
                "modelTurn",
                "modelTurn",
                "generationComplete",
                "turnComplete",
            ],
        };
        assert_eq!(kinds, expected);
        let usage = &heard.last().unwrap()["usageMetadata"];
        assert_eq!(usage["totalTokenCount"], 30);
        assert_eq!(
            usage["responseTokensDetails"][1],
            json!({"modality": "AUDIO", "tokenCount": 15})
        );
    }
}

#[tokio::test]
async fn gemini_takes_answers_to_pending_calls_and_goes_on() {
    let fake = gemini::start().await;
    let mut ws = gemini_session(&fake, json!({})).await;
    fake.answer(Turn {
        tool_call: Some(ToolCall {
            name: "weather".into(),
            arguments: json!({"city": "Paris"}),
        }),
        ..Turn::default()
    });
    fake.answer(Turn {
        audio: Some(vec![0; 10]),
        ..Turn::default()
    });
    say(&mut ws, json!({"realtimeInput": {"text": "weather?"}})).await;
    let call = event(&mut ws).await["toolCall"]["functionCalls"][0].clone();
    assert_eq!(call["args"], json!({"city": "Paris"}));
    // The turn of the call ends at once.
    assert_eq!(
        event(&mut ws).await["serverContent"]["generationComplete"],
        true
    );
    assert_eq!(event(&mut ws).await["serverContent"]["turnComplete"], true);
    let answer = json!({"id": call["id"], "name": "weather", "response": {"c": 20}});
    say(
        &mut ws,
        json!({"toolResponse": {"functionResponses": [answer]}}),
    )
    .await;
    assert!(event(&mut ws).await["serverContent"]["modelTurn"].is_object());
    assert_eq!(
        event(&mut ws).await["serverContent"]["generationComplete"],
        true
    );
    assert_eq!(event(&mut ws).await["serverContent"]["turnComplete"], true);
    // The same id twice names no pending call.
    say(
        &mut ws,
        json!({"toolResponse": {"functionResponses": [answer]}}),
    )
    .await;
    assert_eq!(closed(&mut ws).await, 1007);
}

#[tokio::test]
async fn gemini_speech_interrupts_a_turn_and_cancels_its_calls() {
    let fake = gemini::start().await;
    let mut ws = gemini_session(&fake, json!({})).await;
    fake.answer(Turn {
        tool_call: Some(ToolCall {
            name: "weather".into(),
            arguments: json!({}),
        }),
        ..Turn::default()
    });
    say(&mut ws, json!({"realtimeInput": {"text": "weather?"}})).await;
    let id = event(&mut ws).await["toolCall"]["functionCalls"][0]["id"].clone();
    let ended = [event(&mut ws).await, event(&mut ws).await];
    assert_eq!(ended[1]["serverContent"]["turnComplete"], true);
    fake.user_speaks();
    let start = json!({"voiceActivity": {"type": "ACTIVITY_START"}});
    assert_eq!(
        event(&mut ws).await,
        json!({"serverContent": {"interrupted": true}})
    );
    assert_eq!(
        event(&mut ws).await,
        json!({"toolCallCancellation": {"ids": [id]}})
    );
    assert_eq!(event(&mut ws).await, start);
    // A held turn is interrupted, and ended; nothing else is.
    fake.answer(Turn {
        audio: Some(vec![0; 10]),
        held: true,
        ..Turn::default()
    });
    fake.user_stops();
    assert_eq!(
        event(&mut ws).await,
        json!({"voiceActivity": {"type": "ACTIVITY_END"}})
    );
    assert!(event(&mut ws).await["serverContent"]["modelTurn"].is_object());
    fake.user_speaks();
    assert_eq!(
        event(&mut ws).await,
        json!({"serverContent": {"interrupted": true}})
    );
    assert_eq!(event(&mut ws).await, start);
    assert_eq!(event(&mut ws).await["serverContent"]["turnComplete"], true);
    fake.user_speaks();
    assert_eq!(event(&mut ws).await, start);
    fake.go_away();
    assert_eq!(event(&mut ws).await, json!({"goAway": {"timeLeft": "10s"}}));
    // Activity is the client's once detection is off. A fake serves one
    // session at a time.
    drop(ws);
    let manual = json!({"realtimeInputConfig": {"automaticActivityDetection": {"disabled": true}}});
    let mut ws = gemini_session(&fake, manual).await;
    fake.answer(Turn {
        audio: Some(vec![0; 10]),
        ..Turn::default()
    });
    say(&mut ws, json!({"realtimeInput": {"activityStart": {}}})).await;
    say(&mut ws, json!({"realtimeInput": {"activityEnd": {}}})).await;
    assert!(event(&mut ws).await["serverContent"]["modelTurn"].is_object());
}
