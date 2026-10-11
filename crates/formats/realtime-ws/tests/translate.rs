//! A session crosses formats: a client of one spec, through the gateway, to
//! the other spec's strict fake, which takes what the client says as its
//! API would, and answers it as its API does; the client hears it as its
//! own spec says it. What one spec has and the other has not is dropped
//! where it cannot change the answer, and else refused.
#![allow(clippy::unwrap_used)]

#[allow(dead_code)]
mod common;
#[allow(dead_code)]
#[path = "../../../../tests/support/realtime/mod.rs"]
mod realtime;

use common::{
    Client, GOOGLE_KEY, OPENAI_KEY, Seen, base64, has, keyed, of_type, pcm, pcm_at, serve, unbase64,
};
use realtime::{Fake, ToolCall, Turn, Usage, gemini, openai};
use respan_gateway_realtime::{Input, Output, Status};
use respan_gateway_realtime_ws::{GeminiLive, OpenAiRealtime};
use serde_json::{Value, json};

fn usage() -> Usage {
    Usage {
        input_tokens: 40,
        output_tokens: 12,
        input_audio_tokens: 30,
        output_audio_tokens: 10,
        cached_tokens: 8,
    }
}

/// The types of `events`, a run of one type once.
fn kinds(events: &[Value]) -> Vec<&str> {
    let mut kinds: Vec<&str> = events.iter().map(|e| e["type"].as_str().unwrap()).collect();
    kinds.dedup();
    kinds
}

// OpenAI Realtime's client, to Gemini Live.

/// An OpenAI client through a gateway to the Gemini fake, asking for its
/// model at `?model=`.
async fn to_gemini() -> (Fake, Client, Seen) {
    let fake = gemini::start().await;
    let transport = keyed(GeminiLive.transport(fake.base_url()).unwrap(), GOOGLE_KEY);
    let (addr, seen) = serve(OpenAiRealtime, GeminiLive, transport).await;
    let client = Client::connect(addr, "/v1/realtime?model=gemini-live").await;
    (fake, client, seen)
}

/// A `session.update` of `session`'s fields.
fn update(session: Value) -> Value {
    let mut v = json!({"type": "session.update", "session": {"type": "realtime"}});
    v["session"]
        .as_object_mut()
        .unwrap()
        .extend(session.as_object().unwrap().clone());
    v
}

/// What a voice agent sets: instructions, a voice, transcripts, a tool,
/// server VAD, and what Gemini has no use for.
fn agent() -> Value {
    update(json!({
        "instructions": "Be brief.",
        "output_modalities": ["audio"],
        "audio": {
            "input": {
                "format": {"type": "audio/pcm", "rate": 24000},
                "transcription": {"model": "gpt-4o-mini-transcribe", "language": "en"},
                "noise_reduction": {"type": "near_field"},
                "turn_detection": {"type": "server_vad", "threshold": 0.6,
                    "silence_duration_ms": 500, "prefix_padding_ms": 300,
                    "create_response": true, "interrupt_response": true}
            },
            "output": {"format": {"type": "audio/pcm", "rate": 24000}, "voice": "Kore"}
        },
        "tools": [{"type": "function", "name": "weather", "description": "The weather.",
            "parameters": {"type": "object", "properties": {"city": {"type": "string"}}}}],
        "tool_choice": "auto",
        "tracing": "auto"
    }))
}

/// An OpenAI client to the Gemini fake, set up as `session` says, past its
/// `session.updated`.
async fn set_up(session: Value) -> (Fake, Client, Seen) {
    let (fake, mut client, seen) = to_gemini().await;
    assert_eq!(client.hear().await["type"], "session.created");
    client.say(session).await;
    client.until(of_type("session.updated")).await;
    (fake, client, seen)
}

// 10, 11: the session is created before the client says anything; its
// first update is Gemini's setup, of the model at ?model=.
#[tokio::test]
async fn openai_to_gemini_opens_and_sets_up() {
    let (fake, mut client, _) = to_gemini().await;
    let created = client.hear().await;
    assert_eq!(created["type"], "session.created");
    assert_eq!(
        created["session"]["audio"]["output"]["format"]["rate"],
        24000
    );
    assert!(fake.handshakes()[0].query("model").is_none());
    client.say(agent()).await;
    let updated = client.until(of_type("session.updated")).await;
    let setup = &fake.received(1).await[0];
    let expected = json!({"setup": {
        "model": "models/gemini-live",
        "generationConfig": {"responseModalities": ["AUDIO"],
            "speechConfig": {"voiceConfig": {"prebuiltVoiceConfig": {"voiceName": "Kore"}}}},
        "systemInstruction": {"parts": [{"text": "Be brief."}]},
        "tools": [{"functionDeclarations": [{"name": "weather", "description": "The weather.",
            "parameters": {"type": "object", "properties": {"city": {"type": "string"}}}}]}],
        "realtimeInputConfig": {
            "automaticActivityDetection": {"silenceDurationMs": 500, "prefixPaddingMs": 300},
            "activityHandling": "START_OF_ACTIVITY_INTERRUPTS"},
        "inputAudioTranscription": {}, "outputAudioTranscription": {}}});
    assert_eq!(*setup, expected);
    let session = &updated[0]["session"];
    assert_eq!(session["type"], "realtime");
    assert_eq!(session["instructions"], "Be brief.");
    assert_eq!(session["audio"]["output"]["voice"], "Kore");
    assert!(updated[0]["event_id"].is_string());
}

// 12: an update that changes nothing is acknowledged, and one that changes
// anything closes the session.
#[tokio::test]
async fn openai_to_gemini_acknowledges_an_update_that_changes_nothing() {
    let (fake, mut client, _) = set_up(agent()).await;
    client.say(agent()).await;
    assert_eq!(client.hear().await["type"], "session.updated");
    client
        .say(update(json!({"instructions": "Be verbose."})))
        .await;
    assert_eq!(client.closed().await, Some(1008));
    assert_eq!(fake.received(1).await.len(), 1);
}

// 13: 24 kHz PCM goes as it came, declaring its rate; G.711 is decoded to
// PCM at 8 kHz, which Gemini resamples.
#[tokio::test]
async fn openai_to_gemini_sends_the_users_audio_at_its_rate() {
    let (fake, mut client, _) = set_up(agent()).await;
    let said = pcm(100);
    let append = json!({"type": "input_audio_buffer.append", "audio": base64(&said)});
    client.say(append).await;
    let audio = &fake.received(2).await[1]["realtimeInput"]["audio"];
    assert_eq!(audio["mimeType"], "audio/pcm;rate=24000");
    assert_eq!(unbase64(&audio["data"]), said);

    let mulaw = update(json!({"audio": {"input": {"format": {"type": "audio/pcmu"}}}}));
    let (fake, mut client, _) = set_up(mulaw).await;
    let said: Vec<u8> = (0..800).map(|i| (i % 251) as u8).collect();
    let append = json!({"type": "input_audio_buffer.append", "audio": base64(&said)});
    client.say(append).await;
    let audio = &fake.received(2).await[1]["realtimeInput"]["audio"];
    assert_eq!(audio["mimeType"], "audio/pcm;rate=8000");
    assert_eq!(unbase64(&audio["data"]).len(), 1_600);
}

/// The model's audio a client heard in `events`.
fn spoken(events: &[Value]) -> Vec<u8> {
    let deltas = events
        .iter()
        .filter(|e| e["type"] == "response.output_audio.delta");
    deltas.flat_map(|e| unbase64(&e["delta"])).collect()
}

// 14, 15: the model's audio comes as it was made, or as the G.711 the
// client asked for; a response is told of in OpenAI's order, its usage in
// its end.
#[tokio::test]
async fn openai_to_gemini_tells_of_a_response_as_openai_does() {
    let (fake, mut client, _) = set_up(agent()).await;
    fake.answer(Turn {
        audio: Some(pcm(300)),
        transcript: Some("It is noon.".into()),
        usage: usage(),
        ..Turn::default()
    });
    fake.user_stops();
    let response = client.until(of_type("response.done")).await;
    assert_eq!(
        kinds(&response),
        [
            "response.created",
            "response.output_item.added",
            "conversation.item.added",
            "response.content_part.added",
            "response.output_audio.delta",
            "response.output_audio_transcript.delta",
            "response.output_audio.done",
            "response.output_audio_transcript.done",
            "response.content_part.done",
            "response.output_item.done",
            "conversation.item.done",
            "response.done",
        ]
    );
    assert_eq!(spoken(&response), pcm(300));
    let done = &response.last().unwrap()["response"];
    assert_eq!(done["status"], "completed");
    assert_eq!(done["id"], response[0]["response"]["id"]);
    assert_eq!(done["output"][0]["content"][0]["transcript"], "It is noon.");
    assert_eq!(
        done["usage"],
        json!({"input_tokens": 40, "output_tokens": 12,
            "input_token_details": {"cached_tokens": 8, "audio_tokens": 30},
            "output_token_details": {"audio_tokens": 10}})
    );
    let item = &response[1]["item"]["id"];
    for delta in response
        .iter()
        .filter(|e| e["type"] == "response.output_audio.delta")
    {
        assert_eq!(delta["item_id"], *item);
        assert_eq!(delta["response_id"], done["id"]);
    }

    let g711 = update(json!({"audio": {"output": {"format": {"type": "audio/pcmu"}}}}));
    let (fake, mut client, _) = set_up(g711).await;
    fake.answer(Turn {
        audio: Some(pcm(300)),
        ..Turn::default()
    });
    fake.user_stops();
    let heard = spoken(&client.until(of_type("response.done")).await);
    // A third of the samples, a byte each, but what the resampler holds.
    assert!((2_350..=2_400).contains(&heard.len()), "{}", heard.len());
}

// 16: the user speaks over the model: it stops, and the response ends
// cancelled by the turn; what the client truncates reaches nothing.
#[tokio::test]
async fn openai_to_gemini_barges_in() {
    let (fake, mut client, seen) = set_up(agent()).await;
    fake.answer(Turn {
        audio: Some(pcm(300)),
        held: true,
        ..Turn::default()
    });
    fake.user_stops();
    let speaking = client.until(of_type("response.output_audio.delta")).await;
    let item = speaking[1]["item"]["id"].clone();
    fake.user_speaks();
    let cut = client.until(of_type("response.done")).await;
    let started = cut
        .iter()
        .position(|e| e["type"] == "input_audio_buffer.speech_started");
    let done = cut.last().unwrap();
    assert!(started.is_some(), "{cut:?}");
    assert_eq!(done["response"]["status"], "cancelled");
    assert_eq!(
        done["response"]["status_details"]["reason"],
        "turn_detected"
    );
    assert_eq!(done["response"]["output"][0]["status"], "incomplete");
    let truncate = json!({"type": "conversation.item.truncate", "item_id": item,
        "content_index": 0, "audio_end_ms": 120});
    client.say(truncate).await;
    let append = json!({"type": "input_audio_buffer.append", "audio": base64(&pcm(20))});
    client.say(append).await;
    let received = fake.received(2).await;
    assert!(
        received[1]["realtimeInput"]["audio"].is_object(),
        "{received:?}"
    );
    assert!(seen.inputs().iter().any(|e| matches!(e, Input::Native(_))));
}

// 17: the model calls a tool: OpenAI's function call item; its answer and
// response.create are Gemini's toolResponse, with the tool's name and an
// object response.
#[tokio::test]
async fn openai_to_gemini_calls_a_tool() {
    let (fake, mut client, _) = set_up(agent()).await;
    fake.answer(Turn {
        tool_call: Some(ToolCall {
            name: "weather".into(),
            arguments: json!({"city": "Paris"}),
        }),
        ..Turn::default()
    });
    fake.user_stops();
    let call = client.until(of_type("response.done")).await;
    assert_eq!(
        kinds(&call),
        [
            "response.created",
            "response.output_item.added",
            "conversation.item.added",
            "response.function_call_arguments.delta",
            "response.function_call_arguments.done",
            "response.output_item.done",
            "conversation.item.done",
            "response.done",
        ]
    );
    let done = &call[4];
    assert_eq!(done["name"], "weather");
    assert_eq!(done["arguments"], "{\"city\":\"Paris\"}");
    let id = done["call_id"].clone();
    fake.answer(Turn {
        audio: Some(pcm(100)),
        ..Turn::default()
    });
    let answer = json!({"type": "conversation.item.create", "event_id": "evt_answer",
        "item": {"type": "function_call_output", "call_id": id, "output": "{\"celsius\":21}"}});
    client.say(answer).await;
    client.say(json!({"type": "response.create"})).await;
    let received = fake.received(2).await;
    assert_eq!(
        received[1],
        json!({"toolResponse": {"functionResponses": [
            {"id": id, "name": "weather", "response": {"celsius": 21}}]}})
    );
    let answered = client.until(of_type("response.done")).await;
    assert_eq!(spoken(&answered), pcm(100));
    assert_eq!(
        fake.received(2).await.len(),
        2,
        "response.create is no frame"
    );
}

// 18: with turn detection off, the user's audio opens an activity, and its
// commit and response.create end it.
#[tokio::test]
async fn openai_to_gemini_takes_manual_turns() {
    let manual = update(json!({"audio": {"input": {"turn_detection": null}}}));
    let (fake, mut client, _) = set_up(manual).await;
    assert_eq!(
        fake.received(1).await[0]["setup"]["realtimeInputConfig"],
        json!({"automaticActivityDetection": {"disabled": true}})
    );
    fake.answer(Turn {
        audio: Some(pcm(100)),
        ..Turn::default()
    });
    for _ in 0..2 {
        let append = json!({"type": "input_audio_buffer.append", "audio": base64(&pcm(100))});
        client.say(append).await;
    }
    client
        .say(json!({"type": "input_audio_buffer.commit"}))
        .await;
    client.say(json!({"type": "response.create"})).await;
    client.until(of_type("response.done")).await;
    let received = fake.received(5).await;
    assert_eq!(received[1], json!({"realtimeInput": {"activityStart": {}}}));
    assert!(received[2]["realtimeInput"]["audio"].is_object());
    assert!(received[3]["realtimeInput"]["audio"].is_object());
    assert_eq!(received[4], json!({"realtimeInput": {"activityEnd": {}}}));
    assert_eq!(received.len(), 5);
}

// 19: a cancel ends the response at once; Gemini knows none, so what it
// says after is dropped.
#[tokio::test]
async fn openai_to_gemini_cancels_a_response() {
    let (fake, mut client, seen) = set_up(agent()).await;
    fake.answer(Turn {
        audio: Some(pcm(300)),
        held: true,
        ..Turn::default()
    });
    fake.user_stops();
    client.until(of_type("response.output_audio.delta")).await;
    client.say(json!({"type": "response.cancel"})).await;
    let done = client.until(of_type("response.done")).await;
    let done = &done.last().unwrap()["response"];
    assert_eq!(done["status"], "cancelled");
    assert_eq!(done["status_details"]["reason"], "client_cancelled");
    fake.user_speaks();
    assert_eq!(
        client.hear().await["type"],
        "input_audio_buffer.speech_started"
    );
    let statuses: Vec<Status> = seen
        .outputs()
        .into_iter()
        .filter_map(|e| match e {
            Output::ResponseDone { status, .. } => Some(status),
            _ => None,
        })
        .collect();
    assert_eq!(statuses, [Status::Cancelled]);
}

// 20: what the user said is heard in parts, then whole as the model
// answers.
#[tokio::test]
async fn openai_to_gemini_completes_the_users_transcript() {
    let (fake, mut client, _) = set_up(agent()).await;
    fake.answer(Turn {
        heard: Some("what time is it".into()),
        audio: Some(pcm(100)),
        ..Turn::default()
    });
    fake.user_stops();
    let turn = client.until(of_type("response.created")).await;
    let kind = "conversation.item.input_audio_transcription";
    let delta = turn
        .iter()
        .find(|e| e["type"] == format!("{kind}.delta"))
        .unwrap();
    assert_eq!(delta["delta"], "what time is it");
    let completed = &turn[turn.len() - 2];
    assert_eq!(completed["type"], format!("{kind}.completed"));
    assert_eq!(completed["transcript"], "what time is it");
    assert_eq!(completed["item_id"], delta["item_id"]);
}

// Gemini Live's client, to OpenAI Realtime.

/// A Gemini client through a gateway to the OpenAI fake, asking for its
/// model at `?model=`.
async fn to_openai() -> (Fake, Client, Seen) {
    let fake = openai::start().await;
    let transport = keyed(
        OpenAiRealtime.transport(fake.base_url()).unwrap(),
        OPENAI_KEY,
    );
    let (addr, seen) = serve(GeminiLive, OpenAiRealtime, transport).await;
    let path = format!("{}?model=gpt-realtime", gemini::PATH);
    let client = Client::connect(addr, &path).await;
    (fake, client, seen)
}

fn setup() -> Value {
    json!({"setup": {"model": "models/gemini-live",
        "generationConfig": {"responseModalities": ["AUDIO"]},
        "systemInstruction": {"parts": [{"text": "Be brief."}]},
        "tools": [{"functionDeclarations": [{"name": "weather",
            "parameters": {"type": "object", "properties": {"city": {"type": "string"}}}}]}],
        "outputAudioTranscription": {}}})
}

/// A Gemini client to the OpenAI fake, past its `setupComplete`.
async fn set_up_gemini() -> (Fake, Client, Seen) {
    let (fake, mut client, seen) = to_openai().await;
    client.say(setup()).await;
    assert_eq!(client.hear().await, json!({"setupComplete": {}}));
    (fake, client, seen)
}

// 21: OpenAI takes its model at connect.
#[tokio::test]
async fn gemini_to_openai_dials_the_model_at_its_url() {
    let fake = openai::start().await;
    let transport = keyed(
        OpenAiRealtime.transport(fake.base_url()).unwrap(),
        OPENAI_KEY,
    );
    let (addr, _) = serve(GeminiLive, OpenAiRealtime, transport).await;
    assert!(Client::refused(addr, gemini::PATH).await >= 400);
    let (fake, _client, _) = to_openai().await;
    let handshakes = fake.handshakes();
    assert_eq!(handshakes[0].uri, "/v1/realtime?model=gpt-realtime");
}

// 22: the setup is a session.update of 24 kHz PCM out; its session.updated
// is the setup's completion, once.
#[tokio::test]
async fn gemini_to_openai_sets_up() {
    let (fake, mut client, _) = set_up_gemini().await;
    let update = &fake.received(1).await[0];
    assert_eq!(
        *update,
        json!({"type": "session.update", "session": {"type": "realtime",
            "instructions": "Be brief.", "output_modalities": ["audio"],
            "audio": {"output": {"format": {"type": "audio/pcm", "rate": 24000}}},
            "tools": [{"type": "function", "name": "weather",
                "parameters": {"type": "object", "properties": {"city": {"type": "string"}}}}]}})
    );
    fake.answer(Turn {
        audio: Some(pcm(100)),
        ..Turn::default()
    });
    client.say(json!({"realtimeInput": {"text": "Hi"}})).await;
    let turn = client
        .until(|m| m["serverContent"]["turnComplete"] == true)
        .await;
    assert!(turn.iter().all(|m| m.get("setupComplete").is_none()));
}

// 23, 24, 25: 16 kHz audio is OpenAI's 24 kHz; the model's audio comes as
// inline data at 24 kHz; speech is an interruption, and a response's end
// the turn's.
#[tokio::test]
async fn gemini_to_openai_runs_a_voice_turn() {
    let (fake, mut client, _) = set_up_gemini().await;
    let said = pcm_at(16_000, 200);
    let chunk = json!({"realtimeInput": {"audio": {"mimeType": "audio/pcm;rate=16000",
        "data": base64(&said)}}});
    client.say(chunk).await;
    let append = &fake.received(2).await[1];
    assert_eq!(append["type"], "input_audio_buffer.append");
    let resampled = unbase64(&append["audio"]).len();
    // One and a half times the samples, but what the resampler holds.
    assert!((9_400..=9_600).contains(&resampled), "{resampled}");
    fake.answer(Turn {
        audio: Some(pcm(300)),
        transcript: Some("It is noon.".into()),
        usage: usage(),
        ..Turn::default()
    });
    fake.user_speaks();
    assert_eq!(
        client.hear().await,
        json!({"serverContent": {"interrupted": true}})
    );
    fake.user_stops();
    let turn = client
        .until(|m| m["serverContent"]["turnComplete"] == true)
        .await;
    let audio: Vec<u8> = turn
        .iter()
        .filter_map(|m| m["serverContent"]["modelTurn"]["parts"][0].get("inlineData"))
        .inspect(|d| assert_eq!(d["mimeType"], "audio/pcm;rate=24000"))
        .flat_map(|d| unbase64(&d["data"]))
        .collect();
    assert_eq!(audio, pcm(300));
    let transcript: String = turn
        .iter()
        .filter_map(|m| m["serverContent"]["outputTranscription"]["text"].as_str())
        .collect();
    assert_eq!(transcript, "It is noon.");
    let end = turn.last().unwrap();
    assert_eq!(end["usageMetadata"]["promptTokenCount"], 40);
    assert_eq!(end["usageMetadata"]["responseTokenCount"], 12);
}

// 26: OpenAI's function call is Gemini's toolCall, and its toolResponse an
// item and response.create.
#[tokio::test]
async fn gemini_to_openai_calls_a_tool() {
    let (fake, mut client, _) = set_up_gemini().await;
    fake.answer(Turn {
        tool_call: Some(ToolCall {
            name: "weather".into(),
            arguments: json!({"city": "Paris"}),
        }),
        ..Turn::default()
    });
    client
        .say(json!({"realtimeInput": {"text": "Weather in Paris?"}}))
        .await;
    let call = client.until(has("toolCall")).await;
    let call = &call.last().unwrap()["toolCall"]["functionCalls"][0];
    assert_eq!(call["name"], "weather");
    assert_eq!(call["args"], json!({"city": "Paris"}));
    fake.answer(Turn {
        audio: Some(pcm(100)),
        ..Turn::default()
    });
    let answer = json!({"toolResponse": {"functionResponses": [{"id": call["id"],
        "name": "weather", "response": {"celsius": 21}}]}});
    client.say(answer).await;
    client
        .until(|m| m["serverContent"]["turnComplete"] == true)
        .await;
    let received = fake.received(5).await;
    assert_eq!(
        received[3],
        json!({"type": "conversation.item.create", "item": {"type": "function_call_output",
            "call_id": call["id"], "output": "{\"celsius\":21}"}})
    );
    assert_eq!(received[4], json!({"type": "response.create"}));
}

// 27: Gemini knows a refusal only as its session's close.
#[tokio::test]
async fn gemini_to_openai_closes_on_an_error() {
    let (_fake, mut client, _) = set_up_gemini().await;
    let answer = json!({"toolResponse": {"functionResponses": [{"id": "call_none",
        "name": "weather", "response": {}}]}});
    client.say(answer).await;
    assert_eq!(client.closed().await, Some(1007));
}

// 28: what one spec has and the other has not, each way: dropped where it
// cannot change the answer, else refused, closing the session 1008.

/// What becomes of an event crossing formats.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Fate {
    Dropped,
    Refused,
}

#[tokio::test]
async fn openai_to_gemini_drops_or_refuses_what_gemini_has_not() {
    use Fate::{Dropped, Refused};
    let item = |item: Value| json!({"type": "conversation.item.create", "item": item});
    let text = |role: &str, kind: &str| {
        item(json!({"type": "message", "role": role, "content": [{"type": kind, "text": "hi"}]}))
    };
    let append = json!({"type": "input_audio_buffer.append", "audio": base64(&pcm(20))});
    let cases: Vec<(&str, Vec<Value>, Fate)> = vec![
        (
            "truncate",
            vec![json!({"type": "conversation.item.truncate",
            "item_id": "item_1", "content_index": 0, "audio_end_ms": 10})],
            Dropped,
        ),
        (
            "output clear",
            vec![json!({"type": "output_audio_buffer.clear"})],
            Dropped,
        ),
        (
            "input clear",
            vec![json!({"type": "input_audio_buffer.clear"})],
            Dropped,
        ),
        (
            "tracing",
            vec![update(json!({"tracing": null, "include": null}))],
            Dropped,
        ),
        (
            "audio, then clear",
            vec![append.clone(), json!({"type": "input_audio_buffer.clear"})],
            Refused,
        ),
        (
            "delete",
            vec![json!({"type": "conversation.item.delete", "item_id": "item_1"})],
            Refused,
        ),
        (
            "retrieve",
            vec![json!({"type": "conversation.item.retrieve", "item_id": "item_1"})],
            Refused,
        ),
        (
            "call history",
            vec![item(json!({"type": "function_call", "call_id": "c1",
            "name": "weather", "arguments": "{}"}))],
            Refused,
        ),
        (
            "assistant",
            vec![
                text("assistant", "output_text"),
                json!({"type": "response.create"}),
            ],
            Refused,
        ),
        (
            "unanswered",
            vec![text("user", "input_text"), append.clone()],
            Refused,
        ),
        (
            "placed",
            vec![
                json!({"type": "conversation.item.create", "previous_item_id": "root",
            "item": {"type": "message", "role": "user",
                "content": [{"type": "input_text", "text": "hi"}]}}),
            ],
            Refused,
        ),
        (
            "nothing to answer",
            vec![json!({"type": "response.create"})],
            Refused,
        ),
        (
            "override",
            vec![
                text("user", "input_text"),
                json!({"type": "response.create", "response": {"instructions": "Shout."}}),
            ],
            Refused,
        ),
        (
            "commit",
            vec![append, json!({"type": "input_audio_buffer.commit"})],
            Refused,
        ),
        (
            "idle timeout",
            vec![update(json!({"audio": {"input": {"turn_detection":
            {"type": "server_vad", "idle_timeout_ms": 5000}}}}))],
            Refused,
        ),
    ];
    for (case, events, fate) in cases {
        let (fake, mut client, _) = set_up(agent()).await;
        let sent = events.len();
        for event in events {
            client.say(event).await;
        }
        let ping = json!({"type": "input_audio_buffer.append", "audio": base64(&pcm(10))});
        match fate {
            Dropped => {
                client.say(ping).await;
                let received = fake.received(2).await;
                assert!(
                    received[1]["realtimeInput"]["audio"].is_object(),
                    "{case}: {received:?}"
                );
            }
            Refused => {
                assert_eq!(client.closed().await, Some(1008), "{case}");
                assert!(fake.received(1).await.len() <= sent, "{case}");
            }
        }
    }
    // Refused at the setup: what could change every answer.
    for (case, session) in [
        ("prompt", json!({"prompt": {"id": "pmpt_1"}})),
        ("tool choice", json!({"tool_choice": "required"})),
        (
            "no answer",
            json!({"audio": {"input": {"turn_detection":
            {"type": "server_vad", "create_response": false}}}}),
        ),
    ] {
        let (fake, mut client, _) = to_gemini().await;
        client.hear().await;
        client.say(update(session)).await;
        assert_eq!(client.closed().await, Some(1008), "{case}");
        assert!(fake.sent().is_empty(), "{case}");
    }
}

#[tokio::test]
async fn gemini_to_openai_drops_or_refuses_what_openai_has_not() {
    use Fate::{Dropped, Refused};
    let cases: Vec<(&str, Value, Fate)> = vec![
        (
            "stream end",
            json!({"realtimeInput": {"audioStreamEnd": true}}),
            Dropped,
        ),
        (
            "activity",
            json!({"realtimeInput": {"activityStart": {}}}),
            Dropped,
        ),
        (
            "video",
            json!({"realtimeInput": {"video": {"mimeType": "image/jpeg", "data": "AA=="}}}),
            Refused,
        ),
        (
            "scheduling",
            json!({"toolResponse": {"functionResponses": [{"id": "c1",
            "name": "weather", "response": {}, "scheduling": "SILENT"}]}}),
            Refused,
        ),
    ];
    for (case, message, fate) in cases {
        let (fake, mut client, _) = set_up_gemini().await;
        client.say(message).await;
        let ping = json!({"realtimeInput": {"audio": {"mimeType": "audio/pcm;rate=24000",
            "data": base64(&pcm(10))}}});
        match fate {
            Dropped => {
                client.say(ping).await;
                let received = fake.received(2).await;
                assert_eq!(received[1]["type"], "input_audio_buffer.append", "{case}");
            }
            Refused => assert_eq!(client.closed().await, Some(1008), "{case}"),
        }
    }
    for (case, more, fate) in [
        (
            "compression",
            json!({"contextWindowCompression": {"slidingWindow": {}}}),
            Dropped,
        ),
        (
            "sensitivity",
            json!({"realtimeInputConfig": {"automaticActivityDetection":
            {"startOfSpeechSensitivity": "START_SENSITIVITY_LOW"}}}),
            Dropped,
        ),
        ("resumption", json!({"sessionResumption": {}}), Refused),
        (
            "sampling",
            json!({"generationConfig": {"temperature": 0.2}}),
            Refused,
        ),
        ("search", json!({"tools": [{"googleSearch": {}}]}), Refused),
        (
            "non-blocking",
            json!({"tools": [{"functionDeclarations": [{"name": "weather",
            "behavior": "NON_BLOCKING"}]}]}),
            Refused,
        ),
        (
            "proactivity",
            json!({"proactivity": {"proactiveAudio": true}}),
            Refused,
        ),
    ] {
        let (fake, mut client, _) = to_openai().await;
        let mut setup = json!({"setup": {"model": "models/gemini-live"}});
        setup["setup"]
            .as_object_mut()
            .unwrap()
            .extend(more.as_object().unwrap().clone());
        client.say(setup).await;
        match fate {
            Dropped => assert_eq!(client.hear().await, json!({"setupComplete": {}}), "{case}"),
            Refused => {
                assert_eq!(client.closed().await, Some(1008), "{case}");
                assert!(fake.received(0).await.is_empty(), "{case}");
            }
        }
    }
}
