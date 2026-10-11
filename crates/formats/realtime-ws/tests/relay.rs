//! A relay to the same format keeps what every event says: a client of a
//! spec, through the gateway, to that spec's strict fake, hears what the
//! fake sent, and the fake reads what the client sent, event for event,
//! while plugins see the kind's events in between.
#![allow(clippy::unwrap_used)]

#[allow(dead_code)]
#[path = "../../../../tests/support/realtime/mod.rs"]
mod realtime;

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use realtime::{Fake, ToolCall, Turn, Usage, openai};
use respan_gateway_core::futures::{SinkExt, StreamExt};
use respan_gateway_core::{ClientContext, Fault, Gateway, Plugin, Response, async_trait};
use respan_gateway_http::relay::Relay;
use respan_gateway_realtime::{AudioFormat, Input, Modality, Output, Status, Turns};
use respan_gateway_realtime_ws::OpenAiRealtime;
use respan_gateway_ws::{Limits, WsTransport};
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderName;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

/// The events plugins saw at the client's edge.
#[derive(Clone, Default)]
struct Seen {
    inputs: Arc<Mutex<Vec<Input>>>,
    outputs: Arc<Mutex<Vec<Output>>>,
}

impl Seen {
    fn inputs(&self) -> Vec<Input> {
        self.inputs.lock().unwrap().clone()
    }

    fn outputs(&self) -> Vec<Output> {
        self.outputs.lock().unwrap().clone()
    }
}

#[async_trait]
impl Plugin for Seen {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let inputs = self.inputs.clone();
        cx.filter(move |e: &mut Input| {
            inputs.lock().unwrap().push(e.clone());
            Ok(())
        })?;
        let outputs = self.outputs.clone();
        cx.filter(move |e: &mut Output| {
            outputs.lock().unwrap().push(e.clone());
            Ok(())
        })?;
        Ok(None)
    }
}

/// A gateway serving `format` at both edges, its provider at `transport`.
/// The call keeps the client's head, its key and query among it
/// ([`Relay::RAW`]); its frames are the format's, written anew.
async fn serve<F>(format: F, transport: WsTransport) -> (SocketAddr, Seen)
where
    F: respan_gateway_core::Format<respan_gateway_http::ClientIo, Input, Output>
        + respan_gateway_core::Format<respan_gateway_http::ProviderIo, Output, Input>
        + Clone,
{
    let seen = Seen::default();
    let gateway = Gateway::new()
        .client::<Input, _, _>(format.clone())
        .unwrap()
        .provider::<Input, _, _>("p", format, transport.connect())
        .unwrap()
        .plugin(seen.clone())
        .target("p")
        .unwrap()
        .plugin(Relay::RAW);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = respan_gateway_ws::axum::router(
        gateway,
        |_| 32 << 20,
        Limits::default(),
        std::future::pending(),
    );
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (addr, seen)
}

/// A client of the gateway, which keeps what it said and heard.
struct Client {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    said: Vec<Value>,
    heard: Vec<Value>,
}

impl Client {
    async fn connect(addr: SocketAddr, path: &str, key: (&str, &str)) -> Self {
        let mut request = format!("ws://{addr}{path}").into_client_request().unwrap();
        let (name, value) = key;
        let name: HeaderName = name.parse().unwrap();
        request.headers_mut().insert(name, value.parse().unwrap());
        let (ws, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        Self {
            ws,
            said: Vec::new(),
            heard: Vec::new(),
        }
    }

    async fn say(&mut self, event: Value) {
        let text = event.to_string().into();
        self.ws.send(Message::Text(text)).await.unwrap();
        self.said.push(event);
    }

    async fn hear(&mut self) -> Value {
        loop {
            let next = tokio::time::timeout(Duration::from_secs(5), self.ws.next());
            let event: Value = match next.await.expect("an event in time") {
                Some(Ok(Message::Text(text))) => serde_json::from_str(&text).unwrap(),
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
                other => panic!("expected an event, heard {other:?}"),
            };
            self.heard.push(event.clone());
            return event;
        }
    }

    /// What it hears up to and including the first event `last` says ends.
    async fn until(&mut self, last: impl Fn(&Value) -> bool) -> Vec<Value> {
        let mut events = Vec::new();
        loop {
            let event = self.hear().await;
            let done = last(&event);
            events.push(event);
            if done {
                return events;
            }
        }
    }

    /// The fake read what the client said, and the client heard what the
    /// fake sent, event for event.
    async fn relayed(&self, fake: &Fake) {
        assert_eq!(fake.received(self.said.len()).await, self.said);
        assert_eq!(self.heard, fake.sent());
    }
}

fn of_type(kind: &'static str) -> impl Fn(&Value) -> bool {
    move |event| event["type"] == kind
}

fn pcm(ms: usize) -> Vec<u8> {
    (0..ms * 24)
        .flat_map(|i| (i as i16).to_le_bytes())
        .collect()
}

fn base64(data: &[u8]) -> String {
    STANDARD.encode(data)
}

/// An OpenAI Realtime client through a gateway to the strict fake, past
/// its `session.created`.
async fn openai() -> (Fake, Client, Seen) {
    let fake = openai::start().await;
    // The client's path is kept: the provider is at the vendor's origin.
    let origin = fake.base_url().replace("/v1", "");
    let transport = OpenAiRealtime.transport(origin).unwrap();
    let (addr, seen) = serve(OpenAiRealtime, transport).await;
    let key = ("authorization", "Bearer sk-test");
    let mut client = Client::connect(addr, "/v1/realtime?model=gpt-realtime", key).await;
    assert_eq!(client.hear().await["type"], "session.created");
    (fake, client, seen)
}

fn usage() -> Usage {
    Usage {
        input_tokens: 40,
        output_tokens: 12,
        input_audio_tokens: 30,
        output_audio_tokens: 10,
        cached_tokens: 8,
    }
}

// 4: a voice turn the server detects, with transcripts of both sides.
#[tokio::test]
async fn openai_relays_a_voice_turn_with_its_transcripts() {
    let (fake, mut client, seen) = openai().await;
    let transcribed = json!({"type": "session.update", "event_id": "evt_session",
        "session": {"type": "realtime", "instructions": "Be brief.",
            "audio": {"input": {"transcription": {"model": "gpt-4o-mini-transcribe"},
                "noise_reduction": {"type": "near_field"}}}}});
    client.say(transcribed).await;
    client.until(of_type("session.updated")).await;
    for _ in 0..2 {
        let append = json!({"type": "input_audio_buffer.append", "audio": base64(&pcm(100))});
        client.say(append).await;
    }
    fake.received(3).await;
    fake.answer(Turn {
        heard: Some("what time is it".into()),
        audio: Some(pcm(150)),
        transcript: Some("It is noon.".into()),
        usage: usage(),
        ..Turn::default()
    });
    fake.user_speaks();
    fake.user_stops();
    client.until(of_type("rate_limits.updated")).await;
    client.relayed(&fake).await;

    let inputs = seen.inputs();
    assert!(matches!(&inputs[0], Input::Session(s) if s.model.as_deref() == Some("gpt-realtime")));
    let Input::Session(update) = &inputs[1] else {
        panic!("{inputs:?}")
    };
    assert_eq!(update.instructions.as_deref(), Some("Be brief."));
    assert_eq!(update.input_transcription, Some(true));
    let audio = |e: &Input| {
        matches!(e, Input::Audio { audio, .. }
        if audio.format == AudioFormat::pcm16(24_000) && audio.data == pcm(100))
    };
    assert_eq!(inputs.iter().filter(|e| audio(e)).count(), 2);

    let outputs = seen.outputs();
    let Output::Opened(opened) = &outputs[0] else {
        panic!("{outputs:?}")
    };
    assert_eq!(opened.output, Some(Modality::Audio));
    assert!(matches!(opened.turns, Some(Turns::Server(_))));
    let heard: String = outputs
        .iter()
        .filter_map(|e| match e {
            Output::InputTranscript {
                text,
                complete: false,
                ..
            } => Some(text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(heard, "what time is it");
    let said: String = outputs
        .iter()
        .filter_map(|e| match e {
            Output::Transcript { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(said, "It is noon.");
    let spoken: Vec<u8> = outputs
        .iter()
        .filter_map(|e| match e {
            Output::Audio { audio, .. } => Some(audio.data.to_vec()),
            _ => None,
        })
        .flatten()
        .collect();
    assert_eq!(spoken, pcm(150));
    let kinds: Vec<&str> = outputs.iter().filter_map(variant).collect();
    for kind in [
        "speech_started",
        "speech_stopped",
        "input_transcript",
        "response_start",
        "usage",
        "response_done",
    ] {
        assert!(kinds.contains(&kind), "{kind} in {kinds:?}");
    }
    let used = outputs.iter().find_map(|e| match e {
        Output::Usage(usage) => Some(*usage),
        _ => None,
    });
    let used = used.unwrap();
    assert_eq!(
        (used.tokens.input_tokens, used.tokens.output_tokens),
        (40, 12)
    );
    assert_eq!(used.tokens.cached_input_tokens, 8);
    assert_eq!(
        (used.input_audio_tokens, used.output_audio_tokens),
        (30, 10)
    );
    assert!(outputs.iter().any(|e| matches!(
        e,
        Output::ResponseDone {
            status: Status::Completed,
            ..
        }
    )));
}

/// The kind's name of an output, for a summary.
fn variant(event: &Output) -> Option<&'static str> {
    Some(match event {
        Output::SpeechStarted { .. } => "speech_started",
        Output::SpeechStopped { .. } => "speech_stopped",
        Output::InputTranscript { .. } => "input_transcript",
        Output::ResponseStart { .. } => "response_start",
        Output::Usage(_) => "usage",
        Output::ResponseDone { .. } => "response_done",
        Output::ToolCall { .. } => "tool_call",
        Output::Error { .. } => "error",
        _ => return None,
    })
}

// 5: the user speaks over the model, and the client truncates what it
// played.
#[tokio::test]
async fn openai_relays_a_barge_in_and_its_truncation() {
    let (fake, mut client, seen) = openai().await;
    client
        .say(json!({"type": "input_audio_buffer.append", "audio": base64(&pcm(100))}))
        .await;
    fake.received(1).await;
    fake.answer(Turn {
        audio: Some(pcm(300)),
        transcript: Some("Once upon a time".into()),
        held: true,
        usage: usage(),
        ..Turn::default()
    });
    fake.user_speaks();
    fake.user_stops();
    let speaking = client
        .until(|e| e["type"] == "response.output_audio_transcript.delta" && e["delta"] == "time")
        .await;
    let item = speaking
        .iter()
        .find(|e| e["type"] == "response.output_item.added")
        .map(|e| e["item"]["id"].clone())
        .unwrap();
    fake.user_speaks();
    let cut = client.until(of_type("rate_limits.updated")).await;
    let done = cut.iter().find(|e| e["type"] == "response.done").unwrap();
    assert_eq!(
        done["response"]["status_details"]["reason"],
        "turn_detected"
    );
    let truncate = json!({"type": "conversation.item.truncate", "event_id": "evt_cut",
        "item_id": item, "content_index": 0, "audio_end_ms": 120});
    client.say(truncate).await;
    assert_eq!(client.hear().await["type"], "conversation.item.truncated");
    client.relayed(&fake).await;

    let outputs = seen.outputs();
    let started = outputs
        .iter()
        .filter(|e| matches!(e, Output::SpeechStarted { .. }))
        .count();
    assert_eq!(started, 2);
    assert!(outputs.iter().any(|e| matches!(
        e,
        Output::ResponseDone {
            status: Status::Interrupted,
            ..
        }
    )));
    let inputs = seen.inputs();
    assert!(
        matches!(inputs.last(), Some(Input::Native(_))),
        "{inputs:?}"
    );
}

// 6: the model calls a tool, its arguments streamed, and hears its answer.
#[tokio::test]
async fn openai_relays_a_tool_call_round_trip() {
    let (fake, mut client, seen) = openai().await;
    let tools = json!({"type": "session.update", "session": {"type": "realtime",
        "output_modalities": ["text"], "tool_choice": "auto",
        "tools": [{"type": "function", "name": "weather", "description": "The weather in a city.",
            "parameters": {"type": "object", "properties": {"city": {"type": "string"}}}}]}});
    client.say(tools).await;
    client.until(of_type("session.updated")).await;
    let asks = json!({"type": "conversation.item.create", "event_id": "evt_ask",
        "item": {"type": "message", "role": "user",
            "content": [{"type": "input_text", "text": "Weather in Paris?"}]}});
    client.say(asks).await;
    client.until(of_type("conversation.item.done")).await;
    fake.answer(Turn {
        tool_call: Some(ToolCall {
            name: "weather".into(),
            arguments: json!({"city": "Paris"}),
        }),
        usage: usage(),
        ..Turn::default()
    });
    client.say(json!({"type": "response.create"})).await;
    let call = client.until(of_type("rate_limits.updated")).await;
    let done = call
        .iter()
        .find(|e| e["type"] == "response.function_call_arguments.done")
        .unwrap();
    let id = done["call_id"].clone();
    let answer = json!({"type": "conversation.item.create",
        "item": {"type": "function_call_output", "call_id": id, "output": "{\"celsius\":21}"}});
    client.say(answer).await;
    client.until(of_type("conversation.item.done")).await;
    fake.answer(Turn {
        text: Some("It is 21 degrees.".into()),
        ..Turn::default()
    });
    client
        .say(json!({"type": "response.create", "event_id": "evt_again"}))
        .await;
    client.until(of_type("rate_limits.updated")).await;
    client.relayed(&fake).await;

    let inputs = seen.inputs();
    let Input::Session(session) = &inputs[1] else {
        panic!("{inputs:?}")
    };
    let tools = session.tools.as_ref().unwrap();
    assert_eq!(tools[0].name, "weather");
    assert!(
        inputs
            .iter()
            .any(|e| matches!(e, Input::Message { message, .. }
        if message.text() == "Weather in Paris?"))
    );
    assert!(inputs.iter().any(|e| matches!(e, Input::ToolResult(result)
        if result.id == id && result.output == "{\"celsius\":21}")));
    let respond = inputs
        .iter()
        .filter(|e| matches!(e, Input::Respond { .. }))
        .count();
    assert_eq!(respond, 2);
    let outputs = seen.outputs();
    let call = outputs.iter().find_map(|e| match e {
        Output::ToolCall {
            id: call,
            name,
            arguments,
            ..
        } => Some((call.clone(), name.clone(), arguments.clone())),
        _ => None,
    });
    let (call, name, arguments) = call.unwrap();
    assert_eq!((json!(call), name.as_str()), (id, "weather"));
    let arguments: Value = serde_json::from_str(&arguments).unwrap();
    assert_eq!(arguments, json!({"city": "Paris"}));
    let text: String = outputs
        .iter()
        .filter_map(|e| match e {
            Output::Text { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(text, "It is 21 degrees.");
}

// 7: an error names the client's event it refuses, and the session goes on.
#[tokio::test]
async fn openai_relays_an_error_naming_the_clients_event() {
    let (fake, mut client, seen) = openai().await;
    client
        .say(json!({"type": "response.cancel", "event_id": "evt_cancel"}))
        .await;
    let error = client.hear().await;
    assert_eq!(error["error"]["event_id"], "evt_cancel");
    let beta = json!({"type": "session.update", "event_id": "evt_beta",
        "session": {"type": "realtime", "modalities": ["text"]}});
    client.say(beta).await;
    let error = client.hear().await;
    assert_eq!(error["error"]["code"], "unknown_parameter");
    assert_eq!(error["error"]["event_id"], "evt_beta");
    client.relayed(&fake).await;

    let errors: Vec<(Option<String>, String)> = seen
        .outputs()
        .into_iter()
        .filter_map(|e| match e {
            Output::Error { code, message, .. } => Some((code, message)),
            _ => None,
        })
        .collect();
    assert_eq!(errors.len(), 2);
    assert_eq!(errors[0].0.as_deref(), Some("invalid_request_error"));
    assert!(errors[0].1.contains("no active response"), "{errors:?}");
    let inputs = seen.inputs();
    assert!(matches!(inputs[1], Input::Cancel { .. }), "{inputs:?}");
}

// 8: a G.711 session's audio passes as it came: the formats match, so
// nothing is converted.
#[tokio::test]
async fn openai_relays_a_g711_session_unchanged() {
    let (fake, mut client, seen) = openai().await;
    let mulaw = json!({"type": "session.update", "session": {"type": "realtime",
        "audio": {"input": {"format": {"type": "audio/pcmu"}, "turn_detection": null},
            "output": {"format": {"type": "audio/pcmu"}, "voice": "marin"}}}});
    client.say(mulaw).await;
    let updated = client.until(of_type("session.updated")).await;
    assert_eq!(
        updated[0]["session"]["audio"]["output"]["format"]["type"],
        "audio/pcmu"
    );
    let said: Vec<u8> = (0..800).map(|i| (i % 251) as u8).collect();
    client
        .say(json!({"type": "input_audio_buffer.append", "audio": base64(&said)}))
        .await;
    client
        .say(json!({"type": "input_audio_buffer.commit"}))
        .await;
    client.until(of_type("conversation.item.done")).await;
    let spoken: Vec<u8> = (0..1_600).map(|i| (i % 253) as u8).collect();
    fake.answer(Turn {
        audio: Some(spoken.clone()),
        ..Turn::default()
    });
    client.say(json!({"type": "response.create"})).await;
    client.until(of_type("rate_limits.updated")).await;
    client.relayed(&fake).await;

    let inputs = seen.inputs();
    let Input::Session(session) = &inputs[1] else {
        panic!("{inputs:?}")
    };
    assert_eq!(session.input_audio, Some(AudioFormat::MULAW));
    assert_eq!(session.turns, Some(Turns::Manual));
    assert_eq!(session.voice.as_deref(), Some("marin"));
    assert!(inputs.iter().any(|e| matches!(e, Input::Audio { audio, .. }
        if audio.format == AudioFormat::MULAW && audio.data == said)));
    let heard: Vec<u8> = seen
        .outputs()
        .iter()
        .filter_map(|e| match e {
            Output::Audio { audio, .. } => {
                assert_eq!(audio.format, AudioFormat::MULAW);
                Some(audio.data.to_vec())
            }
            _ => None,
        })
        .flatten()
        .collect();
    assert_eq!(heard, spoken);
}
