#![allow(clippy::unwrap_used)]
//! Opt-in live TLS sessions through the same adapter and transport as mocks,
//! and realtime sessions translated through the shipped binary, an OpenAI
//! Realtime client to Gemini Live and a Gemini Live client to OpenAI
//! Realtime, spoken with OpenAI's speech model. Credentials are environment
//! variables; never include keys in assertions. Models: `OPENAI_REALTIME_MODEL`
//! (`gpt-realtime-mini`) and `GEMINI_LIVE_MODEL` (`gemini-3.8-live`).

#[path = "../../crates/wires/ws/tests/support/mod.rs"]
mod support;

#[path = "../../apps/gateway/tests/support/mod.rs"]
mod binary;

use respan_gateway_core::Provider;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use serde_json::{Value, json};
use support::*;

fn key(name: &str) -> Option<String> {
    let key = std::env::var(name).ok().filter(|s| !s.is_empty());
    if key.is_none() {
        eprintln!("skipped: {name} is not set");
    }
    key
}

async fn event(ws: &mut Ws) -> Value {
    let text = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            use futures::StreamExt;
            use tokio_tungstenite::tungstenite::Message;
            match ws.next().await {
                Some(Ok(Message::Text(text))) => break text.to_string(),
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                _ => panic!("live provider closed before completing the exchange"),
            }
        }
    })
    .await
    .expect("live event deadline");
    serde_json::from_str(&text).expect("provider JSON")
}

async fn finish(ws: &mut Ws, server: &Server) {
    close(ws, 1000, "").await;
    assert!(matches!(hear(ws).await, Heard::Closed(_) | Heard::Broken));
    idle(&server.gateway).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "calls Gemini Live; needs GEMINI_API_KEY; incurs provider charges"]
async fn real_gemini_live_audio_and_transcription_through_tls_gateway() {
    let Some(key) = key("GEMINI_API_KEY") else {
        return;
    };
    let ends = Outcomes::default();
    let transport = respan_gateway_ws::WsTransport::new("wss://generativelanguage.googleapis.com")
        .unwrap()
        .with_binary_text(true);
    let gw = respan_gateway_core::Gateway::new()
        .client::<respan_gateway_passthrough_http::Input<Raw>, _, _>(Raw)
        .unwrap()
        .provider(Provider::new("p", Raw, transport.connect()))
        .unwrap()
        .plugin(ends.clone())
        .target("p")
        .unwrap();
    let server = serve(gw).await;
    let path = format!(
        "/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent?key={key}"
    );
    let mut ws = connect(server.addr, &path, &[])
        .await
        .expect("local gateway handshake");
    let model = std::env::var("GEMINI_LIVE_MODEL").unwrap_or_else(|_| "gemini-3.8-live".into());
    say(&mut ws,&json!({"setup":{"model":format!("models/{model}"),"generationConfig":{"responseModalities":["AUDIO"],"maxOutputTokens":64},"outputAudioTranscription":{}}}).to_string()).await;
    assert!(event(&mut ws).await.get("setupComplete").is_some());
    say(&mut ws, r#"{"realtimeInput":{"text":"Say just ok."}}"#).await;
    let mut audio = false;
    let mut text = String::new();
    loop {
        let event = event(&mut ws).await;
        for part in event["serverContent"]["modelTurn"]["parts"]
            .as_array()
            .into_iter()
            .flatten()
        {
            audio |= part["inlineData"]["data"]
                .as_str()
                .is_some_and(|s| !s.is_empty());
        }
        text.push_str(
            event["serverContent"]["outputTranscription"]["text"]
                .as_str()
                .unwrap_or(""),
        );
        if event["serverContent"]["turnComplete"] == true {
            break;
        }
    }
    assert!(
        audio && !text.trim().is_empty(),
        "expected audio and its transcript"
    );
    finish(&mut ws, &server).await;
    assert_eq!(code(&ends.only()), "ok");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "calls OpenAI Realtime; needs OPENAI_API_KEY; incurs provider charges"]
async fn real_openai_realtime_text_turn_through_tls_gateway() {
    let Some(key) = key("OPENAI_API_KEY") else {
        return;
    };
    let (gw, ends, _) = relay("wss://api.openai.com");
    let server = serve(gw).await;
    let model =
        std::env::var("OPENAI_REALTIME_MODEL").unwrap_or_else(|_| "gpt-realtime-mini".into());
    let mut ws = connect(
        server.addr,
        &format!("/v1/realtime?model={model}"),
        &[("authorization", &format!("Bearer {key}"))],
    )
    .await
    .expect("local gateway handshake");
    assert_eq!(event(&mut ws).await["type"], "session.created");
    say(&mut ws,r#"{"type":"conversation.item.create","item":{"type":"message","role":"user","content":[{"type":"input_text","text":"Say just ok."}]}}"#).await;
    say(&mut ws,r#"{"type":"response.create","response":{"output_modalities":["text"],"max_output_tokens":32}}"#).await;
    let mut text = String::new();
    loop {
        let event = event(&mut ws).await;
        assert_ne!(event["type"], "error", "provider rejected the live request");
        if event["type"] == "response.output_text.delta" {
            text.push_str(event["delta"].as_str().unwrap());
        }
        if event["type"] == "response.done" {
            assert_eq!(event["response"]["status"], "completed");
            break;
        }
    }
    assert!(!text.trim().is_empty());
    finish(&mut ws, &server).await;
    assert_eq!(code(&ends.only()), "ok");
}

/// The shipped binary, serving both realtime formats with the keys of the
/// environment, and the address its sessions connect to.
async fn translating_gateway() -> (binary::Gateway, std::net::SocketAddr) {
    let gw = binary::Gateway::start(
        "providers = { openai = { vendor = \"openai\", key = \"env:OPENAI_API_KEY\" }, gemini = { vendor = \"gemini\", key = \"env:GEMINI_API_KEY\" } }\nformats = [\"openai-realtime\", \"gemini-live\"]\n",
    )
    .await;
    let addr = gw.base.trim_start_matches("http://").parse().unwrap();
    (gw, addr)
}

/// `text` spoken by OpenAI's speech model: 24 kHz 16-bit PCM.
async fn speech(key: &str, text: &str) -> Vec<u8> {
    let body = json!({"model": "gpt-4o-mini-tts", "voice": "alloy", "input": text,
        "response_format": "pcm"});
    let resp = binary::client()
        .post("https://api.openai.com/v1/audio/speech")
        .bearer_auth(key)
        .header("content-type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "speech synthesis");
    resp.bytes().await.unwrap().to_vec()
}

/// 24 kHz PCM at 16 kHz, linearly interpolated.
fn to_16k(pcm: &[u8]) -> Vec<u8> {
    let samples: Vec<i16> = pcm
        .chunks_exact(2)
        .map(|b| i16::from_le_bytes([b[0], b[1]]))
        .collect();
    let n = samples.len() * 2 / 3;
    (0..n)
        .flat_map(|i| {
            let at = i as f64 * 1.5;
            let (j, frac) = (at as usize, at.fract());
            let next = samples.get(j + 1).copied().unwrap_or(samples[j]);
            let s = samples[j] as f64 * (1.0 - frac) + next as f64 * frac;
            (s as i16).to_le_bytes()
        })
        .collect()
}

/// Send `pcm` at `rate` in 100 ms frames `frame` writes, as fast as it
/// plays, then a second of silence for the provider's voice detection.
async fn speak(ws: &mut Ws, pcm: &[u8], rate: usize, frame: impl Fn(String) -> Value) {
    let mut audio = pcm.to_vec();
    audio.resize(pcm.len() + rate * 2, 0);
    for chunk in audio.chunks(rate * 2 / 10) {
        say(ws, &frame(STANDARD.encode(chunk)).to_string()).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// OpenAI's events up to and including the next `response.done`; any
/// `error` fails.
async fn response(ws: &mut Ws) -> Vec<Value> {
    let mut events = Vec::new();
    loop {
        let event = event(ws).await;
        assert_ne!(event["type"], "error", "{event}");
        let done = event["type"] == "response.done";
        events.push(event);
        if done {
            return events;
        }
    }
}

/// OpenAI's events up to and including the `response.done` of a response
/// that completes: voice detection may end the user's turn early, in a
/// pause, and the response to it be interrupted.
async fn answered(ws: &mut Ws) -> Vec<Value> {
    let mut events = Vec::new();
    loop {
        events.extend(response(ws).await);
        if events.last().unwrap()["response"]["status"] == "completed" {
            return events;
        }
    }
}

fn of_type<'a>(events: &'a [Value], kind: &'a str) -> impl Iterator<Item = &'a Value> {
    events.iter().filter(move |e| e["type"] == kind)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "calls Gemini Live, and OpenAI for speech; needs GEMINI_API_KEY and OPENAI_API_KEY; incurs provider charges"]
async fn real_openai_realtime_client_to_gemini_live_through_the_binary() {
    let (Some(openai), Some(_)) = (key("OPENAI_API_KEY"), key("GEMINI_API_KEY")) else {
        return;
    };
    let question = speech(&openai, "What is the capital of France?").await;
    let weather = speech(&openai, "What is the weather in Paris right now?").await;
    let (_gw, addr) = translating_gateway().await;
    let model = std::env::var("GEMINI_LIVE_MODEL").unwrap_or_else(|_| "gemini-3.8-live".into());
    let path = format!("/v1/realtime?model=gemini/{model}");
    let mut ws = connect(addr, &path, &[]).await.expect("gateway handshake");
    assert_eq!(event(&mut ws).await["type"], "session.created");
    let update = json!({"type": "session.update", "session": {"type": "realtime",
        "instructions": "Be very brief. Use get_weather for weather questions.",
        "audio": {"input": {
            "turn_detection": {"type": "server_vad", "silence_duration_ms": 500},
            "transcription": {"model": "gpt-4o-mini-transcribe"}}},
        "tools": [{"type": "function", "name": "get_weather",
            "description": "Current weather in a city.",
            "parameters": {"type": "object", "properties": {"city": {"type": "string"}},
                "required": ["city"]}}]}});
    say(&mut ws, &update.to_string()).await;
    assert_eq!(event(&mut ws).await["type"], "session.updated");
    let append = |audio: String| json!({"type": "input_audio_buffer.append", "audio": audio});

    // A spoken turn: heard, transcribed, answered in speech with its
    // transcript, its usage in its end.
    speak(&mut ws, &question, 24_000, append).await;
    let turn = answered(&mut ws).await;
    assert!(
        of_type(&turn, "input_audio_buffer.speech_started").count() > 0,
        "{turn:?}"
    );
    let heard = of_type(
        &turn,
        "conversation.item.input_audio_transcription.completed",
    );
    let heard: String = heard.filter_map(|e| e["transcript"].as_str()).collect();
    assert!(heard.to_lowercase().contains("france"), "{heard:?}");
    let audio: usize = of_type(&turn, "response.output_audio.delta")
        .map(|e| STANDARD.decode(e["delta"].as_str().unwrap()).unwrap().len())
        .sum();
    assert!(audio > 0, "no audio");
    let said: String = of_type(&turn, "response.output_audio_transcript.delta")
        .filter_map(|e| e["delta"].as_str())
        .collect();
    assert!(!said.trim().is_empty(), "no transcript");
    let done = &turn.last().unwrap()["response"];
    assert_eq!(done["status"], "completed", "{done}");
    assert!(done["usage"]["input_tokens"].as_u64() > Some(0), "{done}");
    assert!(
        done["usage"]["output_token_details"]["audio_tokens"].as_u64() > Some(0),
        "{done}"
    );

    // A turn that calls the tool, and the answer to its result.
    speak(&mut ws, &weather, 24_000, append).await;
    let turn = answered(&mut ws).await;
    let call = of_type(&turn, "response.function_call_arguments.done").next();
    let call = call.unwrap_or_else(|| panic!("no call: {turn:?}")).clone();
    assert_eq!(call["name"], "get_weather");
    let arguments: Value = serde_json::from_str(call["arguments"].as_str().unwrap()).unwrap();
    assert!(arguments["city"].as_str().unwrap().contains("Paris"));
    let output = json!({"type": "conversation.item.create", "item": {"type": "function_call_output",
        "call_id": call["call_id"], "output": "{\"temperature_c\":21,\"sky\":\"sunny\"}"}});
    say(&mut ws, &output.to_string()).await;
    say(&mut ws, r#"{"type":"response.create"}"#).await;
    let turn = answered(&mut ws).await;
    let said: String = of_type(&turn, "response.output_audio_transcript.delta")
        .filter_map(|e| e["delta"].as_str())
        .collect();
    assert!(said.contains("21"), "{said:?}");
    assert_eq!(turn.last().unwrap()["response"]["status"], "completed");

    // A long answer, cancelled once it begins.
    let ask = json!({"type": "conversation.item.create", "item": {"type": "message", "role": "user",
        "content": [{"type": "input_text", "text": "Count slowly from one to one hundred."}]}});
    say(&mut ws, &ask.to_string()).await;
    say(&mut ws, r#"{"type":"response.create"}"#).await;
    loop {
        let event = event(&mut ws).await;
        assert_ne!(event["type"], "error", "{event}");
        if event["type"] == "response.output_audio.delta" {
            break;
        }
    }
    say(&mut ws, r#"{"type":"response.cancel"}"#).await;
    let cancelled = response(&mut ws).await;
    let done = &cancelled.last().unwrap()["response"];
    assert_eq!(done["status"], "cancelled", "{done}");
    assert_eq!(done["status_details"]["reason"], "client_cancelled");
    // What Gemini says of the cancelled turn after is dropped.
    let rest = tokio::time::timeout(Duration::from_secs(3), event(&mut ws)).await;
    assert!(rest.is_err(), "{rest:?}");
    close(&mut ws, 1000, "").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "calls OpenAI Realtime and speech; needs OPENAI_API_KEY; incurs provider charges"]
async fn real_gemini_live_client_to_openai_realtime_through_the_binary() {
    let Some(openai) = key("OPENAI_API_KEY") else {
        return;
    };
    let question = to_16k(&speech(&openai, "What is the capital of France?").await);
    let (_gw, addr) = translating_gateway().await;
    let model =
        std::env::var("OPENAI_REALTIME_MODEL").unwrap_or_else(|_| "gpt-realtime-mini".into());
    let path = format!(
        "/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent?model=openai/{model}"
    );
    let mut ws = connect(addr, &path, &[]).await.expect("gateway handshake");
    // OpenAI transcribes the user only with a named model, which the
    // gateway's OpenAI quirk names.
    let setup = json!({"setup": {"model": format!("models/{model}"),
        "generationConfig": {"responseModalities": ["AUDIO"]},
        "systemInstruction": {"parts": [{"text": "Be very brief."}]},
        "inputAudioTranscription": {}, "outputAudioTranscription": {}}});
    say(&mut ws, &setup.to_string()).await;
    assert_eq!(event(&mut ws).await, json!({"setupComplete": {}}));
    let audio = |data: String| json!({"realtimeInput": {"audio": {"mimeType": "audio/pcm;rate=16000", "data": data}}});
    speak(&mut ws, &question, 16_000, audio).await;
    let (mut spoken, mut said, mut heard) = (0, String::new(), String::new());
    let end = loop {
        let message = event(&mut ws).await;
        let content = &message["serverContent"];
        for part in content["modelTurn"]["parts"]
            .as_array()
            .into_iter()
            .flatten()
        {
            assert_eq!(part["inlineData"]["mimeType"], "audio/pcm;rate=24000");
            spoken += STANDARD
                .decode(part["inlineData"]["data"].as_str().unwrap())
                .unwrap()
                .len();
        }
        said.push_str(
            content["outputTranscription"]["text"]
                .as_str()
                .unwrap_or(""),
        );
        heard.push_str(content["inputTranscription"]["text"].as_str().unwrap_or(""));
        // Voice detection may end the user's turn early, in a pause, and
        // the response to it be interrupted: the answer is the one spoken.
        if content["turnComplete"] == true && spoken > 0 {
            break message;
        }
    };
    assert!(spoken > 0, "no audio");
    assert!(!said.trim().is_empty(), "no transcript");
    assert!(heard.to_lowercase().contains("france"), "{heard:?}");
    let usage = &end["usageMetadata"];
    assert!(usage["promptTokenCount"].as_u64() > Some(0), "{end}");
    assert!(usage["responseTokenCount"].as_u64() > Some(0), "{end}");
    close(&mut ws, 1000, "").await;
}
