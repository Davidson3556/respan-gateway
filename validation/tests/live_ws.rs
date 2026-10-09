#![allow(clippy::unwrap_used)]
//! Opt-in live TLS sessions through the same adapter and transport as mocks.
//! Credentials are environment variables; never include keys in assertions.

#[path = "../../crates/axum/tests/support/mod.rs"]
mod support;

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
    let gw = respan_gateway_sdk::Gateway::new()
        .client::<respan_gateway_http::formats::opaque::Input<Raw>, _, _>(Raw)
        .unwrap()
        .provider::<respan_gateway_http::formats::opaque::Input<Raw>, _, _>(
            "p",
            Raw,
            transport.connect(),
        )
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
