//! OpenAI's quirks, and what its own formats do as their spec says.

#[allow(dead_code)]
#[path = "../../../../tests/support/realtime/mod.rs"]
mod realtime;

use respan_gateway_core::futures::{SinkExt, StreamExt};
use respan_gateway_core::{Gateway, Provider};
use respan_gateway_http::http::{HeaderMap, HeaderValue};
use respan_gateway_realtime::Input;
use respan_gateway_realtime_ws::{GeminiLive, OpenAiRealtime};
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite::Message;

/// What OpenAI is sent of a Gemini Live client's `setup`.
async fn sent_of(setup: Value) -> Value {
    let fake = realtime::openai::start().await;
    let mut key = HeaderMap::new();
    key.insert("authorization", HeaderValue::from_static("Bearer sk-test"));
    let transport = OpenAiRealtime
        .transport(fake.base_url())
        .and_then(|t| t.with_headers(key))
        .expect("a transport");
    let gateway = Gateway::new()
        .client::<Input, _, _>(GeminiLive)
        .and_then(|g| g.provider(Provider::new("openai", OpenAiRealtime, transport.connect())))
        .and_then(|g| g.target("openai"))
        .expect("a gateway");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listen");
    let addr = listener.local_addr().expect("an address");
    let app = respan_gateway_serve::Host::new(gateway)
        .limit(|_| 1 << 20)
        .router();
    tokio::spawn(async move { axum::serve(listener, app).await });
    let url = format!("ws://{addr}{}?model=gpt-realtime", GeminiLive::PATH);
    let (mut ws, _) = tokio_tungstenite::connect_async(url)
        .await
        .expect("a session");
    let text = setup.to_string().into();
    ws.send(Message::Text(text)).await.expect("sent");
    let complete = ws.next().await.expect("an answer").expect("a message");
    assert_eq!(
        complete.into_text().expect("text"),
        r#"{"setupComplete":{}}"#
    );
    fake.received(1).await.remove(0)
}

#[tokio::test]
async fn openai_transcription_model_is_asked_for_when_a_session_names_none() {
    let setup = json!({"setup": {"model": "models/m", "inputAudioTranscription": {}}});
    let update = sent_of(setup).await;
    assert_eq!(
        update["session"]["audio"]["input"]["transcription"],
        json!({"model": "gpt-4o-mini-transcribe"})
    );
    let setup = json!({"setup": {"model": "models/m"}});
    let update = sent_of(setup).await;
    assert!(
        update["session"]["audio"]["input"]
            .get("transcription")
            .is_none()
    );
}

#[tokio::test]
async fn openai_transcription_model_is_asked_for_beside_gemini_only_fields() {
    let setup = json!({"setup": {"model": "models/m", "inputAudioTranscription": {},
        "contextWindowCompression": {"slidingWindow": {}}}});
    let update = sent_of(setup).await;
    assert_eq!(
        update["session"]["audio"]["input"]["transcription"],
        json!({"model": "gpt-4o-mini-transcribe"})
    );
    assert!(update["setup"].is_null(), "{update}");
}

#[test]
fn openai_sends_a_chat_to_responses_by_its_model() {
    use respan_gateway_vendors_green::openai::{VENDOR, responses_only};
    // From gpt-5.4 Chat Completions refuses their reasoning with function
    // tools; some only Responses serves.
    for model in [
        "gpt-6-luna",
        "gpt-5.4-mini",
        "gpt-5.5",
        "o3-pro",
        "gpt-5-codex",
    ] {
        assert!(responses_only(model), "{model}");
    }
    for model in ["gpt-5.2", "gpt-5-mini", "gpt-4.1-mini", "o3", "gpt-4o"] {
        assert!(!responses_only(model), "{model}");
    }
    let ask = |format| respan_gateway_vendor_kit::Ask {
        kind: "chat",
        format,
        model: None,
        client: None,
    };
    let format_for = VENDOR.format_for.expect("a format_for");
    let card = respan_gateway_vendor_kit::Model::new("gpt-6-luna");
    assert_eq!(
        format_for(&ask("anthropic-messages"), &card),
        Some("openai-responses")
    );
}
