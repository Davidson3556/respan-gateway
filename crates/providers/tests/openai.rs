//! OpenAI's quirks.

#[allow(dead_code)]
#[path = "../../../tests/support/realtime/mod.rs"]
mod realtime;

use respan_gateway_core::futures::{SinkExt, StreamExt};
use respan_gateway_core::{Fault, Gateway, Plugin, ProviderContext, async_trait};
use respan_gateway_http::http::{HeaderMap, HeaderValue};
use respan_gateway_providers::openai::OpenAiQuirks;
use respan_gateway_realtime::Input;
use respan_gateway_realtime_ws::{GeminiLive, OpenAiRealtime};
use respan_gateway_ws::Limits;
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite::Message;

/// OpenAI's plugin, run on every call.
struct Quirks;

#[async_trait]
impl Plugin for Quirks {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        OpenAiQuirks.on_provider(px).await
    }
}

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
        .and_then(|g| g.provider::<Input, _, _>("openai", OpenAiRealtime, transport.connect()))
        .and_then(|g| g.target("openai"))
        .expect("a gateway")
        .plugin(Quirks);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listen");
    let addr = listener.local_addr().expect("an address");
    let app = respan_gateway_ws::axum::router(
        gateway,
        |_| 1 << 20,
        Limits::default(),
        std::future::pending(),
    );
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
