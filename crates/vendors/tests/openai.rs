//! OpenAI's quirks.

#[allow(dead_code)]
#[path = "../../../tests/support/realtime/mod.rs"]
mod realtime;

use respan_gateway_core::futures::{SinkExt, StreamExt};
use respan_gateway_core::{Gateway, Provider};
use respan_gateway_http::http::{HeaderMap, HeaderValue};
use respan_gateway_realtime::Input;
use respan_gateway_realtime_ws::{GeminiLive, OpenAiRealtime};
use respan_gateway_vendors::openai::OpenAiQuirks;
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
        .and_then(|g| {
            let openai = Provider::new("openai", OpenAiRealtime, transport.connect());
            g.provider(openai.plugin(&OpenAiQuirks))
        })
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

#[allow(dead_code)]
#[path = "../../../tests/support/http.rs"]
mod http_support;

/// The format OpenAI's `reformat` picks for a chat of `body`, sent as Chat
/// Completions.
async fn reformatted(body: Value) -> Option<&'static str> {
    use respan_gateway_conversation_http::openai::OpenAiChat;
    use respan_gateway_core::{ClientContext, Fault, Plugin, Response, async_trait};
    use std::sync::{Arc, Mutex};

    /// Records the choice, and answers nothing.
    #[derive(Debug)]
    struct Ask(Arc<Mutex<Option<&'static str>>>);
    #[async_trait]
    impl Plugin for Ask {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let reformat = respan_gateway_vendors::openai::VENDOR
                .reformat
                .expect("a reformat");
            *self.0.lock().expect("unpoisoned") = reformat(cx, "openai-chat");
            Err(Fault::not_found("asked"))
        }
    }
    let chosen = Arc::default();
    let gateway = Gateway::new()
        .client::<respan_gateway_conversation::Input, _, _>(OpenAiChat)
        .expect("a gateway")
        .plugin(Ask(Arc::clone(&chosen)));
    let request = http_support::post("/v1/chat/completions", body.to_string());
    let (_, ended) = http_support::serve(&gateway, request).await;
    let _ = ended.await;
    *chosen.lock().expect("unpoisoned")
}

#[tokio::test]
async fn openai_responses_only_takes_tools_with_reasoning_and_its_models() {
    let tools = json!([{ "type": "function", "function": { "name": "f", "parameters": {} } }]);
    let chat = |model: &str, extra: Value| {
        let mut body = json!({ "model": model, "messages": [{ "role": "user", "content": "hi" }] });
        for (k, v) in extra.as_object().cloned().unwrap_or_default() {
            body[k] = v;
        }
        body
    };
    let both = json!({ "tools": tools, "reasoning_effort": "high" });
    assert_eq!(
        reformatted(chat("gpt-6-luna", both)).await,
        Some("openai-responses")
    );
    assert_eq!(
        reformatted(chat("gpt-6-luna", json!({ "tools": tools }))).await,
        None
    );
    let off = json!({ "tools": tools, "reasoning_effort": "none" });
    assert_eq!(
        reformatted(chat("gpt-6-luna", off)).await,
        None,
        "no reasoning"
    );
    assert_eq!(
        reformatted(chat("o3-pro", json!({}))).await,
        Some("openai-responses")
    );
    assert_eq!(
        reformatted(chat("gpt-5-codex", json!({}))).await,
        Some("openai-responses")
    );
    assert_eq!(reformatted(chat("gpt-4o", json!({}))).await, None);
    let n = json!({ "tools": tools, "reasoning_effort": "high", "n": 2 });
    assert_eq!(
        reformatted(chat("gpt-5-mini", n)).await,
        None,
        "only Chat Completions has n"
    );
}
