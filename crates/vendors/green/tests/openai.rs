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

/// Whether OpenAI sends a chat of `body` to Responses: what Chat
/// Completions would refuse.
fn to_responses(body: Value) -> bool {
    use respan_gateway_conversation_http::Schema;
    use respan_gateway_conversation_http::openai::OpenAiChat;
    let input = OpenAiChat
        .decode_request(body.to_string().into())
        .expect("a chat");
    respan_gateway_vendors_green::openai::chat_refuses(&input)
}

#[test]
fn openai_responses_only_takes_tools_with_reasoning_and_its_models() {
    let tools = json!([{ "type": "function", "function": { "name": "f", "parameters": {} } }]);
    let chat = |model: &str, extra: Value| {
        let mut body = json!({ "model": model, "messages": [{ "role": "user", "content": "hi" }] });
        for (k, v) in extra.as_object().cloned().unwrap_or_default() {
            body[k] = v;
        }
        body
    };
    let both = json!({ "tools": tools, "reasoning_effort": "high" });
    assert!(to_responses(chat("gpt-6-luna", both)));
    // From gpt-5.6 they reason unless told not to: tools alone are refused.
    let alone = json!({ "tools": tools });
    for model in ["gpt-6-luna", "gpt-5.6-sol", "gpt-6.1-sol"] {
        assert!(to_responses(chat(model, alone.clone())), "{model}");
    }
    for model in ["gpt-5.5", "gpt-5-mini", "gpt-4.1-mini", "o3"] {
        assert!(!to_responses(chat(model, alone.clone())), "{model}");
    }
    let off = json!({ "tools": tools, "reasoning_effort": "none" });
    assert!(!to_responses(chat("gpt-6-luna", off)), "no reasoning");
    assert!(to_responses(chat("o3-pro", json!({}))));
    assert!(to_responses(chat("gpt-5-codex", json!({}))));
    assert!(!to_responses(chat("gpt-4o", json!({}))));
    // Fields both have go with it.
    let shared = json!({ "tools": tools, "reasoning_effort": "high", "store": false,
        "metadata": { "a": "b" }, "service_tier": "auto", "verbosity": "low",
        "prompt_cache_key": "k", "max_tokens": 100, "parallel_tool_calls": true });
    assert!(to_responses(chat("gpt-6-luna", shared)));
    let n = json!({ "tools": tools, "reasoning_effort": "high", "n": 2 });
    assert!(
        !to_responses(chat("gpt-5-mini", n)),
        "only Chat Completions has n"
    );
}
