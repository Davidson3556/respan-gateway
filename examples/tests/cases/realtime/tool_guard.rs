#![allow(clippy::unwrap_used)]
use super::*;
#[path = "../../../support/live/test.rs"]
mod support;
use serde_json::{Value, json};
use support::*;

/// Gemini Live, as a script: a question is answered with a call of a tool
/// not allowed, and that call's answer with text.
fn gemini(message: Value) -> Vec<Value> {
    if message.get("setup").is_some() {
        return vec![json!({"setupComplete": {}})];
    }
    if message["realtimeInput"].get("text").is_some() {
        return vec![json!({"toolCall": {"functionCalls": [
            {"id": "c1", "name": "delete_account", "args": {}}]}})];
    }
    if message.get("toolResponse").is_some() {
        return vec![
            json!({"serverContent": {"modelTurn": {"parts": [{"text": "I cannot do that."}]}}}),
            json!({"serverContent": {"turnComplete": true}}),
        ];
    }
    Vec::new()
}

#[tokio::test]
async fn a_call_not_allowed_never_reaches_the_client_and_the_model_hears_why() {
    let mut live = start("/v1/realtime?model=gemini-live", gemini, |base| {
        gateway(GeminiLive.transport(base).unwrap(), &["weather"]).unwrap()
    })
    .await;
    assert_eq!(next(&mut live).await.unwrap()["type"], "session.created");
    let tool = |name: &str| json!({"type": "function", "name": name, "parameters": {}});
    let session = json!({"type": "session.update", "session": {"type": "realtime",
        "output_modalities": ["text"], "tools": [tool("weather"), tool("delete_account")]}});
    send(&mut live, session).await;
    assert_eq!(next(&mut live).await.unwrap()["type"], "session.updated");
    let ask = json!({"type": "conversation.item.create", "item": {"type": "message",
        "role": "user", "content": [{"type": "input_text", "text": "Delete my account."}]}});
    send(&mut live, ask).await;
    send(&mut live, json!({"type": "response.create"})).await;
    let mut heard = Vec::new();
    while let Some(event) = next(&mut live).await {
        let done = event["type"] == "response.done" && event["response"]["output"] != json!([]);
        heard.push(event);
        if done {
            break;
        }
    }
    close(&mut live).await;
    assert!(
        heard
            .iter()
            .all(|e| e["type"] != "response.function_call_arguments.done"),
        "{heard:?}"
    );
    let text: String = heard
        .iter()
        .filter_map(|e| {
            e["delta"]
                .as_str()
                .filter(|_| e["type"] == "response.output_text.delta")
        })
        .collect();
    assert_eq!(text, "I cannot do that.");
    let sent = live.received.lock();
    let tools = &sent[0]["setup"]["tools"][0]["functionDeclarations"];
    assert_eq!(
        *tools,
        json!([{"name": "weather", "parametersJsonSchema": {}}])
    );
    let answer = sent
        .iter()
        .find(|m| m.get("toolResponse").is_some())
        .unwrap();
    assert_eq!(
        answer["toolResponse"]["functionResponses"][0],
        json!({"id": "c1", "name": "delete_account",
            "response": {"error": "this tool is not allowed"}})
    );
}
