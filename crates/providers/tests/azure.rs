//! Azure OpenAI's quirks.

mod support;

use respan_gateway_conversation::Output;
use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_providers::azure;
use serde_json::json;

#[test]
fn inert_choice() {
    let chunk = json!({ "id": "c", "choices": [{ "index": 0, "delta": { "content": "hi" },
        "content_filter_results": { "hate": { "filtered": false, "severity": "safe" } } }] });
    let events = support::streamed(azure::CHAT, std::slice::from_ref(&chunk));
    assert!(events.contains(&Output::text(0, "hi")));
    assert!(!support::any_native(&events));
    assert!(support::any_native(&support::streamed(
        OpenAiChat::OPENAI,
        &[chunk]
    )));
}
