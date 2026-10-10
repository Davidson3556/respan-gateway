//! OpenRouter's quirks.

mod support;

use respan_gateway_conversation::{Finish, Output};
use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_providers::openrouter;
use serde_json::json;

#[test]
fn inert_choice() {
    let chunk = json!({ "id": "c", "choices": [{ "index": 0, "delta": {},
        "finish_reason": "stop", "native_finish_reason": "end_turn" }] });
    let events = support::streamed(openrouter::CHAT, std::slice::from_ref(&chunk));
    assert!(events.contains(&Output::finished(Finish::Stop)));
    assert!(!support::any_native(&events));
    assert!(support::any_native(&support::streamed(
        OpenAiChat::OPENAI,
        &[chunk]
    )));
}
