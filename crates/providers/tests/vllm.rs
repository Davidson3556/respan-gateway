//! vLLM's quirks.

mod support;

use respan_gateway_conversation::{Block, Finish, Output};
use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_providers::vllm;
use serde_json::json;

#[test]
fn reasoning_content() {
    let chunk =
        json!({ "id": "c", "choices": [{ "index": 0, "delta": { "reasoning_content": "hm" } }] });
    let events = support::streamed(vllm::CHAT, &[chunk]);
    assert!(events.contains(&Output::block_start(0, Block::Reasoning)));
    assert!(events.contains(&Output::reasoning(0, "hm")));
}

#[test]
fn inert_choice() {
    let chunk = json!({ "id": "c", "choices": [{ "index": 0, "delta": {},
        "finish_reason": "stop", "stop_reason": "</s>" }] });
    let events = support::streamed(vllm::CHAT, std::slice::from_ref(&chunk));
    assert!(events.contains(&Output::finished(Finish::Stop)));
    assert!(!support::any_native(&events));
    assert!(support::any_native(&support::streamed(
        OpenAiChat::OPENAI,
        &[chunk]
    )));
}
