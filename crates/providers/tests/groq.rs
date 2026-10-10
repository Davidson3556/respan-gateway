//! Groq's quirks.

mod support;

use respan_gateway_conversation::{Output, Usage};
use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_providers::groq;
use serde_json::json;

#[test]
fn usage_under() {
    let body = json!({ "id": "c", "choices": [{ "index": 0, "message": { "content": "hi" } }],
        "x_groq": { "usage": { "prompt_tokens": 7, "completion_tokens": 3 } } });
    let usage = Output::usage(Usage::new(7, 3));
    assert!(support::whole(groq::CHAT, body.clone()).contains(&usage));
    // OpenAI's own reads none of it.
    assert!(!support::whole(OpenAiChat::OPENAI, body).contains(&usage));
}
