//! OpenRouter's quirks.

#[path = "../../tests/support/mod.rs"]
mod support;

use respan_gateway_conversation_http::dialect::Dialected;
use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_vendors_yellow::openrouter::OpenRouter;
use support::{Vendor, chat, data, json, messages};

fn finished() -> Vendor {
    Vendor::streaming_in(
        Dialected::new(OpenAiChat, OpenRouter),
        &[
            json!({ "id": "c", "choices": [{ "index": 0, "delta": { "content": "hi" } }] }),
            json!({ "id": "c", "choices": [{ "index": 0, "delta": {},
                "finish_reason": "stop", "native_finish_reason": "end_turn" }] }),
        ],
    )
}

#[tokio::test]
async fn inert_choice_is_dropped_for_anthropic_and_kept_for_openai_chat() {
    let events = data(&finished().anthropic(messages(true)).await);
    let stop = events
        .iter()
        .find(|e| e["type"] == "message_delta")
        .expect("the stop");
    assert_eq!(stop["delta"]["stop_reason"], "end_turn");
    let chunks = data(&finished().openai(chat(true)).await);
    let kept = chunks
        .iter()
        .any(|c| c["choices"][0]["native_finish_reason"] == "end_turn");
    assert!(kept, "{chunks:?}");
}
