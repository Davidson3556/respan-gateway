//! OpenRouter's quirks.

mod support;

use respan_gateway_providers::openrouter::OpenRouterQuirks;
use support::{Vendor, chat, data, json, messages};

fn finished() -> Vendor {
    Vendor::streaming(
        &OpenRouterQuirks,
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
