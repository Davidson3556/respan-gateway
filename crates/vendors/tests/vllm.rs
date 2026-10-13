//! vLLM's quirks.

mod support;

use respan_gateway_vendors::vllm::VllmQuirks;
use support::{Vendor, chat, data, json, messages};

#[tokio::test]
async fn reasoning_content_is_thinking_for_anthropic() {
    let vllm = Vendor::whole(
        &VllmQuirks,
        json!({ "id": "c", "choices": [{ "index": 0, "finish_reason": "stop", "stop_reason": null,
            "message": { "role": "assistant", "reasoning_content": "hm", "content": "hi" } }] }),
    );
    let answer: serde_json::Value =
        serde_json::from_str(&vllm.anthropic(messages(false)).await).expect("JSON");
    assert_eq!(
        answer["content"][0],
        json!({ "type": "thinking", "thinking": "hm" })
    );
    assert_eq!(answer["content"][1]["text"], "hi");
}

fn stopped() -> Vendor {
    Vendor::streaming(
        &VllmQuirks,
        &[
            json!({ "id": "c", "choices": [{ "index": 0, "delta": { "content": "hi" } }] }),
            json!({ "id": "c", "choices": [{ "index": 0, "delta": {},
                "finish_reason": "stop", "stop_reason": "</s>" }] }),
        ],
    )
}

#[tokio::test]
async fn inert_choice_is_dropped_for_anthropic_and_kept_for_openai_chat() {
    let events = data(&stopped().anthropic(messages(true)).await);
    assert_eq!(
        events.last().map(|e| e["type"].clone()),
        Some(json!("message_stop"))
    );
    let chunks = data(&stopped().openai(chat(true)).await);
    assert!(
        chunks
            .iter()
            .any(|c| c["choices"][0]["stop_reason"] == "</s>"),
        "{chunks:?}"
    );
}
