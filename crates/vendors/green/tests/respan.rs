//! Respan's quirks. Its catalog is tested in `models.rs`.

#[path = "../../tests/support/mod.rs"]
mod support;

use respan_gateway_conversation_http::dialect::Dialected;
use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_vendors_green::respan::Respan;
use support::{Vendor, chat, json, messages};

/// A reply over its Chat Completions, as it sends one: the choice and its
/// message with what the vendor behind it said beyond the spec, and its
/// reasoning as `reasoning_content`.
fn vendor() -> Vendor {
    Vendor::whole_in(
        Dialected::new(OpenAiChat, Respan),
        json!({ "id": "c", "object": "chat.completion", "model": "gpt-4o-mini",
            "choices": [{ "index": 0, "finish_reason": "stop", "message": {
                "role": "assistant", "content": "Hi there!", "annotations": [],
                "reasoning_content": "Greet back.",
                "provider_specific_fields": { "refusal": null } },
                "provider_specific_fields": {} }],
            "usage": { "prompt_tokens": 9, "completion_tokens": 3, "total_tokens": 12 } }),
    )
}

#[tokio::test]
async fn provider_specific_fields_are_dropped_and_reasoning_read_for_another_format() {
    let answer: serde_json::Value =
        serde_json::from_str(&vendor().anthropic(messages(false)).await).expect("JSON");
    assert_eq!(answer["content"][0]["type"], "thinking", "{answer}");
    assert_eq!(answer["content"][0]["thinking"], "Greet back.", "{answer}");
    assert_eq!(answer["content"][1]["text"], "Hi there!", "{answer}");

    let answer: serde_json::Value =
        serde_json::from_str(&vendor().openai(chat(false)).await).expect("JSON");
    let message = &answer["choices"][0]["message"];
    assert_eq!(
        message["provider_specific_fields"],
        json!({ "refusal": null }),
        "{answer}"
    );
}
