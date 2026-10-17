//! Respan's quirks. Its catalog is tested in `models.rs`.

#[path = "../../tests/support/mod.rs"]
mod support;

use respan_gateway_conversation_http::dialect::Dialected;
use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_vendors_green::respan::Respan;
use support::{Vendor, chat, json, messages};

/// A reply over its Chat Completions, as it sends one: the choice and its
/// message with what the vendor behind it said beyond the spec, and its
/// reasoning as `reasoning_content` and again as Anthropic's
/// `thinking_blocks`.
fn vendor() -> Vendor {
    Vendor::whole_in(
        Dialected::new(OpenAiChat, Respan),
        json!({ "id": "c", "object": "chat.completion", "model": "gpt-4o-mini",
            "choices": [{ "index": 0, "finish_reason": "stop", "message": {
                "role": "assistant", "content": "Hi there!", "annotations": [],
                "reasoning_content": "Greet back.",
                "thinking_blocks": [{ "type": "thinking", "thinking": "Greet back.",
                    "signature": "sig" }],
                "provider_specific_fields": { "refusal": null } },
                "provider_specific_fields": {} }],
            "usage": { "prompt_tokens": 9, "completion_tokens": 3, "total_tokens": 12 } }),
    )
}

#[tokio::test]
async fn what_only_describes_is_dropped_and_reasoning_read_for_another_format() {
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

/// Claude's thinking, as Respan streams it when Claude thinks unasked:
/// empty text but its signature, in `thinking_blocks` beside
/// `reasoning_content`, before the answer.
#[tokio::test]
async fn a_streamed_signature_in_thinking_blocks_is_no_refusal() {
    let chunk = |delta: serde_json::Value, finish: Option<&str>| {
        json!({ "id": "c", "object": "chat.completion.chunk", "model": "claude-sonnet-5-5",
            "choices": [{ "index": 0, "delta": delta, "finish_reason": finish }] })
    };
    let thinking = |signature: &str| {
        let blocks = json!([{ "type": "thinking", "thinking": "", "signature": signature }]);
        chunk(
            json!({ "role": "assistant", "content": "", "reasoning_content": "",
                "thinking_blocks": blocks, "provider_specific_fields": { "thinking_blocks": blocks } }),
            None,
        )
    };
    let vendor = Vendor::streaming_in(
        Dialected::new(OpenAiChat, Respan),
        &[
            thinking(""),
            thinking("CAQS"),
            chunk(
                json!({ "role": "assistant", "content": "I was made by Anthropic." }),
                None,
            ),
            chunk(json!({ "role": "assistant" }), Some("stop")),
        ],
    );
    let answer = vendor.anthropic(messages(true)).await;
    assert!(answer.contains("I was made by Anthropic."), "{answer}");
    assert!(!answer.contains("untranslatable"), "{answer}");
}
