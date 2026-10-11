//! Groq's quirks.

mod support;

use respan_gateway_providers::groq::GroqQuirks;
use support::{Vendor, chat, data, json, messages};

#[tokio::test]
async fn x_groq_usage_is_a_streams_usage_and_its_other_fields_stay() {
    let groq = Vendor::streaming(
        &GroqQuirks,
        &[
            json!({ "id": "c", "x_groq": { "id": "r" },
                "choices": [{ "index": 0, "delta": { "content": "hi" } }] }),
            json!({ "id": "c", "x_groq": { "id": "r",
                    "usage": { "prompt_tokens": 7, "completion_tokens": 3, "queue_time": 0.1 } },
                "choices": [{ "index": 0, "delta": {}, "finish_reason": "stop" }] }),
        ],
    );
    let chunks = data(&groq.openai(chat(true)).await);
    let usage = &chunks.last().expect("the usage chunk")["usage"];
    assert_eq!(
        (&usage["prompt_tokens"], &usage["completion_tokens"]),
        (&json!(7), &json!(3))
    );
    assert_eq!(usage["queue_time"], 0.1);
    assert!(
        chunks.iter().all(|c| c["x_groq"] == json!({ "id": "r" })),
        "{chunks:?}"
    );
}

#[tokio::test]
async fn x_groq_usage_is_a_whole_bodys_usage_in_another_format_too() {
    let body = json!({ "id": "c", "x_groq": { "usage": { "prompt_tokens": 7, "completion_tokens": 3 } },
        "choices": [{ "index": 0, "message": { "content": "hi" }, "finish_reason": "stop" }] });
    let groq = Vendor::whole(&GroqQuirks, body.clone());
    let answer: serde_json::Value =
        serde_json::from_str(&groq.openai(chat(false)).await).expect("JSON");
    assert_eq!(answer["usage"]["total_tokens"], 10);
    assert!(answer.get("x_groq").is_none(), "{answer}");
    let groq = Vendor::whole(&GroqQuirks, body);
    let answer: serde_json::Value =
        serde_json::from_str(&groq.anthropic(messages(false)).await).expect("JSON");
    assert_eq!(answer["usage"]["input_tokens"], 7);
    assert_eq!(answer["usage"]["output_tokens"], 3);
}

/// Said in `usage` too, it counts once.
#[tokio::test]
async fn x_groq_usage_beside_usage_counts_once() {
    let usage = json!({ "prompt_tokens": 7, "completion_tokens": 3 });
    let groq = Vendor::streaming(
        &GroqQuirks,
        &[
            json!({ "id": "c", "choices": [{ "index": 0, "delta": { "content": "hi" } }] }),
            json!({ "id": "c", "x_groq": { "usage": usage }, "usage": usage,
                "choices": [{ "index": 0, "delta": {}, "finish_reason": "stop" }] }),
        ],
    );
    let chunks = data(&groq.openai(chat(true)).await);
    let said = &chunks.last().expect("the usage chunk")["usage"];
    assert_eq!(said["total_tokens"], 10, "{chunks:?}");
}
