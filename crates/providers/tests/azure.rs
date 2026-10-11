//! Azure OpenAI's quirks.

mod support;

use respan_gateway_providers::azure::AzureQuirks;
use support::{Vendor, data, json, messages};

/// Its moderation annotation, even after the finish, ends a stream to
/// Anthropic Messages cleanly.
#[tokio::test]
async fn inert_choice_after_the_finish_ends_a_translated_stream_cleanly() {
    let azure = Vendor::streaming(
        &AzureQuirks,
        &[
            json!({ "id": "c", "choices": [{ "index": 0, "delta": { "content": "hi" },
                "content_filter_results": { "hate": { "filtered": false, "severity": "safe" } } }] }),
            json!({ "id": "c", "choices": [{ "index": 0, "delta": {}, "finish_reason": "stop" }] }),
            json!({ "id": "", "choices": [{ "index": 0, "delta": {},
                "content_filter_offsets": { "check_offset": 0, "start_offset": 0, "end_offset": 2 },
                "content_filter_results": { "hate": { "filtered": false, "severity": "safe" } } }] }),
        ],
    );
    let out = azure.anthropic(messages(true)).await;
    assert!(!out.contains("event: error"), "{out}");
    let events = data(&out);
    assert_eq!(
        events.last().map(|e| e["type"].clone()),
        Some(json!("message_stop"))
    );
}
