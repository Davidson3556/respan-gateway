//! DeepSeek's quirks.

mod support;

use respan_gateway_conversation::{Block, Output};
use respan_gateway_providers::deepseek;
use serde_json::json;

#[test]
fn reasoning_content() {
    let chunk =
        json!({ "id": "c", "choices": [{ "index": 0, "delta": { "reasoning_content": "hm" } }] });
    let events = support::streamed(deepseek::CHAT, &[chunk]);
    assert!(events.contains(&Output::block_start(0, Block::Reasoning)));
    assert!(events.contains(&Output::reasoning(0, "hm")));
}
