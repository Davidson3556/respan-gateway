//! DeepSeek's quirks.

#[path = "../../tests/support/mod.rs"]
mod support;

use respan_gateway_conversation_http::dialect::Dialected;
use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_vendors_yellow::deepseek::DeepSeek;
use support::{Vendor, chat, data, json, messages};

fn streamed() -> Vendor {
    Vendor::streaming_in(
        Dialected::new(OpenAiChat, DeepSeek),
        &[
            json!({ "id": "c", "choices": [{ "index": 0, "delta": { "reasoning_content": "hm" } }] }),
            json!({ "id": "c", "choices": [{ "index": 0, "delta": { "reasoning_content": "m" } }] }),
            json!({ "id": "c", "choices": [{ "index": 0, "delta": { "content": "hi" } }] }),
            json!({ "id": "c", "choices": [{ "index": 0, "delta": {}, "finish_reason": "stop" }] }),
        ],
    )
}

#[tokio::test]
async fn reasoning_content_is_thinking_for_anthropic() {
    let events = data(&streamed().anthropic(messages(true)).await);
    let blocks: Vec<_> = events
        .iter()
        .filter(|e| e["type"] == "content_block_start")
        .map(|e| (e["index"].clone(), e["content_block"]["type"].clone()))
        .collect();
    assert_eq!(
        blocks,
        [(json!(0), json!("thinking")), (json!(1), json!("text"))]
    );
    let thinking: String = events
        .iter()
        .filter_map(|e| e["delta"]["thinking"].as_str())
        .collect();
    assert_eq!(thinking, "hmm");
}

#[tokio::test]
async fn reasoning_content_relays_to_openai_chat_as_it_came() {
    let chunks = data(&streamed().openai(chat(true)).await);
    let said: String = chunks
        .iter()
        .filter_map(|c| c["choices"][0]["delta"]["reasoning_content"].as_str())
        .collect();
    assert_eq!(said, "hmm");
}

#[tokio::test]
async fn reasoning_content_is_written_from_anthropics_thinking() {
    let deepseek = Vendor::whole_in(
        Dialected::new(OpenAiChat, DeepSeek),
        json!({ "id": "c", "choices": [{ "index": 0, "message": { "content": "ok" }, "finish_reason": "stop" }] }),
    );
    let mut request = messages(false);
    request["messages"] = json!([
        { "role": "user", "content": "hi" },
        { "role": "assistant", "content": [
            { "type": "thinking", "thinking": "hm" }, { "type": "text", "text": "yes" }] },
        { "role": "user", "content": "and?" },
    ]);
    deepseek.anthropic(request).await;
    let sent = &deepseek.sent()[0]["messages"][1];
    assert_eq!(sent["reasoning_content"], "hm");
    assert_eq!(sent["content"], "yes");
}
