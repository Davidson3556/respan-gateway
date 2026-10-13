//! End-to-end tests of vendors' quirks: the shipped binary, a provider of
//! each quirky vendor that is `mockllm`'s strict OpenAI fake, and a client
//! that gets what its spec says.
//!
//! Run: `cargo test -p respan-gateway --test quirks`.

#![allow(clippy::unwrap_used)]

mod support;

use mockllm::{Answer, FakeOpenAi};
use serde_json::{Value, json};
use support::{Gateway, client, json, read_all};

/// The binary with one provider, `p`, of `vendor` (none if empty) at the
/// fake, with a bearer key as the fake takes it, serving OpenAI Chat and
/// Anthropic Messages.
async fn gateway(vendor: &str, fake: &FakeOpenAi) -> Gateway {
    let vendor = match vendor {
        "" => "formats = \"openai-chat\"".to_owned(),
        vendor => format!("vendor = \"{vendor}\""),
    };
    let base = fake.base_url();
    Gateway::start(&format!("providers = {{ p = {{ {vendor}, base_url = \"{base}\", auth = {{ type = \"bearer\", key = \"sk-test\" }} }} }}\nformats = [\"openai-chat\", \"anthropic-messages\"]\nplugins = {{ routing = {{ default = \"p\" }} }}\n"))
    .await
}

/// A stream of `chunks`, then `[DONE]`.
fn stream(chunks: &[Value]) -> Answer {
    let mut events: Vec<Value> = chunks.iter().map(|c| json!(format!("data: {c}"))).collect();
    events.push(json!("data: [DONE]"));
    Answer::events(events)
}

/// What a client of `path` gets for `body`, and whether it ended cleanly.
async fn ask(gw: &Gateway, path: &str, body: Value) -> String {
    let resp = client()
        .post(gw.url(path))
        .header("content-type", "application/json")
        .header("anthropic-version", "2023-06-01")
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let (text, ended) = read_all(resp).await;
    assert!(ended.is_ok(), "{text}");
    text
}

fn chat(stream: bool) -> Value {
    json!({ "model": "m", "stream": stream, "stream_options": { "include_usage": true },
        "messages": [{ "role": "user", "content": "hi" }] })
}

fn messages(stream: bool) -> Value {
    json!({ "model": "m", "max_tokens": 10, "stream": stream,
        "messages": [{ "role": "user", "content": "hi" }] })
}

/// The JSON of each data line of a stream.
fn data(stream: &str) -> Vec<Value> {
    let lines = stream.lines().filter_map(|l| l.strip_prefix("data: "));
    lines.filter_map(|d| serde_json::from_str(d).ok()).collect()
}

#[tokio::test]
async fn groqs_usage_under_x_groq_is_the_clients_usage() {
    let fake = FakeOpenAi::start().await;
    let gw = gateway("groq", &fake).await;
    fake.answer(stream(&[
        json!({ "id": "c", "object": "chat.completion.chunk", "created": 1, "model": "m",
            "x_groq": { "id": "r" }, "choices": [{ "index": 0, "delta": { "content": "hi" } }] }),
        json!({ "id": "c", "object": "chat.completion.chunk", "created": 1, "model": "m",
            "x_groq": { "id": "r", "usage": { "prompt_tokens": 7, "completion_tokens": 3 } },
            "choices": [{ "index": 0, "delta": {}, "finish_reason": "stop" }] }),
    ]));

    let chunks = data(&ask(&gw, "/v1/chat/completions", chat(true)).await);

    let usage = &chunks.last().unwrap()["usage"];
    assert_eq!(usage["total_tokens"], 10, "{chunks:?}");
}

#[tokio::test]
async fn deepseeks_reasoning_content_is_an_anthropic_clients_thinking() {
    let fake = FakeOpenAi::start().await;
    let gw = gateway("deepseek", &fake).await;
    fake.answer(stream(&[
        json!({ "id": "c", "object": "chat.completion.chunk", "created": 1, "model": "m",
            "choices": [{ "index": 0, "delta": { "reasoning_content": "hm" } }] }),
        json!({ "id": "c", "object": "chat.completion.chunk", "created": 1, "model": "m",
            "choices": [{ "index": 0, "delta": { "content": "hi" }, "finish_reason": "stop" }] }),
    ]));

    let events = data(&ask(&gw, "/v1/messages", messages(true)).await);

    let blocks: Vec<&Value> = events
        .iter()
        .filter(|e| e["type"] == "content_block_start")
        .map(|e| &e["content_block"]["type"])
        .collect();
    assert_eq!(blocks, ["thinking", "text"], "{events:?}");
}

#[tokio::test]
async fn vllms_reasoning_content_and_stop_reason_reach_anthropic_whole() {
    let fake = FakeOpenAi::start().await;
    let gw = gateway("vllm", &fake).await;
    let answer = json!({ "id": "c", "object": "chat.completion", "created": 1, "model": "m",
        "choices": [{ "index": 0, "finish_reason": "stop", "stop_reason": "</s>",
            "message": { "role": "assistant", "reasoning_content": "hm", "content": "hi" } }] });
    fake.answer(Answer::raw(answer.to_string()));

    let answer = json(&ask(&gw, "/v1/messages", messages(false)).await);

    assert_eq!(
        answer["content"][0],
        json!({ "type": "thinking", "thinking": "hm" })
    );
    assert_eq!(answer["content"][1]["text"], "hi");
}

/// A choice's own finish reason only describes it: an Anthropic client gets
/// the stream, and a provider of no vendor, which has no quirks, refuses it.
#[tokio::test]
async fn openrouters_native_finish_reason_is_dropped_for_anthropic() {
    let chunks = [
        json!({ "id": "c", "object": "chat.completion.chunk", "created": 1, "model": "m",
            "choices": [{ "index": 0, "delta": { "content": "hi" } }] }),
        json!({ "id": "c", "object": "chat.completion.chunk", "created": 1, "model": "m",
            "choices": [{ "index": 0, "delta": {}, "finish_reason": "stop",
                "native_finish_reason": "end_turn" }] }),
    ];
    let fake = FakeOpenAi::start().await;
    let gw = gateway("openrouter", &fake).await;
    fake.answer(stream(&chunks));
    let out = ask(&gw, "/v1/messages", messages(true)).await;
    assert!(
        out.ends_with("data: {\"type\":\"message_stop\"}\n\n"),
        "{out}"
    );

    let fake = FakeOpenAi::start().await;
    let gw = gateway("", &fake).await;
    fake.answer(stream(&chunks));
    let out = ask(&gw, "/v1/messages", messages(true)).await;
    assert!(out.contains("event: error"), "{out}");
}

#[tokio::test]
async fn azures_moderation_results_are_dropped_for_anthropic() {
    let fake = FakeOpenAi::start().await;
    let gw = gateway("azure", &fake).await;
    let answer = json!({ "id": "c", "object": "chat.completion", "created": 1, "model": "m",
        "choices": [{ "index": 0, "finish_reason": "stop",
            "message": { "role": "assistant", "content": "hi" },
            "content_filter_results": { "hate": { "filtered": false, "severity": "safe" } } }] });
    fake.answer(Answer::raw(answer.to_string()));

    let answer = json(&ask(&gw, "/v1/messages", messages(false)).await);

    assert_eq!(answer["content"][0]["text"], "hi");
}
