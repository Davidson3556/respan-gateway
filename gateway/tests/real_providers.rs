//! End-to-end tests of the shipped binary against real providers.
//!
//! Ignored by default: they cost money and need the network. Each makes
//! one small call and skips, passing, when its key is not set.
//!
//! Run: `OPENAI_API_KEY=... ANTHROPIC_API_KEY=... cargo test -p
//! respan-gateway --test real_providers -- --ignored`. Override the models
//! with `OPENAI_MODEL` and `ANTHROPIC_MODEL`.

#![allow(clippy::unwrap_used)]

mod support;

use support::{Gateway, client, json, read_all};

fn model(var: &str, default: &str) -> String {
    std::env::var(var).unwrap_or_else(|_| default.to_owned())
}

fn has(var: &str) -> bool {
    let set = std::env::var(var).is_ok_and(|v| !v.is_empty());
    if !set {
        eprintln!("skipped: {var} is not set");
    }
    set
}

#[tokio::test]
#[ignore = "calls the real OpenAI API; needs OPENAI_API_KEY; run with --ignored"]
async fn real_openai_chat_relay() {
    if !has("OPENAI_API_KEY") {
        return;
    }
    let gw = Gateway::start(
        "providers:
  openai:
    format: openai-chat
    base_url: https://api.openai.com/v1
    auth: { type: bearer, key: env:OPENAI_API_KEY }
serve: [openai-chat]
target: openai
",
    )
    .await;
    let request = format!(
        r#"{{"model":"{}","max_completion_tokens":16,"messages":[{{"role":"user","content":"Say ok."}}]}}"#,
        model("OPENAI_MODEL", "gpt-4o-mini")
    );

    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .body(request)
        .send()
        .await
        .unwrap();

    let status = resp.status();
    let body = resp.text().await.unwrap();
    assert_eq!(status, 200, "{body}");
    let body = json(&body);
    assert_eq!(body["object"], "chat.completion");
    assert!(
        body["choices"][0]["message"]["content"].is_string(),
        "{body}"
    );
}

#[tokio::test]
#[ignore = "calls the real Anthropic API; needs ANTHROPIC_API_KEY; run with --ignored"]
async fn real_openai_client_to_anthropic_streamed() {
    if !has("ANTHROPIC_API_KEY") {
        return;
    }
    let gw = Gateway::start(
        "providers:
  anthropic:
    format: anthropic-messages
    base_url: https://api.anthropic.com
    auth: { type: header, name: x-api-key, key: env:ANTHROPIC_API_KEY }
serve: [openai-chat]
target: anthropic
",
    )
    .await;
    let request = format!(
        r#"{{"model":"{}","max_tokens":16,"stream":true,"messages":[{{"role":"user","content":"Say ok."}}]}}"#,
        model("ANTHROPIC_MODEL", "claude-haiku-4-5")
    );

    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .body(request)
        .send()
        .await
        .unwrap();

    let status = resp.status();
    let (body, ended) = read_all(resp).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(ended, Ok(()));
    let mut text = String::new();
    for data in body.lines().filter_map(|l| l.strip_prefix("data: ")) {
        if data != "[DONE]" {
            let v = json(data);
            text.push_str(v["choices"][0]["delta"]["content"].as_str().unwrap_or(""));
        }
    }
    assert!(!text.is_empty(), "{body}");
    assert!(body.ends_with("data: [DONE]\n\n"), "{body}");
}
