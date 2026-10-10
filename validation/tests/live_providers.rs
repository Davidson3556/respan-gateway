//! End-to-end tests of the shipped binary against real providers.
//!
//! Ignored by default: they cost money and need the network. Each makes
//! one small call and skips, passing, when its key is not set.
//!
//! Run: `OPENAI_API_KEY=... ANTHROPIC_API_KEY=... cargo test --manifest-path validation/Cargo.toml
//! --test live_providers -- --ignored`. Override the models
//! with `OPENAI_MODEL` and `ANTHROPIC_MODEL`.

#![allow(clippy::unwrap_used)]

#[path = "../../apps/gateway/tests/support/mod.rs"]
mod support;

use support::{Gateway, client, json, read_all};

fn model(var: &str, default: &str) -> String {
    std::env::var(var).unwrap_or_else(|_| default.to_owned())
}

fn openai_config(serve: &str) -> String {
    let base = model("OPENAI_BASE_URL", "https://api.openai.com/v1");
    // JSON strings are also YAML strings; do not interpolate URLs as YAML.
    let base = serde_json::to_string(&base).unwrap();
    format!(
        "providers:\n  p:\n    format: openai-chat\n    base_url: {base}\n    auth: {{ type: bearer, key: env:OPENAI_API_KEY }}\nserve: [{serve}]\ntarget: p\n"
    )
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
    let gw = Gateway::start(&openai_config("openai-chat")).await;
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
#[ignore = "calls an OpenAI-compatible provider; needs OPENAI_API_KEY"]
async fn real_openai_streamed_unicode_tool_call() {
    if !has("OPENAI_API_KEY") {
        return;
    }
    let gw = Gateway::start(&openai_config("openai-chat")).await;
    let request = serde_json::json!({
        "model": model("OPENAI_MODEL", "gpt-4o-mini"), "max_completion_tokens":256,
        "stream":true, "stream_options":{"include_usage":true},
        "messages":[{"role":"user","content":"Call echo with text exactly 雪😀."}],
        "tools":[{"type":"function","function":{"name":"echo","parameters":{"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}}}],
        "tool_choice":{"type":"function","function":{"name":"echo"}}
    });
    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .body(request.to_string())
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let (body, ended) = read_all(resp).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(ended, Ok(()));
    assert!(body.ends_with("data: [DONE]\n\n"), "{body}");
    let mut name = String::new();
    let mut args = String::new();
    let mut finished = false;
    let mut usage = false;
    for data in body
        .lines()
        .filter_map(|l| l.strip_prefix("data: "))
        .filter(|d| *d != "[DONE]")
    {
        let event = json(data);
        for call in event["choices"][0]["delta"]["tool_calls"]
            .as_array()
            .into_iter()
            .flatten()
        {
            assert_eq!(call["index"], 0, "{event}");
            name.push_str(call["function"]["name"].as_str().unwrap_or(""));
            args.push_str(call["function"]["arguments"].as_str().unwrap_or(""));
        }
        finished |= event["choices"][0]["finish_reason"] == "tool_calls";
        usage |= event["usage"]["total_tokens"]
            .as_u64()
            .is_some_and(|n| n > 0);
    }
    assert_eq!(name, "echo", "{body}");
    assert_eq!(json(&args)["text"], "雪😀");
    assert!(finished && usage, "{body}");
}

#[tokio::test]
#[ignore = "calls the real Anthropic API; needs ANTHROPIC_API_KEY"]
async fn real_anthropic_streamed_tool_call_translates_to_openai() {
    if !has("ANTHROPIC_API_KEY") {
        return;
    }
    let base =
        serde_json::to_string(&model("ANTHROPIC_BASE_URL", "https://api.anthropic.com")).unwrap();
    let gw = Gateway::start(&format!("providers:\n  p:\n    format: anthropic-messages\n    base_url: {base}\n    auth: {{ type: header, name: x-api-key, key: env:ANTHROPIC_API_KEY }}\nserve: [openai-chat]\ntarget: p\n")).await;
    let request = serde_json::json!({
        "model":model("ANTHROPIC_MODEL","claude-haiku-4-5"),"max_tokens":256,"stream":true,
        "messages":[{"role":"user","content":"Call echo with text exactly 雪😀."}],
        "tools":[{"type":"function","function":{"name":"echo","description":"Echo text","parameters":{"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}}}],
        "tool_choice":{"type":"function","function":{"name":"echo"}}
    });
    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .body(request.to_string())
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let (body, ended) = read_all(resp).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(ended, Ok(()));
    let mut args = String::new();
    let mut name = String::new();
    let mut finished = false;
    for data in body
        .lines()
        .filter_map(|l| l.strip_prefix("data: "))
        .filter(|d| *d != "[DONE]")
    {
        let event = json(data);
        for call in event["choices"][0]["delta"]["tool_calls"]
            .as_array()
            .into_iter()
            .flatten()
        {
            name.push_str(call["function"]["name"].as_str().unwrap_or(""));
            args.push_str(call["function"]["arguments"].as_str().unwrap_or(""));
        }
        finished |= event["choices"][0]["finish_reason"] == "tool_calls";
    }
    assert_eq!(name, "echo", "{body}");
    assert_eq!(json(&args)["text"], "雪😀");
    assert!(finished && body.ends_with("data: [DONE]\n\n"), "{body}");
}

#[tokio::test]
#[ignore = "calls Gemini native HTTP through passthrough; needs GEMINI_API_KEY"]
async fn real_gemini_native_stream_through_passthrough() {
    if !has("GEMINI_API_KEY") {
        return;
    }
    let gw = Gateway::start("providers:\n  p:\n    format: passthrough\n    base_url: https://generativelanguage.googleapis.com\n    auth: { type: header, name: x-goog-api-key, key: env:GEMINI_API_KEY }\nserve: [passthrough]\ntarget: p\n").await;
    let path = format!(
        "/v1beta/models/{}:streamGenerateContent?alt=sse",
        model("GEMINI_MODEL", "gemini-3.8-flash")
    );
    let request = serde_json::json!({"contents":[{"parts":[{"text":"Say ok."}]}],"generationConfig":{"maxOutputTokens":64,"thinkingConfig":{"thinkingBudget":0}}});
    let resp = client()
        .post(gw.url(&path))
        .header("content-type", "application/json")
        .body(request.to_string())
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let (body, ended) = read_all(resp).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(ended, Ok(()));
    let mut text = String::new();
    let mut finished = false;
    for data in body.lines().filter_map(|l| l.strip_prefix("data: ")) {
        let event = json(data);
        for part in event["candidates"][0]["content"]["parts"]
            .as_array()
            .into_iter()
            .flatten()
        {
            text.push_str(part["text"].as_str().unwrap_or(""));
        }
        finished |= event["candidates"][0]["finishReason"] == "STOP";
    }
    assert!(!text.trim().is_empty() && finished, "{body}");
}

#[tokio::test]
#[ignore = "calls TypeSafe systemone through passthrough; needs TYPESAFE_API_KEY"]
async fn real_typesafe_custom_decisions_through_passthrough() {
    if !has("TYPESAFE_API_KEY") {
        return;
    }
    let gw = Gateway::start("providers:\n  p:\n    format: passthrough\n    base_url: https://api.typesafe.ai\n    auth: { type: bearer, key: env:TYPESAFE_API_KEY }\nserve: [passthrough]\ntarget: p\n").await;
    let request = serde_json::json!({"model":"jev-latest","state":"The sky is blue.","questions":{"blue":{"type":"noul","instructions":"Is the sky blue?"}}});
    let resp = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(45))
        .build()
        .unwrap()
        .post(gw.url("/v1/systemone"))
        .header("content-type", "application/json")
        .body(request.to_string())
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body = resp.text().await.unwrap();
    assert_eq!(status, 200, "{body}");
    let body = json(&body);
    assert_eq!(body["answers"]["blue"]["type"], "noul", "{body}");
    let probability = body["answers"]["blue"]["noul"]
        .as_f64()
        .expect("numeric answer");
    assert!((0.0..=1.0).contains(&probability), "{body}");
    assert!(
        body["usage"]["input_tokens"]
            .as_u64()
            .is_some_and(|n| n > 0),
        "{body}"
    );
}

#[tokio::test]
#[ignore = "calls an OpenAI-compatible provider through passthrough; needs OPENAI_API_KEY"]
async fn real_compatible_native_tool_stream_through_passthrough() {
    if !has("OPENAI_API_KEY") {
        return;
    }
    let base =
        serde_json::to_string(&model("OPENAI_BASE_URL", "https://api.openai.com/v1")).unwrap();
    let gw = Gateway::start(&format!("providers:\n  p:\n    format: passthrough\n    base_url: {base}\n    auth: {{ type: bearer, key: env:OPENAI_API_KEY }}\nserve: [passthrough]\ntarget: p\n")).await;
    let request = serde_json::json!({
        "model":model("OPENAI_MODEL","gpt-4o-mini"),"max_tokens":256,"stream":true,
        "stream_options":{"include_usage":true},
        "messages":[{"role":"user","content":"Call echo with text exactly 雪😀."}],
        "tools":[{"type":"function","function":{"name":"echo","parameters":{"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}}}],
        "tool_choice":{"type":"function","function":{"name":"echo"}}
    });
    let resp = client()
        .post(gw.url("/chat/completions"))
        .header("content-type", "application/json")
        .body(request.to_string())
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let (body, ended) = read_all(resp).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(ended, Ok(()));
    let mut name = String::new();
    let mut args = String::new();
    let mut finished = false;
    let mut usage = false;
    for data in body
        .lines()
        .filter_map(|l| l.strip_prefix("data: "))
        .filter(|d| *d != "[DONE]")
    {
        let event = json(data);
        assert!(event.get("error").is_none(), "{body}");
        for call in event["choices"][0]["delta"]["tool_calls"]
            .as_array()
            .into_iter()
            .flatten()
        {
            name.push_str(call["function"]["name"].as_str().unwrap_or(""));
            args.push_str(call["function"]["arguments"].as_str().unwrap_or(""));
        }
        finished |= event["choices"][0]["finish_reason"] == "tool_calls";
        usage |= event["usage"]["total_tokens"]
            .as_u64()
            .is_some_and(|n| n > 0);
    }
    assert_eq!(name, "echo", "{body}");
    assert_eq!(json(&args)["text"], "雪😀");
    assert!(finished && usage, "{body}");
    // Passthrough preserves the vendor's native framing, including providers
    // which finish without the OpenAI format's required [DONE] sentinel.
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
