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
        "providers:\n  p:\n    formats: openai-chat\n    base_url: {base}\n    auth: {{ type: bearer, key: env:OPENAI_API_KEY }}\nformats: [{serve}]\ndefault: p\n"
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
    let gw = Gateway::start(&format!("providers:\n  p:\n    formats: anthropic-messages\n    base_url: {base}\n    auth: {{ type: header, name: x-api-key, key: env:ANTHROPIC_API_KEY }}\nformats: [openai-chat]\ndefault: p\n")).await;
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
    let gw = Gateway::start("providers:\n  p:\n    formats: passthrough\n    base_url: https://generativelanguage.googleapis.com\n    auth: { type: header, name: x-goog-api-key, key: env:GEMINI_API_KEY }\nformats: [passthrough]\ndefault: p\n").await;
    let path = format!(
        "/v1beta/models/{}:streamGenerateContent?alt=sse",
        model("GEMINI_MODEL", "gemini-3.8-flash")
    );
    let request = serde_json::json!({"contents":[{"parts":[{"text":"Say ok."}]}],"generationConfig":{"maxOutputTokens":256,"thinkingConfig":{"thinkingLevel":"low"}}});
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

fn gemini_config(serve: &str) -> String {
    // The vendor, so its quirks run, speaking only the native format.
    format!(
        "providers:\n  p:\n    vendor: gemini\n    formats: gemini-generate\nformats: [{serve}]\ndefault: p\n"
    )
}

fn gemini_model() -> String {
    model("GEMINI_MODEL", "gemini-3.8-flash")
}

#[tokio::test]
#[ignore = "calls the real Gemini API; needs GEMINI_API_KEY"]
async fn real_gemini_generate_content_relay_and_tool_round_trip() {
    if !has("GEMINI_API_KEY") {
        return;
    }
    let gw = Gateway::start(&gemini_config("gemini-generate")).await;
    let model = gemini_model();
    let post = |method: &str, body: serde_json::Value| {
        let url = gw.url(&format!("/v1beta/models/{model}:{method}"));
        async move {
            let resp = client()
                .post(url)
                .header("content-type", "application/json")
                .body(body.to_string())
                .send()
                .await
                .unwrap();
            let status = resp.status();
            let (body, ended) = read_all(resp).await;
            assert_eq!(status, 200, "{body}");
            assert_eq!(ended, Ok(()));
            body
        }
    };
    let hi = serde_json::json!({
        "systemInstruction": { "parts": [{ "text": "Answer in one word." }] },
        "contents": [{ "role": "user", "parts": [{ "text": "Say ok." }] }],
        "generationConfig": { "maxOutputTokens": 1024 },
    });
    let whole = json(&post("generateContent", hi.clone()).await);
    assert_eq!(whole["candidates"][0]["finishReason"], "STOP", "{whole}");
    assert!(whole["usageMetadata"]["promptTokenCount"].as_u64() > Some(0));
    let streamed = post("streamGenerateContent?alt=sse", hi).await;
    let chunks: Vec<_> = streamed
        .lines()
        .filter_map(|l| l.strip_prefix("data: "))
        .map(json)
        .collect();
    let last = chunks.last().unwrap();
    assert_eq!(last["candidates"][0]["finishReason"], "STOP", "{streamed}");
    assert!(last["usageMetadata"]["totalTokenCount"].as_u64() > Some(0));

    // A function call, and the conversation going on with its result.
    let tools = serde_json::json!([{ "functionDeclarations": [{
        "name": "echo", "description": "Echoes text back",
        "parameters": { "type": "OBJECT", "properties": { "text": { "type": "STRING" } },
            "required": ["text"] } }] }]);
    let ask = serde_json::json!({ "role": "user", "parts": [{ "text": "Call echo with text exactly 雪😀." }] });
    let config = serde_json::json!({ "maxOutputTokens": 1024 });
    let first = json(
        &post(
            "generateContent",
            serde_json::json!({ "contents": [ask], "tools": tools, "generationConfig": config }),
        )
        .await,
    );
    let call = first["candidates"][0]["content"]["parts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p.get("functionCall").is_some())
        .unwrap_or_else(|| panic!("{first}"))
        .clone();
    assert_eq!(call["functionCall"]["name"], "echo");
    assert_eq!(call["functionCall"]["args"]["text"], "雪😀", "{first}");
    let second = json(
        &post(
            "generateContent",
            serde_json::json!({ "contents": [ask,
                { "role": "model", "parts": [call.clone()] },
                { "role": "user", "parts": [{ "functionResponse": { "name": "echo",
                    "response": { "output": "雪😀" } } }] }],
                "tools": tools, "generationConfig": config }),
        )
        .await,
    );
    assert_eq!(second["candidates"][0]["finishReason"], "STOP", "{second}");
}

#[tokio::test]
#[ignore = "calls the real Gemini API through translation; needs GEMINI_API_KEY"]
async fn real_openai_client_reaches_gemini_generate_content() {
    if !has("GEMINI_API_KEY") {
        return;
    }
    let gw = Gateway::start(&gemini_config("openai-chat")).await;
    let tool = serde_json::json!({ "type": "function", "function": { "name": "echo",
        "description": "Echoes text back",
        "parameters": { "type": "object", "properties": { "text": { "type": "string" } },
            "required": ["text"] } } });
    let chat = |body: serde_json::Value| {
        let url = gw.url("/v1/chat/completions");
        async move {
            let resp = client()
                .post(url)
                .body(body.to_string())
                .send()
                .await
                .unwrap();
            let status = resp.status();
            let (body, ended) = read_all(resp).await;
            assert_eq!(status, 200, "{body}");
            assert_eq!(ended, Ok(()));
            body
        }
    };
    let ask = serde_json::json!({ "role": "user", "content": "Call echo with text exactly 雪😀." });
    let first = json(
        &chat(serde_json::json!({ "model": gemini_model(), "max_tokens": 1024,
            "messages": [ask], "tools": [tool], "tool_choice": "required" }))
        .await,
    );
    let call = &first["choices"][0]["message"]["tool_calls"][0];
    assert_eq!(first["choices"][0]["finish_reason"], "tool_calls", "{first}");
    assert_eq!(call["function"]["name"], "echo");
    assert_eq!(json(call["function"]["arguments"].as_str().unwrap())["text"], "雪😀");
    assert!(first["usage"]["prompt_tokens"].as_u64() > Some(0));
    let streamed = chat(serde_json::json!({ "model": gemini_model(), "max_tokens": 1024,
        "stream": true, "stream_options": { "include_usage": true },
        "messages": [serde_json::json!({ "role": "user", "content": "Say ok." })] }))
    .await;
    assert!(streamed.ends_with("data: [DONE]\n\n"), "{streamed}");
    assert!(streamed.contains("\"finish_reason\":\"stop\""), "{streamed}");
    let second = json(
        &chat(serde_json::json!({ "model": gemini_model(), "max_tokens": 1024,
            "messages": [ask,
                { "role": "assistant", "content": null, "tool_calls": [call] },
                { "role": "tool", "tool_call_id": call["id"], "content": "雪😀" }] }))
        .await,
    );
    assert_eq!(second["choices"][0]["finish_reason"], "stop", "{second}");
}

#[tokio::test]
#[ignore = "calls TypeSafe systemone through passthrough; needs TYPESAFE_API_KEY"]
async fn real_typesafe_custom_decisions_through_passthrough() {
    if !has("TYPESAFE_API_KEY") {
        return;
    }
    let gw = Gateway::start("providers:\n  p:\n    formats: passthrough\n    base_url: https://api.typesafe.ai\n    auth: { type: bearer, key: env:TYPESAFE_API_KEY }\nformats: [passthrough]\ndefault: p\n").await;
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
    let gw = Gateway::start(&format!("providers:\n  p:\n    formats: passthrough\n    base_url: {base}\n    auth: {{ type: bearer, key: env:OPENAI_API_KEY }}\nformats: [passthrough]\ndefault: p\n")).await;
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
    formats: anthropic-messages
    base_url: https://api.anthropic.com
    auth: { type: header, name: x-api-key, key: env:ANTHROPIC_API_KEY }
formats: [openai-chat]
default: anthropic
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

fn responses_config(serve: &str) -> String {
    let base = model("OPENAI_BASE_URL", "https://api.openai.com/v1");
    let base = serde_json::to_string(&base).unwrap();
    format!(
        "providers:\n  p:\n    formats: openai-responses\n    base_url: {base}\n    auth: {{ type: bearer, key: env:OPENAI_API_KEY }}\nformats: [{serve}]\ndefault: p\n"
    )
}

#[tokio::test]
#[ignore = "calls the real OpenAI API; needs OPENAI_API_KEY; run with --ignored"]
async fn real_openai_responses_relay_whole_and_streamed() {
    if !has("OPENAI_API_KEY") {
        return;
    }
    let gw = Gateway::start(&responses_config("openai-responses")).await;
    let request = |stream: bool| {
        serde_json::json!({
            "model": model("OPENAI_MODEL", "gpt-4o-mini"), "max_output_tokens": 16,
            "stream": stream, "instructions": "Be brief.", "input": "Say ok.",
        })
        .to_string()
    };

    let resp = client()
        .post(gw.url("/v1/responses"))
        .body(request(false))
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body = resp.text().await.unwrap();
    assert_eq!(status, 200, "{body}");
    let body = json(&body);
    assert_eq!(body["object"], "response");
    assert!(body["id"].as_str().unwrap().starts_with("resp_"), "{body}");
    assert!(
        body["output"][0]["content"][0]["text"].is_string(),
        "{body}"
    );
    assert!(
        body["usage"]["output_tokens"].as_u64().unwrap() > 0,
        "{body}"
    );

    let resp = client()
        .post(gw.url("/v1/responses"))
        .body(request(true))
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let (body, ended) = read_all(resp).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(ended, Ok(()));
    let events: Vec<_> = body
        .lines()
        .filter_map(|l| l.strip_prefix("data: "))
        .map(json)
        .collect();
    for (i, e) in events.iter().enumerate() {
        assert_eq!(e["sequence_number"], i, "{body}");
    }
    assert_eq!(events[0]["type"], "response.created", "{body}");
    let last = &events[events.len() - 1];
    assert_eq!(last["type"], "response.completed", "{body}");
    assert!(last["response"]["usage"]["total_tokens"].as_u64().unwrap() > 0);
    assert!(
        events
            .iter()
            .any(|e| e["type"] == "response.output_text.delta")
    );
}

#[tokio::test]
#[ignore = "calls the real OpenAI API; needs OPENAI_API_KEY; run with --ignored"]
async fn real_openai_chat_client_to_responses_provider() {
    if !has("OPENAI_API_KEY") {
        return;
    }
    let gw = Gateway::start(&responses_config("openai-chat")).await;
    let request = serde_json::json!({
        "model": model("OPENAI_MODEL", "gpt-4o-mini"), "max_completion_tokens": 16,
        "messages": [{"role": "system", "content": "Be brief."}, {"role": "user", "content": "Say ok."}],
    });
    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .body(request.to_string())
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
    assert!(
        body["usage"]["completion_tokens"].as_u64().unwrap() > 0,
        "{body}"
    );
}
