//! End-to-end tests of the shipped binary against real providers.
//!
//! Ignored by default: they cost money and need the network. Each makes
//! one small call and skips, passing, when its key is not set.
//!
//! Run: `OPENAI_API_KEY=... ANTHROPIC_API_KEY=... GEMINI_API_KEY=...
//! RESPAN_API_KEY=... TYPESAFE_API_KEY=... cargo test --manifest-path
//! validation/Cargo.toml --test live_providers -- --ignored`. The keys,
//! base URLs and models each reads are listed in `validation/README.md`.

#![allow(clippy::unwrap_used)]

#[path = "../../apps/gateway/tests/support/mod.rs"]
mod support;

use support::{Gateway, client, json, read_all};

fn model(var: &str, default: &str) -> String {
    std::env::var(var).unwrap_or_else(|_| default.to_owned())
}

fn openai_config(serve: &str) -> String {
    let base = model("OPENAI_BASE_URL", "https://api.openai.com/v1");
    // Quote URLs before inserting them into TOML.
    let base = serde_json::to_string(&base).unwrap();
    format!(
        "providers = {{ p = {{ formats = \"openai-chat\", base_url = {base}, auth = {{ type = \"bearer\", key = \"env:OPENAI_API_KEY\" }} }} }}\nformats = [\"{serve}\"]\nplugins = {{ routing = {{ default = \"p\" }} }}\n"
    )
}

/// Anthropic's Messages at `ANTHROPIC_BASE_URL`, else its API, for clients
/// of OpenAI Chat.
fn anthropic_config() -> String {
    let base = model("ANTHROPIC_BASE_URL", "https://api.anthropic.com");
    let base = serde_json::to_string(&base).unwrap();
    format!(
        "providers = {{ p = {{ formats = \"anthropic-messages\", base_url = {base}, auth = {{ type = \"header\", name = \"x-api-key\", key = \"env:ANTHROPIC_API_KEY\" }} }} }}\nformats = [\"openai-chat\"]\nplugins = {{ routing = {{ default = \"p\" }} }}\n"
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
    let gw = Gateway::start(&anthropic_config()).await;
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
    let gw = Gateway::start("providers = { p = { formats = \"passthrough\", base_url = \"https://generativelanguage.googleapis.com\", auth = { type = \"header\", name = \"x-goog-api-key\", key = \"env:GEMINI_API_KEY\" } } }\nformats = [\"passthrough\"]\nplugins = { routing = { default = \"p\" } }\n").await;
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
        "providers = {{ p = {{ vendor = \"gemini\", formats = \"gemini-generate\" }} }}\nformats = [\"{serve}\"]\nplugins = {{ routing = {{ default = \"p\" }} }}\n"
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
        &chat(
            serde_json::json!({ "model": gemini_model(), "max_tokens": 1024,
            "messages": [ask], "tools": [tool], "tool_choice": "required" }),
        )
        .await,
    );
    let call = &first["choices"][0]["message"]["tool_calls"][0];
    assert_eq!(
        first["choices"][0]["finish_reason"], "tool_calls",
        "{first}"
    );
    assert_eq!(call["function"]["name"], "echo");
    assert_eq!(
        json(call["function"]["arguments"].as_str().unwrap())["text"],
        "雪😀"
    );
    assert!(first["usage"]["prompt_tokens"].as_u64() > Some(0));
    let streamed = chat(
        serde_json::json!({ "model": gemini_model(), "max_tokens": 1024,
        "stream": true, "stream_options": { "include_usage": true },
        "messages": [serde_json::json!({ "role": "user", "content": "Say ok." })] }),
    )
    .await;
    assert!(streamed.ends_with("data: [DONE]\n\n"), "{streamed}");
    assert!(
        streamed.contains("\"finish_reason\":\"stop\""),
        "{streamed}"
    );
    let second = json(
        &chat(
            serde_json::json!({ "model": gemini_model(), "max_tokens": 1024,
            "messages": [ask,
                { "role": "assistant", "content": null, "tool_calls": [call] },
                { "role": "tool", "tool_call_id": call["id"], "content": "雪😀" }] }),
        )
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
    let gw = Gateway::start("providers = { p = { formats = \"passthrough\", base_url = \"https://api.typesafe.ai\", auth = { type = \"bearer\", key = \"env:TYPESAFE_API_KEY\" } } }\nformats = [\"passthrough\"]\nplugins = { routing = { default = \"p\" } }\n").await;
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
    let gw = Gateway::start(&format!("providers = {{ p = {{ formats = \"passthrough\", base_url = {base}, auth = {{ type = \"bearer\", key = \"env:OPENAI_API_KEY\" }} }} }}\nformats = [\"passthrough\"]\nplugins = {{ routing = {{ default = \"p\" }} }}\n")).await;
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
        // Passthrough sends OpenAI's own: a named tool choice ends as `stop`.
        finished |= event["choices"][0]["finish_reason"].is_string();
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
    let gw = Gateway::start(&anthropic_config()).await;
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
        "providers = {{ p = {{ formats = \"openai-responses\", base_url = {base}, auth = {{ type = \"bearer\", key = \"env:OPENAI_API_KEY\" }} }} }}\nformats = [\"{serve}\"]\nplugins = {{ routing = {{ default = \"p\" }} }}\n"
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

// Respan: one key for many vendors' models, in each spec it serves.

fn respan_model() -> String {
    model("RESPAN_MODEL", "gpt-4o-mini")
}

/// Posts `body` to `path` as a client of its format, whole or streamed.
async fn ask(gw: &Gateway, path: &str, body: serde_json::Value) -> String {
    let resp = slow_client()
        .post(gw.url(path))
        .header("anthropic-version", "2023-06-01")
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let (body, ended) = read_all(resp).await;
    assert_eq!(status, 200, "{path}: {body}");
    assert_eq!(ended, Ok(()), "{path}");
    body
}

#[tokio::test]
#[ignore = "calls the real Respan gateway; needs RESPAN_API_KEY"]
async fn real_respan_lists_its_catalog_and_serves_each_spec() {
    if !has("RESPAN_API_KEY") {
        return;
    }
    let gw = Gateway::start(&vendor_config(
        "respan",
        "openai-chat, openai-responses, anthropic-messages, gemini-generate",
    ))
    .await;
    let list = client().get(gw.url("/v1/models")).send().await.unwrap();
    let list = json(&list.text().await.unwrap());
    let data = list["data"].as_array().unwrap();
    assert!(data.len() > 100, "{}", data.len());
    let mini = data.iter().find(|m| m["id"] == "gpt-4o-mini").unwrap();
    assert_eq!(mini["owned_by"], "p");
    assert!(mini["pricing"]["input"].as_f64() > Some(0.0), "{mini}");
    assert!(mini["context_window"].as_u64() > Some(0), "{mini}");
    assert!(
        !data
            .iter()
            .any(|m| m["id"].as_str().is_some_and(|id| id.ends_with(":batch")))
    );

    let hi = serde_json::json!([{ "role": "user", "content": "Say ok." }]);
    let chat = json(
        &ask(
            &gw,
            "/v1/chat/completions",
            serde_json::json!({ "model": respan_model(), "max_tokens": 16, "messages": hi }),
        )
        .await,
    );
    assert!(
        chat["choices"][0]["message"]["content"].is_string(),
        "{chat}"
    );
    let streamed = ask(
        &gw,
        "/v1/chat/completions",
        serde_json::json!({ "model": respan_model(),
        "max_tokens": 16, "stream": true, "stream_options": { "include_usage": true },
        "messages": hi }),
    )
    .await;
    assert!(streamed.ends_with("data: [DONE]\n\n"), "{streamed}");
    let responses = json(
        &ask(
            &gw,
            "/v1/responses",
            serde_json::json!({
        "model": respan_model(), "max_output_tokens": 16, "input": "Say ok." }),
        )
        .await,
    );
    assert_eq!(responses["status"], "completed", "{responses}");
    let claude = json(
        &ask(
            &gw,
            "/v1/messages",
            serde_json::json!({
        "model": model("RESPAN_CLAUDE", "claude-haiku-4-5"), "max_tokens": 16,
        "messages": hi }),
        )
        .await,
    );
    assert_eq!(claude["type"], "message", "{claude}");
    let gemini = model("RESPAN_GEMINI", "gemini-3.5-flash-lite");
    let said = json(
        &ask(
            &gw,
            &format!("/v1beta/models/{gemini}:generateContent"),
            serde_json::json!({ "contents": [{ "role": "user", "parts": [{ "text": "Say ok." }] }],
            "generationConfig": { "maxOutputTokens": 64 } }),
        )
        .await,
    );
    assert!(said["candidates"][0]["content"].is_object(), "{said}");
}

#[tokio::test]
#[ignore = "calls the real Respan gateway; needs RESPAN_API_KEY"]
async fn real_respan_chat_answers_an_anthropic_client_streamed() {
    if !has("RESPAN_API_KEY") {
        return;
    }
    // Only its Chat Completions: the gateway translates.
    let gw = Gateway::start("providers = { p = { vendor = \"respan\", formats = \"openai-chat\" } }\nformats = [\"anthropic-messages\"]\nplugins = { routing = { default = \"p\" } }\n").await;
    let hi = serde_json::json!([{ "role": "user", "content": "Say ok." }]);
    let whole = json(
        &ask(
            &gw,
            "/v1/messages",
            serde_json::json!({
        "model": respan_model(), "max_tokens": 16, "messages": hi }),
        )
        .await,
    );
    assert_eq!(whole["content"][0]["type"], "text", "{whole}");
    assert!(
        whole["usage"]["output_tokens"].as_u64() > Some(0),
        "{whole}"
    );
    let streamed = ask(
        &gw,
        "/v1/messages",
        serde_json::json!({
        "model": respan_model(), "max_tokens": 16, "stream": true, "messages": hi }),
    )
    .await;
    assert!(streamed.contains("event: message_stop"), "{streamed}");
    // A model behind it that reasons: its reasoning, read as thinking.
    let reasoned = json(
        &ask(
            &gw,
            "/v1/messages",
            serde_json::json!({
        "model": model("RESPAN_THINKER", "span-02"), "max_tokens": 64, "messages": hi }),
        )
        .await,
    );
    assert_eq!(reasoned["type"], "message", "{reasoned}");
}

#[tokio::test]
#[ignore = "calls the real Respan gateway; needs RESPAN_API_KEY"]
async fn real_respan_chat_with_tools_and_reasoning_goes_to_its_responses() {
    if !has("RESPAN_API_KEY") {
        return;
    }
    let gw = Gateway::start(&vendor_config("respan", "openai-chat")).await;
    let tool = serde_json::json!({ "type": "function", "function": { "name": "echo",
        "description": "Echoes text back",
        "parameters": { "type": "object", "properties": { "text": { "type": "string" } },
            "required": ["text"] } } });
    let body = |stream: bool| {
        serde_json::json!({
        "model": model("RESPAN_REASONER", "gpt-6-luna"), "reasoning_effort": "low",
        "stream": stream, "tools": [tool], "tool_choice": "required",
        "messages": [{ "role": "user", "content": "Call echo with text exactly 雪😀." }] })
    };
    let whole = json(&ask(&gw, "/v1/chat/completions", body(false)).await);
    let call = &whole["choices"][0]["message"]["tool_calls"][0];
    assert_eq!(call["function"]["name"], "echo", "{whole}");
    assert_eq!(
        json(call["function"]["arguments"].as_str().unwrap())["text"],
        "雪😀"
    );
    let streamed = ask(&gw, "/v1/chat/completions", body(true)).await;
    assert!(
        streamed.contains("\"finish_reason\":\"tool_calls\""),
        "{streamed}"
    );
}

// Media kinds. Each call is the smallest the provider allows.

/// A vendor (its key comes from the environment) serving `formats`.
fn vendor_config(vendor: &str, formats: &str) -> String {
    let formats = formats
        .split(',')
        .map(str::trim)
        .map(|f| serde_json::to_string(f).unwrap())
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "providers = {{ p = {{ vendor = \"{vendor}\" }} }}\nformats = [{formats}]\nplugins = {{ routing = {{ default = \"p\" }} }}\n"
    )
}

/// A client that waits for slow media.
fn slow_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(120))
        .build()
        .unwrap()
}

async fn post_json(gw: &Gateway, path: &str, body: serde_json::Value) -> (u16, bytes::Bytes) {
    let resp = slow_client()
        .post(gw.url(path))
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    (resp.status().as_u16(), resp.bytes().await.unwrap())
}

fn parse(body: &[u8]) -> serde_json::Value {
    json(std::str::from_utf8(body).unwrap_or("<binary>"))
}

#[tokio::test]
#[ignore = "calls the real OpenAI API; needs OPENAI_API_KEY"]
async fn real_openai_embeddings() {
    if !has("OPENAI_API_KEY") {
        return;
    }
    let gw = Gateway::start(&vendor_config("openai", "openai-embeddings")).await;
    let (status, body) = post_json(
        &gw,
        "/v1/embeddings",
        serde_json::json!({ "model": "text-embedding-3-small", "input": "hi" }),
    )
    .await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let body = parse(&body);
    let vector = body["data"][0]["embedding"].as_array().unwrap();
    assert!(
        vector.len() > 100 && vector.iter().all(|n| n.is_number()),
        "{body}"
    );
    assert!(
        body["usage"]["total_tokens"].as_u64().unwrap_or(0) > 0,
        "{body}"
    );
}

#[tokio::test]
#[ignore = "calls the real OpenAI API; needs OPENAI_API_KEY"]
async fn real_openai_image_generation() {
    if !has("OPENAI_API_KEY") {
        return;
    }
    let gw = Gateway::start(&vendor_config("openai", "openai-images")).await;
    let (status, body) = post_json(
        &gw,
        "/v1/images/generations",
        serde_json::json!({ "model": model("OPENAI_IMAGE_MODEL", "gpt-image-1"),
            "prompt": "a plain red circle", "size": "1024x1024", "quality": "low" }),
    )
    .await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let body = parse(&body);
    let image = body["data"][0]["b64_json"]
        .as_str()
        .unwrap_or_else(|| panic!("{body}"));
    assert!(
        image.starts_with("iVBOR"),
        "not a PNG: {}",
        &image[..20.min(image.len())]
    );
}

#[tokio::test]
#[ignore = "calls the real OpenAI API; needs OPENAI_API_KEY"]
async fn real_openai_speech_then_transcription() {
    if !has("OPENAI_API_KEY") {
        return;
    }
    let gw = Gateway::start(&vendor_config(
        "openai",
        "openai-speech, openai-transcriptions",
    ))
    .await;
    let (status, audio) = post_json(
        &gw,
        "/v1/audio/speech",
        serde_json::json!({ "model": "tts-1", "voice": "alloy", "input": "Hello world.",
            "response_format": "mp3" }),
    )
    .await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&audio));
    assert!(audio.len() > 1000, "{} bytes", audio.len());

    // The audio back through the gateway's multipart route.
    let boundary = "respan-validation-boundary";
    let mut form = Vec::new();
    let mut part = |headers: &str, data: &[u8]| {
        form.extend_from_slice(format!("--{boundary}\r\n{headers}\r\n\r\n").as_bytes());
        form.extend_from_slice(data);
        form.extend_from_slice(b"\r\n");
    };
    part(
        "Content-Disposition: form-data; name=\"model\"",
        b"whisper-1",
    );
    part(
        "Content-Disposition: form-data; name=\"file\"; filename=\"hello.mp3\"\r\nContent-Type: audio/mpeg",
        &audio,
    );
    form.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
    let resp = slow_client()
        .post(gw.url("/v1/audio/transcriptions"))
        .header(
            "content-type",
            format!("multipart/form-data; boundary={boundary}"),
        )
        .body(form)
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body = resp.text().await.unwrap();
    assert_eq!(status, 200, "{body}");
    let text = json(&body)["text"].as_str().unwrap().to_lowercase();
    assert!(text.contains("hello"), "{body}");
}

#[tokio::test]
#[ignore = "calls the real Gemini API; needs GEMINI_API_KEY"]
async fn real_gemini_embeddings() {
    if !has("GEMINI_API_KEY") {
        return;
    }
    let gw = Gateway::start(&vendor_config("gemini", "openai-embeddings")).await;
    let (status, body) = post_json(
        &gw,
        "/v1/embeddings",
        serde_json::json!({ "model": model("GEMINI_EMBEDDING_MODEL", "gemini-embedding-001"),
            "input": "hi" }),
    )
    .await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let body = parse(&body);
    assert!(
        body["data"][0]["embedding"]
            .as_array()
            .is_some_and(|v| v.len() > 100),
        "{body}"
    );
}

#[tokio::test]
#[ignore = "calls the real Gemini API; needs GEMINI_API_KEY"]
async fn real_gemini_image_generation() {
    if !has("GEMINI_API_KEY") {
        return;
    }
    let gw = Gateway::start(&vendor_config("gemini", "openai-images")).await;
    let (status, body) = post_json(
        &gw,
        "/v1/images/generations",
        serde_json::json!({ "model": model("GEMINI_IMAGE_MODEL", "gemini-2.5-flash-image"),
            "prompt": "a plain red circle", "response_format": "b64_json", "n": 1 }),
    )
    .await;
    if status == 429 {
        // The reply is the provider's, relayed: a key without image quota (free tier) is not a failure.
        eprintln!("skipped: no image quota on this key");
        return;
    }
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let body = parse(&body);
    assert!(
        body["data"][0]["b64_json"]
            .as_str()
            .is_some_and(|s| s.len() > 100),
        "{body}"
    );
}
