//! End-to-end tests of the shipped binary for the two native chat formats
//! that SDKs speak: Gemini's `generateContent` and OpenAI's Responses.
//!
//! Each is relayed to a provider of the same format and translated to a
//! provider of another (OpenAI Chat for Gemini, Anthropic Messages for
//! Responses), whole and streamed. The providers are strict `mockllm`
//! fakes; the request log is checked for the usage each call carries.
//!
//! Run: `cargo test -p respan-gateway --test e2e_native`.

#![allow(clippy::unwrap_used)]

mod support;

use mockllm::{Answer, FakeAnthropic, FakeGemini, FakeOpenAi};
use serde_json::{Value, json};
use support::{Gateway, client, json};

const GEMINI_MODEL: &str = "gemini-2.5-flash";
const GPT: &str = "gpt-4.1-mini";
const CLAUDE: &str = "claude-sonnet-4-5";

async fn post(gw: &Gateway, path: &str, body: Value) -> (u16, String) {
    let resp = client()
        .post(gw.url(path))
        .header("content-type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    (resp.status().as_u16(), resp.text().await.unwrap())
}

/// The finished request log record of `path` (streamed or not), once the gateway has it.
async fn record(gw: &Gateway, path: &str, stream: bool) -> Value {
    for _ in 0..200 {
        let resp = client()
            .get(gw.url("/ui/api/requests?limit=50"))
            .send()
            .await
            .unwrap();
        let log = json(&resp.text().await.unwrap());
        let found = log["requests"].as_array().unwrap().iter().find(|r| {
            r["path"] == path
                && r["stream"] == stream
                && !r["status"].is_null()
                && !r["duration_ms"].is_null()
        });
        if let Some(found) = found {
            return found.clone();
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("no finished {path} in the request log");
}

fn tokens(record: &Value) -> (u64, u64) {
    let usage = &record["usage"];
    (
        usage["input_tokens"].as_u64().unwrap_or(0),
        usage["output_tokens"].as_u64().unwrap_or(0),
    )
}

/// The `data:` payloads of an SSE body, as JSON (`[DONE]` as a string).
fn data(body: &str) -> Vec<Value> {
    body.lines()
        .filter_map(|l| l.strip_prefix("data: "))
        .map(|d| serde_json::from_str(d).unwrap_or_else(|_| json!(d)))
        .collect()
}

// -- Gemini ---------------------------------------------------------------

fn gemini_ask(text: &str) -> Value {
    json!({ "contents": [{ "role": "user", "parts": [{ "text": text }] }] })
}

fn generate(model: &str) -> String {
    format!("/v1beta/models/{model}:generateContent")
}

fn stream_generate(model: &str) -> String {
    format!("/v1beta/models/{model}:streamGenerateContent?alt=sse")
}

fn gemini_config(fake: &str) -> String {
    format!(
        "providers = {{ g = {{ vendor = \"gemini\", formats = \"gemini-generate\", base_url = \"{fake}/v1beta/openai\", key = \"gw-gemini-key\" }} }}\nformats = [\"gemini-generate\"]\nplugins = {{ routing = {{ default = \"g\" }} }}\n"
    )
}

fn gemini_over_openai_config(fake: &str) -> String {
    format!(
        "providers = {{ o = {{ formats = \"openai-chat\", base_url = \"{fake}\", auth = {{ type = \"bearer\", key = \"sk-gw\" }} }} }}\nformats = [\"gemini-generate\"]\nplugins = {{ routing = {{ default = \"o\" }} }}\n"
    )
}

/// The text of a Gemini response's first candidate.
fn candidate_text(body: &Value) -> String {
    body["candidates"][0]["content"]["parts"]
        .as_array()
        .unwrap_or_else(|| panic!("no parts in {body}"))
        .iter()
        .filter_map(|p| p["text"].as_str())
        .collect()
}

#[tokio::test]
async fn a_gemini_sdk_request_is_relayed_whole_and_streamed_to_a_gemini_provider() {
    let fake = FakeGemini::start().await;
    let gw = Gateway::start(&gemini_config(&fake.base_url())).await;

    fake.answer(Answer::text("Paris"));
    let (status, body) = post(
        &gw,
        &generate(GEMINI_MODEL),
        gemini_ask("capital of France?"),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let body = json(&body);
    assert_eq!(candidate_text(&body), "Paris");
    let seen = &fake.requests()[0];
    assert_eq!(seen.path, generate(GEMINI_MODEL));
    assert_eq!(
        seen.header("x-goog-api-key"),
        Some("gw-gemini-key"),
        "the gateway's key"
    );
    let logged = record(&gw, &generate(GEMINI_MODEL), false).await;
    assert_eq!(logged["format"], "gemini-generate");
    assert_eq!(logged["stream"], false);
    let (input, output) = tokens(&logged);
    assert_eq!(
        input,
        body["usageMetadata"]["promptTokenCount"].as_u64().unwrap()
    );
    assert_eq!(
        output,
        body["usageMetadata"]["candidatesTokenCount"]
            .as_u64()
            .unwrap()
    );
    assert!(input > 0 && output > 0, "{logged}");

    fake.answer(Answer::text("Paris, France"));
    let (status, body) = post(
        &gw,
        &stream_generate(GEMINI_MODEL),
        gemini_ask("capital of France?"),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let events = data(&body);
    assert!(events.len() > 1, "{body}");
    let text: String = events.iter().map(candidate_text_or_empty).collect();
    assert_eq!(text, "Paris, France");
    assert!(
        events
            .iter()
            .any(|e| e["candidates"][0]["finishReason"] == "STOP"),
        "{body}"
    );
    assert_eq!(
        fake.requests()[1].path,
        format!("/v1beta/models/{GEMINI_MODEL}:streamGenerateContent")
    );
    let logged = record(
        &gw,
        &format!("/v1beta/models/{GEMINI_MODEL}:streamGenerateContent"),
        true,
    )
    .await;
    assert_eq!(logged["stream"], true);
    let (input, output) = tokens(&logged);
    assert!(input > 0 && output > 0, "{logged}");
}

fn candidate_text_or_empty(event: &Value) -> String {
    if event["candidates"][0]["content"]["parts"].is_array() {
        candidate_text(event)
    } else {
        String::new()
    }
}

#[tokio::test]
async fn a_gemini_sdk_request_is_translated_to_an_openai_provider() {
    let fake = FakeOpenAi::start().await;
    let gw = Gateway::start(&gemini_over_openai_config(&fake.base_url())).await;

    fake.answer(Answer::text("Paris"));
    let (status, body) = post(&gw, &generate(GPT), gemini_ask("capital of France?")).await;
    assert_eq!(status, 200, "{body}");
    let body = json(&body);
    assert_eq!(candidate_text(&body), "Paris");
    assert_eq!(body["candidates"][0]["finishReason"], "STOP");
    let seen = &fake.requests()[0];
    assert_eq!(seen.path, "/v1/chat/completions");
    assert_eq!(seen.header("authorization"), Some("Bearer sk-gw"));
    let sent = json(std::str::from_utf8(&seen.raw).unwrap());
    assert_eq!(sent["model"], GPT);
    assert_eq!(sent["messages"][0]["content"], "capital of France?");
    let logged = record(&gw, &generate(GPT), false).await;
    let (input, output) = tokens(&logged);
    assert_eq!(
        input,
        body["usageMetadata"]["promptTokenCount"].as_u64().unwrap()
    );
    assert_eq!(
        output,
        body["usageMetadata"]["candidatesTokenCount"]
            .as_u64()
            .unwrap()
    );
    assert!(input > 0 && output > 0, "{logged}");

    fake.answer(Answer::text("Paris, France"));
    let (status, body) = post(&gw, &stream_generate(GPT), gemini_ask("capital of France?")).await;
    assert_eq!(status, 200, "{body}");
    let events = data(&body);
    let text: String = events.iter().map(candidate_text_or_empty).collect();
    assert_eq!(text, "Paris, France");
    assert!(
        events
            .iter()
            .any(|e| e["candidates"][0]["finishReason"] == "STOP"),
        "{body}"
    );
    assert_eq!(
        json(std::str::from_utf8(&fake.requests()[1].raw).unwrap())["stream"],
        true
    );
    let logged = record(
        &gw,
        &format!("/v1beta/models/{GPT}:streamGenerateContent"),
        true,
    )
    .await;
    assert_eq!(logged["stream"], true);
    let (input, output) = tokens(&logged);
    assert!(input > 0 && output > 0, "{logged}");
}

// -- OpenAI Responses -----------------------------------------------------

const RESPONSES: &str = "/v1/responses";

fn responses_config(format: &str, provider: &str) -> String {
    format!(
        "providers = {{ p = {{ formats = \"{format}\", base_url = \"{provider}\", auth = {{ type = \"bearer\", key = \"sk-gw\" }} }} }}\nformats = [\"openai-responses\"]\nplugins = {{ routing = {{ default = \"p\" }} }}\n"
    )
}

/// The text of a Responses response's output messages.
fn output_text(body: &Value) -> String {
    body["output"]
        .as_array()
        .unwrap_or_else(|| panic!("no output in {body}"))
        .iter()
        .filter(|item| item["type"] == "message")
        .flat_map(|item| item["content"].as_array().unwrap().iter())
        .filter_map(|part| part["text"].as_str())
        .collect()
}

/// The text deltas of a Responses stream, and its final `response.completed`.
fn stream_text(body: &str) -> (String, Value) {
    let events = data(body);
    let text = events
        .iter()
        .filter(|e| e["type"] == "response.output_text.delta")
        .filter_map(|e| e["delta"].as_str())
        .collect();
    let completed = events
        .iter()
        .find(|e| e["type"] == "response.completed")
        .unwrap_or_else(|| panic!("no response.completed in {body}"))
        .clone();
    (text, completed)
}

#[tokio::test]
async fn a_responses_request_is_relayed_whole_and_streamed_to_an_openai_provider() {
    let fake = FakeOpenAi::start().await;
    let gw = Gateway::start(&responses_config("openai-responses", &fake.base_url())).await;

    fake.answer(Answer::text("hello there"));
    let asked = json!({ "model": GPT, "input": "hi", "metadata": { "kept": "yes" } });
    let (status, body) = post(&gw, RESPONSES, asked).await;
    assert_eq!(status, 200, "{body}");
    let body = json(&body);
    assert_eq!(output_text(&body), "hello there");
    let seen = &fake.requests()[0];
    assert_eq!(seen.path, RESPONSES);
    assert_eq!(seen.header("authorization"), Some("Bearer sk-gw"));
    assert_eq!(
        json(std::str::from_utf8(&seen.raw).unwrap())["metadata"]["kept"],
        "yes"
    );
    let logged = record(&gw, RESPONSES, false).await;
    assert_eq!(logged["format"], "openai-responses");
    let (input, output) = tokens(&logged);
    assert_eq!(input, body["usage"]["input_tokens"].as_u64().unwrap());
    assert_eq!(output, body["usage"]["output_tokens"].as_u64().unwrap());
    assert!(input > 0 && output > 0, "{logged}");

    fake.answer(Answer::text("hello there"));
    let (status, body) = post(
        &gw,
        RESPONSES,
        json!({ "model": GPT, "input": "hi", "stream": true }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let (text, completed) = stream_text(&body);
    assert_eq!(text, "hello there");
    assert_eq!(completed["response"]["status"], "completed");
    let streamed = record(&gw, RESPONSES, true).await;
    let (input, output) = tokens(&streamed);
    assert_eq!(
        output,
        completed["response"]["usage"]["output_tokens"]
            .as_u64()
            .unwrap()
    );
    assert!(input > 0 && output > 0, "{streamed}");
}

#[tokio::test]
async fn a_responses_request_is_translated_to_an_anthropic_provider() {
    let fake = FakeAnthropic::start().await;
    let config = format!(
        "providers = {{ a = {{ formats = \"anthropic-messages\", base_url = \"{}\", auth = {{ type = \"header\", name = \"x-api-key\", key = \"sk-ant-gw\" }} }} }}\nformats = [\"openai-responses\"]\nplugins = {{ routing = {{ default = \"a\" }} }}\n",
        fake.base_url()
    );
    let gw = Gateway::start(&config).await;

    fake.answer(Answer::text("hello there"));
    let asked = json!({ "model": CLAUDE, "input": "hi", "instructions": "be brief", "max_output_tokens": 64 });
    let (status, body) = post(&gw, RESPONSES, asked).await;
    assert_eq!(status, 200, "{body}");
    let body = json(&body);
    assert_eq!(output_text(&body), "hello there");
    assert_eq!(body["status"], "completed");
    let seen = &fake.requests()[0];
    assert_eq!(seen.path, "/v1/messages");
    assert_eq!(seen.header("x-api-key"), Some("sk-ant-gw"));
    let sent = json(std::str::from_utf8(&seen.raw).unwrap());
    assert_eq!(sent["model"], CLAUDE);
    assert_eq!(sent["system"], "be brief");
    assert_eq!(sent["max_tokens"], 64);
    let logged = record(&gw, RESPONSES, false).await;
    let (input, output) = tokens(&logged);
    assert_eq!(input, body["usage"]["input_tokens"].as_u64().unwrap());
    assert_eq!(output, body["usage"]["output_tokens"].as_u64().unwrap());
    assert!(input > 0 && output > 0, "{logged}");

    fake.answer(Answer::text("hello there"));
    let asked = json!({ "model": CLAUDE, "input": "hi", "stream": true, "max_output_tokens": 64 });
    let (status, body) = post(&gw, RESPONSES, asked).await;
    assert_eq!(status, 200, "{body}");
    let (text, completed) = stream_text(&body);
    assert_eq!(text, "hello there");
    assert_eq!(completed["response"]["status"], "completed");
    assert_eq!(
        json(std::str::from_utf8(&fake.requests()[1].raw).unwrap())["stream"],
        true
    );
    let streamed = record(&gw, RESPONSES, true).await;
    let (input, output) = tokens(&streamed);
    assert_eq!(
        output,
        completed["response"]["usage"]["output_tokens"]
            .as_u64()
            .unwrap()
    );
    assert!(input > 0 && output > 0, "{streamed}");
}
