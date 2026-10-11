//! Gemini's `generateContent` through the kernel: relayed to a strict fake
//! Gemini, and translated to and from OpenAI Chat and Anthropic Messages.

use respan_gateway_http::http;
use std::sync::Arc;

use http::{HeaderValue, StatusCode};
use mockllm::{Answer, FakeAnthropic, FakeGemini, FakeOpenAi, Request};
use respan_gateway_conversation::Input;
use respan_gateway_conversation_http::anthropic::AnthropicMessages;
use respan_gateway_conversation_http::gemini::GeminiGenerate;
use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_core::{ClientContext, Fault, Plugin, Response, async_trait};
#[allow(dead_code, unused_imports)]
#[path = "../../../../tests/support/http.rs"]
mod http_support;
use http_support::{post, read_whole, serve};

mod support;
use respan_gateway_http::HttpTransport;
use serde_json::{Value, json};
use support::{
    CHAT, Call, MESSAGES, SendRequest, add_provider, http_sender, providers, with_plugins,
};

const MODEL: &str = "/v1beta/models/gemini-x";

fn generate(body: Value) -> Call {
    post(&format!("{MODEL}:generateContent"), body.to_string())
}

fn stream(body: Value) -> Call {
    post(
        &format!("{MODEL}:streamGenerateContent?alt=sse"),
        body.to_string(),
    )
}

fn hi() -> Value {
    json!({ "contents": [{ "role": "user", "parts": [{ "text": "hi" }] }] })
}

/// Sends every request to the provider `.0`.
struct To(&'static str);

#[async_trait]
impl Plugin for To {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.set_target(self.0);
        Ok(None)
    }
}

struct Harness {
    gw: respan_gateway_core::Gateway,
    gemini: FakeGemini,
    openai: FakeOpenAi,
    anthropic: FakeAnthropic,
}

fn provider_http(base: &str, header: &'static str, key: &'static str) -> SendRequest {
    let mut headers = http::HeaderMap::new();
    headers.insert(header, HeaderValue::from_static(key));
    http_sender(
        HttpTransport::new(base)
            .and_then(|transport| transport.with_headers(headers))
            .unwrap_or_else(|fault| panic!("{fault}")),
    )
}

impl Harness {
    /// The three providers behind a gateway whose clients speak Gemini, OpenAI
    /// and Anthropic; every request goes to `target`.
    async fn to(target: &'static str) -> Self {
        let (gemini, openai, anthropic) = tokio::join!(
            FakeGemini::start(),
            FakeOpenAi::start(),
            FakeAnthropic::start()
        );
        let base = format!("{}/v1beta", gemini.base_url());
        let gw = providers([
            add_provider(
                "gemini",
                GeminiGenerate,
                provider_http(&base, "x-goog-api-key", "k"),
            ),
            add_provider(
                "openai",
                OpenAiChat,
                provider_http(&openai.base_url(), "authorization", "Bearer sk"),
            ),
            add_provider(
                "anthropic",
                AnthropicMessages,
                provider_http(&anthropic.base_url(), "x-api-key", "sk-ant"),
            ),
        ])
        .and_then(|gw| {
            gw.client::<Input, _, _>(GeminiGenerate)?
                .client::<Input, _, _>(OpenAiChat)?
                .client::<Input, _, _>(AnthropicMessages)
        })
        .map(|gw| with_plugins(gw, [Arc::new(To(target)) as Arc<dyn Plugin>]))
        .unwrap_or_else(|e| panic!("gateway: {e}"));
        Self {
            gw,
            gemini,
            openai,
            anthropic,
        }
    }

    async fn send(&self, req: Call) -> (StatusCode, http::HeaderMap, String) {
        let (status, headers, body) = read_whole(serve(&self.gw, req).await.0).await;
        self.gw.idle().await;
        let body = body.unwrap_or_else(|f| panic!("aborted: {f}"));
        (status, headers, String::from_utf8_lossy(&body).into_owned())
    }

    /// The JSON answer to `req`.
    async fn json(&self, req: Call) -> (StatusCode, Value) {
        let (status, _, body) = self.send(req).await;
        (
            status,
            serde_json::from_str(&body).unwrap_or(Value::String(body)),
        )
    }

    /// The data of each event of the stream answering `req`.
    async fn events(&self, req: Call) -> (StatusCode, Vec<Value>) {
        let (status, headers, body) = self.send(req).await;
        if status == StatusCode::OK {
            let kind = headers.get("content-type").and_then(|v| v.to_str().ok());
            assert!(
                kind.is_some_and(|k| k.starts_with("text/event-stream")),
                "{kind:?}"
            );
        }
        (status, data_of(&body))
    }
}

fn data_of(body: &str) -> Vec<Value> {
    let lines = body.lines().filter_map(|l| l.strip_prefix("data:"));
    lines
        .filter(|d| d.trim() != "[DONE]")
        .map(|d| serde_json::from_str(d.trim()).unwrap_or(Value::String(d.into())))
        .collect()
}

fn only(requests: Vec<Request>) -> Request {
    assert_eq!(requests.len(), 1, "{requests:?}");
    requests
        .into_iter()
        .next()
        .unwrap_or_else(|| unreachable!())
}

/// The text of Gemini responses, joined.
fn gemini_text(chunks: &[Value]) -> String {
    let parts = chunks
        .iter()
        .flat_map(|c| {
            c.pointer("/candidates/0/content/parts")
                .and_then(Value::as_array)
        })
        .flatten();
    parts.filter_map(|p| p["text"].as_str()).collect()
}

fn weather_tools() -> Value {
    json!([{ "functionDeclarations": [{
        "name": "weather", "description": "the weather",
        "parametersJsonSchema": { "type": "object", "properties": { "city": { "type": "string" } },
            "required": ["city"] },
    }] }])
}

// ---- Gemini to Gemini: a relay ----

#[tokio::test]
async fn a_gemini_client_relays_text_to_gemini() {
    let h = Harness::to("gemini").await;
    h.gemini.answer(Answer::text("Hello there friend"));
    let (status, v) = h.json(generate(hi())).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(
        v["candidates"][0]["content"]["parts"][0]["text"],
        "Hello there friend"
    );
    assert_eq!(v["candidates"][0]["content"]["role"], "model");
    assert_eq!(v["candidates"][0]["finishReason"], "STOP");
    assert_eq!(v["modelVersion"], "gemini-x");
    assert!(v["usageMetadata"]["promptTokenCount"].as_u64() > Some(0));
    let sent = only(h.gemini.requests());
    assert_eq!(sent.path, format!("{MODEL}:generateContent"));
    assert_eq!(sent.body, hi());
    assert_eq!(sent.header("x-goog-api-key"), Some("k"));
}

#[tokio::test]
async fn a_gemini_stream_relays_chunk_by_chunk_to_the_last_with_finish_and_usage() {
    let h = Harness::to("gemini").await;
    h.gemini.answer(Answer::text("one two three"));
    let (status, chunks) = h.events(stream(hi())).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(gemini_text(&chunks), "one two three");
    assert!(chunks.len() >= 3, "{chunks:?}");
    let last = chunks.last().unwrap_or(&Value::Null);
    assert_eq!(last["candidates"][0]["finishReason"], "STOP");
    assert!(last["usageMetadata"]["candidatesTokenCount"].as_u64() > Some(0));
    assert_eq!(
        chunks
            .iter()
            .filter(|c| c.get("usageMetadata").is_some())
            .count(),
        1
    );
    let sent = only(h.gemini.requests());
    assert_eq!(sent.path, format!("{MODEL}:streamGenerateContent"));
    assert_eq!(sent.query_param("alt"), Some("sse"));
    assert!(sent.body.get("stream").is_none());
}

#[tokio::test]
async fn a_rich_gemini_request_reaches_gemini_as_it_was_sent() {
    let h = Harness::to("gemini").await;
    let body = json!({
        "systemInstruction": { "parts": [{ "text": "be brief" }] },
        "contents": [
            { "role": "user", "parts": [{ "text": "weather in Paris?" }] },
            { "role": "model", "parts": [
                { "text": "checking" },
                { "functionCall": { "name": "weather", "args": { "city": "Paris" } },
                  "thoughtSignature": "sig" },
            ] },
            { "role": "user", "parts": [
                { "functionResponse": { "name": "weather", "response": { "temp": 21 } } },
                { "inlineData": { "mimeType": "image/png", "data": "aGk=" } },
            ] },
        ],
        "tools": weather_tools(),
        "toolConfig": { "functionCallingConfig": { "mode": "AUTO" } },
        "safetySettings": [{ "category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE" }],
        "generationConfig": { "temperature": 0.25, "topP": 0.5, "topK": 7, "maxOutputTokens": 99,
            "stopSequences": ["END"], "thinkingConfig": { "thinkingBudget": 0 },
            "responseMimeType": "application/json",
            "responseJsonSchema": { "type": "object", "properties": { "a": { "type": "integer" } } } },
    });
    h.gemini.answer(Answer::text("{}"));
    let (status, v) = h.json(generate(body.clone())).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(only(h.gemini.requests()).body, body);
}

#[tokio::test]
async fn a_tool_call_and_its_signature_relay_whole_and_streamed() {
    let h = Harness::to("gemini").await;
    let args = json!({ "city": "Paris" });
    for streamed in [false, true] {
        h.gemini.answer(Answer::tool_call("weather", args.clone()));
        let request = json!({ "contents": hi()["contents"], "tools": weather_tools() });
        let call = match streamed {
            true => h.events(stream(request)).await.1,
            false => vec![h.json(generate(request)).await.1],
        };
        let calls: Vec<&Value> = call
            .iter()
            .flat_map(|c| {
                c.pointer("/candidates/0/content/parts")
                    .and_then(Value::as_array)
            })
            .flatten()
            .filter(|p| p.get("functionCall").is_some())
            .collect();
        assert_eq!(calls.len(), 1, "{call:?}");
        assert_eq!(
            calls[0]["functionCall"],
            json!({ "name": "weather", "args": args })
        );
        let last = call.last().unwrap_or(&Value::Null);
        assert_eq!(last["candidates"][0]["finishReason"], "STOP");
    }
}

#[tokio::test]
async fn thoughts_and_their_tokens_relay() {
    let h = Harness::to("gemini").await;
    h.gemini
        .answer(Answer::text("answer").reasoning("let me think"));
    let mut body = hi();
    body["generationConfig"] = json!({ "thinkingConfig": { "includeThoughts": true } });
    // A model that thinks.
    let req = post(
        "/v1beta/models/gemini-2.5-flash:generateContent",
        body.to_string(),
    );
    let (status, v) = h.json(req).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    let parts = &v["candidates"][0]["content"]["parts"];
    assert_eq!(parts[0], json!({ "text": "let me think", "thought": true }));
    assert_eq!(parts[1], json!({ "text": "answer" }));
    assert_eq!(v["usageMetadata"]["thoughtsTokenCount"], 3);
}

#[tokio::test]
async fn whole_responses_with_every_field_relay_exactly() {
    let h = Harness::to("gemini").await;
    for reason in [
        "STOP",
        "MAX_TOKENS",
        "SAFETY",
        "RECITATION",
        "MALFORMED_FUNCTION_CALL",
    ] {
        let answer = json!({
            "candidates": [{
                "content": { "role": "model", "parts": [{ "text": "partial" }] },
                "finishReason": reason, "index": 0,
                "safetyRatings": [{ "category": "HARM_CATEGORY_HARASSMENT", "probability": "LOW" }],
                "citationMetadata": { "citationSources": [{ "uri": "https://x" }] },
            }],
            "usageMetadata": { "promptTokenCount": 7, "candidatesTokenCount": 2,
                "cachedContentTokenCount": 3, "thoughtsTokenCount": 4, "totalTokenCount": 13,
                "promptTokensDetails": [{ "modality": "TEXT", "tokenCount": 7 }] },
            "modelVersion": "gemini-x", "responseId": "abc",
        });
        h.gemini.answer(Answer::body(answer.clone()));
        let (status, v) = h.json(generate(hi())).await;
        assert_eq!(status, StatusCode::OK, "{reason}: {v}");
        assert_eq!(v, answer, "{reason}");
    }
}

#[tokio::test]
async fn a_blocked_prompt_relays_as_it_came() {
    let h = Harness::to("gemini").await;
    let answer = json!({ "promptFeedback": { "blockReason": "SAFETY",
        "safetyRatings": [{ "category": "X", "probability": "HIGH" }] },
        "usageMetadata": { "promptTokenCount": 3, "totalTokenCount": 3 } });
    h.gemini.answer(Answer::body(answer.clone()));
    let (status, v) = h.json(generate(hi())).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(v, answer);
}

#[tokio::test]
async fn a_provider_error_reaches_a_gemini_client_in_googles_shape() {
    let h = Harness::to("gemini").await;
    h.gemini.answer(Answer::error(429).message("quota gone"));
    let (status, v) = h.json(generate(hi())).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(v["error"]["code"], 429);
    assert_eq!(v["error"]["status"], "RESOURCE_EXHAUSTED");
    assert_eq!(v["error"]["message"], "quota gone");
}

#[tokio::test]
async fn a_stream_cut_short_ends_in_an_error_not_a_finish() {
    let h = Harness::to("gemini").await;
    h.gemini
        .answer(Answer::text("one two three four").cut_after(2));
    let (_, chunks) = h.events(stream(hi())).await;
    assert!(
        chunks
            .iter()
            .all(|c| c.pointer("/candidates/0/finishReason").is_none()),
        "{chunks:?}"
    );
    assert!(
        chunks.last().is_some_and(|c| c.get("error").is_some()),
        "{chunks:?}"
    );
}

#[tokio::test]
async fn a_request_the_provider_refuses_is_the_clients_error() {
    let h = Harness::to("gemini").await;
    // The fake is strict: a field of no part of the API is refused.
    let mut body = hi();
    body["bogus"] = json!(1);
    let (status, v) = h.json(generate(body)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{v}");
    assert_eq!(v["error"]["status"], "INVALID_ARGUMENT");
    assert_eq!(v["error"]["code"], 400);
}

#[tokio::test]
async fn a_stream_without_sse_is_refused_before_a_call() {
    let h = Harness::to("gemini").await;
    let req = post(&format!("{MODEL}:streamGenerateContent"), hi().to_string());
    let (status, v) = h.json(req).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{v}");
    assert_eq!(v["error"]["status"], "INVALID_ARGUMENT");
    assert!(h.gemini.requests().is_empty());
}

// ---- Gemini client to other providers ----

#[tokio::test]
async fn a_gemini_client_reaches_openai_whole_and_streamed() {
    let h = Harness::to("openai").await;
    let body = json!({
        "systemInstruction": { "parts": [{ "text": "be brief" }] },
        "contents": hi()["contents"],
        "generationConfig": { "temperature": 0.5, "maxOutputTokens": 33, "stopSequences": ["END"] },
    });
    h.openai.answer(Answer::text("Hello there"));
    let (status, v) = h.json(generate(body.clone())).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(
        v["candidates"][0]["content"]["parts"][0]["text"],
        "Hello there"
    );
    assert_eq!(v["candidates"][0]["finishReason"], "STOP");
    assert!(v["usageMetadata"]["promptTokenCount"].as_u64() > Some(0));
    let sent = only(h.openai.requests());
    assert_eq!(sent.body["model"], "gemini-x");
    assert_eq!(
        sent.body["messages"],
        json!([
            { "role": "system", "content": "be brief" },
            { "role": "user", "content": "hi" },
        ])
    );
    assert_eq!(sent.body["temperature"], 0.5);
    assert_eq!(sent.body["stop"], json!(["END"]));
    assert!(sent.body["max_completion_tokens"] == 33 || sent.body["max_tokens"] == 33);
    assert!(sent.body["stream"].is_null());

    h.openai.answer(Answer::text("one two three"));
    let (status, chunks) = h.events(stream(body)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(gemini_text(&chunks), "one two three");
    let last = chunks.last().unwrap_or(&Value::Null);
    assert_eq!(last["candidates"][0]["finishReason"], "STOP");
    assert!(last["usageMetadata"]["promptTokenCount"].as_u64() > Some(0));
    assert_eq!(h.openai.requests()[1].body["stream"], true);
}

#[tokio::test]
async fn a_gemini_client_reaches_anthropic_whole_and_streamed() {
    let h = Harness::to("anthropic").await;
    let body = json!({
        "systemInstruction": { "parts": [{ "text": "be brief" }] },
        "contents": hi()["contents"],
        "generationConfig": { "maxOutputTokens": 33 },
    });
    h.anthropic.answer(Answer::text("Hello there"));
    let (status, v) = h.json(generate(body.clone())).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(
        v["candidates"][0]["content"]["parts"][0]["text"],
        "Hello there"
    );
    assert_eq!(v["candidates"][0]["finishReason"], "STOP");
    let sent = only(h.anthropic.requests());
    assert_eq!(sent.body["system"], "be brief");
    assert_eq!(sent.body["max_tokens"], 33);
    assert_eq!(sent.body["model"], "gemini-x");

    h.anthropic.answer(Answer::text("one two three"));
    let (_, chunks) = h.events(stream(body)).await;
    assert_eq!(gemini_text(&chunks), "one two three");
    let last = chunks.last().unwrap_or(&Value::Null);
    assert_eq!(last["candidates"][0]["finishReason"], "STOP");
    assert!(last["usageMetadata"]["candidatesTokenCount"].as_u64() > Some(0));
}

#[tokio::test]
async fn a_function_call_round_trips_through_openai_and_anthropic() {
    for target in ["openai", "anthropic"] {
        let h = Harness::to(target).await;
        let first = json!({
            "contents": hi()["contents"],
            "tools": [{ "functionDeclarations": [{ "name": "weather", "description": "the weather",
                "parameters": { "type": "OBJECT", "properties": { "city": { "type": "STRING" } },
                    "required": ["city"] } }] }],
            "toolConfig": { "functionCallingConfig": { "mode": "ANY", "allowedFunctionNames": ["weather"] } },
        });
        let answer = || Answer::tool_call("weather", json!({ "city": "Paris" }));
        match target {
            "openai" => h.openai.answer(answer()),
            _ => h.anthropic.answer(answer()),
        }
        let (status, v) = h.json(generate(first.clone())).await;
        assert_eq!(status, StatusCode::OK, "{target}: {v}");
        let call = &v["candidates"][0]["content"]["parts"][0]["functionCall"];
        assert_eq!(call["name"], "weather", "{target}");
        assert_eq!(call["args"], json!({ "city": "Paris" }), "{target}");
        assert_eq!(v["candidates"][0]["finishReason"], "STOP");
        let sent = match target {
            "openai" => only(h.openai.requests()).body,
            _ => only(h.anthropic.requests()).body,
        };
        let schema = json!({ "type": "object", "properties": { "city": { "type": "string" } },
            "required": ["city"] });
        match target {
            "openai" => {
                assert_eq!(sent["tools"][0]["function"]["parameters"], schema);
                assert_eq!(sent["tool_choice"]["function"]["name"], "weather");
            }
            _ => {
                assert_eq!(sent["tools"][0]["input_schema"], schema);
                assert_eq!(
                    sent["tool_choice"],
                    json!({ "type": "tool", "name": "weather" })
                );
            }
        }

        // The conversation goes on with the result.
        let history = json!({
            "contents": [
                hi()["contents"][0].clone(),
                { "role": "model", "parts": [{ "functionCall": call }] },
                { "role": "user", "parts": [{ "functionResponse": {
                    "name": "weather", "response": { "output": "sunny" } } }] },
            ],
        });
        match target {
            "openai" => h.openai.answer(Answer::text("It is sunny")),
            _ => h.anthropic.answer(Answer::text("It is sunny")),
        }
        let (status, v) = h.json(generate(history)).await;
        assert_eq!(status, StatusCode::OK, "{target}: {v}");
        assert_eq!(
            v["candidates"][0]["content"]["parts"][0]["text"],
            "It is sunny"
        );
        let requests = match target {
            "openai" => h.openai.requests(),
            _ => h.anthropic.requests(),
        };
        let messages = &requests[1].body["messages"];
        let (call_id, result_id) = match target {
            "openai" => (
                messages[1]["tool_calls"][0]["id"].clone(),
                messages[2]["tool_call_id"].clone(),
            ),
            _ => (
                messages[1]["content"][0]["id"].clone(),
                messages[2]["content"][0]["tool_use_id"].clone(),
            ),
        };
        assert!(
            call_id.is_string() && call_id == result_id,
            "{target}: {messages}"
        );
    }
}

#[tokio::test]
async fn finish_reasons_and_usage_cross_from_the_other_formats() {
    let h = Harness::to("anthropic").await;
    let answer = json!({ "id": "m", "type": "message", "role": "assistant", "model": "c",
        "content": [{ "type": "text", "text": "cut off" }], "stop_reason": "max_tokens",
        "stop_sequence": null,
        "usage": { "input_tokens": 5, "output_tokens": 7, "cache_read_input_tokens": 2 } });
    h.anthropic.answer(Answer::body(answer));
    let (status, v) = h.json(generate(hi())).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(v["candidates"][0]["finishReason"], "MAX_TOKENS");
    // Anthropic's input excludes the cache; Gemini's includes it.
    assert_eq!(v["usageMetadata"]["promptTokenCount"], 7);
    assert_eq!(v["usageMetadata"]["cachedContentTokenCount"], 2);
    assert_eq!(v["usageMetadata"]["candidatesTokenCount"], 7);
    assert_eq!(v["usageMetadata"]["totalTokenCount"], 14);

    let h = Harness::to("openai").await;
    let answer = json!({ "id": "c", "object": "chat.completion", "model": "g", "choices": [{
        "index": 0, "finish_reason": "content_filter", "message": { "role": "assistant", "content": "" } }],
        "usage": { "prompt_tokens": 5, "completion_tokens": 1, "total_tokens": 6 } });
    h.openai.answer(Answer::body(answer));
    let (_, v) = h.json(generate(hi())).await;
    assert_eq!(v["candidates"][0]["finishReason"], "SAFETY");
}

#[tokio::test]
async fn an_error_of_another_format_reaches_a_gemini_client_in_googles_shape() {
    for target in ["openai", "anthropic"] {
        let h = Harness::to(target).await;
        let answer = Answer::error(401).message("bad key");
        match target {
            "openai" => h.openai.answer(answer),
            _ => h.anthropic.answer(answer),
        }
        let (status, v) = h.json(generate(hi())).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{target}: {v}");
        assert_eq!(v["error"]["code"], 401, "{target}");
        assert_eq!(v["error"]["status"], "UNAUTHENTICATED", "{target}");
        assert_eq!(v["error"]["message"], "bad key", "{target}");
    }
}

#[tokio::test]
async fn what_another_format_has_no_form_for_is_refused_not_dropped() {
    let h = Harness::to("openai").await;
    let refused = [
        json!({ "safetySettings": [{ "category": "X", "threshold": "BLOCK_NONE" }] }),
        json!({ "generationConfig": { "thinkingConfig": { "thinkingBudget": 1 } } }),
        json!({ "generationConfig": { "topK": 3 } }),
        json!({ "tools": [{ "googleSearch": {} }] }),
        json!({ "cachedContent": "cachedContents/x" }),
        json!({ "toolConfig": { "functionCallingConfig": { "mode": "VALIDATED" } } }),
    ];
    for extra in refused {
        let mut body = hi();
        body.as_object_mut()
            .into_iter()
            .for_each(|b| b.extend(extra.as_object().cloned().unwrap_or_default()));
        let (status, v) = h.json(generate(body)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{extra}: {v}");
        assert_eq!(v["error"]["status"], "INVALID_ARGUMENT");
    }
    let video = json!({ "contents": [{ "role": "user", "parts": [
        { "fileData": { "mimeType": "video/mp4", "fileUri": "gs://v" } }] }] });
    assert_eq!(h.json(generate(video)).await.0, StatusCode::BAD_REQUEST);
    assert!(h.openai.requests().is_empty(), "nothing was sent");
}

#[tokio::test]
async fn a_thought_signature_is_dropped_not_refused_by_other_formats() {
    let h = Harness::to("openai").await;
    let body = json!({ "contents": [
        hi()["contents"][0].clone(),
        { "role": "model", "parts": [{ "functionCall": { "name": "weather", "args": {} },
            "thoughtSignature": "sig" }] },
        { "role": "user", "parts": [{ "functionResponse": { "name": "weather", "response": { "t": 1 } } }] },
    ] });
    h.openai.answer(Answer::text("ok"));
    let (status, v) = h.json(generate(body)).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(
        h.openai.requests()[0].body["messages"][2]["content"],
        r#"{"t":1}"#
    );
}

// ---- Other clients to Gemini ----

fn chat(body: Value) -> Call {
    post(CHAT, body.to_string())
}

#[tokio::test]
async fn an_openai_client_reaches_gemini_whole_and_streamed() {
    let h = Harness::to("gemini").await;
    let body = json!({
        "model": "gemini-x", "temperature": 0.5, "max_tokens": 40, "stop": ["END"],
        "messages": [
            { "role": "system", "content": "be brief" },
            { "role": "user", "content": "hi" },
        ],
    });
    h.gemini.answer(Answer::text("Hello there"));
    let (status, v) = h.json(chat(body.clone())).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(v["choices"][0]["message"]["content"], "Hello there");
    assert_eq!(v["choices"][0]["finish_reason"], "stop");
    assert_eq!(v["model"], "gemini-x");
    assert!(v["usage"]["prompt_tokens"].as_u64() > Some(0));
    let sent = only(h.gemini.requests());
    assert_eq!(sent.path, format!("{MODEL}:generateContent"));
    assert_eq!(
        sent.body,
        json!({
            "systemInstruction": { "parts": [{ "text": "be brief" }] },
            "contents": [{ "role": "user", "parts": [{ "text": "hi" }] }],
            "generationConfig": { "maxOutputTokens": 40, "temperature": 0.5, "stopSequences": ["END"] },
        })
    );

    h.gemini.answer(Answer::text("one two three"));
    let mut streaming = body;
    streaming["stream"] = true.into();
    streaming["stream_options"] = json!({ "include_usage": true });
    let (_, _, body) = h.send(chat(streaming)).await;
    let chunks = data_of(&body);
    let text: String = chunks
        .iter()
        .filter_map(|c| {
            c.pointer("/choices/0/delta/content")
                .and_then(Value::as_str)
        })
        .collect();
    assert_eq!(text, "one two three");
    assert!(
        chunks
            .iter()
            .any(|c| c.pointer("/choices/0/finish_reason") == Some(&json!("stop")))
    );
    assert!(
        chunks
            .iter()
            .any(|c| c["usage"]["prompt_tokens"].as_u64() > Some(0))
    );
    assert!(body.contains("[DONE]"));
    let second = &h.gemini.requests()[1];
    assert_eq!(second.path, format!("{MODEL}:streamGenerateContent"));
    assert_eq!(second.query_param("alt"), Some("sse"));
}

#[tokio::test]
async fn an_openai_tool_conversation_reaches_gemini_and_back() {
    let h = Harness::to("gemini").await;
    let tool = json!({ "type": "function", "function": { "name": "weather", "description": "the weather",
        "parameters": { "type": "object", "properties": { "city": { "type": "string" } } } } });
    h.gemini
        .answer(Answer::tool_call("weather", json!({ "city": "Paris" })));
    let first = json!({ "model": "gemini-x", "messages": [{ "role": "user", "content": "weather?" }],
        "tools": [tool], "tool_choice": "required" });
    let (status, v) = h.json(chat(first)).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    let call = &v["choices"][0]["message"]["tool_calls"][0];
    assert_eq!(call["function"]["name"], "weather");
    assert_eq!(call["function"]["arguments"], r#"{"city":"Paris"}"#);
    assert_eq!(v["choices"][0]["finish_reason"], "tool_calls");
    let sent = only(h.gemini.requests());
    assert_eq!(
        sent.body["tools"][0]["functionDeclarations"][0]["parametersJsonSchema"],
        tool["function"]["parameters"]
    );
    assert_eq!(
        sent.body["toolConfig"],
        json!({ "functionCallingConfig": { "mode": "ANY" } })
    );

    // The result is a function response that names its tool.
    h.gemini.answer(Answer::text("sunny"));
    let history = json!({ "model": "gemini-x", "messages": [
        { "role": "user", "content": "weather?" },
        { "role": "assistant", "content": null, "tool_calls": [call] },
        { "role": "tool", "tool_call_id": call["id"], "content": "21 degrees" },
    ] });
    let (status, v) = h.json(chat(history)).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    let contents = &h.gemini.requests()[1].body["contents"];
    assert_eq!(contents[1]["parts"][0]["functionCall"]["name"], "weather");
    assert_eq!(contents[2]["role"], "user");
    assert_eq!(
        contents[2]["parts"][0]["functionResponse"]["name"],
        "weather"
    );
    assert_eq!(
        contents[2]["parts"][0]["functionResponse"]["response"],
        json!({ "output": "21 degrees" })
    );
}

#[tokio::test]
async fn an_anthropic_client_reaches_gemini() {
    let h = Harness::to("gemini").await;
    h.gemini.answer(Answer::text("Hello there"));
    let body = json!({ "model": "gemini-x", "max_tokens": 50, "system": "be brief",
        "messages": [{ "role": "user", "content": "hi" }] });
    let (status, v) = h.json(post(MESSAGES, body.to_string())).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(v["content"][0]["text"], "Hello there");
    assert_eq!(v["stop_reason"], "end_turn");
    assert!(v["usage"]["input_tokens"].as_u64() > Some(0));
    let sent = only(h.gemini.requests());
    assert_eq!(
        sent.body["systemInstruction"],
        json!({ "parts": [{ "text": "be brief" }] })
    );
    assert_eq!(
        sent.body["generationConfig"],
        json!({ "maxOutputTokens": 50 })
    );
}

#[tokio::test]
async fn gemini_finish_reasons_reach_an_openai_client() {
    let h = Harness::to("gemini").await;
    for (reason, expected) in [
        ("MAX_TOKENS", "length"),
        ("SAFETY", "content_filter"),
        ("RECITATION", "content_filter"),
    ] {
        h.gemini.answer(Answer::body(json!({
            "candidates": [{ "content": { "role": "model", "parts": [{ "text": "x" }] },
                "finishReason": reason, "index": 0 }],
            "usageMetadata": { "promptTokenCount": 3, "candidatesTokenCount": 1, "totalTokenCount": 4 },
            "modelVersion": "gemini-x" })));
        let body =
            json!({ "model": "gemini-x", "messages": [{ "role": "user", "content": "hi" }] });
        let (status, v) = h.json(chat(body)).await;
        assert_eq!(status, StatusCode::OK, "{reason}: {v}");
        assert_eq!(v["choices"][0]["finish_reason"], expected, "{reason}");
    }
    // A reason of Gemini's alone has no form in another format.
    h.gemini.answer(Answer::body(json!({
        "candidates": [{ "finishReason": "MALFORMED_FUNCTION_CALL", "index": 0 }],
        "modelVersion": "gemini-x" })));
    let body = json!({ "model": "gemini-x", "messages": [{ "role": "user", "content": "hi" }] });
    let (status, _) = h.json(chat(body)).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
}

#[tokio::test]
async fn what_gemini_cannot_say_is_refused_for_an_openai_client() {
    let h = Harness::to("gemini").await;
    for extra in [
        json!({ "user": "u1" }),
        json!({ "parallel_tool_calls": false, "tools": [{
        "type": "function", "function": { "name": "f", "parameters": { "type": "object" } } }] }),
    ] {
        let mut body =
            json!({ "model": "gemini-x", "messages": [{ "role": "user", "content": "hi" }] });
        body.as_object_mut()
            .into_iter()
            .for_each(|b| b.extend(extra.as_object().cloned().unwrap_or_default()));
        let (status, v) = h.json(chat(body)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{extra}: {v}");
    }
    assert!(h.gemini.requests().is_empty());
}

#[tokio::test]
async fn a_gemini_error_reaches_an_openai_client() {
    let h = Harness::to("gemini").await;
    h.gemini.answer(Answer::error(503).message("overloaded"));
    let body = json!({ "model": "gemini-x", "messages": [{ "role": "user", "content": "hi" }] });
    let (status, v) = h.json(chat(body)).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{v}");
    assert_eq!(v["error"]["message"], "overloaded");
}
