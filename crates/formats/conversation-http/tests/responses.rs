//! OpenAI Responses over HTTP: relayed to a provider that speaks it, and
//! translated to and from the other conversation formats, against the strict
//! fakes of OpenAI and Anthropic.

use respan_gateway_core::futures;
use respan_gateway_http::bytes;
use respan_gateway_http::http;
use std::sync::{Arc, Mutex};

use http::{HeaderValue, StatusCode};
use mockllm::{Answer, FakeAnthropic, FakeOpenAi};
use respan_gateway_conversation::{Input, Usage};
use respan_gateway_conversation_http::anthropic::AnthropicMessages;
use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_conversation_http::responses::OpenAiResponses;
use respan_gateway_core::async_trait;
use respan_gateway_core::{ClientContext, Fault, Plugin, Response};
#[allow(dead_code, unused_imports)]
#[path = "../../../../tests/support/http.rs"]
mod http_support;
use http_support::{post, read_whole, serve};

mod support;
use respan_gateway_http::Body;
use respan_gateway_http_transport::HttpTransport;
use serde_json::{Value, json};
use support::{
    Call, Metered, SendRequest, add_provider, http_sender, providers, sender, with_plugins,
};

const RESPONSES: &str = "/v1/responses";
const CHAT: &str = "/v1/chat/completions";
const MESSAGES: &str = "/v1/messages";

fn call(path: &str, body: Value) -> Call {
    post(path, body.to_string())
}

/// Sends every request to the provider `.0`.
struct Target(&'static str);

#[async_trait]
impl Plugin for Target {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.set_target(self.0);
        Ok(None)
    }
}

/// A gateway serving the three conversation formats, with a provider of each
/// (`responses` and `chat` both on the OpenAI fake, `claude` on the Anthropic
/// one), all requests going to `target`.
struct Harness {
    gw: respan_gateway_core::Gateway,
    openai: FakeOpenAi,
    anthropic: FakeAnthropic,
    usage: Metered,
}

/// Scripted replies in place of a fake's, per provider.
#[derive(Default)]
struct Local {
    responses: Option<SendRequest>,
    chat: Option<SendRequest>,
    claude: Option<SendRequest>,
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
    async fn new(target: &'static str) -> Self {
        Self::with(target, Local::default()).await
    }

    async fn with(target: &'static str, local: Local) -> Self {
        let (openai, anthropic) = tokio::join!(FakeOpenAi::start(), FakeAnthropic::start());
        let bearer = || provider_http(&openai.base_url(), "authorization", "Bearer sk-test");
        let usage = Metered::default();
        let plugins: Vec<Arc<dyn Plugin>> = vec![Arc::new(Target(target)), Arc::new(usage.clone())];
        let gw = providers([
            add_provider(
                "responses",
                OpenAiResponses,
                local.responses.unwrap_or_else(bearer),
            ),
            add_provider("chat", OpenAiChat, local.chat.unwrap_or_else(bearer)),
            add_provider(
                "claude",
                AnthropicMessages,
                local.claude.unwrap_or_else(|| {
                    provider_http(&anthropic.base_url(), "x-api-key", "sk-ant-test")
                }),
            ),
        ])
        .and_then(|gw| {
            gw.client::<Input, _, _>(OpenAiResponses)?
                .client::<Input, _, _>(OpenAiChat)?
                .client::<Input, _, _>(AnthropicMessages)
        })
        .map(|gw| with_plugins(gw, plugins))
        .unwrap_or_else(|e| panic!("gateway: {e}"));
        Self {
            gw,
            openai,
            anthropic,
            usage,
        }
    }

    async fn send(&self, req: Call) -> (StatusCode, http::HeaderMap, String) {
        let (status, headers, body) = read_whole(serve(&self.gw, req).await.0).await;
        self.gw.idle().await;
        let body = body.unwrap_or_else(|f| panic!("aborted: {f}"));
        (status, headers, String::from_utf8_lossy(&body).into_owned())
    }

    /// The JSON answer to `req`, which must be a success.
    async fn json(&self, req: Call) -> Value {
        let (status, _, body) = self.send(req).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        serde_json::from_str(&body).unwrap_or_else(|e| panic!("{e}: {body}"))
    }

    /// The events of the stream answering `req`.
    async fn stream(&self, req: Call) -> Vec<Value> {
        let (status, headers, body) = self.send(req).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let kind = headers.get("content-type").and_then(|v| v.to_str().ok());
        assert_eq!(kind, Some("text/event-stream"), "{body}");
        sse_events(&body)
    }

    /// Requests the OpenAI fake received.
    fn openai_sent(&self) -> Vec<Value> {
        self.openai.requests().into_iter().map(|r| r.body).collect()
    }

    fn claude_sent(&self) -> Vec<Value> {
        self.anthropic
            .requests()
            .into_iter()
            .map(|r| r.body)
            .collect()
    }
}

/// The data of each event of an SSE body, checking each names its type.
fn sse_events(body: &str) -> Vec<Value> {
    body.split("\n\n")
        .filter(|e| !e.trim().is_empty())
        .map(|e| {
            let field = |name: &str| e.lines().find_map(|l| l.strip_prefix(name)).map(str::trim);
            let data: Value = serde_json::from_str(field("data:").unwrap_or("null"))
                .unwrap_or_else(|x| panic!("{x}: {e}"));
            if let Some(name) = field("event:") {
                assert_eq!(data["type"], name, "{e}");
            }
            data
        })
        .collect()
}

fn types(events: &[Value]) -> Vec<&str> {
    events
        .iter()
        .map(|e| e["type"].as_str().unwrap_or(""))
        .collect()
}

/// `types`, each run of one type once.
fn runs(events: &[Value]) -> Vec<&str> {
    let mut all = types(events);
    all.dedup();
    all
}

/// The text the deltas of a Responses stream add.
fn streamed_text(events: &[Value]) -> String {
    let deltas = events
        .iter()
        .filter(|e| e["type"] == "response.output_text.delta");
    deltas.filter_map(|e| e["delta"].as_str()).collect()
}

/// Asserts a Responses stream is well formed: numbered from 0, its response
/// created first and ended last, and each item added before it is done.
fn assert_well_formed(events: &[Value]) {
    for (i, e) in events.iter().enumerate() {
        assert_eq!(e["sequence_number"], i, "{e}");
    }
    assert_eq!(events[0]["type"], "response.created");
    assert_eq!(events[0]["response"]["status"], "in_progress");
    let last = &events[events.len() - 1];
    assert!(
        ["response.completed", "response.incomplete"]
            .contains(&last["type"].as_str().unwrap_or("")),
        "{last}"
    );
    let mut open: Vec<u64> = Vec::new();
    for e in events {
        let index = e["output_index"].as_u64();
        match (e["type"].as_str(), index) {
            (Some("response.output_item.added"), Some(i)) => {
                assert!(
                    !open.contains(&i) && open.is_empty(),
                    "one item at a time: {e}"
                );
                open.push(i);
            }
            (Some("response.output_item.done"), Some(i)) => {
                assert_eq!(open.pop(), Some(i), "{e}");
            }
            (Some(t), Some(i)) if t.starts_with("response.") => {
                assert_eq!(open.last(), Some(&i), "{e}");
            }
            _ => {}
        }
    }
    assert!(open.is_empty());
}

fn local_json(status: StatusCode, body: Value) -> SendRequest {
    local(status, "application/json", vec![body.to_string()])
}

/// A stream of `events`, each named by its type as Responses names them, or
/// as `named` says.
fn local_events(events: Vec<(String, Value)>) -> SendRequest {
    let frames = events
        .into_iter()
        .map(|(name, data)| format!("event: {name}\ndata: {data}\n\n"))
        .collect();
    local(StatusCode::OK, "text/event-stream", frames)
}

fn responses_events(events: Vec<Value>) -> SendRequest {
    local_events(
        events
            .into_iter()
            .map(|e| (e["type"].as_str().unwrap_or("").to_owned(), e))
            .collect(),
    )
}

fn local(status: StatusCode, kind: &'static str, chunks: Vec<String>) -> SendRequest {
    let reply = Arc::new(Mutex::new(Some((status, kind, chunks))));
    sender(move |_call| {
        let reply = reply.clone();
        async move {
            let (status, kind, chunks) = reply
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take()
                .expect("one response per provider call");
            let chunks: Vec<Result<bytes::Bytes, Fault>> = chunks
                .into_iter()
                .map(|c| Ok(bytes::Bytes::from(c)))
                .collect();
            let response = http::Response::builder()
                .status(status)
                .header("content-type", kind)
                .body(Body::stream(futures::stream::iter(chunks)))
                .unwrap_or_else(|e| panic!("{e}"));
            Ok((response, None))
        }
    })
}

fn weather_tool() -> Value {
    json!({ "type": "function", "name": "get_weather", "description": "weather",
            "parameters": { "type": "object", "properties": { "city": { "type": "string" } } } })
}

// ============================================================ same format

#[tokio::test]
async fn a_request_is_relayed_as_it_was_and_a_response_comes_back_whole() {
    let h = Harness::new("responses").await;
    let request = json!({
        "model": "gpt-4o-mini",
        "instructions": "be brief",
        "input": [{ "role": "user", "content": "hi" }],
        "max_output_tokens": 20,
        "temperature": 0.5,
        "top_p": 0.9,
        "tools": [weather_tool()],
        "tool_choice": "auto",
        "parallel_tool_calls": true,
        "text": { "format": { "type": "json_schema", "name": "n", "schema": { "type": "object" }, "strict": true } },
    });
    h.openai.answer(Answer::text("Hi there"));
    let body = h.json(call(RESPONSES, request.clone())).await;
    assert_eq!(h.openai_sent(), [request]);
    assert_eq!(body["object"], "response");
    assert_eq!(body["status"], "completed");
    assert_eq!(body["model"], "gpt-4o-mini");
    assert!(
        body["id"]
            .as_str()
            .is_some_and(|id| id.starts_with("resp_fake"))
    );
    assert_eq!(body["output"][0]["type"], "message");
    assert!(
        body["output"][0]["id"]
            .as_str()
            .is_some_and(|id| id.starts_with("msg_fake"))
    );
    assert_eq!(
        body["output"][0]["content"],
        json!([{ "type": "output_text", "text": "Hi there", "annotations": [] }])
    );
    assert_eq!(body["usage"]["output_tokens"], 2);
    assert_eq!(
        body["usage"]["total_tokens"].as_u64(),
        body["usage"]["input_tokens"].as_u64().map(|i| i + 2)
    );
    // The provider's own fields come back.
    assert_eq!(body["store"], true);
    assert_eq!(body["temperature"], 0.5);
    assert_eq!(h.usage.total().output_tokens, 2);
}

#[tokio::test]
async fn a_stream_is_relayed_event_for_event() {
    let h = Harness::new("responses").await;
    h.openai.answer(Answer::text("Hi there you"));
    let request = json!({ "model": "gpt-4o-mini", "stream": true, "input": "say hi" });
    let events = h.stream(call(RESPONSES, request)).await;
    assert_well_formed(&events);
    assert_eq!(
        runs(&events),
        [
            "response.created",
            "response.in_progress",
            "response.output_item.added",
            "response.content_part.added",
            "response.output_text.delta",
            "response.output_text.done",
            "response.content_part.done",
            "response.output_item.done",
            "response.completed",
        ]
    );
    assert_eq!(streamed_text(&events), "Hi there you");
    let done = &events[events.len() - 1]["response"];
    assert_eq!(done["output"][0]["content"][0]["text"], "Hi there you");
    assert_eq!(done["usage"]["output_tokens"], 3);
    // The ids are the provider's.
    assert!(
        events[2]["item"]["id"]
            .as_str()
            .is_some_and(|id| id.starts_with("msg_fake"))
    );
    assert_eq!(events[2]["item"]["id"], events[4]["item_id"]);
    assert!(
        events[0]["response"]["id"]
            .as_str()
            .is_some_and(|id| id.starts_with("resp_fake"))
    );
    // The provider was asked for a stream, and its request was the client's.
    let sent = h.openai_sent();
    assert_eq!(sent[0]["stream"], true);
    assert_eq!(
        sent[0]["input"],
        json!([{ "role": "user", "content": "say hi" }])
    );
}

#[tokio::test]
async fn a_function_call_and_its_output_go_round_in_either_framing() {
    for stream in [false, true] {
        let h = Harness::new("responses").await;
        h.openai
            .answer(Answer::tool_call("get_weather", json!({ "city": "Paris" })));
        let first = json!({
            "model": "gpt-4o-mini", "stream": stream,
            "input": "weather in Paris?", "tools": [weather_tool()],
        });
        let (item, finish) = if stream {
            let events = h.stream(call(RESPONSES, first)).await;
            assert_well_formed(&events);
            let kinds = runs(&events);
            assert!(
                kinds.contains(&"response.function_call_arguments.delta"),
                "{kinds:?}"
            );
            assert!(
                kinds.contains(&"response.function_call_arguments.done"),
                "{kinds:?}"
            );
            let done = &events[events.len() - 1];
            (done["response"]["output"][0].clone(), done["type"].clone())
        } else {
            let body = h.json(call(RESPONSES, first)).await;
            (body["output"][0].clone(), json!("response.completed"))
        };
        assert_eq!(finish, "response.completed");
        assert_eq!(item["type"], "function_call");
        assert_eq!(item["name"], "get_weather");
        assert_eq!(item["arguments"], json!({ "city": "Paris" }).to_string());
        let call_id = item["call_id"].clone();
        assert!(
            item["id"]
                .as_str()
                .is_some_and(|id| id.starts_with("fc_fake"))
        );
        // The turn after: the fake pairs outputs with calls by `call_id`.
        let second = json!({
            "model": "gpt-4o-mini",
            "input": [
                { "role": "user", "content": "weather in Paris?" },
                item,
                { "type": "function_call_output", "call_id": call_id, "output": "sunny" },
            ],
        });
        let body = h.json(call(RESPONSES, second.clone())).await;
        assert_eq!(body["output"][0]["content"][0]["text"], "Hi there");
        assert_eq!(h.openai_sent()[1]["input"], second["input"]);
    }
}

#[tokio::test]
async fn reasoning_with_its_summary_and_encrypted_content_is_relayed() {
    for stream in [false, true] {
        let h = Harness::new("responses").await;
        h.openai
            .answer(Answer::text("Hi").reasoning("one two three"));
        let request = json!({
            "model": "o3", "stream": stream, "input": "hi",
            "reasoning": { "effort": "low", "summary": "auto" },
            "include": ["reasoning.encrypted_content"],
        });
        let output = if stream {
            let events = h.stream(call(RESPONSES, request.clone())).await;
            assert_well_formed(&events);
            let kinds = runs(&events);
            assert!(
                kinds.contains(&"response.reasoning_summary_text.delta"),
                "{kinds:?}"
            );
            events[events.len() - 1]["response"]["output"].clone()
        } else {
            h.json(call(RESPONSES, request.clone())).await["output"].clone()
        };
        assert_eq!(output[0]["type"], "reasoning");
        assert_eq!(
            output[0]["summary"],
            json!([{ "type": "summary_text", "text": "one two three" }])
        );
        assert!(
            output[0]["id"]
                .as_str()
                .is_some_and(|id| id.starts_with("rs_fake"))
        );
        assert!(
            output[0]["encrypted_content"]
                .as_str()
                .is_some_and(|c| c.starts_with("gAAAA"))
        );
        assert_eq!(output[1]["content"][0]["text"], "Hi");
        let mut sent = h.openai_sent()[0].clone();
        sent.as_object_mut().map(|o| o.remove("stream"));
        let mut asked = request.clone();
        asked.as_object_mut().map(|o| o.remove("stream"));
        assert_eq!(sent["reasoning"], asked["reasoning"]);
        assert_eq!(sent["include"], asked["include"]);
    }
}

#[tokio::test]
async fn what_only_openai_resolves_is_relayed_whole() {
    let h = Harness::new("responses").await;
    let request = json!({
        "model": "gpt-4o-mini",
        "input": [{ "role": "user", "content": "go on" }],
        "previous_response_id": "resp_prev",
        "store": false,
        "include": ["web_search_call.results"],
        "metadata": { "k": "v" },
        "truncation": "auto",
        "service_tier": "flex",
        "tools": [{ "type": "web_search_preview" }],
        "tool_choice": { "type": "web_search_preview" },
    });
    h.json(call(RESPONSES, request.clone())).await;
    assert_eq!(h.openai_sent(), [request]);
}

#[tokio::test]
async fn a_providers_error_comes_back_as_it_says() {
    let h = Harness::new("responses").await;
    h.openai
        .answer(Answer::error(429).retry_after(7).message("Slow down"));
    let (status, headers, body) = h
        .send(call(
            RESPONSES,
            json!({ "model": "gpt-4o-mini", "input": "hi" }),
        ))
        .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        headers.get("retry-after").and_then(|v| v.to_str().ok()),
        Some("7")
    );
    let v: Value = serde_json::from_str(&body).unwrap_or_default();
    assert_eq!(v["error"]["message"], "Slow down");
    assert!(v["error"]["type"].is_string(), "{body}");
}

#[tokio::test]
async fn a_request_the_provider_rejects_is_rejected_as_it_says() {
    let h = Harness::new("responses").await;
    let request = json!({ "model": "gpt-4o-mini", "input": "hi", "max_output_tokens": 99_999_999 });
    let (status, _, body) = h.send(call(RESPONSES, request)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let v: Value = serde_json::from_str(&body).unwrap_or_default();
    assert_eq!(v["error"]["param"], "max_output_tokens", "{body}");
}

#[tokio::test]
async fn a_stream_the_provider_breaks_ends_with_its_error_event() {
    let h = Harness::new("responses").await;
    h.openai
        .answer(Answer::text("one two three").error_after(6));
    let events = h
        .stream(call(
            RESPONSES,
            json!({ "model": "gpt-4o-mini", "stream": true, "input": "hi" }),
        ))
        .await;
    let last = &events[events.len() - 1];
    assert_eq!(last["type"], "error", "{events:?}");
    assert!(last["message"].is_string());
    assert!(events.iter().all(|e| e["type"] != "response.completed"));
}

#[tokio::test]
async fn incomplete_and_failed_responses_are_relayed() {
    let incomplete = json!({ "id": "resp_1", "object": "response", "created_at": 1,
        "status": "incomplete", "incomplete_details": { "reason": "max_output_tokens" },
        "model": "gpt-x", "usage": { "input_tokens": 3, "input_tokens_details": { "cached_tokens": 0 },
        "output_tokens": 4, "output_tokens_details": { "reasoning_tokens": 0 }, "total_tokens": 7 },
        "output": [{ "id": "msg_1", "type": "message", "status": "incomplete", "role": "assistant",
        "content": [{ "type": "output_text", "text": "Cut", "annotations": [] }] }] });
    let h = Harness::with(
        "responses",
        Local {
            responses: Some(local_json(StatusCode::OK, incomplete)),
            ..Local::default()
        },
    )
    .await;
    let body = h
        .json(call(RESPONSES, json!({ "model": "gpt-x", "input": "hi" })))
        .await;
    assert_eq!(body["status"], "incomplete");
    assert_eq!(
        body["incomplete_details"],
        json!({ "reason": "max_output_tokens" })
    );
    assert_eq!(body["output"][0]["content"][0]["text"], "Cut");

    let failed = json!({ "type": "response.failed", "sequence_number": 2, "response": {
        "id": "resp_1", "object": "response", "status": "failed",
        "error": { "code": "server_error", "message": "boom" } } });
    let created = json!({ "type": "response.created", "sequence_number": 0, "response": {
        "id": "resp_1", "object": "response", "status": "in_progress", "model": "gpt-x", "output": [] } });
    let h = Harness::with(
        "responses",
        Local {
            responses: Some(responses_events(vec![created, failed])),
            ..Local::default()
        },
    )
    .await;
    let (status, _, body) = h
        .send(call(
            RESPONSES,
            json!({ "model": "gpt-x", "stream": true, "input": "hi" }),
        ))
        .await;
    // A failure before any content leaves nothing started: it is refused.
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{body}");
    let v: Value = serde_json::from_str(&body).unwrap_or_default();
    assert_eq!(
        (&v["error"]["message"], &v["error"]["code"]),
        (&json!("boom"), &json!("server_error"))
    );
}

#[tokio::test]
async fn an_item_the_kind_has_no_form_for_is_relayed_in_a_body_and_a_stream() {
    let search = json!({ "id": "ws_1", "type": "web_search_call", "status": "completed",
        "action": { "type": "search", "query": "q" } });
    let message = json!({ "id": "msg_1", "type": "message", "status": "completed", "role": "assistant",
        "content": [{ "type": "output_text", "text": "Found",
            "annotations": [{ "type": "url_citation", "url": "https://x.test", "title": "t", "start_index": 0, "end_index": 5 }] }] });
    let body = json!({ "id": "resp_1", "object": "response", "created_at": 1, "status": "completed",
        "model": "gpt-x", "output": [search, message], "usage": null });
    let h = Harness::with(
        "responses",
        Local {
            responses: Some(local_json(StatusCode::OK, body)),
            ..Local::default()
        },
    )
    .await;
    let v = h
        .json(call(RESPONSES, json!({ "model": "gpt-x", "input": "hi" })))
        .await;
    assert_eq!(v["output"][0], search);
    assert_eq!(
        v["output"][1]["content"][0]["annotations"][0]["url"],
        "https://x.test"
    );
    assert_eq!(v["output"][1]["content"][0]["text"], "Found");
}

// ===================================================== Responses client out

#[tokio::test]
async fn a_responses_client_reaches_anthropic() {
    let h = Harness::new("claude").await;
    h.anthropic.answer(Answer::text("Hi there"));
    let request = json!({
        "model": "claude-x",
        "instructions": "be brief",
        "input": [
            { "role": "user", "content": "hi" },
            { "role": "assistant", "id": "msg_9", "content": "hello" },
            { "role": "user", "content": [{ "type": "input_text", "text": "again" }] },
        ],
        "max_output_tokens": 50,
        "temperature": 0.5,
        "tools": [weather_tool()],
        "tool_choice": "required",
    });
    let body = h.json(call(RESPONSES, request)).await;
    let sent = &h.claude_sent()[0];
    assert_eq!(sent["system"], "be brief");
    assert_eq!(sent["max_tokens"], 50);
    assert_eq!(sent["temperature"], 0.5);
    assert_eq!(sent["tool_choice"], json!({ "type": "any" }));
    assert_eq!(sent["tools"][0]["name"], "get_weather");
    assert_eq!(sent["tools"][0]["input_schema"]["type"], "object");
    assert_eq!(
        sent["messages"][0],
        json!({ "role": "user", "content": "hi" })
    );
    assert_eq!(sent["messages"][1]["content"], "hello");
    assert_eq!(body["object"], "response");
    assert_eq!(body["status"], "completed");
    assert_eq!(body["output"][0]["content"][0]["text"], "Hi there");
    assert_eq!(body["usage"]["output_tokens"], 2);
    assert!(
        body["usage"]["input_tokens"]
            .as_u64()
            .is_some_and(|n| n > 0)
    );
    assert!(
        body["output"][0]["id"]
            .as_str()
            .is_some_and(|id| id.starts_with("msg_"))
    );
}

#[tokio::test]
async fn a_responses_stream_from_anthropic_is_the_documented_sequence() {
    let h = Harness::new("claude").await;
    h.anthropic.answer(Answer::text("Hi there you"));
    let request =
        json!({ "model": "claude-x", "stream": true, "max_output_tokens": 30, "input": "hi" });
    let events = h.stream(call(RESPONSES, request)).await;
    assert_well_formed(&events);
    assert_eq!(
        runs(&events),
        [
            "response.created",
            "response.in_progress",
            "response.output_item.added",
            "response.content_part.added",
            "response.output_text.delta",
            "response.output_text.done",
            "response.content_part.done",
            "response.output_item.done",
            "response.completed",
        ]
    );
    assert_eq!(streamed_text(&events), "Hi there you");
    // Ids and indexes the provider has none of are made.
    let added = &events[2];
    assert_eq!(added["output_index"], 0);
    assert_eq!(added["item"]["role"], "assistant");
    assert_eq!(added["item"]["status"], "in_progress");
    let id = added["item"]["id"].clone();
    assert!(id.as_str().is_some_and(|id| id.starts_with("msg_")));
    for e in events.iter().filter(|e| {
        e["type"]
            .as_str()
            .is_some_and(|t| t.contains("text") || t.contains("content_part"))
    }) {
        assert_eq!(
            (&e["item_id"], &e["content_index"]),
            (&id, &json!(0)),
            "{e}"
        );
    }
    let done = &events[events.len() - 1]["response"];
    assert_eq!(done["status"], "completed");
    assert_eq!(done["output"][0]["id"], id);
    assert_eq!(done["output"][0]["content"][0]["text"], "Hi there you");
    assert_eq!(done["usage"]["output_tokens"], 3);
    assert_eq!(h.claude_sent()[0]["stream"], true);
}

#[tokio::test]
async fn a_function_call_from_anthropic_and_its_output_go_round() {
    for stream in [false, true] {
        let h = Harness::new("claude").await;
        h.anthropic
            .answer(Answer::tool_call("get_weather", json!({ "city": "Paris" })));
        let first = json!({
            "model": "claude-x", "stream": stream, "max_output_tokens": 64,
            "input": "weather in Paris?", "tools": [weather_tool()],
        });
        let item = if stream {
            let events = h.stream(call(RESPONSES, first)).await;
            assert_well_formed(&events);
            assert!(runs(&events).contains(&"response.function_call_arguments.delta"));
            events[events.len() - 1]["response"]["output"][0].clone()
        } else {
            h.json(call(RESPONSES, first)).await["output"][0].clone()
        };
        assert_eq!(item["type"], "function_call");
        assert_eq!(item["name"], "get_weather");
        assert!(
            item["call_id"]
                .as_str()
                .is_some_and(|id| id.starts_with("toolu_")),
            "{item}"
        );
        let arguments: Value =
            serde_json::from_str(item["arguments"].as_str().unwrap_or("")).unwrap_or_default();
        assert_eq!(arguments, json!({ "city": "Paris" }));
        // The turn after: Anthropic pairs results with calls.
        let second = json!({
            "model": "claude-x", "max_output_tokens": 64,
            "tools": [weather_tool()],
            "input": [
                { "role": "user", "content": "weather in Paris?" },
                item.clone(),
                { "type": "function_call_output", "call_id": item["call_id"], "output": "sunny" },
            ],
        });
        let body = h.json(call(RESPONSES, second)).await;
        assert_eq!(body["status"], "completed");
        let sent = &h.claude_sent()[1];
        assert_eq!(sent["messages"][1]["content"][0]["type"], "tool_use");
        assert_eq!(sent["messages"][1]["content"][0]["id"], item["call_id"]);
        assert_eq!(sent["messages"][2]["content"][0]["type"], "tool_result");
        assert_eq!(sent["messages"][2]["content"][0]["content"], "sunny");
    }
}

#[tokio::test]
async fn a_responses_client_reaches_a_chat_provider() {
    for stream in [false, true] {
        let h = Harness::new("chat").await;
        h.openai.answer(Answer::text("Hi there"));
        let request = json!({
            "model": "gpt-4o-mini", "stream": stream,
            "instructions": "be brief",
            "input": [{ "role": "user", "content": "hi" }],
            "max_output_tokens": 20,
            "user": "u1",
            "text": { "format": { "type": "json_object" } },
        });
        let text = if stream {
            let events = h.stream(call(RESPONSES, request)).await;
            assert_well_formed(&events);
            assert_eq!(
                events[events.len() - 1]["response"]["usage"]["output_tokens"],
                2
            );
            streamed_text(&events)
        } else {
            let body = h.json(call(RESPONSES, request)).await;
            assert_eq!(body["usage"]["output_tokens"], 2);
            body["output"][0]["content"][0]["text"]
                .as_str()
                .unwrap_or_default()
                .to_owned()
        };
        assert_eq!(text, "Hi there");
        let sent = &h.openai_sent()[0];
        assert_eq!(
            sent["messages"][0],
            json!({ "role": "system", "content": "be brief" })
        );
        assert_eq!(
            sent["messages"][1],
            json!({ "role": "user", "content": "hi" })
        );
        assert_eq!(sent["max_completion_tokens"], 20);
        assert_eq!(sent["user"], "u1");
        assert_eq!(sent["response_format"], json!({ "type": "json_object" }));
    }
}

#[tokio::test]
async fn a_function_call_from_a_chat_provider_is_a_function_call_item() {
    let h = Harness::new("chat").await;
    h.openai
        .answer(Answer::tool_call("get_weather", json!({ "city": "Paris" })));
    let request = json!({
        "model": "gpt-4o-mini", "stream": true, "input": "weather?", "tools": [weather_tool()],
        "tool_choice": { "type": "function", "name": "get_weather" },
        "parallel_tool_calls": false,
    });
    let events = h.stream(call(RESPONSES, request)).await;
    assert_well_formed(&events);
    let item = &events[events.len() - 1]["response"]["output"][0];
    assert_eq!(item["type"], "function_call");
    assert!(
        item["call_id"]
            .as_str()
            .is_some_and(|id| id.starts_with("call_fake")),
        "{item}"
    );
    let sent = &h.openai_sent()[0];
    assert_eq!(sent["tools"][0]["function"]["name"], "get_weather");
    assert_eq!(
        sent["tool_choice"],
        json!({ "type": "function", "function": { "name": "get_weather" } })
    );
    assert_eq!(sent["parallel_tool_calls"], false);
}

// ===================================================== Responses provider in

#[tokio::test]
async fn a_chat_client_reaches_a_responses_provider() {
    for stream in [false, true] {
        let h = Harness::new("responses").await;
        h.openai.answer(Answer::text("Hi there"));
        let request = json!({
            "model": "gpt-4o-mini", "stream": stream,
            "stream_options": { "include_usage": true },
            "messages": [
                { "role": "system", "content": "be brief" },
                { "role": "user", "content": "hi" },
            ],
            "max_tokens": 20,
        });
        let (status, _, body) = h.send(call(CHAT, request)).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let sent = &h.openai_sent()[0];
        assert_eq!(sent["instructions"], "be brief");
        assert_eq!(sent["input"], json!([{ "role": "user", "content": "hi" }]));
        assert_eq!(sent["max_output_tokens"], 20);
        assert_eq!(sent.get("stream"), stream.then_some(&json!(true)));
        // Its client can name no response: none is stored, and reasoning
        // comes encrypted, to be carried to a later call.
        assert_eq!(sent["store"], false);
        assert_eq!(sent["include"], json!(["reasoning.encrypted_content"]));
        if stream {
            assert!(body.contains("\"content\":\"Hi\""), "{body}");
            assert!(body.contains("\"completion_tokens\":2"), "{body}");
            assert!(body.trim_end().ends_with("data: [DONE]"));
        } else {
            let v: Value = serde_json::from_str(&body).unwrap_or_default();
            assert_eq!(v["choices"][0]["message"]["content"], "Hi there");
            assert_eq!(v["choices"][0]["finish_reason"], "stop");
            assert_eq!(v["usage"]["completion_tokens"], 2);
        }
    }
}

#[tokio::test]
async fn an_anthropic_client_reaches_a_responses_provider_with_tools() {
    for stream in [false, true] {
        let h = Harness::new("responses").await;
        h.openai
            .answer(Answer::tool_call("get_weather", json!({ "city": "Paris" })));
        let request = json!({
            "model": "gpt-4o-mini", "max_tokens": 64, "stream": stream,
            "system": "be brief",
            "tools": [{ "name": "get_weather", "description": "weather",
                "input_schema": { "type": "object", "properties": { "city": { "type": "string" } } } }],
            "messages": [{ "role": "user", "content": "weather?" }],
        });
        let (status, _, body) = h.send(call(MESSAGES, request)).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let sent = &h.openai_sent()[0];
        assert_eq!(sent["instructions"], "be brief");
        assert_eq!(sent["max_output_tokens"], 64);
        assert_eq!(sent["tools"][0]["type"], "function");
        assert_eq!(
            sent["tools"][0]["strict"], false,
            "another format's tools are not strict"
        );
        assert_eq!(
            sent["input"],
            json!([{ "role": "user", "content": "weather?" }])
        );
        if stream {
            assert!(body.contains("\"type\":\"tool_use\""), "{body}");
            assert!(body.contains("\"stop_reason\":\"tool_use\""), "{body}");
        } else {
            let v: Value = serde_json::from_str(&body).unwrap_or_default();
            assert_eq!(v["content"][0]["type"], "tool_use");
            assert_eq!(v["content"][0]["input"], json!({ "city": "Paris" }));
            assert_eq!(v["stop_reason"], "tool_use");
        }
        // The turn after: the fake pairs outputs with calls.
        let call_id = if stream {
            body.split("\"id\":\"call_fake")
                .nth(1)
                .map(|s| format!("call_fake{}", s.split('"').next().unwrap_or("")))
        } else {
            serde_json::from_str::<Value>(&body)
                .ok()
                .and_then(|v| v["content"][0]["id"].as_str().map(str::to_owned))
        }
        .unwrap_or_default();
        let second = json!({
            "model": "gpt-4o-mini", "max_tokens": 64,
            "messages": [
                { "role": "user", "content": "weather?" },
                { "role": "assistant", "content": [{ "type": "tool_use", "id": call_id, "name": "get_weather", "input": { "city": "Paris" } }] },
                { "role": "user", "content": [{ "type": "tool_result", "tool_use_id": call_id, "content": "sunny" }] },
            ],
        });
        let (status, _, body) = h.send(call(MESSAGES, second)).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let sent = &h.openai_sent()[1];
        assert_eq!(sent["input"][1]["type"], "function_call");
        assert_eq!(sent["input"][1]["call_id"], call_id.as_str());
        assert_eq!(
            sent["input"][2],
            json!({ "type": "function_call_output", "call_id": call_id, "output": "sunny" })
        );
    }
}

// ============================================================== refusals

#[tokio::test]
async fn what_another_provider_cannot_resolve_is_refused_before_a_call() {
    for (field, value) in [
        ("previous_response_id", json!("resp_prev")),
        ("conversation", json!("conv_1")),
        ("include", json!(["reasoning.encrypted_content"])),
        ("reasoning", json!({ "summary": "detailed" })),
        ("truncation", json!("auto")),
        ("tools", json!([{ "type": "web_search_preview" }])),
        ("tool_choice", json!({ "type": "file_search" })),
    ] {
        for target in ["claude", "chat"] {
            let h = Harness::new(target).await;
            let mut request =
                json!({ "model": "claude-x", "input": "hi", "max_output_tokens": 20 });
            request[field] = value.clone();
            let (status, _, body) = h.send(call(RESPONSES, request)).await;
            assert_eq!(
                status,
                StatusCode::BAD_REQUEST,
                "{field} to {target}: {body}"
            );
            let v: Value = serde_json::from_str(&body).unwrap_or_default();
            let message = v["error"]["message"].as_str().unwrap_or_default();
            assert!(
                message.contains(field) && message.contains("translated"),
                "{field}: {body}"
            );
            assert!(
                h.claude_sent().is_empty() && h.openai_sent().is_empty(),
                "{field} to {target}"
            );
        }
    }
}

#[tokio::test]
async fn an_item_a_provider_stored_and_a_failed_result_are_refused() {
    let h = Harness::new("claude").await;
    let reference = json!({ "model": "claude-x", "max_output_tokens": 5, "input": [{ "type": "item_reference", "id": "msg_1" }] });
    let (status, _, _) = h.send(call(RESPONSES, reference)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // An Anthropic client's failed result has no form in Responses.
    let h = Harness::new("responses").await;
    let failed = json!({
        "model": "gpt-4o-mini", "max_tokens": 5,
        "messages": [
            { "role": "user", "content": "go" },
            { "role": "assistant", "content": [{ "type": "tool_use", "id": "t1", "name": "f", "input": {} }] },
            { "role": "user", "content": [{ "type": "tool_result", "tool_use_id": "t1", "content": "boom", "is_error": true }] },
        ],
    });
    let (status, _, body) = h.send(call(MESSAGES, failed)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(h.openai_sent().is_empty());

    // Nor do stop sequences.
    let stop = json!({ "model": "gpt-4o-mini", "max_tokens": 5, "stop_sequences": ["x"], "messages": [{ "role": "user", "content": "go" }] });
    let (status, _, body) = h.send(call(MESSAGES, stop)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(body.contains("stop sequences"), "{body}");
}

// ====================================================== reasoning, usage

fn responses_reply(output: Value, usage: Value) -> Value {
    json!({ "id": "resp_7", "object": "response", "created_at": 1, "status": "completed",
        "model": "gpt-x", "output": output, "usage": usage })
}

fn usage_of_responses() -> Value {
    json!({ "input_tokens": 10, "input_tokens_details": { "cached_tokens": 4 },
        "output_tokens": 6, "output_tokens_details": { "reasoning_tokens": 2 }, "total_tokens": 16 })
}

#[tokio::test]
async fn reasoning_and_usage_of_a_responses_provider_reach_other_clients() {
    let output = json!([
        { "id": "rs_1", "type": "reasoning", "summary": [{ "type": "summary_text", "text": "thinking" }], "encrypted_content": "gAAA" },
        { "id": "msg_1", "type": "message", "status": "completed", "role": "assistant",
          "content": [{ "type": "output_text", "text": "Answer", "annotations": [] }] },
    ]);
    let reply = responses_reply(output, usage_of_responses());
    let local = || Local {
        responses: Some(local_json(StatusCode::OK, reply.clone())),
        ..Local::default()
    };
    // Anthropic: a thinking block, the cache read apart from the input.
    let h = Harness::with("responses", local()).await;
    let v = h.json(call(MESSAGES, json!({ "model": "gpt-x", "max_tokens": 50, "messages": [{ "role": "user", "content": "hi" }] }))).await;
    assert_eq!(
        v["content"][0],
        json!({ "type": "thinking", "thinking": "thinking" })
    );
    assert_eq!(v["content"][1]["text"], "Answer");
    assert_eq!(v["usage"]["input_tokens"], 6);
    assert_eq!(v["usage"]["cache_read_input_tokens"], 4);
    assert_eq!(v["usage"]["output_tokens"], 6);
    let metered = h.usage.total();
    assert_eq!(
        (
            metered.input_tokens,
            metered.cached_input_tokens,
            metered.reasoning_tokens
        ),
        (10, 4, 2)
    );
    // Chat has no field for reasoning, but its usage says it.
    let h = Harness::with("responses", local()).await;
    let (status, _, body) = h
        .send(call(
            CHAT,
            json!({ "model": "gpt-x", "messages": [{ "role": "user", "content": "hi" }] }),
        ))
        .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{body}");
}

#[tokio::test]
async fn reasoning_and_cache_usage_of_other_providers_reach_a_responses_client() {
    // Anthropic: a thinking block, with a signature that has no place here.
    let thinking = json!({ "id": "msg_1", "type": "message", "role": "assistant", "model": "claude-x",
        "content": [
            { "type": "thinking", "thinking": "hmm", "signature": "sig" },
            { "type": "text", "text": "Answer" },
        ],
        "stop_reason": "end_turn", "stop_sequence": null,
        "usage": { "input_tokens": 5, "cache_read_input_tokens": 3, "output_tokens": 4 } });
    let h = Harness::with(
        "claude",
        Local {
            claude: Some(local_json(StatusCode::OK, thinking)),
            ..Local::default()
        },
    )
    .await;
    let v = h
        .json(call(
            RESPONSES,
            json!({ "model": "claude-x", "max_output_tokens": 50, "input": "hi" }),
        ))
        .await;
    assert_eq!(v["output"][0]["type"], "reasoning");
    assert_eq!(
        v["output"][0]["summary"],
        json!([{ "type": "summary_text", "text": "hmm" }])
    );
    assert_eq!(v["output"][1]["content"][0]["text"], "Answer");
    assert_eq!(v["usage"]["input_tokens"], 8);
    assert_eq!(v["usage"]["input_tokens_details"]["cached_tokens"], 3);
    assert_eq!(v["usage"]["total_tokens"], 12);

    // The same, streamed.
    let events: Vec<(String, Value)> = [
        json!({ "type": "message_start", "message": { "id": "msg_1", "type": "message", "role": "assistant", "model": "claude-x", "content": [], "usage": { "input_tokens": 5, "output_tokens": 1 } } }),
        json!({ "type": "content_block_start", "index": 0, "content_block": { "type": "thinking", "thinking": "", "signature": "" } }),
        json!({ "type": "content_block_delta", "index": 0, "delta": { "type": "thinking_delta", "thinking": "hmm" } }),
        json!({ "type": "content_block_delta", "index": 0, "delta": { "type": "signature_delta", "signature": "sig" } }),
        json!({ "type": "content_block_stop", "index": 0 }),
        json!({ "type": "content_block_start", "index": 1, "content_block": { "type": "text", "text": "" } }),
        json!({ "type": "content_block_delta", "index": 1, "delta": { "type": "text_delta", "text": "Answer" } }),
        json!({ "type": "content_block_stop", "index": 1 }),
        json!({ "type": "message_delta", "delta": { "stop_reason": "max_tokens" }, "usage": { "output_tokens": 4 } }),
        json!({ "type": "message_stop" }),
    ]
    .into_iter()
    .map(|e| (e["type"].as_str().unwrap_or("").to_owned(), e))
    .collect();
    let h = Harness::with(
        "claude",
        Local {
            claude: Some(local_events(events)),
            ..Local::default()
        },
    )
    .await;
    let events = h
        .stream(call(
            RESPONSES,
            json!({ "model": "claude-x", "stream": true, "max_output_tokens": 50, "input": "hi" }),
        ))
        .await;
    assert_well_formed(&events);
    let kinds = runs(&events);
    assert!(
        kinds.contains(&"response.reasoning_summary_text.delta"),
        "{kinds:?}"
    );
    let last = &events[events.len() - 1];
    assert_eq!(last["type"], "response.incomplete");
    assert_eq!(
        last["response"]["incomplete_details"],
        json!({ "reason": "max_output_tokens" })
    );
    assert_eq!(last["response"]["output"][0]["summary"][0]["text"], "hmm");
    assert_eq!(
        last["response"]["output"][1]["content"][0]["text"],
        "Answer"
    );
}

#[tokio::test]
async fn cached_and_reasoning_tokens_of_a_chat_provider_reach_a_responses_client() {
    let reply = json!({ "id": "c1", "object": "chat.completion", "created": 1, "model": "gpt-x",
        "choices": [{ "index": 0, "message": { "role": "assistant", "content": "Cut" }, "finish_reason": "length" }],
        "usage": { "prompt_tokens": 9, "completion_tokens": 5, "total_tokens": 14,
            "prompt_tokens_details": { "cached_tokens": 3 }, "completion_tokens_details": { "reasoning_tokens": 2 } } });
    let h = Harness::with(
        "chat",
        Local {
            chat: Some(local_json(StatusCode::OK, reply)),
            ..Local::default()
        },
    )
    .await;
    let v = h
        .json(call(RESPONSES, json!({ "model": "gpt-x", "input": "hi" })))
        .await;
    assert_eq!(v["status"], "incomplete");
    assert_eq!(
        v["incomplete_details"],
        json!({ "reason": "max_output_tokens" })
    );
    assert_eq!(v["usage"]["input_tokens"], 9);
    assert_eq!(v["usage"]["input_tokens_details"]["cached_tokens"], 3);
    assert_eq!(v["usage"]["output_tokens_details"]["reasoning_tokens"], 2);
    assert_eq!(v["output"][0]["content"][0]["text"], "Cut");
}

#[tokio::test]
async fn the_reason_a_responses_provider_stopped_reaches_other_clients() {
    for (reason, chat, claude) in [
        ("max_output_tokens", "length", "max_tokens"),
        ("content_filter", "content_filter", "refusal"),
    ] {
        let mut reply = responses_reply(json!([]), usage_of_responses());
        reply["status"] = "incomplete".into();
        reply["incomplete_details"] = json!({ "reason": reason });
        let local = || Local {
            responses: Some(local_json(StatusCode::OK, reply.clone())),
            ..Local::default()
        };
        let h = Harness::with("responses", local()).await;
        let v = h
            .json(call(
                CHAT,
                json!({ "model": "gpt-x", "messages": [{ "role": "user", "content": "hi" }] }),
            ))
            .await;
        assert_eq!(v["choices"][0]["finish_reason"], chat);
        let h = Harness::with("responses", local()).await;
        let v = h.json(call(MESSAGES, json!({ "model": "gpt-x", "max_tokens": 5, "messages": [{ "role": "user", "content": "hi" }] }))).await;
        assert_eq!(v["stop_reason"], claude);
    }
}

// ================================================================ errors

#[tokio::test]
async fn errors_keep_each_clients_schema_across_formats() {
    // Anthropic's overload reaches a Responses client as an OpenAI error.
    let h = Harness::new("claude").await;
    h.anthropic.answer(Answer::error(529).message("Overloaded"));
    let (status, _, body) = h
        .send(call(
            RESPONSES,
            json!({ "model": "claude-x", "max_output_tokens": 5, "input": "hi" }),
        ))
        .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    let v: Value = serde_json::from_str(&body).unwrap_or_default();
    assert_eq!(v["error"]["message"], "Overloaded");
    assert!(
        v["error"]["type"].is_string() && v.get("type").is_none(),
        "{body}"
    );

    // A Responses provider's error reaches an Anthropic client as Anthropic's.
    let h = Harness::new("responses").await;
    h.openai.answer(Answer::error(429).message("Slow down"));
    let (status, _, body) = h.send(call(MESSAGES, json!({ "model": "gpt-4o-mini", "max_tokens": 5, "messages": [{ "role": "user", "content": "hi" }] }))).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{body}");
    let v: Value = serde_json::from_str(&body).unwrap_or_default();
    assert_eq!(v["type"], "error");
    assert_eq!(v["error"]["message"], "Slow down");

    // A failed response, whole, is the client's error.
    let failed = json!({ "id": "resp_1", "object": "response", "status": "failed", "model": "gpt-x",
        "error": { "code": "server_error", "message": "boom" }, "output": [] });
    let h = Harness::with(
        "responses",
        Local {
            responses: Some(local_json(StatusCode::OK, failed)),
            ..Local::default()
        },
    )
    .await;
    let (status, _, body) = h
        .send(call(
            CHAT,
            json!({ "model": "gpt-x", "messages": [{ "role": "user", "content": "hi" }] }),
        ))
        .await;
    assert!(status.is_server_error(), "{status}: {body}");
    assert!(body.contains("boom"), "{body}");
}

#[tokio::test]
async fn a_stream_cut_before_its_end_is_an_error_to_every_client() {
    let created = json!({ "type": "response.created", "response": {
        "id": "resp_1", "object": "response", "status": "in_progress", "model": "gpt-x", "output": [] } });
    let added = json!({ "type": "response.output_item.added", "output_index": 0,
        "item": { "id": "msg_1", "type": "message", "status": "in_progress", "role": "assistant", "content": [] } });
    let part = json!({ "type": "response.content_part.added", "output_index": 0, "content_index": 0,
        "item_id": "msg_1", "part": { "type": "output_text", "text": "", "annotations": [] } });
    let delta = json!({ "type": "response.output_text.delta", "output_index": 0, "content_index": 0,
        "item_id": "msg_1", "delta": "Hi" });
    let cut = || Local {
        responses: Some(responses_events(vec![
            created.clone(),
            added.clone(),
            part.clone(),
            delta.clone(),
        ])),
        ..Local::default()
    };
    let h = Harness::with("responses", cut()).await;
    let (_, _, body) = h.send(call(MESSAGES, json!({ "model": "gpt-x", "max_tokens": 5, "stream": true, "messages": [{ "role": "user", "content": "hi" }] }))).await;
    assert!(body.contains("event: error"), "{body}");
    assert!(!body.contains("message_stop"), "{body}");
    let h = Harness::with("responses", cut()).await;
    let (_, _, body) = h.send(call(CHAT, json!({ "model": "gpt-x", "stream": true, "messages": [{ "role": "user", "content": "hi" }] }))).await;
    assert!(!body.contains("[DONE]"), "{body}");
}

#[tokio::test]
async fn a_usage_total_is_counted_once_per_call() {
    let h = Harness::new("claude").await;
    h.anthropic.answer(Answer::text("Hi there"));
    h.json(call(
        RESPONSES,
        json!({ "model": "claude-x", "max_output_tokens": 9, "input": "hi" }),
    ))
    .await;
    let total = h.usage.total();
    assert_eq!(total.output_tokens, 2);
    assert!(total.input_tokens > 0);
    let _ = Usage::default();
}
