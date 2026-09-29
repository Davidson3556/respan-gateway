//! End-to-end tests of the shipped binary.
//!
//! Each test starts `respan-gateway` itself (`CARGO_BIN_EXE_respan-gateway`)
//! on a free port, with a temp YAML config whose providers are `mockllm`
//! fakes on localhost: `FakeOpenAi` and `FakeAnthropic` check each request
//! by the provider's own rules and answer in its exact framing, streams and
//! breaks included. Clients speak real HTTP to the real axum server.
//!
//! A byte-exact relay is checked against exact scripted bytes
//! (`Answer::raw`, or verbatim SSE events): the client gets them unchanged.
//!
//! Run: `cargo test -p respan-gateway --test e2e`. Calls to real providers
//! are in `tests/real_providers.rs`, ignored unless asked for.

#![allow(clippy::unwrap_used)]

mod support;

use mockllm::{Answer, FakeAnthropic, FakeOpenAi, Mock, Provider, Request};
use serde_json::{Value, json};
use support::{Gateway, client, closed_port, json, read_all};

const CHAT: &str = "/v1/chat/completions";
const MESSAGES: &str = "/v1/messages";
const GPT: &str = "gpt-4.1-mini";
const CLAUDE: &str = "claude-sonnet-4-5";

/// A config with one provider of `format` at `base_url`, no key of its
/// own, serving both formats, as the default target.
fn one_provider(format: &str, base_url: &str) -> String {
    format!(
        "providers:
  p: {{ format: {format}, base_url: '{base_url}' }}
serve: [openai-chat, anthropic-messages]
target: p
"
    )
}

/// OpenAI and Anthropic providers with keys of their own, Claude models
/// routed to Anthropic, the rest to OpenAI, as in `gateway.example.yaml`.
fn both(openai: &str, anthropic: &str) -> String {
    format!(
        "providers:
  openai: {{ format: openai-chat, base_url: '{openai}', auth: {{ type: bearer, key: sk-gw-openai }} }}
  anthropic: {{ format: anthropic-messages, base_url: '{anthropic}', auth: {{ type: header, name: x-api-key, key: sk-gw-anthropic }} }}
serve: [openai-chat, anthropic-messages]
plugins:
  - name: models
    models: [{{ prefix: claude, provider: anthropic }}]
target: openai
"
    )
}

/// An OpenAI client's key.
const OPENAI_KEY: (&str, &str) = ("authorization", "Bearer sk-client");
/// An Anthropic client's key and version.
const ANTHROPIC_KEY: [(&str, &str); 2] = [
    ("x-api-key", "sk-ant-client"),
    ("anthropic-version", "2023-06-01"),
];

/// Odd spacing and key order, and fields the gateway has no place for: a
/// relay sends it unread.
const OPENAI_REQUEST: &str = r#"{ "messages":[{"role":"user","content":"hi"}], "model":"gpt-4.1-mini","metadata":{"kept":"yes"},"seed":7 }"#;

fn openai_request(extra: &str) -> String {
    format!(r#"{{"model":"{GPT}","messages":[{{"role":"user","content":"hi"}}]{extra}}}"#)
}

fn claude_request(extra: &str) -> String {
    format!(
        r#"{{"model":"{CLAUDE}","max_tokens":64,"messages":[{{"role":"user","content":"hi"}}]{extra}}}"#
    )
}

/// POSTs `body` to `url` with `headers`.
async fn post(url: String, headers: &[(&str, &str)], body: impl Into<String>) -> reqwest::Response {
    let mut req = client().post(url).body(body.into());
    for (name, value) in headers {
        req = req.header(*name, *value);
    }
    req.send().await.unwrap()
}

/// A stream of `events`, each a whole SSE event sent verbatim.
fn script(events: &[&str]) -> Answer {
    Answer::events(events.iter().map(|e| json!(e)).collect())
}

/// The bytes `script(events)` sends.
fn sse(events: &[&str]) -> String {
    events.iter().map(|e| format!("{e}\n\n")).collect()
}

/// An OpenAI stream of "hel lo", with a field the gateway has no place for.
const OPENAI_STREAM: [&str; 5] = [
    r#"data: {"id":"chatcmpl-1","object":"chat.completion.chunk","created":1,"model":"gpt-4.1-mini","choices":[{"index":0,"delta":{"role":"assistant","content":""},"finish_reason":null}],"x_new":1}"#,
    r#"data: {"id":"chatcmpl-1","object":"chat.completion.chunk","created":1,"model":"gpt-4.1-mini","choices":[{"index":0,"delta":{"content":"hel"},"finish_reason":null}],"x_new":1}"#,
    r#"data: {"id":"chatcmpl-1","object":"chat.completion.chunk","created":1,"model":"gpt-4.1-mini","choices":[{"index":0,"delta":{"content":" lo"},"finish_reason":null}],"x_new":1}"#,
    r#"data: {"id":"chatcmpl-1","object":"chat.completion.chunk","created":1,"model":"gpt-4.1-mini","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"x_new":1}"#,
    "data: [DONE]",
];

/// An Anthropic stream of "hel lo", pinged before its content.
const ANTHROPIC_STREAM: [&str; 8] = [
    "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-5\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null,\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}",
    "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}",
    "event: ping\ndata: {\"type\": \"ping\"}",
    "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hel\"}}",
    "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\" lo\"}}",
    "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}",
    "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\",\"stop_sequence\":null},\"usage\":{\"output_tokens\":2}}",
    "event: message_stop\ndata: {\"type\":\"message_stop\"}",
];

fn header<'a>(headers: &'a reqwest::header::HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

/// The text of an OpenAI stream's deltas, and whether it ended `[DONE]`.
fn openai_stream_text(body: &str) -> (String, bool) {
    let mut text = String::new();
    let mut done = false;
    for data in body.lines().filter_map(|l| l.strip_prefix("data: ")) {
        if data == "[DONE]" {
            done = true;
            continue;
        }
        let v = json(data);
        if let Some(t) = v["choices"][0]["delta"]["content"].as_str() {
            text.push_str(t);
        }
    }
    (text, done)
}

// 1. OpenAI chat relay (use case 12).

#[tokio::test]
async fn openai_whole_relay_is_byte_exact_and_passes_the_clients_key() {
    let openai = FakeOpenAi::start().await;
    let gw = Gateway::start(&one_provider("openai-chat", &openai.base_url())).await;
    let headers = [
        OPENAI_KEY,
        ("openai-beta", "assistants=v9"),
        ("content-type", "application/json"),
    ];

    // Odd spacing and a field the gateway has no place for.
    let answer = r#"{ "id":"chatcmpl-1", "object":"chat.completion","created":1,"model":"gpt-4.1-mini","choices":[{"index":0,"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}],"x_new":1 }"#;
    openai.answer(Answer::raw(answer).header("x-request-id", "req_1"));
    let resp = post(gw.url(CHAT), &headers, OPENAI_REQUEST).await;

    assert_eq!(resp.status(), 200);
    assert_eq!(header(resp.headers(), "x-request-id"), Some("req_1"));
    assert_eq!(resp.text().await.unwrap(), answer);
    let got = &openai.requests()[0];
    assert_eq!(got.path, CHAT);
    assert_eq!(
        got.raw,
        OPENAI_REQUEST.as_bytes(),
        "the client's bytes, unread"
    );
    assert_eq!(got.header("authorization"), Some("Bearer sk-client"));
    assert_eq!(got.header("openai-beta"), Some("assistants=v9"));
}

#[tokio::test]
async fn openai_stream_relay_is_byte_exact_and_passes_the_clients_key() {
    let openai = FakeOpenAi::start().await;
    let gw = Gateway::start(&one_provider("openai-chat", &openai.base_url())).await;
    let request = openai_request(
        r#","stream":true,"stream_options":{"include_usage":true},"metadata":{"kept":"yes"}"#,
    );

    openai.answer(script(&OPENAI_STREAM).pace(20));
    let resp = post(gw.url(CHAT), &[OPENAI_KEY], request.clone()).await;

    assert_eq!(resp.status(), 200);
    assert_eq!(
        header(resp.headers(), "content-type"),
        Some("text/event-stream; charset=utf-8")
    );
    let (body, ended) = read_all(resp).await;
    assert_eq!(ended, Ok(()));
    let got = &openai.requests()[0];
    assert_eq!(got.path, CHAT);
    assert_eq!(got.raw, request.as_bytes());
    assert_eq!(got.header("authorization"), Some("Bearer sk-client"));
    assert_eq!(body, sse(&OPENAI_STREAM));
}

#[tokio::test]
async fn usage_the_client_did_not_ask_for_is_asked_for_and_hidden() {
    let openai = FakeOpenAi::start().await;
    let gw = Gateway::start(&one_provider("openai-chat", &openai.base_url())).await;
    let request = openai_request(r#","stream":true,"metadata":{"kept":"yes"}"#);
    // What the provider sends when asked for usage: `"usage":null` on each
    // chunk, then a chunk of usage alone.
    let with_usage: Vec<String> = OPENAI_STREAM[..4]
        .iter()
        .map(|e| e.replace(r#","x_new":1}"#, r#","x_new":1,"usage":null}"#))
        .chain([
            r#"data: {"id":"chatcmpl-1","object":"chat.completion.chunk","created":1,"model":"gpt-4.1-mini","choices":[],"usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}"#.into(),
            OPENAI_STREAM[4].into(),
        ])
        .collect();

    openai.answer(script(
        &with_usage.iter().map(String::as_str).collect::<Vec<_>>(),
    ));
    let resp = post(gw.url(CHAT), &[OPENAI_KEY], request).await;

    let (body, ended) = read_all(resp).await;
    assert_eq!(ended, Ok(()));
    let seen = body_of(&openai, 0);
    assert_eq!(seen["stream_options"]["include_usage"], true);
    assert_eq!(seen["metadata"]["kept"], "yes");
    // What the provider sends a client that did not ask for usage.
    assert_eq!(body, sse(&OPENAI_STREAM), "the usage is hidden");
}

fn body_of<P: Provider>(mock: &Mock<P>, i: usize) -> Value {
    mock.requests()[i].body.clone()
}

#[tokio::test]
async fn a_provider_with_its_own_key_replaces_the_clients() {
    let openai = FakeOpenAi::start().await;
    let config = format!(
        "providers:
  p: {{ format: openai-chat, base_url: '{}', auth: {{ type: bearer, key: sk-own }} }}
serve: [openai-chat]
target: p
",
        openai.base_url()
    );
    let gw = Gateway::start(&config).await;

    let resp = post(
        gw.url(CHAT),
        &[OPENAI_KEY, ("x-api-key", "client-key")],
        OPENAI_REQUEST,
    )
    .await;

    assert_eq!(resp.status(), 200);
    let got = &openai.requests()[0];
    assert_eq!(got.header("authorization"), Some("Bearer sk-own"));
    assert_eq!(got.header("x-api-key"), None);
}

// 2. Anthropic messages relay.

#[tokio::test]
async fn anthropic_stream_relay_is_byte_exact_and_passes_the_clients_key() {
    let anthropic = FakeAnthropic::start().await;
    let gw = Gateway::start(&one_provider("anthropic-messages", &anthropic.base_url())).await;
    let request = claude_request(r#","stream":true,"metadata":{"user_id":"u1"}"#);
    let mut headers = ANTHROPIC_KEY.to_vec();
    headers.push(("anthropic-beta", "some-future-beta-2099-01-01"));

    anthropic.answer(script(&ANTHROPIC_STREAM).pace(20));
    let resp = post(gw.url(MESSAGES), &headers, request.clone()).await;

    assert_eq!(resp.status(), 200);
    let (body, ended) = read_all(resp).await;
    assert_eq!(ended, Ok(()));
    let got = &anthropic.requests()[0];
    assert_eq!(got.path, MESSAGES);
    assert_eq!(got.raw, request.as_bytes());
    assert_eq!(got.header("x-api-key"), Some("sk-ant-client"));
    assert_eq!(got.header("anthropic-version"), Some("2023-06-01"));
    assert_eq!(
        got.header("anthropic-beta"),
        Some("some-future-beta-2099-01-01")
    );
    assert_eq!(body, sse(&ANTHROPIC_STREAM));
}

#[tokio::test]
async fn anthropic_whole_relay_is_byte_exact_and_sends_a_default_version() {
    let anthropic = FakeAnthropic::start().await;
    let gw = Gateway::start(&one_provider("anthropic-messages", &anthropic.base_url())).await;
    let request = claude_request(r#","metadata":{"user_id":"u1"}"#);

    let answer = r#"{"id":"msg_1", "type":"message","role":"assistant","model":"claude-sonnet-4-5","content":[{"type":"text","text":"hello"}],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":1}}"#;
    anthropic.answer(Answer::raw(answer));
    // The client names no version; Anthropic refuses a call without one.
    let resp = post(
        gw.url(MESSAGES),
        &[("x-api-key", "sk-ant-client")],
        request.clone(),
    )
    .await;

    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), answer);
    let got = &anthropic.requests()[0];
    assert_eq!(got.raw, request.as_bytes());
    assert_eq!(got.header("x-api-key"), Some("sk-ant-client"));
    assert_eq!(got.header("anthropic-version"), Some("2023-06-01"));
}

// 3. Translation: each client format to the other provider.

#[tokio::test]
async fn an_openai_client_gets_an_anthropic_answer_in_openai_shape() {
    let (openai, anthropic) = (FakeOpenAi::start().await, FakeAnthropic::start().await);
    let gw = Gateway::start(&both(&openai.base_url(), &anthropic.base_url())).await;

    anthropic.answer(Answer::text("hello"));
    let resp = post(
        gw.url(CHAT),
        &[OPENAI_KEY],
        format!(
            r#"{{"model":"{CLAUDE}","max_tokens":32,"messages":[{{"role":"system","content":"be brief"}},{{"role":"user","content":"hi"}}]}}"#
        ),
    )
    .await;

    let status = resp.status();
    let text = resp.text().await.unwrap();
    assert_eq!(status, 200, "{text}");
    let body = json(&text);
    assert_eq!(body["object"], "chat.completion");
    assert_eq!(body["choices"][0]["message"]["role"], "assistant");
    assert_eq!(body["choices"][0]["message"]["content"], "hello");
    assert_eq!(body["choices"][0]["finish_reason"], "stop");
    assert!(openai.requests().is_empty());
    let got = &anthropic.requests()[0];
    assert_eq!(got.header("x-api-key"), Some("sk-gw-anthropic"));
    assert_eq!(got.header("anthropic-version"), Some("2023-06-01"));
    let got = &got.body;
    assert_eq!(got["model"], CLAUDE);
    assert_eq!(got["max_tokens"], 32);
    assert_eq!(got["system"], "be brief");
    assert_eq!(got["messages"][0]["role"], "user");
}

#[tokio::test]
async fn an_openai_client_gets_an_anthropic_stream_in_openai_shape() {
    let (openai, anthropic) = (FakeOpenAi::start().await, FakeAnthropic::start().await);
    let gw = Gateway::start(&both(&openai.base_url(), &anthropic.base_url())).await;

    anthropic.answer(Answer::text("hel lo").pace(20));
    let resp = post(
        gw.url(CHAT),
        &[],
        format!(
            r#"{{"model":"{CLAUDE}","stream":true,"messages":[{{"role":"user","content":"hi"}}]}}"#
        ),
    )
    .await;

    assert_eq!(resp.status(), 200);
    assert_eq!(
        header(resp.headers(), "content-type"),
        Some("text/event-stream")
    );
    let (body, ended) = read_all(resp).await;
    assert_eq!(ended, Ok(()));
    assert_eq!(openai_stream_text(&body), ("hel lo".into(), true), "{body}");
    let got = &anthropic.requests()[0];
    assert_eq!(got.path, MESSAGES);
    assert_eq!(got.body["stream"], true);
    assert!(got.body["max_tokens"].is_u64(), "required: {}", got.body);
}

#[tokio::test]
async fn an_anthropic_client_gets_an_openai_stream_in_anthropic_shape() {
    let (openai, anthropic) = (FakeOpenAi::start().await, FakeAnthropic::start().await);
    let gw = Gateway::start(&both(&openai.base_url(), &anthropic.base_url())).await;

    openai.answer(Answer::text("hel lo").pace(20));
    let resp = post(
        gw.url(MESSAGES),
        &[],
        r#"{"model":"o3","max_tokens":16,"stream":true,"messages":[{"role":"user","content":"hi"}]}"#,
    )
    .await;

    let status = resp.status();
    let (body, ended) = read_all(resp).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(ended, Ok(()));
    let mut text = String::new();
    let mut events = Vec::new();
    for data in body.lines().filter_map(|l| l.strip_prefix("data: ")) {
        let v = json(data);
        if let Some(t) = v["delta"]["text"].as_str() {
            text.push_str(t);
        }
        events.push(v["type"].as_str().unwrap().to_owned());
    }
    assert_eq!(text, "hel lo", "{body}");
    assert_eq!(
        events.first().map(String::as_str),
        Some("message_start"),
        "{body}"
    );
    assert_eq!(
        events.last().map(String::as_str),
        Some("message_stop"),
        "{body}"
    );
    let got = &openai.requests()[0];
    assert_eq!(got.path, CHAT);
    assert_eq!(got.header("authorization"), Some("Bearer sk-gw-openai"));
    // o3 refuses `max_tokens`.
    assert_eq!(got.body["stream"], true);
    assert_eq!(got.body["max_completion_tokens"], 16, "{}", got.body);
}

// A client's key is for a provider of its own format alone (use case 12,
// a relay); no client header crosses a translation.
#[tokio::test]
async fn an_openai_clients_key_does_not_reach_an_anthropic_provider_without_one() {
    let (openai, anthropic) = (FakeOpenAi::start().await, FakeAnthropic::start().await);
    let config = format!(
        "providers:
  openai: {{ format: openai-chat, base_url: '{}' }}
  anthropic: {{ format: anthropic-messages, base_url: '{}' }}
serve: [openai-chat]
plugins:
  - name: models
    models: [{{ prefix: claude, provider: anthropic }}]
target: openai
",
        openai.base_url(),
        anthropic.base_url()
    );
    let gw = Gateway::start(&config).await;

    let resp = post(gw.url(CHAT), &[OPENAI_KEY], claude_request("")).await;

    // The gateway sends the call without a key; Anthropic refuses it, and
    // the client gets that refusal in its own format.
    assert_eq!(resp.status(), 401);
    let body = json(&resp.text().await.unwrap());
    assert_eq!(
        body["error"]["message"], "x-api-key header is required",
        "{body}"
    );
    assert!(
        body.get("type").is_none(),
        "not Anthropic's envelope: {body}"
    );
    assert!(openai.requests().is_empty());
    let got = &anthropic.requests()[0];
    for (name, value) in &got.headers {
        let value = value.to_str().unwrap_or_default();
        assert!(
            !value.contains("sk-client"),
            "{name} carries the client's key"
        );
    }
    assert!(got.header("authorization").is_none());
    assert!(got.header("x-api-key").is_none());
    assert!(!got.query.contains("sk-client"));
}

// 4. Errors.

#[tokio::test]
async fn a_relayed_429_keeps_its_retry_after_and_body() {
    let openai = FakeOpenAi::start().await;
    let gw = Gateway::start(&one_provider("openai-chat", &openai.base_url())).await;

    openai.answer(Answer::error(429).retry_after(7));
    let resp = post(gw.url(CHAT), &[OPENAI_KEY], OPENAI_REQUEST).await;

    assert_eq!(resp.status(), 429);
    assert_eq!(header(resp.headers(), "retry-after"), Some("7"));
    assert_eq!(
        resp.text().await.unwrap(),
        r#"{"error":{"message":"Rate limit reached for requests.","type":"requests","param":null,"code":"rate_limit_exceeded"}}"#
    );
}

#[tokio::test]
async fn a_translated_429_reaches_an_openai_client_in_its_format() {
    let (openai, anthropic) = (FakeOpenAi::start().await, FakeAnthropic::start().await);
    let gw = Gateway::start(&both(&openai.base_url(), &anthropic.base_url())).await;

    anthropic.answer(Answer::error(429).retry_after(12));
    let resp = post(gw.url(CHAT), &[], claude_request("")).await;

    assert_eq!(resp.status(), 429);
    assert_eq!(header(resp.headers(), "retry-after"), Some("12"));
    let body = json(&resp.text().await.unwrap());
    assert_eq!(
        body["error"]["message"],
        "Number of request tokens has exceeded your per-minute rate limit.",
        "{body}"
    );
    assert!(body["error"]["code"].is_string(), "{body}");
    assert!(
        body.get("type").is_none(),
        "not Anthropic's envelope: {body}"
    );
}

#[tokio::test]
async fn a_translated_500_reaches_an_openai_client_in_its_format() {
    let (openai, anthropic) = (FakeOpenAi::start().await, FakeAnthropic::start().await);
    let gw = Gateway::start(&both(&openai.base_url(), &anthropic.base_url())).await;

    anthropic.answer(Answer::error(500));
    let resp = post(gw.url(CHAT), &[], claude_request("")).await;

    assert_eq!(resp.status(), 500);
    let body = json(&resp.text().await.unwrap());
    assert_eq!(body["error"]["message"], "Internal server error", "{body}");
    assert!(
        body.get("type").is_none(),
        "not Anthropic's envelope: {body}"
    );
}

#[tokio::test]
async fn a_relayed_529_passes_through_byte_exact() {
    let anthropic = FakeAnthropic::start().await;
    let gw = Gateway::start(&one_provider("anthropic-messages", &anthropic.base_url())).await;

    anthropic.answer(Answer::error(529).message("Overloaded, try later"));
    let resp = post(gw.url(MESSAGES), &ANTHROPIC_KEY, claude_request("")).await;

    assert_eq!(resp.status(), 529);
    // The fake numbers its requests across the process; the header names it.
    let id = header(resp.headers(), "request-id").unwrap().to_owned();
    assert_eq!(
        resp.text().await.unwrap(),
        format!(
            r#"{{"type":"error","error":{{"type":"overloaded_error","message":"Overloaded, try later"}},"request_id":"{id}"}}"#
        )
    );
}

#[tokio::test]
async fn a_request_the_provider_refuses_is_relayed_as_it_refused() {
    // The gateway does not check what it relays unread: the provider does.
    let openai = FakeOpenAi::start().await;
    let gw = Gateway::start(&one_provider("openai-chat", &openai.base_url())).await;
    let request = openai_request(r#","x_new_field":1"#);

    let resp = post(gw.url(CHAT), &[OPENAI_KEY], request.clone()).await;

    assert_eq!(resp.status(), 400);
    assert_eq!(
        resp.text().await.unwrap(),
        r#"{"error":{"message":"Unrecognized request argument supplied: x_new_field","type":"invalid_request_error","param":null,"code":"unrecognized_request_argument"}}"#
    );
    assert_eq!(openai.requests()[0].raw, request.as_bytes());
}

#[tokio::test]
async fn an_unreachable_provider_is_a_502_in_the_clients_format() {
    let nowhere = closed_port().await;
    let gw = Gateway::start(&both(&nowhere, &nowhere)).await;

    let openai = post(gw.url(CHAT), &[], OPENAI_REQUEST).await;
    assert_eq!(openai.status(), 502);
    let body = json(&openai.text().await.unwrap());
    assert_eq!(body["error"]["code"], "upstream", "{body}");

    let anthropic = post(gw.url(MESSAGES), &[], claude_request("")).await;
    assert_eq!(anthropic.status(), 502);
    let body = json(&anthropic.text().await.unwrap());
    assert_eq!(body["type"], "error", "{body}");
    assert!(body["error"]["message"].is_string(), "{body}");
}

#[tokio::test]
async fn a_relayed_stream_cut_mid_body_ends_truncated_without_a_hang() {
    let openai = FakeOpenAi::start().await;
    let gw = Gateway::start(&one_provider("openai-chat", &openai.base_url())).await;
    let request = openai_request(r#","stream":true"#);
    // The role chunk and "hel", then the connection drops.
    openai.answer(script(&OPENAI_STREAM).cut_after(2));
    let resp = post(gw.url(CHAT), &[OPENAI_KEY], request).await;

    assert_eq!(resp.status(), 200);
    let (body, ended) = read_all(resp).await;
    // The client's stream ends cleanly with an error event in its format.
    assert_eq!(ended, Ok(()));
    let tail = body
        .strip_prefix(&sse(&OPENAI_STREAM[..2]))
        .unwrap_or_else(|| panic!("{body}"));
    let data = tail.lines().find_map(|l| l.strip_prefix("data: ")).unwrap();
    let error = &json(data)["error"];
    assert_eq!(error["code"], "upstream", "{tail}");
    assert_eq!(error["message"], "the provider's body broke off", "{tail}");
}

#[tokio::test]
async fn an_anthropic_stream_cut_after_content_ends_with_an_anthropic_error_event() {
    let anthropic = FakeAnthropic::start().await;
    let gw = Gateway::start(&one_provider("anthropic-messages", &anthropic.base_url())).await;
    let request = claude_request(r#","stream":true"#);

    // message_start, content_block_start, ping, "hel", then a drop.
    anthropic.answer(script(&ANTHROPIC_STREAM).cut_after(4));
    let resp = post(gw.url(MESSAGES), &ANTHROPIC_KEY, request).await;

    assert_eq!(resp.status(), 200);
    let (body, ended) = read_all(resp).await;
    assert_eq!(ended, Ok(()));
    let tail = body
        .strip_prefix(&sse(&ANTHROPIC_STREAM[..4]))
        .unwrap_or_else(|| panic!("{body}"));
    assert!(tail.starts_with("event: error\n"), "{tail}");
    let data = tail.lines().find_map(|l| l.strip_prefix("data: ")).unwrap();
    let v = json(data);
    assert_eq!(v["type"], "error", "{tail}");
    assert_eq!(v["error"]["type"], "upstream", "{tail}");
}

#[tokio::test]
async fn an_anthropic_stream_cut_before_content_is_refused_502() {
    // The start is held until the first content, so a failure before any
    // can still be refused with a status. Anthropic pings before content.
    let anthropic = FakeAnthropic::start().await;
    let gw = Gateway::start(&one_provider("anthropic-messages", &anthropic.base_url())).await;

    anthropic.answer(Answer::text("hel lo").cut_after(3));
    let resp = post(
        gw.url(MESSAGES),
        &ANTHROPIC_KEY,
        claude_request(r#","stream":true"#),
    )
    .await;

    assert_eq!(resp.status(), 502);
    assert_eq!(
        resp.text().await.unwrap(),
        r#"{"type":"error","error":{"type":"upstream","message":"the provider's body broke off"}}"#
    );
}

#[tokio::test]
async fn a_stream_that_ends_before_done_ends_truncated() {
    let openai = FakeOpenAi::start().await;
    let gw = Gateway::start(&one_provider("openai-chat", &openai.base_url())).await;
    let chunk = json!({
        "id": "c1", "object": "chat.completion.chunk", "created": 1, "model": GPT,
        "choices": [{ "index": 0, "delta": { "role": "assistant", "content": "hel" }, "finish_reason": null }]
    });

    // One chunk, then the body ends, cleanly, without `[DONE]`.
    openai.answer(Answer::events(vec![chunk.clone()]));
    let resp = post(
        gw.url(CHAT),
        &[OPENAI_KEY],
        openai_request(r#","stream":true"#),
    )
    .await;

    assert_eq!(resp.status(), 200);
    let (body, ended) = read_all(resp).await;
    assert_eq!(ended, Ok(()));
    let tail = body
        .strip_prefix(&format!("data: {chunk}\n\n"))
        .unwrap_or_else(|| panic!("{body}"));
    let data = tail.lines().find_map(|l| l.strip_prefix("data: ")).unwrap();
    assert_eq!(json(data)["error"]["code"], "truncated", "{tail}");
}

#[tokio::test]
async fn a_translated_stream_cut_mid_body_ends_truncated_without_a_hang() {
    let (openai, anthropic) = (FakeOpenAi::start().await, FakeAnthropic::start().await);
    let gw = Gateway::start(&both(&openai.base_url(), &anthropic.base_url())).await;

    anthropic.answer(Answer::text("hel lo").cut_after(4));
    let resp = post(
        gw.url(CHAT),
        &[],
        format!(
            r#"{{"model":"{CLAUDE}","stream":true,"messages":[{{"role":"user","content":"hi"}}]}}"#
        ),
    )
    .await;

    assert_eq!(resp.status(), 200);
    let (body, ended) = read_all(resp).await;
    assert_eq!(ended, Ok(()));
    assert_eq!(openai_stream_text(&body), ("hel".into(), false), "{body}");
    assert!(body.ends_with(
        "data: {\"error\":{\"message\":\"the provider's body broke off\",\"type\":\"upstream\",\"param\":null,\"code\":\"upstream\"}}\n\n"
    ), "{body}");
}

#[tokio::test]
async fn a_slow_first_byte_is_waited_for() {
    let openai = FakeOpenAi::start().await;
    let gw = Gateway::start(&one_provider("openai-chat", &openai.base_url())).await;
    let request = openai_request(r#","stream":true"#);

    openai.answer(script(&OPENAI_STREAM).delay_first(700));
    let resp = post(gw.url(CHAT), &[OPENAI_KEY], request).await;

    assert_eq!(resp.status(), 200);
    let (body, ended) = read_all(resp).await;
    assert_eq!(ended, Ok(()));
    assert_eq!(body, sse(&OPENAI_STREAM));
}

// 5. Model routing (the binary has no fallback; see README).

#[tokio::test]
async fn models_route_by_prefix_and_the_rest_go_to_the_target() {
    let (openai, anthropic) = (FakeOpenAi::start().await, FakeAnthropic::start().await);
    let gw = Gateway::start(&both(&openai.base_url(), &anthropic.base_url())).await;

    // Each client format to each provider: two relays, two translations.
    let r = post(gw.url(CHAT), &[], OPENAI_REQUEST).await;
    let text = r.text().await.unwrap();
    assert_eq!(json(&text)["object"], "chat.completion", "{text}");
    let r = post(gw.url(MESSAGES), &[], claude_request("")).await;
    let text = r.text().await.unwrap();
    assert_eq!(json(&text)["type"], "message", "{text}");
    let r = post(gw.url(CHAT), &[], claude_request("")).await;
    let text = r.text().await.unwrap();
    assert_eq!(json(&text)["object"], "chat.completion", "{text}");
    let r = post(
        gw.url(MESSAGES),
        &[],
        format!(
            r#"{{"model":"{GPT}","max_tokens":8,"messages":[{{"role":"user","content":"hi"}}]}}"#
        ),
    )
    .await;
    let text = r.text().await.unwrap();
    let body = json(&text);
    assert_eq!(body["type"], "message", "{text}");
    assert_eq!(body["content"][0]["text"], "Hi there");

    let paths = |r: Vec<Request>| r.into_iter().map(|r| r.path).collect::<Vec<_>>();
    assert_eq!(paths(openai.requests()), [CHAT, CHAT]);
    assert_eq!(paths(anthropic.requests()), [MESSAGES, MESSAGES]);
}

// 6. Valid requests a translation makes invalid: the provider refuses what
// the gateway sends. Each is a gateway bug, filed in target/review/BUGS.md.

/// Sends an OpenAI `request` for Claude through a translation, and asserts
/// that Anthropic, if called, accepted the call.
async fn anthropic_accepts_the_translation_of(request: &str) {
    let (openai, anthropic) = (FakeOpenAi::start().await, FakeAnthropic::start().await);
    let gw = Gateway::start(&both(&openai.base_url(), &anthropic.base_url())).await;

    let resp = post(gw.url(CHAT), &[], request).await;

    let status = resp.status();
    let text = resp.text().await.unwrap();
    let sent = anthropic.requests();
    assert!(
        status == 200 || sent.is_empty(),
        "Anthropic refused {}: {status} {text}",
        sent[0].body
    );
}

#[tokio::test]
async fn an_openai_temperature_above_1_is_not_sent_to_anthropic_as_it_is() {
    // OpenAI takes 0..=2; Anthropic 0..=1.
    anthropic_accepts_the_translation_of(&format!(
        r#"{{"model":"{CLAUDE}","temperature":1.5,"messages":[{{"role":"user","content":"hi"}}]}}"#
    ))
    .await;
}

#[tokio::test]
async fn an_empty_openai_assistant_turn_is_not_sent_to_anthropic() {
    anthropic_accepts_the_translation_of(&format!(
        r#"{{"model":"{CLAUDE}","messages":[{{"role":"user","content":"hi"}},{{"role":"assistant","content":""}},{{"role":"user","content":"again"}}]}}"#
    ))
    .await;
}

#[tokio::test]
async fn a_system_only_openai_request_is_not_sent_to_anthropic_without_messages() {
    // Anthropic refuses `messages: []`.
    anthropic_accepts_the_translation_of(&format!(
        r#"{{"model":"{CLAUDE}","messages":[{{"role":"system","content":"hi"}}]}}"#
    ))
    .await;
}

// 7. The server's own behaviour.

#[tokio::test]
async fn an_unknown_path_is_a_404() {
    let openai = FakeOpenAi::start().await;
    let gw = Gateway::start(&one_provider("openai-chat", &openai.base_url())).await;

    for (verb, path) in [
        (reqwest::Method::POST, "/v1/nope"),
        (reqwest::Method::GET, CHAT),
        (reqwest::Method::GET, "/"),
    ] {
        let resp = client()
            .request(verb.clone(), gw.url(path))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 404, "{verb} {path}");
        assert_eq!(
            header(resp.headers(), "content-type"),
            Some("text/plain; charset=utf-8"),
            "{verb} {path}"
        );
        assert!(!resp.text().await.unwrap().is_empty());
    }
    assert!(openai.requests().is_empty());
}

#[tokio::test]
async fn a_body_over_32_mib_is_refused_413_in_the_clients_format() {
    let openai = FakeOpenAi::start().await;
    let gw = Gateway::start(&one_provider("openai-chat", &openai.base_url())).await;
    let mut big = format!(r#"{{"model":"{GPT}","messages":[{{"role":"user","content":""#);
    big.push_str(&"x".repeat(32 * 1024 * 1024));
    big.push_str("\"}]}");

    let resp = post(gw.url(CHAT), &[OPENAI_KEY], big).await;

    assert_eq!(resp.status(), 413);
    let body = json(&resp.text().await.unwrap());
    assert_eq!(body["error"]["code"], "too_large", "{body}");
    assert!(openai.requests().is_empty());
}

#[tokio::test]
async fn cookies_and_forwarding_headers_do_not_reach_the_provider() {
    let openai = FakeOpenAi::start().await;
    let gw = Gateway::start(&one_provider("openai-chat", &openai.base_url())).await;

    let resp = post(
        gw.url(CHAT),
        &[
            OPENAI_KEY,
            ("cookie", "session=gateway-origin"),
            ("forwarded", "for=10.0.0.1"),
            ("via", "1.1 proxy"),
            ("x-forwarded-for", "10.0.0.1"),
            ("x-forwarded-host", "gw.example"),
            ("x-real-ip", "10.0.0.1"),
            ("x-custom", "kept"),
        ],
        OPENAI_REQUEST,
    )
    .await;

    assert_eq!(resp.status(), 200);
    let got = &openai.requests()[0];
    for gone in [
        "cookie",
        "forwarded",
        "via",
        "x-forwarded-for",
        "x-forwarded-host",
        "x-real-ip",
    ] {
        assert!(got.header(gone).is_none(), "{gone} reached the provider");
    }
    assert_eq!(got.header("x-custom"), Some("kept"));
}

#[tokio::test]
async fn set_cookie_and_hsts_do_not_reach_the_client() {
    // A provider behind a CDN that sets cookies, HSTS and Alt-Svc.
    let openai = FakeOpenAi::start().await;
    let gw = Gateway::start(&one_provider("openai-chat", &openai.base_url())).await;

    openai.answer(
        Answer::text("hello")
            .header("set-cookie", "p=1")
            .header("strict-transport-security", "max-age=1")
            .header("alt-svc", "h3=\":443\""),
    );
    let resp = post(gw.url(CHAT), &[OPENAI_KEY], OPENAI_REQUEST).await;

    assert_eq!(resp.status(), 200);
    for gone in ["set-cookie", "strict-transport-security", "alt-svc"] {
        assert!(
            resp.headers().get(gone).is_none(),
            "{gone} reached the client"
        );
    }
}

#[cfg(unix)]
#[tokio::test]
async fn sigterm_finishes_an_open_stream_then_exits() {
    let openai = FakeOpenAi::start().await;
    let mut gw = Gateway::start(&one_provider("openai-chat", &openai.base_url())).await;

    // Five events 200 ms apart: the role chunk, "a", " b", the finish, [DONE].
    openai.answer(Answer::text("a b").pace(200));
    let mut resp = post(
        gw.url(CHAT),
        &[OPENAI_KEY],
        openai_request(r#","stream":true"#),
    )
    .await;
    assert_eq!(resp.status(), 200);
    // The stream is open: its first chunk arrived.
    let first = tokio::time::timeout(support::BOUND, resp.chunk())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let pid = gw.child.id().unwrap().to_string();
    let killed = std::process::Command::new("kill")
        .args(["-TERM", &pid])
        .status()
        .unwrap();
    assert!(killed.success());

    let (rest, ended) = read_all(resp).await;
    assert_eq!(ended, Ok(()));
    let body = format!("{}{rest}", String::from_utf8_lossy(&first));
    assert_eq!(openai_stream_text(&body), ("a b".into(), true), "{body}");
    let status = tokio::time::timeout(support::BOUND, gw.child.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(status.success(), "{status}");
    // Nothing listens any more.
    assert!(client().get(gw.url("/")).send().await.is_err());
}

// The binary's command line.

#[test]
fn check_says_ok_for_a_good_config_and_fails_a_bad_one() {
    let bin = env!("CARGO_BIN_EXE_respan-gateway");
    let good = support::write_config(&one_provider("openai-chat", "http://127.0.0.1:1/v1"));
    let out = std::process::Command::new(bin)
        .args(["--config", good.to_str().unwrap(), "check"])
        .output()
        .unwrap();
    assert!(out.status.success(), "{out:?}");
    assert!(String::from_utf8_lossy(&out.stdout).ends_with(": ok\n"));

    let bad = support::write_config("serve: [nope]\n");
    let out = std::process::Command::new(bin)
        .args(["--config", bad.to_str().unwrap(), "check"])
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("unknown format 'nope'"));
}
