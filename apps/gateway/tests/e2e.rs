//! End-to-end tests of the shipped binary.
//!
//! Each test starts `respan-gateway` itself (`CARGO_BIN_EXE_respan-gateway`)
//! on a free port, with a temp TOML config whose providers are `mockllm`
//! fakes on localhost: `FakeOpenAi`, `FakeAnthropic` and `FakeGemini` check
//! each request by the provider's own rules and answer in its exact
//! framing, streams, sessions and breaks included. Clients speak real HTTP
//! and WebSocket to the real axum server.
//!
//! A request is written again by its format, which round-trips one of its
//! own: these tests check it says what the client's did, with its spec's
//! headers. Responses are re-encoded, so they are checked by meaning: every
//! event and field the provider sent is present, in the same format, and
//! the gateway may add what the format requires.
//!
//! Run: `cargo test -p respan-gateway --test e2e`. Calls to real providers
//! are in `validation/tests/live_providers.rs`, ignored unless asked for.

#![allow(clippy::unwrap_used)]

#[allow(dead_code)]
#[path = "../../../tests/support/realtime/mod.rs"]
mod realtime;
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
        "providers = {{ p = {{ formats = \"{format}\", base_url = \"{base_url}\" }} }}\nformats = [\"openai-chat\", \"anthropic-messages\"]\nplugins = {{ routing = {{ default = \"p\" }} }}\n"
    )
}

/// OpenAI and Anthropic providers with keys of their own, Claude models
/// routed to Anthropic, the rest to OpenAI, as in `gateway.example.toml`.
fn both(openai: &str, anthropic: &str) -> String {
    format!(
        "providers = {{ openai = {{ formats = \"openai-chat\", base_url = \"{openai}\", auth = {{ type = \"bearer\", key = \"sk-gw-openai\" }} }}, anthropic = {{ formats = \"anthropic-messages\", base_url = \"{anthropic}\", auth = {{ type = \"header\", name = \"x-api-key\", key = \"sk-gw-anthropic\" }} }} }}\nformats = [\"openai-chat\", \"anthropic-messages\"]\nplugins = {{ routing = {{ default = \"openai\" }} }}\n[routes.\"claude*\"]\nto = \"anthropic\"\n"
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

/// Whether `actual` holds all of `expected`: the same values, with objects
/// allowed more fields (a re-encoded response adds what its format requires).
fn holds(actual: &Value, expected: &Value) -> bool {
    match (actual, expected) {
        (Value::Object(a), Value::Object(e)) => {
            e.iter().all(|(k, v)| a.get(k).is_some_and(|x| holds(x, v)))
        }
        (Value::Array(a), Value::Array(e)) => {
            a.len() == e.len() && a.iter().zip(e).all(|(x, v)| holds(x, v))
        }
        _ => actual == expected,
    }
}

/// The data of each event of an SSE body: JSON, or the string `[DONE]`.
fn frames(body: &str) -> Vec<Value> {
    body.split("\n\n")
        .filter(|frame| !frame.is_empty())
        .map(|frame| {
            let data = frame
                .lines()
                .find_map(|l| l.strip_prefix("data: "))
                .unwrap_or_else(|| panic!("no data in {frame:?}"));
            serde_json::from_str(data).unwrap_or_else(|_| json!(data))
        })
        .collect()
}

/// `body` starts with the events `sent` (the provider's), each as it means.
/// Returns the events after them.
fn assert_relays_events(body: &str, sent: &[&str]) -> Vec<Value> {
    let got = frames(body);
    let want = frames(&sse(sent));
    assert!(got.len() >= want.len(), "too few events in {body}");
    for (got, want) in got.iter().zip(&want) {
        assert!(holds(got, want), "{got} does not hold {want}\n{body}");
    }
    got[want.len()..].to_vec()
}

/// `body` is exactly the events `sent`, each as it means.
fn assert_relays_all(body: &str, sent: &[&str]) {
    let rest = assert_relays_events(body, sent);
    assert!(rest.is_empty(), "extra events {rest:?} in {body}");
}

// 1. OpenAI chat relay (use case 12).

#[tokio::test]
async fn openai_whole_relay_sends_the_request_byte_exact_and_passes_the_clients_key() {
    let openai = FakeOpenAi::start().await;
    let gw = Gateway::run(&[
        "proxy",
        "--listen",
        "127.0.0.1:0",
        "--openai",
        "--url",
        &openai.base_url(),
    ])
    .await;
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
    let text = resp.text().await.unwrap();
    assert!(holds(&json(&text), &json(answer)), "{text}");
    let got = &openai.requests()[0];
    assert_eq!(got.path, CHAT);
    assert_eq!(
        got.body,
        json(OPENAI_REQUEST),
        "the client's request, round-tripped"
    );
    assert_eq!(got.header("authorization"), Some("Bearer sk-client"));
    assert_eq!(got.header("openai-beta"), Some("assistants=v9"));
}

#[tokio::test]
async fn openai_stream_relay_sends_the_request_byte_exact_and_passes_the_clients_key() {
    let openai = FakeOpenAi::start().await;
    let gw = Gateway::run(&[
        "proxy",
        "--listen",
        "127.0.0.1:0",
        "--openai",
        "--url",
        &openai.base_url(),
    ])
    .await;
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
    assert_eq!(
        got.body,
        json(&request),
        "the client's request, round-tripped"
    );
    assert_eq!(got.header("authorization"), Some("Bearer sk-client"));
    assert_relays_all(&body, &OPENAI_STREAM);
}

#[tokio::test]
async fn a_proxy_asks_for_usage_its_client_never_sees() {
    let openai = FakeOpenAi::start().await;
    let gw = Gateway::run(&[
        "proxy",
        "--listen",
        "127.0.0.1:0",
        "--openai",
        "--url",
        &openai.base_url(),
    ])
    .await;
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
    // The gateway asks for usage, to log it; the client did not.
    let seen = body_of(&openai, 0);
    assert_eq!(seen["stream_options"]["include_usage"], true);
    assert_eq!(seen["metadata"]["kept"], "yes");
    // What the provider sends a client that did not ask for usage.
    assert_relays_all(&body, &OPENAI_STREAM);
    assert!(
        frames(&body).iter().all(|f| f.get("usage").is_none()),
        "the usage is hidden: {body}"
    );
}

fn body_of<P: Provider>(mock: &Mock<P>, i: usize) -> Value {
    mock.requests()[i].body.clone()
}

#[tokio::test]
async fn a_provider_with_its_own_key_replaces_the_clients() {
    let openai = FakeOpenAi::start().await;
    let config = format!(
        "providers = {{ p = {{ formats = \"openai-chat\", base_url = \"{}\", auth = {{ type = \"bearer\", key = \"sk-own\" }} }} }}\nformats = [\"openai-chat\"]\nplugins = {{ routing = {{ default = \"p\" }} }}\n",
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
async fn anthropic_stream_relay_sends_the_request_byte_exact_and_passes_the_clients_key() {
    let anthropic = FakeAnthropic::start().await;
    let gw = Gateway::run(&[
        "proxy",
        "--listen",
        "127.0.0.1:0",
        "--anthropic",
        "--url",
        &anthropic.base_url(),
    ])
    .await;
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
    assert_eq!(
        got.body,
        json(&request),
        "the client's request, round-tripped"
    );
    assert_eq!(got.header("x-api-key"), Some("sk-ant-client"));
    assert_eq!(got.header("anthropic-version"), Some("2023-06-01"));
    assert_eq!(
        got.header("anthropic-beta"),
        Some("some-future-beta-2099-01-01")
    );
    assert_relays_all(&body, &ANTHROPIC_STREAM);
}

#[tokio::test]
async fn anthropic_whole_relay_sends_the_request_byte_exact_with_the_clients_own_version() {
    let anthropic = FakeAnthropic::start().await;
    let gw = Gateway::run(&[
        "proxy",
        "--listen",
        "127.0.0.1:0",
        "--anthropic",
        "--url",
        &anthropic.base_url(),
    ])
    .await;
    let request = claude_request(r#","metadata":{"user_id":"u1"}"#);

    let answer = r#"{"id":"msg_1", "type":"message","role":"assistant","model":"claude-sonnet-4-5","content":[{"type":"text","text":"hello"}],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":1}}"#;
    anthropic.answer(Answer::raw(answer));
    // A call of the client's own format carries its spec's headers, its
    // version too.
    let resp = post(
        gw.url(MESSAGES),
        &[
            ("x-api-key", "sk-ant-client"),
            ("anthropic-version", "2023-06-01"),
        ],
        request.clone(),
    )
    .await;

    assert_eq!(resp.status(), 200);
    let text = resp.text().await.unwrap();
    assert!(holds(&json(&text), &json(answer)), "{text}");
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
    assert!(got.body["max_tokens"].is_u64(), "required = {  }", got.body);
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
        "providers = {{ openai = {{ formats = \"openai-chat\", base_url = \"{}\" }}, anthropic = {{ formats = \"anthropic-messages\", base_url = \"{}\" }} }}\nformats = [\"openai-chat\"]\nplugins = {{ routing = {{ default = \"openai\" }} }}\n[routes.\"claude*\"]\nto = \"anthropic\"\n",
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
    let gw = Gateway::run(&[
        "proxy",
        "--listen",
        "127.0.0.1:0",
        "--openai",
        "--url",
        &openai.base_url(),
    ])
    .await;

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
    let gw = Gateway::run(&[
        "proxy",
        "--listen",
        "127.0.0.1:0",
        "--anthropic",
        "--url",
        &anthropic.base_url(),
    ])
    .await;

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
    let gw = Gateway::run(&[
        "proxy",
        "--listen",
        "127.0.0.1:0",
        "--openai",
        "--url",
        &openai.base_url(),
    ])
    .await;
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
    let gw = Gateway::run(&[
        "proxy",
        "--listen",
        "127.0.0.1:0",
        "--openai",
        "--url",
        &openai.base_url(),
    ])
    .await;
    let request = openai_request(r#","stream":true"#);
    // The role chunk and "hel", then the connection drops.
    openai.answer(script(&OPENAI_STREAM).cut_after(2));
    let resp = post(gw.url(CHAT), &[OPENAI_KEY], request).await;

    assert_eq!(resp.status(), 200);
    let (body, ended) = read_all(resp).await;
    // The client's stream ends cleanly with an error event in its format.
    assert_eq!(ended, Ok(()));
    let tail = assert_relays_events(&body, &OPENAI_STREAM[..2]);
    let error = &tail[0]["error"];
    assert_eq!(tail.len(), 1, "{body}");
    assert_eq!(error["code"], "upstream", "{body}");
    assert_eq!(error["message"], "the provider's body broke off", "{body}");
}

#[tokio::test]
async fn an_anthropic_stream_cut_after_content_ends_with_an_anthropic_error_event() {
    let anthropic = FakeAnthropic::start().await;
    let gw = Gateway::run(&[
        "proxy",
        "--listen",
        "127.0.0.1:0",
        "--anthropic",
        "--url",
        &anthropic.base_url(),
    ])
    .await;
    let request = claude_request(r#","stream":true"#);

    // message_start, content_block_start, ping, "hel", then a drop.
    anthropic.answer(script(&ANTHROPIC_STREAM).cut_after(4));
    let resp = post(gw.url(MESSAGES), &ANTHROPIC_KEY, request).await;

    assert_eq!(resp.status(), 200);
    let (body, ended) = read_all(resp).await;
    assert_eq!(ended, Ok(()));
    let tail = assert_relays_events(&body, &ANTHROPIC_STREAM[..4]);
    assert_eq!(tail.len(), 1, "{body}");
    assert!(body.contains("event: error\n"), "{body}");
    assert_eq!(tail[0]["type"], "error", "{body}");
    assert_eq!(tail[0]["error"]["type"], "upstream", "{body}");
}

#[tokio::test]
async fn an_anthropic_stream_cut_before_content_is_refused_502() {
    // The start is held until the first content, so a failure before any
    // can still be refused with a status. Anthropic pings before content.
    let anthropic = FakeAnthropic::start().await;
    let gw = Gateway::run(&[
        "proxy",
        "--listen",
        "127.0.0.1:0",
        "--anthropic",
        "--url",
        &anthropic.base_url(),
    ])
    .await;

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
    let gw = Gateway::run(&[
        "proxy",
        "--listen",
        "127.0.0.1:0",
        "--openai",
        "--url",
        &openai.base_url(),
    ])
    .await;
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
    // The chunk's role and text may come as two chunks; the stream is cut
    // short of `[DONE]` and ends with the error.
    assert_eq!(openai_stream_text(&body), ("hel".into(), false), "{body}");
    let last = frames(&body).pop().unwrap();
    assert_eq!(last["error"]["code"], "truncated", "{body}");
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
    let gw = Gateway::run(&[
        "proxy",
        "--listen",
        "127.0.0.1:0",
        "--openai",
        "--url",
        &openai.base_url(),
    ])
    .await;
    let request = openai_request(r#","stream":true"#);

    openai.answer(script(&OPENAI_STREAM).delay_first(700));
    let resp = post(gw.url(CHAT), &[OPENAI_KEY], request).await;

    assert_eq!(resp.status(), 200);
    let (body, ended) = read_all(resp).await;
    assert_eq!(ended, Ok(()));
    assert_relays_all(&body, &OPENAI_STREAM);
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
    // Without the web UI, which takes `/`.
    let config = one_provider("openai-chat", &openai.base_url());
    let gw = Gateway::start(&format!("ui = false\n{config}")).await;

    for (verb, path) in [
        (reqwest::Method::POST, "/v1/nope"),
        (reqwest::Method::POST, "/v1/embeddings"),
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
            Some("application/json"),
            "{verb} {path}"
        );
        let error = json(&resp.text().await.unwrap());
        assert_eq!(error["error"]["type"], "not_found_error", "{verb} {path}");
        let message = error["error"]["message"].as_str().unwrap();
        assert!(message.starts_with("no format served accepts"), "{message}");
    }
    assert!(openai.requests().is_empty());
}

#[tokio::test]
async fn a_body_over_32_mib_is_refused_413_in_the_clients_format() {
    let openai = FakeOpenAi::start().await;
    let gw = Gateway::run(&[
        "proxy",
        "--listen",
        "127.0.0.1:0",
        "--openai",
        "--url",
        &openai.base_url(),
    ])
    .await;
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
    let gw = Gateway::run(&[
        "proxy",
        "--listen",
        "127.0.0.1:0",
        "--openai",
        "--url",
        &openai.base_url(),
    ])
    .await;

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
    // Nor does a header of no spec of the format, which writes the head.
    assert_eq!(got.header("x-custom"), None);
}

#[tokio::test]
async fn set_cookie_and_hsts_do_not_reach_the_client() {
    // A provider behind a CDN that sets cookies, HSTS and Alt-Svc.
    let openai = FakeOpenAi::start().await;
    let gw = Gateway::run(&[
        "proxy",
        "--listen",
        "127.0.0.1:0",
        "--openai",
        "--url",
        &openai.base_url(),
    ])
    .await;

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
    let mut gw = Gateway::run(&[
        "proxy",
        "--listen",
        "127.0.0.1:0",
        "--openai",
        "--url",
        &openai.base_url(),
    ])
    .await;

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

#[cfg(unix)]
#[tokio::test]
async fn sigterm_cuts_a_stream_still_open_at_the_shutdown_deadline() {
    let openai = FakeOpenAi::start().await;
    let mut gw = Gateway::run(&[
        "proxy",
        "--listen",
        "127.0.0.1:0",
        "--openai",
        "--url",
        &openai.base_url(),
    ])
    .await;
    // The first event opens the response; the next is delayed beyond the
    // binary's 30-second shutdown deadline.
    openai.answer(script(&OPENAI_STREAM[1..]).pace(60_000));
    // The client must outlive the server deadline: its own timeout must not
    // masquerade as a server-induced truncation.
    let mut resp = client()
        .post(gw.url(CHAT))
        .header(OPENAI_KEY.0, OPENAI_KEY.1)
        .body(openai_request(r#", "stream":true"#))
        .timeout(std::time::Duration::from_secs(50))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let first = tokio::time::timeout(support::BOUND, resp.chunk())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!first.is_empty());

    let pid = gw.child.id().unwrap().to_string();
    let start = tokio::time::Instant::now();
    assert!(
        std::process::Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .unwrap()
            .success()
    );
    let status = tokio::time::timeout(std::time::Duration::from_secs(38), gw.child.wait())
        .await
        .expect("the binary must exit after the 30-second shutdown deadline")
        .unwrap();
    assert!(status.success(), "{status}");
    assert!(start.elapsed() >= std::time::Duration::from_secs(28));
    let mut body = first.to_vec();
    let ended = tokio::time::timeout(support::BOUND, async {
        while let Some(chunk) = resp.chunk().await? {
            body.extend_from_slice(&chunk);
        }
        Ok::<_, reqwest::Error>(())
    })
    .await
    .expect("the response must end after the server exits");
    let error = ended.expect_err("the server must cut the unfinished HTTP body");
    assert!(
        !error.is_timeout(),
        "the client timed out instead: {error:?}"
    );
    let body = String::from_utf8_lossy(&body);
    assert!(!body.contains("data: [DONE]"), "{body}");
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

    let bad = support::write_config("formats = [\"nope\"]\n");
    let out = std::process::Command::new(bin)
        .args(["--config", bad.to_str().unwrap(), "check"])
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("unknown format 'nope'"));
}

// The vendor's other endpoints, and running with no config file.

const COUNT_TOKENS: &str = "/v1/messages/count_tokens";
const RESPONSES: &str = "/v1/responses";

#[tokio::test]
async fn an_endpoint_no_format_knows_is_relayed_byte_exact_with_the_clients_key() {
    let anthropic = FakeAnthropic::start().await;
    let gw = Gateway::run(&[
        "proxy",
        "--listen",
        "127.0.0.1:0",
        "--anthropic",
        "--url",
        &anthropic.base_url(),
    ])
    .await;
    let counted = r#"{ "input_tokens":  12,"x_new":1 }"#;
    anthropic.answer(Answer::raw(counted).header("x-request-id", "req_1"));
    let asked = r#"{ "model":"claude-sonnet-4-5",  "messages":[{"role":"user","content":"hi"}] }"#;
    let headers = [
        ANTHROPIC_KEY[0],
        ANTHROPIC_KEY[1],
        ("anthropic-beta", "token-counting-2024-11-01"),
        ("content-type", "application/json"),
    ];
    let resp = post(gw.url(COUNT_TOKENS), &headers, asked).await;

    assert_eq!(resp.status(), 200);
    assert_eq!(header(resp.headers(), "x-request-id"), Some("req_1"));
    assert_eq!(resp.text().await.unwrap(), counted);
    let got = &anthropic.requests()[0];
    assert_eq!(
        (got.method.as_str(), got.path.as_str()),
        ("POST", COUNT_TOKENS)
    );
    assert_eq!(got.raw, asked.as_bytes(), "the client's bytes, unread");
    assert_eq!(got.header("x-api-key"), Some("sk-ant-client"));
    assert_eq!(
        got.header("anthropic-beta"),
        Some("token-counting-2024-11-01")
    );
}

#[tokio::test]
async fn the_chat_format_still_answers_its_own_path_before_the_catch_all() {
    let anthropic = FakeAnthropic::start().await;
    let gw = Gateway::run(&[
        "proxy",
        "--listen",
        "127.0.0.1:0",
        "--anthropic",
        "--url",
        &anthropic.base_url(),
    ])
    .await;
    anthropic.answer(script(&ANTHROPIC_STREAM));
    let headers = [ANTHROPIC_KEY[0], ANTHROPIC_KEY[1]];
    let resp = post(
        gw.url(MESSAGES),
        &headers,
        claude_request(r#","stream":true"#),
    )
    .await;
    assert_eq!(resp.status(), 200);
    assert_relays_all(&resp.text().await.unwrap(), &ANTHROPIC_STREAM);
    assert_eq!(anthropic.requests()[0].path, MESSAGES);
}

#[tokio::test]
async fn serving_passthrough_needs_a_provider_of_that_format() {
    let bin = env!("CARGO_BIN_EXE_respan-gateway");
    let config = support::write_config(
        "providers = { p = { formats = \"openai-chat\", base_url = \"http://127.0.0.1:1/v1\" } }\nformats = [\"openai-chat\", \"passthrough\"]\nplugins = { routing = { default = \"p\" } }\n",
    );
    let out = std::process::Command::new(bin)
        .args(["--config", config.to_str().unwrap(), "check"])
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("exactly one provider"));
}

#[tokio::test]
async fn proxy_needs_no_config_and_relays_claude_code_with_its_own_credentials() {
    let anthropic = FakeAnthropic::start().await;
    let gw = Gateway::run(&[
        "proxy",
        "--anthropic",
        "--url",
        &anthropic.base_url(),
        "--listen",
        "127.0.0.1:0",
    ])
    .await;
    let headers = [
        ("x-api-key", "sk-ant-own"),
        ("anthropic-version", "2023-06-01"),
        ("anthropic-beta", "interleaved-thinking-2025-05-14"),
    ];
    // Chat, streamed, in odd spacing: written again, it says the same.
    anthropic.answer(script(&ANTHROPIC_STREAM));
    let asked = r#"{ "model":"claude-sonnet-4-5","max_tokens":64, "stream":true,"messages":[{"role":"user","content":"hi"}],"thinking":{"type":"disabled"} }"#;
    let resp = post(gw.url(MESSAGES), &headers, asked).await;
    assert_eq!(resp.status(), 200);
    assert_relays_all(&resp.text().await.unwrap(), &ANTHROPIC_STREAM);
    let got = &anthropic.requests()[0];
    assert_eq!(got.body, json(asked), "the client's request, round-tripped");
    assert_eq!(got.header("x-api-key"), Some("sk-ant-own"));
    assert_eq!(
        got.header("anthropic-beta"),
        Some("interleaved-thinking-2025-05-14")
    );
    // Token counting, which no chat format serves, is relayed too.
    anthropic.answer(Answer::raw(r#"{"input_tokens":5}"#));
    let counting = r#"{"model":"claude-sonnet-4-5","messages":[{"role":"user","content":"hi"}]}"#;
    let resp = post(gw.url(COUNT_TOKENS), &headers, counting).await;
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), r#"{"input_tokens":5}"#);
    let got = &anthropic.requests()[1];
    assert_eq!(got.path, COUNT_TOKENS);
    assert_eq!(got.header("x-api-key"), Some("sk-ant-own"));
}

#[tokio::test]
async fn proxy_relays_codex_the_responses_api_stream_byte_exact() {
    let openai = FakeOpenAi::start().await;
    let gw = Gateway::run(&[
        "proxy",
        "--openai",
        "--url",
        &openai.base_url(),
        "--listen",
        "127.0.0.1:0",
    ])
    .await;
    let events = [
        "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"status\":\"in_progress\"},\"x_new\":1}",
        "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"hel\"}",
        "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"status\":\"completed\"}}",
    ];
    openai.answer(script(&events));
    let asked = r#"{ "model":"gpt-4.1-mini","input":"hi","stream":true }"#;
    let resp = post(gw.url(RESPONSES), &[OPENAI_KEY], asked).await;
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), sse(&events));
    let got = &openai.requests()[0];
    assert_eq!(got.path, RESPONSES);
    assert_eq!(got.raw, asked.as_bytes());
    assert_eq!(got.header("authorization"), Some("Bearer sk-client"));
    // Chat completions still take the chat path, to the same vendor.
    openai.answer(Answer::text("hi"));
    let resp = post(gw.url(CHAT), &[OPENAI_KEY], openai_request("")).await;
    assert_eq!(resp.status(), 200);
    assert_eq!(openai.requests()[1].path, CHAT);
}

#[test]
fn proxy_rejects_explicit_config_in_either_argument_order() {
    let config = support::write_config("formats = []\n");
    let path = config.to_str().unwrap();
    for args in [
        vec!["--config", path, "proxy", "--openai", "--listen", "invalid"],
        vec!["proxy", "--anthropic", "-c", path, "--listen", "invalid"],
    ] {
        let out = std::process::Command::new(env!("CARGO_BIN_EXE_respan-gateway"))
            .args(&args)
            .output()
            .unwrap();
        assert!(!out.status.success(), "{args:?}: {out:?}");
        let error = String::from_utf8_lossy(&out.stderr);
        assert!(error.contains("--config"), "{args:?}: {error}");
        assert!(error.contains("proxy"), "{args:?}: {error}");
    }
}

#[test]
fn proxy_without_a_vendor_or_with_a_bad_address_is_a_usage_error() {
    let bin = env!("CARGO_BIN_EXE_respan-gateway");
    for args in [
        &["proxy"][..],
        &[
            "proxy",
            "--listen",
            "127.0.0.1:0",
            "--anthropic",
            "--listen",
            "nowhere",
        ],
        &[
            "proxy",
            "--listen",
            "127.0.0.1:0",
            "--anthropic",
            "check",
            "--listen",
        ],
        &[
            "proxy",
            "--listen",
            "127.0.0.1:0",
            "--openai",
            "--check",
            "--url",
        ],
        &[
            "proxy",
            "--listen",
            "127.0.0.1:0",
            "--openai",
            "--anthropic",
        ],
        &["--config"],
        &["-c"],
    ] {
        let out = std::process::Command::new(bin).args(args).output().unwrap();
        assert!(!out.status.success(), "{args:?}");
        assert!(!out.stderr.is_empty(), "{args:?}");
    }
}

#[test]
fn proxy_url_with_a_quote_is_not_parsed_as_toml() {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_respan-gateway"))
        .args([
            "proxy",
            "--openai",
            "--check",
            "--url",
            "http://localhost/vendor's/v1",
        ])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

// Sessions over WebSocket.

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// A WebSocket session with the gateway at `path`, offering `protocols`,
/// and the subprotocol the gateway chose.
async fn session(gw: &Gateway, path: &str, protocols: &str) -> (Ws, Option<String>) {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let url = format!("{}{path}", gw.base.replace("http://", "ws://"));
    let mut req = url.into_client_request().unwrap();
    if !protocols.is_empty() {
        let offered = protocols.parse().unwrap();
        req.headers_mut().insert("sec-websocket-protocol", offered);
    }
    let (ws, resp) = tokio_tungstenite::connect_async(req).await.unwrap();
    let chosen = header(resp.headers(), "sec-websocket-protocol").map(str::to_owned);
    (ws, chosen)
}

/// Sends `event` as a text frame.
async fn say(ws: &mut Ws, event: Value) {
    use futures::SinkExt;
    let text = event.to_string().into();
    ws.send(tokio_tungstenite::tungstenite::Message::Text(text))
        .await
        .unwrap();
}

/// The next message `ws` hears, bounded: a JSON event, or the code of a
/// Close (`Err`).
async fn hear(ws: &mut Ws) -> Result<Value, Option<u16>> {
    use futures::StreamExt;
    use tokio_tungstenite::tungstenite::Message;
    let next = tokio::time::timeout(support::BOUND, ws.next()).await;
    match next.unwrap() {
        Some(Ok(Message::Text(text))) => Ok(json(&text)),
        Some(Ok(Message::Close(frame))) => Err(frame.map(|f| f.code.into())),
        other => panic!("no event or Close: {other:?}"),
    }
}

/// The events `ws` hears up to and including the first `last` says ends.
async fn until(ws: &mut Ws, last: impl Fn(&Value) -> bool) -> String {
    let mut heard = String::new();
    loop {
        let event = hear(ws).await.unwrap();
        heard.push_str(&event.to_string());
        if last(&event) {
            return heard;
        }
    }
}

#[tokio::test]
async fn sessions_from_toml_relay_with_the_gateways_keys_and_close_1001_on_sigterm() {
    let openai = FakeOpenAi::start().await;
    let gemini = mockllm::FakeGemini::start().await;
    openai.answer(Answer::text("hi there"));
    gemini.answer(Answer::text("Paris"));
    let ws = |base: String| base.replace("http://", "ws://");
    let mut gw = Gateway::start(&format!(
        "providers = {{ realtime = {{ formats = \"openai-realtime\", base_url = \"{}\", auth = {{ type = \"bearer\", key = \"sk-gw\" }} }}, live = {{ formats = \"gemini-live\", base_url = \"{}\", auth = {{ type = \"header\", name = \"x-goog-api-key\", key = \"AIza-gw\" }} }} }}\nformats = [\"openai-realtime\", \"gemini-live\"]\n",
        ws(openai.base_url()),
        ws(gemini.base_url())
    ))
    .await;

    // A browser's offer: the subprotocol, and a key, which never goes back.
    let offer = "realtime, openai-insecure-api-key.sk-client";
    let (mut realtime, chosen) = session(&gw, "/v1/realtime?model=gpt-realtime", offer).await;
    assert_eq!(chosen.as_deref(), Some("realtime"));
    assert_eq!(
        hear(&mut realtime).await.unwrap()["type"],
        "session.created"
    );
    let says = json!({"type": "conversation.item.create", "item": {"type": "message",
        "role": "user", "content": [{"type": "input_text", "text": "hi"}]}});
    say(&mut realtime, says).await;
    say(&mut realtime, json!({"type": "response.create"})).await;
    let turn = until(&mut realtime, |e| e["type"] == "response.done").await;
    assert!(turn.contains("hi there"), "{turn}");
    let upgrade = &openai.requests()[0];
    assert_eq!(upgrade.path, "/v1/realtime");
    assert_eq!(upgrade.header("authorization"), Some("Bearer sk-gw"));

    // Without its ?model=, a Gemini client would go to the realtime default.
    let live = "/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent?model=live/gemini-live";
    let (mut live, chosen) = session(&gw, live, "").await;
    assert_eq!(chosen, None);
    let setup = json!({"setup": {"model": "models/gemini-live",
        "generationConfig": {"responseModalities": ["TEXT"]}}});
    say(&mut live, setup).await;
    assert!(
        hear(&mut live)
            .await
            .unwrap()
            .get("setupComplete")
            .is_some()
    );
    say(
        &mut live,
        json!({"realtimeInput": {"text": "Capital of France?"}}),
    )
    .await;
    let turn = until(&mut live, |e| e["serverContent"]["turnComplete"] == true).await;
    assert!(turn.contains("Paris"), "{turn}");
    assert_eq!(
        gemini.requests()[0].header("x-goog-api-key"),
        Some("AIza-gw")
    );

    let pid = gw.child.id().unwrap().to_string();
    let killed = std::process::Command::new("kill")
        .args(["-TERM", &pid])
        .status()
        .unwrap();
    assert!(killed.success());
    assert_eq!(hear(&mut realtime).await, Err(Some(1001)));
    assert_eq!(hear(&mut live).await, Err(Some(1001)));
    let status = tokio::time::timeout(support::BOUND, gw.child.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(status.success(), "{status}");
}

/// An event of `kind`'s type.
fn of_type(kind: &'static str) -> impl Fn(&Value) -> bool {
    move |event| event["type"] == kind
}

/// The bytes of standard base64 `v`.
fn unbase64(v: &Value) -> Vec<u8> {
    use base64::Engine;
    let text = v.as_str().unwrap();
    base64::engine::general_purpose::STANDARD
        .decode(text)
        .unwrap()
}

/// `ms` of 24 kHz PCM, a ramp.
fn pcm(ms: usize) -> Vec<u8> {
    (0..ms * 24)
        .flat_map(|i| (i as i16).to_le_bytes())
        .collect()
}

const LIVE: &str = "/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent";

#[tokio::test]
async fn realtime_sessions_from_toml_cross_vendors_by_the_model_they_ask_for() {
    use realtime::{ToolCall, Turn};
    let openai = realtime::openai::start().await;
    let gemini = realtime::gemini::start().await;
    let http = |ws: String| ws.replace("ws://", "http://");
    let mut gw = Gateway::start(&format!(
        "providers = {{ openai = {{ vendor = \"openai\", base_url = \"{}\", key = \"sk-gw\" }}, gemini = {{ vendor = \"gemini\", base_url = \"{}/v1beta/openai\", key = \"AIza-gw\" }} }}\nformats = [\"openai-realtime\", \"gemini-live\"]\n",
        http(openai.base_url()),
        http(gemini.base_url())
    ))
    .await;

    // An OpenAI client asking for a Gemini model is answered by Gemini.
    let (mut realtime, _) = session(&gw, "/v1/realtime?model=gemini-live", "realtime").await;
    assert_eq!(
        hear(&mut realtime).await.unwrap()["type"],
        "session.created"
    );
    let update = json!({"type": "session.update", "session": {"type": "realtime",
        "instructions": "Be brief.", "audio": {"output": {"voice": "Kore"}},
        "tools": [{"type": "function", "name": "weather",
            "parameters": {"type": "object", "properties": {"city": {"type": "string"}}}}]}});
    say(&mut realtime, update).await;
    until(&mut realtime, of_type("session.updated")).await;
    let setup = &gemini.received(1).await[0]["setup"];
    assert_eq!(setup["model"], "models/gemini-live");
    assert_eq!(
        gemini.handshakes()[0].header("x-goog-api-key"),
        Some("AIza-gw")
    );
    // A voice turn.
    let append = json!({"type": "input_audio_buffer.append", "audio": base64(&pcm(100))});
    say(&mut realtime, append).await;
    gemini.received(2).await;
    gemini.answer(Turn {
        audio: Some(pcm(200)),
        transcript: Some("It is noon.".into()),
        ..Turn::default()
    });
    gemini.user_stops();
    let turn = until(&mut realtime, of_type("response.done")).await;
    assert!(turn.contains("response.output_audio.delta"), "{turn}");
    assert!(turn.contains("It is noon."), "{turn}");
    assert!(turn.contains("\"status\":\"completed\""), "{turn}");
    // A barge-in.
    gemini.answer(Turn {
        audio: Some(pcm(300)),
        held: true,
        ..Turn::default()
    });
    gemini.user_stops();
    until(&mut realtime, of_type("response.output_audio.delta")).await;
    gemini.user_speaks();
    let cut = until(&mut realtime, of_type("response.done")).await;
    assert!(cut.contains("input_audio_buffer.speech_started"), "{cut}");
    assert!(cut.contains("turn_detected"), "{cut}");
    // A tool call, and its answer.
    gemini.answer(Turn {
        tool_call: Some(ToolCall {
            name: "weather".into(),
            arguments: json!({"city": "Paris"}),
        }),
        ..Turn::default()
    });
    gemini.user_stops();
    let mut call = Value::Null;
    while call.is_null() {
        let event = hear(&mut realtime).await.unwrap();
        if event["type"] == "response.function_call_arguments.done" {
            call = event;
        }
    }
    assert_eq!(call["name"], "weather");
    until(&mut realtime, of_type("response.done")).await;
    gemini.answer(Turn {
        audio: Some(pcm(100)),
        ..Turn::default()
    });
    let answer = json!({"type": "conversation.item.create", "item": {"type": "function_call_output",
        "call_id": call["call_id"], "output": "{\"celsius\":21}"}});
    say(&mut realtime, answer).await;
    say(&mut realtime, json!({"type": "response.create"})).await;
    until(&mut realtime, of_type("response.done")).await;
    let answered = gemini
        .received(3)
        .await
        .into_iter()
        .find(|m| m.get("toolResponse").is_some())
        .unwrap();
    assert_eq!(
        answered["toolResponse"]["functionResponses"][0],
        json!({"id": call["call_id"], "name": "weather", "response": {"celsius": 21}})
    );

    // A provider's own name for its model is stripped; with none, a Gemini
    // client goes to the default, OpenAI, which needs one.
    let (mut named, _) = session(&gw, &format!("{LIVE}?model=openai/gpt-realtime-2.1"), "").await;
    say(&mut named, json!({"setup": {"model": "models/x"}})).await;
    hear(&mut named).await.unwrap();
    assert_eq!(
        openai.handshakes()[0].uri,
        "/v1/realtime?model=gpt-realtime-2.1"
    );
    // The fake serves one session at a time.
    drop(named);
    let url = format!("{}{LIVE}", gw.base.replace("http://", "ws://"));
    match tokio_tungstenite::connect_async(url).await {
        Err(tokio_tungstenite::tungstenite::Error::Http(answer)) => {
            assert_eq!(answer.status(), 400)
        }
        other => panic!("expected a refusal: {other:?}"),
    }

    // A Gemini client asking for an OpenAI model is answered by OpenAI.
    let (mut live, _) = session(&gw, &format!("{LIVE}?model=gpt-realtime"), "").await;
    let setup = json!({"setup": {"model": "models/gemini-live",
        "generationConfig": {"responseModalities": ["AUDIO"]}}});
    say(&mut live, setup).await;
    assert_eq!(hear(&mut live).await.unwrap(), json!({"setupComplete": {}}));
    let upgrade = &openai.handshakes()[1];
    assert_eq!(upgrade.uri, "/v1/realtime?model=gpt-realtime");
    assert_eq!(upgrade.header("authorization"), Some("Bearer sk-gw"));
    let chunk = json!({"realtimeInput": {"audio": {"mimeType": "audio/pcm;rate=16000",
        "data": base64(&pcm(100))}}});
    say(&mut live, chunk).await;
    openai.received(2).await;
    openai.answer(Turn {
        audio: Some(pcm(200)),
        ..Turn::default()
    });
    openai.user_speaks();
    openai.user_stops();
    let mut heard = Vec::new();
    loop {
        let message = hear(&mut live).await.unwrap();
        let parts = &message["serverContent"]["modelTurn"]["parts"];
        if let Some(data) = parts[0]["inlineData"].get("data") {
            heard.extend(unbase64(data));
        }
        if message["serverContent"]["turnComplete"] == true {
            break;
        }
    }
    assert_eq!(heard, pcm(200));

    let pid = gw.child.id().unwrap().to_string();
    let killed = std::process::Command::new("kill")
        .args(["-TERM", &pid])
        .status()
        .unwrap();
    assert!(killed.success());
    assert_eq!(hear(&mut realtime).await, Err(Some(1001)));
    assert_eq!(hear(&mut live).await, Err(Some(1001)));
    let status = tokio::time::timeout(support::BOUND, gw.child.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(status.success(), "{status}");
}

fn base64(data: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(data)
}

#[tokio::test]
async fn a_realtime_session_is_one_record_that_ends_with_the_session_and_sums_its_usage() {
    use realtime::{Turn, Usage};
    let openai = realtime::openai::start().await;
    let gw = Gateway::start(&format!(
        "providers = {{ openai = {{ vendor = \"openai\", base_url = \"{}\", key = \"sk-gw\" }} }}\nformats = [\"openai-realtime\"]\n",
        openai.base_url().replace("ws://", "http://")
    ))
    .await;
    let records = || async {
        let resp = client()
            .get(gw.url("/ui/api/requests"))
            .send()
            .await
            .unwrap();
        let all = json(&resp.text().await.unwrap());
        let all = all["requests"].as_array().unwrap().clone();
        all.into_iter()
            .filter(|r| r["path"] == "/v1/realtime")
            .collect::<Vec<_>>()
    };
    let (mut ws, _) = session(&gw, "/v1/realtime?model=gpt-realtime", "realtime").await;
    until(&mut ws, of_type("session.created")).await;
    for (text, input, output) in [("one", 10, 20), ("two", 5, 7)] {
        openai.answer(Turn {
            text: Some(text.into()),
            usage: Usage {
                input_tokens: input,
                output_tokens: output,
                ..Usage::default()
            },
            ..Turn::default()
        });
        say(&mut ws, json!({"type": "response.create"})).await;
        until(&mut ws, of_type("response.done")).await;
    }
    // Still open: no end, no duration, no usage claimed from the handshake.
    let open = records().await;
    assert_eq!(open.len(), 1, "{open:?}");
    assert_eq!(open[0]["status"], 101);
    assert!(open[0]["duration_ms"].is_null(), "{open:?}");
    let before = open[0]["id"].clone();

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    ws.close(None).await.unwrap();
    let mut done = Vec::new();
    for _ in 0..500 {
        done = records().await;
        if !done[0]["duration_ms"].is_null() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(done.len(), 1, "{done:?}");
    let record = &done[0];
    assert_eq!(record["id"], before);
    assert!(record["duration_ms"].as_u64().unwrap() >= 300, "{record}");
    assert!(record["ttfb_ms"].as_u64().unwrap() < record["duration_ms"].as_u64().unwrap());
    assert_eq!(record["usage"]["input_tokens"], 15, "{record}");
    assert_eq!(record["usage"]["output_tokens"], 27, "{record}");
    assert!(record["error"].is_null(), "{record}");
}
