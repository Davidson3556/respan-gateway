//! End-to-end tests of the shipped binary.
//!
//! Each test starts `respan-gateway` itself (`CARGO_BIN_EXE_respan-gateway`)
//! on a free port, with a temp YAML config whose providers point at fake
//! providers on localhost: wiremock for whole bodies and errors, and a
//! small axum server (`support::Streamer`) for streams over time. Clients
//! speak real HTTP to the real axum server.
//!
//! Run: `cargo test -p respan-gateway --test e2e`. Calls to real providers
//! are in `tests/real_providers.rs`, ignored unless asked for.

#![allow(clippy::unwrap_used)]

mod support;

use std::time::Duration;

use support::{Gateway, Step, Streamer, client, closed_port, json, read_all, sent};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

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

/// OpenAI and Anthropic providers, Claude models routed to Anthropic, the
/// rest to OpenAI, as in `gateway.example.yaml` but without keys.
fn both(openai: &str, anthropic: &str) -> String {
    format!(
        "providers:
  openai: {{ format: openai-chat, base_url: '{openai}/v1' }}
  anthropic: {{ format: anthropic-messages, base_url: '{anthropic}' }}
serve: [openai-chat, anthropic-messages]
plugins:
  - name: models
    models: [{{ prefix: claude, provider: anthropic }}]
target: openai
"
    )
}

const OPENAI_REQUEST: &str = r#"{"model":"gpt-x","messages":[{"role":"user","content":"hi"}],"x_new_field":{"kept":[1,2,3]}}"#;
const OPENAI_WHOLE: &str = r#"{"id":"c1","object":"chat.completion","model":"gpt-x","choices":[{"index":0,"message":{"role":"assistant","content":"hello","refusal":null,"annotations":[]},"logprobs":null,"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2},"system_fingerprint":"fp_1","x_top_extra":{"a":1}}"#;

const OPENAI_STREAM: &[Step] = &[
    Step::Send(
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"hel\"}}],\"x_chunk_extra\":1}\n\n",
    ),
    Step::Wait(Duration::from_millis(50)),
    Step::Send(": keep-alive\n\n"),
    Step::Send(
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"lo\"},\"finish_reason\":\"stop\"}]}\n\n",
    ),
    Step::Wait(Duration::from_millis(50)),
    Step::Send("data: [DONE]\n\n"),
];

const ANTHROPIC_STREAM: &[Step] = &[
    Step::Send(
        "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-x\",\"content\":[],\"usage\":{\"input_tokens\":3,\"output_tokens\":1}},\"x_start_extra\":1}\n\n",
    ),
    Step::Send(
        "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
    ),
    Step::Wait(Duration::from_millis(50)),
    Step::Send("event: ping\ndata: {\"type\":\"ping\"}\n\n"),
    Step::Send(
        "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hel\"}}\n\n",
    ),
    Step::Wait(Duration::from_millis(50)),
    Step::Send(
        "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"lo\"}}\n\n",
    ),
    Step::Send(
        "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
    ),
    Step::Send(
        "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":2}}\n\n",
    ),
    Step::Send("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"),
];

const ANTHROPIC_WHOLE: &str = r#"{"id":"msg_1","type":"message","role":"assistant","model":"claude-x","content":[{"type":"text","text":"hello"}],"stop_reason":"end_turn","usage":{"input_tokens":3,"output_tokens":2}}"#;

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
    let provider = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/json")
                .insert_header("x-request-id", "req_123")
                .set_body_raw(OPENAI_WHOLE, "application/json"),
        )
        .expect(1)
        .mount(&provider)
        .await;
    let gw = Gateway::start(&one_provider(
        "openai-chat",
        &format!("{}/v1", provider.uri()),
    ))
    .await;

    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .header("authorization", "Bearer sk-client")
        .header("openai-beta", "assistants=v9")
        .header("content-type", "application/json")
        .body(OPENAI_REQUEST)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert_eq!(header(resp.headers(), "x-request-id"), Some("req_123"));
    assert_eq!(resp.text().await.unwrap(), OPENAI_WHOLE);
    let got = &provider.received_requests().await.unwrap()[0];
    assert_eq!(
        got.body,
        OPENAI_REQUEST.as_bytes(),
        "the client's bytes, unread"
    );
    let sent = |name: &str| got.headers.get(name).and_then(|v| v.to_str().ok());
    assert_eq!(sent("authorization"), Some("Bearer sk-client"));
    assert_eq!(sent("openai-beta"), Some("assistants=v9"));
}

/// A usage chunk, sent last when a request asks for it.
const OPENAI_USAGE: Step = Step::Send(
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":2,\"total_tokens\":3}}\n\n",
);

/// [`OPENAI_STREAM`] with the usage chunk before `[DONE]`.
fn openai_stream_with_usage() -> Vec<Step> {
    let mut steps = OPENAI_STREAM.to_vec();
    steps.insert(steps.len() - 1, OPENAI_USAGE);
    steps
}

#[tokio::test]
async fn openai_stream_relay_is_byte_exact_and_passes_the_clients_key() {
    let steps = openai_stream_with_usage();
    let provider = Streamer::start(steps.clone()).await;
    let gw = Gateway::start(&one_provider(
        "openai-chat",
        &format!("{}/v1", provider.base),
    ))
    .await;
    let request = r#"{"model":"gpt-x","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"hi"}],"x_new_field":1}"#;

    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .header("authorization", "Bearer sk-client")
        .body(request)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert_eq!(
        header(resp.headers(), "content-type"),
        Some("text/event-stream")
    );
    let (body, ended) = read_all(resp).await;
    assert_eq!(ended, Ok(()));
    assert_eq!(body, sent(&steps));
    let seen = &provider.seen()[0];
    assert_eq!(seen.path, "/v1/chat/completions");
    assert_eq!(seen.body, request.as_bytes());
    assert_eq!(
        seen.headers.get("authorization").unwrap(),
        "Bearer sk-client"
    );
}

#[tokio::test]
async fn usage_the_client_did_not_ask_for_is_asked_for_and_hidden() {
    let provider = Streamer::start(openai_stream_with_usage()).await;
    let gw = Gateway::start(&one_provider(
        "openai-chat",
        &format!("{}/v1", provider.base),
    ))
    .await;
    let request = r#"{"model":"gpt-x","stream":true,"messages":[{"role":"user","content":"hi"}],"x_new_field":1}"#;

    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .body(request)
        .send()
        .await
        .unwrap();

    let (body, ended) = read_all(resp).await;
    assert_eq!(ended, Ok(()));
    assert_eq!(body, sent(OPENAI_STREAM), "the usage chunk is hidden");
    let seen = json(std::str::from_utf8(&provider.seen()[0].body).unwrap());
    assert_eq!(seen["stream_options"]["include_usage"], true);
    assert_eq!(seen["x_new_field"], 1);
}

#[tokio::test]
async fn a_provider_with_its_own_key_replaces_the_clients() {
    let provider = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(OPENAI_WHOLE, "application/json"))
        .mount(&provider)
        .await;
    let config = format!(
        "providers:
  p: {{ format: openai-chat, base_url: '{}/v1', auth: {{ type: bearer, key: sk-own }} }}
serve: [openai-chat]
target: p
",
        provider.uri()
    );
    let gw = Gateway::start(&config).await;

    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .header("authorization", "Bearer sk-client")
        .header("x-api-key", "client-key")
        .body(OPENAI_REQUEST)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let got = &provider.received_requests().await.unwrap()[0];
    let sent = |name: &str| got.headers.get(name).and_then(|v| v.to_str().ok());
    assert_eq!(sent("authorization"), Some("Bearer sk-own"));
    assert_eq!(sent("x-api-key"), None);
}

// 2. Anthropic messages relay.

#[tokio::test]
async fn anthropic_stream_relay_is_byte_exact_and_passes_the_clients_key() {
    let provider = Streamer::start(ANTHROPIC_STREAM.to_vec()).await;
    let gw = Gateway::start(&one_provider("anthropic-messages", &provider.base)).await;
    let request = r#"{"model":"claude-x","max_tokens":64,"stream":true,"messages":[{"role":"user","content":"hi"}],"x_new_field":{"a":[true]}}"#;

    let resp = client()
        .post(gw.url("/v1/messages"))
        .header("x-api-key", "sk-ant-client")
        .header("anthropic-version", "2023-06-01")
        .header("anthropic-beta", "some-future-beta-2099-01-01")
        .body(request)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let (body, ended) = read_all(resp).await;
    assert_eq!(ended, Ok(()));
    assert_eq!(body, sent(ANTHROPIC_STREAM));
    let seen = &provider.seen()[0];
    assert_eq!(seen.path, "/v1/messages");
    assert_eq!(seen.body, request.as_bytes());
    let h = |name: &str| seen.headers.get(name).and_then(|v| v.to_str().ok());
    assert_eq!(h("x-api-key"), Some("sk-ant-client"));
    assert_eq!(h("anthropic-version"), Some("2023-06-01"));
    assert_eq!(h("anthropic-beta"), Some("some-future-beta-2099-01-01"));
}

#[tokio::test]
async fn anthropic_whole_relay_is_byte_exact() {
    let provider = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(ANTHROPIC_WHOLE, "application/json"))
        .mount(&provider)
        .await;
    let gw = Gateway::start(&one_provider("anthropic-messages", &provider.uri())).await;
    let request = r#"{"model":"claude-x","max_tokens":64,"messages":[{"role":"user","content":"hi"}],"x_new":1}"#;

    let resp = client()
        .post(gw.url("/v1/messages"))
        .header("x-api-key", "sk-ant-client")
        .body(request)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), ANTHROPIC_WHOLE);
    let got = &provider.received_requests().await.unwrap()[0];
    assert_eq!(got.body, request.as_bytes());
    assert_eq!(got.headers.get("x-api-key").unwrap(), "sk-ant-client");
}

// 3. Translation: an OpenAI client to an Anthropic provider.

#[tokio::test]
async fn an_openai_client_gets_an_anthropic_answer_in_openai_shape() {
    let openai = MockServer::start().await;
    let anthropic = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(ANTHROPIC_WHOLE, "application/json"))
        .expect(1)
        .mount(&anthropic)
        .await;
    let gw = Gateway::start(&both(&openai.uri(), &anthropic.uri())).await;

    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .header("x-api-key", "sk-ant-client")
        .body(r#"{"model":"claude-x","max_tokens":32,"messages":[{"role":"system","content":"be brief"},{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body = json(&resp.text().await.unwrap());
    assert_eq!(body["object"], "chat.completion");
    assert_eq!(body["choices"][0]["message"]["role"], "assistant");
    assert_eq!(body["choices"][0]["message"]["content"], "hello");
    assert_eq!(body["choices"][0]["finish_reason"], "stop");
    assert!(openai.received_requests().await.unwrap().is_empty());
    let got =
        json(std::str::from_utf8(&anthropic.received_requests().await.unwrap()[0].body).unwrap());
    assert_eq!(got["model"], "claude-x");
    assert_eq!(got["max_tokens"], 32);
    assert_eq!(got["system"], "be brief");
    assert_eq!(got["messages"][0]["role"], "user");
}

#[tokio::test]
async fn an_openai_client_gets_an_anthropic_stream_in_openai_shape() {
    let openai = MockServer::start().await;
    let anthropic = Streamer::start(ANTHROPIC_STREAM.to_vec()).await;
    let gw = Gateway::start(&both(&openai.uri(), &anthropic.base)).await;

    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .body(r#"{"model":"claude-x","stream":true,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert_eq!(
        header(resp.headers(), "content-type"),
        Some("text/event-stream")
    );
    let (body, ended) = read_all(resp).await;
    assert_eq!(ended, Ok(()));
    let (text, done) = openai_stream_text(&body);
    assert_eq!(text, "hello", "{body}");
    assert!(done, "ends with [DONE]: {body}");
    let seen = &anthropic.seen()[0];
    assert_eq!(seen.path, "/v1/messages");
    assert_eq!(
        json(std::str::from_utf8(&seen.body).unwrap())["stream"],
        true
    );
}

#[tokio::test]
async fn an_anthropic_client_gets_an_openai_stream_in_anthropic_shape() {
    let openai = Streamer::start(openai_stream_with_usage()).await;
    let anthropic = MockServer::start().await;
    let gw = Gateway::start(&both(&openai.base, &anthropic.uri())).await;

    let resp = client()
        .post(gw.url("/v1/messages"))
        .body(r#"{"model":"gpt-x","max_tokens":16,"stream":true,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let (body, ended) = read_all(resp).await;
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
    assert_eq!(text, "hello", "{body}");
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
    let seen = json(std::str::from_utf8(&openai.seen()[0].body).unwrap());
    assert_eq!(openai.seen()[0].path, "/v1/chat/completions");
    assert_eq!(seen["stream"], true);
    assert_eq!(seen["max_completion_tokens"], 16, "{seen}");
}

// 4. Errors.

#[tokio::test]
async fn a_relayed_429_keeps_its_retry_after_and_body() {
    let provider = MockServer::start().await;
    let error = r#"{"error":{"message":"slow down","type":"rate_limit_error","code":"rate_limit_exceeded"}}"#;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(429)
                .insert_header("retry-after", "7")
                .set_body_raw(error, "application/json"),
        )
        .mount(&provider)
        .await;
    let gw = Gateway::start(&one_provider(
        "openai-chat",
        &format!("{}/v1", provider.uri()),
    ))
    .await;

    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .body(OPENAI_REQUEST)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 429);
    assert_eq!(header(resp.headers(), "retry-after"), Some("7"));
    assert_eq!(resp.text().await.unwrap(), error);
}

#[tokio::test]
async fn a_translated_429_reaches_an_openai_client_in_its_format() {
    let openai = MockServer::start().await;
    let anthropic = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(429)
                .insert_header("retry-after", "12")
                .set_body_raw(
                    r#"{"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}"#,
                    "application/json",
                ),
        )
        .mount(&anthropic)
        .await;
    let gw = Gateway::start(&both(&openai.uri(), &anthropic.uri())).await;

    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .body(r#"{"model":"claude-x","messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 429);
    assert_eq!(header(resp.headers(), "retry-after"), Some("12"));
    let body = json(&resp.text().await.unwrap());
    assert_eq!(body["error"]["message"], "slow down", "{body}");
    assert!(body["error"]["code"].is_string(), "{body}");
    assert!(
        body.get("type").is_none(),
        "not Anthropic's envelope: {body}"
    );
}

#[tokio::test]
async fn a_translated_500_reaches_an_openai_client_in_its_format() {
    let openai = MockServer::start().await;
    let anthropic = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500).set_body_raw(
            r#"{"type":"error","error":{"type":"api_error","message":"boom"}}"#,
            "application/json",
        ))
        .mount(&anthropic)
        .await;
    let gw = Gateway::start(&both(&openai.uri(), &anthropic.uri())).await;

    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .body(r#"{"model":"claude-x","messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 500);
    let body = json(&resp.text().await.unwrap());
    assert_eq!(body["error"]["message"], "boom", "{body}");
}

#[tokio::test]
async fn a_relayed_500_passes_through() {
    let provider = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500).set_body_raw("upstream exploded", "text/plain"))
        .mount(&provider)
        .await;
    let gw = Gateway::start(&one_provider("anthropic-messages", &provider.uri())).await;

    let resp = client()
        .post(gw.url("/v1/messages"))
        .body(r#"{"model":"claude-x","max_tokens":8,"messages":[]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 500);
    assert_eq!(resp.text().await.unwrap(), "upstream exploded");
}

#[tokio::test]
async fn an_unreachable_provider_is_a_502_in_the_clients_format() {
    let nowhere = closed_port().await;
    let gw = Gateway::start(&both(&nowhere, &nowhere)).await;

    let openai = client()
        .post(gw.url("/v1/chat/completions"))
        .body(OPENAI_REQUEST)
        .send()
        .await
        .unwrap();
    assert_eq!(openai.status(), 502);
    let body = json(&openai.text().await.unwrap());
    assert_eq!(body["error"]["code"], "upstream", "{body}");

    let anthropic = client()
        .post(gw.url("/v1/messages"))
        .body(r#"{"model":"claude-x","max_tokens":8,"messages":[]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(anthropic.status(), 502);
    let body = json(&anthropic.text().await.unwrap());
    assert_eq!(body["type"], "error", "{body}");
    assert!(body["error"]["message"].is_string(), "{body}");
}

/// The first two chunks of [`OPENAI_STREAM`], then the provider breaks off.
const OPENAI_CUT: &[Step] = &[
    Step::Send(
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"hel\"}}]}\n\n",
    ),
    Step::Wait(Duration::from_millis(50)),
    Step::Cut,
];

#[tokio::test]
async fn a_relayed_stream_cut_mid_body_ends_truncated_without_a_hang() {
    let provider = Streamer::start(OPENAI_CUT.to_vec()).await;
    let gw = Gateway::start(&one_provider(
        "openai-chat",
        &format!("{}/v1", provider.base),
    ))
    .await;

    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .body(r#"{"model":"gpt-x","stream":true,"messages":[]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let (body, ended) = read_all(resp).await;
    assert!(body.starts_with(sent(OPENAI_CUT).as_str()), "{body}");
    let tail = &body[sent(OPENAI_CUT).len()..];
    let (_, done) = openai_stream_text(&body);
    assert!(!done, "a cut stream is not [DONE]: {body}");
    // The client's stream ends cleanly with an error event in its format.
    assert_eq!(ended, Ok(()));
    let data = tail.lines().find_map(|l| l.strip_prefix("data: ")).unwrap();
    let error = &json(data)["error"];
    assert_eq!(error["code"], "upstream", "{tail}");
    assert_eq!(error["message"], "the provider's body broke off", "{tail}");
}

#[tokio::test]
async fn an_anthropic_stream_cut_after_content_ends_with_an_anthropic_error_event() {
    let steps = vec![
        ANTHROPIC_STREAM[0].clone(),
        ANTHROPIC_STREAM[1].clone(),
        ANTHROPIC_STREAM[4].clone(),
        Step::Wait(Duration::from_millis(50)),
        Step::Cut,
    ];
    let provider = Streamer::start(steps.clone()).await;
    let gw = Gateway::start(&one_provider("anthropic-messages", &provider.base)).await;

    let resp = client()
        .post(gw.url("/v1/messages"))
        .body(r#"{"model":"claude-x","max_tokens":8,"stream":true,"messages":[]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let (body, ended) = read_all(resp).await;
    assert_eq!(ended, Ok(()));
    let tail = body.strip_prefix(sent(&steps).as_str()).unwrap();
    assert!(tail.starts_with("event: error\n"), "{tail}");
    let data = tail.lines().find_map(|l| l.strip_prefix("data: ")).unwrap();
    let v = json(data);
    assert_eq!(v["type"], "error", "{tail}");
    assert_eq!(v["error"]["type"], "upstream", "{tail}");
}

#[tokio::test]
async fn an_anthropic_stream_cut_before_content_is_refused_502() {
    // The start is held until the first content, so a failure before any
    // can still be refused with a status.
    let steps = vec![
        ANTHROPIC_STREAM[0].clone(),
        ANTHROPIC_STREAM[1].clone(),
        Step::Wait(Duration::from_millis(50)),
        Step::Cut,
    ];
    let provider = Streamer::start(steps).await;
    let gw = Gateway::start(&one_provider("anthropic-messages", &provider.base)).await;

    let resp = client()
        .post(gw.url("/v1/messages"))
        .body(r#"{"model":"claude-x","max_tokens":8,"stream":true,"messages":[]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 502);
    assert_eq!(
        resp.text().await.unwrap(),
        r#"{"type":"error","error":{"type":"upstream","message":"the provider's body broke off"}}"#
    );
}

#[tokio::test]
async fn a_stream_that_ends_before_done_ends_truncated() {
    let steps = vec![OPENAI_STREAM[0].clone()];
    let provider = Streamer::start(steps.clone()).await;
    let gw = Gateway::start(&one_provider(
        "openai-chat",
        &format!("{}/v1", provider.base),
    ))
    .await;

    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .body(r#"{"model":"gpt-x","stream":true,"messages":[]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let (body, ended) = read_all(resp).await;
    assert_eq!(ended, Ok(()));
    let tail = body.strip_prefix(sent(&steps).as_str()).unwrap();
    let data = tail.lines().find_map(|l| l.strip_prefix("data: ")).unwrap();
    assert_eq!(json(data)["error"]["code"], "truncated", "{tail}");
}

#[tokio::test]
async fn a_translated_stream_cut_mid_body_ends_truncated_without_a_hang() {
    let openai = MockServer::start().await;
    let anthropic = Streamer::start(vec![
        ANTHROPIC_STREAM[0].clone(),
        ANTHROPIC_STREAM[1].clone(),
        ANTHROPIC_STREAM[4].clone(),
        Step::Wait(Duration::from_millis(50)),
        Step::Cut,
    ])
    .await;
    let gw = Gateway::start(&both(&openai.uri(), &anthropic.base)).await;

    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .body(r#"{"model":"claude-x","stream":true,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let (body, ended) = read_all(resp).await;
    let (text, done) = openai_stream_text(&body);
    assert_eq!(text, "hel", "{body}");
    assert!(!done, "a cut stream is not [DONE]: {body}");
    assert_eq!(ended, Ok(()));
    assert!(body.ends_with(
        "data: {\"error\":{\"message\":\"the provider's body broke off\",\"type\":\"upstream\",\"param\":null,\"code\":\"upstream\"}}\n\n"
    ), "{body}");
}

#[tokio::test]
async fn a_slow_first_byte_is_waited_for() {
    let mut steps = vec![Step::Wait(Duration::from_millis(700))];
    steps.extend_from_slice(OPENAI_STREAM);
    let provider = Streamer::start(steps).await;
    let gw = Gateway::start(&one_provider(
        "openai-chat",
        &format!("{}/v1", provider.base),
    ))
    .await;

    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .body(r#"{"model":"gpt-x","stream":true,"messages":[]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let (body, ended) = read_all(resp).await;
    assert_eq!(ended, Ok(()));
    assert_eq!(body, sent(OPENAI_STREAM));
}

// 5. Model routing (the binary has no fallback; see README).

#[tokio::test]
async fn models_route_by_prefix_and_the_rest_go_to_the_target() {
    let openai = MockServer::start().await;
    let anthropic = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(OPENAI_WHOLE, "application/json"))
        .expect(2)
        .mount(&openai)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(ANTHROPIC_WHOLE, "application/json"))
        .expect(2)
        .mount(&anthropic)
        .await;
    let gw = Gateway::start(&both(&openai.uri(), &anthropic.uri())).await;
    let c = client();
    let post = |path: &str, body: &'static str| c.post(gw.url(path)).body(body).send();

    // Each client format to each provider: two relays, two translations.
    let r = post("/v1/chat/completions", OPENAI_REQUEST).await.unwrap();
    assert_eq!(r.text().await.unwrap(), OPENAI_WHOLE);
    let r = post(
        "/v1/messages",
        r#"{"model":"claude-x","max_tokens":8,"messages":[{"role":"user","content":"hi"}]}"#,
    )
    .await
    .unwrap();
    assert_eq!(r.text().await.unwrap(), ANTHROPIC_WHOLE);
    let r = post(
        "/v1/chat/completions",
        r#"{"model":"claude-x","messages":[{"role":"user","content":"hi"}]}"#,
    )
    .await
    .unwrap();
    assert_eq!(json(&r.text().await.unwrap())["object"], "chat.completion");
    let r = post(
        "/v1/messages",
        r#"{"model":"gpt-x","max_tokens":8,"messages":[{"role":"user","content":"hi"}]}"#,
    )
    .await
    .unwrap();
    let text = r.text().await.unwrap();
    let body = json(&text);
    assert_eq!(body["type"], "message", "{text}");
    assert_eq!(body["content"][0]["text"], "hello");
    // `expect(2)` on each mock is checked when the servers drop.
}

// 6. The server's own behaviour.

#[tokio::test]
async fn an_unknown_path_is_a_404() {
    let provider = MockServer::start().await;
    let gw = Gateway::start(&one_provider("openai-chat", &provider.uri())).await;

    for (verb, path) in [
        (reqwest::Method::POST, "/v1/nope"),
        (reqwest::Method::GET, "/v1/chat/completions"),
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
    assert!(provider.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn a_body_over_32_mib_is_refused_413_in_the_clients_format() {
    let provider = MockServer::start().await;
    let gw = Gateway::start(&one_provider(
        "openai-chat",
        &format!("{}/v1", provider.uri()),
    ))
    .await;
    let mut big = String::from(r#"{"model":"gpt-x","messages":[],"pad":""#);
    big.push_str(&"x".repeat(32 * 1024 * 1024));
    big.push_str("\"}");

    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .body(big)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 413);
    let body = json(&resp.text().await.unwrap());
    assert_eq!(body["error"]["code"], "too_large", "{body}");
    assert!(provider.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn cookies_and_forwarding_headers_do_not_reach_the_provider() {
    let provider = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(OPENAI_WHOLE, "application/json"))
        .mount(&provider)
        .await;
    let gw = Gateway::start(&one_provider(
        "openai-chat",
        &format!("{}/v1", provider.uri()),
    ))
    .await;

    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .header("cookie", "session=gateway-origin")
        .header("forwarded", "for=10.0.0.1")
        .header("via", "1.1 proxy")
        .header("x-forwarded-for", "10.0.0.1")
        .header("x-forwarded-host", "gw.example")
        .header("x-real-ip", "10.0.0.1")
        .header("x-custom", "kept")
        .body(OPENAI_REQUEST)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let got = &provider.received_requests().await.unwrap()[0];
    for gone in [
        "cookie",
        "forwarded",
        "via",
        "x-forwarded-for",
        "x-forwarded-host",
        "x-real-ip",
    ] {
        assert!(
            got.headers.get(gone).is_none(),
            "{gone} reached the provider"
        );
    }
    assert_eq!(got.headers.get("x-custom").unwrap(), "kept");
}

#[tokio::test]
async fn set_cookie_and_hsts_do_not_reach_the_client() {
    let provider = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("set-cookie", "p=1")
                .insert_header("strict-transport-security", "max-age=1")
                .insert_header("alt-svc", "h3=\":443\"")
                .set_body_raw(OPENAI_WHOLE, "application/json"),
        )
        .mount(&provider)
        .await;
    let gw = Gateway::start(&one_provider(
        "openai-chat",
        &format!("{}/v1", provider.uri()),
    ))
    .await;

    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .body(OPENAI_REQUEST)
        .send()
        .await
        .unwrap();

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
    let steps = vec![
        Step::Send("data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"a\"}}]}\n\n"),
        Step::Wait(Duration::from_millis(400)),
        Step::Send("data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"b\"}}]}\n\n"),
        Step::Wait(Duration::from_millis(400)),
        Step::Send("data: [DONE]\n\n"),
    ];
    let provider = Streamer::start(steps.clone()).await;
    let mut gw = Gateway::start(&one_provider(
        "openai-chat",
        &format!("{}/v1", provider.base),
    ))
    .await;

    let mut resp = client()
        .post(gw.url("/v1/chat/completions"))
        .body(r#"{"model":"gpt-x","stream":true,"messages":[]}"#)
        .send()
        .await
        .unwrap();
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
    assert_eq!(
        format!("{}{rest}", String::from_utf8_lossy(&first)),
        sent(&steps)
    );
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
