//! A client in one format reaching a provider in another.

use respan_gateway_http::bytes;
use respan_gateway_http::http;
use respan_gateway_sdk::futures;
use std::sync::{Arc, Mutex};

use http::{HeaderValue, StatusCode};
use mockllm::{Answer, FakeAnthropic, FakeOpenAi, Request};
use respan_gateway_plugins::ModelRouter;
use respan_gateway_providers::{AnthropicMessages, OpenAiChat};
use respan_gateway_sdk::async_trait;
use respan_gateway_sdk::conversation::{Conversation, Finish, Piece, Reply, units};
use respan_gateway_sdk::{ClientContext, Fault, Plugin, ProviderContext, Response, codes};
#[allow(dead_code)] // Shared hooks also expose a call log not used by this suite.
#[path = "../../tests/support/hooks.rs"]
mod hooks;
#[allow(dead_code, unused_imports)]
#[path = "../../tests/support/http.rs"]
mod http_support;
use hooks::Outcomes;
use http_support::{post, read_whole, serve};

mod support;
use respan_gateway_http::{Body, Completion, HttpClient};
use respan_gateway_sdk::Filter;
use serde_json::{Value, json};
use support::{
    Adds, CHAT, Call, Disclaimer, Flip, Footer, HoldAll, MESSAGES, Metered, Nameless, Shout,
    Silence, Unmask, add_provider, providers, with_plugins,
};
use support::{SendRequest, http_sender, sender};

fn call(path: &str, body: impl Into<bytes::Bytes>) -> Call {
    post(path, body)
}

/// An event at a time, preserving the source SSE bytes and unusual provider events.
fn sse(chunks: &[&str]) -> Answer {
    let events = chunks
        .iter()
        .flat_map(|chunk| chunk.split("\n\n"))
        .filter(|event| !event.is_empty())
        .map(|event| Value::String(event.to_owned()))
        .collect();
    Answer::events(events)
}

fn json_response(status: StatusCode, body: &str) -> Answer {
    if status.is_success() {
        Answer::raw(body.to_owned())
    } else {
        let value: Value = serde_json::from_str(body).unwrap_or_default();
        let message = value["error"]["message"]
            .as_str()
            .unwrap_or("upstream error");
        Answer::error(status.as_u16()).message(message)
    }
}

struct Harness {
    gw: respan_gateway_sdk::Gateway,
    openai: FakeOpenAi,
    anthropic: FakeAnthropic,
    log: Outcomes,
    usage: Metered,
}

const ANTHROPIC_STREAM: &[&str] = &[
    "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-x\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null,\"usage\":{\"input_tokens\":5,\"output_tokens\":1}}}\n\n",
    "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\nevent: ping\ndata: {\"type\": \"ping\"}\n\n",
    "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello\"}}\n\n",
    "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\" world\"}}\n\n",
    "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
    "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\",\"stop_sequence\":null},\"usage\":{\"output_tokens\":2}}\n\n",
    "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
];

const OPENAI_STREAM: &[&str] = &[
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"}}]}\n\n",
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello\"}}]}\n\n",
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"}}]}\n\n",
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2,\"total_tokens\":7}}\n\n",
    "data: [DONE]\n\n",
];

fn openai_request(model: &str, stream: bool) -> Call {
    call(CHAT, openai_body(model, stream))
}

fn openai_body(model: &str, stream: bool) -> String {
    json!({
        "model": model,
        "stream": stream,
        "stream_options": { "include_usage": stream },
        "messages": [
            { "role": "system", "content": "be brief" },
            { "role": "user", "content": "hi" },
        ],
    })
    .to_string()
}

fn anthropic_request(stream: bool) -> Call {
    let body = json!({
        "model": "gpt-x",
        "max_tokens": 100,
        "stream": stream,
        "system": "be brief",
        "messages": [{ "role": "user", "content": "hi" }],
    });
    call(MESSAGES, body.to_string())
}

/// Sends each client to the provider of the other format, as a gateway
/// that translates everything would; a router after it may override.
struct Cross;

#[async_trait]
impl Plugin for Cross {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        match cx.format() {
            "openai-chat" => cx.set_target("anthropic"),
            _ => cx.set_target("openai"),
        }
        Ok(None)
    }
}

impl Harness {
    /// Plugins, in order: the log, [`Cross`], `extra`, and a router sending
    /// Claude models to Anthropic, as gateway.example.yaml does.
    async fn new(extra: Vec<Arc<dyn Plugin>>) -> Self {
        let mut chain: Vec<Arc<dyn Plugin>> = vec![Arc::new(Cross)];
        chain.extend(extra);
        Self::with(chain).await
    }

    /// Serves both formats with no target. Plugins, in order: the log,
    /// `chain`, and a router sending Claude models to Anthropic.
    async fn with(chain: Vec<Arc<dyn Plugin>>) -> Self {
        Self::with_local(chain, None, None).await
    }

    /// Replace just one protocol boundary for a deterministic malformed reply.
    async fn with_local(
        chain: Vec<Arc<dyn Plugin>>,
        openai_reply: Option<SendRequest>,
        anthropic_reply: Option<SendRequest>,
    ) -> Self {
        let log = Outcomes::default();
        let (openai, anthropic) = tokio::join!(FakeOpenAi::start(), FakeAnthropic::start());
        let openai_http = openai_reply.unwrap_or_else(|| {
            provider_http(&openai.base_url(), "authorization", "Bearer sk-test")
        });
        let anthropic_http = anthropic_reply
            .unwrap_or_else(|| provider_http(&anthropic.base_url(), "x-api-key", "sk-ant-test"));
        let mut plugins: Vec<Arc<dyn Plugin>> = vec![Arc::new(log.clone())];
        plugins.extend(chain);
        plugins.push(Arc::new(
            ModelRouter::new(|c: &Conversation| c.model.as_deref()).route("claude", "anthropic"),
        ));
        let usage = Metered::default();
        plugins.push(Arc::new(usage.clone()));
        let gw = providers([
            add_provider("openai", OpenAiChat, openai_http),
            add_provider("anthropic", AnthropicMessages, anthropic_http),
        ])
        .and_then(|gw| {
            gw.client::<Conversation, _, _>(OpenAiChat)?
                .client::<Conversation, _, _>(AnthropicMessages)
        })
        .and_then(|gw| with_plugins(gw, plugins))
        .unwrap_or_else(|e| panic!("gateway: {e}"));
        Self {
            gw,
            openai,
            anthropic,
            log,
            usage,
        }
    }

    fn sent(&self) -> Vec<Request> {
        let mut sent = self.openai.requests();
        sent.extend(self.anthropic.requests());
        sent
    }

    fn sent_json(&self, i: usize) -> Value {
        self.sent()[i].body.clone()
    }

    fn assert_requests(&self, openai: usize, anthropic: usize) {
        assert_eq!(
            (
                self.openai.requests().len(),
                self.anthropic.requests().len()
            ),
            (openai, anthropic),
        );
    }

    async fn send(&self, req: Call) -> (StatusCode, http::HeaderMap, Result<bytes::Bytes, Fault>) {
        let out = read_whole(serve(&self.gw, req).await.0).await;
        self.gw.idle().await;
        out
    }

    async fn text(&self, req: Call) -> (StatusCode, String) {
        let (status, _, body) = self.send(req).await;
        let body = body.unwrap_or_else(|f| panic!("aborted: {f}"));
        (status, String::from_utf8_lossy(&body).into_owned())
    }
}

fn provider_http(base: &str, header: &'static str, key: &'static str) -> SendRequest {
    let mut headers = http::HeaderMap::new();
    headers.insert(header, HeaderValue::from_static(key));
    http_sender(
        HttpClient::new(base)
            .and_then(|transport| transport.with_headers(headers))
            .unwrap_or_else(|fault| panic!("{fault}")),
    )
}

/// One deliberately irregular response, with no transport registry or URL dispatch.
fn one_response(reply: http::Response<Vec<Result<bytes::Bytes, Fault>>>) -> SendRequest {
    let reply = Arc::new(Mutex::new(Some(reply)));
    sender(move |_call| {
        let reply = reply.clone();
        async move {
            let response = reply
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take()
                .expect("one response per provider call");
            let (head, chunks) = response.into_parts();
            Ok((
                http::Response::from_parts(head, Body::stream(futures::stream::iter(chunks))),
                None,
            ))
        }
    })
}

fn local_json(status: StatusCode, body: &str) -> SendRequest {
    let reply = http::Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(vec![Ok(bytes::Bytes::from(body.to_owned()))])
        .unwrap_or_else(|e| panic!("{e}"));
    one_response(reply)
}

/// Each SSE event as `(event name, data)`.
fn events(body: &str) -> Vec<(Option<String>, Value)> {
    body.split("\n\n")
        .filter(|e| !e.trim().is_empty())
        .map(|e| {
            let field = |name: &str| {
                e.lines()
                    .find_map(|l| l.strip_prefix(name))
                    .map(|v| v.trim().to_owned())
            };
            let data = field("data:").unwrap_or_default();
            let data = serde_json::from_str(&data).unwrap_or(Value::String(data));
            (field("event:"), data)
        })
        .collect()
}

// Only a relay carries the client's headers: a translated call says
// Anthropic's default version, a relay the client's.
#[tokio::test]
async fn only_a_relay_carries_the_clients_anthropic_version() {
    let h = Harness::new(Vec::new()).await;
    let version = HeaderValue::from_static("2099-01-01");
    let messages = json!({
        "model": "claude-x", "max_tokens": 9, "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
    });
    for mut req in [
        openai_request("claude-x", true),
        call(MESSAGES, messages.to_string()),
    ] {
        h.anthropic.answer(sse(ANTHROPIC_STREAM));
        req.headers_mut()
            .insert("anthropic-version", version.clone());
        let (status, _) = h.text(req).await;
        assert_eq!(status, StatusCode::OK);
    }
    h.assert_requests(0, 2);
    let sent = h.sent();
    let said = |i: usize| sent[i].headers.get("anthropic-version").cloned();
    assert_eq!(said(0), Some(HeaderValue::from_static("2023-06-01")));
    assert_eq!(said(1), Some(version));
}

/// A lookalike of the marker a plugin might try to set.
#[derive(Clone)]
struct Relayed;

/// Sends every request to OpenAI, and in `on_provider` does all it can to
/// make every call look like a relay: a marker in its extensions, which
/// does not set the private relay marker.
struct Forges;

#[async_trait]
impl Plugin for Forges {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.set_target("openai");
        Ok(None)
    }

    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        px.extensions_mut().insert(Relayed);
        Ok(())
    }
}

/// A transport that notes whether each call came `relayed`.
struct Marks<T>(T, Arc<Mutex<Vec<bool>>>);

impl Marks<SendRequest> {
    pub async fn send(
        &self,
        call: http::Request<Body>,
    ) -> Result<(http::Response<Body>, Option<Completion>), Fault> {
        let relayed = respan_gateway_http::is_relayed(&call);
        self.1
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(relayed);
        (self.0)(call).await
    }
}
impl<T: Into<SendRequest>> From<Marks<T>> for SendRequest {
    fn from(value: Marks<T>) -> Self {
        let value = std::sync::Arc::new(Marks(value.0.into(), value.1));
        sender(move |call| {
            let value = value.clone();
            async move { value.send(call).await }
        })
    }
}

// The kernel sets the private relay marker on a call that relays the client's
// own message, and no other, whatever a plugin did; only a relay carries
// the client's key to a provider with none of its own.
#[tokio::test]
async fn only_a_relay_is_marked_and_carries_the_clients_key() {
    let (openai_fake, anthropic_fake) = tokio::join!(FakeOpenAi::start(), FakeAnthropic::start());
    let marks = Arc::default();
    // OpenAI has no transport key, so its relay needs the client's key.
    // Anthropic has its own key: the translated call must never reuse that client key.
    let openai_http =
        http_sender(HttpClient::new(openai_fake.base_url()).unwrap_or_else(|f| panic!("{f}")));
    let anthropic_http = provider_http(&anthropic_fake.base_url(), "x-api-key", "sk-ant-test");
    let gw = providers([
        add_provider("openai", OpenAiChat, Marks(openai_http, Arc::clone(&marks))),
        add_provider(
            "anthropic",
            AnthropicMessages,
            Marks(anthropic_http, Arc::clone(&marks)),
        ),
    ])
    .and_then(|gw| gw.client::<Conversation, _, _>(OpenAiChat))
    .and_then(|gw| {
        let router = ModelRouter::new(|c: &Conversation| c.model.as_deref());
        let plugins: Vec<Arc<dyn Plugin>> = vec![
            Arc::new(Forges),
            Arc::new(router.route("claude", "anthropic")),
        ];
        with_plugins(gw, plugins)
    })
    .unwrap_or_else(|e| panic!("gateway: {e}"));
    let unary = |model| {
        let body = json!({ "model": model, "messages": [{ "role": "user", "content": "hi" }] });
        let mut req = call(CHAT, body.to_string());
        let key = HeaderValue::from_static("Bearer sk-client");
        req.headers_mut().insert("authorization", key);
        req
    };
    openai_fake.answer(Answer::text("hi"));
    anthropic_fake.answer(Answer::text("hi"));
    for model in ["gpt-x", "claude-x"] {
        let (resp, _) = serve(&gw, unary(model)).await;
        let (status, _, _) = read_whole(resp).await;
        assert_eq!(status, StatusCode::OK, "{model}");
    }
    gw.idle().await;

    assert_eq!(
        *marks.lock().unwrap_or_else(|e| e.into_inner()),
        [true, false]
    );
    let openai = openai_fake.requests();
    let anthropic = anthropic_fake.requests();
    assert_eq!((openai.len(), anthropic.len()), (1, 1));
    assert_eq!(openai[0].header("authorization"), Some("Bearer sk-client"));
    assert_eq!(anthropic[0].header("authorization"), None);
    assert_eq!(anthropic[0].header("x-api-key"), Some("sk-ant-test"));
}

#[tokio::test]
async fn an_openai_client_streams_from_anthropic() {
    let h = Harness::new(Vec::new()).await;
    h.anthropic.answer(sse(ANTHROPIC_STREAM));

    let (status, body) = h.text(openai_request("claude-x", true)).await;

    assert_eq!(status, StatusCode::OK);
    h.assert_requests(0, 1);
    let sent = &h.sent()[0];
    assert_eq!(sent.path, "/v1/messages");
    assert_eq!(
        sent.headers.get("anthropic-version"),
        Some(&HeaderValue::from_static("2023-06-01"))
    );
    assert_eq!(
        h.sent_json(0),
        json!({
            "model": "claude-x",
            "max_tokens": 4096,
            "system": "be brief",
            "messages": [{ "role": "user", "content": "hi" }],
            "stream": true,
        })
    );

    let chunks: Vec<Value> = events(&body).into_iter().map(|(_, d)| d).collect();
    assert_eq!(chunks[0]["id"], "msg_1");
    assert_eq!(chunks[0]["model"], "claude-x");
    assert_eq!(chunks[0]["choices"][0]["delta"]["role"], "assistant");
    let text: String = chunks
        .iter()
        .filter_map(|c| {
            c.pointer("/choices/0/delta/content")
                .and_then(Value::as_str)
        })
        .collect();
    assert_eq!(text, "Hello world");
    let n = chunks.len();
    assert_eq!(chunks[n - 3]["choices"][0]["finish_reason"], "stop");
    assert_eq!(
        chunks[n - 2]["usage"],
        json!({ "prompt_tokens": 5, "completion_tokens": 2, "total_tokens": 7 })
    );
    assert_eq!(chunks[n - 1], Value::String("[DONE]".into()));

    assert_eq!(h.log.only(), Ok(()));
    let usage = h.usage.total();
    assert_eq!(usage.get(units::INPUT_TOKENS), Some(5.0));
    assert_eq!(usage.get(units::OUTPUT_TOKENS), Some(2.0));
}

#[tokio::test]
async fn an_anthropic_client_streams_from_openai() {
    let h = Harness::new(Vec::new()).await;
    h.openai.answer(sse(OPENAI_STREAM));

    let (status, body) = h.text(anthropic_request(true)).await;

    assert_eq!(status, StatusCode::OK);
    h.assert_requests(1, 0);
    assert_eq!(h.sent()[0].path, "/v1/chat/completions");
    assert_eq!(
        h.sent_json(0),
        json!({
            "model": "gpt-x",
            "messages": [
                { "role": "system", "content": "be brief" },
                { "role": "user", "content": "hi" },
            ],
            "max_completion_tokens": 100,
            "stream": true,
            "stream_options": { "include_usage": true },
        })
    );

    let events = events(&body);
    let names: Vec<&str> = events.iter().filter_map(|(e, _)| e.as_deref()).collect();
    assert_eq!(
        names,
        [
            "message_start",
            "content_block_start",
            "content_block_delta",
            "content_block_delta",
            "content_block_stop",
            "message_delta",
            "message_stop",
        ]
    );
    assert_eq!(events[0].1["message"]["id"], "c1");
    assert_eq!(
        events[2].1["delta"],
        json!({ "type": "text_delta", "text": "Hello" })
    );
    assert_eq!(
        events[5].1,
        json!({
            "type": "message_delta",
            "delta": { "stop_reason": "end_turn", "stop_sequence": null },
            "usage": { "input_tokens": 5, "output_tokens": 2 },
        })
    );
    assert_eq!(h.log.only(), Ok(()));
}

#[tokio::test]
async fn the_client_gets_the_streaming_it_asked_for_whatever_a_plugin_asks_of_the_provider() {
    struct Whole;

    #[async_trait]
    impl Plugin for Whole {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            use respan_gateway_sdk::conversation::Conversation;
            cx.filter(|chat: &mut Conversation| {
                chat.stream = false;
                Ok(())
            })?;
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Whole)]).await;
    let reply = json!({
        "id": "msg_2", "type": "message", "role": "assistant", "model": "claude-x",
        "content": [{ "type": "text", "text": "hi" }],
        "stop_reason": "end_turn", "usage": { "input_tokens": 1, "output_tokens": 1 },
    });
    h.anthropic
        .answer(json_response(StatusCode::OK, &reply.to_string()));

    let (_, body) = h.text(openai_request("claude-x", true)).await;

    let sent: Value = serde_json::from_slice(h.sent()[0].raw.as_ref()).unwrap_or_default();
    assert_eq!(
        sent["stream"],
        Value::Null,
        "the provider was asked for a whole answer"
    );
    assert!(body.starts_with("data: "), "{body}");
    assert!(body.contains("\"content\":\"hi\""), "{body}");
}

#[tokio::test]
async fn unary_responses_translate_both_ways() {
    let h = Harness::new(Vec::new()).await;
    let reply = json!({
        "id": "msg_2", "type": "message", "role": "assistant", "model": "claude-x",
        "content": [{ "type": "text", "text": "hi there" }],
        "stop_reason": "max_tokens", "stop_sequence": null,
        "usage": { "input_tokens": 4, "output_tokens": 2 },
    });
    h.anthropic
        .answer(json_response(StatusCode::OK, &reply.to_string()));

    let (_, body) = h.text(openai_request("claude-x", false)).await;

    let v: Value = serde_json::from_str(&body).unwrap_or_default();
    assert_eq!(v["id"], "msg_2");
    assert_eq!(v["object"], "chat.completion");
    assert_eq!(
        v["choices"][0],
        json!({
            "index": 0,
            "message": { "role": "assistant", "content": "hi there" },
            "finish_reason": "length",
        })
    );
    assert_eq!(
        v["usage"],
        json!({ "prompt_tokens": 4, "completion_tokens": 2, "total_tokens": 6 })
    );

    let h = Harness::new(Vec::new()).await;
    let reply = json!({
        "id": "c2", "object": "chat.completion", "model": "gpt-x",
        "choices": [{ "index": 0, "message": { "role": "assistant", "content": "hi there" }, "finish_reason": "stop" }],
        "usage": { "prompt_tokens": 4, "completion_tokens": 2 },
    });
    h.openai
        .answer(json_response(StatusCode::OK, &reply.to_string()));

    let (_, body) = h.text(anthropic_request(false)).await;

    let v: Value = serde_json::from_str(&body).unwrap_or_default();
    assert_eq!(
        v,
        json!({
            "id": "c2", "type": "message", "role": "assistant", "model": "gpt-x",
            "content": [{ "type": "text", "text": "hi there" }],
            "stop_reason": "end_turn", "stop_sequence": null,
            "usage": { "input_tokens": 4, "output_tokens": 2 },
        })
    );
}

#[tokio::test]
async fn a_provider_error_reaches_the_client_in_its_format() {
    let h = Harness::new(Vec::new()).await;
    let error = r#"{"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}"#;
    h.anthropic
        .answer(json_response(StatusCode::TOO_MANY_REQUESTS, error));

    let (status, body) = h.text(openai_request("claude-x", true)).await;

    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    let v: Value = serde_json::from_str(&body).unwrap_or_default();
    assert_eq!(
        v,
        json!({ "error": { "message": "slow down", "type": "rate_limit_error", "param": null, "code": "rate_limit_error" } })
    );
    assert!(matches!(h.log.only(), Err(f) if f.code() == "rate_limit_error"));
}

#[tokio::test]
async fn a_request_that_cannot_be_translated_is_refused() {
    let h = Harness::new(Vec::new()).await;
    let format = json!({ "type": "json_object" });
    let body = json!({ "model": "claude-x", "messages": [], "response_format": format });

    let (status, body) = h.text(call(CHAT, body.to_string())).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body.contains("'response_format' in the request cannot be translated"),
        "{body}"
    );
    assert!(h.sent().is_empty());
}

// The same request, edited by a plugin whose edit does not cause the
// failure, is still the client's fault: its own request fails the same way.
#[tokio::test]
async fn an_edit_that_does_not_cause_the_failure_leaves_it_the_clients_fault() {
    /// Edits every conversation.
    struct Edit;

    #[async_trait]
    impl Plugin for Edit {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(|chat: &mut Conversation| {
                chat.temperature = Some(0.5);
                Ok(())
            })?;
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Edit)]).await;
    let format = json!({ "type": "json_object" });
    let body = json!({ "model": "claude-x", "messages": [], "response_format": format });

    let (status, _) = h.text(call(CHAT, body.to_string())).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(matches!(h.log.only(), Err(f) if f.code() == codes::BAD_REQUEST));
}

// An edit that itself makes the request untranslatable is the plugin's
// fault: the client's own request translates.
#[tokio::test]
async fn an_edit_that_causes_the_failure_is_the_gateways_fault() {
    /// Adds a field Anthropic has no place for.
    struct Break;

    #[async_trait]
    impl Plugin for Break {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(|chat: &mut Conversation| {
                let format = json!({ "type": "json_object" });
                chat.unsupported.insert("response_format".into(), format);
                Ok(())
            })?;
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Break)]).await;
    let body = json!({ "model": "claude-x", "max_tokens": 9, "messages": [{ "role": "user", "content": "hi" }] });

    let (status, _) = h.text(call(CHAT, body.to_string())).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(matches!(h.log.only(), Err(f) if f.code() == codes::CONFIG));
}

/// An agent's turn in OpenAI's words: tools, a call made, its result, and
/// a question after it.
fn openai_agent() -> Call {
    let tool = json!({ "type": "function", "function": {
        "name": "read", "description": "read a file", "parameters": { "type": "object" } } });
    let call_made = json!({ "id": "call_1", "type": "function",
        "function": { "name": "read", "arguments": "{\"path\":\"a.rs\"}" } });
    let body = json!({
        "model": "claude-x",
        "tools": [tool],
        "tool_choice": { "type": "function", "function": { "name": "read" } },
        "parallel_tool_calls": false,
        "messages": [
            { "role": "user", "content": "open a.rs" },
            { "role": "assistant", "content": "", "tool_calls": [call_made] },
            { "role": "tool", "tool_call_id": "call_1", "content": "fn main() {}" },
            { "role": "user", "content": "now what?" },
        ],
    });
    call(CHAT, body.to_string())
}

/// The same turn in Anthropic's words.
fn anthropic_agent() -> Call {
    let tool = json!({ "name": "read", "description": "read a file",
        "input_schema": { "type": "object" } });
    let body = json!({
        "model": "gpt-x",
        "max_tokens": 100,
        "tools": [tool],
        "tool_choice": { "type": "tool", "name": "read", "disable_parallel_tool_use": true },
        "messages": [
            { "role": "user", "content": "open a.rs" },
            { "role": "assistant", "content": [
                { "type": "text", "text": "Reading." },
                { "type": "tool_use", "id": "call_1", "name": "read", "input": { "path": "a.rs" } },
            ] },
            { "role": "user", "content": [
                { "type": "tool_result", "tool_use_id": "call_1", "content": "fn main() {}" },
                { "type": "text", "text": "now what?" },
            ] },
        ],
    });
    call(MESSAGES, body.to_string())
}

const ANTHROPIC_TOOL_STREAM: &[&str] = &[
    "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-x\",\"content\":[],\"usage\":{\"input_tokens\":5,\"output_tokens\":1}}}\n\n",
    "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
    "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Reading.\"}}\n\n",
    "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
    "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"read\",\"input\":{}}}\n\n",
    "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"path\\\":\"}}\n\n",
    "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"\\\"a.rs\\\"}\"}}\n\n",
    "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":1}\n\n",
    "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\",\"stop_sequence\":null},\"usage\":{\"output_tokens\":9}}\n\n",
    "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
];

const OPENAI_TOOL_STREAM: &[&str] = &[
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":null,\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"read\",\"arguments\":\"\"}}]}}]}\n\n",
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"{\\\"path\\\":\"}}]}}]}\n\n",
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"\\\"a.rs\\\"}\"}}]}}]}\n\n",
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":9,\"total_tokens\":14}}\n\n",
    "data: [DONE]\n\n",
];

#[tokio::test]
async fn an_openai_agent_request_reaches_anthropic_with_its_tools_and_calls() {
    let h = Harness::new(Vec::new()).await;
    h.anthropic.answer(Answer::text("hi"));
    let (status, response) = h.text(openai_agent()).await;
    assert_eq!(status, StatusCode::OK, "{response}");
    h.assert_requests(0, 1);

    let sent = h.sent_json(0);
    let tool = json!({ "name": "read", "description": "read a file",
        "input_schema": { "type": "object" } });
    assert_eq!(sent["tools"], json!([tool]));
    let choice = json!({ "type": "tool", "name": "read", "disable_parallel_tool_use": true });
    assert_eq!(sent["tool_choice"], choice);
    assert!(sent["parallel_tool_calls"].is_null());
    let call_made = json!({ "type": "tool_use", "id": "call_1", "name": "read",
        "input": { "path": "a.rs" } });
    let result =
        json!({ "type": "tool_result", "tool_use_id": "call_1", "content": "fn main() {}" });
    assert_eq!(
        sent["messages"],
        json!([
            { "role": "user", "content": "open a.rs" },
            { "role": "assistant", "content": [call_made] },
            { "role": "user", "content": [result] },
            { "role": "user", "content": "now what?" },
        ])
    );
}

#[tokio::test]
async fn an_anthropic_agent_request_reaches_openai_with_its_tools_and_calls() {
    let h = Harness::new(Vec::new()).await;
    h.openai.answer( json_response(StatusCode::OK, r#"{"id":"c","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}"#));
    let (status, _) = h.text(anthropic_agent()).await;
    assert_eq!(status, StatusCode::OK);
    h.assert_requests(1, 0);

    let sent = h.sent_json(0);
    let function = json!({ "name": "read", "description": "read a file",
        "parameters": { "type": "object" } });
    assert_eq!(
        sent["tools"],
        json!([{ "type": "function", "function": function }])
    );
    assert_eq!(
        sent["tool_choice"],
        json!({ "type": "function", "function": { "name": "read" } })
    );
    assert_eq!(sent["parallel_tool_calls"], false);
    let call_made = json!({ "id": "call_1", "type": "function",
        "function": { "name": "read", "arguments": "{\"path\":\"a.rs\"}" } });
    assert_eq!(
        sent["messages"],
        json!([
            { "role": "user", "content": "open a.rs" },
            { "role": "assistant", "content": "Reading.", "tool_calls": [call_made] },
            { "role": "tool", "tool_call_id": "call_1", "content": "fn main() {}" },
            { "role": "user", "content": "now what?" },
        ])
    );
}

#[tokio::test]
async fn a_streamed_anthropic_tool_call_reaches_an_openai_client() {
    let h = Harness::new(Vec::new()).await;
    h.anthropic.answer(sse(ANTHROPIC_TOOL_STREAM));
    // Streamed, with usage.
    let body = json!({ "model": "claude-x", "stream": true, "stream_options": { "include_usage": true },
        "tools": [{ "type": "function", "function": { "name": "read" } }],
        "messages": [{ "role": "user", "content": "hi" }] });
    let req = call(CHAT, body.to_string());
    let (_, out) = h.text(req).await;
    h.assert_requests(0, 1);

    let chunks = events(&out);
    let deltas: Vec<&Value> = chunks
        .iter()
        .map(|(_, d)| &d["choices"][0]["delta"])
        .collect();
    let text: String = deltas
        .iter()
        .filter_map(|d| d["content"].as_str())
        .collect();
    assert_eq!(text, "Reading.");
    let calls: Vec<&Value> = deltas
        .iter()
        .flat_map(|d| d["tool_calls"].as_array())
        .flatten()
        .collect();
    assert_eq!(calls[0]["id"], "toolu_1");
    assert_eq!(calls[0]["function"]["name"], "read");
    assert_eq!(calls[0]["index"], 0);
    let args: String = calls
        .iter()
        .filter_map(|c| c["function"]["arguments"].as_str())
        .collect();
    assert_eq!(args, r#"{"path":"a.rs"}"#);
    let finish = chunks
        .iter()
        .find_map(|(_, d)| d["choices"][0]["finish_reason"].as_str());
    assert_eq!(finish, Some("tool_calls"));
    assert!(out.ends_with("data: [DONE]\n\n"), "{out}");
}

#[tokio::test]
async fn a_streamed_openai_tool_call_reaches_an_anthropic_client() {
    let h = Harness::new(Vec::new()).await;
    h.openai.answer(sse(OPENAI_TOOL_STREAM));
    let body = json!({ "model": "gpt-x", "max_tokens": 100, "stream": true,
        "tools": [{ "name": "read", "input_schema": { "type": "object" } }],
        "messages": [{ "role": "user", "content": "hi" }] });
    let (_, out) = h.text(call(MESSAGES, body.to_string())).await;
    h.assert_requests(1, 0);

    let sent = events(&out);
    let kinds: Vec<&str> = sent
        .iter()
        .filter_map(|(_, d)| d["type"].as_str())
        .collect();
    assert_eq!(
        kinds,
        [
            "message_start",
            "content_block_start",
            "content_block_delta",
            "content_block_delta",
            "content_block_stop",
            "message_delta",
            "message_stop",
        ]
    );
    let start = &sent[1].1;
    assert_eq!(
        start["content_block"],
        json!({ "type": "tool_use", "id": "call_1", "name": "read", "input": {} })
    );
    let args: String = sent
        .iter()
        .filter_map(|(_, d)| d["delta"]["partial_json"].as_str())
        .collect();
    assert_eq!(args, r#"{"path":"a.rs"}"#);
    assert_eq!(sent[5].1["delta"]["stop_reason"], "tool_use");
}

#[tokio::test]
async fn a_whole_tool_call_crosses_formats_both_ways() {
    // Anthropic's answer to an OpenAI client.
    let h = Harness::new(Vec::new()).await;
    let answer = json!({ "id": "m", "type": "message", "role": "assistant", "model": "c",
        "content": [{ "type": "tool_use", "id": "t1", "name": "read", "input": { "path": "a" } }],
        "stop_reason": "tool_use", "usage": { "input_tokens": 1, "output_tokens": 1 } });
    h.anthropic
        .answer(json_response(StatusCode::OK, &answer.to_string()));
    let body = json!({ "model": "claude-x", "messages": [{ "role": "user", "content": "hi" }],
        "tools": [{ "type": "function", "function": { "name": "read" } }] });
    let (_, out) = h.text(call(CHAT, body.to_string())).await;
    h.assert_requests(0, 1);
    let v: Value = serde_json::from_str(&out).unwrap_or_default();
    let message = &v["choices"][0]["message"];
    assert!(message["content"].is_null(), "{out}");
    assert_eq!(
        message["tool_calls"][0]["function"]["arguments"],
        r#"{"path":"a"}"#
    );
    assert_eq!(v["choices"][0]["finish_reason"], "tool_calls");

    // OpenAI's answer to an Anthropic client.
    let h = Harness::new(Vec::new()).await;
    let answer = json!({ "id": "c", "model": "g", "choices": [{ "index": 0, "finish_reason": "tool_calls",
        "message": { "role": "assistant", "content": null, "tool_calls": [
            { "id": "c1", "type": "function", "function": { "name": "read", "arguments": "{\"path\":\"a\"}" } }] } }],
        "usage": { "prompt_tokens": 1, "completion_tokens": 1 } });
    h.openai
        .answer(json_response(StatusCode::OK, &answer.to_string()));
    let body = json!({ "model": "gpt-x", "max_tokens": 9, "messages": [{ "role": "user", "content": "hi" }],
        "tools": [{ "name": "read", "input_schema": { "type": "object" } }] });
    let (_, out) = h.text(call(MESSAGES, body.to_string())).await;
    h.assert_requests(1, 0);
    let v: Value = serde_json::from_str(&out).unwrap_or_default();
    assert_eq!(
        v["content"],
        json!([{ "type": "tool_use", "id": "c1", "name": "read", "input": { "path": "a" } }])
    );
    assert_eq!(v["stop_reason"], "tool_use");
}

#[tokio::test]
async fn generated_tool_calls_cross_both_ways_whole_and_streamed() {
    let args = json!({ "path": "src/lib.rs", "line": 17 });
    for stream in [false, true] {
        let h = Harness::new(Vec::new()).await;
        h.anthropic.answer(Answer::tool_call("read", args.clone()));
        let request = json!({
            "model": "claude-sonnet-4-5", "stream": stream,
            "stream_options": { "include_usage": stream },
            "messages": [{ "role": "user", "content": "read src/lib.rs" }],
            "tools": [{ "type": "function", "function": {
                "name": "read", "parameters": { "type": "object" }
            }}],
        });
        let (status, out) = h.text(call(CHAT, request.to_string())).await;
        assert_eq!(status, StatusCode::OK, "{out}");
        h.assert_requests(0, 1);
        assert_eq!(h.anthropic.requests()[0].body["model"], "claude-sonnet-4-5");
        if stream {
            let chunks = events(&out);
            let calls: Vec<&Value> = chunks
                .iter()
                .flat_map(|(_, c)| {
                    c.pointer("/choices/0/delta/tool_calls")
                        .and_then(Value::as_array)
                })
                .flatten()
                .collect();
            assert_eq!(calls[0]["function"]["name"], "read", "{out}");
            let arguments: String = calls
                .iter()
                .filter_map(|c| c["function"]["arguments"].as_str())
                .collect();
            assert_eq!(
                serde_json::from_str::<Value>(&arguments).expect("valid tool JSON"),
                args
            );
            assert!(
                chunks
                    .iter()
                    .any(|(_, c)| c["choices"][0]["finish_reason"] == "tool_calls")
            );
            assert!(chunks.iter().any(|(_, c)| {
                c["usage"]["completion_tokens"]
                    .as_u64()
                    .is_some_and(|n| n > 0)
            }));
            assert!(out.ends_with("data: [DONE]\n\n"), "{out}");
        } else {
            let response: Value = serde_json::from_str(&out).expect("valid response JSON");
            let call = &response["choices"][0]["message"]["tool_calls"][0];
            assert_eq!(call["function"]["name"], "read");
            assert_eq!(
                serde_json::from_str::<Value>(
                    call["function"]["arguments"]
                        .as_str()
                        .expect("tool arguments")
                )
                .expect("valid tool JSON"),
                args
            );
            assert_eq!(response["choices"][0]["finish_reason"], "tool_calls");
            assert!(
                response["usage"]["completion_tokens"]
                    .as_u64()
                    .is_some_and(|n| n > 0)
            );
        }

        let h = Harness::new(Vec::new()).await;
        h.openai.answer(Answer::tool_call("read", args.clone()));
        let request = json!({
            "model": "gpt-4o", "max_tokens": 100, "stream": stream,
            "messages": [{ "role": "user", "content": "read src/lib.rs" }],
            "tools": [{ "name": "read", "input_schema": { "type": "object" } }],
        });
        let (status, out) = h.text(call(MESSAGES, request.to_string())).await;
        assert_eq!(status, StatusCode::OK, "{out}");
        h.assert_requests(1, 0);
        assert_eq!(h.openai.requests()[0].body["model"], "gpt-4o");
        if stream {
            let frames = events(&out);
            let starts: Vec<&Value> = frames
                .iter()
                .filter(|(name, _)| name.as_deref() == Some("content_block_start"))
                .map(|(_, body)| &body["content_block"])
                .collect();
            assert_eq!(starts.len(), 1, "{out}");
            assert_eq!(starts[0]["name"], "read");
            let arguments: String = frames
                .iter()
                .filter_map(|(_, body)| body["delta"]["partial_json"].as_str())
                .collect();
            assert_eq!(
                serde_json::from_str::<Value>(&arguments).expect("valid tool JSON"),
                args
            );
            assert!(
                frames
                    .iter()
                    .any(|(_, body)| body["delta"]["stop_reason"] == "tool_use")
            );
            assert!(frames.iter().any(|(_, body)| {
                body["usage"]["output_tokens"]
                    .as_u64()
                    .is_some_and(|n| n > 0)
            }));
        } else {
            let response: Value = serde_json::from_str(&out).expect("valid response JSON");
            assert_eq!(response["content"][0]["type"], "tool_use");
            assert_eq!(response["content"][0]["name"], "read");
            assert_eq!(response["content"][0]["input"], args);
            assert_eq!(response["stop_reason"], "tool_use");
            assert!(
                response["usage"]["output_tokens"]
                    .as_u64()
                    .is_some_and(|n| n > 0)
            );
        }
    }
}

#[tokio::test]
async fn generated_named_model_text_and_usage_relay_in_native_formats() {
    let router = ModelRouter::new(|c: &Conversation| c.model.as_deref()).route("gpt", "openai");
    let h = Harness::with(vec![Arc::new(router)]).await;
    for stream in [false, true] {
        h.openai.answer(Answer::text("Hello from OpenAI"));
        let mut req = json!({ "model": "gpt-4o", "stream": stream,
            "messages": [{ "role": "user", "content": "hi" }] });
        if stream {
            req["stream_options"] = json!({ "include_usage": true });
        }
        let (status, out) = h.text(call(CHAT, req.to_string())).await;
        assert_eq!(status, StatusCode::OK, "{out}");
        if stream {
            let frames = events(&out);
            let text: String = frames
                .iter()
                .filter_map(|(_, c)| {
                    c.pointer("/choices/0/delta/content")
                        .and_then(Value::as_str)
                })
                .collect();
            assert_eq!(text, "Hello from OpenAI");
            assert!(
                frames
                    .iter()
                    .any(|(_, c)| c["usage"]["prompt_tokens"].as_u64().is_some_and(|n| n > 0))
            );
            assert!(out.ends_with("data: [DONE]\n\n"), "{out}");
        } else {
            let response: Value = serde_json::from_str(&out).expect("valid response JSON");
            assert_eq!(
                response["choices"][0]["message"]["content"],
                "Hello from OpenAI"
            );
            assert!(
                response["usage"]["prompt_tokens"]
                    .as_u64()
                    .is_some_and(|n| n > 0)
            );
        }

        h.anthropic.answer(Answer::text("Hello from Anthropic"));
        let req = json!({ "model": "claude-sonnet-4-5", "max_tokens": 100,
            "stream": stream, "messages": [{ "role": "user", "content": "hi" }] });
        let (status, out) = h.text(call(MESSAGES, req.to_string())).await;
        assert_eq!(status, StatusCode::OK, "{out}");
        if stream {
            assert_eq!(anthropic_texts(&out), "Hello from Anthropic");
            assert!(
                events(&out)
                    .iter()
                    .any(|(_, c)| c["usage"]["output_tokens"].as_u64().is_some_and(|n| n > 0))
            );
        } else {
            let response: Value = serde_json::from_str(&out).expect("valid response JSON");
            assert_eq!(response["content"][0]["text"], "Hello from Anthropic");
            assert!(
                response["usage"]["output_tokens"]
                    .as_u64()
                    .is_some_and(|n| n > 0)
            );
        }
    }
    h.assert_requests(2, 2);
    assert!(
        h.openai
            .requests()
            .iter()
            .all(|r| r.body["model"] == "gpt-4o")
    );
    assert!(
        h.anthropic
            .requests()
            .iter()
            .all(|r| r.body["model"] == "claude-sonnet-4-5")
    );
}

#[tokio::test]
async fn generated_anthropic_reasoning_relays_without_reinterpreting_thinking() {
    let h = Harness::with(Vec::new()).await;
    for stream in [false, true] {
        h.anthropic
            .answer(Answer::text("Answer").reasoning("First consider the question"));
        let req = json!({
            "model": "claude-sonnet-4-5", "max_tokens": 2048, "stream": stream,
            "thinking": { "type": "enabled", "budget_tokens": 1024, "display": "summarized" },
            "messages": [{ "role": "user", "content": "hi" }],
        });
        let (status, out) = h.text(call(MESSAGES, req.to_string())).await;
        assert_eq!(status, StatusCode::OK, "{out}");
        if stream {
            let frames = events(&out);
            let thinking: String = frames
                .iter()
                .filter_map(|(_, c)| c["delta"]["thinking"].as_str())
                .collect();
            assert_eq!(thinking, "First consider the question");
            assert_eq!(anthropic_texts(&out), "Answer");
            assert!(frames.iter().any(|(_, c)| {
                c["usage"]["output_tokens_details"]["thinking_tokens"]
                    .as_u64()
                    .is_some_and(|n| n > 0)
            }));
        } else {
            let response: Value = serde_json::from_str(&out).expect("valid response JSON");
            assert_eq!(response["content"][0]["type"], "thinking");
            assert_eq!(
                response["content"][0]["thinking"],
                "First consider the question"
            );
            assert_eq!(response["content"][1]["text"], "Answer");
            assert!(
                response["usage"]["output_tokens_details"]["thinking_tokens"]
                    .as_u64()
                    .is_some_and(|n| n > 0)
            );
        }
    }
    h.assert_requests(0, 2);
}

#[tokio::test]
async fn a_tool_that_anthropic_runs_cannot_be_sent_to_openai() {
    let h = Harness::new(Vec::new()).await;
    let body = json!({ "model": "gpt-x", "max_tokens": 9, "messages": [{ "role": "user", "content": "hi" }],
        "tools": [{ "type": "web_search_20250305", "name": "web_search" }] });
    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{out}");
    assert!(h.sent().is_empty());
}

#[tokio::test]
async fn a_cut_translated_stream_ends_with_an_error_in_the_client_format() {
    let h = Harness::new(Vec::new()).await;
    h.anthropic.answer(sse(&ANTHROPIC_STREAM[..3]));

    let (_, body) = h.text(openai_request("claude-x", true)).await;

    let last = events(&body).pop().map(|(_, d)| d).unwrap_or_default();
    assert_eq!(last["error"]["type"], "truncated");
    assert!(!body.contains("[DONE]"));
    assert!(matches!(h.log.only(), Err(f) if f.code() == codes::TRUNCATED));
}

#[tokio::test]
async fn a_filter_can_turn_content_of_another_format_into_text() {
    /// Reports that the model thought, from Anthropic's thinking block.
    struct Thought;

    #[async_trait]
    impl Plugin for Thought {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(|r: &mut Reply| {
                for piece in &mut r.content {
                    if let Piece::Other { format, raw, .. } = piece
                        && &**format == "anthropic-messages"
                        && raw["type"] == "thinking"
                    {
                        *piece = Piece::text(0, "[thought] ");
                    }
                }
                Ok(())
            })?;
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Thought)]).await;
    let thinking = "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"thinking\",\"thinking\":\"\"}}\n\n";
    let mut stream = ANTHROPIC_STREAM.to_vec();
    stream.insert(1, thinking);
    h.anthropic.answer(sse(&stream));

    let (status, body) = h.text(openai_request("claude-x", true)).await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains("[thought] "), "{body}");
}

#[tokio::test]
async fn a_translated_stream_that_starts_with_what_cannot_be_translated_is_refused() {
    let h = Harness::new(Vec::new()).await;
    let thinking = "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"thinking\",\"thinking\":\"\"}}\n\n";
    h.anthropic.answer(sse(&[ANTHROPIC_STREAM[0], thinking]));

    let (status, body) = h.text(openai_request("claude-x", true)).await;

    // Nothing was sent before the failure, so it is a refusal.
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{body}");
    assert!(!body.contains("\"role\":\"assistant\""), "{body}");
}

#[tokio::test]
async fn a_translated_chunk_that_fails_partway_is_refused_whole() {
    let h = Harness::new(Vec::new()).await;
    let chunk = r#"data: {"id":"c","model":"gpt-x","choices":[{"index":0,"delta":{"content":"hi","refusal":"no"}}]}

"#;
    h.openai.answer(sse(&[chunk]));

    let (status, body) = h.text(anthropic_request(true)).await;

    // Its text was not sent before its refusal failed.
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{body}");
    assert!(!body.contains("message_start"), "{body}");
}

#[tokio::test]
async fn an_error_in_a_success_is_a_failure_not_an_empty_answer() {
    let h = Harness::new(Vec::new()).await;
    let error = r#"{"error":{"message":"boom","type":"server_error"}}"#;
    h.openai.answer(json_response(StatusCode::OK, error));

    let (status, body) = h.text(anthropic_request(false)).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY, "{body}");
    assert!(body.contains("\"type\":\"server_error\""), "{body}");
    assert!(h.log.only().is_err());
}

#[tokio::test]
async fn a_success_that_is_not_a_response_is_a_failure_not_an_empty_answer() {
    let h = Harness::new(Vec::new()).await;
    h.openai
        .answer(json_response(StatusCode::OK, r#"{"status":"queued"}"#));

    let (status, body) = h.text(anthropic_request(false)).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY, "{body}");
    assert!(h.log.only().is_err());
}

#[tokio::test]
async fn a_filter_edits_translated_text() {
    let h = Harness::new(vec![Arc::new(Shout)]).await;
    h.openai.answer(sse(OPENAI_STREAM));

    let (_, body) = h.text(anthropic_request(true)).await;

    let text: String = events(&body)
        .iter()
        .filter_map(|(_, d)| {
            d.pointer("/delta/text")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .collect();
    assert_eq!(text, "HELLO WORLD");
}

/// Answers with a stream it makes from facts, like a semantic cache.
struct Canned;

#[async_trait]
impl Plugin for Canned {
    async fn on_client(&self, _cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let events = [
            Reply::start("cached_1", "claude-x"),
            Reply::text(0, "Hi"),
            Reply::finished(Finish::Stop),
        ];
        let events = futures::stream::iter(events.map(Ok));
        Ok(Some(Response::stream(events)))
    }
}

#[tokio::test]
async fn a_plugin_can_stream_from_facts_in_the_client_format() {
    let h = Harness::new(vec![Arc::new(Canned)]).await;

    let (_, body) = h.text(anthropic_request(true)).await;

    let names: Vec<Option<String>> = events(&body).into_iter().map(|(e, _)| e).collect();
    let names: Vec<&str> = names.iter().flatten().map(String::as_str).collect();
    assert_eq!(
        names,
        [
            "message_start",
            "content_block_start",
            "content_block_delta",
            "content_block_stop",
            "message_delta",
            "message_stop",
        ]
    );
    assert!(body.contains("\"id\":\"cached_1\""), "{body}");
    assert!(h.sent().is_empty());
}

#[tokio::test]
async fn an_anthropic_stream_is_relayed_byte_for_byte() {
    let h = Harness::new(Vec::new()).await;
    h.anthropic.answer(sse(ANTHROPIC_STREAM));
    // Same format end to end: nothing is translated, so tools are fine.
    let body = json!({
        "model": "claude-x",
        "max_tokens": 10,
        "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
        "tools": [{ "name": "f", "input_schema": { "type": "object" } }],
    });

    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(out, ANTHROPIC_STREAM.concat());
}

#[tokio::test]
async fn the_clients_query_is_kept_within_one_format_only() {
    let h = Harness::new(Vec::new()).await;
    h.anthropic.answer(sse(ANTHROPIC_STREAM));
    h.openai.answer(sse(OPENAI_STREAM));
    for model in ["claude-x", "gpt-x"] {
        let body = json!({
            "model": model,
            "max_tokens": 10,
            "stream": true,
            "messages": [{ "role": "user", "content": "hi" }],
        });
        let mut req = call(MESSAGES, body.to_string());
        *req.uri_mut() = http::Uri::from_static("/v1/messages?beta=true");
        assert_eq!(h.text(req).await.0, StatusCode::OK);
    }

    let anthropic = h.anthropic.requests();
    let openai = h.openai.requests();
    assert_eq!((anthropic.len(), openai.len()), (1, 1));
    assert_eq!(anthropic[0].path, "/v1/messages");
    assert_eq!(anthropic[0].query, "beta=true");
    assert_eq!(openai[0].path, "/v1/chat/completions");
    assert!(openai[0].query.is_empty());
}

#[tokio::test]
async fn a_model_nothing_answers_is_a_404_in_the_client_format() {
    let h = Harness::with(Vec::new()).await;

    let (status, body) = h.text(call(CHAT, openai_body("llama-3", false))).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    let v: Value = serde_json::from_str(&body).unwrap_or_default();
    assert_eq!(v["error"]["message"], "nothing answers this request");
    assert!(h.sent().is_empty());
}

#[tokio::test]
async fn an_openai_error_reaches_an_anthropic_client_in_its_format() {
    let error = r#"{"error":{"message":"bad key","type":"invalid_request_error"}}"#;
    let h = Harness::with_local(
        vec![Arc::new(Cross)],
        Some(local_json(StatusCode::UNAUTHORIZED, error)),
        None,
    )
    .await;

    let (status, body) = h.text(anthropic_request(false)).await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let v: Value = serde_json::from_str(&body).unwrap_or_default();
    assert_eq!(
        v,
        json!({ "type": "error", "error": { "type": "invalid_request_error", "message": "bad key" } })
    );
}

#[tokio::test]
async fn provider_stream_quirks_translate_cleanly() {
    // Azure's shape: an opening chunk with no id, then finish and usage in
    // one chunk.
    let quirky = [
        "data: {\"id\":\"\",\"choices\":[],\"prompt_filter_results\":[]}\n\n",
        "data: {\"id\":\"c9\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hi\"}}]}\n\n",
        "data: {\"id\":\"c9\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"length\"}],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":1}}\n\n",
        "data: [DONE]\n\n",
    ];
    let h = Harness::new(Vec::new()).await;
    h.openai.answer(sse(&quirky));

    let (_, body) = h.text(anthropic_request(true)).await;

    let events = events(&body);
    assert_eq!(events[0].1["message"]["id"], "c9");
    let delta = events
        .iter()
        .find(|(e, _)| e.as_deref() == Some("message_delta"));
    assert_eq!(
        delta.map(|(_, d)| d.clone()),
        Some(json!({
            "type": "message_delta",
            "delta": { "stop_reason": "max_tokens", "stop_sequence": null },
            "usage": { "input_tokens": 3, "output_tokens": 1 },
        }))
    );
}

#[tokio::test]
async fn an_anthropic_error_event_reaches_an_openai_client_with_its_code() {
    let mut stream = ANTHROPIC_STREAM[..3].to_vec();
    stream.push("event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n");
    let h = Harness::new(Vec::new()).await;
    h.anthropic.answer(sse(&stream));

    let (_, body) = h.text(openai_request("claude-x", true)).await;

    let last = events(&body).pop().map(|(_, d)| d).unwrap_or_default();
    assert_eq!(last["error"]["type"], "overloaded_error");
    assert!(matches!(h.log.only(), Err(f) if f.code() == "overloaded_error"));
}

#[tokio::test]
async fn several_anthropic_text_blocks_reach_an_openai_client_in_order() {
    let reply = json!({
        "id": "msg_3", "type": "message", "role": "assistant", "model": "claude-x",
        "content": [{ "type": "text", "text": "one " }, { "type": "text", "text": "two" }],
        "stop_reason": "end_turn",
        "usage": { "input_tokens": 1, "output_tokens": 2 },
    });
    let h = Harness::new(Vec::new()).await;
    h.anthropic
        .answer(json_response(StatusCode::OK, &reply.to_string()));

    let (_, body) = h.text(openai_request("claude-x", false)).await;

    let v: Value = serde_json::from_str(&body).unwrap_or_default();
    assert_eq!(v["choices"][0]["message"]["content"], "one two");
}

#[tokio::test]
async fn a_filter_edits_an_anthropic_stream_in_its_own_shape() {
    let h = Harness::new(vec![Arc::new(Shout)]).await;
    h.anthropic.answer(sse(ANTHROPIC_STREAM));
    let body = json!({
        "model": "claude-x",
        "max_tokens": 10,
        "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    let (_, out) = h.text(call(MESSAGES, body.to_string())).await;

    let expected = ANTHROPIC_STREAM
        .concat()
        .replace("\"text\":\"Hello\"", "\"text\":\"HELLO\"")
        .replace("\"text\":\" world\"", "\"text\":\" WORLD\"");
    assert_eq!(out, expected);
}

#[tokio::test]
async fn a_translated_stream_split_mid_event_decodes_once() {
    let whole = ANTHROPIC_STREAM.concat();
    let (a, b) = whole.split_at(whole.len() / 2);
    let split = http::Response::builder()
        .header("content-type", "text/event-stream")
        .body(vec![
            Ok(bytes::Bytes::from(a.to_owned())),
            Ok(bytes::Bytes::from(b.to_owned())),
        ])
        .unwrap_or_else(|e| panic!("{e}"));
    let h = Harness::with_local(vec![Arc::new(Cross)], None, Some(one_response(split))).await;

    let (_, body) = h.text(openai_request("claude-x", true)).await;

    let text: String = events(&body)
        .iter()
        .filter_map(|(_, c)| {
            c.pointer("/choices/0/delta/content")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .collect();
    assert_eq!(text, "Hello world");
    assert_eq!(h.log.only(), Ok(()));
}

#[tokio::test]
async fn a_filtered_stream_that_ends_in_a_provider_error_ends_with_that_error() {
    let mut stream = ANTHROPIC_STREAM[..3].to_vec();
    stream.push("event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n");
    let h = Harness::new(vec![Arc::new(Shout)]).await;
    h.anthropic.answer(sse(&stream));
    let body = json!({
        "model": "claude-x",
        "max_tokens": 10,
        "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    let (_, out) = h.text(call(MESSAGES, body.to_string())).await;

    let (name, data) = events(&out).pop().unwrap_or_default();
    assert_eq!(name.as_deref(), Some("error"));
    assert_eq!(data["error"]["type"], "overloaded_error");
    assert!(matches!(h.log.only(), Err(f) if f.code() == "overloaded_error"));
}

#[tokio::test]
async fn anthropic_beta_features_reach_anthropic() {
    let h = Harness::new(Vec::new()).await;
    h.anthropic.answer(sse(ANTHROPIC_STREAM));
    let body = json!({
        "model": "claude-x",
        "max_tokens": 10,
        "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
    });
    let request = http::Request::post("/v1/messages")
        .header("anthropic-beta", "context-1m-2025-08-07")
        .header("anthropic-beta", "files-api-2025-04-14")
        .body(())
        .unwrap_or_default()
        .map(|()| post(MESSAGES, body.to_string()).into_body());

    h.text(request).await;

    let sent = h.sent();
    let beta: Vec<_> = sent[0].headers.get_all("anthropic-beta").iter().collect();
    assert_eq!(beta, ["context-1m-2025-08-07", "files-api-2025-04-14"]);
}

#[tokio::test]
async fn a_system_message_mid_conversation_is_refused_not_moved() {
    let h = Harness::new(Vec::new()).await;
    let body = json!({
        "model": "claude-x",
        "messages": [
            { "role": "user", "content": "hi" },
            { "role": "system", "content": "be brief" },
        ],
    });

    let (status, _) = h.text(call(CHAT, body.to_string())).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(h.sent().is_empty());
}

#[tokio::test]
async fn a_filter_edits_events_that_share_a_chunk_while_translating() {
    struct Truncated;

    #[async_trait]
    impl Plugin for Truncated {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(|r: &mut Reply| {
                if let Some(start) = &mut r.start {
                    start.model = "alias".into();
                }
                if let Some(finish) = &mut r.finish {
                    *finish = Finish::Length;
                }
                Ok(())
            })?;
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Truncated)]).await;
    let chunk = "data: {\"id\":\"c1\",\"model\":\"gpt-real\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":\"stop\"}]}\n\n";
    h.openai.answer(sse(&[chunk, "data: [DONE]\n\n"]));

    let (_, body) = h.text(anthropic_request(true)).await;

    let events = events(&body);
    assert_eq!(events[0].1["message"]["model"], "alias");
    let stop = events
        .iter()
        .find_map(|(_, d)| d.pointer("/delta/stop_reason"));
    assert_eq!(stop, Some(&json!("max_tokens")));
}

#[tokio::test]
async fn usage_counts_even_when_its_chunk_cannot_be_translated() {
    let h = Harness::new(Vec::new()).await;
    // A provider that reports usage on a tool call chunk.
    let chunk = "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"{}\"}}]}}],\"usage\":{\"prompt_tokens\":9,\"completion_tokens\":1}}\n\n";
    h.openai.answer(sse(&[chunk]));

    let _ = h.text(anthropic_request(true)).await;

    assert_eq!(h.usage.total().get(units::INPUT_TOKENS), Some(9.0));
    assert!(h.log.only().is_err());
}

#[tokio::test]
async fn a_translated_response_keeps_the_providers_retry_after() {
    let h = Harness::new(Vec::new()).await;
    h.openai
        .answer(Answer::error(429).message("slow down").retry_after(7));

    let (status, headers, _) = read_whole(serve(&h.gw, anthropic_request(false)).await.0).await;

    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        headers.get("retry-after"),
        Some(&HeaderValue::from_static("7"))
    );
}

#[tokio::test]
async fn an_empty_plugin_made_stream_is_a_whole_anthropic_stream() {
    struct Silent;

    #[async_trait]
    impl Plugin for Silent {
        async fn on_client(&self, _cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let none = futures::stream::empty::<Result<Reply, Fault>>();
            Ok(Some(Response::stream(none)))
        }
    }

    let h = Harness::new(vec![Arc::new(Silent)]).await;

    let (_, body) = h.text(anthropic_request(true)).await;

    let names: Vec<String> = events(&body).into_iter().filter_map(|(e, _)| e).collect();
    assert_eq!(names, ["message_start", "message_delta", "message_stop"]);
}

#[tokio::test]
async fn a_real_openai_reply_reaches_an_anthropic_client() {
    let h = Harness::new(Vec::new()).await;
    // OpenAI's real shape, with null and empty fields.
    let reply = r#"{"id":"chatcmpl-1","object":"chat.completion","created":1,"model":"gpt-x","choices":[{"index":0,"message":{"role":"assistant","content":"hi","refusal":null,"annotations":[]},"logprobs":null,"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":1,"total_tokens":4,"prompt_tokens_details":{"cached_tokens":0,"audio_tokens":0},"completion_tokens_details":{"reasoning_tokens":0,"audio_tokens":0,"accepted_prediction_tokens":0,"rejected_prediction_tokens":0}},"service_tier":"default","system_fingerprint":"fp"}"#;
    h.openai.answer(json_response(StatusCode::OK, reply));

    let (status, body) = h.text(anthropic_request(false)).await;

    assert_eq!(status, StatusCode::OK, "{body}");
    let v: Value = serde_json::from_str(&body).unwrap_or_default();
    assert_eq!(v["content"], json!([{ "type": "text", "text": "hi" }]));
}

/// Anthropic's stream with a placeholder split across its deltas and a `<`
/// left open at the end.
fn masked_anthropic_stream() -> Vec<String> {
    let delta = |text: &str| {
        let data = json!({"type": "content_block_delta", "index": 0,
            "delta": {"type": "text_delta", "text": text}});
        format!("event: content_block_delta\ndata: {data}\n\n")
    };
    let mut stream: Vec<String> = ANTHROPIC_STREAM.iter().map(|s| s.to_string()).collect();
    stream.splice(2..4, [delta("Hi <P"), delta("1>, bye <")]);
    stream
}

fn anthropic_texts(body: &str) -> String {
    events(body)
        .iter()
        .filter_map(|(_, v)| v.pointer("/delta/text").and_then(Value::as_str))
        .collect()
}

/// Asserts Anthropic's event order: the message starts first and stops
/// last, and each block opens before it closes. Keep-alives go where they
/// came, anywhere.
fn assert_well_formed(names: &[String], out: &str) {
    let names: Vec<&String> = names.iter().filter(|n| *n != "ping").collect();
    assert_eq!(
        names.first().map(|n| n.as_str()),
        Some("message_start"),
        "{out}"
    );
    assert_eq!(
        names.last().map(|n| n.as_str()),
        Some("message_stop"),
        "{out}"
    );
    let mut open = 0_i32;
    for name in names {
        match name.as_str() {
            "content_block_start" => open += 1,
            "content_block_stop" => open -= 1,
            _ => {}
        }
        assert!((0..=1).contains(&open), "{out}");
    }
}

#[tokio::test]
async fn a_filter_restores_text_across_an_anthropic_stream_and_releases_the_rest() {
    let h = Harness::new(vec![Arc::new(Unmask)]).await;
    let stream = masked_anthropic_stream();
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.anthropic.answer(sse(&chunks));
    let body = json!({
        "model": "claude-x", "max_tokens": 10, "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(anthropic_texts(&out), "Hi Alice, bye <");
    // What was kept back goes into Anthropic's own block, before it closes.
    let names: Vec<String> = events(&out)
        .into_iter()
        .filter_map(|(name, _)| name)
        .collect();
    let tail = &names[names.len() - 4..];
    assert_eq!(
        tail,
        [
            "content_block_delta",
            "content_block_stop",
            "message_delta",
            "message_stop"
        ]
    );
    assert!(!out.contains("\"index\":1"), "{out}");
    assert_eq!(h.log.only(), Ok(()));
}

#[tokio::test]
async fn a_filter_restores_text_across_a_translated_stream() {
    let h = Harness::new(vec![Arc::new(Unmask)]).await;
    let stream = masked_anthropic_stream();
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.anthropic.answer(sse(&chunks));

    let (_, body) = h.text(openai_request("claude-x", true)).await;

    let text: String = events(&body)
        .iter()
        .filter_map(|(_, c)| {
            c.pointer("/choices/0/delta/content")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .collect();
    assert_eq!(text, "Hi Alice, bye <");
    assert!(body.ends_with("data: [DONE]\n\n"));
}

#[tokio::test]
async fn a_filter_releases_what_it_held_into_a_whole_message() {
    let h = Harness::new(vec![Arc::new(Unmask)]).await;
    let reply = json!({
        "id": "msg_1", "type": "message", "role": "assistant", "model": "claude-x",
        "content": [{ "type": "text", "text": "Hi <P1>, bye <", "citations": null }],
        "stop_reason": "end_turn", "usage": { "input_tokens": 5, "output_tokens": 2 },
    });
    h.anthropic
        .answer(json_response(StatusCode::OK, &reply.to_string()));
    let body = json!({
        "model": "claude-x", "max_tokens": 10,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;

    assert_eq!(status, StatusCode::OK, "{out}");
    let out: Value = serde_json::from_str(&out).unwrap_or_default();
    assert_eq!(out["content"][0]["text"], "Hi Alice, bye <");
    assert_eq!(out["content"][0]["citations"], Value::Null);
    assert_eq!(out["id"], "msg_1");
}

#[tokio::test]
async fn anthropics_own_end_is_kept_when_it_breaks_off_after_it() {
    let h = Harness::new(vec![Arc::new(Unmask)]).await;
    let cut = &ANTHROPIC_STREAM[..ANTHROPIC_STREAM.len() - 1];
    h.anthropic.answer(sse(cut));
    let body = json!({
        "model": "claude-x", "max_tokens": 10, "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    let (_, out) = h.text(call(MESSAGES, body.to_string())).await;

    let names: Vec<String> = events(&out).into_iter().filter_map(|(n, _)| n).collect();
    assert_eq!(names[names.len() - 2..], ["message_delta", "error"]);
}

#[tokio::test]
async fn a_relayed_stream_cut_after_its_last_block_keeps_every_byte() {
    let h = Harness::new(Vec::new()).await;
    let cut = &ANTHROPIC_STREAM[..ANTHROPIC_STREAM.len() - 1];
    h.anthropic.answer(sse(cut));
    let body = json!({
        "model": "claude-x", "max_tokens": 10, "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    let (_, out) = h.text(call(MESSAGES, body.to_string())).await;

    assert!(out.starts_with(&cut.concat()), "{out}");
    assert!(
        out[cut.concat().len()..].starts_with("event: error"),
        "{out}"
    );
}

/// Fails the end of a response that failed, as an audit might.
struct Audit;

impl Filter<Reply> for Audit {
    fn edit(&mut self, event: Reply, out: &mut Vec<Reply>) -> Result<(), Fault> {
        out.push(event);
        Ok(())
    }

    fn end(&mut self, end: &Result<(), Fault>, _out: &mut Vec<Reply>) -> Result<(), Fault> {
        match end {
            Ok(()) => Ok(()),
            _ => Err(Fault::upstream("audit: failed response")),
        }
    }
}

#[tokio::test]
async fn a_providers_error_body_is_the_whole_body_whatever_a_filter_says() {
    struct Audited;

    #[async_trait]
    impl Plugin for Audited {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(Audit)?;
            Ok(None)
        }
    }

    let limited = r#"{"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}"#;
    let h = Harness::with_local(
        vec![Arc::new(Cross), Arc::new(Audited)],
        None,
        Some(local_json(StatusCode::TOO_MANY_REQUESTS, limited)),
    )
    .await;
    let body = json!({
        "model": "claude-x", "max_tokens": 10,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;

    assert_eq!(
        (status, out.as_str()),
        (StatusCode::TOO_MANY_REQUESTS, limited)
    );
}

/// Adds a footer in a block of its own.
struct NewBlock;

#[async_trait]
impl Plugin for NewBlock {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.filter(Footer(1, None))?;
        Ok(None)
    }
}

#[tokio::test]
async fn a_block_a_filter_adds_at_the_end_opens_after_anthropics_own() {
    let h = Harness::new(vec![Arc::new(NewBlock)]).await;
    h.anthropic.answer(sse(ANTHROPIC_STREAM));
    let body = json!({
        "model": "claude-x", "max_tokens": 10, "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;

    assert_eq!(status, StatusCode::OK, "{out}");
    let starts: Vec<Value> = events(&out)
        .into_iter()
        .filter(|(n, _)| n.as_deref() == Some("content_block_start"))
        .map(|(_, v)| v["index"].clone())
        .collect();
    assert_eq!(starts, [json!(0), json!(1)]);
    assert_eq!(anthropic_texts(&out), "Hello world [footer]");
    let delta = events(&out)
        .into_iter()
        .find(|(n, _)| n.as_deref() == Some("message_delta"));
    let delta = delta.map(|(_, v)| v).unwrap_or_default();
    assert_eq!(
        delta.pointer("/delta/stop_reason"),
        Some(&json!("end_turn"))
    );
}

#[tokio::test]
async fn a_block_a_filter_adds_to_a_whole_message_is_appended() {
    let h = Harness::new(vec![Arc::new(NewBlock)]).await;
    let reply = json!({
        "id": "msg_1", "type": "message", "role": "assistant", "model": "claude-x",
        "content": [{ "type": "text", "text": "Hello" }],
        "stop_reason": "end_turn", "usage": { "input_tokens": 5, "output_tokens": 2 },
    });
    h.anthropic
        .answer(json_response(StatusCode::OK, &reply.to_string()));
    let body = json!({
        "model": "claude-x", "max_tokens": 10,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;

    assert_eq!(status, StatusCode::OK, "{out}");
    let out: Value = serde_json::from_str(&out).unwrap_or_default();
    assert_eq!(out["content"][1]["text"], " [footer]");
    assert_eq!(out["stop_reason"], "end_turn");
}

async fn anthropic_messages(stream: &[String]) -> (Harness, String) {
    let body = json!({
        "model": "claude-x", "max_tokens": 10, "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
    });
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    let h = Harness::new(vec![Arc::new(Unmask)]).await;
    h.anthropic.answer(sse(&chunks));
    (h, body.to_string())
}

#[tokio::test]
async fn text_released_before_a_tool_call_stays_in_its_own_block() {
    let tool = |name: &str, data: Value| format!("event: {name}\ndata: {data}\n\n");
    let mut stream = masked_anthropic_stream();
    stream.splice(
        5..5,
        [
            tool(
                "content_block_start",
                json!({"type": "content_block_start", "index": 1,
                "content_block": {"type": "tool_use", "id": "t", "name": "f", "input": {}}}),
            ),
            tool(
                "content_block_stop",
                json!({"type": "content_block_stop", "index": 1}),
            ),
        ],
    );
    let (h, body) = anthropic_messages(&stream).await;

    let (status, out) = h.text(call(MESSAGES, body)).await;

    assert_eq!(status, StatusCode::OK, "{out}");
    let seen: Vec<(String, Value)> = events(&out)
        .into_iter()
        .filter_map(|(n, v)| Some((n?, v["index"].clone())))
        .filter(|(n, _)| n.starts_with("content_block"))
        .collect();
    let at = |name: &str, index: i64| (name.to_owned(), json!(index));
    assert_eq!(
        seen,
        [
            at("content_block_start", 0),
            at("content_block_delta", 0),
            at("content_block_delta", 0),
            at("content_block_delta", 0),
            at("content_block_stop", 0),
            at("content_block_start", 1),
            at("content_block_stop", 1),
        ]
    );
    assert_eq!(anthropic_texts(&out), "Hi Alice, bye <");
}

#[test]
fn a_filter_is_tested_on_its_own() {
    let mut footer = Footer(0, None);
    let mut out = Vec::new();
    let _ = footer.edit(Reply::finished(Finish::Stop), &mut out);
    // The message is kept, saying nothing now, as its usage is still shown.
    assert_eq!(out, [Reply::default()]);
    let _ = footer.end(&Ok(()), &mut out);
    let mut footed = Reply::text(0, " [footer]");
    footed.finish = Some(Finish::Stop);
    assert_eq!(out[1..], [footed]);
}

#[tokio::test]
async fn a_finish_held_to_the_end_still_ends_anthropics_stream() {
    struct Foot;

    #[async_trait]
    impl Plugin for Foot {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(Footer(0, None))?;
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Foot)]).await;
    h.anthropic.answer(sse(ANTHROPIC_STREAM));
    let body = json!({
        "model": "claude-x", "max_tokens": 10, "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;

    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(anthropic_texts(&out), "Hello world [footer]");
    let delta = events(&out)
        .into_iter()
        .find(|(n, _)| n.as_deref() == Some("message_delta"));
    let delta = delta.map(|(_, v)| v).unwrap_or_default();
    assert_eq!(
        delta.pointer("/delta/stop_reason"),
        Some(&json!("end_turn"))
    );
    assert_eq!(delta.pointer("/usage/output_tokens"), Some(&json!(2)));
    assert!(out.ends_with("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"));
}

/// Puts a note before the answer, as soon as the response starts.
struct Note(bool);

impl Filter<Reply> for Note {
    fn edit(&mut self, event: Reply, out: &mut Vec<Reply>) -> Result<(), Fault> {
        let first = event.start.is_some() && !std::mem::replace(&mut self.0, true);
        out.push(event);
        if first {
            out.push(Reply::text(0, "Note: "));
        }
        Ok(())
    }
}

async fn filtered<F: Filter<Reply> + Clone + Sync>(filter: F) -> Harness {
    Harness::new(vec![Arc::new(Adds(filter))]).await
}

impl Clone for Note {
    fn clone(&self) -> Self {
        Note(self.0)
    }
}

#[tokio::test]
async fn text_put_before_anthropics_first_block_goes_into_it() {
    let h = filtered(Note(false)).await;
    h.anthropic.answer(sse(ANTHROPIC_STREAM));
    let body = json!({
        "model": "claude-x", "max_tokens": 10, "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;

    assert_eq!(status, StatusCode::OK, "{out}");
    let names: Vec<String> = events(&out).into_iter().filter_map(|(n, _)| n).collect();
    let count = |name: &str| names.iter().filter(|n| *n == name).count();
    assert_eq!(
        (count("message_start"), count("content_block_start")),
        (1, 1),
        "{out}"
    );
    assert_eq!(anthropic_texts(&out), "Note: Hello world");
}

#[tokio::test]
async fn a_whole_message_a_filter_took_all_text_of_is_still_sent() {
    let h = filtered(Silence).await;
    let reply = json!({
        "id": "msg_1", "type": "message", "role": "assistant", "model": "claude-x",
        "content": [{ "type": "text", "text": "secret" }],
        "stop_reason": "end_turn", "usage": { "input_tokens": 5, "output_tokens": 2 },
    });
    h.anthropic
        .answer(json_response(StatusCode::OK, &reply.to_string()));
    let body = json!({
        "model": "claude-x", "max_tokens": 10,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;

    assert_eq!(status, StatusCode::OK, "{out}");
    let out: Value = serde_json::from_str(&out).unwrap_or_default();
    assert_eq!(out["content"], json!([]));
    assert_eq!(out["stop_reason"], "end_turn");
}

#[tokio::test]
async fn made_text_stays_out_of_a_thinking_block() {
    let h = filtered(Note(false)).await;
    let event = |name: &str, data: Value| format!("event: {name}\ndata: {data}\n\n");
    let stream = [
        ANTHROPIC_STREAM[0].to_owned(),
        event(
            "content_block_start",
            json!({"type": "content_block_start", "index": 0,
            "content_block": {"type": "thinking", "thinking": ""}}),
        ),
        event(
            "content_block_delta",
            json!({"type": "content_block_delta", "index": 0,
            "delta": {"type": "thinking_delta", "thinking": "hmm"}}),
        ),
        event(
            "content_block_stop",
            json!({"type": "content_block_stop", "index": 0}),
        ),
        event(
            "content_block_start",
            json!({"type": "content_block_start", "index": 1,
            "content_block": {"type": "text", "text": ""}}),
        ),
        event(
            "content_block_delta",
            json!({"type": "content_block_delta", "index": 1,
            "delta": {"type": "text_delta", "text": "Hello"}}),
        ),
        event(
            "content_block_stop",
            json!({"type": "content_block_stop", "index": 1}),
        ),
        ANTHROPIC_STREAM[5].to_owned(),
        ANTHROPIC_STREAM[6].to_owned(),
    ];
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.anthropic.answer(sse(&chunks));
    let body = json!({
        "model": "claude-x", "max_tokens": 10, "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;

    assert_eq!(status, StatusCode::OK, "{out}");
    let in_thinking = events(&out)
        .into_iter()
        .any(|(_, v)| v["index"] == 0 && v.pointer("/delta/type") == Some(&json!("text_delta")));
    assert!(!in_thinking, "{out}");
    assert!(anthropic_texts(&out).contains("Note: "), "{out}");
}

#[tokio::test]
async fn a_filter_holding_everything_of_anthropics_stream_releases_it_whole() {
    let h = filtered(HoldAll(Vec::new())).await;
    h.anthropic.answer(sse(ANTHROPIC_STREAM));
    let body = json!({
        "model": "claude-x", "max_tokens": 10, "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;

    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(anthropic_texts(&out), "Hello world");
    let names: Vec<String> = events(&out).into_iter().filter_map(|(n, _)| n).collect();
    assert_eq!(
        names.iter().filter(|n| *n == "message_start").count(),
        1,
        "{out}"
    );
    assert_eq!(names.last().map(String::as_str), Some("message_stop"));
    // A buffering guard's release keeps the provider's framing in order.
    assert_well_formed(&names, &out);
}

#[tokio::test]
async fn text_before_the_start_of_an_anthropic_message_is_written() {
    let h = filtered(Disclaimer).await;
    let reply = json!({
        "id": "msg_1", "type": "message", "role": "assistant", "model": "claude-x",
        "content": [{ "type": "text", "text": "Hello" }],
        "stop_reason": "end_turn", "usage": { "input_tokens": 5, "output_tokens": 2 },
    });
    h.anthropic
        .answer(json_response(StatusCode::OK, &reply.to_string()));
    let body = json!({
        "model": "claude-x", "max_tokens": 10,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;

    assert_eq!(status, StatusCode::OK, "{out}");
    let out: Value = serde_json::from_str(&out).unwrap_or_default();
    assert_eq!(out["content"][0]["text"], "NOTE: Hello");
}

#[tokio::test]
async fn a_block_that_opens_with_text_a_filter_holds_opens_once() {
    let h = filtered(HoldAll(Vec::new())).await;
    let mut stream: Vec<String> = ANTHROPIC_STREAM.iter().map(|s| s.to_string()).collect();
    stream[1] = "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"Well, \"}}\n\n".to_owned();
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.anthropic.answer(sse(&chunks));
    let body = json!({
        "model": "claude-x", "max_tokens": 10, "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;

    assert_eq!(status, StatusCode::OK, "{out}");
    let names: Vec<String> = events(&out).into_iter().filter_map(|(n, _)| n).collect();
    let count = |name: &str| names.iter().filter(|n| *n == name).count();
    assert_eq!(
        (count("content_block_start"), count("content_block_stop")),
        (1, 1),
        "{out}"
    );
    // Released as it came, its opening text stays in its block's start, and
    // the stream keeps its order.
    let opening = events(&out).into_iter().find_map(|(_, v)| {
        let text = v.pointer("/content_block/text")?.as_str()?;
        Some(text.to_owned())
    });
    assert_eq!(opening.as_deref(), Some("Well, "), "{out}");
    assert_eq!(anthropic_texts(&out), "Hello world");
    assert_well_formed(&names, &out);
    let start = events(&out)
        .into_iter()
        .find(|(n, _)| n.as_deref() == Some("message_start"));
    assert_eq!(
        start.map(|(_, v)| v["message"]["id"].clone()),
        Some(json!("msg_1"))
    );
}

#[tokio::test]
async fn an_anthropic_stream_of_data_lines_only_is_written_once() {
    let h = Harness::new(vec![Arc::new(Unmask)]).await;
    let stream: Vec<String> = ANTHROPIC_STREAM
        .iter()
        .map(|e| {
            e.lines()
                .filter(|l| !l.starts_with("event:"))
                .map(|l| format!("{l}\n"))
                .collect()
        })
        .collect();
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.anthropic.answer(sse(&chunks));
    let body = json!({
        "model": "claude-x", "max_tokens": 10, "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;

    assert_eq!(status, StatusCode::OK, "{out}");
    let types: Vec<String> = events(&out)
        .into_iter()
        .filter_map(|(_, v)| v["type"].as_str().map(str::to_owned))
        .collect();
    let count = |t: &str| types.iter().filter(|n| *n == t).count();
    assert_eq!(
        (
            count("message_start"),
            count("message_delta"),
            count("message_stop")
        ),
        (1, 1, 1),
        "{out}"
    );
}

/// Keeps text back to the end, letting everything else through.
#[derive(Clone)]
struct HoldText(Vec<Reply>);

impl Filter<Reply> for HoldText {
    fn edit(&mut self, event: Reply, out: &mut Vec<Reply>) -> Result<(), Fault> {
        let text = event.texts().next().is_some();
        match text {
            true => self.0.push(event),
            false => out.push(event),
        }
        Ok(())
    }

    fn end(&mut self, _end: &Result<(), Fault>, out: &mut Vec<Reply>) -> Result<(), Fault> {
        out.append(&mut self.0);
        Ok(())
    }
}

#[tokio::test]
async fn text_released_at_the_end_of_a_translated_stream_goes_before_its_finish() {
    let h = filtered(HoldText(Vec::new())).await;
    h.anthropic.answer(sse(ANTHROPIC_STREAM));

    let (status, body) = h.text(openai_request("claude-x", true)).await;

    assert_eq!(status, StatusCode::OK, "{body}");
    let chunks: Vec<Value> = events(&body).into_iter().map(|(_, v)| v).collect();
    let text_at = chunks.iter().rposition(|c| {
        c.pointer("/choices/0/delta/content")
            .is_some_and(|t| t != "")
    });
    let finish_at = chunks.iter().position(|c| {
        !c.pointer("/choices/0/finish_reason")
            .is_none_or(Value::is_null)
    });
    assert!(text_at < finish_at, "{body}");
    assert!(body.ends_with("data: [DONE]\n\n"));
}

#[tokio::test]
async fn a_whole_anthropic_message_a_filter_took_the_start_of_is_sent() {
    let h = filtered(Nameless).await;
    let reply = json!({
        "id": "msg_1", "type": "message", "role": "assistant", "model": "claude-x",
        "content": [{ "type": "text", "text": "Hello" }],
        "stop_reason": "end_turn", "usage": { "input_tokens": 5, "output_tokens": 2 },
    });
    h.anthropic
        .answer(json_response(StatusCode::OK, &reply.to_string()));
    let body = json!({
        "model": "claude-x", "max_tokens": 10,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;

    assert_eq!(status, StatusCode::OK, "{out}");
    let out: Value = serde_json::from_str(&out).unwrap_or_default();
    assert_eq!(out["id"], "msg_1");
}

#[tokio::test]
async fn a_tool_block_held_in_anthropics_stream_is_released_as_it_came() {
    let h = filtered(HoldAll(Vec::new())).await;
    let event = |name: &str, data: Value| format!("event: {name}\ndata: {data}\n\n");
    let stream = [
        ANTHROPIC_STREAM[0].to_owned(),
        event(
            "content_block_start",
            json!({"type": "content_block_start", "index": 0,
            "content_block": {"type": "tool_use", "id": "t", "name": "f", "input": {}}}),
        ),
        event(
            "content_block_stop",
            json!({"type": "content_block_stop", "index": 0}),
        ),
        ANTHROPIC_STREAM[5].to_owned(),
        ANTHROPIC_STREAM[6].to_owned(),
    ];
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.anthropic.answer(sse(&chunks));
    let body = json!({
        "model": "claude-x", "max_tokens": 10, "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    let (_, out) = h.text(call(MESSAGES, body.to_string())).await;

    // It keeps its bytes, so it goes out as it came, where the provider put
    // it, not as the plugin's fault.
    assert!(!out.contains("event: error"), "{out}");
    assert!(out.contains("\"tool_use\""), "{out}");
    let names: Vec<String> = events(&out).into_iter().filter_map(|(n, _)| n).collect();
    assert_well_formed(&names, &out);
}

#[tokio::test]
async fn azures_filter_annotation_after_the_finish_ends_a_translated_stream_cleanly() {
    let h = Harness::new(Vec::new()).await;
    let mut stream: Vec<String> = OPENAI_STREAM.iter().map(|s| s.to_string()).collect();
    let annotation = json!({"id": "", "choices": [{"index": 0, "delta": {},
        "content_filter_offsets": {"check_offset": 0, "start_offset": 0, "end_offset": 11},
        "content_filter_results": {"hate": {"filtered": false, "severity": "safe"}}}]});
    stream.insert(4, format!("data: {annotation}\n\n"));
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.openai.answer(sse(&chunks));

    let (status, out) = h.text(anthropic_request(true)).await;

    assert_eq!(status, StatusCode::OK, "{out}");
    assert!(!out.contains("event: error"), "{out}");
    assert!(
        out.ends_with("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"),
        "{out}"
    );
}

/// Keeps each text back until the next one comes, releasing the last at the end.
#[derive(Clone)]
struct LagByOne(Option<Reply>);

impl Filter<Reply> for LagByOne {
    fn edit(&mut self, event: Reply, out: &mut Vec<Reply>) -> Result<(), Fault> {
        let text = event.texts().next().is_some();
        match text {
            true => out.extend(self.0.replace(event)),
            false => out.push(event),
        }
        Ok(())
    }

    fn end(&mut self, _end: &Result<(), Fault>, out: &mut Vec<Reply>) -> Result<(), Fault> {
        out.extend(self.0.take());
        Ok(())
    }
}

#[tokio::test]
async fn text_held_across_two_anthropic_blocks_stays_in_its_block() {
    let h = filtered(LagByOne(None)).await;
    let event = |name: &str, data: Value| format!("event: {name}\ndata: {data}\n\n");
    let block = |i: u32, text: &str| {
        [
            event(
                "content_block_start",
                json!({"type": "content_block_start", "index": i,
                "content_block": {"type": "text", "text": ""}}),
            ),
            event(
                "content_block_delta",
                json!({"type": "content_block_delta", "index": i,
                "delta": {"type": "text_delta", "text": text}}),
            ),
            event(
                "content_block_stop",
                json!({"type": "content_block_stop", "index": i}),
            ),
        ]
    };
    let mut stream = vec![ANTHROPIC_STREAM[0].to_owned()];
    stream.extend(block(0, "a"));
    stream.extend(block(1, "b"));
    stream.extend([
        ANTHROPIC_STREAM[5].to_owned(),
        ANTHROPIC_STREAM[6].to_owned(),
    ]);
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.anthropic.answer(sse(&chunks));
    let body = json!({
        "model": "claude-x", "max_tokens": 10, "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;

    assert_eq!(status, StatusCode::OK, "{out}");
    assert!(!out.contains("event: error"), "{out}");
    let deltas: Vec<(Value, Value)> = events(&out)
        .into_iter()
        .filter(|(n, _)| n.as_deref() == Some("content_block_delta"))
        .map(|(_, v)| (v["index"].clone(), v["delta"]["text"].clone()))
        .collect();
    assert_eq!(deltas, [(json!(0), json!("a")), (json!(1), json!("b"))]);
}

fn two_text_blocks(a: &str, b: &str) -> Vec<String> {
    let event = |name: &str, data: Value| format!("event: {name}\ndata: {data}\n\n");
    let block = |i: u32, text: &str| {
        [
            event(
                "content_block_start",
                json!({"type": "content_block_start", "index": i,
                "content_block": {"type": "text", "text": ""}}),
            ),
            event(
                "content_block_delta",
                json!({"type": "content_block_delta", "index": i,
                "delta": {"type": "text_delta", "text": text}}),
            ),
            event(
                "content_block_stop",
                json!({"type": "content_block_stop", "index": i}),
            ),
        ]
    };
    let mut stream = vec![ANTHROPIC_STREAM[0].to_owned()];
    stream.extend(block(0, a));
    stream.extend(block(1, b));
    stream.extend([
        ANTHROPIC_STREAM[5].to_owned(),
        ANTHROPIC_STREAM[6].to_owned(),
    ]);
    stream
}

async fn anthropic_through<F: Filter<Reply> + Clone + Sync>(
    filter: F,
    stream: &[String],
) -> String {
    let h = filtered(filter).await;
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.anthropic.answer(sse(&chunks));
    let body = json!({
        "model": "claude-x", "max_tokens": 10, "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
    });
    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    out
}

#[tokio::test]
async fn text_held_from_two_anthropic_blocks_keeps_its_order() {
    let out = anthropic_through(HoldAll(Vec::new()), &two_text_blocks("first ", "second")).await;

    assert_eq!(anthropic_texts(&out), "first second", "{out}");
}

#[tokio::test]
async fn a_guardrail_ending_an_anthropic_stream_early_ends_it() {
    /// Cuts the answer at " world", ending it as filtered.
    #[derive(Clone)]
    struct Cut;

    impl Filter<Reply> for Cut {
        fn edit(&mut self, mut reply: Reply, out: &mut Vec<Reply>) -> Result<(), Fault> {
            if reply.texts().any(|t| t == " world") {
                for text in reply.texts_mut().filter(|t| *t == " world") {
                    *text = " [cut]".into();
                }
                reply.finish = Some(Finish::Filtered);
            }
            out.push(reply);
            Ok(())
        }
    }

    let stream: Vec<String> = ANTHROPIC_STREAM.iter().map(|s| s.to_string()).collect();
    let out = anthropic_through(Cut, &stream).await;

    assert!(!out.contains("event: error"), "{out}");
    assert_eq!(anthropic_texts(&out), "Hello [cut]");
}

#[tokio::test]
async fn an_anthropic_response_is_reframed_as_its_client_asked() {
    let h = Harness::new(vec![Arc::new(Flip)]).await;
    let whole = json!({
        "id": "msg_2", "type": "message", "role": "assistant", "model": "claude-x",
        "content": [{ "type": "text", "text": "hi" }],
        "stop_reason": "end_turn", "usage": { "input_tokens": 1, "output_tokens": 1 },
    });
    h.anthropic
        .answer(json_response(StatusCode::OK, &whole.to_string()));
    h.anthropic.answer(sse(ANTHROPIC_STREAM));
    let ask = |stream: bool| {
        let body = json!({
            "model": "claude-x", "max_tokens": 10, "stream": stream,
            "messages": [{ "role": "user", "content": "hi" }],
        });
        call(MESSAGES, body.to_string())
    };

    let (_, streamed) = h.text(ask(true)).await;
    let (_, whole) = h.text(ask(false)).await;

    let names: Vec<_> = events(&streamed)
        .into_iter()
        .filter_map(|(n, _)| n)
        .collect();
    assert_eq!(names.first().map(String::as_str), Some("message_start"));
    assert_eq!(names.last().map(String::as_str), Some("message_stop"));
    assert!(streamed.contains("\"text\":\"hi\""), "{streamed}");
    let whole: Value = serde_json::from_str(&whole).unwrap_or_default();
    assert_eq!(whole["content"][0]["text"], "Hello world", "{whole}");
    assert_eq!(whole["usage"]["output_tokens"], 2, "{whole}");
}

/// Records how each call ends on the provider edge.
#[derive(Clone, Default)]
struct CallEnds(Arc<Mutex<Vec<Result<(), Fault>>>>);

#[async_trait]
impl Plugin for CallEnds {
    async fn on_provider_end(&self, _px: &ProviderContext, end: &Result<(), Fault>) {
        self.0.lock().map(|mut all| all.push(end.clone())).ok();
    }
}

#[tokio::test]
async fn any_translated_3xx_with_a_location_is_a_bad_gateway() {
    let choices = http::Response::builder()
        .status(StatusCode::MULTIPLE_CHOICES)
        .header("location", "http://elsewhere/")
        .body(Vec::new())
        .unwrap_or_else(|e| panic!("{e}"));
    let h = Harness::with_local(vec![Arc::new(Cross)], None, Some(one_response(choices))).await;

    let (status, headers, _) = h.send(openai_request("claude-x", false)).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(headers.get("location").is_none());
}

#[tokio::test]
async fn a_translated_redirect_is_a_bad_gateway() {
    let ends = CallEnds::default();
    let moved = http::Response::builder()
        .status(StatusCode::FOUND)
        .header("location", "http://elsewhere/")
        .body(Vec::new())
        .unwrap_or_else(|e| panic!("{e}"));
    let h = Harness::with_local(
        vec![Arc::new(Cross), Arc::new(ends.clone())],
        None,
        Some(one_response(moved)),
    )
    .await;

    let (status, headers, body) = h.send(openai_request("claude-x", false)).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(headers.get("location").is_none());
    let body: Value = serde_json::from_slice(&body.unwrap_or_default()).unwrap_or_default();
    assert_eq!(body["error"]["code"], "upstream", "{body}");
    // The call ends with the refusal, which carries the provider's answer.
    let ends = ends.0.lock().map(|all| all.clone()).unwrap_or_default();
    assert!(matches!(&ends[..], [o] if o.as_ref().is_err_and(|f| f.code() == codes::UPSTREAM)));
    let answer = ends[0].as_ref().err();
    let answer = answer.and_then(|f| {
        f.answer::<http::Response<respan_gateway_http::bytes::Bytes>>("anthropic-messages")
    });
    assert_eq!(answer.map(|a| a.status()), Some(StatusCode::FOUND));
}

#[tokio::test]
async fn a_provider_edge_edit_is_encoded_for_its_translated_target() {
    struct Cool;

    #[async_trait]
    impl Plugin for Cool {
        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            assert_eq!(px.format(), "anthropic-messages");
            px.filter(|chat: &mut Conversation| {
                chat.temperature = Some(0.0);
                Ok(())
            })
        }
    }

    let h = Harness::new(vec![Arc::new(Cool)]).await;
    h.anthropic.answer(sse(ANTHROPIC_STREAM));

    assert_eq!(
        h.text(openai_request("claude-x", true)).await.0,
        StatusCode::OK
    );
    assert_eq!(h.sent_json(0)["temperature"], 0.0);
}

#[tokio::test]
async fn a_translated_failure_labelled_as_a_stream_is_still_a_failure() {
    let failed = http::Response::builder()
        .status(StatusCode::SERVICE_UNAVAILABLE)
        .header("content-type", "text/event-stream")
        .body(
            OPENAI_STREAM
                .iter()
                .map(|c| Ok(bytes::Bytes::from(*c)))
                .collect::<Vec<_>>(),
        )
        .unwrap_or_default();
    let h = Harness::with_local(vec![Arc::new(Cross)], Some(one_response(failed)), None).await;

    let (status, _, body) = h.send(anthropic_request(true)).await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    let body: Value = serde_json::from_slice(&body.unwrap_or_default()).unwrap_or_default();
    assert_eq!(body["type"], "error", "{body}");
    let end = h
        .log
        .only()
        .map_err(|f| respan_gateway_http::status(&f).as_u16());
    assert_eq!(end, Err(503));
}

#[tokio::test]
async fn a_provider_error_framed_as_an_event_keeps_its_words() {
    let error =
        "data: {\"error\":{\"type\":\"rate_limit_exceeded\",\"message\":\"slow down\"}}\n\n";
    let failed = http::Response::builder()
        .status(StatusCode::TOO_MANY_REQUESTS)
        .header("content-type", "text/event-stream")
        .body(vec![Ok(bytes::Bytes::from(error))])
        .unwrap_or_default();
    let h = Harness::with_local(vec![Arc::new(Cross)], Some(one_response(failed)), None).await;

    let (status, _, body) = h.send(anthropic_request(true)).await;

    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert!(body.contains("slow down"), "{body}");
    let end = h.log.only().err().map(|f| f.message().to_owned());
    assert_eq!(end.as_deref(), Some("slow down"));
}

#[tokio::test]
async fn an_anthropic_stream_error_with_no_type_keeps_its_words() {
    let h = Harness::new(Vec::new()).await;
    let stream = [
        ANTHROPIC_STREAM[0],
        "data: {\"error\":{\"type\":\"overloaded_error\",\"message\":\"busy\"}}\n\n",
    ];
    h.anthropic.answer(sse(&stream));

    let (_, _, body) = h.send(openai_request("claude-x", true)).await;

    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert!(
        body.contains("busy") && body.contains("overloaded_error"),
        "{body}"
    );
    let end = h
        .log
        .only()
        .map_err(|f| (f.code().to_owned(), f.message().to_owned()));
    assert_eq!(end, Err(("overloaded_error".to_owned(), "busy".to_owned())));
}

#[tokio::test]
async fn a_providers_error_event_mid_stream_is_written_anew_when_translated() {
    let h = Harness::new(Vec::new()).await;
    let error = "data: {\"error\":{\"message\":\"boom\",\"type\":\"server_error\",\"param\":null,\"code\":null}}\n\n";
    h.openai
        .answer(sse(&[OPENAI_STREAM[0], OPENAI_STREAM[1], error]));

    let (_, _, body) = h.send(anthropic_request(true)).await;

    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    let (name, data) = events(&body).pop().unwrap_or_default();
    assert_eq!(name.as_deref(), Some("error"));
    assert_eq!(data["type"], "error");
    assert_eq!(
        (&data["error"]["type"], &data["error"]["message"]),
        (&json!("server_error"), &json!("boom"))
    );
}

#[tokio::test]
async fn an_anthropic_error_after_its_message_delta_keeps_both_and_ends_last() {
    let h = Harness::new(Vec::new()).await;
    let error = "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"busy\"}}\n\n";
    let stream = [ANTHROPIC_STREAM[0], ANTHROPIC_STREAM[5], error];
    h.anthropic.answer(sse(&stream));
    let body = json!({
        "model": "claude-x", "max_tokens": 10, "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    let (_, out) = h.text(call(MESSAGES, body.to_string())).await;

    assert!(out.contains("message_delta"), "{out}");
    assert!(out.ends_with(error), "{out}");
    let end = h.log.only().map_err(|f| f.code().to_owned());
    assert_eq!(end, Err("overloaded_error".to_owned()));
}

#[tokio::test]
async fn a_translated_call_passes_none_of_the_clients_headers() {
    let h = Harness::new(Vec::new()).await;
    h.anthropic.answer(sse(ANTHROPIC_STREAM));
    let mut req = openai_request("claude-x", true);
    for (name, value) in [("user-agent", "OpenAI/Python 1.0"), ("x-app", "notes")] {
        req.headers_mut()
            .insert(name, HeaderValue::from_static(value));
    }

    let _ = h.send(req).await;

    let sent = h.sent();
    for name in ["user-agent", "x-app"] {
        assert!(sent[0].headers.get(name).is_none(), "{name}");
    }
}

// An Anthropic error event goes as it came to an Anthropic client, and in
// OpenAI's words to an OpenAI one.
#[tokio::test]
async fn an_anthropic_error_event_goes_as_it_came_only_to_its_format() {
    let error = "event: error\ndata: {\"type\": \"error\", \"error\": {\"type\": \"overloaded_error\", \"message\": \"busy\"}}\n\n";
    let mut stream = ANTHROPIC_STREAM[..3].to_vec();
    stream.push(error);

    let h = Harness::new(Vec::new()).await;
    h.anthropic.answer(sse(&stream));
    let body = json!({
        "model": "claude-x", "max_tokens": 10, "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
    });
    let (_, out) = h.text(call(MESSAGES, body.to_string())).await;
    assert!(out.ends_with(error), "{out}");

    let h = Harness::new(Vec::new()).await;
    h.anthropic.answer(sse(&stream));
    let (_, out) = h.text(openai_request("claude-x", true)).await;
    let last = events(&out).pop().map(|(_, d)| d).unwrap_or_default();
    assert_eq!(last["error"]["code"], "overloaded_error", "{out}");
    assert!(last["type"].is_null(), "{out}");
}

// An OpenAI error event goes in Anthropic's words to an Anthropic client.
#[tokio::test]
async fn an_openai_error_event_reaches_an_anthropic_client_in_its_words() {
    let mut stream = OPENAI_STREAM[..2].to_vec();
    stream
        .push("data: {\"error\":{\"message\":\"boom\",\"type\":\"server_error\",\"extra\":1}}\n\n");
    let h = Harness::new(Vec::new()).await;
    h.openai.answer(sse(&stream));

    let (_, out) = h.text(anthropic_request(true)).await;

    let (name, last) = events(&out).pop().unwrap_or((None, Value::Null));
    assert_eq!(name.as_deref(), Some("error"), "{out}");
    assert_eq!(last["type"], "error", "{out}");
    assert_eq!(last["error"]["message"], "boom", "{out}");
}

// An error inside a 200 goes as it came to a client of its format, and in
// the client's words to another.
#[tokio::test]
async fn an_error_in_a_success_goes_as_it_came_only_to_its_format() {
    let anthropic = r#"{"type": "error", "error": {"type": "overloaded_error", "message": "busy", "retry": true}}"#;
    let openai = r#"{"error": {"message": "boom", "type": "server_error", "extra": 1}}"#;
    let messages = json!({
        "model": "claude-x", "max_tokens": 10,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    // Anthropic to its own client, as it came.
    let h = Harness::new(Vec::new()).await;
    h.anthropic.answer(json_response(StatusCode::OK, anthropic));
    let (_, out) = h.text(call(MESSAGES, messages.to_string())).await;
    assert_eq!(out, anthropic);

    // Anthropic to an OpenAI client, in OpenAI's words.
    let h = Harness::new(Vec::new()).await;
    h.anthropic.answer(json_response(StatusCode::OK, anthropic));
    let (_, out) = h.text(openai_request("claude-x", false)).await;
    let v: Value = serde_json::from_str(&out).unwrap_or_default();
    assert_eq!(v["error"]["code"], "overloaded_error", "{out}");
    assert!(v["type"].is_null(), "{out}");

    // OpenAI to an Anthropic client, in Anthropic's words.
    let h = Harness::new(Vec::new()).await;
    h.openai.answer(json_response(StatusCode::OK, openai));
    let (_, out) = h.text(anthropic_request(false)).await;
    let v: Value = serde_json::from_str(&out).unwrap_or_default();
    assert_eq!(
        (&v["type"], &v["error"]["message"]),
        (&json!("error"), &json!("boom"))
    );
}

/// Aliases the model, as a tenant or routing plugin does; touches nothing else.
struct Alias;

#[async_trait]
impl Plugin for Alias {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.filter(|chat: &mut Conversation| {
            chat.model = Some("claude-sonnet-x".into());
            Ok(())
        })?;
        Ok(None)
    }
}

/// The client's own untranslatable field, unchanged by the plugin (which only
/// aliased the model), is still the client's fault: a 400, as it is without
/// the plugin.
#[tokio::test]
async fn an_untranslatable_client_field_stays_the_clients_fault_when_a_plugin_only_aliases_the_model()
 {
    let h = Harness::new(vec![Arc::new(Alias)]).await;
    let format = json!({ "type": "json_object" });
    let body = json!({ "model": "claude-x", "messages": [{ "role": "user", "content": "hi" }], "response_format": format });
    let (status, text) = h.text(call(CHAT, body.to_string())).await;
    let code = match h.log.only() {
        Err(f) => f.code().to_owned(),
        Ok(()) => "ok".into(),
    };
    assert_eq!(
        (status, code.as_str()),
        (StatusCode::BAD_REQUEST, codes::BAD_REQUEST),
        "{text}"
    );
}

const OPENAI_PARALLEL_STREAM: &[&str] = &[
    // Both calls start in one chunk, then their arguments come by index.
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":null,\"tool_calls\":[{\"index\":0,\"id\":\"call_a\",\"type\":\"function\",\"function\":{\"name\":\"read\",\"arguments\":\"\"}},{\"index\":1,\"id\":\"call_b\",\"type\":\"function\",\"function\":{\"name\":\"read\",\"arguments\":\"\"}}]}}]}\n\n",
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"{\\\"path\\\":\\\"a\\\"}\"}}]}}]}\n\n",
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":1,\"function\":{\"arguments\":\"{\\\"path\\\":\\\"b\\\"}\"}}]}}]}\n\n",
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
    "data: [DONE]\n\n",
];

/// OpenAI names each tool call's fragment by `index`; the Anthropic client
/// must get each call's arguments in its own block.
#[tokio::test]
async fn parallel_openai_tool_calls_keep_their_arguments_for_an_anthropic_client() {
    let h = Harness::new(Vec::new()).await;
    h.openai.answer(sse(OPENAI_PARALLEL_STREAM));
    let body = json!({ "model": "gpt-x", "max_tokens": 100, "stream": true,
        "tools": [{ "name": "read", "input_schema": { "type": "object" } }],
        "messages": [{ "role": "user", "content": "hi" }] });
    let (_, out) = h.text(call(MESSAGES, body.to_string())).await;
    let sent = events(&out);
    let mut by_block: std::collections::BTreeMap<u64, (String, String)> = Default::default();
    for (_, d) in &sent {
        let i = d["index"].as_u64().unwrap_or(99);
        if d["type"] == "content_block_start" {
            by_block.entry(i).or_default().0 =
                d["content_block"]["id"].as_str().unwrap_or("").into();
        }
        if let Some(p) = d["delta"]["partial_json"].as_str() {
            by_block.entry(i).or_default().1.push_str(p);
        }
    }
    let got: Vec<(String, String)> = by_block.into_values().collect();
    assert_eq!(
        got,
        vec![
            ("call_a".into(), r#"{"path":"a"}"#.into()),
            ("call_b".into(), r#"{"path":"b"}"#.into())
        ],
        "{out}"
    );
}

/// The same calls read whole (the plugin asks the provider for a stream, the
/// client asked for a whole answer): each call keeps its own arguments.
#[tokio::test]
async fn parallel_openai_tool_calls_keep_their_arguments_in_a_whole_anthropic_answer() {
    struct Streamed;
    #[async_trait]
    impl Plugin for Streamed {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(|chat: &mut Conversation| {
                chat.stream = true;
                Ok(())
            })?;
            Ok(None)
        }
    }
    let h = Harness::new(vec![Arc::new(Streamed)]).await;
    h.openai.answer(sse(OPENAI_PARALLEL_STREAM));
    let body = json!({ "model": "gpt-x", "max_tokens": 100,
        "tools": [{ "name": "read", "input_schema": { "type": "object" } }],
        "messages": [{ "role": "user", "content": "hi" }] });
    let (_, out) = h.text(call(MESSAGES, body.to_string())).await;
    let v: Value = serde_json::from_str(&out).unwrap_or_default();
    assert_eq!(
        v["content"],
        json!([
            { "type": "tool_use", "id": "call_a", "name": "read", "input": { "path": "a" } },
            { "type": "tool_use", "id": "call_b", "name": "read", "input": { "path": "b" } },
        ]),
        "{out}"
    );
}

/// Anthropic's `is_error` on a tool result says the tool failed; sent to
/// OpenAI it must be kept (in the text) or refused, not dropped.
#[tokio::test]
async fn a_failed_tool_result_is_not_silently_turned_into_a_success() {
    let h = Harness::new(Vec::new()).await;
    h.openai.answer( json_response(StatusCode::OK, r#"{"id":"c","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}"#));
    let body = json!({
        "model": "gpt-x", "max_tokens": 100,
        "tools": [{ "name": "read", "input_schema": { "type": "object" } }],
        "messages": [
            { "role": "user", "content": "open a.rs" },
            { "role": "assistant", "content": [{ "type": "tool_use", "id": "t1", "name": "read", "input": {} }] },
            { "role": "user", "content": [{ "type": "tool_result", "tool_use_id": "t1", "is_error": true, "content": "ENOENT" }] },
        ],
    });
    let (status, text) = h.text(call(MESSAGES, body.to_string())).await;
    if status == StatusCode::OK {
        let sent = h.sent_json(0);
        panic!("is_error dropped: sent {}", sent["messages"]);
    }
    assert_eq!(status, StatusCode::BAD_REQUEST, "{text}");
}

/// OpenAI's `strict` on a function asks for schema-exact arguments; sent to
/// Anthropic it must be kept or refused, not dropped.
#[tokio::test]
async fn a_strict_function_is_not_silently_loosened() {
    let h = Harness::new(Vec::new()).await;
    h.anthropic.answer(sse(ANTHROPIC_STREAM));
    let tool = json!({ "type": "function", "function": { "name": "read", "strict": true,
        "parameters": { "type": "object", "properties": {}, "additionalProperties": false } } });
    let body = json!({ "model": "claude-x", "tools": [tool], "messages": [{ "role": "user", "content": "hi" }] });
    let (status, _) = h.text(call(CHAT, body.to_string())).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// Extra fields on Anthropic's tool, call and result have no place in
/// OpenAI's: refused, never dropped.
#[tokio::test]
async fn an_extra_tool_field_is_refused_not_dropped() {
    let cached = json!({ "type": "ephemeral" });
    let tool =
        json!({ "name": "read", "input_schema": { "type": "object" }, "cache_control": cached });
    let use_block = json!({ "type": "tool_use", "id": "t1", "name": "read", "input": {} });
    let result = json!({ "type": "tool_result", "tool_use_id": "t1", "content": "x", "cache_control": cached });
    let ask = json!({ "role": "user", "content": "go" });
    let call_made = json!({ "role": "assistant", "content": [use_block] });
    let answer = json!({ "role": "user", "content": [result] });
    for body in [
        json!({ "model": "gpt-x", "max_tokens": 9, "tools": [tool], "messages": [ask] }),
        json!({ "model": "gpt-x", "max_tokens": 9, "messages": [ask, call_made, answer] }),
    ] {
        let h = Harness::new(Vec::new()).await;
        let (status, text) = h.text(call(MESSAGES, body.to_string())).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{text}");
        assert!(h.sent().is_empty());
    }
}

/// A repeated id on every delta of one call, as some providers send, is
/// one call.
#[tokio::test]
async fn a_provider_repeating_a_calls_start_on_every_delta_makes_one_call() {
    const REPEATED: &[&str] = &[
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_a\",\"type\":\"function\",\"function\":{\"name\":\"read\",\"arguments\":\"{\\\"path\\\":\"}}]}}]}\n\n",
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_a\",\"type\":\"function\",\"function\":{\"name\":\"read\",\"arguments\":\"\\\"a\\\"}\"}}]}}]}\n\n",
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
        "data: [DONE]\n\n",
    ];
    let h = Harness::new(Vec::new()).await;
    h.openai.answer(sse(REPEATED));
    let body = json!({ "model": "gpt-x", "max_tokens": 100, "stream": true,
        "tools": [{ "name": "read", "input_schema": { "type": "object" } }],
        "messages": [{ "role": "user", "content": "hi" }] });
    let (_, out) = h.text(call(MESSAGES, body.to_string())).await;
    let sent = events(&out);
    let starts = sent
        .iter()
        .filter(|(_, d)| d["type"] == "content_block_start")
        .count();
    let json: String = sent
        .iter()
        .filter_map(|(_, d)| d["delta"]["partial_json"].as_str())
        .collect();
    assert_eq!((starts, json.as_str()), (1, r#"{"path":"a"}"#), "{out}");
}

/// A whole OpenAI answer cut by the token limit has arguments that do not
/// parse; the Anthropic client must not be told the call had no input.
#[tokio::test]
async fn cut_off_tool_arguments_are_not_replaced_by_an_empty_input() {
    let h = Harness::new(Vec::new()).await;
    let answer = json!({ "id": "c", "model": "g", "choices": [{ "index": 0, "finish_reason": "length",
        "message": { "role": "assistant", "content": null, "tool_calls": [
            { "id": "c1", "type": "function", "function": { "name": "rm", "arguments": "{\"path\":\"/tmp/a\",\"recursive\":tr" } }] } }],
        "usage": { "prompt_tokens": 1, "completion_tokens": 1 } });
    h.openai
        .answer(json_response(StatusCode::OK, &answer.to_string()));
    let body = json!({ "model": "gpt-x", "max_tokens": 9, "messages": [{ "role": "user", "content": "hi" }],
        "tools": [{ "name": "rm", "input_schema": { "type": "object" } }] });
    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;
    assert_eq!(
        status,
        StatusCode::BAD_GATEWAY,
        "cut arguments became input: {out}"
    );
}

/// Control: OpenAI's own order (each call's start, then its arguments) works.
#[tokio::test]
async fn control_sequential_openai_tool_calls_reach_an_anthropic_client() {
    const SEQ: &[&str] = &[
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":null,\"tool_calls\":[{\"index\":0,\"id\":\"call_a\",\"type\":\"function\",\"function\":{\"name\":\"read\",\"arguments\":\"\"}}]}}]}\n\n",
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"{\\\"path\\\":\\\"a\\\"}\"}}]}}]}\n\n",
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":1,\"id\":\"call_b\",\"type\":\"function\",\"function\":{\"name\":\"read\",\"arguments\":\"\"}}]}}]}\n\n",
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":1,\"function\":{\"arguments\":\"{\\\"path\\\":\\\"b\\\"}\"}}]}}]}\n\n",
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
        "data: [DONE]\n\n",
    ];
    let h = Harness::new(Vec::new()).await;
    h.openai.answer(sse(SEQ));
    let body = json!({ "model": "gpt-x", "max_tokens": 100, "stream": true,
        "tools": [{ "name": "read", "input_schema": { "type": "object" } }],
        "messages": [{ "role": "user", "content": "hi" }] });
    let (_, out) = h.text(call(MESSAGES, body.to_string())).await;
    let args: Vec<String> = events(&out)
        .iter()
        .filter_map(|(_, d)| {
            d["delta"]["partial_json"]
                .as_str()
                .map(|s| format!("{}:{s}", d["index"]))
        })
        .collect();
    assert_eq!(
        args,
        vec![
            r#"0:{"path":"a"}"#.to_owned(),
            r#"1:{"path":"b"}"#.to_owned()
        ],
        "{out}"
    );
}

// ---------------- loop-4 audit ----------------

/// Sends the client's message as the call's own input, as a guardrail that
/// rewrites the prompt does: never a relay, so the provider's pieces are
/// written anew even in the client's own format.
struct L4Own(&'static str);

#[async_trait]
impl Plugin for L4Own {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let chat = cx.input::<Conversation>()?.clone();
        Ok(Some(cx.call(self.0).with_input(chat).await?))
    }
}

/// Each OpenAI tool call's accumulated `(id, arguments)` by index, as an
/// OpenAI SDK accumulates a stream.
fn l4_openai_calls(out: &str) -> Vec<(String, String)> {
    let mut by: std::collections::BTreeMap<u64, (String, String)> = Default::default();
    for (_, d) in events(out) {
        for c in d["choices"][0]["delta"]["tool_calls"]
            .as_array()
            .into_iter()
            .flatten()
        {
            let e = by.entry(c["index"].as_u64().unwrap_or(99)).or_default();
            if let Some(id) = c["id"].as_str() {
                e.0 = id.into();
            }
            e.1.push_str(c["function"]["arguments"].as_str().unwrap_or(""));
        }
    }
    by.into_values().collect()
}

/// An OpenAI client, an OpenAI provider, a call that is not a relay: the
/// parallel calls OpenAI numbers by index must each keep valid arguments.
#[tokio::test]
async fn l4_parallel_openai_tool_calls_written_anew_for_an_openai_client_keep_valid_arguments() {
    let h = Harness::with(vec![Arc::new(L4Own("openai"))]).await;
    h.openai.answer(sse(OPENAI_PARALLEL_STREAM));
    let body = json!({ "model": "gpt-x", "stream": true,
        "tools": [{ "type": "function", "function": { "name": "read", "parameters": { "type": "object" } } }],
        "messages": [{ "role": "user", "content": "hi" }] });
    let (_, out) = h.text(call(CHAT, body.to_string())).await;
    assert_eq!(
        l4_openai_calls(&out),
        vec![
            ("call_a".into(), r#"{"path":"a"}"#.into()),
            ("call_b".into(), r#"{"path":"b"}"#.into())
        ],
        "{out}"
    );
}

/// Anthropic's blocks come one after another; written anew for an Anthropic
/// client, each tool block must stop before the next one starts.
#[tokio::test]
async fn l4_sequential_anthropic_tool_blocks_written_anew_stop_before_the_next_starts() {
    const TWO_TOOLS: &[&str] = &[
        "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-x\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null,\"usage\":{\"input_tokens\":5,\"output_tokens\":1}}}\n\n",
        "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"t1\",\"name\":\"read\",\"input\":{}}}\n\n",
        "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"path\\\":\\\"a\\\"}\"}}\n\n",
        "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
        "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"t2\",\"name\":\"read\",\"input\":{}}}\n\n",
        "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"path\\\":\\\"b\\\"}\"}}\n\n",
        "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":1}\n\n",
        "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\",\"stop_sequence\":null},\"usage\":{\"output_tokens\":2}}\n\n",
        "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
    ];
    let h = Harness::with(vec![Arc::new(L4Own("anthropic"))]).await;
    h.anthropic.answer(sse(TWO_TOOLS));
    let body = json!({ "model": "claude-x", "max_tokens": 100, "stream": true,
        "tools": [{ "name": "read", "input_schema": { "type": "object" } }],
        "messages": [{ "role": "user", "content": "hi" }] });
    let (_, out) = h.text(call(MESSAGES, body.to_string())).await;
    let seq: Vec<String> = events(&out)
        .into_iter()
        .filter(|(_, d)| d["type"] == "content_block_start" || d["type"] == "content_block_stop")
        .map(|(_, d)| format!("{}{}", d["type"].as_str().unwrap_or(""), d["index"]))
        .collect();
    assert_eq!(
        seq,
        [
            "content_block_start0",
            "content_block_stop0",
            "content_block_start1",
            "content_block_stop1"
        ],
        "{out}"
    );
}

/// A text block after a tool call, then the tool call's arguments (index 0
/// again): an Anthropic client must still get the arguments in the tool's
/// block, or a refusal, never a mangled stream.
#[tokio::test]
async fn l4_openai_arguments_after_interleaved_text_reach_an_anthropic_client() {
    const MIXED: &[&str] = &[
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"tool_calls\":[{\"index\":0,\"id\":\"call_a\",\"type\":\"function\",\"function\":{\"name\":\"read\",\"arguments\":\"\"}}]}}]}\n\n",
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hm\"}}]}\n\n",
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"{\\\"path\\\":\\\"a\\\"}\"}}]}}]}\n\n",
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
        "data: [DONE]\n\n",
    ];
    let h = Harness::new(Vec::new()).await;
    h.openai.answer(sse(MIXED));
    let body = json!({ "model": "gpt-x", "max_tokens": 100, "stream": true,
        "tools": [{ "name": "read", "input_schema": { "type": "object" } }],
        "messages": [{ "role": "user", "content": "hi" }] });
    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    h.assert_requests(1, 0);
    let events = events(&out);
    assert!(
        events
            .iter()
            .any(|(_, d)| d["content_block"]["id"] == "call_a"),
        "{out}"
    );
    assert!(
        events.iter().any(|(_, d)| d["delta"]["text"] == "hm"),
        "{out}"
    );
    assert!(
        events
            .iter()
            .any(|(_, d)| d["error"]["type"] == "untranslatable"),
        "{out}"
    );
    assert!(!out.contains("partial_json"), "{out}");
}

/// A tool result's text block carries `cache_control`, which OpenAI has no
/// place for: refused, as the module says of fields with no equivalent.
#[tokio::test]
async fn l4_a_tool_result_text_blocks_extra_field_is_refused_not_dropped() {
    let h = Harness::new(Vec::new()).await;
    h.openai.answer( json_response(StatusCode::OK, r#"{"id":"c","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}"#));
    let body = json!({
        "model": "gpt-x", "max_tokens": 100,
        "messages": [
            { "role": "user", "content": "open a.rs" },
            { "role": "assistant", "content": [{ "type": "tool_use", "id": "t1", "name": "read", "input": {} }] },
            { "role": "user", "content": [{ "type": "tool_result", "tool_use_id": "t1",
                "content": [{ "type": "text", "text": "fn main", "cache_control": { "type": "ephemeral" } }] }] },
        ],
    });
    let (status, text) = h.text(call(MESSAGES, body.to_string())).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{text}");
    h.assert_requests(0, 0);
}

/// Anthropic's `tool_choice` with a field OpenAI has no place for is
/// refused, not dropped.
#[tokio::test]
async fn l4_an_anthropic_tool_choice_extra_field_is_refused_not_dropped() {
    let h = Harness::new(Vec::new()).await;
    h.openai.answer( json_response(StatusCode::OK, r#"{"id":"c","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}"#));
    let body = json!({
        "model": "gpt-x", "max_tokens": 100,
        "tools": [{ "name": "read", "input_schema": { "type": "object" } }],
        "tool_choice": { "type": "auto", "cache_control": { "type": "ephemeral" } },
        "messages": [{ "role": "user", "content": "hi" }],
    });
    let (status, text) = h.text(call(MESSAGES, body.to_string())).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{text}");
    h.assert_requests(0, 0);
}

/// With `n > 1`, each choice numbers its own tool calls, so a call of the
/// second choice must not merge into the first's: it is refused.
#[tokio::test]
async fn a_tool_call_of_a_later_choice_is_not_merged_into_the_first_choices() {
    const TWO_CHOICES: &[&str] = &[
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"tool_calls\":[{\"index\":0,\"id\":\"call_a\",\"type\":\"function\",\"function\":{\"name\":\"read\",\"arguments\":\"{}\"}}]}}]}\n\n",
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":1,\"delta\":{\"role\":\"assistant\",\"tool_calls\":[{\"index\":0,\"id\":\"call_b\",\"type\":\"function\",\"function\":{\"name\":\"read\",\"arguments\":\"{}\"}}]}}]}\n\n",
        "data: [DONE]\n\n",
    ];
    let h = Harness::new(Vec::new()).await;
    h.openai.answer(sse(TWO_CHOICES));
    let body = json!({ "model": "gpt-x", "max_tokens": 100, "stream": true,
        "tools": [{ "name": "read", "input_schema": { "type": "object" } }],
        "messages": [{ "role": "user", "content": "hi" }] });
    let (_, out) = h.text(call(MESSAGES, body.to_string())).await;
    assert!(!out.contains("call_b"), "{out}");
    assert!(out.contains("event: error"), "{out}");
}

/// Parallel OpenAI calls reach an Anthropic client one block at a time.
#[tokio::test]
async fn parallel_openai_tool_calls_reach_an_anthropic_client_in_blocks_that_do_not_overlap() {
    let h = Harness::new(Vec::new()).await;
    h.openai.answer(sse(OPENAI_PARALLEL_STREAM));
    let body = json!({ "model": "gpt-x", "max_tokens": 100, "stream": true,
        "tools": [{ "name": "read", "input_schema": { "type": "object" } }],
        "messages": [{ "role": "user", "content": "hi" }] });
    let (_, out) = h.text(call(MESSAGES, body.to_string())).await;
    let seq: Vec<String> = events(&out)
        .into_iter()
        .filter(|(_, d)| d["type"] == "content_block_start" || d["type"] == "content_block_stop")
        .map(|(_, d)| format!("{}{}", d["type"].as_str().unwrap_or(""), d["index"]))
        .collect();
    assert_eq!(
        seq,
        [
            "content_block_start0",
            "content_block_stop0",
            "content_block_start1",
            "content_block_stop1"
        ],
        "{out}"
    );
}

/// Restores a hidden email in tool calls' arguments, as a PII plugin does.
#[derive(Clone)]
struct RestoreArguments;

impl Filter<Reply> for RestoreArguments {
    fn edit(&mut self, mut reply: Reply, out: &mut Vec<Reply>) -> Result<(), Fault> {
        for piece in &mut reply.content {
            if let Piece::ToolCall { arguments, .. } = piece {
                *arguments = arguments.replace("<EMAIL_1>", "bob@x.io");
            }
        }
        out.push(reply);
        Ok(())
    }
}

/// An edit of a streamed tool call's arguments is written into the
/// provider's own events, which keep the rest of what they say.
#[tokio::test]
async fn an_edited_tool_call_is_written_into_anthropics_own_events() {
    let event = |data: Value| {
        format!(
            "event: {}\ndata: {data}\n\n",
            data["type"].as_str().unwrap_or("")
        )
    };
    let mut stream: Vec<String> = ANTHROPIC_STREAM.iter().map(|s| s.to_string()).collect();
    stream.splice(
        5..5,
        [
            event(json!({"type": "content_block_start", "index": 1,
                "content_block": {"type": "tool_use", "id": "t", "name": "send", "input": {}}})),
            event(json!({"type": "content_block_delta", "index": 1,
                "delta": {"type": "input_json_delta", "partial_json": "{\"to\":\"<EMAIL_1>\"}"}})),
            event(json!({"type": "content_block_stop", "index": 1})),
        ],
    );
    let h = Harness::new(vec![Arc::new(Adds(RestoreArguments))]).await;
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.anthropic.answer(sse(&chunks));
    let body = json!({ "model": "claude-x", "max_tokens": 10, "stream": true,
        "messages": [{ "role": "user", "content": "mail bob" }] });

    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;

    assert_eq!(status, StatusCode::OK, "{out}");
    let events = events(&out);
    let arguments: String = events
        .iter()
        .filter_map(|(_, v)| v.pointer("/delta/partial_json").and_then(Value::as_str))
        .collect();
    assert_eq!(arguments, "{\"to\":\"bob@x.io\"}", "{out}");
    let start = events
        .iter()
        .find(|(_, v)| v["content_block"]["type"] == "tool_use")
        .map(|(_, v)| v["content_block"].clone());
    assert_eq!(
        start,
        Some(json!({"type": "tool_use", "id": "t", "name": "send", "input": {}}))
    );
}

/// Sends every request to OpenAI, so OpenAI's own chunks are relayed.
struct ToOpenAi;

#[async_trait]
impl Plugin for ToOpenAi {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.set_target("openai");
        Ok(None)
    }
}

/// The same edit in OpenAI's chunks, streamed and whole.
#[tokio::test]
async fn an_edited_tool_call_is_written_into_openais_own_chunks() {
    let chunk = |choices: Value| {
        format!(
            "data: {}\n\n",
            json!({"id": "c1", "model": "gpt-x", "choices": choices})
        )
    };
    let stream = [
        chunk(
            json!([{"index": 0, "delta": {"role": "assistant", "content": null,
            "tool_calls": [{"index": 0, "id": "t", "type": "function",
            "function": {"name": "send", "arguments": ""}}]}}]),
        ),
        chunk(json!([{"index": 0, "delta": {"tool_calls": [{"index": 0,
            "function": {"arguments": "{\"to\":\"<EMAIL_1>\"}"}}]}}])),
        chunk(json!([{"index": 0, "delta": {}, "finish_reason": "tool_calls"}])),
        "data: [DONE]\n\n".to_owned(),
    ];
    let plugins =
        || -> Vec<Arc<dyn Plugin>> { vec![Arc::new(ToOpenAi), Arc::new(Adds(RestoreArguments))] };
    let h = Harness::with(plugins()).await;
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.openai.answer(sse(&chunks));
    let body = json!({ "model": "gpt-x", "stream": true,
        "messages": [{ "role": "user", "content": "mail bob" }] });

    let (status, out) = h.text(call(CHAT, body.to_string())).await;

    assert_eq!(status, StatusCode::OK, "{out}");
    let calls: Vec<Value> = events(&out)
        .into_iter()
        .filter_map(|(_, v)| v.pointer("/choices/0/delta/tool_calls/0").cloned())
        .collect();
    let arguments: String = calls
        .iter()
        .filter_map(|c| c.pointer("/function/arguments").and_then(Value::as_str))
        .collect();
    assert_eq!(arguments, "{\"to\":\"bob@x.io\"}", "{out}");
    assert_eq!(calls[0]["id"], "t", "{out}");

    let whole = json!({"id": "c2", "model": "gpt-x", "choices": [{"index": 0,
        "finish_reason": "tool_calls", "message": {"role": "assistant", "content": null,
        "tool_calls": [{"id": "t", "type": "function",
            "function": {"name": "send", "arguments": "{\"to\":\"<EMAIL_1>\"}"}}]}}]});
    let h = Harness::with(plugins()).await;
    h.openai
        .answer(json_response(StatusCode::OK, &whole.to_string()));
    let body = json!({ "model": "gpt-x", "messages": [{ "role": "user", "content": "mail bob" }] });

    let (status, out) = h.text(call(CHAT, body.to_string())).await;

    assert_eq!(status, StatusCode::OK, "{out}");
    let mut expected = whole;
    expected["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"] =
        "{\"to\":\"bob@x.io\"}".into();
    assert_eq!(serde_json::from_str::<Value>(&out).ok(), Some(expected));
}
