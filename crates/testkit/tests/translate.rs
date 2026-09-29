//! A client in one format reaching a provider in another.

use respan_gateway_core::futures;
use respan_gateway_http::bytes;
use respan_gateway_http::http;
use std::sync::{Arc, Mutex};

use http::{HeaderValue, StatusCode};
use respan_gateway_core::async_trait;
use respan_gateway_core::{ClientContext, Fault, Plugin, ProviderContext, Response, codes};
use respan_gateway_formats::conversation::{Conversation, Finish, Piece, Reply, units};
use respan_gateway_formats::{AnthropicMessages, OpenAiChat};
use respan_gateway_plugins::ModelRouter;
use respan_gateway_testkit::{MockTransport, Outcomes, mock, post, read_whole, serve};

mod common;
use common::{
    Adds, CHAT, Call, Calls, Disclaimer, Flip, Footer, Harness, HoldAll, MESSAGES, Metered,
    Nameless, OPENAI, Shout, Silence, Unmask, add_provider, call, providers, with_plugins,
};
use respan_gateway_core::Filter;
use respan_gateway_http::{Exchange, Transport};
use serde_json::{Value, json};

const ANTHROPIC: &str = "http://anthropic.test";

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
    fn new(extra: Vec<Arc<dyn Plugin>>) -> Self {
        let mut chain: Vec<Arc<dyn Plugin>> = vec![Arc::new(Cross)];
        chain.extend(extra);
        Self::with(chain)
    }

    /// Serves both formats with no target. Plugins, in order: the log,
    /// `chain`, and a router sending Claude models to Anthropic.
    fn with(chain: Vec<Arc<dyn Plugin>>) -> Self {
        let (log, calls) = (Outcomes::default(), Calls::default());
        let mock = MockTransport::new();
        let mut plugins: Vec<Arc<dyn Plugin>> =
            vec![Arc::new(log.clone()), Arc::new(calls.clone())];
        plugins.extend(chain);
        plugins.push(Arc::new(
            ModelRouter::new(|c: &Conversation| c.model.as_deref()).route("claude", "anthropic"),
        ));
        let usage = Metered::default();
        plugins.push(Arc::new(usage.clone()));
        let gw = providers([
            add_provider("openai", OpenAiChat, mock.at(OPENAI)),
            add_provider("anthropic", AnthropicMessages, mock.at(ANTHROPIC)),
        ])
        .and_then(|gw| gw.serve(OpenAiChat)?.serve(AnthropicMessages))
        .and_then(|gw| with_plugins(gw, plugins))
        .unwrap_or_else(|e| panic!("gateway: {e}"));
        Self {
            gw,
            mock,
            log,
            calls,
            usage,
        }
    }

    fn sent_json(&self, i: usize) -> Value {
        serde_json::from_slice(self.mock.sent()[i].body()).unwrap_or_default()
    }
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
    let h = Harness::new(Vec::new());
    let version = HeaderValue::from_static("2099-01-01");
    let messages = json!({
        "model": "claude-x", "max_tokens": 9, "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
    });
    for mut req in [
        openai_request("claude-x", true),
        call(MESSAGES, messages.to_string()),
    ] {
        h.mock.reply(ANTHROPIC, mock::sse(ANTHROPIC_STREAM));
        req.headers_mut()
            .insert("anthropic-version", version.clone());
        let (status, _) = h.text(req).await;
        assert_eq!(status, StatusCode::OK);
    }
    let sent = h.mock.sent();
    let said = |i: usize| sent[i].headers().get("anthropic-version").cloned();
    assert_eq!(said(0), Some(HeaderValue::from_static("2023-06-01")));
    assert_eq!(said(1), Some(version));
}

/// A lookalike of the marker a plugin might try to set.
#[derive(Clone)]
struct Relayed;

/// Sends every request to OpenAI, and in `on_provider` does all it can to
/// make every call look like a relay: a marker in its extensions, which
/// does not set `Exchange::relayed`.
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

#[async_trait]
impl<T: Transport> Transport for Marks<T> {
    async fn send(
        &self,
        call: Exchange<respan_gateway_http::RequestHead>,
    ) -> Result<Exchange<respan_gateway_http::ResponseHead>, Fault> {
        let relayed = call.relayed;
        self.1
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(relayed);
        self.0.send(call).await
    }
}

// The kernel sets `Exchange::relayed` on a call that relays the client's
// own message, and no other, whatever a plugin did; only a relay carries
// the client's key to a provider with none of its own.
#[tokio::test]
async fn only_a_relay_is_marked_and_carries_the_clients_key() {
    let (mock, marks) = (MockTransport::new(), Arc::default());
    let at = |base| Marks(mock.at(base), Arc::clone(&marks));
    let gw = providers([
        add_provider("openai", OpenAiChat, at(OPENAI)),
        add_provider("anthropic", AnthropicMessages, at(ANTHROPIC)),
    ])
    .and_then(|gw| gw.serve(OpenAiChat))
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
    let openai = json!({
        "id": "c1", "object": "chat.completion", "model": "gpt-x",
        "choices": [{ "index": 0, "message": { "role": "assistant", "content": "hi" },
            "finish_reason": "stop" }],
    });
    let anthropic = json!({
        "id": "msg_1", "type": "message", "role": "assistant", "model": "claude-x",
        "content": [{ "type": "text", "text": "hi" }],
        "stop_reason": "end_turn", "usage": { "input_tokens": 1, "output_tokens": 1 },
    });
    mock.reply(OPENAI, mock::json(StatusCode::OK, &openai.to_string()));
    mock.reply(
        ANTHROPIC,
        mock::json(StatusCode::OK, &anthropic.to_string()),
    );
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
    let sent = mock.sent();
    let key = |i: usize, name| sent[i].headers().get(name).cloned();
    assert_eq!(
        key(0, "authorization"),
        Some(HeaderValue::from_static("Bearer sk-client"))
    );
    assert_eq!(key(1, "authorization"), None);
    assert_eq!(key(1, "x-api-key"), None);
}

#[tokio::test]
async fn an_openai_client_streams_from_anthropic() {
    let h = Harness::new(Vec::new());
    h.mock.reply(ANTHROPIC, mock::sse(ANTHROPIC_STREAM));

    let (status, body) = h.text(openai_request("claude-x", true)).await;

    assert_eq!(status, StatusCode::OK);
    let sent = &h.mock.sent()[0];
    assert_eq!(sent.uri(), "http://anthropic.test/v1/messages");
    assert_eq!(
        sent.headers().get("anthropic-version"),
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
    let h = Harness::new(Vec::new());
    h.mock.reply(OPENAI, mock::sse(OPENAI_STREAM));

    let (status, body) = h.text(anthropic_request(true)).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        h.mock.sent()[0].uri(),
        "http://openai.test/v1/chat/completions"
    );
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
            use respan_gateway_formats::conversation::Conversation;
            cx.filter(|chat: &mut Conversation| {
                chat.stream = false;
                Ok(())
            })?;
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Whole)]);
    let reply = json!({
        "id": "msg_2", "type": "message", "role": "assistant", "model": "claude-x",
        "content": [{ "type": "text", "text": "hi" }],
        "stop_reason": "end_turn", "usage": { "input_tokens": 1, "output_tokens": 1 },
    });
    h.mock
        .reply(ANTHROPIC, mock::json(StatusCode::OK, &reply.to_string()));

    let (_, body) = h.text(openai_request("claude-x", true)).await;

    let sent: Value = serde_json::from_slice(h.mock.sent()[0].body()).unwrap_or_default();
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
    let h = Harness::new(Vec::new());
    let reply = json!({
        "id": "msg_2", "type": "message", "role": "assistant", "model": "claude-x",
        "content": [{ "type": "text", "text": "hi there" }],
        "stop_reason": "max_tokens", "stop_sequence": null,
        "usage": { "input_tokens": 4, "output_tokens": 2 },
    });
    h.mock
        .reply(ANTHROPIC, mock::json(StatusCode::OK, &reply.to_string()));

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

    let h = Harness::new(Vec::new());
    let reply = json!({
        "id": "c2", "object": "chat.completion", "model": "gpt-x",
        "choices": [{ "index": 0, "message": { "role": "assistant", "content": "hi there" }, "finish_reason": "stop" }],
        "usage": { "prompt_tokens": 4, "completion_tokens": 2 },
    });
    h.mock
        .reply(OPENAI, mock::json(StatusCode::OK, &reply.to_string()));

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
    let h = Harness::new(Vec::new());
    let error = r#"{"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}"#;
    h.mock
        .reply(ANTHROPIC, mock::json(StatusCode::TOO_MANY_REQUESTS, error));

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
    let h = Harness::new(Vec::new());
    let format = json!({ "type": "json_object" });
    let body = json!({ "model": "claude-x", "messages": [], "response_format": format });

    let (status, body) = h.text(call(CHAT, body.to_string())).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body.contains("'response_format' in the request cannot be translated"),
        "{body}"
    );
    assert!(h.mock.sent().is_empty());
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
    let h = Harness::new(Vec::new());
    h.mock.reply(ANTHROPIC, mock::sse(ANTHROPIC_STREAM));
    let (status, _) = h.text(openai_agent()).await;
    assert_eq!(status, StatusCode::OK);

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
    let h = Harness::new(Vec::new());
    h.mock
        .reply(OPENAI, mock::json(StatusCode::OK, r#"{"id":"c","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}"#));
    let (status, _) = h.text(anthropic_agent()).await;
    assert_eq!(status, StatusCode::OK);

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
    let h = Harness::new(Vec::new());
    h.mock.reply(ANTHROPIC, mock::sse(ANTHROPIC_TOOL_STREAM));
    // Streamed, with usage.
    let body = json!({ "model": "claude-x", "stream": true, "stream_options": { "include_usage": true },
        "tools": [{ "type": "function", "function": { "name": "read" } }],
        "messages": [{ "role": "user", "content": "hi" }] });
    let req = call(CHAT, body.to_string());
    let (_, out) = h.text(req).await;

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
    let h = Harness::new(Vec::new());
    h.mock.reply(OPENAI, mock::sse(OPENAI_TOOL_STREAM));
    let body = json!({ "model": "gpt-x", "max_tokens": 100, "stream": true,
        "tools": [{ "name": "read", "input_schema": { "type": "object" } }],
        "messages": [{ "role": "user", "content": "hi" }] });
    let (_, out) = h.text(call(MESSAGES, body.to_string())).await;

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
    let h = Harness::new(Vec::new());
    let answer = json!({ "id": "m", "type": "message", "role": "assistant", "model": "c",
        "content": [{ "type": "tool_use", "id": "t1", "name": "read", "input": { "path": "a" } }],
        "stop_reason": "tool_use", "usage": { "input_tokens": 1, "output_tokens": 1 } });
    h.mock
        .reply(ANTHROPIC, mock::json(StatusCode::OK, &answer.to_string()));
    let body = json!({ "model": "claude-x", "messages": [{ "role": "user", "content": "hi" }],
        "tools": [{ "type": "function", "function": { "name": "read" } }] });
    let (_, out) = h.text(call(CHAT, body.to_string())).await;
    let v: Value = serde_json::from_str(&out).unwrap_or_default();
    let message = &v["choices"][0]["message"];
    assert!(message["content"].is_null(), "{out}");
    assert_eq!(
        message["tool_calls"][0]["function"]["arguments"],
        r#"{"path":"a"}"#
    );
    assert_eq!(v["choices"][0]["finish_reason"], "tool_calls");

    // OpenAI's answer to an Anthropic client.
    let h = Harness::new(Vec::new());
    let answer = json!({ "id": "c", "model": "g", "choices": [{ "index": 0, "finish_reason": "tool_calls",
        "message": { "role": "assistant", "content": null, "tool_calls": [
            { "id": "c1", "type": "function", "function": { "name": "read", "arguments": "{\"path\":\"a\"}" } }] } }],
        "usage": { "prompt_tokens": 1, "completion_tokens": 1 } });
    h.mock
        .reply(OPENAI, mock::json(StatusCode::OK, &answer.to_string()));
    let body = json!({ "model": "gpt-x", "max_tokens": 9, "messages": [{ "role": "user", "content": "hi" }],
        "tools": [{ "name": "read", "input_schema": { "type": "object" } }] });
    let (_, out) = h.text(call(MESSAGES, body.to_string())).await;
    let v: Value = serde_json::from_str(&out).unwrap_or_default();
    assert_eq!(
        v["content"],
        json!([{ "type": "tool_use", "id": "c1", "name": "read", "input": { "path": "a" } }])
    );
    assert_eq!(v["stop_reason"], "tool_use");
}

#[tokio::test]
async fn a_tool_that_anthropic_runs_cannot_be_sent_to_openai() {
    let h = Harness::new(Vec::new());
    let body = json!({ "model": "gpt-x", "max_tokens": 9, "messages": [{ "role": "user", "content": "hi" }],
        "tools": [{ "type": "web_search_20250305", "name": "web_search" }] });
    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{out}");
    assert!(h.mock.sent().is_empty());
}

#[tokio::test]
async fn a_cut_translated_stream_ends_with_an_error_in_the_client_format() {
    let h = Harness::new(Vec::new());
    h.mock.reply(ANTHROPIC, mock::sse(&ANTHROPIC_STREAM[..3]));

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

    let h = Harness::new(vec![Arc::new(Thought)]);
    let thinking = "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"thinking\",\"thinking\":\"\"}}\n\n";
    let mut stream = ANTHROPIC_STREAM.to_vec();
    stream.insert(1, thinking);
    h.mock.reply(ANTHROPIC, mock::sse(&stream));

    let (status, body) = h.text(openai_request("claude-x", true)).await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains("[thought] "), "{body}");
}

#[tokio::test]
async fn a_translated_stream_that_starts_with_what_cannot_be_translated_is_refused() {
    let h = Harness::new(Vec::new());
    let thinking = "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"thinking\",\"thinking\":\"\"}}\n\n";
    h.mock
        .reply(ANTHROPIC, mock::sse(&[ANTHROPIC_STREAM[0], thinking]));

    let (status, body) = h.text(openai_request("claude-x", true)).await;

    // Nothing was sent before the failure, so it is a refusal.
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{body}");
    assert!(!body.contains("\"role\":\"assistant\""), "{body}");
}

#[tokio::test]
async fn a_translated_chunk_that_fails_partway_is_refused_whole() {
    let h = Harness::new(Vec::new());
    let chunk = r#"data: {"id":"c","model":"gpt-x","choices":[{"index":0,"delta":{"content":"hi","refusal":"no"}}]}

"#;
    h.mock.reply(OPENAI, mock::sse(&[chunk]));

    let (status, body) = h.text(anthropic_request(true)).await;

    // Its text was not sent before its refusal failed.
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{body}");
    assert!(!body.contains("message_start"), "{body}");
}

#[tokio::test]
async fn an_error_in_a_success_is_a_failure_not_an_empty_answer() {
    let h = Harness::new(Vec::new());
    let error = r#"{"error":{"message":"boom","type":"server_error"}}"#;
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, error));

    let (status, body) = h.text(anthropic_request(false)).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY, "{body}");
    assert!(body.contains("\"type\":\"server_error\""), "{body}");
    assert!(h.log.only().is_err());
}

#[tokio::test]
async fn a_success_that_is_not_a_response_is_a_failure_not_an_empty_answer() {
    let h = Harness::new(Vec::new());
    h.mock
        .reply(OPENAI, mock::json(StatusCode::OK, r#"{"status":"queued"}"#));

    let (status, body) = h.text(anthropic_request(false)).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY, "{body}");
    assert!(h.log.only().is_err());
}

#[tokio::test]
async fn a_filter_edits_translated_text() {
    let h = Harness::new(vec![Arc::new(Shout)]);
    h.mock.reply(OPENAI, mock::sse(OPENAI_STREAM));

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
    let h = Harness::new(vec![Arc::new(Canned)]);

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
    assert!(h.mock.sent().is_empty());
}

#[tokio::test]
async fn an_anthropic_stream_is_relayed_byte_for_byte() {
    let h = Harness::new(Vec::new());
    h.mock.reply(ANTHROPIC, mock::sse(ANTHROPIC_STREAM));
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
    let h = Harness::new(Vec::new());
    h.mock.reply(ANTHROPIC, mock::sse(ANTHROPIC_STREAM));
    h.mock.reply(OPENAI, mock::sse(OPENAI_STREAM));
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

    let sent = h.mock.sent();
    assert_eq!(sent[0].uri(), "http://anthropic.test/v1/messages?beta=true");
    assert_eq!(sent[1].uri().query(), None);
}

#[tokio::test]
async fn a_model_nothing_answers_is_a_404_in_the_client_format() {
    let h = Harness::with(Vec::new());

    let (status, body) = h.text(call(CHAT, openai_body("llama-3", false))).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    let v: Value = serde_json::from_str(&body).unwrap_or_default();
    assert_eq!(v["error"]["message"], "nothing answers this request");
    assert!(h.mock.sent().is_empty());
}

#[tokio::test]
async fn an_openai_error_reaches_an_anthropic_client_in_its_format() {
    let h = Harness::new(Vec::new());
    let error = r#"{"error":{"message":"bad key","type":"invalid_request_error"}}"#;
    h.mock
        .reply(OPENAI, mock::json(StatusCode::UNAUTHORIZED, error));

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
    let h = Harness::new(Vec::new());
    h.mock.reply(OPENAI, mock::sse(&quirky));

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
    let h = Harness::new(Vec::new());
    h.mock.reply(ANTHROPIC, mock::sse(&stream));

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
    let h = Harness::new(Vec::new());
    h.mock
        .reply(ANTHROPIC, mock::json(StatusCode::OK, &reply.to_string()));

    let (_, body) = h.text(openai_request("claude-x", false)).await;

    let v: Value = serde_json::from_str(&body).unwrap_or_default();
    assert_eq!(v["choices"][0]["message"]["content"], "one two");
}

#[tokio::test]
async fn a_filter_edits_an_anthropic_stream_in_its_own_shape() {
    let h = Harness::new(vec![Arc::new(Shout)]);
    h.mock.reply(ANTHROPIC, mock::sse(ANTHROPIC_STREAM));
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
    let h = Harness::new(Vec::new());
    h.mock.reply(ANTHROPIC, mock::sse(&[a, b]));

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
    let h = Harness::new(vec![Arc::new(Shout)]);
    h.mock.reply(ANTHROPIC, mock::sse(&stream));
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
    let h = Harness::new(Vec::new());
    h.mock.reply(ANTHROPIC, mock::sse(ANTHROPIC_STREAM));
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

    let sent = h.mock.sent();
    let beta: Vec<_> = sent[0].headers().get_all("anthropic-beta").iter().collect();
    assert_eq!(beta, ["context-1m-2025-08-07", "files-api-2025-04-14"]);
}

#[tokio::test]
async fn a_system_message_mid_conversation_is_refused_not_moved() {
    let h = Harness::new(Vec::new());
    let body = json!({
        "model": "claude-x",
        "messages": [
            { "role": "user", "content": "hi" },
            { "role": "system", "content": "be brief" },
        ],
    });

    let (status, _) = h.text(call(CHAT, body.to_string())).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(h.mock.sent().is_empty());
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

    let h = Harness::new(vec![Arc::new(Truncated)]);
    let chunk = "data: {\"id\":\"c1\",\"model\":\"gpt-real\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":\"stop\"}]}\n\n";
    h.mock
        .reply(OPENAI, mock::sse(&[chunk, "data: [DONE]\n\n"]));

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
    let h = Harness::new(Vec::new());
    // A provider that reports usage on a tool call chunk.
    let chunk = "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"{}\"}}]}}],\"usage\":{\"prompt_tokens\":9,\"completion_tokens\":1}}\n\n";
    h.mock.reply(OPENAI, mock::sse(&[chunk]));

    let _ = h.text(anthropic_request(true)).await;

    assert_eq!(h.usage.total().get(units::INPUT_TOKENS), Some(9.0));
    assert!(h.log.only().is_err());
}

#[tokio::test]
async fn a_translated_response_keeps_the_providers_retry_after() {
    let h = Harness::new(Vec::new());
    let mut limited = mock::json(
        StatusCode::TOO_MANY_REQUESTS,
        r#"{"error":{"type":"rate_limit_error","message":"slow down"}}"#,
    );
    limited
        .headers_mut()
        .insert("retry-after", HeaderValue::from_static("7"));
    h.mock.reply(OPENAI, limited);

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

    let h = Harness::new(vec![Arc::new(Silent)]);

    let (_, body) = h.text(anthropic_request(true)).await;

    let names: Vec<String> = events(&body).into_iter().filter_map(|(e, _)| e).collect();
    assert_eq!(names, ["message_start", "message_delta", "message_stop"]);
}

#[tokio::test]
async fn a_real_openai_reply_reaches_an_anthropic_client() {
    let h = Harness::new(Vec::new());
    // OpenAI's real shape, with null and empty fields.
    let reply = r#"{"id":"chatcmpl-1","object":"chat.completion","created":1,"model":"gpt-x","choices":[{"index":0,"message":{"role":"assistant","content":"hi","refusal":null,"annotations":[]},"logprobs":null,"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":1,"total_tokens":4,"prompt_tokens_details":{"cached_tokens":0,"audio_tokens":0},"completion_tokens_details":{"reasoning_tokens":0,"audio_tokens":0,"accepted_prediction_tokens":0,"rejected_prediction_tokens":0}},"service_tier":"default","system_fingerprint":"fp"}"#;
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, reply));

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
    let h = Harness::new(vec![Arc::new(Unmask)]);
    let stream = masked_anthropic_stream();
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.mock.reply(ANTHROPIC, mock::sse(&chunks));
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
    let h = Harness::new(vec![Arc::new(Unmask)]);
    let stream = masked_anthropic_stream();
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.mock.reply(ANTHROPIC, mock::sse(&chunks));

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
    let h = Harness::new(vec![Arc::new(Unmask)]);
    let reply = json!({
        "id": "msg_1", "type": "message", "role": "assistant", "model": "claude-x",
        "content": [{ "type": "text", "text": "Hi <P1>, bye <", "citations": null }],
        "stop_reason": "end_turn", "usage": { "input_tokens": 5, "output_tokens": 2 },
    });
    h.mock
        .reply(ANTHROPIC, mock::json(StatusCode::OK, &reply.to_string()));
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
    let h = Harness::new(vec![Arc::new(Unmask)]);
    let cut = &ANTHROPIC_STREAM[..ANTHROPIC_STREAM.len() - 1];
    h.mock.reply(ANTHROPIC, mock::sse(cut));
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
    let h = Harness::new(Vec::new());
    let cut = &ANTHROPIC_STREAM[..ANTHROPIC_STREAM.len() - 1];
    h.mock.reply(ANTHROPIC, mock::sse(cut));
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

    let h = Harness::new(vec![Arc::new(Audited)]);
    let limited = r#"{"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}"#;
    h.mock.reply(
        ANTHROPIC,
        mock::json(StatusCode::TOO_MANY_REQUESTS, limited),
    );
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
    let h = Harness::new(vec![Arc::new(NewBlock)]);
    h.mock.reply(ANTHROPIC, mock::sse(ANTHROPIC_STREAM));
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
    let h = Harness::new(vec![Arc::new(NewBlock)]);
    let reply = json!({
        "id": "msg_1", "type": "message", "role": "assistant", "model": "claude-x",
        "content": [{ "type": "text", "text": "Hello" }],
        "stop_reason": "end_turn", "usage": { "input_tokens": 5, "output_tokens": 2 },
    });
    h.mock
        .reply(ANTHROPIC, mock::json(StatusCode::OK, &reply.to_string()));
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

fn anthropic_messages(stream: &[String]) -> (Harness, String) {
    let body = json!({
        "model": "claude-x", "max_tokens": 10, "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
    });
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    let h = Harness::new(vec![Arc::new(Unmask)]);
    h.mock.reply(ANTHROPIC, mock::sse(&chunks));
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
    let (h, body) = anthropic_messages(&stream);

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

    let h = Harness::new(vec![Arc::new(Foot)]);
    h.mock.reply(ANTHROPIC, mock::sse(ANTHROPIC_STREAM));
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

fn filtered<F: Filter<Reply> + Clone + Sync>(filter: F) -> Harness {
    Harness::new(vec![Arc::new(Adds(filter))])
}

impl Clone for Note {
    fn clone(&self) -> Self {
        Note(self.0)
    }
}

#[tokio::test]
async fn text_put_before_anthropics_first_block_goes_into_it() {
    let h = filtered(Note(false));
    h.mock.reply(ANTHROPIC, mock::sse(ANTHROPIC_STREAM));
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
    let h = filtered(Silence);
    let reply = json!({
        "id": "msg_1", "type": "message", "role": "assistant", "model": "claude-x",
        "content": [{ "type": "text", "text": "secret" }],
        "stop_reason": "end_turn", "usage": { "input_tokens": 5, "output_tokens": 2 },
    });
    h.mock
        .reply(ANTHROPIC, mock::json(StatusCode::OK, &reply.to_string()));
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
    let h = filtered(Note(false));
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
    h.mock.reply(ANTHROPIC, mock::sse(&chunks));
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
    let h = filtered(HoldAll(Vec::new()));
    h.mock.reply(ANTHROPIC, mock::sse(ANTHROPIC_STREAM));
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
    let h = filtered(Disclaimer);
    let reply = json!({
        "id": "msg_1", "type": "message", "role": "assistant", "model": "claude-x",
        "content": [{ "type": "text", "text": "Hello" }],
        "stop_reason": "end_turn", "usage": { "input_tokens": 5, "output_tokens": 2 },
    });
    h.mock
        .reply(ANTHROPIC, mock::json(StatusCode::OK, &reply.to_string()));
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
    let h = filtered(HoldAll(Vec::new()));
    let mut stream: Vec<String> = ANTHROPIC_STREAM.iter().map(|s| s.to_string()).collect();
    stream[1] = "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"Well, \"}}\n\n".to_owned();
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.mock.reply(ANTHROPIC, mock::sse(&chunks));
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
    let h = Harness::new(vec![Arc::new(Unmask)]);
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
    h.mock.reply(ANTHROPIC, mock::sse(&chunks));
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
    let h = filtered(HoldText(Vec::new()));
    h.mock.reply(ANTHROPIC, mock::sse(ANTHROPIC_STREAM));

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
    let h = filtered(Nameless);
    let reply = json!({
        "id": "msg_1", "type": "message", "role": "assistant", "model": "claude-x",
        "content": [{ "type": "text", "text": "Hello" }],
        "stop_reason": "end_turn", "usage": { "input_tokens": 5, "output_tokens": 2 },
    });
    h.mock
        .reply(ANTHROPIC, mock::json(StatusCode::OK, &reply.to_string()));
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
    let h = filtered(HoldAll(Vec::new()));
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
    h.mock.reply(ANTHROPIC, mock::sse(&chunks));
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
    let h = Harness::new(Vec::new());
    let mut stream: Vec<String> = OPENAI_STREAM.iter().map(|s| s.to_string()).collect();
    let annotation = json!({"id": "", "choices": [{"index": 0, "delta": {},
        "content_filter_offsets": {"check_offset": 0, "start_offset": 0, "end_offset": 11},
        "content_filter_results": {"hate": {"filtered": false, "severity": "safe"}}}]});
    stream.insert(4, format!("data: {annotation}\n\n"));
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.mock.reply(OPENAI, mock::sse(&chunks));

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
    let h = filtered(LagByOne(None));
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
    h.mock.reply(ANTHROPIC, mock::sse(&chunks));
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
    let h = filtered(filter);
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.mock.reply(ANTHROPIC, mock::sse(&chunks));
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
    let h = Harness::new(vec![Arc::new(Flip)]);
    let whole = json!({
        "id": "msg_2", "type": "message", "role": "assistant", "model": "claude-x",
        "content": [{ "type": "text", "text": "hi" }],
        "stop_reason": "end_turn", "usage": { "input_tokens": 1, "output_tokens": 1 },
    });
    h.mock
        .reply(ANTHROPIC, mock::json(StatusCode::OK, &whole.to_string()))
        .reply(ANTHROPIC, mock::sse(ANTHROPIC_STREAM));
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
    let h = Harness::new(Vec::new());
    let choices = http::Response::builder()
        .status(StatusCode::MULTIPLE_CHOICES)
        .header("location", "http://elsewhere/")
        .body(Vec::new())
        .unwrap_or_default();
    h.mock.reply(ANTHROPIC, choices);

    let (status, headers, _) = h.send(openai_request("claude-x", false)).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(headers.get("location").is_none());
}

#[tokio::test]
async fn a_translated_redirect_is_a_bad_gateway() {
    let ends = CallEnds::default();
    let h = Harness::new(vec![Arc::new(ends.clone())]);
    let moved = http::Response::builder()
        .status(StatusCode::FOUND)
        .header("location", "http://elsewhere/")
        .body(Vec::new())
        .unwrap_or_default();
    h.mock.reply(ANTHROPIC, moved);

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

    let h = Harness::new(vec![Arc::new(Cool)]);
    h.mock.reply(ANTHROPIC, mock::sse(ANTHROPIC_STREAM));

    assert_eq!(
        h.text(openai_request("claude-x", true)).await.0,
        StatusCode::OK
    );
    assert_eq!(h.sent_json(0)["temperature"], 0.0);
}

#[tokio::test]
async fn a_translated_failure_labelled_as_a_stream_is_still_a_failure() {
    let h = Harness::new(Vec::new());
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
    h.mock.reply(OPENAI, failed);

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
    let h = Harness::new(Vec::new());
    let error =
        "data: {\"error\":{\"type\":\"rate_limit_exceeded\",\"message\":\"slow down\"}}\n\n";
    let failed = http::Response::builder()
        .status(StatusCode::TOO_MANY_REQUESTS)
        .header("content-type", "text/event-stream")
        .body(vec![Ok(bytes::Bytes::from(error))])
        .unwrap_or_default();
    h.mock.reply(OPENAI, failed);

    let (status, _, body) = h.send(anthropic_request(true)).await;

    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert!(body.contains("slow down"), "{body}");
    let end = h.log.only().err().map(|f| f.message().to_owned());
    assert_eq!(end.as_deref(), Some("slow down"));
}

#[tokio::test]
async fn an_anthropic_stream_error_with_no_type_keeps_its_words() {
    let h = Harness::new(Vec::new());
    let stream = [
        ANTHROPIC_STREAM[0],
        "data: {\"error\":{\"type\":\"overloaded_error\",\"message\":\"busy\"}}\n\n",
    ];
    h.mock.reply(ANTHROPIC, mock::sse(&stream));

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
    let h = Harness::new(Vec::new());
    let error = "data: {\"error\":{\"message\":\"boom\",\"type\":\"server_error\",\"param\":null,\"code\":null}}\n\n";
    h.mock.reply(
        OPENAI,
        mock::sse(&[OPENAI_STREAM[0], OPENAI_STREAM[1], error]),
    );

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
    let h = Harness::new(Vec::new());
    let error = "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"busy\"}}\n\n";
    let stream = [ANTHROPIC_STREAM[0], ANTHROPIC_STREAM[5], error];
    h.mock.reply(ANTHROPIC, mock::sse(&stream));
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
    let h = Harness::new(Vec::new());
    h.mock.reply(ANTHROPIC, mock::sse(ANTHROPIC_STREAM));
    let mut req = openai_request("claude-x", true);
    for (name, value) in [("user-agent", "OpenAI/Python 1.0"), ("x-app", "notes")] {
        req.headers_mut()
            .insert(name, HeaderValue::from_static(value));
    }

    let _ = h.send(req).await;

    let sent = h.mock.sent();
    for name in ["user-agent", "x-app"] {
        assert!(sent[0].headers().get(name).is_none(), "{name}");
    }
}

// An Anthropic error event goes as it came to an Anthropic client, and in
// OpenAI's words to an OpenAI one.
#[tokio::test]
async fn an_anthropic_error_event_goes_as_it_came_only_to_its_format() {
    let error = "event: error\ndata: {\"type\": \"error\", \"error\": {\"type\": \"overloaded_error\", \"message\": \"busy\"}}\n\n";
    let mut stream = ANTHROPIC_STREAM[..3].to_vec();
    stream.push(error);

    let h = Harness::new(Vec::new());
    h.mock.reply(ANTHROPIC, mock::sse(&stream));
    let body = json!({
        "model": "claude-x", "max_tokens": 10, "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
    });
    let (_, out) = h.text(call(MESSAGES, body.to_string())).await;
    assert!(out.ends_with(error), "{out}");

    let h = Harness::new(Vec::new());
    h.mock.reply(ANTHROPIC, mock::sse(&stream));
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
    let h = Harness::new(Vec::new());
    h.mock.reply(OPENAI, mock::sse(&stream));

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
    let h = Harness::new(Vec::new());
    h.mock
        .reply(ANTHROPIC, mock::json(StatusCode::OK, anthropic));
    let (_, out) = h.text(call(MESSAGES, messages.to_string())).await;
    assert_eq!(out, anthropic);

    // Anthropic to an OpenAI client, in OpenAI's words.
    let h = Harness::new(Vec::new());
    h.mock
        .reply(ANTHROPIC, mock::json(StatusCode::OK, anthropic));
    let (_, out) = h.text(openai_request("claude-x", false)).await;
    let v: Value = serde_json::from_str(&out).unwrap_or_default();
    assert_eq!(v["error"]["code"], "overloaded_error", "{out}");
    assert!(v["type"].is_null(), "{out}");

    // OpenAI to an Anthropic client, in Anthropic's words.
    let h = Harness::new(Vec::new());
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, openai));
    let (_, out) = h.text(anthropic_request(false)).await;
    let v: Value = serde_json::from_str(&out).unwrap_or_default();
    assert_eq!(
        (&v["type"], &v["error"]["message"]),
        (&json!("error"), &json!("boom"))
    );
}
