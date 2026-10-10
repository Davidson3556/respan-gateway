//! A client in one format reaching a provider in another.

use respan_gateway_core::futures;
use respan_gateway_http::bytes;
use respan_gateway_http::http;
use std::sync::{Arc, Mutex};

use http::{HeaderValue, StatusCode};
use mockllm::{Answer, FakeAnthropic, FakeOpenAi, Request};
use respan_gateway_conversation::{Block, Delta, Finish, Input, Output};
use respan_gateway_conversation_http::anthropic::AnthropicMessages;
use respan_gateway_conversation_http::openai::{ChatQuirks, OpenAiChat};
use respan_gateway_core::NativeFields;
use respan_gateway_core::async_trait;
use respan_gateway_core::{ClientContext, Fault, Plugin, ProviderContext, Response, Upstream};
use respan_gateway_plugins::ModelRouter;
#[allow(dead_code)] // Shared hooks also expose a call log not used by this suite.
#[path = "../../../../tests/support/hooks.rs"]
mod hooks;
#[allow(dead_code, unused_imports)]
#[path = "../../../../tests/support/http.rs"]
mod http_support;
use hooks::Outcomes;
use http_support::{post, read_whole, serve};

mod support;
use respan_gateway_core::Filter;
use respan_gateway_http::{Body, Completion, HttpTransport};
use serde_json::{Value, json};
use support::{
    Adds, CHAT, Call, Disclaimer, Flip, Footer, HoldAll, MESSAGES, Metered, Nameless, Shout,
    Silence, TextBlocks, Unmask, add_provider, fault_code, providers, says, text, text_of,
    with_plugins,
};
use support::{SendRequest, http_sender, sender};

/// A dialect whose choices carry the moderation they passed, as Azure's do.
const AZURE: OpenAiChat = OpenAiChat::new(
    "azure-chat",
    ChatQuirks {
        inert_choice: &["content_filter_results", "content_filter_offsets"],
        ..ChatQuirks::NONE
    },
);

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
    gw: respan_gateway_core::Gateway,
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
    /// Plugins, in order: the log, [`Cross`], `native`, and a router sending
    /// Claude models to Anthropic, as gateway.example.yaml does.
    async fn new(native: Vec<Arc<dyn Plugin>>) -> Self {
        let mut chain: Vec<Arc<dyn Plugin>> = vec![Arc::new(Cross)];
        chain.extend(native);
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
            ModelRouter::new(|c: &Input| c.model.as_deref()).route("claude", "anthropic"),
        ));
        let usage = Metered::default();
        plugins.push(Arc::new(usage.clone()));
        let gw = providers([
            add_provider("openai", OpenAiChat::OPENAI, openai_http.clone()),
            add_provider("azure", AZURE, openai_http),
            add_provider("anthropic", AnthropicMessages, anthropic_http),
        ])
        .and_then(|gw| {
            gw.client::<Input, _, _>(OpenAiChat::OPENAI)?
                .client::<Input, _, _>(AnthropicMessages)
        })
        .map(|gw| with_plugins(gw, plugins))
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
        HttpTransport::new(base)
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

/// The events of an Anthropic stream, each as `(name, data)`, as a client
/// that reads them sees them: a `message_delta`'s usage is the cumulative
/// output tokens, whatever else it counts.
fn semantic(body: &str) -> Vec<(Option<String>, Value)> {
    let mut all = events(body);
    for (_, data) in &mut all {
        if data["type"] == "message_delta" {
            let output = data["usage"]["output_tokens"].clone();
            data["usage"] = json!({ "output_tokens": output });
        }
    }
    all
}

/// A lookalike of the marker a plugin might try to set.
#[derive(Clone)]
struct Lookalike;

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
        px.extensions_mut().insert(Lookalike);
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
        let relayed = call
            .extensions()
            .get::<respan_gateway_core::Extensions>()
            .is_some_and(|e| e.get::<respan_gateway_http::SendRaw>().is_some());
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

/// Answers with a stream it makes from facts, like a semantic cache.
struct Canned;

#[async_trait]
impl Plugin for Canned {
    async fn on_client(&self, _cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let mut events = vec![Output::start("cached_1", "claude-x")];
        events.extend(text(0, "Hi"));
        events.push(Output::finished(Finish::Stop));
        let events = futures::stream::iter(events.into_iter().map(Ok));
        Ok(Some(Response::stream(events)))
    }
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

/// Fails the end of a response that failed, as an audit might.
struct Audit;

impl Filter<Output> for Audit {
    fn edit(&mut self, event: Output, out: &mut Vec<Output>) -> Result<(), Fault> {
        out.push(event);
        Ok(())
    }

    fn end(&mut self, end: &Result<(), Fault>, _out: &mut Vec<Output>) -> Result<(), Fault> {
        match end {
            Ok(()) => Ok(()),
            _ => Err(Upstream::fault("audit: failed response")),
        }
    }
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

/// Puts a note before the answer, as soon as the response starts.
struct Note(bool);

impl Filter<Output> for Note {
    fn edit(&mut self, event: Output, out: &mut Vec<Output>) -> Result<(), Fault> {
        let first = matches!(event, Output::Start(_)) && !std::mem::replace(&mut self.0, true);
        out.push(event);
        if first {
            // Into the answer's first block, which opens as it comes.
            out.extend([
                Output::block_start(0, Block::Text),
                Output::text(0, "Note: "),
            ]);
        }
        Ok(())
    }
}

async fn filtered<F: Filter<Output> + Clone + Sync>(filter: F) -> Harness {
    Harness::new(vec![Arc::new(Adds(filter))]).await
}

impl Clone for Note {
    fn clone(&self) -> Self {
        Note(self.0)
    }
}

/// Keeps text back to the end, with the end of its block, letting everything
/// else through.
#[derive(Clone, Default)]
struct HoldText(Vec<Output>, TextBlocks);

impl Filter<Output> for HoldText {
    fn edit(&mut self, event: Output, out: &mut Vec<Output>) -> Result<(), Fault> {
        self.1.note(&event);
        match self.1.is_text(&event) && !matches!(event, Output::BlockStart { .. }) {
            true => self.0.push(event),
            false => out.push(event),
        }
        Ok(())
    }

    fn end(&mut self, _end: &Result<(), Fault>, out: &mut Vec<Output>) -> Result<(), Fault> {
        out.append(&mut self.0);
        Ok(())
    }
}

/// Keeps each text back, with the end of its block, until the next one
/// comes, releasing the last at the end.
#[derive(Clone, Default)]
struct LagByOne(Vec<Output>, TextBlocks);

impl Filter<Output> for LagByOne {
    fn edit(&mut self, event: Output, out: &mut Vec<Output>) -> Result<(), Fault> {
        self.1.note(&event);
        let text = text_of(&event).is_some();
        let ends = matches!(&event, Output::BlockEnd { block } if self.1.contains(*block));
        match (text, ends) {
            (true, _) => {
                out.append(&mut self.0);
                self.0.push(event);
            }
            (_, true) => self.0.push(event),
            _ => out.push(event),
        }
        Ok(())
    }

    fn end(&mut self, _end: &Result<(), Fault>, out: &mut Vec<Output>) -> Result<(), Fault> {
        out.append(&mut self.0);
        Ok(())
    }
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

async fn anthropic_through<F: Filter<Output> + Clone + Sync>(
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

/// Records how each call ends on the provider edge.
#[derive(Clone, Default)]
struct CallEnds(Arc<Mutex<Vec<Result<(), Fault>>>>);

#[async_trait]
impl Plugin for CallEnds {
    async fn on_provider_end(&self, _px: &ProviderContext, end: &Result<(), Fault>) {
        self.0.lock().map(|mut all| all.push(end.clone())).ok();
    }
}

/// Aliases the model, as a tenant or routing plugin does; touches nothing else.
struct Alias;

#[async_trait]
impl Plugin for Alias {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.filter(|chat: &mut Input| {
            chat.model = Some("claude-sonnet-x".into());
            Ok(())
        })?;
        Ok(None)
    }
}

const OPENAI_PARALLEL_STREAM: &[&str] = &[
    // Both calls start in one chunk, then their arguments come by index.
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":null,\"tool_calls\":[{\"index\":0,\"id\":\"call_a\",\"type\":\"function\",\"function\":{\"name\":\"read\",\"arguments\":\"\"}},{\"index\":1,\"id\":\"call_b\",\"type\":\"function\",\"function\":{\"name\":\"read\",\"arguments\":\"\"}}]}}]}\n\n",
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"{\\\"path\\\":\\\"a\\\"}\"}}]}}]}\n\n",
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":1,\"function\":{\"arguments\":\"{\\\"path\\\":\\\"b\\\"}\"}}]}}]}\n\n",
    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
    "data: [DONE]\n\n",
];

// ---------------- loop-4 audit ----------------

/// Sends the client's message as the call's own input, as a guardrail that
/// rewrites the prompt does: never a relay, so the provider's pieces are
/// written anew even in the client's own format.
struct L4Own(&'static str);

#[async_trait]
impl Plugin for L4Own {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let chat = cx.input::<Input>()?.clone();
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

/// Restores a hidden email in tool calls' arguments, as a PII plugin does.
#[derive(Clone, Default)]
struct RestoreArguments;

impl Filter<Output> for RestoreArguments {
    fn edit(&mut self, mut reply: Output, out: &mut Vec<Output>) -> Result<(), Fault> {
        if let Output::Delta {
            delta: Delta::Arguments(arguments),
            ..
        } = &mut reply
        {
            *arguments = arguments.replace("<EMAIL_1>", "bob@x.io");
        }
        out.push(reply);
        Ok(())
    }
}

/// Sends every request to the provider it names, as `openai`, so its own
/// chunks are relayed.
struct To(&'static str);

#[async_trait]
impl Plugin for To {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.set_target(self.0);
        Ok(None)
    }
}

#[path = "translate/errors.rs"]
mod errors;
#[path = "translate/filters.rs"]
mod filters;
#[path = "translate/requests.rs"]
mod requests;
#[path = "translate/responses.rs"]
mod responses;
#[path = "translate/tools.rs"]
mod tools;
