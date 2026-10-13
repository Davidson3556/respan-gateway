//! The kernel's promises, driven through the OpenAI format.

use respan_gateway_conversation_http::Schema;
#[allow(dead_code)]
#[path = "../../../../tests/support/hooks.rs"]
mod hooks;
#[allow(dead_code)]
#[path = "../../../../tests/support/http.rs"]
mod http_support;
mod support;

use futures::TryStreamExt;
use mockllm::{Answer, FakeOpenAi};
use respan_gateway_core::futures;
use respan_gateway_http::bytes;
use respan_gateway_http::{http, http_body, http_body_util};
use respan_gateway_http_faults::Status;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use hooks::{Calls, Outcomes};
use http::{HeaderMap, HeaderValue, StatusCode};
use http_support::{post, read_whole, serve};
use respan_gateway_conversation::Usage;
use respan_gateway_conversation::{
    Assembled, Content, Delta, Finish, Input, Message, Output, Role, set_model,
};
use respan_gateway_conversation_http::anthropic::AnthropicMessages;
use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_core::Details;
use respan_gateway_core::async_trait;
use respan_gateway_core::{
    BadRequest, ClientContext, Connection, ErrorCode, Fault, Filter, Format, Gateway, Plugin,
    ProviderContext, Response, Upstream,
};
use respan_gateway_http::Body;
use respan_gateway_http::raw::Original;
use respan_gateway_http::{ClientIo, Completion, Encoder};
use respan_gateway_http_transport::HttpTransport;
use serde_json::json;
use support::{
    Adds, Call, Disclaimer, Flip, Footer, HoldAll, Metered, Nameless, SendRequest, Shout, Silence,
    Unmask, add_provider, fault_code, http_sender, providers, says, sender, text, text_of,
    text_of_mut, with_plugins,
};

const OPENAI: &str = "http://openai.test/v1";
const BACKUP: &str = "http://backup.test/v1";
const CLAUDE: &str = "http://claude.test";
const CHAT: &str = "/v1/chat/completions";
const MESSAGES: &str = "/v1/messages";
const STREAM: &[&str] = &[
    "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hel",
    "lo\"}}]}\n\ndata: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"}}]}\n\n",
    "data: {\"id\":\"c1\",\"choices\":[],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2}}\n\n",
    "data: [DONE]\n\n",
];
const STREAM_REQ: &str = r#"{"model":"gpt-x","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"hi"}]}"#;
const UNARY_REQ: &str = r#"{"model":"gpt-x",  "messages":[]}"#;
const UNARY: &str = r#"{"id":"c2","choices":[{"message":{"content":"hi"}}],"usage":{"prompt_tokens":4,"completion_tokens":1}}"#;

fn call(path: &str, body: impl Into<bytes::Bytes>) -> Call {
    post(path, body)
}

/// The events of a complete OpenAI body, streamed or whole.
fn events_of(body: &str) -> Vec<Output> {
    let kind = match body.starts_with("data:") {
        true => "text/event-stream",
        false => "application/json",
    };
    let mut head = http::Response::new(()).into_parts().0;
    head.headers
        .insert(http::header::CONTENT_TYPE, HeaderValue::from_static(kind));
    let ask = http::Request::new(()).into_parts().0;
    let answer = http::Response::from_parts(head, bytes::Bytes::from(body.to_owned()));
    OpenAiChat
        .decode_response(answer, &ask)
        .unwrap_or_else(|f| panic!("{f}: {body}"))
}

/// What a complete OpenAI body says, folded into the whole answer: how the
/// answer is framed, and what the provider's fields beside it were, do not
/// matter.
fn gist(body: &str) -> Assembled {
    let mut all = Assembled::default();
    for event in events_of(body) {
        all.push(&event).unwrap_or_else(|f| panic!("{f}: {body}"));
    }
    if let Some(start) = &mut all.start {
        start.native = None;
    }
    all
}

/// The text the chunks of an OpenAI stream add.
fn gist_text(chunks: &[serde_json::Value]) -> String {
    let text = chunks
        .iter()
        .filter_map(|c| c["choices"][0]["delta"]["content"].as_str());
    text.collect()
}

/// The JSON of each data line of an OpenAI stream, but its `[DONE]`.
fn sent_chunks(body: &str) -> Vec<serde_json::Value> {
    body.split("\n\n")
        .filter_map(|e| e.strip_prefix("data: "))
        .filter_map(|d| serde_json::from_str(d).ok())
        .collect()
}
fn chat() -> Call {
    call(CHAT, STREAM_REQ)
}
fn unary_chat() -> Call {
    call(CHAT, UNARY_REQ)
}
/// A valid provider request for cases that exercise a strict real fake server.
fn valid_unary_chat() -> Call {
    call(
        CHAT,
        r#"{"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}"#,
    )
}

/// Exact protocol fragments and faults that a strict server cannot emit.
mod fixture {
    use super::*;
    pub type Reply = http::Response<Vec<Result<bytes::Bytes, Fault>>>;
    pub fn sse(chunks: &[&str]) -> Reply {
        typed(
            StatusCode::OK,
            "text/event-stream",
            chunks
                .iter()
                .map(|c| Ok(bytes::Bytes::copy_from_slice(c.as_bytes())))
                .collect(),
        )
    }
    pub fn json(status: StatusCode, body: &str) -> Reply {
        typed(
            status,
            "application/json",
            vec![Ok(bytes::Bytes::copy_from_slice(body.as_bytes()))],
        )
    }
    fn typed(
        status: StatusCode,
        kind: &'static str,
        chunks: Vec<Result<bytes::Bytes, Fault>>,
    ) -> Reply {
        let mut answer = http::Response::new(chunks);
        *answer.status_mut() = status;
        answer
            .headers_mut()
            .insert(http::header::CONTENT_TYPE, HeaderValue::from_static(kind));
        answer
    }
}

/// One provider's exact responses and an ordered record shared with peers.
/// Routing is explicit at gateway construction; there is no URL registry.
#[derive(Clone)]
struct Local {
    base: &'static str,
    replies: Arc<Mutex<VecDeque<fixture::Reply>>>,
    sent: Sent,
}
#[derive(Clone, Default)]
struct Sent(Arc<Mutex<Vec<http::Request<bytes::Bytes>>>>);
impl Sent {
    fn sent(&self) -> Vec<http::Request<bytes::Bytes>> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}
impl Local {
    fn new(base: &'static str, sent: Sent) -> Self {
        Self {
            base,
            replies: Arc::default(),
            sent,
        }
    }
    fn reply(&self, answer: fixture::Reply) -> &Self {
        self.replies
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push_back(answer);
        self
    }
    fn transport(&self, headers: HeaderMap) -> SendRequest {
        let site = self.clone();
        sender(move |call| {
            let site = site.clone();
            let headers = headers.clone();
            async move {
                let mut call = call;
                respan_gateway_http::call::credentials(&mut call, &headers);
                let (mut head, body) = call.into_parts();
                let path = head.uri.path_and_query().map_or("/", |p| p.as_str());
                head.uri = format!("{}{path}", site.base)
                    .parse()
                    .map_err(|e| Fault::config("bad test provider URL").with_source(e))?;
                let bytes = http_body_util::BodyExt::collect(body).await?.to_bytes();
                head.extensions = http::Extensions::new();
                site.sent
                    .0
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(http::Request::from_parts(head, bytes));
                let answer = site
                    .replies
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .pop_front()
                    .ok_or_else(|| Upstream::fault("no scripted provider response"))?;
                let (mut head, frames) = answer.into_parts();
                respan_gateway_http::call::strip_origin_headers(&mut head.headers);
                Ok((
                    http::Response::from_parts(head, Body::stream(futures::stream::iter(frames))),
                    None,
                ))
            }
        })
    }
    fn plain(&self) -> SendRequest {
        self.transport(HeaderMap::new())
    }
}
#[derive(Clone)]
struct Sites {
    openai: Local,
    backup: Local,
    claude: Local,
    sent: Sent,
}
impl Default for Sites {
    fn default() -> Self {
        let sent = Sent::default();
        Self {
            openai: Local::new(OPENAI, sent.clone()),
            backup: Local::new(BACKUP, sent.clone()),
            claude: Local::new(CLAUDE, sent.clone()),
            sent,
        }
    }
}

/// A gateway with a named OpenAI provider and optional provider key.
fn provider(site: &Local, name: &str, key: Option<&'static str>) -> support::AddProvider {
    let mut headers = HeaderMap::new();
    if let Some(key) = key {
        headers.insert(http::header::AUTHORIZATION, HeaderValue::from_static(key));
    }
    add_provider(name, OpenAiChat, site.transport(headers))
}
fn real_sse(chunks: &[&str]) -> Answer {
    let events = chunks
        .iter()
        .flat_map(|chunk| chunk.split("\n\n"))
        .filter(|event| !event.is_empty())
        .map(|event| json!(event))
        .collect();
    Answer::events(events)
}

fn fake_transport(fake: &FakeOpenAi) -> SendRequest {
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::AUTHORIZATION,
        HeaderValue::from_static("Bearer sk-test"),
    );
    http_sender(
        HttpTransport::new(fake.base_url())
            .and_then(|http| http.with_headers(headers))
            .unwrap_or_else(|fault| panic!("fake provider transport: {fault}")),
    )
}

fn gateway(
    list: impl IntoIterator<Item = support::AddProvider>,
    plugins: Vec<Arc<dyn Plugin>>,
    target: Option<&str>,
) -> Result<Gateway, Fault> {
    let gw = with_plugins(providers(list)?.client::<Input, _, _>(OpenAiChat)?, plugins);
    match target {
        Some(target) => gw.target(target),
        None => Ok(gw),
    }
}

struct Harness {
    gw: Gateway,
    sites: Sites,
    log: Outcomes,
    calls: Calls,
    usage: Metered,
}
impl Harness {
    fn new(chain: Vec<Arc<dyn Plugin>>, target: Option<&str>) -> Self {
        Self::with_openai(chain, target, None)
    }
    fn real(fake: &FakeOpenAi, chain: Vec<Arc<dyn Plugin>>, target: Option<&str>) -> Self {
        Self::with_openai(chain, target, Some(fake))
    }
    fn with_openai(
        chain: Vec<Arc<dyn Plugin>>,
        target: Option<&str>,
        fake: Option<&FakeOpenAi>,
    ) -> Self {
        let (log, calls, sites) = (Outcomes::default(), Calls::default(), Sites::default());
        let mut plugins: Vec<Arc<dyn Plugin>> =
            vec![Arc::new(log.clone()), Arc::new(calls.clone())];
        plugins.extend(chain);
        let usage = Metered::default();
        plugins.push(Arc::new(usage.clone()));
        let openai = match fake {
            Some(fake) => add_provider("openai", OpenAiChat, fake_transport(fake)),
            None => provider(&sites.openai, "openai", Some("Bearer sk-test")),
        };
        let list = [openai, provider(&sites.backup, "backup", None)];
        let gw = gateway(list, plugins, target).unwrap_or_else(|e| panic!("gateway: {e}"));
        Self {
            gw,
            sites,
            log,
            calls,
            usage,
        }
    }
    async fn send(&self, req: Call) -> (StatusCode, HeaderMap, Result<bytes::Bytes, Fault>) {
        let out = read_whole(serve(&self.gw, req).await.0).await;
        self.gw.idle().await;
        out
    }
    async fn body(&self, req: Call) -> String {
        self.text(req).await.1
    }
    async fn text(&self, req: Call) -> (StatusCode, String) {
        let (status, _, body) = self.send(req).await;
        let body = body.unwrap_or_else(|fault| panic!("aborted: {fault}"));
        (status, String::from_utf8_lossy(&body).into_owned())
    }
}
fn relay() -> Harness {
    Harness::new(Vec::new(), Some("openai"))
}

struct Cool;
#[async_trait]
impl Plugin for Cool {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.filter(|chat: &mut Input| {
            chat.options.temperature = Some(0.0);
            Ok(())
        })?;
        Ok(None)
    }
}
struct OnProvider(fn(&mut ProviderContext) -> Result<(), Fault>);
#[async_trait]
impl Plugin for OnProvider {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        (self.0)(px)
    }
}

/// The error frame for a stream that stops short.
const TRUNCATED: &str = "data: {\"error\":{\"message\":\"stream ended before completion\",\"type\":\"truncated\",\"param\":null,\"code\":\"truncated\"}}\n\n";

/// An output guardrail: fails the response with 403 `blocked` at the first
/// output it catches.
struct Guard(fn(&Output) -> bool);

#[async_trait]
impl Plugin for Guard {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let catches = self.0;
        cx.filter(move |e: &mut Output| {
            if catches(e) {
                return Err(Fault::new("not allowed")
                    .with(ErrorCode("blocked".into()))
                    .with(Status(StatusCode::FORBIDDEN)));
            }
            Ok(())
        })?;
        Ok(None)
    }
}

/// Gathers the streamed text without changing anything.
#[derive(Clone, Default)]
struct Transcript(Arc<Mutex<String>>);

impl Transcript {
    fn text(&self) -> String {
        self.0.lock().map(|t| t.clone()).unwrap_or_default()
    }
}

#[async_trait]
impl Plugin for Transcript {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let text = self.0.clone();
        cx.filter(move |e: &mut Output| {
            if let (Ok(mut t), Some(event)) = (text.lock(), text_of(e)) {
                t.push_str(event);
            }
            Ok(())
        })?;
        Ok(None)
    }
}

struct Refuse(Fault);

#[async_trait]
impl Plugin for Refuse {
    async fn on_client(&self, _cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        Err(self.0.clone())
    }
}

/// Never finishes its request hook.
struct Hang(Outcomes);

#[async_trait]
impl Plugin for Hang {
    async fn on_client(&self, _cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        std::future::pending().await
    }

    async fn on_client_end(&self, cx: &ClientContext, end: &Result<(), Fault>) {
        self.0.on_client_end(cx, end).await;
    }
}

struct PanicAtEnd;

#[async_trait]
impl Plugin for PanicAtEnd {
    async fn on_client_end(&self, _cx: &ClientContext, _end: &Result<(), Fault>) {
        panic!("a buggy plugin");
    }
}

/// Answers from the first provider that succeeds, read to its content, as
/// a provider's error answer is a failure.
struct Fallback(Vec<&'static str>);

#[async_trait]
impl Plugin for Fallback {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let mut last = Err(Upstream::fault("no providers"));
        for name in &self.0 {
            match cx.call(*name).await {
                Ok(mut resp) => {
                    let held = resp.buffer_until(Output::is_content).await;
                    if held.is_ok() {
                        return Ok(Some(resp));
                    }
                    // Retain the final refusal, including its head and bytes;
                    // replacing it explicitly abandons the earlier attempt.
                    last = Ok(resp);
                }
                Err(fault) => last = Err(fault),
            }
        }
        last.map(Some)
    }
}

/// Answers every request itself, like a cache hit.
struct Cached;

#[async_trait]
impl Plugin for Cached {
    async fn on_client(&self, _cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let mut cost = Usage::default();
        cost.output_tokens += 1;
        // Shown in its reply; no provider edge, so no meter, sees it.
        let mut answer = vec![Output::start("cached", "gpt-x")];
        answer.extend(text(0, "hi"));
        answer.push(Output::usage(cost));
        let answer = futures::stream::iter(answer.into_iter().map(Ok));
        Ok(Some(Response::stream(answer)))
    }
}

/// Records its name in a shared log on each hook, both edges'.
struct Named(&'static str, Arc<Mutex<Vec<String>>>);

impl Named {
    fn note(&self, hook: &str) {
        if let Ok(mut log) = self.1.lock() {
            log.push(format!("{} {hook}", self.0));
        }
    }
}

#[async_trait]
impl Plugin for Named {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        self.note("client");
        let name = self.0;
        cx.filter(move |r: &mut Output| {
            if let Some(text) = text_of_mut(r) {
                *text = format!("{name}({text})");
            }
            Ok(())
        })?;
        Ok(None)
    }

    async fn on_client_end(&self, _cx: &ClientContext, _end: &Result<(), Fault>) {
        self.note("client_end");
    }

    async fn on_provider(&self, _px: &mut ProviderContext) -> Result<(), Fault> {
        self.note("provider");
        Ok(())
    }

    async fn on_provider_end(&self, _px: &ProviderContext, _end: &Result<(), Fault>) {
        self.note("provider_end");
    }
}

/// The chunks an OpenAI client gets from `stream` through a filter that
/// keeps the finish back to the end, adding a footer before it.
async fn footed(stream: &[String]) -> (String, Vec<serde_json::Value>) {
    struct Foot;

    #[async_trait]
    impl Plugin for Foot {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(Footer(0, None))?;
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Foot)], Some("openai"));
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.sites.openai.reply(fixture::sse(&chunks));
    let (status, _, body) = h.send(chat()).await;
    assert_eq!(status, StatusCode::OK);
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    let chunks = body
        .split("\n\n")
        .filter_map(|e| e.strip_prefix("data: "))
        .filter_map(|d| serde_json::from_str(d).ok())
        .collect();
    (body, chunks)
}

fn chunk(v: serde_json::Value) -> String {
    format!("data: {v}\n\n")
}

fn said(chunks: &[serde_json::Value]) -> (String, Option<&str>, Option<u64>) {
    let text = chunks
        .iter()
        .filter_map(|c| c.pointer("/choices/0/delta/content")?.as_str())
        .collect();
    let finish = chunks
        .iter()
        .find_map(|c| c.pointer("/choices/0/finish_reason")?.as_str());
    let usage = chunks
        .iter()
        .find_map(|c| c["usage"]["completion_tokens"].as_u64());
    (text, finish, usage)
}

fn filtered<F: Filter<Output> + Clone + Sync>(filter: F) -> Harness {
    Harness::new(vec![Arc::new(Adds(filter))], Some("openai"))
}

fn tool_call_message(content: serde_json::Value) -> String {
    json!({"id": "c2", "choices": [{"index": 0, "finish_reason": "tool_calls",
        "message": {"role": "assistant", "content": content,
            "tool_calls": [{"id": "t", "type": "function", "function": {"name": "f", "arguments": "{}"}}]}}]})
    .to_string()
}

// The provider edge: hooks once per call, whoever makes it.

/// Bills on the provider edge: each call, as `target/model`, when its
/// `on_provider` runs and when it ends; `client` if its `on_client` runs.
#[derive(Clone, Default)]
struct Bill {
    started: Arc<Mutex<Vec<String>>>,
    ended: Arc<Mutex<Vec<Ended>>>,
}

/// A call's name and how it ended.
type Ended = (String, Result<(), Fault>);

impl Bill {
    fn started(&self) -> Vec<String> {
        self.started.lock().map(|s| s.clone()).unwrap_or_default()
    }

    /// Each call's end, by call.
    fn ended(&self) -> Vec<Ended> {
        let mut ended = self.ended.lock().map(|e| e.clone()).unwrap_or_default();
        ended.sort_by(|a, b| a.0.cmp(&b.0));
        ended
    }
}

fn call_name(px: &ProviderContext) -> String {
    let model = px.input::<Input>().ok().and_then(|c| c.model.clone());
    format!("{}/{}", px.provider(), model.unwrap_or_default())
}

#[async_trait]
impl Plugin for Bill {
    async fn on_client(&self, _cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        self.started
            .lock()
            .map(|mut s| s.push("client".into()))
            .ok();
        Ok(None)
    }

    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        self.started.lock().map(|mut s| s.push(call_name(px))).ok();
        Ok(())
    }

    async fn on_provider_end(&self, px: &ProviderContext, end: &Result<(), Fault>) {
        let ended = (call_name(px), end.clone());
        self.ended.lock().map(|mut e| e.push(ended)).ok();
    }
}

fn input_tokens(usage: &Usage) -> u64 {
    usage.input_tokens
}

/// Each metered call's input tokens, by call.
fn metered(h: &Harness) -> Vec<(String, u64)> {
    let mut calls: Vec<_> = h.usage.by_call();
    calls.sort_by(|a, b| a.0.cmp(&b.0));
    calls
        .into_iter()
        .map(|(c, u)| (c, input_tokens(&u)))
        .collect()
}

fn ended_with(bill: &Bill, code: &str) -> bool {
    let ended = bill.ended();
    ended.len() == 1 && matches!(&ended[0].1, Err(f) if fault_code(f) == code)
}

/// A tenant's key, which a format's request encoder does not see: a
/// provider-edge plugin leaves it in the call's extensions for the transport.
#[derive(Clone)]
struct TenantKey(&'static str);

/// Sends the tenant key a provider-edge plugin left in the call's
/// extensions.
struct TenantSender(SendRequest);

impl TenantSender {
    pub async fn send(
        &self,
        mut req: http::Request<Body>,
    ) -> Result<(http::Response<Body>, Option<Completion>), Fault> {
        if let Some(key) = req
            .extensions()
            .get::<respan_gateway_core::Extensions>()
            .and_then(|e| e.get::<TenantKey>())
            .cloned()
        {
            let key = HeaderValue::from_static(key.0);
            req.headers_mut().insert("x-tenant-key", key);
        }
        (self.0)(req).await
    }
}
impl From<TenantSender> for SendRequest {
    fn from(value: TenantSender) -> Self {
        let value = std::sync::Arc::new(value);
        sender(move |call| {
            let value = value.clone();
            async move { value.send(call).await }
        })
    }
}

/// A response a plugin keeps past its request.
#[derive(Clone, Default)]
struct Kept(Arc<Mutex<Option<Response>>>);

impl Kept {
    fn take(&self) -> Option<Response> {
        self.0.lock().ok().and_then(|mut r| r.take())
    }
}

/// Records each end hook, in the order they ran.
#[derive(Clone, Default)]
struct Ends(Arc<Mutex<Vec<String>>>);

impl Ends {
    fn all(&self) -> Vec<String> {
        self.0.lock().map(|e| e.clone()).unwrap_or_default()
    }
}

#[async_trait]
impl Plugin for Ends {
    async fn on_provider_end(&self, px: &ProviderContext, end: &Result<(), Fault>) {
        let end = end.as_ref().err().map_or("ok".to_owned(), fault_code);
        let end = format!("{} {end}", px.provider());
        self.0.lock().map(|mut e| e.push(end)).ok();
    }

    async fn on_client_end(&self, _cx: &ClientContext, _end: &Result<(), Fault>) {
        self.0.lock().map(|mut e| e.push("client".into())).ok();
    }
}

/// Sends the request and waits for its end hooks, which must not hang.
async fn closes(h: &Harness, call: Call) -> StatusCode {
    let sent = tokio::time::timeout(Duration::from_secs(5), h.send(call));
    sent.await.expect("the end hooks ran").0
}

/// Sets its flag when dropped.
struct Dropped(Arc<AtomicBool>);

impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// A provider that answers 200 and never ends its stream, keeping a guard
/// that marks when the provider's body is dropped.
struct Endless(Arc<AtomicBool>);

impl Endless {
    pub async fn send(
        &self,
        _req: http::Request<Body>,
    ) -> Result<(http::Response<Body>, Option<Completion>), Fault> {
        let guard = Dropped(self.0.clone());
        let never = futures::StreamExt::inspect(futures::stream::pending(), move |_| {
            let _ = &guard;
        });
        let body = http_body_util::StreamBody::new(never);
        let mut resp = http::Response::new(Body::stream(
            http_body_util::BodyExt::into_data_stream(body),
        ));
        let sse = HeaderValue::from_static("text/event-stream");
        resp.headers_mut().insert("content-type", sse);
        Ok((resp, None))
    }
}
impl From<Endless> for SendRequest {
    fn from(value: Endless) -> Self {
        let value = std::sync::Arc::new(value);
        sender(move |call| {
            let value = value.clone();
            async move { value.send(call).await }
        })
    }
}

// How a response begins and ends, whatever goes wrong around it.

fn code(end: &Result<(), Fault>) -> String {
    end.as_ref().err().map_or("ok".to_owned(), fault_code)
}

/// Reads a body frame by frame: the text that arrived, and the fault that
/// broke it off, if any.
async fn frames(resp: http::Response<Body>) -> (String, Option<Fault>) {
    let mut body = resp.into_body();
    let mut text = String::new();
    while let Some(frame) = http_body_util::BodyExt::frame(&mut body).await {
        match frame.map(|f| f.into_data()) {
            Ok(Ok(data)) => text.push_str(&String::from_utf8_lossy(&data)),
            Ok(Err(_)) => {}
            Err(fault) => return (text, Some(fault)),
        }
    }
    (text, None)
}

const HI: &str =
    "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hi\"}}]}\n\n";

fn text_chunk(text: &str) -> String {
    chunk(json!({"id": "c1", "choices": [{"index": 0, "delta": {"content": text}}]}))
}

/// OpenAI Chat whose model panics on an empty body and whose encoder
/// writes part of the text `bad`, then fails.
#[derive(Clone)]
struct Fragile;

impl Format<ClientIo, Input, Output> for Fragile {
    fn name(&self) -> &str {
        OpenAiChat::NAME
    }
    fn matches(&self, io: &ClientIo) -> bool {
        <OpenAiChat as Format<ClientIo, Input, Output>>::matches(&OpenAiChat, io)
    }
    fn bind(&self, io: ClientIo) -> Result<Connection<Input, Output>, Fault> {
        Ok(io
            .with_error(|fault, request, head| OpenAiChat.refusal(fault, request, head))
            .bind(
                |request| OpenAiChat.read_request(request),
                move |reply| {
                    let mut head = reply.head();
                    let encoder = OpenAiChat.response_encoder(&mut head, reply.request());
                    Ok(reply.write(head, Box::new(Garbling(encoder))))
                },
            ))
    }
}

struct Garbling(Box<dyn Encoder<Output>>);

impl Encoder<Output> for Garbling {
    fn encode(&mut self, reply: &Output, out: &mut Vec<bytes::Bytes>) -> Result<(), Fault> {
        if matches!(reply, Output::Delta { delta, .. } if delta.as_str() == "bad") {
            out.push(bytes::Bytes::from_static(b"GARBAGE"));
            return Err(Fault::new("cannot write it")
                .with(ErrorCode("unwritable".into()))
                .with(Status(StatusCode::UNPROCESSABLE_ENTITY)));
        }
        self.0.encode(reply, out)
    }

    fn end(&mut self, end: Result<(), Fault>, out: &mut Vec<bytes::Bytes>) -> Result<(), Fault> {
        self.0.end(end, out)
    }
}

/// A gateway serving `Fragile` to the providers of [`Harness`], answering
/// through `openai`.
fn fragile(sites: &Sites, plugins: Vec<Arc<dyn Plugin>>) -> Gateway {
    let list = [provider(&sites.openai, "openai", None)];
    let gw = providers(list).and_then(|g| g.client::<Input, _, _>(Fragile));
    let gw = gw.and_then(|g| with_plugins(g, plugins).target("openai"));
    gw.unwrap_or_else(|e| panic!("gateway: {e}"))
}

/// Filters the response sent to the client through a filter that panics.
struct PanicFilter;

#[async_trait]
impl Plugin for PanicFilter {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.filter(|_: &mut Output| -> Result<(), Fault> { panic!("a buggy filter") })?;
        Ok(None)
    }
}

/// An OpenAI client's gateway whose target is an Anthropic provider, with
/// `relay` registered.
fn translating(sites: &Sites, relay: respan_gateway_plugin_relay::Relay) -> Gateway {
    let p = add_provider("anthropic", AnthropicMessages, sites.claude.plain());
    let gw = providers([p]).and_then(|g| {
        g.client::<Input, _, _>(OpenAiChat)?
            .plugin(relay)
            .target("anthropic")
    });
    gw.unwrap_or_else(|e| panic!("gateway: {e}"))
}

/// Notes whether each call was relayed.
#[derive(Clone, Default)]
struct RawPermission(Arc<Mutex<Vec<bool>>>);

#[async_trait]
impl Plugin for RawPermission {
    async fn on_provider_end(&self, px: &ProviderContext, _end: &Result<(), Fault>) {
        let granted = px
            .extensions()
            .get::<respan_gateway_http::raw::Relayed>()
            .is_some();
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(granted);
    }
}

/// Sends `openai`'s answer if it has content, else `backup`'s.
struct FirstContent;

#[async_trait]
impl Plugin for FirstContent {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let mut first = cx.call("openai").await?;
        if first.buffer_until(Output::is_content).await.is_ok() {
            return Ok(Some(first));
        }
        drop(first);
        Ok(Some(cx.call("backup").await?))
    }
}

/// Passes every reply; fails at its end.
struct FailsAtEnd;

impl Filter<Output> for FailsAtEnd {
    fn edit(&mut self, reply: Output, out: &mut Vec<Output>) -> Result<(), Fault> {
        out.push(reply);
        Ok(())
    }

    fn end(&mut self, _end: &Result<(), Fault>, _out: &mut Vec<Output>) -> Result<(), Fault> {
        Err(Fault::internal("fails at end"))
    }
}

fn over_the_cap(reply: &mut Output) -> Result<(), Fault> {
    match reply {
        Output::Usage(_) => Err(Fault::new("over the cap")
            .with(ErrorCode("cap".into()))
            .with(Status(StatusCode::PAYMENT_REQUIRED))),
        _ => Ok(()),
    }
}

/// Fails a call at the event that reports its usage.
struct ProviderCap;

#[async_trait]
impl Plugin for ProviderCap {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        px.filter(over_the_cap)
    }
}

// ---- r6 internals probes ----

// ---- r6 fix pass ----

/// `chat` asking for `model`.
fn judged(mut chat: Input, model: &str) -> Input {
    chat.model = Some(model.into());
    chat
}

/// A provider's 429 with `Retry-After` and its own error body.
fn limited() -> fixture::Reply {
    let body = r#"{"error":{"message":"slow down","type":"rate_limit","code":"rate_limit"}}"#;
    let mut r = fixture::json(StatusCode::TOO_MANY_REQUESTS, body);
    r.headers_mut()
        .insert("retry-after", HeaderValue::from_static("7"));
    r
}

/// Answers with the call's response, `until_content()` first if `.0`.
struct Answers(bool);

#[async_trait]
impl Plugin for Answers {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let mut resp = cx.call("openai").await?;
        if self.0 {
            resp.buffer_until(Output::is_content).await?;
        }
        Ok(Some(resp))
    }
}

/// A fallback that preserves the last provider refusal.
struct ArchFallback(Vec<&'static str>);

#[async_trait]
impl Plugin for ArchFallback {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let mut last = Err(Upstream::fault("no provider to try"));
        for name in &self.0 {
            match cx.call(*name).await {
                Ok(mut resp) => {
                    let held = resp.buffer_until(Output::is_content).await;
                    if held.is_ok() {
                        return Ok(Some(resp));
                    }
                    last = Ok(resp);
                }
                Err(fault) => last = Err(fault),
            }
        }
        last.map(Some)
    }
}

// --- Plugin 1: PII redactor on the provider edge ---------------------------

/// Placeholders `<PII_i>` for the i-th value redacted from one call.
#[derive(Default)]
struct Vault(Mutex<Vec<String>>);

impl Vault {
    fn redact(&self, text: &str) -> String {
        let mut seen = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let words = text.split(' ').map(|w| match w.contains('@') {
            false => w.to_owned(),
            true => {
                seen.push(w.to_owned());
                format!("<PII_{}>", seen.len() - 1)
            }
        });
        words.collect::<Vec<_>>().join(" ")
    }

    fn restore(&self, mut text: String) -> String {
        let seen = self.0.lock().unwrap_or_else(|e| e.into_inner());
        for (i, value) in seen.iter().enumerate() {
            text = text.replace(&format!("<PII_{i}>"), value);
        }
        text
    }
}

/// Restores placeholders, holding a `<` that may start one split across
/// deltas.
struct Restore {
    vault: Arc<Vault>,
    held: String,
    block: u32,
}

impl Filter<Output> for Restore {
    fn edit(&mut self, mut reply: Output, out: &mut Vec<Output>) -> Result<(), Fault> {
        // Nothing follows a finish, and text kept back goes before other
        // content: hold nothing past them.
        let text = matches!(
            &reply,
            Output::Delta {
                delta: Delta::Text(_),
                ..
            }
        );
        let last = !text && reply.is_content();
        if let Output::Delta {
            block,
            delta: Delta::Text(delta),
        } = &mut reply
        {
            let mut t = std::mem::take(&mut self.held) + delta.as_str();
            if let Some(i) = t.rfind('<').filter(|&i| !t[i..].contains('>')) {
                self.held = t.split_off(i);
            }
            (self.block, *delta) = (*block, self.vault.restore(t));
            if delta.is_empty() {
                return Ok(());
            }
        }
        if last {
            self.end(&Ok(()), out)?;
        }
        out.push(reply);
        Ok(())
    }

    fn end(&mut self, _end: &Result<(), Fault>, out: &mut Vec<Output>) -> Result<(), Fault> {
        if !self.held.is_empty() {
            let held = std::mem::take(&mut self.held);
            out.push(Output::text(self.block, self.vault.restore(held)));
        }
        Ok(())
    }
}

struct Pii;

#[async_trait]
impl Plugin for Pii {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        if !px.is_kind::<Input>() {
            return Ok(());
        }
        let vault = Arc::new(Vault::default());
        let redact = vault.clone();
        px.filter(move |chat: &mut Input| {
            for message in &mut chat.messages {
                if !message.is_plain() {
                    return Err(BadRequest::fault("content PII redaction cannot read"));
                }
                for part in &mut message.content {
                    if let Content::Text { text, .. } = part {
                        *text = redact.redact(text);
                    }
                }
            }
            Ok(())
        })?;
        px.filter(Restore {
            vault,
            held: String::new(),
            block: 0,
        })
    }
}

// --- Plugin 2: fallback with until_content() ---------------------------------------

/// Tries each provider; answers with the first success that has content,
/// else the last provider's own error response, else the last failure.
struct Fallback11(Vec<&'static str>);

#[async_trait]
impl Plugin for Fallback11 {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let mut last = Err(Upstream::fault("no provider to fall back on"));
        for name in &self.0 {
            // A provider's error answer is the failure
            // `until_content` returns, not a status to check first.
            let resp = match cx.call(*name).await {
                Ok(resp) => resp,
                Err(fault) => {
                    last = Err(fault);
                    continue;
                }
            };
            let mut resp = resp;
            match resp.buffer_until(Output::is_content).await {
                Ok(()) => return Ok(Some(resp)),
                Err(fault) => {
                    drop(resp);
                    last = Err(fault);
                }
            }
        }
        last.map(Some)
    }
}

/// Reports every call's input tokens as 99, as a reseller's markup would.
struct Markup;

#[async_trait]
impl Plugin for Markup {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        px.filter(|r: &mut Output| {
            if let Output::Usage(usage) = r
                && usage.input_tokens > 0
            {
                usage.input_tokens = 99;
            }
            Ok(())
        })
    }
}

// --- until_end ---------------------------------------------------------------

/// Answers with `openai`'s response read to its end, retaining even its
/// failure so earlier bytes and relay metadata replay before the error.
struct ToEnd(Option<&'static str>);

#[async_trait]
impl Plugin for ToEnd {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let mut resp = cx.call("openai").await?;
        let end = resp.until_end().await;
        assert_eq!(end.as_ref().err().map(fault_code).as_deref(), self.0);
        Ok(Some(resp))
    }
}

/// `body` without the time its response says it was `created`, which two
/// runs may not share.
fn steady(body: &str) -> String {
    let mut out = String::new();
    let mut rest = body;
    while let Some(at) = rest.find("\"created\":") {
        let (head, tail) = rest.split_at(at);
        out.push_str(head);
        let tail = &tail["\"created\":".len()..];
        let digits = tail
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(tail.len());
        out.push_str("\"created\":0");
        rest = &tail[digits..];
    }
    out.push_str(rest);
    out
}

/// What the client got for `reply` to `call`, relayed, then through
/// `until_end`, with each run's end hooks.
async fn relayed_and_read_to_its_end(
    reply: impl Fn() -> fixture::Reply,
    call: impl Fn() -> Call,
    end_code: Option<&'static str>,
) -> [(StatusCode, HeaderMap, String, Vec<String>); 2] {
    let mut got = Vec::new();
    for chain in [
        Vec::new(),
        vec![Arc::new(ToEnd(end_code)) as Arc<dyn Plugin>],
    ] {
        let ends = Ends::default();
        let mut plugins: Vec<Arc<dyn Plugin>> = vec![Arc::new(ends.clone())];
        plugins.extend(chain);
        let h = Harness::new(plugins, Some("openai"));
        h.sites.openai.reply(reply());
        let (status, headers, body) = h.send(call()).await;
        let body = steady(&String::from_utf8_lossy(&body.unwrap_or_default()));
        got.push((status, headers, body, ends.all()));
    }
    let [relayed, read]: [_; 2] = got.try_into().unwrap_or_else(|_| unreachable!());
    [relayed, read]
}

/// A sites answer whose head waits `head`, and whose last frame waits `end`
/// more.
struct Paced(SendRequest, Duration, Duration);

impl Paced {
    pub async fn send(
        &self,
        call: http::Request<Body>,
    ) -> Result<(http::Response<Body>, Option<Completion>), Fault> {
        tokio::time::sleep(self.1).await;
        use futures::StreamExt;
        let (resp, completion) = (self.0)(call).await?;
        let (head, body) = resp.into_parts();
        let mut frames: Vec<_> = http_body_util::BodyExt::into_data_stream(body)
            .collect()
            .await;
        let (last, end) = (frames.pop(), self.2);
        let last = futures::stream::once(async move {
            tokio::time::sleep(end).await;
            last
        })
        .filter_map(|last| async move { last });
        let body = futures::stream::iter(frames).chain(last);
        Ok((
            http::Response::from_parts(head, Body::stream(body)),
            completion,
        ))
    }
}
impl From<Paced> for SendRequest {
    fn from(value: Paced) -> Self {
        let value = std::sync::Arc::new(value);
        sender(move |call| {
            let value = value.clone();
            async move { value.send(call).await }
        })
    }
}

#[path = "relay/buffering.rs"]
mod buffering;
#[path = "relay/errors.rs"]
mod errors;
#[path = "relay/filters.rs"]
mod filters;
#[path = "relay/lifecycle.rs"]
mod lifecycle;
#[path = "relay/requests.rs"]
mod requests;
#[path = "relay/responses.rs"]
mod responses;
#[path = "relay/routing.rs"]
mod routing;
#[path = "relay/usage.rs"]
mod usage;
