//! The kernel's promises, driven through the OpenAI format.

use respan_gateway_http::relay as explicit;
#[allow(dead_code)]
#[path = "../../tests/support/hooks.rs"]
mod hooks;
#[allow(dead_code)]
#[path = "../../tests/support/http.rs"]
mod http_support;
mod support;

use futures::TryStreamExt;
use mockllm::{Answer, FakeOpenAi};
use respan_gateway_http::Status;
use respan_gateway_http::bytes;
use respan_gateway_http::{http, http_body, http_body_util};
use respan_gateway_sdk::futures;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use hooks::{Calls, Outcomes};
use http::{HeaderMap, HeaderValue, StatusCode};
use http_support::{post, read_whole, serve};
use respan_gateway_http::Body;
use respan_gateway_http::Wire;
use respan_gateway_http::{Completion, Encoder, HttpClient, Incoming};
use respan_gateway_providers::{AnthropicMessages, OpenAiChat};
use respan_gateway_sdk::async_trait;
use respan_gateway_sdk::conversation::{
    Assembled, Content, Conversation, Delta, Extra, Finish, Message, Reply, Role, set_model, units,
};
use respan_gateway_sdk::{
    BadRequest, ClientContext, Connection, ErrorCode, Fault, Filter, Format, Gateway, Plugin,
    ProviderContext, Response, Upstream,
};
use respan_gateway_sdk::{Details, Usage};
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
fn events_of(body: &str) -> Vec<Reply> {
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
        start.extra = Extra::default();
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
                let (mut head, body) = call.into_parts();
                respan_gateway_http::proxy_call(&mut head);
                respan_gateway_http::strip_client_origin_headers(&mut head.headers);
                let mut call = http::Request::from_parts(head, body);
                respan_gateway_http::credentials(&mut call, &headers);
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
                respan_gateway_http::strip_origin_headers(&mut head.headers);
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
        HttpClient::new(fake.base_url())
            .and_then(|http| http.with_headers(headers))
            .unwrap_or_else(|fault| panic!("fake provider transport: {fault}")),
    )
}

fn gateway(
    list: impl IntoIterator<Item = support::AddProvider>,
    plugins: Vec<Arc<dyn Plugin>>,
    target: Option<&str>,
) -> Result<Gateway, Fault> {
    let gw = with_plugins(
        providers(list)?.client::<Conversation, _, _>(OpenAiChat)?,
        plugins,
    );
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
        cx.filter(|chat: &mut Conversation| {
            chat.temperature = Some(0.0);
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
struct Guard(fn(&Reply) -> bool);

#[async_trait]
impl Plugin for Guard {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let catches = self.0;
        cx.filter(move |e: &mut Reply| {
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
        cx.filter(move |e: &mut Reply| {
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
                    let held = resp.buffer_until(Reply::is_content).await;
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
        cost.add(units::OUTPUT_TOKENS, 1);
        // Shown in its reply; no provider edge, so no meter, sees it.
        let mut answer = vec![Reply::start("cached", "gpt-x")];
        answer.extend(text(0, "hi"));
        answer.push(Reply::usage(cost));
        let answer = futures::stream::iter(answer.into_iter().map(Ok));
        Ok(Some(Response::stream(answer)))
    }
}

#[tokio::test]
async fn a_stream_is_relayed_with_usage() {
    let h = relay();
    h.sites.openai.reply(fixture::sse(STREAM));

    let body = h.body(chat()).await;
    let (said, sent) = (gist(&body), gist(&STREAM.concat()));
    assert_eq!(said.message, sent.message);
    assert_eq!((said.usage, said.start), (sent.usage, sent.start));
    assert!(body.ends_with("data: [DONE]\n\n"), "{body}");
    assert_eq!(says(h.log.only()), Ok(()));
    let usage = h.usage.total();
    assert_eq!(usage.get(units::INPUT_TOKENS), Some(5));
    assert_eq!(usage.get(units::OUTPUT_TOKENS), Some(2));
}

#[tokio::test]
async fn a_unary_response_is_relayed_with_usage() {
    let fake = FakeOpenAi::start().await;
    let h = Harness::real(&fake, Vec::new(), Some("openai"));
    fake.answer(Answer::raw(UNARY));

    let body = h.body(valid_unary_chat()).await;
    let said: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
    assert_eq!(said["id"], "c2");
    assert_eq!(said["choices"][0]["message"]["content"], "hi");
    assert_eq!(said["usage"]["prompt_tokens"], 4);
    assert_eq!(said["usage"]["completion_tokens"], 1);
    assert_eq!(says(h.log.only()), Ok(()));
    assert_eq!(h.usage.total().get(units::INPUT_TOKENS), Some(4));
}

#[tokio::test]
async fn an_error_page_is_relayed_as_it_is() {
    let h = Harness::new(vec![Arc::new(explicit::Relay::HEADS)], Some("openai"));
    let page = "<html>bad gateway</html>";
    h.sites
        .openai
        .reply(fixture::json(StatusCode::BAD_GATEWAY, page));

    let (status, _, body) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(body.ok().as_deref(), Some(page.as_bytes()));
    // The provider answered with the failure, which the end carries.
    let o = h.log.only();
    assert!(
        matches!(o, Err(f) if respan_gateway_http::status(&f) == StatusCode::BAD_GATEWAY && f.get::<respan_gateway_http::ProviderError>().map(|e| &e.response).is_some())
    );
}

#[tokio::test]
async fn the_provider_gets_its_auth_and_the_usage_option() {
    let h = Harness::new(Vec::new(), Some("backup"));
    h.sites.backup.reply(fixture::sse(STREAM));
    let _ = h.send(chat()).await;

    let sent = &h.sites.sent.sent()[0];
    assert_eq!(sent.uri(), "http://backup.test/v1/chat/completions");
    let body: serde_json::Value = serde_json::from_slice(sent.body()).unwrap_or_default();
    assert_eq!(body["stream_options"]["include_usage"], true);

    let h = relay();
    h.sites.openai.reply(fixture::sse(STREAM));
    let _ = h.send(chat()).await;
    let auth = h.sites.sent.sent()[0]
        .headers()
        .get("authorization")
        .cloned();
    assert_eq!(auth, Some(HeaderValue::from_static("Bearer sk-test")));
}

#[tokio::test]
async fn a_plugin_asks_a_model_its_own_question_and_reads_the_answer() {
    /// Refuses what a judge model calls unsafe.
    struct Judge;

    #[async_trait]
    impl Plugin for Judge {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let mut prompt = cx.input::<Conversation>()?.clone();
            let rule = Message::new(Role::System, "Answer SAFE or UNSAFE.");
            prompt.messages.insert(0, rule);
            prompt.model = Some("judge-1".into());
            let call = cx.call("openai").with_input(prompt);
            let verdict = call
                .await?
                .outputs::<Reply>()
                .try_collect::<Vec<_>>()
                .await?;
            let unsafe_ =
                |e: &Reply| matches!(e, Reply::Delta { delta, .. } if delta.as_str() == "UNSAFE");
            match verdict.iter().any(unsafe_) {
                true => Err(Fault::new("the judge refused")
                    .with(ErrorCode("unsafe".into()))
                    .with(Status(StatusCode::FORBIDDEN))),
                false => Ok(None),
            }
        }
    }

    for (verdict, status) in [("SAFE", StatusCode::OK), ("UNSAFE", StatusCode::FORBIDDEN)] {
        let h = Harness::new(vec![Arc::new(Judge)], Some("openai"));
        let judged = json!({
            "id": "j",
            "choices": [{"message": {"content": verdict}}],
            "usage": {"prompt_tokens": 50, "completion_tokens": 50},
        });
        h.sites
            .openai
            .reply(fixture::json(StatusCode::OK, &judged.to_string()))
            .reply(fixture::json(StatusCode::OK, UNARY));
        let (got, _, _) = h.send(unary_chat()).await;
        assert_eq!(got, status, "{verdict}");
        // Each call counts on the provider edge, the judge's too.
        if status == StatusCode::OK {
            let usage = h.usage.total();
            let billed = (
                usage.get(units::INPUT_TOKENS),
                usage.get(units::OUTPUT_TOKENS),
            );
            assert_eq!(billed, (Some(54), Some(51)));
        }
        let sent = h.sites.sent.sent();
        let judge: serde_json::Value = serde_json::from_slice(sent[0].body()).unwrap_or_default();
        assert_eq!(judge["model"], "judge-1");
        assert_eq!(judge["messages"][0]["role"], "system");
        if status == StatusCode::OK {
            // The client's request is untouched by the judge's call.
            let own: serde_json::Value = serde_json::from_slice(sent[1].body()).unwrap_or_default();
            assert_eq!(own, json!({ "model": "gpt-x", "messages": [] }));
        }
    }
}

#[tokio::test]
async fn an_unedited_request_reaches_the_provider_as_it_said() {
    let h = relay();
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));
    let _ = h.send(unary_chat()).await;
    let sent: serde_json::Value =
        serde_json::from_slice(h.sites.sent.sent()[0].body()).unwrap_or_default();
    assert_eq!(sent, json!({ "model": "gpt-x", "messages": [] }));
}

#[tokio::test]
async fn a_request_edit_reaches_the_provider() {
    let h = Harness::new(vec![Arc::new(Cool)], Some("openai"));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));
    let _ = h.send(unary_chat()).await;
    let sent: serde_json::Value =
        serde_json::from_slice(h.sites.sent.sent()[0].body()).unwrap_or_default();
    assert_eq!(
        sent,
        json!({ "model": "gpt-x", "messages": [], "temperature": 0.0 })
    );
}

#[tokio::test]
async fn a_filter_re_encodes_only_what_changed() {
    let h = Harness::new(vec![Arc::new(Shout)], Some("openai"));
    h.sites.openai.reply(fixture::sse(STREAM));

    let said = gist(&h.body(chat()).await);
    assert_eq!(said.message.text(), "HELLO WORLD");
    assert_eq!(said.usage.get(units::OUTPUT_TOKENS), Some(2));
    assert_eq!(said.usage.get(units::INPUT_TOKENS), Some(5));
    assert_eq!(said.start.map(|s| s.id), Some("c1".to_owned()));
    assert_eq!(h.usage.total().get(units::OUTPUT_TOKENS), Some(2));
}

#[tokio::test]
async fn a_filter_may_keep_state_that_is_not_sync() {
    /// Numbers the text outputs of each stream, in a `Cell`.
    struct Count;

    #[async_trait]
    impl Plugin for Count {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let seen = std::cell::Cell::new(0);
            cx.filter(move |e: &mut Reply| {
                if let Some(text) = text_of_mut(e) {
                    seen.set(seen.get() + 1);
                    *text = format!("{}:{text}", seen.get());
                }
                Ok(())
            })?;
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Count)], Some("openai"));
    h.sites.openai.reply(fixture::sse(STREAM));

    let body = h.body(chat()).await;

    assert!(
        body.contains("1:Hello") && body.contains("2: world"),
        "{body}"
    );
}

#[tokio::test]
async fn edits_apply_the_last_added_first() {
    let transcript = Transcript::default();
    let h = Harness::new(
        vec![Arc::new(Shout), Arc::new(transcript.clone())],
        Some("openai"),
    );
    h.sites.openai.reply(fixture::sse(STREAM));

    let body = h.body(chat()).await;

    // The transcript, added after Shout, sees the text before Shout does.
    assert_eq!(transcript.text(), "Hello world");
    assert!(body.contains("HEL"), "{body}");
}

#[tokio::test]
async fn a_refusal_is_an_error_in_the_client_format() {
    let fault = Fault::new("balance too low")
        .with(ErrorCode("insufficient_credits".into()))
        .with(Status(StatusCode::PAYMENT_REQUIRED))
        .with(Details(json!({ "balance": 0.12 })));
    let h = Harness::new(vec![Arc::new(Refuse(fault.clone()))], Some("openai"));

    let (status, headers, body) = h.send(chat()).await;

    assert_eq!(status, StatusCode::PAYMENT_REQUIRED);
    assert_eq!(headers["content-type"], "application/json");
    let err: serde_json::Value = body
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or_default();
    assert_eq!(err["error"]["code"], "insufficient_credits");
    assert_eq!(err["error"]["message"], "balance too low");
    assert_eq!(err["error"]["balance"], 0.12);
    assert!(h.sites.sent.sent().is_empty());
    // The fault carries the status.
    let logged = h.log.only().expect_err("the refusal is logged");
    assert_eq!(logged.message(), fault.message());
    assert_eq!(fault_code(&logged), "insufficient_credits");
    assert_eq!(logged.get::<Status>().map(|s| s.0.as_u16()), Some(402));
    assert_eq!(logged.get::<Details>(), fault.get::<Details>());
}

#[tokio::test]
async fn a_plugin_can_respond_itself() {
    let h = Harness::new(vec![Arc::new(Cached)], None);

    let body: serde_json::Value =
        serde_json::from_str(&h.body(unary_chat()).await).unwrap_or_default();
    assert_eq!(body["id"], "cached");
    assert_eq!(body["choices"][0]["message"]["content"], "hi");
    assert_eq!(body["usage"]["completion_tokens"], 1);
    // A plugin's own answer passes no provider edge, so
    // no meter there sees its usage; no call ran.
    assert_eq!(says(h.log.only()), Ok(()));
    assert!(h.usage.all().is_empty());
}

#[tokio::test]
async fn a_plugin_can_answer_by_calling_providers() {
    let h = Harness::new(vec![Arc::new(Fallback(vec!["openai", "backup"]))], None);
    let overloaded = r#"{"error":{"message":"overloaded"}}"#;
    h.sites
        .openai
        .reply(fixture::json(StatusCode::SERVICE_UNAVAILABLE, overloaded));
    h.sites.backup.reply(fixture::sse(STREAM));

    let said = gist(&h.body(chat()).await);
    assert_eq!(said.message, gist(&STREAM.concat()).message);
    assert_eq!(h.sites.sent.sent().len(), 2);
    assert_eq!(says(h.log.only()), Ok(()));
}

#[tokio::test]
async fn a_cut_stream_ends_with_an_error_event_relayed_or_filtered() {
    for (cut, chain) in [
        (2, Vec::<Arc<dyn Plugin>>::new()),
        (2, vec![Arc::new(Shout) as Arc<dyn Plugin>]),
        // Mid-event: the half event is dropped, nothing was sent, so the
        // response is refused.
        (1, Vec::new()),
        (1, vec![Arc::new(Shout) as Arc<dyn Plugin>]),
    ] {
        let filtered = !chain.is_empty();
        let h = Harness::new(chain, Some("openai"));
        h.sites.openai.reply(fixture::sse(&STREAM[..cut]));

        let (status, _, body) = h.send(chat()).await;
        let body = String::from_utf8_lossy(&body.unwrap_or_default()).into_owned();

        if cut == 1 {
            assert_ne!(status, StatusCode::OK, "cut {cut}, filtered {filtered}");
            assert!(body.contains("\"code\":\"truncated\""), "{body}");
        } else {
            assert!(
                body.ends_with(TRUNCATED),
                "cut {cut}, filtered {filtered}: {body}"
            );
        }
        assert!(!body.contains("[DONE]"));
        assert!(matches!(h.log.only(), Err(f) if fault_code(&f) == "truncated"));
    }
}

#[tokio::test]
async fn a_transport_error_mid_stream_ends_with_an_error_event() {
    let h = relay();
    let mut broken = fixture::sse(STREAM);
    broken.body_mut().truncate(2);
    broken
        .body_mut()
        .push(Err(Upstream::fault("connection reset")));
    h.sites.openai.reply(broken);

    let body = h.body(chat()).await;

    let text: String = sent_chunks(&body)
        .iter()
        .filter_map(|c| {
            c["choices"][0]["delta"]["content"]
                .as_str()
                .map(str::to_owned)
        })
        .collect();
    assert_eq!(text, "Hello world", "{body}");
    assert!(body.ends_with("\"type\":\"upstream\",\"param\":null,\"code\":\"upstream\"}}\n\n"));
    assert!(matches!(h.log.only(), Err(f) if fault_code(&f) == "upstream"));
}

#[tokio::test]
async fn a_unary_body_cut_before_any_bytes_is_an_error_in_the_client_format() {
    let h = relay();
    let mut reset = fixture::json(StatusCode::OK, UNARY);
    *reset.body_mut() = vec![Err(Upstream::fault("connection reset"))];
    h.sites.openai.reply(reset);

    let (status, _, body) = h.send(unary_chat()).await;

    // Nothing was sent, so the head says what happened.
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    let body = String::from_utf8_lossy(&body.unwrap_or_default()).into_owned();
    assert!(body.contains("\"code\":\"upstream\""), "{body}");
    assert!(matches!(h.log.only(), Err(f) if fault_code(&f) == "upstream"));
}

#[tokio::test]
async fn a_broken_unary_success_is_refused_as_the_providers_failure() {
    let h = relay();
    let cut = r#"{"id":"#;
    h.sites.openai.reply(fixture::json(StatusCode::OK, cut));

    // It failed before any content, as `until_content` would find.
    let (status, body) = h.text(unary_chat()).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(body.contains("\"code\":\"upstream\""), "{body}");
    assert!(matches!(h.log.only(), Err(f) if fault_code(&f) == "upstream"));
}

#[tokio::test]
async fn a_client_that_leaves_mid_response_ends_once() {
    let h = relay();
    h.sites.openai.reply(fixture::sse(STREAM));

    let (_, mut body) = serve(&h.gw, chat()).await.0.into_parts();
    let _first = http_body_util::BodyExt::frame(&mut body).await;
    drop(body);
    h.gw.idle().await;

    let o = h.log.only();
    assert!(matches!(o, Err(f) if fault_code(&f) == "abandoned"));
}

#[tokio::test]
async fn a_client_that_leaves_before_a_response_ends_once() {
    let hang = Outcomes::default();
    let h = Harness::new(vec![Arc::new(Hang(hang.clone()))], Some("openai"));

    let gave_up = tokio::time::timeout(Duration::from_millis(10), serve(&h.gw, chat())).await;
    assert!(gave_up.is_err());
    h.gw.idle().await;

    let o = h.log.only();
    assert!(matches!(o, Err(f) if fault_code(&f) == "abandoned"));
    assert_eq!(
        hang.all().len(),
        1,
        "the plugin cut short gets its end hook too"
    );
}

#[tokio::test]
async fn only_plugins_that_ran_get_end_hooks() {
    let after = Outcomes::default();
    let h = Harness::new(
        vec![
            Arc::new(Refuse(BadRequest::fault("no"))),
            Arc::new(after.clone()),
        ],
        Some("openai"),
    );

    let _ = h.send(chat()).await;

    assert_eq!(h.log.all().len(), 1);
    assert!(after.all().is_empty());
}

#[tokio::test]
async fn a_panicking_end_hook_does_not_skip_the_others() {
    let after = Outcomes::default();
    let h = Harness::new(
        vec![Arc::new(PanicAtEnd), Arc::new(after.clone())],
        Some("openai"),
    );
    h.sites.openai.reply(fixture::sse(STREAM));

    let _ = h.send(chat()).await;

    assert_eq!(says(after.only()), Ok(()));
}

#[tokio::test]
async fn requests_no_served_format_accepts_are_404() {
    let h = relay();
    let mut get = call(CHAT, "{}");
    *get.method_mut() = http::Method::GET;
    for req in [call("/nope", "{}"), call("/v1/messages", "{}"), get] {
        let (status, headers, body) = h.send(req).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(headers["content-type"], "text/plain; charset=utf-8");
        let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
        assert!(body.starts_with("no format served accepts "), "{body}");
    }
    assert!(h.log.all().is_empty());
    assert!(h.sites.sent.sent().is_empty());
}

#[tokio::test]
async fn a_gateway_that_cannot_work_fails_as_it_is_built() {
    let sites = Sites::default();
    let openai = || provider(&sites.openai, "openai", None);
    let built =
        |gw: Result<Gateway, Fault>| gw.err().map(|f| (fault_code(&f), f.message().to_owned()));
    let cases = [
        (
            providers([openai(), openai()]),
            "provider 'openai' is defined twice",
        ),
        (
            providers([openai()]).and_then(|g| g.target("missing")),
            "unknown provider 'missing'",
        ),
    ];
    for (gw, expected) in cases {
        let expected = Some(("config".to_owned(), expected.to_owned()));
        assert_eq!(built(gw), expected);
    }
}

#[tokio::test]
async fn a_gateway_with_no_target_and_no_plugins_refuses_404_in_the_clients_format() {
    let sites = Sites::default();
    let gw = gateway([provider(&sites.openai, "openai", None)], Vec::new(), None)
        .unwrap_or_else(|e| panic!("gateway: {e}"));

    let (status, headers, body) = read_whole(serve(&gw, unary_chat()).await.0).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(headers["content-type"], "application/json");
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert!(body.contains("not_found"), "{body}");
    assert!(sites.sent.sent().is_empty());
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
        cx.filter(move |r: &mut Reply| {
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

#[tokio::test]
async fn plugins_run_in_registration_order_on_both_edges() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let named = |name| Named(name, log.clone());
    let sites = Sites::default();
    // Registered d, b, a, c, and they run in that order.
    let gw = providers([provider(&sites.openai, "openai", None)])
        .and_then(|g| g.client::<Conversation, _, _>(OpenAiChat)?.target("openai"))
        .map(|g| {
            let g = g.plugin(named("d")).plugin(named("b"));
            g.plugin(named("a")).plugin(named("c"))
        })
        .unwrap_or_else(|e| panic!("gateway: {e}"));
    sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    let (_, _, body) = read_whole(serve(&gw, unary_chat()).await.0).await;
    gw.idle().await;

    let order = ["d", "b", "a", "c"];
    let hooks = ["client", "provider", "provider_end", "client_end"];
    let expected: Vec<String> = hooks
        .iter()
        .flat_map(|hook| order.iter().map(move |name| format!("{name} {hook}")))
        .collect();
    assert_eq!(log.lock().map(|l| l.clone()).ok(), Some(expected));
    // An earlier plugin's filter wraps a later one's.
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert!(body.contains(r#""content":"d(b(a(c(hi))))""#), "{body}");
}

#[tokio::test]
async fn a_filter_edits_a_unary_response_and_keeps_its_other_fields() {
    let fake = FakeOpenAi::start().await;
    let h = Harness::real(&fake, vec![Arc::new(Shout)], Some("openai"));
    let rich = json!({
        "id": "c2", "object": "chat.completion", "created": 5, "model": "gpt-x",
        "system_fingerprint": "fp", "service_tier": "default",
        "choices": [{ "index": 0, "logprobs": { "content": [] },
            "message": { "role": "assistant", "content": "hi", "refusal": null },
            "finish_reason": "stop" }],
        "usage": { "prompt_tokens": 4, "completion_tokens": 1, "total_tokens": 5 },
    });
    fake.answer(Answer::raw(rich.to_string()));

    let said: serde_json::Value =
        serde_json::from_str(&h.body(valid_unary_chat()).await).unwrap_or_default();

    // Unknown fields of the same format survive, the edit is made, and the
    // rest of what it said is as it was.
    assert_eq!(said["choices"][0]["message"]["content"], "HI");
    for (field, value) in [("system_fingerprint", "fp"), ("service_tier", "default")] {
        assert_eq!(said[field], value, "{said}");
    }
    assert_eq!(said["created"], 5);
    assert_eq!(said["choices"][0]["logprobs"], json!({ "content": [] }));
    assert_eq!(said["choices"][0]["finish_reason"], "stop");
    assert_eq!(said["usage"], rich["usage"]);
}

#[tokio::test]
async fn a_client_that_did_not_ask_for_usage_does_not_get_the_usage_chunk() {
    let h = relay();
    h.sites.openai.reply(fixture::sse(STREAM));
    let request = r#"{"model":"gpt-x","stream":true,"messages":[]}"#;

    let body = h.body(call(CHAT, request)).await;

    assert!(!body.contains("usage"), "{body}");
    assert_eq!(gist(&body).message.text(), "Hello world");
    assert!(body.ends_with("data: [DONE]\n\n"), "{body}");
    assert_eq!(h.usage.total().get(units::OUTPUT_TOKENS), Some(2));
    let sent: serde_json::Value =
        serde_json::from_slice(h.sites.sent.sent()[0].body()).unwrap_or_default();
    assert_eq!(sent["stream_options"]["include_usage"], true);
}

#[tokio::test]
async fn the_null_usage_the_gateway_asked_for_does_not_reach_the_client() {
    let h = relay();
    let chunk = |usage: &str| {
        format!(
            "data: {{\"id\":\"c1\",\"choices\":[{{\"index\":0,\"delta\":{{\"content\":\"hi\"}}}}]{usage}}}\n\n"
        )
    };
    let stream = [chunk(",\"usage\":null"), "data: [DONE]\n\n".into()];
    let stream: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.sites.openai.reply(fixture::sse(&stream));
    let request = r#"{"model":"gpt-x","stream":true,"messages":[]}"#;

    let body = h.body(call(CHAT, request)).await;

    assert!(!body.contains("usage"), "{body}");
    assert_eq!(gist(&body).message.text(), "hi");
    assert!(body.ends_with("data: [DONE]\n\n"), "{body}");
}

#[tokio::test]
async fn a_chunk_that_says_more_than_null_usage_is_kept() {
    let h = relay();
    let filter = |usage: &str| {
        format!(
            "data: {{\"choices\":[],\"id\":\"\",\"prompt_filter_results\":[{{\"prompt_index\":0}}]{usage}}}\n\n"
        )
    };
    let stream = [
        filter(",\"usage\":null"),
        "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\n"
            .into(),
        "data: [DONE]\n\n".into(),
    ];
    let stream: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.sites.openai.reply(fixture::sse(&stream));
    let request = r#"{"model":"gpt-x","stream":true,"messages":[]}"#;

    let body = h.body(call(CHAT, request)).await;

    // What the chunk said beside its null usage is kept, in the chunks the
    // response is written in.
    assert!(!body.contains("usage"), "{body}");
    let first = &sent_chunks(&body)[0];
    assert_eq!(
        first["prompt_filter_results"],
        json!([{ "prompt_index": 0 }])
    );
    assert_eq!(gist(&body).message.text(), "hi");
}

#[tokio::test]
async fn caller_values_reach_plugins_by_type() {
    #[derive(Clone)]
    struct Tenant(&'static str);

    struct NeedsTenant;

    #[async_trait]
    impl Plugin for NeedsTenant {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            match cx.extensions().get::<Tenant>() {
                Some(Tenant("acme")) => Ok(None),
                _ => Err(Fault::new("unknown tenant")
                    .with(ErrorCode("forbidden".into()))
                    .with(Status(StatusCode::FORBIDDEN))),
            }
        }
    }

    let h = Harness::new(vec![Arc::new(NeedsTenant)], Some("openai"));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    let (anonymous, _, _) = h.send(unary_chat()).await;
    let mut req = post(CHAT, UNARY_REQ);
    // The kernel's extensions travel in the HTTP request
    // as one value of their own.
    let mut values = respan_gateway_sdk::Extensions::new();
    values.insert(Tenant("acme"));
    req.extensions_mut().insert(values);
    let (acme, _, _) = h.send(req).await;

    assert_eq!((anonymous, acme), (StatusCode::FORBIDDEN, StatusCode::OK));
}

#[tokio::test]
async fn a_panic_in_a_filter_is_refused_as_internal() {
    struct Explode;

    #[async_trait]
    impl Plugin for Explode {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(|_: &mut Reply| panic!("a buggy filter"))?;
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Explode)], Some("openai"));
    h.sites.openai.reply(fixture::sse(STREAM));

    let (status, _, body) = h.send(chat()).await;

    // Nothing was sent before the panic, so the client gets an error in its
    // format, not a dropped connection.
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let body = body.map(|b| String::from_utf8_lossy(&b).into_owned());
    assert!(body.is_ok_and(|b| b.contains("internal")));
    assert!(matches!(h.log.only(), Err(f) if fault_code(&f) == "internal"));
}

#[tokio::test]
async fn a_client_body_that_breaks_off_is_refused_in_the_clients_format() {
    let h = relay();
    let broken = futures::stream::iter([Err::<http_body::Frame<bytes::Bytes>, _>(
        Fault::new("request body too large")
            .with(ErrorCode("too_large".into()))
            .with(Status(StatusCode::PAYLOAD_TOO_LARGE)),
    )]);
    let body = Body::stream(http_body_util::BodyExt::into_data_stream(
        http_body_util::StreamBody::new(broken),
    ));

    let mut req = http::Request::new(body);
    *req.method_mut() = http::Method::POST;
    *req.uri_mut() = http::Uri::from_static(CHAT);
    let (status, headers, body) = h.send(req).await;

    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(headers["content-type"], "application/json");
    let body = body.map(|b| String::from_utf8_lossy(&b).into_owned());
    assert!(body.is_ok_and(|b| b.contains("\"code\":\"too_large\"")));
    assert!(h.sites.sent.sent().is_empty());
}

#[tokio::test]
async fn a_plugin_that_panics_is_refused_as_internal() {
    struct Explode;

    #[async_trait]
    impl Plugin for Explode {
        async fn on_client(&self, _cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            panic!("a buggy plugin")
        }
    }

    let h = Harness::new(vec![Arc::new(Explode)], Some("openai"));

    let (status, _, body) = h.send(chat()).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let body = body.map(|b| String::from_utf8_lossy(&b).into_owned());
    assert!(body.is_ok_and(|b| b.contains("internal")));
    assert!(matches!(h.log.only(), Err(f) if fault_code(&f) == "internal"));
}

#[tokio::test]
async fn a_refusal_its_format_cannot_write_is_a_plain_500() {
    // OpenAI has no error under a success, so the refusal cannot be written.
    let refuse = Refuse(
        Fault::new("not an error")
            .with(ErrorCode("odd".into()))
            .with(Status(StatusCode::OK)),
    );
    let h = Harness::new(vec![Arc::new(refuse)], Some("openai"));

    let (status, headers, body) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(headers["content-type"], "text/plain; charset=utf-8");
    assert_eq!(body.ok().as_deref(), Some(&b"not an error"[..]));
    // The adapter wrote the plain 500.
    let o = h.log.only();
    assert!(matches!(o, Err(f) if fault_code(&f) == "odd"));
}

#[tokio::test]
async fn an_edit_in_the_same_format_keeps_what_a_conversation_cannot_hold() {
    let h = Harness::new(vec![Arc::new(Cool)], Some("openai"));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));
    let request = json!({
        "model": "gpt-x",
        "messages": [
            { "role": "user", "content": "hi" },
            { "role": "developer", "content": "answer in French" },
            { "role": "user", "content": [{ "type": "image_url", "image_url": { "url": "u" } }] },
        ],
        "tools": [{ "type": "function", "function": { "name": "f" } }],
        "seed": 7,
    });

    let (status, _, _) = h.send(call(CHAT, request.to_string())).await;

    assert_eq!(status, StatusCode::OK);
    let sent: serde_json::Value =
        serde_json::from_slice(h.sites.sent.sent()[0].body()).unwrap_or_default();
    // Even the developer role is as the client spelled it.
    let mut expected = request;
    expected["temperature"] = json!(0.0);
    assert_eq!(sent, expected);
}

#[tokio::test]
async fn an_edited_streaming_request_keeps_the_clients_usage_chunk() {
    let h = Harness::new(vec![Arc::new(Cool)], Some("openai"));
    h.sites.openai.reply(fixture::sse(STREAM));

    let said = gist(&h.body(chat()).await);
    assert_eq!(said.usage, gist(&STREAM.concat()).usage);
    assert_eq!(said.usage.get(units::OUTPUT_TOKENS), Some(2));
}

#[tokio::test]
async fn an_edit_to_the_start_is_written_in_the_clients_format() {
    struct Rename;

    #[async_trait]
    impl Plugin for Rename {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(|e: &mut Reply| {
                if let Reply::Start(start) = e {
                    start.id = "hidden".into();
                }
                Ok(())
            })?;
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Rename)], Some("openai"));
    // As OpenAI streams begin: a chunk with the role and no text.
    let opening = "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"}}]}\n\n";
    let stream = [opening, STREAM[0], STREAM[1], STREAM[2], STREAM[3]];
    h.sites.openai.reply(fixture::sse(&stream));

    let body = h.body(chat()).await;

    assert!(body.starts_with("data: {\"id\":\"hidden\""), "{body}");
    assert!(body.contains("\"content\":\"Hello\""), "{body}");
    // In every chunk, not just the first.
    assert!(!body.contains("\"c1\""), "{body}");
}

#[tokio::test]
async fn a_filter_that_changes_nothing_keeps_what_was_said() {
    struct Idle;

    #[async_trait]
    impl Plugin for Idle {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(|_: &mut Reply| Ok(()))?;
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Idle)], Some("openai"));
    let spaced = "data: {\"id\": \"c1\", \"choices\": [{\"index\": 0, \"delta\": {\"content\": \"caf\\u00e9\"}}]}\n\n";
    h.sites
        .openai
        .reply(fixture::sse(&[spaced, "data: [DONE]\n\n"]));

    assert_eq!(gist(&h.body(chat()).await).message.text(), "café");
}

#[tokio::test]
async fn a_stream_guardrail_refuses_before_any_bytes_and_fails_the_stream_after() {
    let h = Harness::new(vec![Arc::new(Guard(|_| true))], Some("openai"));
    h.sites.openai.reply(fixture::sse(STREAM));
    let (status, _, _) = h.send(chat()).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    // Its fault has the status.
    let end = h.log.only();
    assert!(matches!(end, Err(f) if respan_gateway_http::status(&f) == StatusCode::FORBIDDEN));

    let world =
        |e: &Reply| matches!(e, Reply::Delta { delta, .. } if delta.as_str().contains("world"));
    let h = Harness::new(vec![Arc::new(Guard(world))], Some("openai"));
    h.sites.openai.reply(fixture::sse(STREAM));

    let body = h.body(chat()).await;

    assert!(body.contains("Hello"), "{body}");
    assert!(!body.contains("world"), "{body}");
    assert!(
        body.ends_with("\"type\":\"blocked\",\"param\":null,\"code\":\"blocked\"}}\n\n"),
        "{body}"
    );
    assert!(matches!(h.log.only(), Err(f) if fault_code(&f) == "blocked"));
}

#[tokio::test]
async fn plugins_see_the_request_and_change_its_model() {
    #[derive(Clone)]
    struct Seen(&'static str);

    /// Changes the model and records what `on_client_end` finds in the
    /// extensions.
    #[derive(Clone, Default)]
    struct Redirect(Arc<Mutex<Option<&'static str>>>);

    #[async_trait]
    impl Plugin for Redirect {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let asked = cx
                .extensions()
                .get::<Wire>()
                .and_then(Wire::get)
                .map(|a| a.method().clone());
            assert_eq!(asked, Some(http::Method::POST));
            // The client's request, as it said it.
            let chat = cx.input::<Conversation>()?;
            assert!(chat.stream);
            let said = chat.messages[0].text();
            cx.extensions_mut()
                .insert(Seen(if said == "hi" { "hi" } else { "other" }));
            cx.filter(set_model("gpt-small"))?;
            Ok(None)
        }

        async fn on_client_end(&self, cx: &ClientContext, _end: &Result<(), Fault>) {
            if let Ok(mut seen) = self.0.lock() {
                *seen = cx.extensions().get::<Seen>().map(|s| s.0);
            }
        }
    }

    let redirect = Redirect::default();
    let h = Harness::new(vec![Arc::new(redirect.clone())], Some("openai"));
    h.sites.openai.reply(fixture::sse(STREAM));
    let _ = h.send(chat()).await;

    let sent: serde_json::Value =
        serde_json::from_slice(h.sites.sent.sent()[0].body()).unwrap_or_default();
    assert_eq!(sent["model"], "gpt-small");
    assert_eq!(says(h.log.only()), Ok(()));
    assert_eq!(redirect.0.lock().map(|s| *s).ok().flatten(), Some("hi"));
}

#[tokio::test]
async fn a_filter_edits_several_events_of_one_chunk_in_its_shape() {
    struct Truncate;

    #[async_trait]
    impl Plugin for Truncate {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(move |e: &mut Reply| {
                if let Some(text) = text_of_mut(e) {
                    *text = text.to_uppercase();
                }
                if let Reply::Finish(finish) = e {
                    *finish = Finish::Length;
                }
                Ok(())
            })?;
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Truncate)], Some("openai"));
    let chunk = "data: {\"id\":\"x\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":\"stop\"}]}\n\n";
    h.sites
        .openai
        .reply(fixture::sse(&[chunk, "data: [DONE]\n\n"]));

    let body = h.body(chat()).await;
    let said = gist(&body);
    assert_eq!(said.message.text(), "HI");
    assert_eq!(said.finish, Some(Finish::Length));
    assert!(body.ends_with("data: [DONE]\n\n"), "{body}");
}

#[tokio::test]
async fn whole_untyped_bytes_answer_a_streaming_client_as_a_stream() {
    let h = relay();
    let untyped = http::Response::new(vec![Ok(bytes::Bytes::from(UNARY))]);
    h.sites.openai.reply(untyped);

    // Whole untyped bytes are rewritten as the stream the client asked for.
    let body = h.body(chat()).await;
    assert!(body.starts_with("data: {"), "{body}");
    assert!(body.contains("\"content\":\"hi\""), "{body}");
    assert!(body.ends_with("data: [DONE]\n\n"), "{body}");
    assert_eq!(says(h.log.only()), Ok(()));
}

#[tokio::test]
async fn a_unary_guardrail_refuses_with_its_status() {
    let text = |e: &Reply| matches!(e, Reply::Delta { .. });
    let h = Harness::new(vec![Arc::new(Guard(text))], Some("openai"));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    let (status, _, body) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    let body = String::from_utf8_lossy(&body.unwrap_or_default()).into_owned();
    assert!(body.contains("\"code\":\"blocked\""), "{body}");
    // Its fault has the status.
    let end = h.log.only();
    assert!(matches!(end, Err(f) if respan_gateway_http::status(&f) == StatusCode::FORBIDDEN));
}

#[tokio::test]
async fn a_call_kept_past_its_request_cannot_call() {
    /// Keeps a call, and awaits it once the request ended.
    #[derive(Clone, Default)]
    struct Late(
        Arc<Mutex<Option<respan_gateway_sdk::Call>>>,
        Arc<Mutex<Option<String>>>,
    );

    #[async_trait]
    impl Plugin for Late {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let call = cx.call("openai");
            self.0.lock().map(|mut kept| *kept = Some(call)).ok();
            Ok(None)
        }

        async fn on_client_end(&self, _cx: &ClientContext, _end: &Result<(), Fault>) {
            let Some(call) = self.0.lock().ok().and_then(|mut kept| kept.take()) else {
                return;
            };
            let err = call.await.err().map(|f| f.message().to_owned());
            self.1.lock().map(|mut seen| *seen = err).ok();
        }
    }

    let late = Late::default();
    let h = Harness::new(vec![Arc::new(late.clone())], Some("openai"));
    h.sites.openai.reply(fixture::sse(STREAM));
    let _ = h.send(chat()).await;
    h.gw.idle().await;

    let err = late.1.lock().map(|e| e.clone()).ok().flatten();
    assert_eq!(
        err.as_deref(),
        Some("providers can be called until the request ends")
    );
    assert_eq!(h.sites.sent.sent().len(), 1);
}

#[tokio::test]
async fn a_client_that_leaves_before_the_head_ends_abandoned() {
    /// A provider that answers 200 and never sends a byte.
    struct Silent;

    impl Silent {
        pub async fn send(
            &self,
            _req: http::Request<Body>,
        ) -> Result<(http::Response<Body>, Option<Completion>), Fault> {
            let never = http_body_util::StreamBody::new(futures::stream::pending());
            let mut resp = http::Response::new(Body::stream(
                http_body_util::BodyExt::into_data_stream(never),
            ));
            resp.headers_mut().insert(
                "content-type",
                HeaderValue::from_static("text/event-stream"),
            );
            Ok((resp, None))
        }
    }
    impl From<Silent> for SendRequest {
        fn from(value: Silent) -> Self {
            let value = std::sync::Arc::new(value);
            sender(move |call| {
                let value = value.clone();
                async move { value.send(call).await }
            })
        }
    }

    let log = Outcomes::default();
    let gw = gateway(
        [add_provider("openai", OpenAiChat, Silent)],
        vec![Arc::new(log.clone())],
        Some("openai"),
    )
    .unwrap_or_else(|e| panic!("{e}"));

    let gave_up = tokio::time::timeout(Duration::from_millis(10), serve(&gw, chat())).await;
    assert!(gave_up.is_err());
    gw.idle().await;

    assert!(matches!(log.only(), Err(f) if fault_code(&f) == "abandoned"));
}

#[test]
#[should_panic(expected = "Tokio 1.x runtime")]
fn handle_needs_a_tokio_runtime() {
    let sites = Sites::default();
    let gw = gateway(
        [provider(&sites.openai, "openai", None)],
        vec![],
        Some("openai"),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    futures::executor::block_on(async {
        let _ = serve(&gw, unary_chat()).await;
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn end_hooks_run_on_the_requests_runtime_wherever_its_response_is_read() {
    /// Notes whether its end hook ran on a tokio runtime.
    #[derive(Clone, Default)]
    struct OnRuntime(Arc<Mutex<Option<bool>>>);

    #[async_trait]
    impl Plugin for OnRuntime {
        async fn on_client_end(&self, _cx: &ClientContext, _end: &Result<(), Fault>) {
            let on = tokio::runtime::Handle::try_current().is_ok();
            self.0.lock().map(|mut s| *s = Some(on)).ok();
        }
    }

    let sites = Sites::default();
    let noted = OnRuntime::default();
    let gw = gateway(
        [provider(&sites.openai, "openai", None)],
        vec![Arc::new(noted.clone())],
        Some("openai"),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    let resp = serve(&gw, unary_chat()).await.0;
    // Read to its end on a thread with no runtime, which starts the hooks.
    let read = std::thread::spawn(move || futures::executor::block_on(read_whole(resp)));
    let _ = read.join();
    gw.idle().await;

    assert_eq!(noted.0.lock().map(|s| *s).ok().flatten(), Some(true));
}

#[tokio::test]
async fn a_router_can_set_the_target_and_let_later_plugins_run() {
    struct Pick;

    #[async_trait]
    impl Plugin for Pick {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            assert_eq!(cx.target(), Some("openai"));
            cx.set_target("backup");
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Pick), Arc::new(Cool)], Some("openai"));
    h.sites.backup.reply(fixture::json(StatusCode::OK, UNARY));
    let _ = h.send(unary_chat()).await;

    let sent = &h.sites.sent.sent()[0];
    assert!(sent.uri().to_string().starts_with(BACKUP));
    let body: serde_json::Value = serde_json::from_slice(sent.body()).unwrap_or_default();
    assert_eq!(body["temperature"], 0.0, "the plugin after the router ran");
}

#[tokio::test]
async fn a_filter_restores_text_across_an_openai_stream_and_releases_the_rest() {
    let chunk = |delta: serde_json::Value, finish: serde_json::Value| {
        let chunk = json!({"id": "c1", "object": "chat.completion.chunk", "model": "gpt-x",
            "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]});
        format!("data: {chunk}\n\n")
    };
    let stream = [
        chunk(json!({"role": "assistant", "content": ""}), json!(null)),
        chunk(json!({"content": "Hi <P"}), json!(null)),
        chunk(json!({"content": "1>, bye <"}), json!(null)),
        chunk(json!({}), json!("stop")),
        "data: [DONE]\n\n".to_owned(),
    ];
    let h = Harness::new(vec![Arc::new(Unmask)], Some("openai"));
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.sites.openai.reply(fixture::sse(&chunks));

    let (status, _, body) = h.send(chat()).await;

    assert_eq!(status, StatusCode::OK, "{body:?}");
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    let chunks: Vec<serde_json::Value> = body
        .split("\n\n")
        .filter_map(|e| e.strip_prefix("data: "))
        .filter_map(|d| serde_json::from_str(d).ok())
        .collect();
    let text: String = chunks
        .iter()
        .filter_map(|c| {
            c.pointer("/choices/0/delta/content")
                .and_then(|t| t.as_str())
        })
        .collect();
    assert_eq!(text, "Hi Alice, bye <");
    // Text kept back goes out before the provider's finish, in the same stream.
    let last = &chunks[chunks.len() - 1];
    assert_eq!(
        last.pointer("/choices/0/finish_reason"),
        Some(&json!("stop"))
    );
    assert!(chunks.iter().all(|c| c["id"] == "c1"));
    assert!(body.ends_with("data: [DONE]\n\n"));
}

#[tokio::test]
async fn a_filter_releases_what_it_held_into_a_whole_openai_message() {
    let reply = json!({"id": "c2", "choices": [{"message": {"content": "Hi <P1>, bye <"}}]});
    let h = Harness::new(vec![Arc::new(Unmask)], Some("openai"));
    h.sites
        .openai
        .reply(fixture::json(StatusCode::OK, &reply.to_string()));

    let (status, _, body) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&body.unwrap_or_default()).unwrap_or_default();
    assert_eq!(
        body.pointer("/choices/0/message/content"),
        Some(&json!("Hi Alice, bye <"))
    );
}

#[tokio::test]
async fn a_whole_message_a_filter_held_keeps_its_fields_and_usage() {
    struct Hold;

    #[async_trait]
    impl Plugin for Hold {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(HoldAll(Vec::new()))?;
            Ok(None)
        }
    }

    let tool_call = json!({"id": "c2", "model": "gpt-x", "system_fingerprint": "fp",
        "choices": [{"message": {"content": "hi", "tool_calls": [{"id": "t", "type": "function",
            "function": {"name": "f", "arguments": "{}"}}]}, "finish_reason": "tool_calls"}],
        "usage": {"prompt_tokens": 4, "completion_tokens": 1}});
    let h = Harness::new(vec![Arc::new(Hold)], Some("openai"));
    h.sites
        .openai
        .reply(fixture::json(StatusCode::OK, &tool_call.to_string()));

    let (status, _, body) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&body.unwrap_or_default()).unwrap_or_default();
    assert_eq!(body["id"], "c2");
    assert_eq!(body["model"], "gpt-x");
    assert_eq!(body["system_fingerprint"], "fp");
    let (said, sent) = (&body["choices"][0], &tool_call["choices"][0]);
    assert_eq!(said["message"]["content"], "hi");
    assert_eq!(said["message"]["tool_calls"], sent["message"]["tool_calls"]);
    assert_eq!(said["finish_reason"], "tool_calls");
    assert_eq!(body["usage"]["prompt_tokens"], 4);
    assert_eq!(body["usage"]["completion_tokens"], 1);
}

#[tokio::test]
async fn what_the_provider_sent_after_its_finish_is_kept_when_it_breaks_off() {
    let stream = [
        "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello\"}}]}\n\n",
        "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"},\"finish_reason\":\"stop\"}]}\n\n",
    ];
    let h = relay();
    h.sites.openai.reply(fixture::sse(&stream));

    let (_, _, body) = h.send(chat()).await;

    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    // What came is kept, the finish too, and the failure ends the stream.
    let said = sent_chunks(&body);
    let text: String = said
        .iter()
        .filter_map(|c| c["choices"][0]["delta"]["content"].as_str())
        .collect();
    assert_eq!(text, "Hello world", "{body}");
    assert!(
        said.iter()
            .any(|c| c["choices"][0]["finish_reason"] == "stop")
    );
    assert!(body.ends_with(TRUNCATED), "{body}");
}

#[tokio::test]
async fn a_filter_fills_a_cache_with_the_answer_and_its_usage() {
    type Entry = Arc<Mutex<Option<(String, u64)>>>;

    /// Keeps a finished answer and what it cost, to replay later: the
    /// filter sees every reply, the usage alone at the stream's end too.
    struct Fill(String, u64, Entry);

    impl Filter<Reply> for Fill {
        fn edit(&mut self, event: Reply, out: &mut Vec<Reply>) -> Result<(), Fault> {
            if let Some(text) = text_of(&event) {
                self.0.push_str(text);
            }
            if let Reply::Usage(usage) = &event {
                self.1 += usage.get(units::OUTPUT_TOKENS).unwrap_or_default();
            }
            out.push(event);
            Ok(())
        }

        fn end(&mut self, end: &Result<(), Fault>, _out: &mut Vec<Reply>) -> Result<(), Fault> {
            if let (Ok(()), Ok(mut entry)) = (end, self.2.lock()) {
                *entry = Some((std::mem::take(&mut self.0), self.1));
            }
            Ok(())
        }
    }

    struct Cache(Entry);

    #[async_trait]
    impl Plugin for Cache {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(Fill(String::new(), 0, self.0.clone()))?;
            Ok(None)
        }
    }

    let entry = Entry::default();
    let h = Harness::new(vec![Arc::new(Cache(entry.clone()))], Some("openai"));
    h.sites.openai.reply(fixture::sse(STREAM));

    let _ = h.send(chat()).await;

    let kept = entry.lock().ok().and_then(|e| e.clone());
    assert_eq!(kept, Some(("Hello world".to_owned(), 2)));
}

#[tokio::test]
async fn a_delta_a_filter_splits_in_two_is_written_as_two_and_reads_back_whole() {
    /// Splits each text after its first character.
    #[derive(Default)]
    struct Split;

    impl Filter<Reply> for Split {
        fn edit(&mut self, reply: Reply, out: &mut Vec<Reply>) -> Result<(), Fault> {
            match (text_of(&reply), &reply) {
                (Some(text), Reply::Delta { block, .. }) if text.len() > 1 => {
                    let (a, b) = text.split_at(1);
                    out.extend([Reply::text(*block, a), Reply::text(*block, b)]);
                }
                _ => out.push(reply),
            }
            Ok(())
        }
    }

    struct Splitter;

    #[async_trait]
    impl Plugin for Splitter {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(Split)?;
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Splitter)], Some("openai"));
    h.sites.openai.reply(fixture::sse(STREAM));

    let (status, _, body) = h.send(chat()).await;

    assert_eq!(status, StatusCode::OK);
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert!(body.contains("\"content\":\"H\""), "{body}");
    assert!(body.contains("\"content\":\"ello\""), "{body}");
    assert_eq!(gist(&body).message.text(), "Hello world");
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

#[tokio::test]
async fn usage_in_the_finish_chunk_survives_a_held_finish() {
    let stream = [
        chunk(json!({"id": "c1", "choices": [{"index": 0, "delta": {"content": "Hi"}}]})),
        chunk(
            json!({"id": "c1", "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 5, "completion_tokens": 2}}),
        ),
        "data: [DONE]\n\n".to_owned(),
    ];

    let (body, chunks) = footed(&stream).await;

    assert_eq!(
        said(&chunks),
        ("Hi [footer]".to_owned(), Some("stop"), Some(2)),
        "{body}"
    );
}

#[tokio::test]
async fn the_usage_chunk_stays_last_after_a_held_finish() {
    let stream = [
        chunk(json!({"id": "c1", "choices": [{"index": 0, "delta": {"content": "Hi"}}]})),
        chunk(json!({"id": "c1", "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]})),
        chunk(json!({"id": "c1", "choices": [],
            "usage": {"prompt_tokens": 5, "completion_tokens": 2}})),
        "data: [DONE]\n\n".to_owned(),
    ];

    let (body, chunks) = footed(&stream).await;

    assert_eq!(
        said(&chunks),
        ("Hi [footer]".to_owned(), Some("stop"), Some(2)),
        "{body}"
    );
    assert!(
        chunks
            .last()
            .is_some_and(|c| c["usage"]["completion_tokens"] == 2),
        "{body}"
    );
    assert!(body.ends_with("data: [DONE]\n\n"));
}

fn filtered<F: Filter<Reply> + Clone + Sync>(filter: F) -> Harness {
    Harness::new(vec![Arc::new(Adds(filter))], Some("openai"))
}

#[tokio::test]
async fn a_stream_that_opens_with_no_choices_is_not_held() {
    let stream = [
        chunk(json!({"id": "", "choices": [], "prompt_filter_results": []})),
        chunk(json!({"id": "c1", "choices": [{"index": 0, "delta": {"content": "Hi"}}]})),
        chunk(json!({"id": "c1", "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]})),
        "data: [DONE]\n\n".to_owned(),
    ];

    let (body, chunks) = footed(&stream).await;

    assert_eq!(
        said(&chunks),
        ("Hi [footer]".to_owned(), Some("stop"), None),
        "{body}"
    );
}

#[tokio::test]
async fn a_filter_holding_everything_of_an_own_stream_releases_it_whole() {
    let h = filtered(HoldAll(Vec::new()));
    h.sites.openai.reply(fixture::sse(STREAM));

    let (status, _, body) = h.send(chat()).await;

    assert_eq!(status, StatusCode::OK);
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    let chunks: Vec<serde_json::Value> = body
        .split("\n\n")
        .filter_map(|e| e.strip_prefix("data: "))
        .filter_map(|d| serde_json::from_str(d).ok())
        .collect();
    assert_eq!(said(&chunks).0, "Hello world", "{body}");
    assert!(body.ends_with("data: [DONE]\n\n"));
}

#[tokio::test]
async fn text_before_the_start_of_a_whole_message_is_written() {
    let h = filtered(Disclaimer);
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    let (status, _, body) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&body.unwrap_or_default()).unwrap_or_default();
    assert_eq!(
        body.pointer("/choices/0/message/content"),
        Some(&json!("NOTE: hi"))
    );
}

#[tokio::test]
async fn usage_on_every_chunk_does_not_hold_the_stream() {
    let usage = json!({"prompt_tokens": 5, "completion_tokens": 1});
    let stream = [
        chunk(
            json!({"id": "c1", "choices": [{"index": 0, "delta": {"content": "Hi"}}], "usage": usage}),
        ),
        chunk(
            json!({"id": "c1", "choices": [{"index": 0, "delta": {"content": " there"}}], "usage": usage}),
        ),
        chunk(
            json!({"id": "c1", "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}], "usage": usage}),
        ),
        "data: [DONE]\n\n".to_owned(),
    ];

    let (body, chunks) = footed(&stream).await;

    assert_eq!(
        said(&chunks),
        ("Hi there [footer]".to_owned(), Some("stop"), Some(1)),
        "{body}"
    );
}

#[tokio::test]
async fn an_edit_to_the_start_leaves_other_choices_alone() {
    let h = filtered(|e: &mut Reply| {
        if let Reply::Start(start) = e {
            start.model = "alias".into();
        }
        Ok(())
    });
    let stream = [
        chunk(
            json!({"id": "c1", "model": "gpt-x", "choices": [{"index": 0, "delta": {"content": "A"}}]}),
        ),
        chunk(
            json!({"id": "c1", "model": "gpt-x", "choices": [{"index": 1, "delta": {"content": "B"}}]}),
        ),
        "data: [DONE]\n\n".to_owned(),
    ];
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.sites.openai.reply(fixture::sse(&chunks));

    let (status, _, body) = h.send(chat()).await;

    assert_eq!(status, StatusCode::OK);
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert!(
        body.contains(r#""index":1,"delta":{"content":"B"}"#),
        "{body}"
    );
    assert!(!body.contains("gpt-x"), "{body}");
}

#[tokio::test]
async fn an_embedder_reads_how_its_request_ended() {
    let h = relay();
    h.sites.openai.reply(fixture::sse(STREAM));
    let req = chat();

    let (resp, ended) = serve(&h.gw, req).await;
    let _ = read_whole(resp).await;

    assert_eq!(says(ended.await), Ok(()));
}

#[tokio::test]
async fn a_whole_message_a_filter_took_the_start_of_is_sent() {
    let h = filtered(Nameless);
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    let (status, _, body) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::OK);
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert_eq!(gist(&body).message.text(), "hi");
    assert_eq!(gist(&body).usage.get(units::INPUT_TOKENS), Some(4));
}

#[tokio::test]
async fn a_tool_call_held_in_an_own_stream_is_released() {
    let h = filtered(HoldAll(Vec::new()));
    // As OpenAI streams a tool call: its role, no content, and the call.
    let call = json!([{"index": 0, "delta": {"role": "assistant", "content": null,
        "tool_calls": [{"index": 0, "id": "t", "type": "function",
        "function": {"name": "f", "arguments": "{}"}}]}}]);
    let stream = [
        chunk(json!({"id": "c1", "choices": call})),
        chunk(
            json!({"id": "c1", "choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls"}]}),
        ),
        "data: [DONE]\n\n".to_owned(),
    ];
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.sites.openai.reply(fixture::sse(&chunks));

    let (status, _, body) = h.send(chat()).await;

    assert_eq!(status, StatusCode::OK);
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert!(body.contains("\"tool_calls\""), "{body}");
    assert!(body.contains("\"finish_reason\":\"tool_calls\""), "{body}");
    assert!(body.ends_with("data: [DONE]\n\n"));
}

#[tokio::test]
async fn every_response_says_how_its_request_ended() {
    let h = relay();

    let (resp, ended) = serve(&h.gw, post("/nowhere", UNARY_REQ)).await;

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    // Its end is that of the plain refusal.
    let end = ended.await.map_err(|f| fault_code(&f));
    assert_eq!(end, Err("not_found".to_owned()));
}

#[tokio::test]
async fn a_plugin_races_two_calls_and_keeps_the_first() {
    struct Hedge;

    #[async_trait]
    impl Plugin for Hedge {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let (a, b) = (cx.call("openai"), cx.call("backup"));
            let first = match futures::future::select(a, b).await {
                futures::future::Either::Left((won, _))
                | futures::future::Either::Right((won, _)) => won,
            };
            first.map(Some)
        }
    }

    let h = Harness::new(vec![Arc::new(Hedge)], None);
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));
    h.sites.backup.reply(fixture::json(StatusCode::OK, UNARY));

    let (status, _, body) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::OK);
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert_eq!(gist(&body).message.text(), "hi");
}

fn tool_call_message(content: serde_json::Value) -> String {
    json!({"id": "c2", "choices": [{"index": 0, "finish_reason": "tool_calls",
        "message": {"role": "assistant", "content": content,
            "tool_calls": [{"id": "t", "type": "function", "function": {"name": "f", "arguments": "{}"}}]}}]})
    .to_string()
}

#[tokio::test]
async fn text_taken_beside_a_tool_call_leaves_the_call() {
    let h = filtered(Silence::default());
    h.sites.openai.reply(fixture::json(
        StatusCode::OK,
        &tool_call_message(json!("Let me check")),
    ));

    let (status, _, body) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&body.unwrap_or_default()).unwrap_or_default();
    assert_eq!(
        body.pointer("/choices/0/message/content"),
        Some(&json!(null))
    );
    assert_eq!(
        body.pointer("/choices/0/message/tool_calls/0/id"),
        Some(&json!("t"))
    );
}

#[tokio::test]
async fn a_footer_joins_a_message_of_only_a_tool_call() {
    let h = filtered(Disclaimer);
    h.sites.openai.reply(fixture::json(
        StatusCode::OK,
        &tool_call_message(json!(null)),
    ));

    let (status, _, body) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&body.unwrap_or_default()).unwrap_or_default();
    assert_eq!(
        body.pointer("/choices/0/message/content"),
        Some(&json!("NOTE: "))
    );
    assert_eq!(
        body.pointer("/choices/0/message/tool_calls/0/id"),
        Some(&json!("t"))
    );
}

#[tokio::test]
async fn an_unreadable_event_a_filter_held_goes_out_as_it_came() {
    let h = filtered(HoldAll(Vec::new()));
    let stream = [
        chunk(json!({"id": "c1", "choices": [{"index": 0, "delta": {"content": "Hi"}}]})),
        "data: not json\n\n".to_owned(),
        "data: [DONE]\n\n".to_owned(),
    ];
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.sites.openai.reply(fixture::sse(&chunks));

    let (_, _, body) = h.send(chat()).await;

    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert!(body.contains("data: not json\n\n"), "{body}");
    assert!(!body.contains("\"choices\":\"not json\""), "{body}");
}

#[tokio::test]
async fn every_filter_shape_writes_every_openai_message_shape() {
    let call =
        json!([{"id": "t", "type": "function", "function": {"name": "f", "arguments": "{}"}}]);
    let shapes = [
        json!({"role": "assistant", "content": null, "tool_calls": call, "refusal": null}),
        json!({"role": "assistant", "content": null, "refusal": "I can't"}),
        json!({"role": "assistant", "content": "Let me check", "tool_calls": call}),
        json!({"role": "assistant", "content": "hi"}),
    ];
    for shape in shapes {
        let reply = json!({"id": "c2", "choices": [{"index": 0, "message": shape,
            "logprobs": null, "finish_reason": "stop"}]})
        .to_string();
        let text = shape["content"].is_string();
        let mut all = vec![
            filtered(Footer(7, None)),
            filtered(Silence::default()),
            filtered(HoldAll(Vec::new())),
            filtered(Disclaimer),
            filtered(Nameless),
        ];
        // A footer joins the text of a message that has some.
        if text {
            all.push(filtered(Footer(0, None)));
        }
        for h in all {
            h.sites.openai.reply(fixture::json(StatusCode::OK, &reply));
            let (status, _, body) = h.send(unary_chat()).await;
            let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
            assert_eq!(status, StatusCode::OK, "{shape}: {body}");
        }
    }
}

#[tokio::test]
async fn other_choices_a_filter_took_leave_their_chunks() {
    /// Takes other content.
    #[derive(Clone)]
    struct Plain;

    impl Filter<Reply> for Plain {
        fn edit(&mut self, reply: Reply, out: &mut Vec<Reply>) -> Result<(), Fault> {
            if !matches!(reply, Reply::Other { .. }) {
                out.push(reply);
            }
            Ok(())
        }
    }

    let h = filtered(Plain);
    let stream = [
        chunk(
            json!({"id": "c1", "choices": [{"index": 0, "delta": {"role": "assistant", "content": "A"}}]}),
        ),
        chunk(
            json!({"id": "c1", "choices": [{"index": 1, "delta": {"role": "assistant", "content": "B"}}]}),
        ),
        chunk(json!({"id": "c1", "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]})),
        "data: [DONE]\n\n".to_owned(),
    ];
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.sites.openai.reply(fixture::sse(&chunks));

    let (status, _, body) = h.send(chat()).await;

    assert_eq!(status, StatusCode::OK);
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert!(!body.contains("error") && !body.contains("\"B\""), "{body}");
    assert!(body.ends_with("data: [DONE]\n\n"));
    assert_eq!(says(h.log.only()), Ok(()));
}

#[tokio::test]
async fn a_block_a_filter_adds_joins_the_one_text_of_an_openai_message() {
    let h = filtered(Footer(1, None));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    let (status, _, body) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&body.unwrap_or_default()).unwrap_or_default();
    assert_eq!(
        body.pointer("/choices/0/message/content"),
        Some(&json!("hi [footer]"))
    );
}

#[tokio::test]
async fn several_choices_are_relayed_each_with_its_own_words() {
    let stream = [
        chunk(
            json!({"id": "c1", "choices": [{"index": 0, "delta": {"content": "a"}, "finish_reason": "stop"}]}),
        ),
        chunk(json!({"id": "c1", "choices": [{"index": 1, "delta": {"content": "b"}}]})),
        chunk(json!({"id": "c1", "choices": [{"index": 1, "delta": {}, "finish_reason": "stop"}]})),
        "data: [DONE]\n\n".to_owned(),
    ];
    let h = relay();
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.sites.openai.reply(fixture::sse(&chunks));

    let body = h.body(chat()).await;
    let said = sent_chunks(&body);
    // Each choice keeps its words, those that are not the first as they came.
    let of = |index: u64| {
        let choices = said
            .iter()
            .flat_map(|c| c["choices"].as_array().into_iter().flatten());
        let own: Vec<_> = choices.filter(|c| c["index"] == index).collect();
        let text: String = own
            .iter()
            .filter_map(|c| c["delta"]["content"].as_str())
            .collect();
        (text, own.iter().any(|c| c["finish_reason"] == "stop"))
    };
    assert_eq!(of(0), ("a".to_owned(), true), "{body}");
    assert_eq!(of(1), ("b".to_owned(), true), "{body}");
    assert!(body.ends_with("data: [DONE]\n\n"));
}

#[tokio::test]
async fn a_filter_annotation_after_a_held_finish_waits_with_it() {
    let stream = [
        chunk(json!({"id": "c1", "choices": [{"index": 0, "delta": {"content": "Hi"}}]})),
        chunk(json!({"id": "c1", "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]})),
        chunk(json!({"id": "", "choices": [{"index": 0, "delta": {},
            "content_filter_offsets": {"check_offset": 0, "start_offset": 0, "end_offset": 2}}]})),
        "data: [DONE]\n\n".to_owned(),
    ];

    let (body, chunks) = footed(&stream).await;

    assert_eq!(
        said(&chunks),
        ("Hi [footer]".to_owned(), Some("stop"), None),
        "{body}"
    );
    let footer = body.find("[footer]").unwrap_or(usize::MAX);
    let finish = body.find("\"finish_reason\":\"stop\"").unwrap_or(0);
    assert!(footer < finish, "{body}");
}

#[tokio::test]
async fn groqs_usage_beside_the_response_is_counted() {
    let h = relay();
    let reply = json!({"id": "c2", "choices": [{"index": 0, "message": {"content": "hi"}}],
        "x_groq": {"usage": {"prompt_tokens": 7, "completion_tokens": 3}}});
    h.sites
        .openai
        .reply(fixture::json(StatusCode::OK, &reply.to_string()));

    let _ = h.send(unary_chat()).await;

    let usage = h.usage.total();
    assert_eq!(
        (
            usage.get(units::INPUT_TOKENS),
            usage.get(units::OUTPUT_TOKENS)
        ),
        (Some(7), Some(3))
    );
}

#[tokio::test]
async fn collecting_the_wrong_type_fails_even_with_nothing_said() {
    struct Wrong;

    #[async_trait]
    impl Plugin for Wrong {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let got = cx
                .call("openai")
                .await?
                .outputs::<String>()
                .try_collect::<Vec<_>>()
                .await;
            assert_eq!(
                got.err().map(|f| fault_code(&f)),
                Some("internal".to_owned())
            );
            // Its own kind reads, though empty.
            let got = cx
                .call("openai")
                .await?
                .outputs::<Reply>()
                .try_collect::<Vec<_>>()
                .await;
            assert!(got.is_ok_and(|events| events.is_empty()));
            Err(BadRequest::fault("checked"))
        }
    }

    let h = Harness::new(vec![Arc::new(Wrong)], None);
    h.sites
        .openai
        .reply(fixture::sse(&["data: [DONE]\n\n"]))
        .reply(fixture::sse(&["data: [DONE]\n\n"]));

    let (status, _, _) = h.send(chat()).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn a_plugin_knows_which_format_the_client_speaks() {
    struct Seen(Arc<Mutex<Vec<String>>>);

    #[async_trait]
    impl Plugin for Seen {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            if let Ok(mut seen) = self.0.lock() {
                seen.push(cx.format().to_owned());
            }
            Ok(None)
        }
    }

    let seen = Arc::new(Mutex::new(Vec::new()));
    let sites = Sites::default();
    let gw = providers([provider(&sites.openai, "openai", Some("Bearer sk-test"))])
        .and_then(|g| {
            g.client::<Conversation, _, _>(OpenAiChat)?
                .client::<Conversation, _, _>(AnthropicMessages)
        })
        .and_then(|g| g.plugin(Seen(seen.clone())).target("openai"))
        .expect("builds");
    let messages = r#"{"model":"gpt-x","max_tokens":10,"messages":[]}"#;
    for req in [post(CHAT, UNARY_REQ), post(MESSAGES, messages)] {
        sites.openai.reply(fixture::json(StatusCode::OK, UNARY));
        let _ = read_whole(serve(&gw, req).await.0).await;
    }

    assert_eq!(
        seen.lock().map(|s| s.clone()).ok(),
        Some(vec![
            "openai-chat".to_owned(),
            "anthropic-messages".to_owned()
        ])
    );
}

#[tokio::test]
async fn a_calls_own_input_its_format_cannot_write_is_refused_as_a_bad_request() {
    /// Asks again with a message of its own the format has no form for.
    struct Own;

    #[async_trait]
    impl Plugin for Own {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let image = json!({"type": "image", "source": {"type": "url", "url": "u"}});
            let message = Message::new(Role::User, Content::other("anthropic-messages", image));
            let own = Conversation::new([message]);
            cx.call("openai").with_input(own).await.map(Some)
        }
    }

    let h = Harness::new(vec![Arc::new(Own)], None);

    let (status, _, _) = h.send(unary_chat()).await;

    // Without the bytes a request came as, the gateway cannot tell a plugin's
    // message from the client's: a request it cannot write is the client's.
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(matches!(h.log.only(), Err(f) if fault_code(&f) == "bad_request"));
    assert!(h.sites.sent.sent().is_empty());
}

// A call's own input copied from the client's keeps what the kind has no
// place for, in the format's own terms.
#[tokio::test]
async fn a_calls_own_input_copied_from_the_clients_keeps_what_it_said() {
    /// Asks again with a trimmed copy of the client's request.
    struct Trim;

    #[async_trait]
    impl Plugin for Trim {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let mut trimmed = cx.input::<Conversation>()?.clone();
            trimmed.messages.truncate(1);
            cx.call("openai").with_input(trimmed).await.map(Some)
        }
    }

    let h = Harness::new(vec![Arc::new(Trim)], None);
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));
    let body = json!({"model": "gpt-x", "tools": [{"type": "function", "function": {"name": "f"}}],
        "messages": [{"role": "user", "content": "a"}, {"role": "user", "content": "b"}]});

    let (status, _, _) = h.send(call(CHAT, body.to_string())).await;

    assert_eq!(status, StatusCode::OK);
    let sent: serde_json::Value =
        serde_json::from_slice(h.sites.sent.sent()[0].body()).unwrap_or_default();
    assert_eq!(sent["tools"], body["tools"]);
    assert_eq!(sent["messages"], json!([{"role": "user", "content": "a"}]));
}

#[tokio::test]
async fn a_judges_own_prompt_carries_nothing_of_the_clients() {
    struct Judge;

    #[async_trait]
    impl Plugin for Judge {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let said = cx.input::<Conversation>()?.messages[0].text();
            let prompt = Conversation::new([
                Message::new(Role::System, "Answer SAFE or UNSAFE."),
                Message::new(Role::User, said),
            ]);
            let _verdict = cx
                .call("openai")
                .with_input(judged(prompt, "judge-1"))
                .await?
                .outputs::<Reply>()
                .try_collect::<Vec<_>>()
                .await?;
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Judge)], Some("openai"));
    h.sites
        .openai
        .reply(fixture::json(StatusCode::OK, UNARY))
        .reply(fixture::json(StatusCode::OK, UNARY));
    let body = json!({"model": "gpt-x", "response_format": {"type": "json_object"},
        "tools": [{"type": "function", "function": {"name": "f"}}], "user": "u1",
        "messages": [{"role": "user", "content": "hi"}]});
    let mut req = call("/v1/chat/completions?tenant=t1", body.to_string());
    let headers = req.headers_mut();
    headers.insert("authorization", HeaderValue::from_static("Bearer client"));
    headers.insert("x-app", HeaderValue::from_static("app-1"));

    let (status, _, _) = h.send(req).await;

    assert_eq!(status, StatusCode::OK);
    let sent = h.sites.sent.sent();
    let judge: serde_json::Value = serde_json::from_slice(sent[0].body()).unwrap_or_default();
    assert!(
        judge.get("tools").is_none()
            && judge.get("response_format").is_none()
            && judge.get("user").is_none(),
        "{judge}"
    );
    assert_eq!(judge["messages"][1]["content"], "hi");
    // Nor its query or headers: the key is the transport's own.
    assert_eq!(sent[0].uri().query(), None);
    let headers = sent[0].headers();
    assert!(headers.get("x-app").is_none());
    assert_eq!(headers["authorization"], "Bearer sk-test");
    // The client's own call still carries them.
    assert_eq!(sent[1].uri().query(), None);
    assert!(sent[1].headers().get("x-app").is_none());
}

#[tokio::test]
async fn a_judge_asks_whole_while_its_client_streams() {
    struct Judge;

    #[async_trait]
    impl Plugin for Judge {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let prompt = Conversation::new([Message::new(Role::User, "safe?")]);
            let verdict = cx
                .call("openai")
                .with_input(prompt)
                .await?
                .outputs::<Reply>()
                .try_collect::<Vec<_>>()
                .await?;
            let hi = |e: &Reply| matches!(e, Reply::Delta { delta, .. } if delta.as_str() == "hi");
            assert!(verdict.iter().any(hi));
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Judge)], Some("openai"));
    h.sites
        .openai
        .reply(fixture::json(StatusCode::OK, UNARY))
        .reply(fixture::sse(STREAM));

    let (status, _, body) = h.send(chat()).await;

    assert_eq!(status, StatusCode::OK);
    let judge: serde_json::Value =
        serde_json::from_slice(h.sites.sent.sent()[0].body()).unwrap_or_default();
    assert_ne!(judge["stream"], true, "{judge}");
    assert!(judge.get("stream_options").is_none(), "{judge}");
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert_eq!(gist(&body).message, gist(&STREAM.concat()).message);
    assert!(body.ends_with("data: [DONE]\n\n"), "{body}");
}

#[tokio::test]
async fn a_whole_answer_to_a_calls_own_input_reaches_a_streaming_client_as_a_stream() {
    struct Trim;

    #[async_trait]
    impl Plugin for Trim {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let own = Conversation::new([Message::new(Role::User, "hi")]);
            Ok(Some(cx.call("openai").with_input(own).await?))
        }
    }

    let h = Harness::new(vec![Arc::new(Trim)], Some("openai"));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    let (status, headers, body) = h.send(chat()).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["content-type"], "text/event-stream");
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert!(body.starts_with("data: {"), "{body}");
    assert!(body.contains("\"content\":\"hi\""), "{body}");
    assert!(body.ends_with("data: [DONE]\n\n"), "{body}");
}

#[tokio::test]
async fn a_plugin_asking_the_provider_otherwise_leaves_the_client_its_framing() {
    let fake = FakeOpenAi::start().await;
    let h = Harness::real(&fake, vec![Arc::new(Flip)], Some("openai"));
    fake.answer(Answer::raw(UNARY));
    fake.answer(real_sse(&[&STREAM.concat()]));

    let (_, streamed, body) = h.send(chat()).await;
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert_eq!(streamed["content-type"], "text/event-stream");
    assert!(body.contains("\"content\":\"hi\""), "{body}");
    assert!(body.ends_with("data: [DONE]\n\n"), "{body}");

    let (_, whole, body) = h.send(valid_unary_chat()).await;
    let body: serde_json::Value =
        serde_json::from_slice(&body.unwrap_or_default()).unwrap_or_default();
    assert_eq!(whole["content-type"], "application/json");
    assert_eq!(
        body["choices"][0]["message"]["content"], "Hello world",
        "{body}"
    );
}

#[tokio::test]
async fn a_reframed_stream_gets_usage_only_if_it_asked() {
    let h = Harness::new(vec![Arc::new(Flip)], Some("openai"));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));
    let quiet = r#"{"model":"gpt-x","stream":true,"messages":[]}"#;

    let body = h.body(call(CHAT, quiet)).await;

    assert!(!body.contains("usage"), "{body}");
    assert!(body.ends_with("data: [DONE]\n\n"), "{body}");
    assert_eq!(h.usage.total().get(units::OUTPUT_TOKENS), Some(1));
}

#[tokio::test]
async fn a_reframed_stream_that_fails_is_refused_in_the_clients_words() {
    let h = Harness::new(vec![Arc::new(Flip)], Some("openai"));
    let failing = [
        STREAM[0],
        "\"}}]}\n\ndata: {\"error\":{\"message\":\"busy\",\"type\":\"overloaded\",\"param\":null,\"code\":\"overloaded\"}}\n\n",
    ];
    h.sites.openai.reply(fixture::sse(&failing));

    let (status, headers, body) = h.send(unary_chat()).await;

    assert!(!status.is_success(), "{status}");
    assert_eq!(headers["content-type"], "application/json");
    let body: serde_json::Value =
        serde_json::from_slice(&body.unwrap_or_default()).unwrap_or_default();
    assert_eq!(body["error"]["code"], "overloaded", "{body}");
}

#[tokio::test]
async fn a_tool_call_is_reframed_as_the_client_asked() {
    let h = Harness::new(vec![Arc::new(Flip)], Some("openai"));
    let tool = r#"{"id":"c3","choices":[{"index":0,"message":{"role":"assistant","tool_calls":[{"id":"t","type":"function","function":{"name":"f","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}"#;
    h.sites.openai.reply(fixture::json(StatusCode::OK, tool));

    let (status, _, body) = h.send(chat()).await;

    assert_eq!(status, StatusCode::OK);
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert!(
        body.contains(r#""name":"f""#) && body.contains(r#""arguments":"{}""#),
        "{body}"
    );
    assert!(body.contains(r#""finish_reason":"tool_calls""#), "{body}");
}

#[tokio::test]
async fn a_relayed_redirect_passes_as_it_came_and_is_no_answer() {
    let h = Harness::new(vec![Arc::new(explicit::Relay::HEADS)], Some("openai"));
    let moved = http::Response::builder()
        .status(StatusCode::FOUND)
        .header("location", "http://elsewhere/")
        .body(Vec::new())
        .unwrap_or_default();
    h.sites.openai.reply(moved);

    let (status, headers, body) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::FOUND);
    assert_eq!(headers["location"], "http://elsewhere/");
    // r13 (F12-4): a head without a content type gets the format's, a
    // redirect's too.
    assert_eq!(headers["content-type"], "application/json");
    assert_eq!(body.ok().as_deref(), Some(&b""[..]));
    // The redirect is a fault, a 502 to any other format, that
    // carries the provider's answer.
    let end = h.log.only().err();
    assert_eq!(
        end.as_ref().map(respan_gateway_http::status),
        Some(StatusCode::BAD_GATEWAY)
    );
    let answer = end.as_ref().and_then(|f| {
        f.get::<respan_gateway_http::ProviderError>()
            .map(|e| &e.response)
    });
    assert_eq!(answer.map(|a| a.status()), Some(StatusCode::FOUND));
}

#[tokio::test]
async fn a_judge_reads_a_redirect_as_a_failure() {
    struct Judge;

    #[async_trait]
    impl Plugin for Judge {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let verdict = cx
                .call("openai")
                .await?
                .outputs::<Reply>()
                .try_collect::<Vec<_>>()
                .await;
            // A 502 fault carrying the provider's 302.
            let fault = verdict.err();
            assert_eq!(
                fault.as_ref().map(respan_gateway_http::status),
                Some(StatusCode::BAD_GATEWAY)
            );
            let answer = fault.as_ref().and_then(|f| {
                f.get::<respan_gateway_http::ProviderError>()
                    .map(|e| &e.response)
            });
            assert_eq!(answer.map(|a| a.status()), Some(StatusCode::FOUND));
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Judge)], Some("openai"));
    let moved = http::Response::builder()
        .status(StatusCode::FOUND)
        .body(Vec::new())
        .unwrap_or_default();
    h.sites
        .openai
        .reply(moved)
        .reply(fixture::json(StatusCode::OK, UNARY));

    let (status, _, _) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn a_failure_labelled_as_a_stream_is_still_a_failure() {
    let h = Harness::new(vec![Arc::new(explicit::Relay::HEADS)], Some("openai"));
    let failed = || {
        http::Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .header("content-type", "text/event-stream")
            .body(
                STREAM
                    .iter()
                    .map(|c| Ok(bytes::Bytes::from(*c)))
                    .collect::<Vec<_>>(),
            )
            .unwrap_or_default()
    };
    h.sites.openai.reply(failed());

    let (status, _, body) = h.send(chat()).await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body.ok().as_deref(), Some(STREAM.concat().as_bytes()));
    let end = h.log.only().map_err(|f| respan_gateway_http::status(&f));
    assert_eq!(end, Err(StatusCode::SERVICE_UNAVAILABLE));
}

#[tokio::test]
async fn a_redirect_answering_a_calls_own_input_is_a_bad_gateway() {
    struct Trim;

    #[async_trait]
    impl Plugin for Trim {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let own = Conversation::new([Message::new(Role::User, "hi")]);
            // The call's response fails as read, not the call at
            // its head; sent, the provider's words stay off a client whose
            // message the call did not relay.
            Ok(Some(cx.call("openai").with_input(own).await?))
        }
    }

    let h = Harness::new(vec![Arc::new(Trim)], Some("openai"));
    let moved = http::Response::builder()
        .status(StatusCode::FOUND)
        .header("location", "http://elsewhere/")
        .body(Vec::new())
        .unwrap_or_default();
    h.sites.openai.reply(moved);

    let (status, headers, _) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(headers.get("location").is_none());
}

#[tokio::test]
async fn a_success_framed_as_an_event_is_the_providers_failure_on_every_path() {
    let odd = "data: {\"id\":\"c2\",\"choices\":[{\"message\":{\"content\":\"hi\"}}]}\n\n";
    for plugins in [Vec::new(), vec![Arc::new(Shout) as Arc<dyn Plugin>]] {
        let h = Harness::new(plugins, Some("openai"));
        h.sites.openai.reply(fixture::json(StatusCode::OK, odd));

        let (status, body) = h.text(unary_chat()).await;

        // Refused and counted a provider failure, plugin or not.
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert!(body.contains("\"code\":\"upstream\""), "{body}");
        let end = h.log.only().map_err(|f| fault_code(&f));
        assert_eq!(end, Err("upstream".to_owned()));
    }
}

#[tokio::test]
async fn a_failure_of_several_events_keeps_the_providers_words() {
    struct Ask;

    #[async_trait]
    impl Plugin for Ask {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let answer = cx
                .call("openai")
                .await?
                .outputs::<Reply>()
                .try_collect::<Vec<_>>()
                .await;
            let fault = answer.err();
            assert_eq!(fault.as_ref().map(Fault::message), Some("busy"));
            assert_eq!(
                fault.as_ref().map(fault_code).as_deref(),
                Some("overloaded_error")
            );
            assert_eq!(
                fault.map(|f| respan_gateway_http::status(&f).as_u16()),
                Some(529)
            );
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Ask)], Some("openai"));
    let busy = "event: ping\ndata: {\"type\":\"ping\"}\n\nevent: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"busy\"}}\n\n";
    let failed = http::Response::builder()
        .status(StatusCode::from_u16(529).unwrap_or_default())
        .header("content-type", "text/event-stream")
        .body(vec![Ok(bytes::Bytes::from(busy))])
        .unwrap_or_default();
    h.sites
        .openai
        .reply(failed)
        .reply(fixture::json(StatusCode::OK, UNARY));

    let (status, _, _) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn a_failure_answering_a_calls_own_input_keeps_its_status_and_retry_after() {
    struct Trim;

    #[async_trait]
    impl Plugin for Trim {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let own = Conversation::new([Message::new(Role::User, "hi")]);
            Ok(Some(cx.call("openai").with_input(own).await?))
        }
    }

    let h = Harness::new(vec![Arc::new(Trim)], Some("openai"));
    let mut limited = fixture::json(
        StatusCode::TOO_MANY_REQUESTS,
        r#"{"error":{"message":"slow"}}"#,
    );
    limited
        .headers_mut()
        .insert("retry-after", HeaderValue::from_static("7"));
    limited
        .headers_mut()
        .insert("x-request-id", HeaderValue::from_static("req_1"));
    h.sites.openai.reply(limited);

    let (status, headers, _) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(headers["retry-after"], "7");
    assert!(headers.get("x-request-id").is_none());
}

#[tokio::test]
async fn an_error_in_a_success_says_what_the_provider_answered() {
    let h = Harness::new(Vec::new(), Some("openai"));
    h.sites
        .openai
        .reply(fixture::json(StatusCode::OK, r#"{"error":{"code":"x"}}"#))
        .reply(fixture::sse(&[
            "data: {\"error\":\"boom\",\"error_type\":\"overloaded\"}\n\n",
        ]));

    let _ = h.send(unary_chat()).await;
    let _ = h.send(chat()).await;

    let ends: Vec<_> = h
        .log
        .all()
        .into_iter()
        .map(|o| {
            o.map_err(|f| {
                (
                    respan_gateway_http::status(&f),
                    fault_code(&f),
                    f.message().to_owned(),
                )
            })
        })
        .collect();
    assert_eq!(
        ends,
        [
            Err((
                StatusCode::BAD_GATEWAY,
                "x".to_owned(),
                "the provider returned 200".to_owned()
            )),
            Err((
                StatusCode::BAD_GATEWAY,
                "overloaded".to_owned(),
                "boom".to_owned()
            )),
        ]
    );
}

#[tokio::test]
async fn a_gateway_dropped_mid_response_serves_what_it_began() {
    let h = Harness::new(Vec::new(), Some("openai"));
    h.sites.openai.reply(fixture::sse(STREAM));

    let req = chat();
    let (resp, ended) = serve(&h.gw, req).await;
    drop(h);
    let (_, _, body) = read_whole(resp).await;

    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert_eq!(gist(&body).message, gist(&STREAM.concat()).message);
    assert_eq!(says(ended.await), Ok(()));
}

#[tokio::test]
async fn a_providers_trailers_are_not_passed_on() {
    /// A provider whose whole answer ends in a trailer.
    struct Trailed;

    impl Trailed {
        pub async fn send(
            &self,
            _req: http::Request<Body>,
        ) -> Result<(http::Response<Body>, Option<Completion>), Fault> {
            let mut trailers = http::HeaderMap::new();
            trailers.insert("x-trailer", HeaderValue::from_static("1"));
            let frames = [
                Ok(http_body::Frame::data(bytes::Bytes::from_static(
                    UNARY.as_bytes(),
                ))),
                Ok(http_body::Frame::trailers(trailers)),
            ];
            let body = http_body_util::StreamBody::new(futures::stream::iter(frames));
            let mut resp = http::Response::new(Body::stream(
                http_body_util::BodyExt::into_data_stream(body),
            ));
            resp.headers_mut()
                .insert("content-type", HeaderValue::from_static("application/json"));
            Ok((resp, None))
        }
    }
    impl From<Trailed> for SendRequest {
        fn from(value: Trailed) -> Self {
            let value = std::sync::Arc::new(value);
            sender(move |call| {
                let value = value.clone();
                async move { value.send(call).await }
            })
        }
    }

    let log = Outcomes::default();
    let gw = gateway(
        [add_provider("openai", OpenAiChat, Trailed)],
        vec![Arc::new(log.clone())],
        Some("openai"),
    )
    .unwrap_or_else(|e| panic!("{e}"));

    let req = unary_chat();
    let (resp, _) = serve(&gw, req).await;
    let (_, body) = resp.into_parts();
    let collected = http_body_util::BodyExt::collect(body).await;
    gw.idle().await;

    let collected = collected.unwrap_or_else(|e| panic!("{e}"));
    assert!(collected.trailers().is_none());
    let body = String::from_utf8_lossy(&collected.to_bytes()).into_owned();
    assert_eq!(gist(&body).message.text(), "hi");
    assert_eq!(says(log.only()), Ok(()));
}

#[tokio::test]
async fn a_providers_error_event_in_a_stream_goes_out_as_it_came() {
    let h = relay();
    let error = "data: {\"error\":{\"message\":\"boom\",\"type\":\"server_error\",\"param\":null,\"code\":null}}\n\n";
    h.sites
        .openai
        .reply(fixture::sse(&[STREAM[0], "lo\"}}]}\n\n", error]));

    let (_, _, body) = h.send(chat()).await;

    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert!(body.ends_with(error), "{body}");
    let end = h.log.only().map_err(|f| fault_code(&f));
    assert_eq!(end, Err("server_error".to_owned()));
}

#[tokio::test]
async fn a_guard_refuses_what_it_cannot_read() {
    struct Guard;

    #[async_trait]
    impl Plugin for Guard {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let chat = cx.input::<Conversation>()?;
            if !chat.messages.iter().all(Message::is_plain) {
                let why = "content the guard cannot read";
                return Err(Fault::new(why)
                    .with(ErrorCode("unreadable".into()))
                    .with(Status(StatusCode::FORBIDDEN)));
            }
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Guard)], Some("openai"));
    let image = r#"{"model":"m","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"x"}}]}]}"#;

    let (status, _, _) = h.send(call(CHAT, image)).await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(h.sites.sent.sent().is_empty());
}

#[tokio::test]
async fn a_providers_error_after_its_finish_keeps_both_and_ends_last() {
    let error = "data: {\"error\":{\"message\":\"boom\",\"type\":\"server_error\",\"param\":null,\"code\":null}}\n\n";
    let content =
        "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hi\"}}]}\n\n";
    let finish = "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n";
    for held in [false, true] {
        // A filter that keeps the finish back and adds a tail at the end.
        let h = match held {
            true => filtered(Footer(0, None)),
            false => relay(),
        };
        h.sites
            .openai
            .reply(fixture::sse(&[content, finish, error]));

        let (_, _, body) = h.send(chat()).await;

        let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
        assert!(body.contains("\"content\":\"Hi\""), "held {held}: {body}");
        // A provider's finish is written. One a filter kept back past a failed
        // end is not, as it would claim the answer complete.
        let finished = body.contains("\"finish_reason\":\"stop\"");
        assert_eq!(finished, !held, "{body}");
        assert!(body.ends_with(error), "held {held}: {body}");
        assert_eq!(body.contains("[footer]"), held, "{body}");
        let end = h.log.only().map_err(|f| fault_code(&f));
        assert_eq!(end, Err("server_error".to_owned()));
    }
}

// A provider's error event mid-stream goes as it came to a client of its
// format, fields its format lacks and all.
#[tokio::test]
async fn a_providers_error_event_mid_stream_goes_as_it_came_to_its_format() {
    let h = Harness::new(vec![Arc::new(explicit::Relay::HEADS)], Some("openai"));
    let error = "data: {\"error\":{\"code\":502,\"message\":\"Provider returned error\",\"metadata\":{\"raw\":\"x\"}}}\n\n";
    let content =
        "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hi\"}}]}\n\n";
    h.sites.openai.reply(fixture::sse(&[content, error]));

    let (_, _, body) = h.send(chat()).await;

    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert_eq!(gist_text(&sent_chunks(&body)), "Hi");
    assert!(body.ends_with(error), "{body}");
    assert!(h.log.only().is_err());
}

// A provider's error inside a 200 goes as it came to a client of its format.
#[tokio::test]
async fn a_providers_error_in_a_success_goes_as_it_came_to_its_format() {
    let h = Harness::new(vec![Arc::new(explicit::Relay::HEADS)], Some("openai"));
    let error = r#"{"error": {"code": 502, "message": "Provider returned error", "metadata": {"raw": "x"}}}"#;
    h.sites.openai.reply(fixture::json(StatusCode::OK, error));

    let (status, body) = h.text(unary_chat()).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(body, error);
}

#[tokio::test]
async fn a_response_dropped_unsent_cancels_its_provider_body() {
    /// Sets its flag when dropped.
    struct Dropped(Arc<AtomicBool>);

    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    /// A provider that answers 200 and streams forever, keeping a guard.
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
            resp.headers_mut().insert(
                "content-type",
                HeaderValue::from_static("text/event-stream"),
            );
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

    /// Calls the provider, drops its response and answers itself.
    struct Discard;

    #[async_trait]
    impl Plugin for Discard {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            drop(cx.call("openai").await?);
            Cached.on_client(cx).await
        }
    }

    let dropped = Arc::new(AtomicBool::new(false));
    let log = Outcomes::default();
    let gw = gateway(
        [add_provider("openai", OpenAiChat, Endless(dropped.clone()))],
        vec![Arc::new(log.clone()), Arc::new(Discard)],
        None,
    )
    .unwrap_or_else(|e| panic!("{e}"));

    let req = chat();
    let (status, _, _) = read_whole(serve(&gw, req).await.0).await;
    gw.idle().await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(says(log.only()), Ok(()));
    assert!(dropped.load(Ordering::SeqCst));
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
    let model = px
        .input::<Conversation>()
        .ok()
        .and_then(|c| c.model.clone());
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
    usage.get(units::INPUT_TOKENS).unwrap_or_default()
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

#[tokio::test]
async fn every_call_a_smart_router_makes_is_billed_once_on_the_provider_edge() {
    /// Asks a judge model which provider should answer, then targets it.
    struct SmartRouter;

    #[async_trait]
    impl Plugin for SmartRouter {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let ask = Conversation::new([Message::new(Role::User, "openai or backup?")]);
            let judge = cx.call("openai").with_input(judged(ask, "judge-1"));
            let verdict = judge
                .await?
                .outputs::<Reply>()
                .try_collect::<Vec<_>>()
                .await?;
            let pick = verdict.iter().find_map(|e| match e {
                Reply::Delta { delta, .. } => Some(delta.as_str().to_owned()),
                _ => None,
            });
            cx.set_target(pick.unwrap_or_else(|| "openai".into()));
            Ok(None)
        }
    }

    let bill = Bill::default();
    let h = Harness::new(vec![Arc::new(SmartRouter), Arc::new(bill.clone())], None);
    let verdict = json!({"id": "j", "choices": [{"message": {"content": "backup"}}],
        "usage": {"prompt_tokens": 50, "completion_tokens": 1}});
    h.sites
        .openai
        .reply(fixture::json(StatusCode::OK, &verdict.to_string()));
    h.sites.backup.reply(fixture::json(StatusCode::OK, UNARY));

    let (status, _) = h.text(unary_chat()).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(bill.started(), ["openai/judge-1", "client", "backup/gpt-x"]);
    let ended = bill.ended();
    let ended: Vec<_> = ended.iter().map(|(c, o)| (c.as_str(), o.is_ok())).collect();
    assert_eq!(ended, [("backup/gpt-x", true), ("openai/judge-1", true)]);
    let billed = [
        ("backup/gpt-x".to_owned(), 4),
        ("openai/judge-1".to_owned(), 50),
    ];
    assert_eq!(metered(&h), billed);
    assert_eq!(input_tokens(&h.usage.total()), 54);
}

#[tokio::test]
async fn a_call_refused_on_the_provider_edge_refuses_the_request_before_sending() {
    struct Deny;

    #[async_trait]
    impl Plugin for Deny {
        async fn on_provider(&self, _px: &mut ProviderContext) -> Result<(), Fault> {
            Err(Fault::new("no calls")
                .with(ErrorCode("denied".into()))
                .with(Status(StatusCode::FORBIDDEN)))
        }
    }

    let bill = Bill::default();
    let h = Harness::new(vec![Arc::new(Deny), Arc::new(bill.clone())], Some("openai"));

    let (status, body) = h.text(unary_chat()).await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(body.contains(r#""code":"denied""#), "{body}");
    assert!(h.sites.sent.sent().is_empty());
    // Refused before the plugins after it ran on the provider edge.
    assert_eq!(bill.started(), ["client"]);
    assert!(matches!(h.log.only(), Err(f) if fault_code(&f) == "denied"));
}

#[tokio::test]
async fn a_calls_own_copy_of_the_input_changes_only_that_call() {
    /// Cools only calls to `openai`; copies every call's input.
    struct CoolOpenai;

    #[async_trait]
    impl Plugin for CoolOpenai {
        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            if px.provider() != "openai" {
                return Ok(());
            }
            px.filter(|chat: &mut Conversation| {
                chat.temperature = Some(0.0);
                Ok(())
            })
        }
    }

    let chain: Vec<Arc<dyn Plugin>> = vec![
        Arc::new(CoolOpenai),
        Arc::new(Fallback(vec!["openai", "backup"])),
    ];
    let h = Harness::new(chain, None);
    h.sites
        .openai
        .reply(fixture::json(StatusCode::SERVICE_UNAVAILABLE, "{}"));
    h.sites.backup.reply(fixture::json(StatusCode::OK, UNARY));

    assert_eq!(h.text(unary_chat()).await.0, StatusCode::OK);

    let sent = h.sites.sent.sent();
    let cooled: serde_json::Value = serde_json::from_slice(sent[0].body()).unwrap_or_default();
    assert_eq!(cooled["temperature"], 0.0);
    // The other call is the request as it said it.
    let plain: serde_json::Value = serde_json::from_slice(sent[1].body()).unwrap_or_default();
    assert_eq!(plain, json!({ "model": "gpt-x", "messages": [] }));
}

#[tokio::test]
async fn provider_edge_filters_run_before_client_edge_ones_each_last_added_first() {
    /// Marks the text on both edges.
    struct Mark(&'static str);

    fn mark(tag: String) -> impl FnMut(&mut Reply) -> Result<(), Fault> + Send + 'static {
        move |e| {
            if let Some(text) = text_of_mut(e) {
                text.push_str(&tag);
            }
            Ok(())
        }
    }

    #[async_trait]
    impl Plugin for Mark {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(mark(format!(" c{}", self.0)))?;
            Ok(None)
        }

        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            px.filter(mark(format!(" p{}", self.0)))
        }
    }

    let h = Harness::new(
        vec![Arc::new(Mark("1")), Arc::new(Mark("2"))],
        Some("openai"),
    );
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    let body: serde_json::Value =
        serde_json::from_str(&h.body(unary_chat()).await).unwrap_or_default();

    assert_eq!(body["choices"][0]["message"]["content"], "hi p2 p1 c2 c1");
}

#[tokio::test]
async fn a_call_dropped_unread_ends_once_as_abandoned() {
    /// Calls a provider and never reads its answer.
    struct Peek;

    #[async_trait]
    impl Plugin for Peek {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            drop(cx.call("backup").await?);
            Ok(None)
        }
    }

    let bill = Bill::default();
    let h = Harness::new(vec![Arc::new(Peek), Arc::new(bill.clone())], Some("openai"));
    h.sites.backup.reply(fixture::sse(STREAM));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    assert_eq!(h.text(unary_chat()).await.0, StatusCode::OK);

    let ended = bill.ended();
    assert_eq!(ended.len(), 2);
    let (call, dropped) = &ended[0];
    assert_eq!(call, "backup/gpt-x");
    assert!(matches!(&dropped, Err(f) if fault_code(f) == "abandoned"));
    assert_eq!(says(ended[1].1.clone()), Ok(()));
}

#[tokio::test]
async fn each_attempt_of_a_fallback_gets_the_provider_edge() {
    let bill = Bill::default();
    let chain: Vec<Arc<dyn Plugin>> = vec![
        Arc::new(Fallback(vec!["openai", "backup"])),
        Arc::new(bill.clone()),
    ];
    let h = Harness::new(chain, None);
    h.sites
        .openai
        .reply(fixture::json(StatusCode::SERVICE_UNAVAILABLE, "{}"));
    h.sites.backup.reply(fixture::sse(STREAM));

    let said = gist(&h.body(chat()).await);
    assert_eq!(said.message, gist(&STREAM.concat()).message);

    // The fallback gave the answer, so the plugin after it had no client edge,
    // yet saw both calls.
    assert_eq!(bill.started(), ["openai/gpt-x", "backup/gpt-x"]);
    let ended = bill.ended();
    // The failed call's end is the provider's answer, which carries its
    // status.
    let (failed, answered) = (&ended[1].1, &ended[0].1);
    let failed = failed.as_ref().err();
    assert!(failed.is_some_and(|f| {
        f.get::<respan_gateway_http::ProviderError>()
            .map(|e| &e.response)
            .is_some()
    }));
    assert_eq!(
        failed.map(respan_gateway_http::status),
        Some(StatusCode::SERVICE_UNAVAILABLE)
    );
    assert_eq!(says(answered.clone()), Ok(()));
    let backup = metered(&h)
        .into_iter()
        .find(|(c, _)| c.starts_with("backup"));
    assert_eq!(backup.map(|(_, n)| n), Some(5));
}

fn ended_with(bill: &Bill, code: &str) -> bool {
    let ended = bill.ended();
    ended.len() == 1 && matches!(&ended[0].1, Err(f) if fault_code(f) == code)
}

#[tokio::test]
async fn a_panic_in_on_provider_ends_the_call_and_refuses_the_request() {
    let bill = Bill::default();
    let panics = OnProvider(|_| panic!("a buggy plugin"));
    let h = Harness::new(
        vec![Arc::new(bill.clone()), Arc::new(panics)],
        Some("openai"),
    );

    assert_eq!(h.text(chat()).await.0, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(h.sites.sent.sent().is_empty());
    assert!(ended_with(&bill, "internal"));
}

#[tokio::test]
async fn a_panic_in_a_provider_filter_ends_the_call_as_internal() {
    let bill = Bill::default();
    let panics = OnProvider(|px| px.filter(|_: &mut Reply| panic!("a buggy filter")));
    let h = Harness::new(
        vec![Arc::new(bill.clone()), Arc::new(panics)],
        Some("openai"),
    );
    h.sites.openai.reply(fixture::sse(STREAM));

    assert_eq!(h.text(chat()).await.0, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(ended_with(&bill, "internal"));
    assert!(matches!(h.log.only(), Err(f) if fault_code(&f) == "internal"));
}

#[tokio::test]
async fn a_client_that_leaves_mid_stream_ends_the_call_abandoned() {
    let bill = Bill::default();
    let h = Harness::new(vec![Arc::new(bill.clone())], Some("openai"));
    h.sites.openai.reply(fixture::sse(STREAM));

    let req = chat();
    let (_, mut body) = serve(&h.gw, req).await.0.into_parts();
    let _first = http_body_util::BodyExt::frame(&mut body).await;
    drop(body);
    h.gw.idle().await;

    assert!(ended_with(&bill, "abandoned"));
}

#[tokio::test]
async fn a_call_the_transport_fails_ends_with_its_fault() {
    let bill = Bill::default();
    let h = Harness::new(vec![Arc::new(bill.clone())], Some("openai"));

    assert_eq!(h.text(chat()).await.0, StatusCode::BAD_GATEWAY);
    // This one is no answer.
    assert!(ended_with(&bill, "upstream"));
    assert!(bill.ended()[0].1.as_ref().is_err_and(|f| {
        f.get::<respan_gateway_http::ProviderError>()
            .map(|e| &e.response)
            .is_none()
    }));
}

#[tokio::test]
async fn a_refused_call_ends_for_the_refusing_plugin_and_those_before_it() {
    /// Bills the call, then refuses it.
    struct Deny(Bill);

    #[async_trait]
    impl Plugin for Deny {
        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            self.0.on_provider(px).await?;
            Err(Fault::new("no calls")
                .with(ErrorCode("denied".into()))
                .with(Status(StatusCode::FORBIDDEN)))
        }

        async fn on_provider_end(&self, px: &ProviderContext, end: &Result<(), Fault>) {
            self.0.on_provider_end(px, end).await;
        }
    }

    let (before, deny, after) = (Bill::default(), Bill::default(), Bill::default());
    let chain: Vec<Arc<dyn Plugin>> = vec![
        Arc::new(before.clone()),
        Arc::new(Deny(deny.clone())),
        Arc::new(after.clone()),
    ];
    let h = Harness::new(chain, Some("openai"));

    assert_eq!(h.text(unary_chat()).await.0, StatusCode::FORBIDDEN);
    assert!(ended_with(&before, "denied"));
    assert!(ended_with(&deny, "denied"));
    assert!(after.ended().is_empty());
}

#[tokio::test]
async fn a_model_set_on_the_provider_edge_is_that_calls_own() {
    let cheap = OnProvider(|px| {
        if px.provider() == "backup" {
            px.filter(set_model("cheap"))?;
        }
        Ok(())
    });
    let bill = Bill::default();
    let chain: Vec<Arc<dyn Plugin>> = vec![
        Arc::new(cheap),
        Arc::new(bill.clone()),
        Arc::new(Fallback(vec!["openai", "backup"])),
    ];
    let h = Harness::new(chain, None);
    h.sites
        .openai
        .reply(fixture::json(StatusCode::SERVICE_UNAVAILABLE, "{}"));
    h.sites.backup.reply(fixture::json(StatusCode::OK, UNARY));

    assert_eq!(h.text(unary_chat()).await.0, StatusCode::OK);

    let models: Vec<serde_json::Value> = h
        .sites
        .sent
        .sent()
        .iter()
        .map(|r| serde_json::from_slice::<serde_json::Value>(r.body()).unwrap_or_default())
        .map(|v| v["model"].clone())
        .collect();
    assert_eq!(models, [json!("gpt-x"), json!("cheap")]);
    assert_eq!(bill.started(), ["client", "openai/gpt-x", "backup/cheap"]);
}

#[tokio::test]
async fn extensions_set_in_on_provider_reach_that_calls_end_only() {
    /// A reservation made for one call.
    #[derive(Clone)]
    struct Reservation(&'static str);

    #[derive(Clone, Default)]
    struct Reserve(Arc<Mutex<Vec<Option<&'static str>>>>);

    #[async_trait]
    impl Plugin for Reserve {
        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            px.extensions_mut().insert(Reservation("r1"));
            // The call's extensions are its copy of the request's.
            let call = &px.extensions();
            assert!(call.get::<Reservation>().is_some());
            Ok(())
        }

        async fn on_provider_end(&self, px: &ProviderContext, _end: &Result<(), Fault>) {
            let held = px.extensions().get::<Reservation>().map(|r| r.0);
            self.0.lock().map(|mut s| s.push(held)).ok();
        }

        async fn on_client_end(&self, cx: &ClientContext, _end: &Result<(), Fault>) {
            let held = cx.extensions().get::<Reservation>().map(|r| r.0);
            self.0.lock().map(|mut s| s.push(held)).ok();
        }
    }

    let reserve = Reserve::default();
    let h = Harness::new(vec![Arc::new(reserve.clone())], Some("openai"));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    assert_eq!(h.text(unary_chat()).await.0, StatusCode::OK);

    let mut seen = reserve.0.lock().map(|s| s.clone()).unwrap_or_default();
    seen.sort();
    assert_eq!(seen, [None, Some("r1")]);
}

#[tokio::test]
async fn a_calls_request_is_as_it_was_when_the_call_was_made() {
    /// Calls twice, changing the request's model in between.
    #[derive(Clone, Default)]
    struct Twice(Arc<Mutex<Vec<String>>>);

    #[async_trait]
    impl Plugin for Twice {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(set_model("a"))?;
            drop(cx.call("openai").await?);
            cx.filter(set_model("b"))?;
            cx.call("openai").await.map(Some)
        }

        async fn on_provider_end(&self, px: &ProviderContext, _end: &Result<(), Fault>) {
            let model = px.client().input::<Conversation>().ok();
            let model = model.and_then(|c| c.model.clone());
            self.0.lock().map(|mut s| s.extend(model)).ok();
        }
    }

    let twice = Twice::default();
    let h = Harness::new(vec![Arc::new(twice.clone())], Some("openai"));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    assert_eq!(h.text(unary_chat()).await.0, StatusCode::OK);
    let mut seen = twice.0.lock().map(|s| s.clone()).unwrap_or_default();
    seen.sort();
    assert_eq!(seen, ["a", "b"]);
}

#[tokio::test]
async fn idle_waits_for_a_slow_on_provider_end() {
    #[derive(Clone, Default)]
    struct Slow(Arc<AtomicBool>);

    #[async_trait]
    impl Plugin for Slow {
        async fn on_provider_end(&self, _px: &ProviderContext, _end: &Result<(), Fault>) {
            tokio::time::sleep(Duration::from_millis(50)).await;
            self.0.store(true, Ordering::SeqCst);
        }
    }

    let slow = Slow::default();
    let h = Harness::new(vec![Arc::new(slow.clone())], Some("openai"));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    // `send` returns once `idle` does.
    assert_eq!(h.text(unary_chat()).await.0, StatusCode::OK);
    assert!(slow.0.load(Ordering::SeqCst));
}

#[tokio::test]
async fn an_undecodable_input_is_refused_without_explicit_raw_delivery() {
    /// Reads the input on both edges and changes nothing.
    struct Read;

    #[async_trait]
    impl Plugin for Read {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let _ = cx.input::<Conversation>();
            Ok(None)
        }

        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            let _ = px.input::<Conversation>();
            Ok(())
        }
    }

    let h = Harness::new(vec![Arc::new(Read)], Some("openai"));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));
    let request = r#"["not an object"]"#;

    assert_eq!(h.text(call(CHAT, request)).await.0, StatusCode::BAD_REQUEST);
    assert!(h.sites.sent.sent().is_empty());
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
            .get::<respan_gateway_sdk::Extensions>()
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

#[tokio::test]
async fn extensions_set_in_on_provider_reach_the_transport() {
    let key = OnProvider(|px| {
        px.extensions_mut().insert(TenantKey("k1"));
        Ok(())
    });
    let sites = Sites::default();
    let gw = gateway(
        [add_provider(
            "openai",
            OpenAiChat,
            TenantSender(sites.openai.plain()),
        )],
        vec![Arc::new(key)],
        Some("openai"),
    )
    .unwrap_or_else(|e| panic!("gateway: {e}"));
    sites.openai.reply(fixture::json(StatusCode::OK, UNARY));
    let req = unary_chat();

    assert_eq!(read_whole(serve(&gw, req).await.0).await.0, StatusCode::OK);
    let sent = sites.sent.sent();
    let key = sent[0].headers().get("x-tenant-key");
    assert_eq!(key.and_then(|k| k.to_str().ok()), Some("k1"));
}

#[tokio::test]
async fn a_calls_own_extension_reaches_its_transport() {
    /// Answers with a call carrying a tenant's key.
    struct Tenant;

    #[async_trait]
    impl Plugin for Tenant {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let call = cx.call("openai").with_extension(TenantKey("k2"));
            call.await.map(Some)
        }
    }

    let sites = Sites::default();
    let transport = TenantSender(sites.openai.plain());
    let gw = gateway(
        [add_provider("openai", OpenAiChat, transport)],
        vec![Arc::new(Tenant)],
        None,
    )
    .unwrap_or_else(|e| panic!("gateway: {e}"));
    sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    assert_eq!(
        read_whole(serve(&gw, unary_chat()).await.0).await.0,
        StatusCode::OK
    );
    let key = sites.sent.sent()[0].headers().get("x-tenant-key").cloned();
    assert_eq!(key, Some(HeaderValue::from_static("k2")));
}

// Each call a request makes is metered once, on its provider edge.
#[tokio::test]
async fn each_call_a_request_makes_is_metered_once() {
    /// Reads one call through, then answers with a second.
    struct Twice;

    #[async_trait]
    impl Plugin for Twice {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let first = cx.call("openai").await?.outputs::<Reply>();
            first.try_collect::<Vec<_>>().await?;
            Ok(Some(cx.call("openai").await?))
        }
    }

    let bill = Bill::default();
    let h = Harness::new(vec![Arc::new(bill.clone()), Arc::new(Twice)], None);
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    assert_eq!(h.text(unary_chat()).await.0, StatusCode::OK);
    assert_eq!(bill.ended().len(), 2);
    let calls: Vec<u64> = metered(&h).into_iter().map(|(_, n)| n).collect();
    assert_eq!(calls, [4, 4]);
    assert_eq!(input_tokens(&h.usage.total()), 8);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_calls_provider_end_runs_before_the_clients_end() {
    /// Records each end hook, the provider edge's slowly.
    #[derive(Clone, Default)]
    struct Order(Arc<Mutex<Vec<&'static str>>>);

    #[async_trait]
    impl Plugin for Order {
        async fn on_provider_end(&self, _px: &ProviderContext, _end: &Result<(), Fault>) {
            tokio::time::sleep(Duration::from_millis(20)).await;
            self.0.lock().map(|mut o| o.push("provider")).ok();
        }

        async fn on_client_end(&self, _cx: &ClientContext, _end: &Result<(), Fault>) {
            self.0.lock().map(|mut o| o.push("client")).ok();
        }
    }

    /// Calls a provider and drops its answer unread.
    struct Peek;

    #[async_trait]
    impl Plugin for Peek {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            drop(cx.call("backup").await?);
            Ok(None)
        }
    }

    let order = Order::default();
    let h = Harness::new(
        vec![Arc::new(order.clone()), Arc::new(Peek)],
        Some("openai"),
    );
    h.sites.backup.reply(fixture::sse(STREAM));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    assert_eq!(h.text(unary_chat()).await.0, StatusCode::OK);
    let order = order.0.lock().map(|o| o.clone()).unwrap_or_default();
    assert_eq!(order, ["provider", "provider", "client"]);
}

#[tokio::test]
async fn a_provider_filter_edits_a_call_a_plugin_collects() {
    /// Targets the provider a judge names.
    struct Pick;

    #[async_trait]
    impl Plugin for Pick {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let ask = Conversation::new([Message::new(Role::User, "which?")]);
            let judge = cx.call("openai").with_input(judged(ask, "judge-1"));
            let verdict = judge
                .await?
                .outputs::<Reply>()
                .try_collect::<Vec<_>>()
                .await?;
            let pick = verdict.iter().find_map(|e| match e {
                Reply::Delta { delta, .. } => Some(delta.as_str().to_owned()),
                _ => None,
            });
            cx.set_target(pick.unwrap_or_default());
            Ok(None)
        }
    }

    // Overrules the judge on the provider edge.
    let overrule =
        OnProvider(
            |px| match px.input::<Conversation>()?.model.as_deref() == Some("judge-1") {
                true => px.filter(move |r: &mut Reply| {
                    if let Some(text) = text_of_mut(r) {
                        *text = text.replace("openai", "backup");
                    }
                    Ok(())
                }),
                _ => Ok(()),
            },
        );
    let h = Harness::new(vec![Arc::new(overrule), Arc::new(Pick)], None);
    let verdict = json!({"id": "j", "choices": [{"message": {"content": "openai"}}]});
    h.sites
        .openai
        .reply(fixture::json(StatusCode::OK, &verdict.to_string()));
    h.sites.backup.reply(fixture::json(StatusCode::OK, UNARY));

    assert_eq!(h.text(unary_chat()).await.0, StatusCode::OK);
    assert!(h.sites.sent.sent()[1].uri().to_string().starts_with(BACKUP));
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

#[tokio::test]
async fn a_response_kept_in_the_extensions_ends_as_the_request_closes() {
    /// Calls a provider and keeps its answer, unread, in the request.
    struct Stash;

    #[async_trait]
    impl Plugin for Stash {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let kept = Kept(Arc::new(Mutex::new(Some(cx.call("backup").await?))));
            cx.extensions_mut().insert(kept);
            Ok(None)
        }
    }

    let ends = Ends::default();
    let h = Harness::new(
        vec![Arc::new(ends.clone()), Arc::new(Stash)],
        Some("openai"),
    );
    h.sites.backup.reply(fixture::sse(STREAM));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    assert_eq!(closes(&h, unary_chat()).await, StatusCode::OK);
    let mut ends = ends.all();
    assert_eq!(ends.pop().as_deref(), Some("client"));
    ends.sort();
    assert_eq!(ends, ["backup abandoned", "openai ok"]);
}

#[tokio::test]
async fn a_response_kept_past_its_request_ends_abandoned_and_reads_so() {
    /// Races two providers and answers with the first, keeping the loser.
    struct Hedge(Kept);

    #[async_trait]
    impl Plugin for Hedge {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let (first, loser) = (cx.call("openai"), cx.call("backup"));
            let (first, loser) = futures::join!(first, loser);
            *self.0.0.lock().map_err(|_| Fault::internal("poisoned"))? = Some(loser?);
            first.map(Some)
        }
    }

    let (ends, kept) = (Ends::default(), Kept::default());
    let h = Harness::new(
        vec![Arc::new(ends.clone()), Arc::new(Hedge(kept.clone()))],
        None,
    );
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));
    h.sites.backup.reply(fixture::sse(STREAM));

    assert_eq!(closes(&h, unary_chat()).await, StatusCode::OK);
    let mut ends = ends.all();
    assert_eq!(ends.pop().as_deref(), Some("client"));
    ends.sort();
    assert_eq!(ends, ["backup abandoned", "openai ok"]);
    let read = kept
        .take()
        .expect("kept")
        .outputs::<Reply>()
        .try_collect::<Vec<_>>()
        .await;
    assert_eq!(read.err().map(|f| fault_code(&f)), Some("abandoned".into()));
}

#[tokio::test]
async fn a_client_edge_failure_ends_the_call_whose_response_was_sent() {
    let bill = Bill::default();
    let guard =
        Guard(|e| matches!(e, Reply::Delta { delta, .. } if delta.as_str().contains("world")));
    let h = Harness::new(
        vec![Arc::new(bill.clone()), Arc::new(guard)],
        Some("openai"),
    );
    let stream = [
        chunk(
            json!({"id": "c1", "choices": [{"index": 0, "delta": {"content": "Hi"}}],
            "usage": {"prompt_tokens": 5, "completion_tokens": 1}}),
        ),
        chunk(json!({"id": "c1", "choices": [{"index": 0, "delta": {"content": " world"}}]})),
        "data: [DONE]\n\n".to_owned(),
    ];
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.sites.openai.reply(fixture::sse(&chunks));

    let _ = h.send(chat()).await;

    let ended = bill.ended();
    assert_eq!(ended.len(), 1);
    let end = &ended[0].1;
    assert_eq!(
        end.as_ref().err().map(fault_code).as_deref(),
        Some("blocked")
    );
    assert_eq!(input_tokens(&h.usage.all()[0]), 5);
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

#[tokio::test]
async fn a_response_goes_only_to_the_request_whose_call_it_answers() {
    /// On a request marked `x-keep`, keeps a call's response where the
    /// next request sends it.
    struct Swap(Kept);

    #[async_trait]
    impl Plugin for Swap {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let asked = cx.extensions().get::<Wire>().and_then(Wire::get);
            match asked.is_some_and(|a| a.headers().contains_key("x-keep")) {
                true => {
                    let resp = cx.call("backup").await?;
                    *self.0.0.lock().map_err(|_| Fault::internal("poisoned"))? = Some(resp);
                    Ok(None)
                }
                _ => Ok(self.0.take()),
            }
        }
    }

    let (ends, swap) = (Ends::default(), Arc::new(Swap(Kept::default())));
    let sites = Sites::default();
    let gw = gateway(
        [
            provider(&sites.openai, "openai", Some("Bearer sk-test")),
            provider(&sites.backup, "backup", None),
        ],
        vec![Arc::new(ends.clone()), swap],
        Some("openai"),
    )
    .expect("builds");
    sites.backup.reply(fixture::sse(STREAM));
    sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    // Request `a` is still open, its answer unread, when `b` runs.
    let mut a = post(CHAT, UNARY_REQ);
    a.headers_mut()
        .insert("x-keep", HeaderValue::from_static("1"));
    let (a, _) = serve(&gw, a).await;
    let (status, _, body) = read_whole(serve(&gw, post(CHAT, UNARY_REQ)).await.0).await;
    let _ = read_whole(a).await;
    gw.idle().await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let body = String::from_utf8_lossy(&body.unwrap_or_default()).into_owned();
    assert!(body.contains("internal"), "{body}");
    let mut ends = ends.all();
    ends.sort();
    assert_eq!(ends, ["backup abandoned", "client", "client", "openai ok"]);
}

#[tokio::test]
async fn a_reader_waiting_on_a_kept_response_wakes_as_its_request_closes() {
    type Reading = tokio::task::JoinHandle<Result<Vec<Reply>, Fault>>;

    /// Reads a call whose body never ends, in a task of its own.
    struct Watch(Arc<Mutex<Option<Reading>>>);

    #[async_trait]
    impl Plugin for Watch {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let reading = tokio::spawn(
                cx.call("slow")
                    .await?
                    .outputs::<Reply>()
                    .try_collect::<Vec<_>>(),
            );
            *self.0.lock().map_err(|_| Fault::internal("poisoned"))? = Some(reading);
            Ok(None)
        }
    }

    let (dropped, reader) = (Arc::new(AtomicBool::new(false)), Arc::default());
    let sites = Sites::default();
    let gw = gateway(
        [
            provider(&sites.openai, "openai", Some("Bearer sk-test")),
            add_provider("slow", OpenAiChat, Endless(dropped.clone())),
        ],
        vec![Arc::new(Watch(Arc::clone(&reader)))],
        Some("openai"),
    )
    .expect("builds");
    sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    let (status, _, _) = read_whole(serve(&gw, post(CHAT, UNARY_REQ)).await.0).await;
    let reading = reader.lock().ok().and_then(|mut r| r.take()).expect("read");
    let read = tokio::time::timeout(Duration::from_secs(5), reading).await;
    gw.idle().await;

    assert_eq!(status, StatusCode::OK);
    let read = read.expect("the reader woke").expect("the reader ran");
    assert_eq!(read.err().map(|f| fault_code(&f)), Some("abandoned".into()));
    assert!(dropped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn filters_of_a_call_abandoned_at_close_get_their_end() {
    /// Keeps a call's answer, unread, in the request.
    struct Stash;

    #[async_trait]
    impl Plugin for Stash {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let kept = Kept(Arc::new(Mutex::new(Some(cx.call("backup").await?))));
            cx.extensions_mut().insert(kept);
            Ok(None)
        }
    }

    /// Notes how each call to `backup` ended, as its filter learns it.
    #[derive(Clone, Default)]
    struct Note(Arc<Mutex<Vec<String>>>);

    struct Noted(Arc<Mutex<Vec<String>>>);

    impl Filter<Reply> for Noted {
        fn edit(&mut self, event: Reply, out: &mut Vec<Reply>) -> Result<(), Fault> {
            out.push(event);
            Ok(())
        }

        fn end(&mut self, end: &Result<(), Fault>, _out: &mut Vec<Reply>) -> Result<(), Fault> {
            let end = end.as_ref().err().map_or("ok".to_owned(), fault_code);
            self.0.lock().map(|mut n| n.push(end.to_owned())).ok();
            Ok(())
        }
    }

    #[async_trait]
    impl Plugin for Note {
        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            match px.provider() {
                "backup" => px.filter(Noted(self.0.clone())),
                _ => Ok(()),
            }
        }
    }

    let note = Note::default();
    let h = Harness::new(
        vec![Arc::new(note.clone()), Arc::new(Stash)],
        Some("openai"),
    );
    h.sites.backup.reply(fixture::sse(STREAM));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    assert_eq!(closes(&h, unary_chat()).await, StatusCode::OK);
    let noted = note.0.lock().map(|n| n.clone()).unwrap_or_default();
    assert_eq!(noted, ["abandoned"]);
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

impl Format<Incoming, Conversation, Reply> for Fragile {
    fn name(&self) -> &str {
        OpenAiChat.name()
    }
    fn matches(&self, io: &Incoming) -> bool {
        <OpenAiChat as Format<Incoming, Conversation, Reply>>::matches(&OpenAiChat, io)
    }
    fn bind(&self, io: Incoming) -> Result<Connection<Conversation, Reply>, Fault> {
        Ok(io.bind(
            |request| OpenAiChat.read_request(request),
            move |reply, source| {
                let mut head = reply.head();
                match OpenAiChat.response_encoder(&mut head, reply.request()) {
                    Some(encoder) => Ok(reply.write(head, Box::new(Garbling(encoder)))),
                    None => Ok(reply.empty(head, source)),
                }
            },
            |fault, request, head| OpenAiChat.refusal(fault, request, head),
        ))
    }
}

struct Garbling(Box<dyn Encoder<Reply>>);

impl Encoder<Reply> for Garbling {
    fn encode(&mut self, reply: &Reply, out: &mut bytes::BytesMut) -> Result<(), Fault> {
        if matches!(reply, Reply::Delta { delta, .. } if delta.as_str() == "bad") {
            out.extend_from_slice(b"GARBAGE");
            return Err(Fault::new("cannot write it")
                .with(ErrorCode("unwritable".into()))
                .with(Status(StatusCode::UNPROCESSABLE_ENTITY)));
        }
        self.0.encode(reply, out)
    }

    fn end(&mut self, end: &Result<(), Fault>, out: &mut bytes::BytesMut) -> Result<(), Fault> {
        self.0.end(end, out)
    }
}

/// A gateway serving `Fragile` to the providers of [`Harness`], answering
/// through `openai`.
fn fragile(sites: &Sites, plugins: Vec<Arc<dyn Plugin>>) -> Gateway {
    let list = [provider(&sites.openai, "openai", None)];
    let gw = providers(list).and_then(|g| g.client::<Conversation, _, _>(Fragile));
    let gw = gw.and_then(|g| with_plugins(g, plugins).target("openai"));
    gw.unwrap_or_else(|e| panic!("gateway: {e}"))
}

#[tokio::test]
async fn a_response_whose_last_bytes_were_read_ends_complete() {
    let h = relay();
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    let (_, mut body) = serve(&h.gw, unary_chat()).await.0.into_parts();
    let frame = http_body_util::BodyExt::frame(&mut body).await;
    let data = frame.and_then(|f| f.ok()?.into_data().ok());
    let data = data
        .map(|d| String::from_utf8_lossy(&d).into_owned())
        .unwrap_or_default();
    assert_eq!(gist(&data).message.text(), "hi");
    // The host stops at the last bytes, without polling for the end.
    drop(body);
    h.gw.idle().await;

    assert_eq!(says(h.log.only()), Ok(()));
}

/// Filters the response sent to the client through a filter that panics.
struct PanicFilter;

#[async_trait]
impl Plugin for PanicFilter {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.filter(|_: &mut Reply| -> Result<(), Fault> { panic!("a buggy filter") })?;
        Ok(None)
    }
}

#[tokio::test]
async fn a_panic_before_the_head_ends_the_call_that_answered_internal() {
    let bill = Bill::default();
    let chain: Vec<Arc<dyn Plugin>> = vec![Arc::new(bill.clone()), Arc::new(PanicFilter)];
    let h = Harness::new(chain, Some("openai"));
    h.sites.openai.reply(fixture::sse(STREAM));

    let (status, _, _) = h.send(chat()).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(code(&h.log.only()), "internal");
    // The call ends with the panic that refused its response.
    let ended = bill.ended();
    assert_eq!(code(&ended[0].1), "internal");
}

#[tokio::test]
async fn a_panic_before_any_bytes_is_refused() {
    /// Filters the provider's response through a filter that panics.
    struct Busy;

    #[async_trait]
    impl Plugin for Busy {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(|_: &mut Reply| -> Result<(), Fault> { panic!("a buggy filter") })?;
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Busy)], Some("openai"));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    let (status, body) = h.text(unary_chat()).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(body.contains("internal"), "{body}");
    assert_eq!(code(&h.log.only()), "internal");
}

#[tokio::test]
async fn a_panic_after_bytes_went_out_keeps_those_written_before_it() {
    let h = Harness::new(
        vec![Arc::new(Guard(|e| {
            if matches!(e, Reply::Delta { delta, .. } if delta.as_str() == "boom") {
                panic!("a buggy guard");
            }
            false
        }))],
        Some("openai"),
    );
    let both = format!("{}{}", text_chunk(" world"), text_chunk("boom"));
    h.sites
        .openai
        .reply(fixture::sse(&[HI, &both, "data: [DONE]\n\n"]));

    let (text, fault) = frames(serve(&h.gw, chat()).await.0).await;
    h.gw.idle().await;

    // What was written before the panic in the same batch is sent, and the
    // panic, an internal fault like a filter's `Err`, is written after it.
    assert!(
        text.contains("\"Hi\"") && text.contains("\" world\""),
        "{text}"
    );
    assert!(text.contains("internal"), "{text}");
    assert!(fault.is_none());
}

#[tokio::test]
async fn a_failed_encode_writes_nothing() {
    let sites = Sites::default();
    let gw = fragile(&sites, Vec::new());
    let stream = [HI, &text_chunk("bad"), "data: [DONE]\n\n"];
    sites.openai.reply(fixture::sse(&stream));

    let (_, _, body) = read_whole(serve(&gw, chat()).await.0).await;

    let body = String::from_utf8_lossy(&body.unwrap_or_default()).into_owned();
    assert!(
        body.contains("\"Hi\"") && body.contains("unwritable"),
        "{body}"
    );
    assert!(!body.contains("GARBAGE"), "{body}");
}

#[tokio::test]
async fn an_empty_stream_of_another_kind_is_refused() {
    struct Wrong;

    #[async_trait]
    impl Plugin for Wrong {
        async fn on_client(&self, _cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let none = futures::stream::empty::<Result<String, Fault>>();
            Ok(Some(Response::stream(none)))
        }
    }

    let h = Harness::new(vec![Arc::new(Wrong)], None);

    let (status, body) = h.text(unary_chat()).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(body.contains("internal"), "{body}");
    assert_eq!(code(&h.log.only()), "internal");
}

#[tokio::test]
async fn a_client_edge_failure_ends_the_call_however_its_bytes_were_split() {
    let world = text_chunk(" world");
    let done = "data: [DONE]\n\n";
    let together = format!("{world}{done}");
    // r7, as N5 made it: a call settles at the end of its body, so the
    // failure that stopped its reader ends it, however the bytes fell.
    let each = [
        (vec![HI, &*world, done], "blocked"),
        (vec![HI, &*together], "blocked"),
    ];
    for (chunks, want) in each {
        let bill = Bill::default();
        let guard =
            Guard(|e| matches!(e, Reply::Delta { delta, .. } if delta.as_str().contains("world")));
        let h = Harness::new(
            vec![Arc::new(bill.clone()), Arc::new(guard)],
            Some("openai"),
        );
        h.sites.openai.reply(fixture::sse(&chunks));

        let body = h.body(chat()).await;

        assert!(body.contains("blocked"), "{body}");
        let ended = bill.ended();
        let end = ended[0]
            .1
            .as_ref()
            .err()
            .map_or("ok".to_owned(), fault_code);
        assert_eq!(end, want, "{chunks:?}");
    }
}

// A translated head keeps no status; a 304 is neither a success
// nor an error, so a failure, in the client's words.
#[tokio::test]
async fn a_translated_304_is_a_bad_gateway() {
    let sites = Sites::default();
    let p = add_provider("anthropic", AnthropicMessages, sites.claude.plain());
    let gw = providers([p]).and_then(|g| {
        g.client::<Conversation, _, _>(OpenAiChat)?
            .target("anthropic")
    });
    let gw = gw.unwrap_or_else(|e| panic!("gateway: {e}"));
    let not_modified = http::Response::builder().status(StatusCode::NOT_MODIFIED);
    sites
        .claude
        .reply(not_modified.body(Vec::new()).unwrap_or_default());
    let req = call(
        CHAT,
        r#"{"model":"claude-x","messages":[{"role":"user","content":"hi"}]}"#,
    );

    let (status, _, body) = read_whole(serve(&gw, req).await.0).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    let body = String::from_utf8_lossy(&body.unwrap_or_default()).into_owned();
    assert!(body.contains("\"code\":\"upstream\""), "{body}");
}

/// An OpenAI client's gateway whose target is an Anthropic provider, with
/// `relay` registered.
fn translating(sites: &Sites, relay: explicit::Relay) -> Gateway {
    let p = add_provider("anthropic", AnthropicMessages, sites.claude.plain());
    let gw = providers([p]).and_then(|g| {
        g.client::<Conversation, _, _>(OpenAiChat)?
            .plugin(relay)
            .target("anthropic")
    });
    gw.unwrap_or_else(|e| panic!("gateway: {e}"))
}

// A raw relay sends the client's bytes and key; to another vendor it
// would send both, so it is refused before anything is sent.
#[tokio::test]
async fn a_raw_relay_never_reaches_a_provider_of_another_format() {
    let sites = Sites::default();
    let gw = translating(&sites, explicit::Relay::RAW);
    let mut req = call(
        CHAT,
        r#"{"model":"claude-x","messages":[{"role":"user","content":"hi"}]}"#,
    );
    req.headers_mut().insert(
        http::header::AUTHORIZATION,
        HeaderValue::from_static("Bearer client-key"),
    );

    let (status, _, _) = read_whole(serve(&gw, req).await.0).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(sites.sent.sent().is_empty(), "nothing reached the provider");
}

// Another vendor's head and error body are not the client's protocol: the
// client gets its own format's error.
#[tokio::test]
async fn relayed_heads_and_errors_stay_within_one_format() {
    let sites = Sites::default();
    let gw = translating(&sites, explicit::Relay::HEADS);
    let raw = r#"{"type":"error","error":{"type":"invalid_request_error","message":"bad"}}"#;
    let mut refused = fixture::json(StatusCode::BAD_REQUEST, raw);
    refused
        .headers_mut()
        .insert("anthropic-organization-id", HeaderValue::from_static("org"));
    sites.claude.reply(refused);
    let req = call(
        CHAT,
        r#"{"model":"claude-x","messages":[{"role":"user","content":"hi"}]}"#,
    );

    let (_, headers, body) = read_whole(serve(&gw, req).await.0).await;

    let body = String::from_utf8_lossy(&body.unwrap_or_default()).into_owned();
    assert_ne!(body, raw, "the provider's body was relayed raw");
    let error: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
    assert!(error.get("type").is_none(), "{body}");
    assert!(error["error"]["message"].is_string(), "{body}");
    assert!(!headers.contains_key("anthropic-organization-id"));
}

#[tokio::test]
async fn a_raw_relay_sends_an_unedited_request_as_it_came() {
    let h = Harness::new(vec![Arc::new(explicit::Relay::RAW)], Some("openai"));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    let (status, _, _) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(&h.sites.sent.sent()[0].body()[..], UNARY_REQ.as_bytes());
}

#[tokio::test]
async fn idle_waits_for_a_response_still_streaming() {
    let h = relay();
    h.sites.openai.reply(fixture::sse(STREAM));

    let (resp, _) = serve(&h.gw, chat()).await;
    let waited = tokio::time::timeout(Duration::from_millis(50), h.gw.idle()).await;
    assert!(waited.is_err(), "idle returned with a response unread");

    let _ = read_whole(resp).await;
    h.gw.idle().await;
    assert_eq!(says(h.log.only()), Ok(()));
}

#[tokio::test]
async fn a_gateway_changed_after_cloning_waits_only_for_its_own_requests() {
    let h = Harness::new(vec![Arc::new(Hang(Outcomes::default()))], Some("openai"));
    let changed = h.gw.clone().plugin(Outcomes::default());

    // A request of the original hangs in its plugin.
    let pending = tokio::spawn({
        let gw = h.gw.clone();
        async move { serve(&gw, unary_chat()).await }
    });
    tokio::time::sleep(Duration::from_millis(10)).await;

    let own = tokio::time::timeout(Duration::from_millis(50), h.gw.idle()).await;
    let other = tokio::time::timeout(Duration::from_millis(50), changed.idle()).await;
    assert!(own.is_err() && other.is_ok());
    pending.abort();
}

#[tokio::test]
async fn a_body_that_broke_off_is_refused_without_its_model_read() {
    let sites = Sites::default();
    let gw = fragile(&sites, Vec::new());
    let too_large = Fault::new("too large")
        .with(ErrorCode("too_large".into()))
        .with(Status(StatusCode::PAYLOAD_TOO_LARGE));
    let broken = futures::stream::iter([Err::<http_body::Frame<bytes::Bytes>, _>(too_large)]);
    let mut req = http::Request::new(http_body_util::StreamBody::new(broken));
    *req.method_mut() = http::Method::POST;
    *req.uri_mut() = http::Uri::from_static(CHAT);

    let (resp, ended) = serve(&gw, req).await;
    let (status, headers, _) = read_whole(resp).await;

    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(headers["content-type"], "application/json");
    let end = ended.await.map_err(|f| fault_code(&f));
    assert_eq!(end, Err("too_large".to_owned()));
}

// A call a plugin reads is metered on its own provider edge, as the
// target's is.
#[tokio::test]
async fn a_call_read_in_on_client_is_metered_like_the_targets() {
    /// Reads a call's response, then passes.
    #[derive(Clone, Default)]
    struct Read(Arc<Mutex<Option<Fault>>>);

    #[async_trait]
    impl Plugin for Read {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let read = cx.call("openai").await?;
            let fault = read.outputs::<Reply>().try_collect::<Vec<_>>().await.err();
            self.0.lock().map(|mut f| *f = fault).ok();
            Ok(None)
        }
    }

    let read = Read::default();
    let h = Harness::new(vec![Arc::new(read.clone())], Some("backup"));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));
    h.sites.backup.reply(fixture::json(StatusCode::OK, UNARY));

    assert_eq!(h.text(unary_chat()).await.0, StatusCode::OK);

    let fault = read.0.lock().ok().and_then(|mut f| f.take());
    assert!(fault.is_none());
    let calls = metered(&h);
    let calls: Vec<_> = calls.iter().map(|(c, n)| (c.as_str(), *n)).collect();
    assert_eq!(calls, [("backup/gpt-x", 4), ("openai/gpt-x", 4)]);
}

#[tokio::test]
async fn a_panicking_call_leaves_its_sibling_abandoned() {
    struct Never;

    impl Never {
        pub async fn send(
            &self,
            _req: http::Request<Body>,
        ) -> Result<(http::Response<Body>, Option<Completion>), Fault> {
            std::future::pending().await
        }
    }
    impl From<Never> for SendRequest {
        fn from(value: Never) -> Self {
            let value = std::sync::Arc::new(value);
            sender(move |call| {
                let value = value.clone();
                async move { value.send(call).await }
            })
        }
    }

    /// Panics sending.
    struct Boom;

    impl Boom {
        pub async fn send(
            &self,
            _req: http::Request<Body>,
        ) -> Result<(http::Response<Body>, Option<Completion>), Fault> {
            panic!("a buggy transport")
        }
    }
    impl From<Boom> for SendRequest {
        fn from(value: Boom) -> Self {
            let value = std::sync::Arc::new(value);
            sender(move |call| {
                let value = value.clone();
                async move { value.send(call).await }
            })
        }
    }

    /// Calls both at once.
    struct Race;

    #[async_trait]
    impl Plugin for Race {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let (never, boom) = (cx.call("never"), cx.call("boom"));
            let _ = futures::join!(never, boom);
            Ok(None)
        }
    }

    let ends = Ends::default();
    let list = [
        add_provider("never", OpenAiChat, Never),
        add_provider("boom", OpenAiChat, Boom),
    ];
    let plugins: Vec<Arc<dyn Plugin>> = vec![Arc::new(ends.clone()), Arc::new(Race)];
    let gw = gateway(list, plugins, None).unwrap_or_else(|e| panic!("gateway: {e}"));

    let (status, _, _) = read_whole(serve(&gw, unary_chat()).await.0).await;
    gw.idle().await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let mut all = ends.all();
    all.sort();
    assert_eq!(all, ["boom internal", "client", "never abandoned"]);
}

#[tokio::test]
async fn a_client_leaving_mid_stream_ends_the_call_and_its_filters() {
    /// Notes each provider-edge filter end.
    #[derive(Clone, Default)]
    struct Note(Arc<Mutex<Vec<String>>>);

    struct Noted(Arc<Mutex<Vec<String>>>);

    impl Filter<Reply> for Noted {
        fn edit(&mut self, event: Reply, out: &mut Vec<Reply>) -> Result<(), Fault> {
            out.push(event);
            Ok(())
        }

        fn end(&mut self, end: &Result<(), Fault>, _out: &mut Vec<Reply>) -> Result<(), Fault> {
            let end = code(end).to_owned();
            self.0.lock().map(|mut n| n.push(end)).ok();
            Ok(())
        }
    }

    #[async_trait]
    impl Plugin for Note {
        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            px.filter(Noted(self.0.clone()))
        }
    }

    let (note, ends) = (Note::default(), Ends::default());
    let h = Harness::new(
        vec![Arc::new(note.clone()), Arc::new(ends.clone())],
        Some("openai"),
    );
    let world = text_chunk(" world");
    h.sites
        .openai
        .reply(fixture::sse(&[HI, &world, "data: [DONE]\n\n"]));

    let (_, mut body) = serve(&h.gw, chat()).await.0.into_parts();
    let _ = http_body_util::BodyExt::frame(&mut body).await;
    drop(body);
    h.gw.idle().await;

    let noted = note.0.lock().map(|n| n.clone()).unwrap_or_default();
    assert_eq!(noted, ["abandoned"]);
    assert_eq!(ends.all(), ["openai abandoned", "client"]);
}

#[tokio::test]
async fn a_panic_reading_a_calls_events_ends_the_call_internal() {
    /// Reads a judge's verdict through a provider-edge filter that panics.
    struct Judge;

    #[async_trait]
    impl Plugin for Judge {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let verdict = cx.call("backup").await?;
            verdict.outputs::<Reply>().try_collect::<Vec<_>>().await?;
            Ok(None)
        }
    }

    let buggy = OnProvider(|px| {
        px.filter(|_: &mut Reply| -> Result<(), Fault> { panic!("a buggy filter") })
    });
    let ends = Ends::default();
    let plugins: Vec<Arc<dyn Plugin>> =
        vec![Arc::new(ends.clone()), Arc::new(buggy), Arc::new(Judge)];
    let h = Harness::new(plugins, Some("openai"));
    h.sites.backup.reply(fixture::json(StatusCode::OK, UNARY));

    let (status, _) = h.text(unary_chat()).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(ends.all(), ["backup internal", "client"]);
}

#[tokio::test]
async fn a_fallback_reads_a_call_to_its_first_content() {
    /// Sends `openai`'s answer if it has content, else `backup`'s.
    struct Fallback;

    #[async_trait]
    impl Plugin for Fallback {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let mut first = cx.call("openai").await?;
            if first.buffer_until(Reply::is_content).await.is_ok() {
                return Ok(Some(first));
            }
            drop(first);
            let mut backup = cx.call("backup").await?;
            backup.buffer_until(Reply::is_content).await?;
            Ok(Some(backup))
        }
    }

    let ends = Ends::default();
    let h = Harness::new(vec![Arc::new(ends.clone()), Arc::new(Fallback)], None);
    // A success that fails before any content.
    let overloaded = "data: {\"error\":{\"message\":\"Overloaded\",\"code\":\"overloaded\"}}\n\n";
    h.sites.openai.reply(fixture::sse(&[overloaded]));
    h.sites.backup.reply(fixture::sse(STREAM));

    let body = h.body(chat()).await;

    // What `until_content` read is sent, with the rest of the answer.
    assert_eq!(gist(&body).message, gist(&STREAM.concat()).message);
    assert_eq!(ends.all(), ["openai overloaded", "backup ok", "client"]);
}

#[tokio::test]
async fn a_whole_body_call_ends_only_once_its_until_content_response_is_read() {
    /// Returns `openai`'s answer read to its content; its provider-edge filter notes its end.
    #[derive(Clone, Default)]
    struct Returns(Arc<Mutex<Vec<&'static str>>>);

    impl Returns {
        fn note(&self, what: &'static str) {
            self.0.lock().map(|mut v| v.push(what)).ok();
        }
    }

    impl Filter<Reply> for Returns {
        fn edit(&mut self, reply: Reply, out: &mut Vec<Reply>) -> Result<(), Fault> {
            out.push(reply);
            Ok(())
        }

        fn end(&mut self, _end: &Result<(), Fault>, _out: &mut Vec<Reply>) -> Result<(), Fault> {
            self.note("filter ended");
            Ok(())
        }
    }

    #[async_trait]
    impl Plugin for Returns {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let mut resp = cx.call("openai").await?;
            resp.buffer_until(Reply::is_content).await?;
            self.note("returned");
            Ok(Some(resp))
        }

        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            px.filter(self.clone())
        }

        async fn on_provider_end(&self, _: &ProviderContext, _: &Result<(), Fault>) {
            self.note("call ended");
        }
    }

    let (ends, returns) = (Ends::default(), Returns::default());
    let h = Harness::new(
        vec![Arc::new(ends.clone()), Arc::new(returns.clone())],
        None,
    );
    // A whole body: its content and its end come in one batch.
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    let (status, body) = h.text(unary_chat()).await;

    assert_eq!(status, StatusCode::OK);
    let said = gist(&body);
    assert_eq!(said.message.text(), "hi");
    assert_eq!(said.usage.get(units::INPUT_TOKENS), Some(4));
    let order = returns.0.lock().map(|v| v.clone()).unwrap_or_default();
    assert_eq!(order, ["filter ended", "returned", "call ended"]);
    assert_eq!(ends.all(), ["openai ok", "client"]);
}

#[tokio::test]
async fn a_plugin_answers_with_its_calls_events_read_as_they_are_sent() {
    /// Answers with one call's outputs, then another's.
    struct Merge;

    #[async_trait]
    impl Plugin for Merge {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let first = cx.call("openai").await?.outputs::<Reply>();
            // The second answer's blocks follow the first's, as one response.
            let second = cx.call("backup").await?.outputs::<Reply>();
            let after = |reply: Reply| {
                let block = |block: u32| block + 1;
                futures::future::ready(Ok(match reply {
                    Reply::Start(_) => None,
                    Reply::BlockStart { block: b, kind } => {
                        Some(Reply::block_start(block(b), kind))
                    }
                    Reply::Delta { block: b, delta } => Some(Reply::Delta {
                        block: block(b),
                        delta,
                    }),
                    Reply::BlockEnd { block: b } => Some(Reply::block_end(block(b))),
                    other => Some(other),
                }))
            };
            let second = futures::TryStreamExt::try_filter_map(second, after);
            let events = futures::StreamExt::chain(first, second);
            Ok(Some(Response::stream(events)))
        }
    }

    let ends = Ends::default();
    let h = Harness::new(vec![Arc::new(ends.clone()), Arc::new(Merge)], None);
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));
    h.sites.backup.reply(fixture::json(StatusCode::OK, UNARY));

    let (status, body) = h.text(unary_chat()).await;

    assert_eq!(status, StatusCode::OK);
    let body: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
    assert_eq!(body["choices"][0]["message"]["content"], "hihi");
    assert_eq!(ends.all(), ["openai ok", "backup ok", "client"]);
}

/// Sends `openai`'s answer if it has content, else `backup`'s.
struct FirstContent;

#[async_trait]
impl Plugin for FirstContent {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let mut first = cx.call("openai").await?;
        if first.buffer_until(Reply::is_content).await.is_ok() {
            return Ok(Some(first));
        }
        drop(first);
        Ok(Some(cx.call("backup").await?))
    }
}

#[tokio::test]
async fn a_fallback_skips_a_success_that_fails_after_only_its_start() {
    let ends = Ends::default();
    let h = Harness::new(vec![Arc::new(ends.clone()), Arc::new(FirstContent)], None);
    let role = "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"}}]}\n\n";
    let overloaded = "data: {\"error\":{\"message\":\"Overloaded\",\"code\":\"overloaded\"}}\n\n";
    h.sites.openai.reply(fixture::sse(&[role, overloaded]));
    h.sites.backup.reply(fixture::sse(STREAM));

    let body = h.body(chat()).await;

    assert_eq!(gist(&body).message, gist(&STREAM.concat()).message);
    assert_eq!(ends.all(), ["openai overloaded", "backup ok", "client"]);
}

#[tokio::test]
async fn buffer_until_judges_provider_outputs_after_filters() {
    /// Takes every output of `openai`'s response.
    struct Mute;

    impl Filter<Reply> for Mute {
        fn edit(&mut self, _output: Reply, _out: &mut Vec<Reply>) -> Result<(), Fault> {
            Ok(())
        }
    }

    let mute = OnProvider(|px| match px.provider() {
        "openai" => px.filter(Mute),
        _ => Ok(()),
    });
    let ends = Ends::default();
    let chain: Vec<Arc<dyn Plugin>> = vec![
        Arc::new(ends.clone()),
        Arc::new(mute),
        Arc::new(FirstContent),
    ];
    let h = Harness::new(chain, None);
    let overloaded = "data: {\"error\":{\"message\":\"Overloaded\",\"code\":\"overloaded\"}}\n\n";
    h.sites.openai.reply(fixture::sse(&[HI, overloaded]));
    h.sites.backup.reply(fixture::sse(STREAM));

    let (status, _) = h.text(chat()).await;

    // The filter removed the qualifying events, so the policy falls back.
    assert_eq!(status, StatusCode::OK);
    assert_eq!(ends.all(), ["openai overloaded", "backup ok", "client"]);
    assert_eq!(h.sites.sent.sent().len(), 2);
}

#[tokio::test]
async fn buffer_until_rechecks_filtered_values_with_a_new_predicate() {
    /// Replaces every output with a non-content event.
    struct Mute;

    impl Filter<Reply> for Mute {
        fn edit(&mut self, _output: Reply, out: &mut Vec<Reply>) -> Result<(), Fault> {
            out.push(Reply::usage(Usage::default()));
            Ok(())
        }
    }

    /// Readies `openai`'s response twice, falling back if the second fails.
    struct UntilContentTwice;

    #[async_trait]
    impl Plugin for UntilContentTwice {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let mut first = cx.call("openai").await?;
            first.buffer_until(|_: &Reply| true).await?;
            match first.buffer_until(Reply::is_content).await {
                Ok(()) => Ok(Some(first)),
                Err(_) => {
                    drop(first);
                    Ok(Some(cx.call("backup").await?))
                }
            }
        }
    }

    let mute = OnProvider(|px| match px.provider() {
        "openai" => px.filter(Mute),
        _ => Ok(()),
    });
    let ends = Ends::default();
    let chain: Vec<Arc<dyn Plugin>> = vec![
        Arc::new(ends.clone()),
        Arc::new(mute),
        Arc::new(UntilContentTwice),
    ];
    let h = Harness::new(chain, None);
    let overloaded = "data: {\"error\":{\"message\":\"Overloaded\",\"code\":\"overloaded\"}}\n\n";
    h.sites.openai.reply(fixture::sse(&[HI, overloaded]));
    h.sites.backup.reply(fixture::sse(STREAM));

    let (status, _) = h.text(chat()).await;

    // The second predicate rejects the buffered values and finds the failure.
    assert_eq!(status, StatusCode::OK);
    assert_eq!(ends.all(), ["openai overloaded", "backup ok", "client"]);
    assert_eq!(h.sites.sent.sent().len(), 2);
}

#[tokio::test]
async fn a_provider_hook_that_fails_refuses_the_call() {
    let unsigned = OnProvider(|_| {
        Err(Fault::new("no key")
            .with(ErrorCode("unsigned".into()))
            .with(Status(StatusCode::FORBIDDEN)))
    });
    let h = Harness::new(vec![Arc::new(unsigned)], Some("openai"));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    let (status, body) = h.text(unary_chat()).await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(body.contains("unsigned"), "{body}");
    assert!(h.sites.sent.sent().is_empty());
}

#[tokio::test]
async fn a_dropped_response_runs_its_provider_filters_end() {
    /// Records each provider-edge filter's `end`, by provider.
    #[derive(Clone, Default)]
    struct FilterEnds(Arc<Mutex<Vec<String>>>);

    struct NoteEnd(FilterEnds, String);

    impl Filter<Reply> for NoteEnd {
        fn edit(&mut self, output: Reply, out: &mut Vec<Reply>) -> Result<(), Fault> {
            out.push(output);
            Ok(())
        }

        fn end(&mut self, end: &Result<(), Fault>, _out: &mut Vec<Reply>) -> Result<(), Fault> {
            let end = format!("{} {}", self.1, code(end));
            self.0.0.lock().map(|mut e| e.push(end)).ok();
            Ok(())
        }
    }

    #[async_trait]
    impl Plugin for FilterEnds {
        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            let provider = px.provider().to_owned();
            px.filter(NoteEnd(self.clone(), provider))
        }
    }

    /// Reads `openai` to its first content, then prefers `backup` anyway.
    struct Hedge;

    #[async_trait]
    impl Plugin for Hedge {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let mut first = cx.call("openai").await?;
            first.buffer_until(Reply::is_content).await?;
            drop(first);
            Ok(Some(cx.call("backup").await?))
        }
    }

    let ends = FilterEnds::default();
    let h = Harness::new(vec![Arc::new(ends.clone()), Arc::new(Hedge)], None);
    h.sites.openai.reply(fixture::sse(STREAM));
    h.sites.backup.reply(fixture::sse(STREAM));

    let _ = h.body(chat()).await;

    let got = ends.0.lock().map(|e| e.clone()).unwrap_or_default();
    assert_eq!(got, ["openai abandoned", "backup ok"]);
}

#[tokio::test]
async fn a_205_is_sent_with_no_body() {
    let h = Harness::new(vec![Arc::new(explicit::Relay::HEADS)], Some("openai"));
    // A provider that writes a body under a 205 anyway.
    h.sites
        .openai
        .reply(fixture::json(StatusCode::RESET_CONTENT, UNARY));

    let (status, _, body) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::RESET_CONTENT);
    assert_eq!(body.ok(), Some(bytes::Bytes::new()));
    assert_eq!(says(h.log.only()), Ok(()));
}

// A call's usage is metered on its provider edge, as decoded.
#[tokio::test]
async fn a_calls_usage_is_metered_on_its_provider_edge() {
    let h = Harness::new(Vec::new(), Some("openai"));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    assert_eq!(h.text(unary_chat()).await.0, StatusCode::OK);

    assert_eq!(h.usage.total().get(units::INPUT_TOKENS), Some(4));
}

#[tokio::test]
async fn a_plugins_own_response_sends_its_head_at_its_first_byte() {
    /// Answers with a stream that has nothing yet, or opens with a first
    /// output, as a stream that waits on the client does.
    struct Waiting(bool);

    #[async_trait]
    impl Plugin for Waiting {
        async fn on_client(&self, _cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let opens = self.0.then(|| text(0, "Hi")).into_iter().flatten().map(Ok);
            let pending = futures::stream::pending::<Result<Reply, Fault>>();
            let quiet = futures::StreamExt::chain(futures::stream::iter(opens), pending);
            Ok(Some(Response::stream(quiet)))
        }
    }

    /// Answers with a stream that fails before any output.
    struct Failing;

    #[async_trait]
    impl Plugin for Failing {
        async fn on_client(&self, _cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let fails = [Err::<Reply, _>(
                Fault::new("no")
                    .with(ErrorCode("no".into()))
                    .with(Status(StatusCode::FORBIDDEN)),
            )];
            Ok(Some(Response::stream(futures::stream::iter(fails))))
        }
    }

    let h = Harness::new(vec![Arc::new(Waiting(false))], None);
    let handled = tokio::time::timeout(Duration::from_millis(100), serve(&h.gw, chat())).await;
    assert!(handled.is_err(), "the head went out before a first byte");
    let h = Harness::new(vec![Arc::new(Waiting(true))], None);
    let handled = tokio::time::timeout(Duration::from_secs(5), serve(&h.gw, chat())).await;
    let (resp, _) = handled.unwrap_or_else(|_| panic!("the head waited past its first byte"));
    assert_eq!(resp.status(), StatusCode::OK);

    // A failure before its first byte is refused with the fault's status.
    let h = Harness::new(vec![Arc::new(Failing)], None);
    let (status, body) = h.text(chat()).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(body.contains(r#""code":"no""#), "{body}");

    /// Answers from a cache, its outputs at hand.
    struct Cached;

    #[async_trait]
    impl Plugin for Cached {
        async fn on_client(&self, _cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            // Its first output, which the guard sees before anything is written.
            let cached = [Ok(Reply::text(0, "world"))];
            Ok(Some(Response::stream(futures::stream::iter(cached))))
        }
    }

    // So a guard refuses a cached answer as it refuses a provider's.
    let guard =
        Guard(|e| matches!(e, Reply::Delta { delta, .. } if delta.as_str().contains("world")));
    let h = Harness::new(vec![Arc::new(guard), Arc::new(Cached)], None);
    let (status, body) = h.text(chat()).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(body.contains("blocked"), "{body}");
}

#[tokio::test]
async fn a_call_with_its_own_input_names_only_its_own_model() {
    /// Calls with its own input, with a model and without.
    struct Own;

    #[async_trait]
    impl Plugin for Own {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let chat = || Conversation::new([Message::new(Role::User, "judge")]);
            let _ = cx.call("openai").with_input(chat()).await?;
            let _ = cx
                .call("openai")
                .with_input(judged(chat(), "judge"))
                .await?;
            Ok(Some(cx.call("openai").await?))
        }
    }

    let h = Harness::new(vec![Arc::new(Own)], None);
    for _ in 0..3 {
        h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));
    }

    assert_eq!(h.text(unary_chat()).await.0, StatusCode::OK);

    let models: Vec<serde_json::Value> = h
        .sites
        .sent
        .sent()
        .iter()
        .map(|r| serde_json::from_slice::<serde_json::Value>(r.body()).unwrap_or_default())
        .map(|body| body["model"].clone())
        .collect();
    assert_eq!(models, [json!(null), json!("judge"), json!("gpt-x")]);
}

#[tokio::test]
async fn a_failed_input_filter_fails_the_input_for_every_later_reader() {
    /// Redacts, then fails a second filter and ignores its `Err`.
    struct Careless;

    #[async_trait]
    impl Plugin for Careless {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(|chat: &mut Conversation| {
                chat.temperature = Some(0.0);
                Ok(())
            })?;
            let fails = |_: &mut Conversation| {
                Err(Fault::new("no")
                    .with(ErrorCode("no".into()))
                    .with(Status(StatusCode::FORBIDDEN)))
            };
            let _ = cx.filter(fails);
            let read = cx.input::<Conversation>().err().map(|f| fault_code(&f));
            assert_eq!(read.as_deref(), Some("no"));
            Ok(None)
        }
    }

    /// Drops the input.
    struct Gone;

    impl Filter<Conversation> for Gone {
        fn edit(&mut self, _chat: Conversation, _out: &mut Vec<Conversation>) -> Result<(), Fault> {
            Ok(())
        }
    }

    /// Refuses by dropping the input, which only `Err` may do.
    struct Dropping;

    #[async_trait]
    impl Plugin for Dropping {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(Gone)?;
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Careless)], Some("openai"));
    // The earlier edit is not sent without the failed one: nothing is.
    assert_eq!(h.text(unary_chat()).await.0, StatusCode::FORBIDDEN);
    assert!(h.sites.sent.sent().is_empty());

    let h = Harness::new(vec![Arc::new(Dropping)], Some("openai"));
    assert_eq!(
        h.text(unary_chat()).await.0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(code(&h.log.only()), "internal");
    assert!(h.sites.sent.sent().is_empty());
}

#[tokio::test]
async fn a_call_is_a_copy_sent_until_the_request_ends() {
    /// Sends a clone of a sent call, then keeps a call for after the end.
    #[derive(Clone, Default)]
    struct Keep(Arc<Mutex<Option<respan_gateway_sdk::Call>>>);

    #[async_trait]
    impl Plugin for Keep {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let call = cx.call("openai");
            let copy = call.clone();
            let _ = call.await?;
            self.0.lock().map(|mut k| *k = Some(copy.clone())).ok();
            Ok(Some(copy.await?))
        }
    }

    let keep = Keep::default();
    let h = Harness::new(vec![Arc::new(keep.clone())], None);
    for _ in 0..2 {
        h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));
    }

    assert_eq!(h.text(unary_chat()).await.0, StatusCode::OK);
    assert_eq!(h.sites.sent.sent().len(), 2);

    let late = keep.0.lock().ok().and_then(|mut k| k.take());
    let late = late.unwrap_or_else(|| panic!("no call kept"));
    let refused = late.await.err().map(|f| fault_code(&f));
    assert_eq!(refused.as_deref(), Some("internal"));
    assert_eq!(h.sites.sent.sent().len(), 2);
}

/// Passes every reply; fails at its end.
struct FailsAtEnd;

impl Filter<Reply> for FailsAtEnd {
    fn edit(&mut self, reply: Reply, out: &mut Vec<Reply>) -> Result<(), Fault> {
        out.push(reply);
        Ok(())
    }

    fn end(&mut self, _end: &Result<(), Fault>, _out: &mut Vec<Reply>) -> Result<(), Fault> {
        Err(Fault::internal("fails at end"))
    }
}

// A meter registered last sees each reply first, so a filter keeping all
// back and another failing at its end hide nothing from it.
#[tokio::test]
async fn a_meter_sees_usage_though_a_filter_keeps_all_back_and_another_fails_at_its_end() {
    struct Both;

    #[async_trait]
    impl Plugin for Both {
        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            px.filter(HoldAll(Vec::new()))?;
            px.filter(FailsAtEnd)
        }
    }

    let h = Harness::new(vec![Arc::new(Both)], Some("openai"));
    h.sites.openai.reply(fixture::sse(STREAM));
    let _ = h.send(chat()).await;
    assert_eq!(code(&h.calls.all()[0]), "internal");
    assert_eq!(h.usage.total().get(units::INPUT_TOKENS), Some(5));
}

#[tokio::test]
async fn a_calls_response_read_to_its_end_then_dropped_ends_ok() {
    /// Reads a call's whole response with `until_content`, then drops it.
    struct ContentThenDrop;

    #[async_trait]
    impl Plugin for ContentThenDrop {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let mut response = cx.call("openai").await?;
            response.buffer_until(Reply::is_content).await?;
            drop(response);
            Err(Fault::not_found("done"))
        }
    }

    let fake = FakeOpenAi::start().await;
    let h = Harness::real(&fake, vec![Arc::new(ContentThenDrop)], None);
    fake.answer(Answer::raw(UNARY));
    let _ = h.send(valid_unary_chat()).await;

    let all = h.calls.all();
    assert_eq!(all.len(), 1);
    assert_eq!(says(all[0].clone()), Ok(()));
    assert_eq!(h.usage.all()[0].get(units::INPUT_TOKENS), Some(4));
}

// A meter registered last sees the usage in the message a filter fails on.
#[tokio::test]
async fn a_meter_sees_usage_a_filter_fails_on() {
    // The call reported 4 input tokens.
    let h = Harness::new(vec![Arc::new(ProviderCap)], Some("openai"));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));
    let _ = h.send(unary_chat()).await;
    assert_eq!(code(&h.calls.all()[0]), "cap");
    assert_eq!(h.usage.total().get(units::INPUT_TOKENS), Some(4));
}

fn over_the_cap(reply: &mut Reply) -> Result<(), Fault> {
    match reply {
        Reply::Usage(_) => Err(Fault::new("over the cap")
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

// A marked call's own filter, added in `on_provider`, goes with each copy
// sent, each its own, and each copy is metered.
#[tokio::test]
async fn a_calls_own_filter_is_each_copys_own() {
    #[derive(Clone)]
    struct Mark;

    /// Calls twice through clones of one marked call, reading each
    /// response, and notes how many replies each call's filter had seen.
    #[derive(Clone, Default)]
    struct Reader(Arc<Mutex<Vec<u32>>>);

    #[async_trait]
    impl Plugin for Reader {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let call = cx.call("openai").with_extension(Mark);
            for _ in 0..2 {
                let read = call.clone().await?.outputs::<Reply>();
                let _ = read.try_collect::<Vec<_>>().await?;
            }
            Err(Fault::not_found("done"))
        }

        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            if px.extensions().get::<Mark>().is_none() {
                return Ok(());
            }
            let (seen, mut count) = (self.0.clone(), 0);
            px.filter(move |_: &mut Reply| {
                count += 1;
                seen.lock().map(|mut s| s.push(count)).ok();
                Ok(())
            })
        }
    }

    let reader = Reader::default();
    let h = Harness::new(vec![Arc::new(reader.clone())], None);
    h.sites
        .openai
        .reply(fixture::sse(STREAM))
        .reply(fixture::sse(STREAM));
    let _ = h.send(chat()).await;

    let seen = reader.0.lock().map(|s| s.clone()).unwrap_or_default();
    let (first, second) = seen.split_at(seen.len() / 2);
    assert!(!first.is_empty() && first == second, "{seen:?}");
    assert_eq!(first[0], 1);
    assert_eq!(h.usage.total().get(units::INPUT_TOKENS), Some(10));
}

#[tokio::test]
async fn a_refusal_before_the_head_ends_a_plugins_response_filters() {
    /// Notes each end it gets.
    #[derive(Clone, Default)]
    struct Ends(Arc<Mutex<Vec<String>>>);

    impl Filter<Reply> for Ends {
        fn edit(&mut self, reply: Reply, out: &mut Vec<Reply>) -> Result<(), Fault> {
            out.push(reply);
            Ok(())
        }

        fn end(&mut self, end: &Result<(), Fault>, _out: &mut Vec<Reply>) -> Result<(), Fault> {
            let end = end
                .as_ref()
                .err()
                .map_or("ok".to_owned(), fault_code)
                .to_owned();
            self.0.lock().map(|mut e| e.push(end)).ok();
            Ok(())
        }
    }

    /// Filters its own answer.
    struct Answer(Ends);

    #[async_trait]
    impl Plugin for Answer {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(self.0.clone())?;
            let answer = futures::stream::iter(text(0, "hi").into_iter().map(Ok));
            Ok(Some(Response::stream(answer)))
        }
    }

    // The panicking filter, added first, runs after `Ends`.
    let ends = Ends::default();
    let h = Harness::new(
        vec![Arc::new(PanicFilter), Arc::new(Answer(ends.clone()))],
        None,
    );
    let (status, _, _) = h.send(chat()).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let ends = ends.0.lock().map(|e| e.clone()).unwrap_or_default();
    assert_eq!(ends, ["internal"]);
}

#[tokio::test]
async fn a_filter_added_after_a_call_is_made_misses_that_whole_calls_input() {
    struct FilterAfterCall;

    #[async_trait]
    impl Plugin for FilterAfterCall {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let call = cx.call("openai");
            cx.filter(|c: &mut Conversation| {
                c.temperature = Some(0.0);
                Ok(())
            })?;
            Ok(Some(call.await?))
        }
    }

    let h = Harness::new(vec![Arc::new(FilterAfterCall)], None);
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));
    let _ = h.send(unary_chat()).await;
    let sent: serde_json::Value =
        serde_json::from_slice(h.sites.sent.sent()[0].body()).unwrap_or_default();
    assert!(sent.get("model").is_some() && sent.get("temperature").is_none());
}

// ---- r6 internals probes ----

/// The same provider bytes and the same client-edge guard must give the
/// call the same end, however the transport split the bytes.
#[tokio::test]
async fn r6_a_calls_outcome_does_not_depend_on_how_its_bytes_were_split() {
    let guard = || -> Vec<Arc<dyn Plugin>> {
        vec![Arc::new(Guard(
            |e| matches!(e, Reply::Delta { delta, .. } if delta.as_str() == " world"),
        ))]
    };
    let end = |h: &Harness| {
        let all = h.calls.all();
        let o = &all[0];
        (
            o.as_ref().err().map(fault_code),
            h.usage.all()[0].get(units::OUTPUT_TOKENS),
        )
    };
    let split = Harness::new(guard(), Some("openai"));
    split.sites.openai.reply(fixture::sse(STREAM));
    let _ = split.send(chat()).await;

    let joined = Harness::new(guard(), Some("openai"));
    joined.sites.openai.reply(fixture::sse(&[&STREAM.concat()]));
    let _ = joined.send(chat()).await;

    // N5: a call settles at the end of its body, so both end as the guard
    // refused; each meter saw what was decoded, joined the usage that came
    // with the bytes refused, as provider-edge filters run first.
    assert_eq!(end(&split), (Some("blocked".to_owned()), None));
    assert_eq!(end(&joined), (Some("blocked".to_owned()), Some(2)));
}

/// A guard refuses a plugin's own answer that fails before its first byte,
/// whether or not the plugin's stream awaited before its first output.
#[tokio::test]
async fn r6_a_guard_refuses_a_plugins_answer_whether_or_not_it_awaited_first() {
    struct Answer(bool);

    #[async_trait]
    impl Plugin for Answer {
        async fn on_client(&self, _cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let waits = self.0;
            // Its first content, which the guard sees before anything is written.
            let answer = [Reply::start("cached", "gpt-x"), Reply::text(0, "bomb")];
            let answer = futures::StreamExt::then(
                futures::stream::iter(answer.map(Ok)),
                move |r| async move {
                    if waits {
                        // Such as a cache lookup or a call's bytes not yet here.
                        tokio::task::yield_now().await;
                    }
                    r
                },
            );
            Ok(Some(Response::stream(answer)))
        }
    }

    let status = |waits: bool| async move {
        let guard = Guard(|e| matches!(e, Reply::Delta { delta, .. } if delta.as_str() == "bomb"));
        let h = Harness::new(vec![Arc::new(guard), Arc::new(Answer(waits))], None);
        h.send(unary_chat()).await.0
    };
    assert_eq!(status(false).await, StatusCode::FORBIDDEN);
    assert_eq!(status(true).await, StatusCode::FORBIDDEN);
}

// ---- r6 fix pass ----

/// Usage in a message a provider-edge filter fails on reaches a meter
/// registered after it, unwritten.
#[tokio::test]
async fn usage_a_filter_fails_on_is_metered_not_written() {
    /// OpenAI Chat whose encoder notes the usage it is given.
    #[derive(Clone, Default)]
    struct Tally(Arc<Mutex<Usage>>);

    impl Format<Incoming, Conversation, Reply> for Tally {
        fn name(&self) -> &str {
            OpenAiChat.name()
        }
        fn matches(&self, io: &Incoming) -> bool {
            <OpenAiChat as Format<Incoming, Conversation, Reply>>::matches(&OpenAiChat, io)
        }
        fn bind(&self, io: Incoming) -> Result<Connection<Conversation, Reply>, Fault> {
            let tally = self.0.clone();
            Ok(io.bind(
                |request| OpenAiChat.read_request(request),
                move |reply, source| {
                    let mut head = reply.head();
                    match OpenAiChat.response_encoder(&mut head, reply.request()) {
                        Some(encoder) => {
                            Ok(reply.write(head, Box::new(Tallying(tally.clone(), encoder))))
                        }
                        None => Ok(reply.empty(head, source)),
                    }
                },
                |fault, request, head| OpenAiChat.refusal(fault, request, head),
            ))
        }
    }

    struct Tallying(Arc<Mutex<Usage>>, Box<dyn Encoder<Reply>>);

    impl Encoder<Reply> for Tallying {
        fn encode(&mut self, reply: &Reply, out: &mut bytes::BytesMut) -> Result<(), Fault> {
            if let Reply::Usage(usage) = reply {
                self.0.lock().map(|mut t| *t += usage).ok();
            }
            self.1.encode(reply, out)
        }

        fn end(&mut self, end: &Result<(), Fault>, out: &mut bytes::BytesMut) -> Result<(), Fault> {
            self.1.end(end, out)
        }
    }

    let (sites, calls, tally) = (Sites::default(), Calls::default(), Tally::default());
    let meter = Metered::default();
    let chain: Vec<Arc<dyn Plugin>> = vec![
        Arc::new(calls.clone()),
        Arc::new(ProviderCap),
        Arc::new(meter.clone()),
    ];
    let gw = providers([provider(&sites.openai, "openai", None)])
        .and_then(|g| g.client::<Conversation, _, _>(tally.clone()))
        .map(|g| with_plugins(g, chain))
        .and_then(|g| g.target("openai"))
        .unwrap_or_else(|e| panic!("gateway: {e}"));
    sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    let _ = read_whole(serve(&gw, unary_chat()).await.0).await;
    gw.idle().await;

    assert_eq!(code(&calls.all()[0]), "cap");
    assert_eq!(input_tokens(&meter.total()), 4);
    assert!(tally.0.lock().map(|t| t.is_empty()).unwrap_or_default());
}

/// `chat` asking for `model`.
fn judged(mut chat: Conversation, model: &str) -> Conversation {
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
            resp.buffer_until(Reply::is_content).await?;
        }
        Ok(Some(resp))
    }
}

// A provider's 429 reaches the client as it came, status, `Retry-After` and
// body, whether sent directly or through `until_content`.
#[tokio::test]
async fn r11_1_until_content_on_an_error_status_changes_what_the_client_gets() {
    let sent = Harness::new(vec![Arc::new(Answers(false))], None);
    sent.sites.openai.reply(limited());
    let (s1, h1, b1) = sent.send(unary_chat()).await;

    let readied = Harness::new(vec![Arc::new(Answers(true))], None);
    readied.sites.openai.reply(limited());
    let (s2, h2, b2) = readied.send(unary_chat()).await;

    assert_eq!(s1, s2);
    assert_eq!(h1.get("retry-after"), h2.get("retry-after"));
    assert_eq!(b1.ok(), b2.ok());
}

/// The fallback in ARCHITECTURE, verbatim.
struct ArchFallback(Vec<&'static str>);

#[async_trait]
impl Plugin for ArchFallback {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let mut last = Err(Upstream::fault("no provider to try"));
        for name in &self.0 {
            match cx.call(*name).await {
                Ok(mut resp) => {
                    let held = resp.buffer_until(Reply::is_content).await;
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

// The documented fallback turns every provider rate-limited into the last
// 429 with its `Retry-After`, not a 502.
#[tokio::test]
async fn r11_2_the_documented_fallback_keeps_the_last_429() {
    let h = Harness::new(vec![Arc::new(ArchFallback(vec!["openai", "backup"]))], None);
    h.sites.openai.reply(limited());
    h.sites.backup.reply(limited());
    let (status, headers, _) = h.send(unary_chat()).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert!(headers.contains_key("retry-after"));
    // It still answers with the first success.
    h.sites.openai.reply(limited());
    h.sites.backup.reply(fixture::sse(STREAM));
    assert_eq!(
        gist(&h.body(chat()).await).message,
        gist(&STREAM.concat()).message
    );
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

impl Filter<Reply> for Restore {
    fn edit(&mut self, mut reply: Reply, out: &mut Vec<Reply>) -> Result<(), Fault> {
        // Nothing follows a finish, and text kept back goes before other
        // content: hold nothing past them.
        let text = matches!(
            &reply,
            Reply::Delta {
                delta: Delta::Text(_),
                ..
            }
        );
        let last = !text && reply.is_content();
        if let Reply::Delta {
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

    fn end(&mut self, _end: &Result<(), Fault>, out: &mut Vec<Reply>) -> Result<(), Fault> {
        if !self.held.is_empty() {
            let held = std::mem::take(&mut self.held);
            out.push(Reply::text(self.block, self.vault.restore(held)));
        }
        Ok(())
    }
}

struct Pii;

#[async_trait]
impl Plugin for Pii {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        if !px.is_kind::<Conversation>() {
            return Ok(());
        }
        let vault = Arc::new(Vault::default());
        let redact = vault.clone();
        px.filter(move |chat: &mut Conversation| {
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

#[tokio::test]
async fn r11_plugin_pii_redacts_every_call_and_restores_split_placeholders() {
    let h = Harness::new(vec![Arc::new(Pii)], Some("openai"));
    let split = [
        "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi <PI\"}}]}\n\n",
        "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"I_0>!\"}}]}\n\n",
        "data: [DONE]\n\n",
    ];
    h.sites.openai.reply(fixture::sse(&split));
    let req =
        r#"{"model":"m","stream":true,"messages":[{"role":"user","content":"mail bob@x.io now"}]}"#;
    let body = h.body(call(CHAT, req)).await;
    let sent: serde_json::Value =
        serde_json::from_slice(h.sites.sent.sent()[0].body()).unwrap_or_default();
    assert_eq!(sent["messages"][0]["content"], "mail <PII_0> now");
    assert!(body.contains("bob@x.io"), "{body}");
    assert!(!body.contains("PII"), "{body}");
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
            match resp.buffer_until(Reply::is_content).await {
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

#[tokio::test]
async fn r11_plugin_fallback_moves_on_and_keeps_the_last_error_as_sent() {
    let h = Harness::new(vec![Arc::new(Fallback11(vec!["openai", "backup"]))], None);
    h.sites
        .openai
        .reply(fixture::json(StatusCode::INTERNAL_SERVER_ERROR, "{}"));
    h.sites.backup.reply(fixture::sse(STREAM));
    assert_eq!(
        gist(&h.body(chat()).await).message,
        gist(&STREAM.concat()).message
    );

    h.sites.openai.reply(limited());
    h.sites.backup.reply(limited());
    let (status, headers, _) = h.send(unary_chat()).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert!(headers.contains_key("retry-after"));
}

/// Reports every call's input tokens as 99, as a reseller's markup would.
struct Markup;

#[async_trait]
impl Plugin for Markup {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        px.filter(|r: &mut Reply| {
            if let Reply::Usage(usage) = r
                && usage.get(units::INPUT_TOKENS).is_some()
            {
                let mut marked = Usage::default();
                for (unit, n) in usage.iter() {
                    let n = if unit == units::INPUT_TOKENS { 99 } else { n };
                    marked.add(unit, n);
                }
                *usage = marked;
            }
            Ok(())
        })
    }
}

// A usage edit is written as the reply says on both paths, relayed or
// translated; a meter registered after it sees what the provider reported.
#[tokio::test]
async fn r11_3_a_usage_edit_is_shown_on_both_paths_and_hidden_from_a_later_meter() {
    let sites = Sites::default();
    let list = [
        provider(&sites.openai, "openai", None),
        add_provider("claude", AnthropicMessages, sites.claude.plain()),
    ];
    let meter = Metered::default();
    let plugins: Vec<Arc<dyn Plugin>> = vec![Arc::new(Markup), Arc::new(meter.clone())];
    let gw = gateway(list, plugins, None).unwrap_or_else(|e| panic!("{e}"));
    let prompt_tokens = |target: &'static str| {
        let (gw, sites) = (gw.clone(), sites.clone());
        async move {
            struct To(&'static str);
            #[async_trait]
            impl Plugin for To {
                async fn on_client(
                    &self,
                    cx: &mut ClientContext,
                ) -> Result<Option<Response>, Fault> {
                    cx.set_target(self.0);
                    Ok(None)
                }
            }
            let gw = gw.plugin(To(target));
            let _ = &sites;
            // Anthropic needs a message, so the translated call has one.
            let ask = r#"{"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}"#;
            let (_, _, body) = read_whole(serve(&gw, call(CHAT, ask)).await.0).await;
            gw.idle().await;
            let v: serde_json::Value =
                serde_json::from_slice(&body.unwrap_or_default()).unwrap_or_default();
            v["usage"]["prompt_tokens"].clone()
        }
    };
    sites.openai.reply(fixture::json(StatusCode::OK, UNARY));
    let anthropic = r#"{"id":"m","type":"message","role":"assistant","model":"c","content":[{"type":"text","text":"hi"}],"stop_reason":"end_turn","usage":{"input_tokens":4,"output_tokens":1}}"#;
    sites.claude.reply(fixture::json(StatusCode::OK, anthropic));
    let relayed = prompt_tokens("openai").await;
    let translated = prompt_tokens("claude").await;
    // The meter saw what both calls reported.
    let counted: Vec<_> = meter
        .all()
        .iter()
        .map(|u| u.get(units::INPUT_TOKENS))
        .collect();
    assert_eq!(counted, [Some(4), Some(4)]);
    assert_eq!((&relayed, &translated), (&json!(99), &json!(99)));
}

// The caller chooses whether a synthetic quiet event is enough to commit.
#[tokio::test]
async fn synthetic_outputs_use_the_callers_predicate() {
    struct FallsBack;

    #[async_trait]
    impl Plugin for FallsBack {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let mut one = Usage::default();
            one.add(units::INPUT_TOKENS, 1);
            let quiet = Reply::usage(one);
            // A policy accepting any event commits to the quiet reply.
            let made = [Ok(quiet.clone()), Err(Upstream::fault("gone"))];
            let mut unfiltered = Response::stream(futures::stream::iter(made));
            assert!(unfiltered.buffer_until(|_: &Reply| true).await.is_ok());
            drop(unfiltered);
            let made = [Ok(quiet), Err(Upstream::fault("gone"))];
            let content = |r: &Reply| std::future::ready(r.is_content());
            let made = futures::stream::iter(made).try_filter(content);
            let mut made = Response::stream(made);
            match made.buffer_until(Reply::is_content).await {
                Ok(()) => Ok(Some(made)),
                Err(_) => {
                    drop(made);
                    Ok(Some(cx.call("openai").await?))
                }
            }
        }
    }

    let h = Harness::new(vec![Arc::new(FallsBack)], None);
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    let (status, body) = h.text(unary_chat()).await;

    assert_eq!(status, StatusCode::OK);
    assert!(body.contains(r#""content":"hi""#), "{body}");
    assert_eq!(h.calls.all().len(), 1);
}

// `outputs` yields every value, keep-alives included, so a bridge
// or merge can show usage; readers filter with the kind's own check.
#[tokio::test]
async fn outputs_yields_values_that_are_no_content_such_as_a_keep_alive() {
    /// Reads the call, noting each output's usage and whether it is
    /// content, and answers with the content.
    #[derive(Clone, Default)]
    struct Reads(Arc<Mutex<Vec<Noted>>>);

    /// Whether an output is content, and its input tokens.
    type Noted = (bool, Option<u64>);

    #[async_trait]
    impl Plugin for Reads {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let read: Vec<Reply> = cx.call("openai").await?.outputs().try_collect().await?;
            let noted = read.iter().map(|r| match r {
                Reply::Usage(u) => (false, u.get(units::INPUT_TOKENS)),
                r => (r.is_content(), None),
            });
            self.0.lock().map(|mut n| n.extend(noted)).ok();
            let said = read.into_iter().filter(Reply::is_content).map(Ok);
            Ok(Some(Response::stream(futures::stream::iter(said))))
        }
    }

    let reads = Reads::default();
    let h = Harness::new(vec![Arc::new(reads.clone())], None);
    let mut stream = STREAM.to_vec();
    // A line that is no chunk, as a server's own keep-alive may be.
    stream.insert(0, "data: keep-alive\n\n");
    h.sites.openai.reply(fixture::sse(&stream));

    assert_eq!(h.send(chat()).await.0, StatusCode::OK);
    let noted = reads.0.lock().map(|n| n.clone()).unwrap_or_default();
    assert_eq!(noted.first(), Some(&(false, None)), "{noted:?}");
    // Usage is shown, though it is no content.
    assert!(noted.contains(&(false, Some(5))), "{noted:?}");
}

// Filters see every value, yet `buffer_until(Reply::is_content)` matches neither
// a keep-alive nor usage alone: a stream that sends both, then fails, reaches
// no content, and fallback moves on. The meter still saw the failed call's usage.
#[tokio::test]
async fn until_content_commits_on_neither_a_keep_alive_nor_usage_alone() {
    struct ContentOrBackup;

    #[async_trait]
    impl Plugin for ContentOrBackup {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let mut resp = cx.call("openai").await?;
            match resp.buffer_until(Reply::is_content).await {
                Ok(()) => Ok(Some(resp)),
                Err(_) => {
                    drop(resp);
                    Ok(Some(cx.call("backup").await?))
                }
            }
        }
    }

    let h = Harness::new(vec![Arc::new(ContentOrBackup)], None);
    let quiet = [
        ": keep-alive\n\n",
        "data: {\"id\":\"c1\",\"choices\":[],\"usage\":{\"prompt_tokens\":7}}\n\n",
        "data: {\"error\":{\"message\":\"overloaded\"}}\n\n",
    ];
    h.sites.openai.reply(fixture::sse(&quiet));
    h.sites.backup.reply(fixture::sse(STREAM));

    assert_eq!(
        gist(&h.body(chat()).await).message,
        gist(&STREAM.concat()).message
    );
    let calls = metered(&h);
    let calls: Vec<_> = calls.iter().map(|(c, n)| (c.as_str(), *n)).collect();
    assert_eq!(calls, [("backup/gpt-x", 5), ("openai/gpt-x", 7)]);
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

#[tokio::test]
async fn until_end_replays_a_stream_and_a_whole_body_alike() {
    let stream = relayed_and_read_to_its_end(|| fixture::sse(STREAM), chat, None).await;
    let whole =
        relayed_and_read_to_its_end(|| fixture::json(StatusCode::OK, UNARY), unary_chat, None)
            .await;

    for [relayed, read] in [stream.clone(), whole.clone()] {
        assert_eq!(read, relayed);
        assert_eq!(read.0, StatusCode::OK);
        assert_eq!(read.3, ["openai ok", "client"]);
    }
    assert_eq!(gist(&stream[1].2).message, gist(&STREAM.concat()).message);
    assert_eq!(gist(&whole[1].2).message, gist(UNARY).message);
}

#[tokio::test]
async fn until_end_holds_a_failure_and_it_is_delivered_as_it_would_have_been() {
    // Mid-stream, after content: held and sent after what came before it.
    let broken = || {
        let mut broken = fixture::sse(STREAM);
        broken.body_mut().truncate(2);
        broken
            .body_mut()
            .push(Err(Upstream::fault("connection reset")));
        broken
    };
    let [relayed, read] = relayed_and_read_to_its_end(broken, chat, Some("upstream")).await;
    assert_eq!(read, relayed);
    let text: String = sent_chunks(&read.2)
        .iter()
        .filter_map(|c| {
            c["choices"][0]["delta"]["content"]
                .as_str()
                .map(str::to_owned)
        })
        .collect();
    assert_eq!(text, "Hello world", "{}", read.2);
    assert!(
        read.2.ends_with("\"code\":\"upstream\"}}\n\n"),
        "{}",
        read.2
    );
    assert_eq!(read.3, ["openai upstream", "client"]);

    // Before any content: `Err`, refused as a relay refuses it.
    let [relayed, read] =
        relayed_and_read_to_its_end(|| fixture::sse(&STREAM[..1]), chat, Some("truncated")).await;
    assert_eq!((read.0, &read.2), (relayed.0, &relayed.2));
    assert_eq!(read.0, StatusCode::BAD_GATEWAY);
    assert_eq!(read.3, ["openai truncated", "client"]);
}

#[tokio::test]
async fn until_end_says_how_a_response_ended_and_changes_nothing() {
    /// Records the end `until_end` returns, and answers with the response.
    struct Ending(Arc<Mutex<Vec<String>>>);

    #[async_trait]
    impl Plugin for Ending {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let mut resp = cx.call("openai").await?;
            let end = resp.until_end().await;
            let seen = end
                .as_ref()
                .err()
                .map_or("ok".to_owned(), fault_code)
                .to_owned();
            self.0.lock().map(|mut e| e.push(seen)).ok();
            Ok(Some(resp))
        }
    }

    let broken = || {
        let mut broken = fixture::sse(STREAM);
        broken.body_mut().truncate(2);
        broken.body_mut().push(Err(Upstream::fault("reset")));
        broken
    };
    let clean = || fixture::sse(STREAM);
    let cases: [(&dyn Fn() -> fixture::Reply, &str); 2] = [(&clean, "ok"), (&broken, "upstream")];
    for (reply, seen) in cases {
        let text = |(status, headers, body): (StatusCode, HeaderMap, Result<_, Fault>)| {
            let body: bytes::Bytes = body.unwrap_or_default();
            (status, headers, steady(&String::from_utf8_lossy(&body)))
        };
        let relayed = Harness::new(Vec::new(), Some("openai"));
        relayed.sites.openai.reply(reply());
        let relayed = text(relayed.send(chat()).await);

        let (seen_ends, ends) = (Arc::new(Mutex::new(Vec::new())), Ends::default());
        let plugins: Vec<Arc<dyn Plugin>> =
            vec![Arc::new(ends.clone()), Arc::new(Ending(seen_ends.clone()))];
        let h = Harness::new(plugins, None);
        h.sites.openai.reply(reply());
        let read = text(h.send(chat()).await);

        let seen_ends = seen_ends.lock().map(|e| e.clone()).unwrap_or_default();
        assert_eq!(seen_ends, [seen]);
        // Asking settled nothing: the call ends as its end is sent, as it came.
        assert_eq!(read, relayed);
        assert_eq!(ends.all(), [format!("openai {seen}"), "client".into()]);
    }
}

#[tokio::test]
async fn a_response_dropped_after_until_end_ends_its_call_as_it_ended() {
    /// Reads `openai` to its end, drops it, and answers with `backup`.
    struct ReadAndDrop(bool);

    #[async_trait]
    impl Plugin for ReadAndDrop {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let mut response = cx.call("openai").await?;
            let end = response.until_end().await;
            assert_eq!(
                end.as_ref().err().map(fault_code).as_deref(),
                self.0.then_some("upstream")
            );
            drop(response);
            Ok(Some(cx.call("backup").await?))
        }
    }

    let broken = || {
        let mut broken = fixture::sse(STREAM);
        broken.body_mut().truncate(2);
        broken.body_mut().push(Err(Upstream::fault("reset")));
        broken
    };
    let cases: [(fixture::Reply, &str); 3] = [
        (fixture::sse(STREAM), "openai ok"),
        (fixture::json(StatusCode::OK, UNARY), "openai ok"),
        (broken(), "openai upstream"),
    ];
    for (reply, openai) in cases {
        let ends = Ends::default();
        let h = Harness::new(
            vec![
                Arc::new(ends.clone()),
                Arc::new(ReadAndDrop(openai == "openai upstream")),
            ],
            None,
        );
        h.sites.openai.reply(reply);
        h.sites.backup.reply(fixture::sse(STREAM));

        let body = h.body(chat()).await;
        assert_eq!(gist(&body).message, gist(&STREAM.concat()).message);
        // Its end was held: dropped, the call ends so, before `backup` starts.
        assert_eq!(ends.all(), [openai, "backup ok", "client"]);
    }
}

#[tokio::test]
async fn a_response_read_to_its_end_and_kept_past_its_request_ends_abandoned() {
    /// Reads `backup` to its end and keeps it, unsent, in the request.
    struct Stash;

    #[async_trait]
    impl Plugin for Stash {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let mut read = cx.call("backup").await?;
            read.until_end().await?;
            cx.extensions_mut()
                .insert(Kept(Arc::new(Mutex::new(Some(read)))));
            Ok(None)
        }
    }

    let ends = Ends::default();
    let h = Harness::new(
        vec![Arc::new(ends.clone()), Arc::new(Stash)],
        Some("openai"),
    );
    h.sites.backup.reply(fixture::sse(STREAM));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    assert_eq!(closes(&h, unary_chat()).await, StatusCode::OK);
    let mut ends = ends.all();
    assert_eq!(ends.pop().as_deref(), Some("client"));
    ends.sort();
    // Never read, so never settled: the request's close abandons it.
    assert_eq!(ends, ["backup abandoned", "openai ok"]);
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

#[tokio::test]
async fn a_race_by_completion_picks_the_provider_that_finishes_first() {
    /// Races both providers, each read to its content or its end, and
    /// answers with the first.
    struct Race(bool);

    #[async_trait]
    impl Plugin for Race {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let to_end = self.0;
            let race = ["openai", "backup"].map(|p| {
                let call = cx.call(p);
                Box::pin(async move {
                    let mut resp = call.await?;
                    if to_end {
                        resp.until_end().await?;
                    } else {
                        resp.buffer_until(Reply::is_content).await?;
                    }
                    Ok::<Response, Fault>(resp)
                })
            });
            let (resp, _) = futures::future::select_ok(race).await?;
            Ok(Some(resp))
        }
    }

    const SLOW_REPLY: &[&str] = &[
        "data: {\"id\":\"slow\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"first\"}}]}\n\n",
        "data: [DONE]\n\n",
    ];
    const FAST_REPLY: &[&str] = &[
        "data: {\"id\":\"fast\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"done\"}}]}\n\n",
        "data: [DONE]\n\n",
    ];
    for (to_end, winner) in [(false, "openai"), (true, "backup")] {
        let (ends, sites) = (Ends::default(), Sites::default());
        // `openai` streams content at once but ends late; `backup` starts
        // later and ends first.
        let ms = Duration::from_millis;
        let slow = Paced(sites.openai.plain(), Duration::ZERO, ms(600));
        let fast = Paced(sites.backup.plain(), ms(100), Duration::ZERO);
        let list = [
            add_provider("openai", OpenAiChat, slow),
            add_provider("backup", OpenAiChat, fast),
        ];
        let plugins: Vec<Arc<dyn Plugin>> = vec![Arc::new(ends.clone()), Arc::new(Race(to_end))];
        let gw = gateway(list, plugins, None).unwrap_or_else(|e| panic!("{e}"));
        sites.openai.reply(fixture::sse(SLOW_REPLY));
        sites.backup.reply(fixture::sse(FAST_REPLY));

        let req = post(CHAT, STREAM_REQ);
        let (_, _, body) = read_whole(serve(&gw, req).await.0).await;
        gw.idle().await;

        let body = String::from_utf8_lossy(&body.unwrap_or_default()).into_owned();
        let expected = if winner == "openai" {
            SLOW_REPLY
        } else {
            FAST_REPLY
        };
        let (said, sent) = (gist(&body), gist(&expected.concat()));
        assert_eq!(said.message, sent.message, "to its end: {to_end}");
        assert_eq!(said.start, sent.start, "to its end: {to_end}");
        let mut ends = ends.all();
        assert_eq!(ends.pop().as_deref(), Some("client"));
        ends.sort();
        let loser = if winner == "openai" {
            "backup"
        } else {
            "openai"
        };
        let mut expected = vec![format!("{winner} ok"), format!("{loser} abandoned")];
        expected.sort();
        assert_eq!(ends, expected, "to its end: {to_end}");
    }
}
