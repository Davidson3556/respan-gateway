//! The kernel's promises, driven through the OpenAI format.

use respan_gateway_core::{bytes, futures, http, http_body, http_body_util, serde_json};
use std::future::IntoFuture;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use http::{HeaderMap, HeaderValue, StatusCode};
use respan_gateway_core::async_trait;
use respan_gateway_core::{
    Body, ClientView, Closed, Context, Decoder, Doc, Encoder, Fault, Filter, Format, Gateway,
    Outcome, Plugin, Provider, ProviderContext, Response, Transport, Usage, codes,
};
use respan_gateway_formats::conversation::{Conversation, Finish, Message, Reply, Role, units};
use respan_gateway_formats::{AnthropicMessages, OpenAiChat};
use respan_gateway_server::CallHeaders;
use respan_gateway_testkit::{MockAt, MockTransport, Outcomes, collect, mock, post};

mod common;
use common::{
    CHAT, Call, Disclaimer, Flip, Footer, Harness, HoldAll, MESSAGES, Nameless, OPENAI, Shout,
    Silence, Unmask, call, with_plugins,
};
use serde_json::json;

const BACKUP: &str = "http://backup.test/v1";

const STREAM: &[&str] = &[
    "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hel",
    "lo\"}}]}\n\ndata: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"}}]}\n\n",
    "data: {\"id\":\"c1\",\"choices\":[],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2}}\n\n",
    "data: [DONE]\n\n",
];

const STREAM_REQ: &str = r#"{"model":"gpt-x","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"hi"}]}"#;
const UNARY_REQ: &str = r#"{"model":"gpt-x",  "messages":[]}"#;
const UNARY: &str = r#"{"id":"c2","choices":[{"message":{"content":"hi"}}],"usage":{"prompt_tokens":4,"completion_tokens":1}}"#;

/// The error frame for a stream that stops short.
const TRUNCATED: &str = "data: {\"error\":{\"message\":\"stream ended before completion\",\"type\":\"truncated\",\"param\":null,\"code\":\"truncated\"}}\n\n";

fn chat() -> Call {
    call(CHAT, STREAM_REQ)
}

fn unary_chat() -> Call {
    call(CHAT, UNARY_REQ)
}

/// An output guardrail: fails the response with 403 `blocked` at the first
/// event it catches.
struct Guard(fn(&Reply) -> bool);

#[async_trait]
impl Plugin for Guard {
    async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
        let catches = self.0;
        cx.filter(move |e: &mut Reply| {
            if catches(e) {
                return Err(Fault::new(StatusCode::FORBIDDEN, "blocked", "not allowed"));
            }
            Ok(())
        })?;
        Ok(None)
    }
}

struct Stamp;

#[async_trait]
impl Plugin for Stamp {
    async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
        cx.map_head(|h| {
            h.headers
                .insert("x-served-by", HeaderValue::from_static("respan"));
        })?;
        Ok(None)
    }
}

/// Collects the streamed text without changing anything.
#[derive(Clone, Default)]
struct Transcript(Arc<Mutex<String>>);

impl Transcript {
    fn text(&self) -> String {
        self.0.lock().map(|t| t.clone()).unwrap_or_default()
    }
}

#[async_trait]
impl Plugin for Transcript {
    async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
        let text = self.0.clone();
        cx.filter(move |e: &mut Reply| {
            if let (Reply::Text { text: event, .. }, Ok(mut t)) = (e, text.lock()) {
                t.push_str(event);
            }
            Ok(())
        })?;
        Ok(None)
    }
}

/// Sets the temperature of every request.
struct Cool;

#[async_trait]
impl Plugin for Cool {
    async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
        cx.input_mut::<Conversation>()?.temperature = Some(0.0);
        Ok(None)
    }
}

struct Refuse(Fault);

#[async_trait]
impl Plugin for Refuse {
    async fn on_client(&self, _cx: &mut Context) -> Result<Option<Response>, Fault> {
        Err(self.0.clone())
    }
}

/// Never finishes its request hook.
struct Hang(Outcomes);

#[async_trait]
impl Plugin for Hang {
    async fn on_client(&self, _cx: &mut Context) -> Result<Option<Response>, Fault> {
        std::future::pending().await
    }

    async fn on_client_end(&self, cx: &Context, outcome: &Outcome) {
        self.0.on_client_end(cx, outcome).await;
    }
}

struct PanicAtEnd;

#[async_trait]
impl Plugin for PanicAtEnd {
    async fn on_client_end(&self, _cx: &Context, _outcome: &Outcome) {
        panic!("a buggy plugin");
    }
}

/// Answers from the first provider that succeeds.
struct Fallback(Vec<&'static str>);

#[async_trait]
impl Plugin for Fallback {
    async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
        let mut last = Err(Fault::upstream("no providers"));
        for name in &self.0 {
            last = cx.call(*name).await;
            if last.as_ref().is_ok_and(|r| r.head().status.is_success()) {
                break;
            }
        }
        last.map(Some)
    }
}

/// Answers every request itself, like a cache hit.
struct Cached;

#[async_trait]
impl Plugin for Cached {
    async fn on_client(&self, _cx: &mut Context) -> Result<Option<Response>, Fault> {
        let answer = [Reply::start("cached", "gpt-x"), Reply::text(0, "hi")];
        let mut cost = Usage::default();
        cost.add(units::OUTPUT_TOKENS, 1.0);
        let answer = futures::stream::iter(answer.map(Ok));
        Ok(Some(
            Response::stream(http::Response::new(answer)).with_usage(cost),
        ))
    }
}

/// A provider at `base_url` with its own key, or with no headers, so the
/// client's credentials pass through.
fn provider(
    mock: &MockTransport,
    name: &str,
    base_url: &str,
    key: Option<&'static str>,
) -> Provider {
    let mut headers = HeaderMap::new();
    if let Some(k) = key {
        headers.insert("authorization", HeaderValue::from_static(k));
    }
    Provider::new(name, OpenAiChat, mock.at(base_url).with_headers(headers))
}

/// A gateway serving OpenAI clients: `providers`, `plugins` in order, and
/// `target`.
fn gateway(
    providers: impl IntoIterator<Item = Provider>,
    plugins: Vec<Arc<dyn Plugin>>,
    target: Option<&str>,
) -> Result<Gateway, Fault> {
    let gw = with_plugins(Gateway::new(providers)?.serve(OpenAiChat)?, plugins)?;
    match target {
        Some(t) => gw.target(t),
        None => Ok(gw),
    }
}

impl Harness {
    /// An [`Outcomes`] recorder, then `chain`, then `target`.
    fn new(chain: Vec<Arc<dyn Plugin>>, target: Option<&str>) -> Self {
        let log = Outcomes::default();
        let mock = MockTransport::new();
        let mut plugins: Vec<Arc<dyn Plugin>> = vec![Arc::new(log.clone())];
        plugins.extend(chain);
        let providers = [
            provider(&mock, "openai", OPENAI, Some("Bearer sk-test")),
            provider(&mock, "backup", BACKUP, None),
        ];
        let gw = gateway(providers, plugins, target).unwrap_or_else(|e| panic!("gateway: {e}"));
        Self { gw, mock, log }
    }
}

fn relay() -> Harness {
    Harness::new(Vec::new(), Some("openai"))
}

#[tokio::test]
async fn a_stream_is_relayed_byte_for_byte_with_usage() {
    let h = relay();
    h.mock.reply(OPENAI, mock::sse(STREAM));

    assert_eq!(h.body(chat()).await, STREAM.concat());
    let o = h.log.only();
    assert_eq!((o.status, &o.end), (Some(StatusCode::OK), &Ok(())));
    assert_eq!(o.usage.get(units::INPUT_TOKENS), 5.0);
    assert_eq!(o.usage.get(units::OUTPUT_TOKENS), 2.0);
}

#[tokio::test]
async fn a_unary_response_is_relayed_byte_for_byte_with_usage() {
    let h = relay();
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));

    assert_eq!(h.body(unary_chat()).await, UNARY);
    let o = h.log.only();
    assert_eq!(o.end, Ok(()));
    assert_eq!(o.usage.get(units::INPUT_TOKENS), 4.0);
}

#[tokio::test]
async fn an_error_page_is_relayed_as_it_is() {
    let h = relay();
    let page = "<html>bad gateway</html>";
    h.mock
        .reply(OPENAI, mock::json(StatusCode::BAD_GATEWAY, page));

    let (status, _, body) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(body.ok().as_deref(), Some(page.as_bytes()));
    let o = h.log.only();
    assert_eq!(o.status, Some(StatusCode::BAD_GATEWAY));
    assert!(matches!(o.end, Err(f) if f.status() == StatusCode::BAD_GATEWAY));
}

#[tokio::test]
async fn the_provider_gets_its_auth_and_the_usage_option() {
    let h = Harness::new(Vec::new(), Some("backup"));
    h.mock.reply(BACKUP, mock::sse(STREAM));
    let _ = h.send(chat()).await;

    let sent = &h.mock.sent()[0];
    assert_eq!(sent.uri(), "http://backup.test/v1/chat/completions");
    let body: serde_json::Value = serde_json::from_slice(sent.body()).unwrap_or_default();
    assert_eq!(body["stream_options"]["include_usage"], true);

    let h = relay();
    h.mock.reply(OPENAI, mock::sse(STREAM));
    let _ = h.send(chat()).await;
    let auth = h.mock.sent()[0].headers().get("authorization").cloned();
    assert_eq!(auth, Some(HeaderValue::from_static("Bearer sk-test")));
}

#[tokio::test]
async fn the_clients_credentials_reach_only_a_provider_with_none_configured() {
    // The transport owns credentials: one with headers of its own drops the
    // client's, whatever their names; other headers always go. One with
    // none gets the client's, as a local subscription proxy needs.
    for (target, base, auth) in [
        ("openai", OPENAI, Some("Bearer sk-test")),
        ("backup", BACKUP, Some("Bearer client-key")),
    ] {
        let h = Harness::new(Vec::new(), Some(target));
        let mut req = post(CHAT, UNARY_REQ);
        let headers = req.headers_mut();
        headers.insert(
            "authorization",
            HeaderValue::from_static("Bearer client-key"),
        );
        headers.insert(
            "openai-organization",
            HeaderValue::from_static("client-org"),
        );
        headers.insert("openai-beta", HeaderValue::from_static("assistants=v2"));
        h.mock.reply(base, mock::json(StatusCode::OK, UNARY));
        let (status, _, _) = h.send(req).await;
        assert_eq!(status, StatusCode::OK, "{target}");
        let sent = h.mock.sent();
        let got = |name| sent[0].headers().get(name).and_then(|v| v.to_str().ok());
        assert_eq!(got("authorization"), auth, "{target}");
        assert_eq!(got("openai-organization"), Some("client-org"), "{target}");
        assert_eq!(got("openai-beta"), Some("assistants=v2"), "{target}");
    }
}

#[tokio::test]
async fn a_plugin_asks_a_model_its_own_question_and_reads_the_answer() {
    /// Refuses what a judge model calls unsafe.
    struct Judge;

    #[async_trait]
    impl Plugin for Judge {
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            let mut prompt = cx.input::<Conversation>()?.clone();
            let rule = Message::new(Role::System, "Answer SAFE or UNSAFE.");
            prompt.messages.insert(0, rule);
            let call = cx.call("openai").model("judge-1").input(prompt);
            let verdict = call.await?.collect::<Reply>().await?;
            let unsafe_ = |e: &Reply| matches!(e, Reply::Text { text, .. } if text == "UNSAFE");
            match verdict.iter().any(unsafe_) {
                true => Err(Fault::new(
                    StatusCode::FORBIDDEN,
                    "unsafe",
                    "the judge refused",
                )),
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
        h.mock
            .reply(OPENAI, mock::json(StatusCode::OK, &judged.to_string()))
            .reply(OPENAI, mock::json(StatusCode::OK, UNARY));
        let (got, _, _) = h.send(unary_chat()).await;
        assert_eq!(got, status, "{verdict}");
        // Only the committed call's usage counts, not the judge's.
        if status == StatusCode::OK {
            let usage = h.log.only().usage;
            let billed = (
                usage.get(units::INPUT_TOKENS),
                usage.get(units::OUTPUT_TOKENS),
            );
            assert_eq!(billed, (4.0, 1.0));
        }
        let sent = h.mock.sent();
        let judge: serde_json::Value = serde_json::from_slice(sent[0].body()).unwrap_or_default();
        assert_eq!(judge["model"], "judge-1");
        assert_eq!(judge["messages"][0]["role"], "system");
        if status == StatusCode::OK {
            // The client's request is untouched by the judge's call.
            assert_eq!(&sent[1].body()[..], UNARY_REQ.as_bytes());
        }
    }
}

#[tokio::test]
async fn an_unedited_request_reaches_the_provider_byte_for_byte() {
    let h = relay();
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));
    let _ = h.send(unary_chat()).await;
    assert_eq!(&h.mock.sent()[0].body()[..], UNARY_REQ.as_bytes());
}

#[tokio::test]
async fn a_request_edit_reaches_the_provider() {
    let h = Harness::new(vec![Arc::new(Cool)], Some("openai"));
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));
    let _ = h.send(unary_chat()).await;
    let sent: serde_json::Value =
        serde_json::from_slice(h.mock.sent()[0].body()).unwrap_or_default();
    assert_eq!(
        sent,
        json!({ "model": "gpt-x", "messages": [], "temperature": 0.0 })
    );
}

#[tokio::test]
async fn a_head_edit_keeps_the_relay() {
    struct Mask(Arc<Mutex<Option<StatusCode>>>);

    /// Keeps the status its `end` is given.
    struct Seen(Arc<Mutex<Option<StatusCode>>>);

    impl Filter<Reply> for Seen {
        fn event(&mut self, event: Reply, out: &mut Vec<Reply>) -> Result<(), Fault> {
            out.push(event);
            Ok(())
        }

        fn end(&mut self, outcome: &Outcome, _out: &mut Vec<Reply>) -> Result<(), Fault> {
            if let Ok(mut seen) = self.0.lock() {
                *seen = outcome.status;
            }
            Ok(())
        }
    }

    #[async_trait]
    impl Plugin for Mask {
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            cx.map_head(|h| h.status = StatusCode::OK)?;
            cx.filter(Seen(self.0.clone()))?;
            Ok(None)
        }
    }

    let seen = Arc::new(Mutex::new(None));
    let mask = Mask(seen.clone());
    let h = Harness::new(vec![Arc::new(Stamp), Arc::new(mask)], Some("openai"));
    let error = r#"{"error":{"message":"boom","type":"server_error"}}"#;
    h.mock
        .reply(OPENAI, mock::json(StatusCode::INTERNAL_SERVER_ERROR, error));

    let (status, headers, body) = h.send(unary_chat()).await;

    // The status and headers change; the body is relayed as it is.
    assert_eq!(status, StatusCode::OK);
    let stamp = headers.get("x-served-by");
    assert_eq!(stamp, Some(&HeaderValue::from_static("respan")));
    assert_eq!(body.ok().as_deref(), Some(error.as_bytes()));
    // A filter's `end` sees the provider's status; `on_client_end` the one
    // sent.
    assert_eq!(
        *seen.lock().unwrap_or_else(|e| e.into_inner()),
        Some(StatusCode::INTERNAL_SERVER_ERROR)
    );
    assert_eq!(h.log.only().status, Some(StatusCode::OK));
}

#[tokio::test]
async fn a_filter_re_encodes_only_what_changed() {
    let h = Harness::new(vec![Arc::new(Shout)], Some("openai"));
    h.mock.reply(OPENAI, mock::sse(STREAM));

    let expected = [
        "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"HELLO\"}}]}\n\n",
        "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" WORLD\"}}]}\n\n",
        STREAM[2],
        STREAM[3],
    ];
    assert_eq!(h.body(chat()).await, expected.concat());
    assert_eq!(h.log.only().usage.get(units::OUTPUT_TOKENS), 2.0);
}

#[tokio::test]
async fn a_filter_may_keep_state_that_is_not_sync() {
    /// Numbers the text events of each stream, in a `Cell`.
    struct Count;

    #[async_trait]
    impl Plugin for Count {
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            let seen = std::cell::Cell::new(0);
            cx.filter(move |e: &mut Reply| {
                if let Reply::Text { text, .. } = e {
                    seen.set(seen.get() + 1);
                    *text = format!("{}:{text}", seen.get());
                }
                Ok(())
            })?;
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Count)], Some("openai"));
    h.mock.reply(OPENAI, mock::sse(STREAM));

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
    h.mock.reply(OPENAI, mock::sse(STREAM));

    let body = h.body(chat()).await;

    // The transcript, added after Shout, sees the text before Shout does.
    assert_eq!(transcript.text(), "Hello world");
    assert!(body.contains("HEL"), "{body}");
}

#[tokio::test]
async fn a_refusal_is_an_error_in_the_client_format() {
    let fault = Fault::new(
        StatusCode::PAYMENT_REQUIRED,
        "insufficient_credits",
        "balance too low",
    )
    .with_details(json!({ "balance": 0.12 }));
    let h = Harness::new(
        vec![Arc::new(Stamp), Arc::new(Refuse(fault.clone()))],
        Some("openai"),
    );

    let (status, headers, body) = h.send(chat()).await;

    assert_eq!(status, StatusCode::PAYMENT_REQUIRED);
    let header = |name| headers.get(name).and_then(|v| v.to_str().ok());
    assert_eq!(header("x-served-by"), Some("respan"));
    let err: serde_json::Value = body
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or_default();
    assert_eq!(err["error"]["code"], "insufficient_credits");
    assert_eq!(err["error"]["message"], "balance too low");
    assert_eq!(err["error"]["balance"], 0.12);
    assert!(h.mock.sent().is_empty());
    let o = h.log.only();
    assert_eq!(
        (o.status, o.end),
        (Some(StatusCode::PAYMENT_REQUIRED), Err(fault))
    );
}

#[tokio::test]
async fn a_plugin_can_respond_itself() {
    let h = Harness::new(vec![Arc::new(Cached)], None);

    let body: serde_json::Value =
        serde_json::from_str(&h.body(unary_chat()).await).unwrap_or_default();
    assert_eq!(body["id"], "cached");
    assert_eq!(body["choices"][0]["message"]["content"], "hi");
    assert_eq!(body["usage"]["completion_tokens"], 1);
    let o = h.log.only();
    assert_eq!((o.end, o.usage.get(units::OUTPUT_TOKENS)), (Ok(()), 1.0));
}

#[tokio::test]
async fn a_plugin_can_answer_by_calling_providers() {
    let h = Harness::new(vec![Arc::new(Fallback(vec!["openai", "backup"]))], None);
    let overloaded = r#"{"error":{"message":"overloaded"}}"#;
    h.mock.reply(
        OPENAI,
        mock::json(StatusCode::SERVICE_UNAVAILABLE, overloaded),
    );
    h.mock.reply(BACKUP, mock::sse(STREAM));

    assert_eq!(h.body(chat()).await, STREAM.concat());
    assert_eq!(h.mock.sent().len(), 2);
    assert_eq!(h.log.only().end, Ok(()));
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
        h.mock.reply(OPENAI, mock::sse(&STREAM[..cut]));

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
        assert!(matches!(h.log.only().end, Err(f) if f.code() == codes::TRUNCATED));
    }
}

#[tokio::test]
async fn a_transport_error_mid_stream_ends_with_an_error_event() {
    let h = relay();
    let mut broken = mock::sse(STREAM);
    broken.body_mut().truncate(2);
    broken
        .body_mut()
        .push(Err(Fault::upstream("connection reset")));
    h.mock.reply(OPENAI, broken);

    let body = h.body(chat()).await;

    assert!(body.starts_with(&STREAM[..2].concat()), "{body}");
    assert!(
        body.ends_with(
            "\"type\":\"upstream_error\",\"param\":null,\"code\":\"upstream_error\"}}\n\n"
        )
    );
    assert!(matches!(h.log.only().end, Err(f) if f.code() == codes::UPSTREAM));
}

#[tokio::test]
async fn a_unary_body_cut_before_any_bytes_is_an_error_in_the_client_format() {
    let h = relay();
    let mut reset = mock::json(StatusCode::OK, UNARY);
    *reset.body_mut() = vec![Err(Fault::upstream("connection reset"))];
    h.mock.reply(OPENAI, reset);

    let (status, _, body) = h.send(unary_chat()).await;

    // Nothing was sent, so the head says what happened.
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    let body = String::from_utf8_lossy(&body.unwrap_or_default()).into_owned();
    assert!(body.contains("\"code\":\"upstream_error\""), "{body}");
    assert!(matches!(h.log.only().end, Err(f) if f.code() == codes::UPSTREAM));
}

#[tokio::test]
async fn a_broken_unary_body_is_relayed_as_it_is_and_closed_as_failed() {
    let h = relay();
    let cut = r#"{"id":"#;
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, cut));

    assert_eq!(h.body(unary_chat()).await, cut);
    assert!(matches!(h.log.only().end, Err(f) if f.code() == codes::UPSTREAM));
}

#[tokio::test]
async fn a_client_that_leaves_mid_response_ends_once() {
    let h = relay();
    h.mock.reply(OPENAI, mock::sse(STREAM));

    let (_, mut body) = h.gw.handle(chat()).await.into_parts();
    let _first = http_body_util::BodyExt::frame(&mut body).await;
    drop(body);
    h.gw.idle().await;

    let o = h.log.only();
    assert_eq!(o.status, Some(StatusCode::OK));
    assert!(matches!(o.end, Err(f) if f.code() == codes::ABANDONED));
}

#[tokio::test]
async fn a_client_that_leaves_before_a_response_ends_once() {
    let hang = Outcomes::default();
    let h = Harness::new(vec![Arc::new(Hang(hang.clone()))], Some("openai"));

    let gave_up = tokio::time::timeout(Duration::from_millis(10), h.gw.handle(chat())).await;
    assert!(gave_up.is_err());
    h.gw.idle().await;

    let o = h.log.only();
    assert_eq!(o.status, None);
    assert!(matches!(o.end, Err(f) if f.code() == codes::ABANDONED));
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
            Arc::new(Refuse(Fault::bad_request("no"))),
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
    h.mock.reply(OPENAI, mock::sse(STREAM));

    let _ = h.send(chat()).await;

    assert_eq!(after.only().end, Ok(()));
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
    assert!(h.mock.sent().is_empty());
}

#[tokio::test]
async fn a_gateway_that_cannot_work_fails_as_it_is_built() {
    let mock = MockTransport::new();
    let openai = || provider(&mock, "openai", OPENAI, None);
    let log = || Outcomes::default();
    let built = |gw: Result<Gateway, Fault>| {
        gw.err()
            .map(|f| (f.code().to_owned(), f.message().to_owned()))
    };
    let cases = [
        (
            Gateway::new([openai(), openai()]),
            "provider 'openai' is defined twice",
        ),
        (
            Gateway::new([openai()]).and_then(|g| g.target("missing")),
            "unknown provider 'missing'",
        ),
        (
            Gateway::new([]).and_then(|g| g.plugin("log", log())?.plugin("log", log())),
            "plugin 'log' is registered twice",
        ),
        (
            Gateway::new([]).and_then(|g| g.plugin("a", log())?.after("b", ["a"])),
            "unknown plugin 'b'",
        ),
        (
            Gateway::new([]).and_then(|g| g.plugin("a", log())?.after("a", ["b"])),
            "plugin 'a' runs after unknown plugin 'b'",
        ),
        (
            Gateway::new([]).and_then(|g| g.plugin("a", log())?.after("a", ["a"])),
            "plugin 'a' cannot run after 'a', which runs after it",
        ),
        (
            Gateway::new([]).and_then(|g| {
                let g = g
                    .plugin("a", log())?
                    .plugin("b", log())?
                    .plugin("c", log())?;
                g.after("b", ["a"])?.after("c", ["b"])?.after("a", ["c"])
            }),
            "plugin 'a' cannot run after 'c', which runs after it",
        ),
    ];
    for (gw, expected) in cases {
        let expected = Some((codes::CONFIG.to_owned(), expected.to_owned()));
        assert_eq!(built(gw), expected);
    }
}

#[tokio::test]
async fn a_gateway_with_no_target_and_no_plugins_refuses_404_in_the_clients_format() {
    let mock = MockTransport::new();
    let gw = gateway([provider(&mock, "openai", OPENAI, None)], Vec::new(), None)
        .unwrap_or_else(|e| panic!("gateway: {e}"));

    let (status, headers, body) = collect(gw.handle(unary_chat()).await).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(headers["content-type"], "application/json");
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert!(body.contains(codes::NOT_FOUND), "{body}");
    assert!(mock.sent().is_empty());
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
    async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
        self.note("client");
        let name = self.0;
        cx.filter(move |r: &mut Reply| {
            if let Reply::Text { text, .. } = r {
                *text = format!("{name}({text})");
            }
            Ok(())
        })?;
        Ok(None)
    }

    async fn on_client_end(&self, _cx: &Context, _outcome: &Outcome) {
        self.note("client_end");
    }

    async fn on_provider(&self, _px: &mut ProviderContext) -> Result<(), Fault> {
        self.note("provider");
        Ok(())
    }

    async fn on_provider_end(&self, _px: &ProviderContext, _outcome: &Outcome) {
        self.note("provider_end");
    }
}

#[tokio::test]
async fn plugins_run_in_registration_order_moved_only_as_after_needs() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let named = |name| Named(name, log.clone());
    let mock = MockTransport::new();
    // Registered d, c, b, a; c must run after a, b after d. Ties keep
    // registration order, so: d, b, a, c.
    let gw = Gateway::new([provider(&mock, "openai", OPENAI, None)])
        .and_then(|g| g.serve(OpenAiChat)?.target("openai"))
        .and_then(|g| {
            let g = g.plugin("d", named("d"))?.plugin("c", named("c"))?;
            let g = g.plugin("b", named("b"))?.plugin("a", named("a"))?;
            g.after("c", ["a"])?.after("b", ["d"])
        })
        .unwrap_or_else(|e| panic!("gateway: {e}"));
    mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));

    let (_, _, body) = collect(gw.handle(unary_chat()).await).await;
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
    let h = Harness::new(vec![Arc::new(Shout)], Some("openai"));
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));

    assert_eq!(
        h.body(unary_chat()).await,
        UNARY.replace("\"content\":\"hi\"", "\"content\":\"HI\"")
    );
}

#[tokio::test]
async fn a_client_that_did_not_ask_for_usage_does_not_get_the_usage_chunk() {
    let h = relay();
    h.mock.reply(OPENAI, mock::sse(STREAM));
    let request = r#"{"model":"gpt-x","stream":true,"messages":[]}"#;

    let body = h.body(call(CHAT, request)).await;

    assert_eq!(body, [STREAM[0], STREAM[1], STREAM[3]].concat());
    assert_eq!(h.log.only().usage.get(units::OUTPUT_TOKENS), 2.0);
    let sent: serde_json::Value =
        serde_json::from_slice(h.mock.sent()[0].body()).unwrap_or_default();
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
    h.mock.reply(OPENAI, mock::sse(&stream));
    let request = r#"{"model":"gpt-x","stream":true,"messages":[]}"#;

    let body = h.body(call(CHAT, request)).await;

    assert_eq!(body, chunk("") + "data: [DONE]\n\n");
}

#[tokio::test]
async fn a_chunk_that_says_more_than_null_usage_is_kept() {
    let h = relay();
    let filter = |usage: &str| {
        format!("data: {{\"choices\":[],\"id\":\"\",\"prompt_filter_results\":[]{usage}}}\n\n")
    };
    let stream = [filter(",\"usage\":null"), "data: [DONE]\n\n".into()];
    let stream: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.mock.reply(OPENAI, mock::sse(&stream));
    let request = r#"{"model":"gpt-x","stream":true,"messages":[]}"#;

    let body = h.body(call(CHAT, request)).await;

    assert_eq!(body, filter("") + "data: [DONE]\n\n");
}

#[tokio::test]
async fn caller_values_reach_plugins_by_type() {
    #[derive(Clone)]
    struct Tenant(&'static str);

    struct NeedsTenant;

    #[async_trait]
    impl Plugin for NeedsTenant {
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            match cx.head().extensions.get::<Tenant>() {
                Some(Tenant("acme")) => Ok(None),
                _ => Err(Fault::new(
                    StatusCode::FORBIDDEN,
                    "forbidden",
                    "unknown tenant",
                )),
            }
        }
    }

    let h = Harness::new(vec![Arc::new(NeedsTenant)], Some("openai"));
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));

    let (anonymous, _, _) = h.send(unary_chat()).await;
    let mut req = post(CHAT, UNARY_REQ);
    req.extensions_mut().insert(Tenant("acme"));
    let (acme, _, _) = h.send(req).await;

    assert_eq!((anonymous, acme), (StatusCode::FORBIDDEN, StatusCode::OK));
}

#[tokio::test]
async fn a_panic_in_a_filter_is_refused_as_internal() {
    struct Explode;

    #[async_trait]
    impl Plugin for Explode {
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            cx.filter(|_: &mut Reply| panic!("a buggy filter"))?;
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Explode)], Some("openai"));
    h.mock.reply(OPENAI, mock::sse(STREAM));

    let (status, _, body) = h.send(chat()).await;

    // Nothing was sent before the panic, so the client gets an error in its
    // format, not a dropped connection.
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let body = body.map(|b| String::from_utf8_lossy(&b).into_owned());
    assert!(body.is_ok_and(|b| b.contains(codes::INTERNAL)));
    assert!(matches!(h.log.only().end, Err(f) if f.code() == codes::INTERNAL));
}

#[tokio::test]
async fn a_head_edit_that_panics_is_refused_as_internal() {
    struct Explode;

    #[async_trait]
    impl Plugin for Explode {
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            cx.map_head(|_| panic!("a buggy head edit"))?;
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Explode)], Some("openai"));
    h.mock.reply(OPENAI, mock::sse(STREAM));

    let (status, _, _) = h.send(chat()).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(matches!(h.log.only().end, Err(f) if f.code() == codes::INTERNAL));
}

#[tokio::test]
async fn a_client_body_that_breaks_off_is_refused_in_the_clients_format() {
    let h = relay();
    let broken = futures::stream::iter([Err::<http_body::Frame<bytes::Bytes>, _>(Fault::new(
        StatusCode::PAYLOAD_TOO_LARGE,
        "too_large",
        "request body too large",
    ))]);
    let body = http_body_util::BodyExt::boxed_unsync(http_body_util::StreamBody::new(broken));

    let mut req = http::Request::new(body);
    *req.method_mut() = http::Method::POST;
    *req.uri_mut() = http::Uri::from_static(CHAT);
    let (status, headers, body) = h.send(req).await;

    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(headers["content-type"], "application/json");
    let body = body.map(|b| String::from_utf8_lossy(&b).into_owned());
    assert!(body.is_ok_and(|b| b.contains("\"code\":\"too_large\"")));
    assert!(h.mock.sent().is_empty());
}

#[tokio::test]
async fn a_plugin_that_panics_is_refused_as_internal() {
    struct Explode;

    #[async_trait]
    impl Plugin for Explode {
        async fn on_client(&self, _cx: &mut Context) -> Result<Option<Response>, Fault> {
            panic!("a buggy plugin")
        }
    }

    let h = Harness::new(vec![Arc::new(Explode)], Some("openai"));

    let (status, _, body) = h.send(chat()).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let body = body.map(|b| String::from_utf8_lossy(&b).into_owned());
    assert!(body.is_ok_and(|b| b.contains(codes::INTERNAL)));
    assert!(matches!(h.log.only().end, Err(f) if f.code() == codes::INTERNAL));
}

#[tokio::test]
async fn a_refusal_with_a_non_error_status_is_a_500() {
    let h = Harness::new(
        vec![Arc::new(Refuse(Fault::new(
            StatusCode::OK,
            "odd",
            "not an error",
        )))],
        Some("openai"),
    );

    let (status, _, body) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(body.is_ok_and(|b| b.starts_with(br#"{"error""#)));
}

#[tokio::test]
async fn an_edit_in_the_same_format_keeps_what_a_conversation_cannot_hold() {
    let h = Harness::new(vec![Arc::new(Cool)], Some("openai"));
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));
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
        serde_json::from_slice(h.mock.sent()[0].body()).unwrap_or_default();
    let mut expected = request;
    expected["temperature"] = json!(0.0);
    assert_eq!(sent, expected);
}

#[tokio::test]
async fn an_edited_streaming_request_keeps_the_clients_usage_chunk() {
    let h = Harness::new(vec![Arc::new(Cool)], Some("openai"));
    h.mock.reply(OPENAI, mock::sse(STREAM));

    assert_eq!(h.body(chat()).await, STREAM.concat());
}

#[tokio::test]
async fn an_edit_to_the_start_is_written_in_the_clients_format() {
    struct Rename;

    #[async_trait]
    impl Plugin for Rename {
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            cx.filter(|e: &mut Reply| {
                if let Reply::Start { id, .. } = e {
                    *id = "hidden".into();
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
    h.mock.reply(OPENAI, mock::sse(&stream));

    let body = h.body(chat()).await;

    assert!(body.starts_with("data: {\"id\":\"hidden\""), "{body}");
    assert!(body.contains("\"content\":\"Hello\""), "{body}");
    // In every chunk, not just the first.
    assert!(!body.contains("\"c1\""), "{body}");
}

#[tokio::test]
async fn a_filter_that_changes_nothing_keeps_the_bytes() {
    struct Idle;

    #[async_trait]
    impl Plugin for Idle {
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            cx.filter(|_: &mut Reply| Ok(()))?;
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Idle)], Some("openai"));
    let spaced = "data: {\"id\": \"c1\", \"choices\": [{\"index\": 0, \"delta\": {\"content\": \"caf\\u00e9\"}}]}\n\n";
    h.mock
        .reply(OPENAI, mock::sse(&[spaced, "data: [DONE]\n\n"]));

    assert_eq!(h.body(chat()).await, [spaced, "data: [DONE]\n\n"].concat());
}

#[tokio::test]
async fn a_stream_guardrail_refuses_before_any_bytes_and_fails_the_stream_after() {
    let h = Harness::new(vec![Arc::new(Guard(|_| true))], Some("openai"));
    h.mock.reply(OPENAI, mock::sse(STREAM));
    let (status, _, _) = h.send(chat()).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(h.log.only().status, Some(StatusCode::FORBIDDEN));

    let world = |e: &Reply| matches!(e, Reply::Text { text, .. } if text.contains("world"));
    let h = Harness::new(vec![Arc::new(Guard(world))], Some("openai"));
    h.mock.reply(OPENAI, mock::sse(STREAM));

    let body = h.body(chat()).await;

    assert!(body.contains("Hello"), "{body}");
    assert!(!body.contains("world"), "{body}");
    assert!(
        body.ends_with("\"type\":\"blocked\",\"param\":null,\"code\":\"blocked\"}}\n\n"),
        "{body}"
    );
    assert!(matches!(h.log.only().end, Err(f) if f.code() == "blocked"));
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
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            assert_eq!(cx.head().method, http::Method::POST);
            assert_eq!(
                cx.body()?.json().map(|v| v["model"].clone()),
                Some(json!("gpt-x"))
            );
            let said = cx.input::<Conversation>()?.messages[0].text();
            cx.head_mut()
                .extensions
                .insert(Seen(if said == "hi" { "hi" } else { "other" }));
            cx.set_model("gpt-small");
            Ok(None)
        }

        async fn on_client_end(&self, cx: &Context, _outcome: &Outcome) {
            if let Ok(mut seen) = self.0.lock() {
                *seen = cx.head().extensions.get::<Seen>().map(|s| s.0);
            }
        }
    }

    let redirect = Redirect::default();
    let h = Harness::new(vec![Arc::new(redirect.clone())], Some("openai"));
    h.mock.reply(OPENAI, mock::sse(STREAM));
    let _ = h.send(chat()).await;

    let sent: serde_json::Value =
        serde_json::from_slice(h.mock.sent()[0].body()).unwrap_or_default();
    assert_eq!(sent["model"], "gpt-small");
    assert_eq!(h.log.only().end, Ok(()));
    assert_eq!(redirect.0.lock().map(|s| *s).ok().flatten(), Some("hi"));
}

#[tokio::test]
async fn a_filter_edits_several_events_of_one_chunk_in_its_shape() {
    struct Truncate;

    #[async_trait]
    impl Plugin for Truncate {
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            cx.filter(|e: &mut Reply| {
                match e {
                    Reply::Text { text, .. } => *text = text.to_uppercase(),
                    Reply::Finish(f) => *f = Finish::Length,
                    _ => {}
                }
                Ok(())
            })?;
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Truncate)], Some("openai"));
    let chunk = "data: {\"id\":\"x\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":\"stop\"}]}\n\n";
    h.mock
        .reply(OPENAI, mock::sse(&[chunk, "data: [DONE]\n\n"]));

    assert_eq!(
        h.body(chat()).await,
        "data: {\"id\":\"x\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"HI\"},\"finish_reason\":\"length\"}]}\n\ndata: [DONE]\n\n"
    );
}

#[tokio::test]
async fn cached_whole_bytes_answer_a_streaming_client_as_a_stream() {
    struct Stored;

    #[async_trait]
    impl Plugin for Stored {
        async fn on_client(&self, _cx: &mut Context) -> Result<Option<Response>, Fault> {
            Ok(Some(http::Response::new(UNARY.to_owned()).into()))
        }
    }

    let h = Harness::new(vec![Arc::new(Stored)], None);

    // Whole untyped bytes are rewritten as the stream the client asked for.
    let body = h.body(chat()).await;
    assert!(body.starts_with("data: {"), "{body}");
    assert!(body.contains("\"content\":\"hi\""), "{body}");
    assert!(body.ends_with("data: [DONE]\n\n"), "{body}");
    assert_eq!(h.log.only().end, Ok(()));
}

#[tokio::test]
async fn a_unary_guardrail_refuses_with_its_status() {
    let text = |e: &Reply| matches!(e, Reply::Text { .. });
    let h = Harness::new(vec![Arc::new(Stamp), Arc::new(Guard(text))], Some("openai"));
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));

    let (status, headers, body) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    let body = String::from_utf8_lossy(&body.unwrap_or_default()).into_owned();
    assert!(body.contains("\"code\":\"blocked\""), "{body}");
    assert_eq!(h.log.only().status, Some(StatusCode::FORBIDDEN));
    // The head edits apply to the refusal.
    assert_eq!(
        headers.get("x-served-by"),
        Some(&HeaderValue::from_static("respan"))
    );
}

#[tokio::test]
async fn end_hooks_cannot_call_providers() {
    #[derive(Clone, Default)]
    struct Late(Arc<Mutex<Option<String>>>);

    #[async_trait]
    impl Plugin for Late {
        async fn on_client_end(&self, cx: &Context, _outcome: &Outcome) {
            let err = cx
                .call("openai")
                .await
                .err()
                .map(|f| f.message().to_owned());
            if let Ok(mut seen) = self.0.lock() {
                *seen = err;
            }
        }
    }

    let late = Late::default();
    let h = Harness::new(vec![Arc::new(late.clone())], Some("openai"));
    h.mock.reply(OPENAI, mock::sse(STREAM));
    let _ = h.send(chat()).await;

    let err = late.0.lock().map(|e| e.clone()).ok().flatten();
    assert_eq!(
        err.as_deref(),
        Some("providers can be called only from on_client")
    );
    assert_eq!(h.mock.sent().len(), 1);
}

#[tokio::test]
async fn a_client_that_leaves_before_the_head_has_no_status() {
    /// A provider that answers 200 and never sends a byte.
    struct Silent;

    #[async_trait]
    impl respan_gateway_core::Transport for Silent {
        async fn send(
            &self,
            _req: http::Request<respan_gateway_core::Body>,
        ) -> Result<http::Response<respan_gateway_core::Body>, Fault> {
            let never = http_body_util::StreamBody::new(futures::stream::pending());
            let mut resp = http::Response::new(http_body_util::BodyExt::boxed_unsync(never));
            resp.headers_mut().insert(
                "content-type",
                HeaderValue::from_static("text/event-stream"),
            );
            Ok(resp)
        }
    }

    let log = Outcomes::default();
    let gw = gateway(
        [Provider::new("openai", OpenAiChat, Silent)],
        vec![Arc::new(log.clone())],
        Some("openai"),
    )
    .unwrap_or_else(|e| panic!("{e}"));

    let gave_up = tokio::time::timeout(Duration::from_millis(10), gw.handle(chat())).await;
    assert!(gave_up.is_err());
    gw.idle().await;

    assert_eq!(log.only().status, None);
}

#[test]
fn end_hooks_run_without_a_tokio_runtime() {
    let mock = MockTransport::new();
    let log = Outcomes::default();
    let gw = gateway(
        [provider(&mock, "openai", OPENAI, None)],
        vec![Arc::new(log.clone())],
        Some("openai"),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));

    futures::executor::block_on(async {
        let _ = collect(gw.handle(unary_chat()).await).await;
        gw.idle().await;
    });

    assert_eq!(log.only().end, Ok(()));
}

#[tokio::test]
async fn a_router_can_set_the_target_and_let_later_plugins_run() {
    struct Pick;

    #[async_trait]
    impl Plugin for Pick {
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            assert_eq!(cx.target().map(|t| t.name()), Some("openai"));
            cx.set_target("backup");
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Pick), Arc::new(Cool)], Some("openai"));
    h.mock.reply(BACKUP, mock::json(StatusCode::OK, UNARY));
    let _ = h.send(unary_chat()).await;

    let sent = &h.mock.sent()[0];
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
    h.mock.reply(OPENAI, mock::sse(&chunks));

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
    // Held text goes out before the provider's finish, in the same stream.
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
    h.mock
        .reply(OPENAI, mock::json(StatusCode::OK, &reply.to_string()));

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
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            cx.filter(HoldAll(Vec::new()))?;
            Ok(None)
        }
    }

    let tool_call = json!({"id": "c2", "model": "gpt-x", "system_fingerprint": "fp",
        "choices": [{"message": {"content": "hi", "tool_calls": [{"id": "t", "type": "function",
            "function": {"name": "f", "arguments": "{}"}}]}, "finish_reason": "tool_calls"}],
        "usage": {"prompt_tokens": 4, "completion_tokens": 1}});
    let h = Harness::new(vec![Arc::new(Hold)], Some("openai"));
    h.mock
        .reply(OPENAI, mock::json(StatusCode::OK, &tool_call.to_string()));

    let (status, _, body) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&body.unwrap_or_default()).unwrap_or_default();
    assert_eq!(body, tool_call);
}

#[tokio::test]
async fn what_the_provider_sent_after_its_finish_is_kept_when_it_breaks_off() {
    let stream = [
        "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello\"}}]}\n\n",
        "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"},\"finish_reason\":\"stop\"}]}\n\n",
    ];
    let h = relay();
    h.mock.reply(OPENAI, mock::sse(&stream));

    let (_, _, body) = h.send(chat()).await;

    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert_eq!(body, stream.concat() + TRUNCATED);
}

#[tokio::test]
async fn a_filter_fills_a_cache_with_the_answer_and_its_usage() {
    type Entry = Arc<Mutex<Option<(String, f64)>>>;

    /// Keeps a finished answer and what it cost, to replay later.
    struct Fill(String, Entry);

    impl Filter<Reply> for Fill {
        fn event(&mut self, event: Reply, out: &mut Vec<Reply>) -> Result<(), Fault> {
            if let Reply::Text { text, .. } = &event {
                self.0.push_str(text);
            }
            out.push(event);
            Ok(())
        }

        fn end(&mut self, outcome: &Outcome, _out: &mut Vec<Reply>) -> Result<(), Fault> {
            if let (Ok(()), Ok(mut entry)) = (&outcome.end, self.1.lock()) {
                let cost = outcome.usage.get(units::OUTPUT_TOKENS);
                *entry = Some((std::mem::take(&mut self.0), cost));
            }
            Ok(())
        }
    }

    struct Cache(Entry);

    #[async_trait]
    impl Plugin for Cache {
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            cx.filter(Fill(String::new(), self.0.clone()))?;
            Ok(None)
        }
    }

    let entry = Entry::default();
    let h = Harness::new(vec![Arc::new(Cache(entry.clone()))], Some("openai"));
    h.mock.reply(OPENAI, mock::sse(STREAM));

    let _ = h.send(chat()).await;

    let kept = entry.lock().ok().and_then(|e| e.clone());
    assert_eq!(kept, Some(("Hello world".to_owned(), 2.0)));
}

#[tokio::test]
async fn a_chunk_a_filter_splits_in_two_is_rebuilt_whole() {
    /// Splits each text after its first character.
    struct Split;

    impl Filter<Reply> for Split {
        fn event(&mut self, event: Reply, out: &mut Vec<Reply>) -> Result<(), Fault> {
            match event {
                Reply::Text { block, text, .. } if text.len() > 1 => {
                    let (a, b) = text.split_at(1);
                    out.extend([Reply::text(block, a), Reply::text(block, b)]);
                }
                event => out.push(event),
            }
            Ok(())
        }
    }

    struct Splitter;

    #[async_trait]
    impl Plugin for Splitter {
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            cx.filter(Split)?;
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Splitter)], Some("openai"));
    h.mock.reply(OPENAI, mock::sse(STREAM));

    let (status, _, body) = h.send(chat()).await;

    assert_eq!(status, StatusCode::OK);
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert!(body.contains("\"content\":\"Hello\""), "{body}");
    assert!(body.contains("\"content\":\" world\""), "{body}");
}

#[tokio::test]
async fn usage_a_plugin_adds_to_its_bytes_is_counted() {
    struct Replay;

    #[async_trait]
    impl Plugin for Replay {
        async fn on_client(&self, _cx: &mut Context) -> Result<Option<Response>, Fault> {
            let mut cost = Usage::default();
            cost.add(units::OUTPUT_TOKENS, 1.0);
            let bytes = http::Response::new(http_body_util::Full::new(bytes::Bytes::from(UNARY)));
            Ok(Some(Response::from(bytes).with_usage(cost)))
        }
    }

    let h = Harness::new(vec![Arc::new(Replay)], None);

    let body = h.body(unary_chat()).await;

    assert_eq!(body, UNARY);
    assert_eq!(h.log.only().usage.get(units::OUTPUT_TOKENS), 2.0);
}

/// The chunks an OpenAI client gets from `stream` through a filter that
/// holds the finish to the end, adding a footer before it.
async fn footed(stream: &[String]) -> (String, Vec<serde_json::Value>) {
    struct Footer(Option<Reply>);

    impl Filter<Reply> for Footer {
        fn event(&mut self, event: Reply, out: &mut Vec<Reply>) -> Result<(), Fault> {
            match event {
                Reply::Finish(_) => self.0 = Some(event),
                event => out.push(event),
            }
            Ok(())
        }

        fn end(&mut self, _outcome: &Outcome, out: &mut Vec<Reply>) -> Result<(), Fault> {
            out.push(Reply::text(0, " [footer]"));
            out.extend(self.0.take());
            Ok(())
        }
    }

    struct Foot;

    #[async_trait]
    impl Plugin for Foot {
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            cx.filter(Footer(None))?;
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Foot)], Some("openai"));
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.mock.reply(OPENAI, mock::sse(&chunks));
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
    struct Adds<F>(F);

    #[async_trait]
    impl<F: Filter<Reply> + Clone + Sync> Plugin for Adds<F> {
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            cx.filter(self.0.clone())?;
            Ok(None)
        }
    }

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
    h.mock.reply(OPENAI, mock::sse(STREAM));

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
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));

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
        if let Reply::Start { model, .. } = e {
            *model = "alias".into();
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
    h.mock.reply(OPENAI, mock::sse(&chunks));

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
    h.mock.reply(OPENAI, mock::sse(STREAM));
    let req = chat();

    let resp = h.gw.handle(req).await;
    let closed = Closed::of(&resp);
    let _ = collect(resp).await;

    let outcome = closed.await;
    let outcome = outcome.map(|o| (o.status, o.end, o.usage.get(units::OUTPUT_TOKENS)));
    assert_eq!(outcome, Some((Some(StatusCode::OK), Ok(()), 2.0)));
}

#[tokio::test]
async fn a_whole_message_a_filter_took_the_start_of_is_sent() {
    let h = filtered(Nameless);
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));

    let (status, _, body) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.ok().as_deref(), Some(UNARY.as_bytes()));
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
    h.mock.reply(OPENAI, mock::sse(&chunks));

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

    let resp = h.gw.handle(post("/nowhere", UNARY_REQ)).await;

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    assert!(resp.extensions().get::<Closed>().is_some());
    assert!(Closed::of(&resp).await.is_none());
    // A response the gateway did not make has none.
    assert!(Closed::of(&http::Response::new(())).await.is_none());
}

#[tokio::test]
async fn the_clients_connection_sets_the_version() {
    let h = relay();
    let mut reply = mock::json(StatusCode::OK, UNARY);
    *reply.version_mut() = http::Version::HTTP_2;
    h.mock.reply(OPENAI, reply);

    let resp = h.gw.handle(post(CHAT, UNARY_REQ)).await;

    assert_eq!(resp.version(), http::Version::default());
}

#[tokio::test]
async fn a_plugin_races_two_calls_and_keeps_the_first() {
    struct Hedge;

    #[async_trait]
    impl Plugin for Hedge {
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            let (a, b) = (
                cx.call("openai").into_future(),
                cx.call("backup").into_future(),
            );
            let first = match futures::future::select(a, b).await {
                futures::future::Either::Left((won, _))
                | futures::future::Either::Right((won, _)) => won,
            };
            first.map(Some)
        }
    }

    let h = Harness::new(vec![Arc::new(Hedge)], None);
    h.mock
        .reply(OPENAI, mock::json(StatusCode::OK, UNARY))
        .reply(BACKUP, mock::json(StatusCode::OK, UNARY));

    let (status, _, body) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.ok().as_deref(), Some(UNARY.as_bytes()));
}

fn tool_call_message(content: serde_json::Value) -> String {
    json!({"id": "c2", "choices": [{"index": 0, "finish_reason": "tool_calls",
        "message": {"role": "assistant", "content": content,
            "tool_calls": [{"id": "t", "type": "function", "function": {"name": "f", "arguments": "{}"}}]}}]})
    .to_string()
}

#[tokio::test]
async fn text_taken_beside_a_tool_call_leaves_the_call() {
    let h = filtered(Silence);
    h.mock.reply(
        OPENAI,
        mock::json(StatusCode::OK, &tool_call_message(json!("Let me check"))),
    );

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
    h.mock.reply(
        OPENAI,
        mock::json(StatusCode::OK, &tool_call_message(json!(null))),
    );

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
    h.mock.reply(OPENAI, mock::sse(&chunks));

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
        for h in [
            filtered(Footer(0, None)),
            filtered(Silence),
            filtered(HoldAll(Vec::new())),
            filtered(Disclaimer),
            filtered(Nameless),
        ] {
            h.mock.reply(OPENAI, mock::json(StatusCode::OK, &reply));
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
        fn event(&mut self, event: Reply, out: &mut Vec<Reply>) -> Result<(), Fault> {
            if !matches!(event, Reply::Other { .. }) {
                out.push(event);
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
    h.mock.reply(OPENAI, mock::sse(&chunks));

    let (status, _, body) = h.send(chat()).await;

    assert_eq!(status, StatusCode::OK);
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert!(!body.contains("error") && !body.contains("\"B\""), "{body}");
    assert!(body.ends_with("data: [DONE]\n\n"));
    assert_eq!(h.log.only().end, Ok(()));
}

#[tokio::test]
async fn a_block_a_filter_adds_joins_the_one_text_of_an_openai_message() {
    let h = filtered(Footer(1, None));
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));

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
async fn several_choices_are_relayed_in_the_order_they_came() {
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
    h.mock.reply(OPENAI, mock::sse(&chunks));

    assert_eq!(h.body(chat()).await, stream.concat());
}

#[tokio::test]
async fn the_clients_key_stays_off_a_transport_with_headers_of_its_own() {
    let mock = MockTransport::new();
    // Any header of its own takes the client's key off, whatever its name:
    // `x-team` is no known key, yet the provider may take it for one.
    let at = |name: &'static str| {
        let name = http::HeaderName::from_static(name);
        let headers = HeaderMap::from_iter([(name, HeaderValue::from_static("provider"))]);
        mock.at(OPENAI).with_headers(headers)
    };
    let gw = gateway(
        [
            Provider::new("api-key", OpenAiChat, at("api-key")),
            Provider::new("x-team", OpenAiChat, at("x-team")),
            Provider::new("shared", OpenAiChat, Arc::new(at("x-team"))),
        ],
        vec![Arc::new(ByHeader)],
        None,
    )
    .expect("builds");
    for target in ["api-key", "x-team", "shared"] {
        mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));
        let mut req = post(CHAT, UNARY_REQ);
        let headers = req.headers_mut();
        headers.insert("x-to", HeaderValue::from_static(target));
        headers.insert("authorization", HeaderValue::from_static("Bearer client"));

        let (status, _, _) = collect(gw.handle(req).await).await;

        assert_eq!(status, StatusCode::OK, "{target}");
        let sent = mock.sent();
        let sent = sent.last().expect("sent");
        assert!(sent.headers().get("authorization").is_none(), "{target}");
    }
}

/// Sends each request to the provider its `x-to` header names.
struct ByHeader;

#[async_trait]
impl Plugin for ByHeader {
    async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
        let to = cx.head().headers.get("x-to").and_then(|v| v.to_str().ok());
        let to = to.unwrap_or_default().to_owned();
        cx.set_target(to);
        Ok(None)
    }
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
async fn a_relayed_head_leaves_out_the_providers_origin_headers() {
    let h = relay();
    let mut reply = mock::json(StatusCode::OK, UNARY);
    for (name, value) in [
        ("openai-organization", "org-operator"),
        ("set-cookie", "__cf_bm=x; Domain=.openai.com"),
        ("alt-svc", "h3=\":443\""),
        ("strict-transport-security", "max-age=31536000"),
        ("connection", "x-hop"),
        ("x-hop", "1"),
        ("keep-alive", "timeout=5"),
        ("x-request-id", "req_1"),
    ] {
        reply
            .headers_mut()
            .insert(name, HeaderValue::from_static(value));
    }
    h.mock.reply(OPENAI, reply);

    let (_, headers, _) = h.send(unary_chat()).await;

    // Headers about the response stay; the provider origin's state does not
    // reach clients.
    for origin in [
        "set-cookie",
        "alt-svc",
        "strict-transport-security",
        "connection",
        "x-hop",
        "keep-alive",
    ] {
        assert!(headers.get(origin).is_none(), "{origin}");
    }
    assert!(headers.get("openai-organization").is_some());
    assert_eq!(
        headers.get("x-request-id"),
        Some(&HeaderValue::from_static("req_1"))
    );
}

#[tokio::test]
async fn groqs_usage_beside_the_response_is_counted() {
    let h = relay();
    let reply = json!({"id": "c2", "choices": [{"index": 0, "message": {"content": "hi"}}],
        "x_groq": {"usage": {"prompt_tokens": 7, "completion_tokens": 3}}});
    h.mock
        .reply(OPENAI, mock::json(StatusCode::OK, &reply.to_string()));

    let _ = h.send(unary_chat()).await;

    let usage = h.log.only().usage;
    assert_eq!(
        (
            usage.get(units::INPUT_TOKENS),
            usage.get(units::OUTPUT_TOKENS)
        ),
        (7.0, 3.0)
    );
}

#[tokio::test]
async fn collecting_the_wrong_type_fails_even_with_nothing_said() {
    struct Wrong;

    #[async_trait]
    impl Plugin for Wrong {
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            let got = cx.call("openai").await?.collect::<String>().await;
            assert_eq!(
                got.err().map(|f| f.code().to_owned()),
                Some(codes::WRONG_KIND.to_owned())
            );
            // Its own kind collects, though empty.
            let got = cx.call("openai").await?.collect::<Reply>().await;
            assert!(got.is_ok_and(|events| events.is_empty()));
            Err(Fault::bad_request("checked"))
        }
    }

    let h = Harness::new(vec![Arc::new(Wrong)], None);
    h.mock
        .reply(OPENAI, mock::sse(&["data: [DONE]\n\n"]))
        .reply(OPENAI, mock::sse(&["data: [DONE]\n\n"]));

    let (status, _, _) = h.send(chat()).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn a_plugin_knows_which_format_the_client_speaks() {
    struct Seen(Arc<Mutex<Vec<String>>>);

    #[async_trait]
    impl Plugin for Seen {
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            if let Ok(mut seen) = self.0.lock() {
                seen.push(cx.format().to_owned());
            }
            Ok(None)
        }
    }

    let seen = Arc::new(Mutex::new(Vec::new()));
    let mock = MockTransport::new();
    let gw = Gateway::new([provider(&mock, "openai", OPENAI, Some("Bearer sk-test"))])
        .and_then(|g| g.serve(OpenAiChat)?.serve(AnthropicMessages))
        .and_then(|g| g.plugin("seen", Seen(seen.clone()))?.target("openai"))
        .expect("builds");
    let messages = r#"{"model":"gpt-x","max_tokens":10,"messages":[]}"#;
    for req in [post(CHAT, UNARY_REQ), post(MESSAGES, messages)] {
        mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));
        let _ = collect(gw.handle(req).await).await;
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
async fn the_clients_credentials_go_once() {
    let h = Harness::new(Vec::new(), Some("backup"));
    let mut req = post(CHAT, UNARY_REQ);
    let headers = req.headers_mut();
    headers.insert(
        "authorization",
        HeaderValue::from_static("Bearer client-key"),
    );
    headers.insert("openai-organization", HeaderValue::from_static("org-1"));
    h.mock.reply(BACKUP, mock::json(StatusCode::OK, UNARY));

    let _ = h.send(req).await;

    let sent = h.mock.sent();
    for name in ["authorization", "openai-organization"] {
        assert_eq!(sent[0].headers().get_all(name).iter().count(), 1, "{name}");
    }
}

#[tokio::test]
async fn a_calls_own_input_keeps_what_the_client_sent_in_its_format() {
    /// Asks again with a trimmed copy of the client's request.
    struct Trim;

    #[async_trait]
    impl Plugin for Trim {
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            let mut trimmed = cx.input::<Conversation>()?.clone();
            trimmed.messages.truncate(1);
            cx.call("openai").input(trimmed).await.map(Some)
        }
    }

    let h = Harness::new(vec![Arc::new(Trim)], None);
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));
    let body = json!({"model": "gpt-x", "tools": [{"type": "function", "function": {"name": "f"}}],
        "messages": [{"role": "user", "content": "a"}, {"role": "user", "content": "b"}]});

    let (status, _, _) = h.send(call(CHAT, body.to_string())).await;

    assert_eq!(status, StatusCode::OK);
    let sent: serde_json::Value =
        serde_json::from_slice(h.mock.sent()[0].body()).unwrap_or_default();
    assert_eq!(sent["tools"][0]["function"]["name"], "f");
    assert_eq!(sent["messages"].as_array().map(Vec::len), Some(1));
}

#[tokio::test]
async fn a_judges_own_prompt_carries_nothing_of_the_clients() {
    struct Judge;

    #[async_trait]
    impl Plugin for Judge {
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            let said = cx.input::<Conversation>()?.messages[0].text();
            let prompt = Conversation::new([
                Message::new(Role::System, "Answer SAFE or UNSAFE."),
                Message::new(Role::User, said),
            ]);
            let _verdict = cx
                .call("openai")
                .model("judge-1")
                .input(prompt)
                .await?
                .collect::<Reply>()
                .await?;
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Judge)], Some("openai"));
    h.mock
        .reply(OPENAI, mock::json(StatusCode::OK, UNARY))
        .reply(OPENAI, mock::json(StatusCode::OK, UNARY));
    let body = json!({"model": "gpt-x", "response_format": {"type": "json_object"},
        "tools": [{"type": "function", "function": {"name": "f"}}],
        "messages": [{"role": "user", "content": "hi"}]});

    let (status, _, _) = h.send(call(CHAT, body.to_string())).await;

    assert_eq!(status, StatusCode::OK);
    let judge: serde_json::Value =
        serde_json::from_slice(h.mock.sent()[0].body()).unwrap_or_default();
    assert!(
        judge.get("tools").is_none() && judge.get("response_format").is_none(),
        "{judge}"
    );
    assert_eq!(judge["messages"][1]["content"], "hi");
}

#[tokio::test]
async fn a_judge_asks_whole_while_its_client_streams() {
    struct Judge;

    #[async_trait]
    impl Plugin for Judge {
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            let prompt = Conversation::new([Message::new(Role::User, "safe?")]);
            let verdict = cx
                .call("openai")
                .input(prompt)
                .await?
                .collect::<Reply>()
                .await?;
            assert!(
                verdict
                    .iter()
                    .any(|e| matches!(e, Reply::Text { text, .. } if text == "hi"))
            );
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Judge)], Some("openai"));
    h.mock
        .reply(OPENAI, mock::json(StatusCode::OK, UNARY))
        .reply(OPENAI, mock::sse(STREAM));

    let (status, _, body) = h.send(chat()).await;

    assert_eq!(status, StatusCode::OK);
    let judge: serde_json::Value =
        serde_json::from_slice(h.mock.sent()[0].body()).unwrap_or_default();
    assert_eq!(judge["stream"], false);
    assert!(judge.get("stream_options").is_none(), "{judge}");
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert_eq!(body, STREAM.concat());
}

#[tokio::test]
async fn a_whole_answer_to_a_calls_own_input_reaches_a_streaming_client_as_a_stream() {
    struct Trim;

    #[async_trait]
    impl Plugin for Trim {
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            let own = Conversation::new([Message::new(Role::User, "hi")]);
            Ok(Some(cx.call("openai").input(own).await?))
        }
    }

    let h = Harness::new(vec![Arc::new(Trim)], Some("openai"));
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));

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
    let h = Harness::new(vec![Arc::new(Flip)], Some("openai"));
    h.mock
        .reply(OPENAI, mock::json(StatusCode::OK, UNARY))
        .reply(OPENAI, mock::sse(STREAM));

    let (_, streamed, body) = h.send(chat()).await;
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert_eq!(streamed["content-type"], "text/event-stream");
    assert!(body.contains("\"content\":\"hi\""), "{body}");
    assert!(body.ends_with("data: [DONE]\n\n"), "{body}");

    let (_, whole, body) = h.send(unary_chat()).await;
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
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));
    let quiet = r#"{"model":"gpt-x","stream":true,"messages":[]}"#;

    let body = h.body(call(CHAT, quiet)).await;

    assert!(!body.contains("usage"), "{body}");
    assert!(body.ends_with("data: [DONE]\n\n"), "{body}");
    assert_eq!(h.log.only().usage.get(units::OUTPUT_TOKENS), 1.0);
}

#[tokio::test]
async fn a_reframed_stream_that_fails_is_refused_in_the_clients_words() {
    let h = Harness::new(vec![Arc::new(Flip)], Some("openai"));
    let failing = [
        STREAM[0],
        "\"}}]}\n\ndata: {\"error\":{\"message\":\"busy\",\"type\":\"overloaded\",\"param\":null,\"code\":\"overloaded\"}}\n\n",
    ];
    h.mock.reply(OPENAI, mock::sse(&failing));

    let (status, headers, body) = h.send(unary_chat()).await;

    assert!(!status.is_success(), "{status}");
    assert_eq!(headers["content-type"], "application/json");
    let body: serde_json::Value =
        serde_json::from_slice(&body.unwrap_or_default()).unwrap_or_default();
    assert_eq!(body["error"]["code"], "overloaded", "{body}");
}

#[tokio::test]
async fn a_tool_call_cannot_be_reframed_yet_and_says_so() {
    let h = Harness::new(vec![Arc::new(Flip)], Some("openai"));
    let tool = r#"{"id":"c3","choices":[{"index":0,"message":{"role":"assistant","tool_calls":[{"id":"t","type":"function","function":{"name":"f","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}"#;
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, tool));

    let (status, _, body) = h.send(chat()).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert!(body.contains("written anew"), "{body}");
}

#[tokio::test]
async fn a_relayed_redirect_passes_as_it_came_and_is_no_answer() {
    let h = Harness::new(Vec::new(), Some("openai"));
    let moved = http::Response::builder()
        .status(StatusCode::FOUND)
        .header("location", "http://elsewhere/")
        .body(Vec::new())
        .unwrap_or_default();
    h.mock.reply(OPENAI, moved);

    let (status, headers, body) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::FOUND);
    assert_eq!(headers["location"], "http://elsewhere/");
    assert!(headers.get("content-type").is_none());
    assert_eq!(body.ok().as_deref(), Some(&b""[..]));
    let end = h.log.only().end.map_err(|f| f.status());
    assert_eq!(end, Err(StatusCode::FOUND));
}

#[tokio::test]
async fn a_judge_reads_a_redirect_as_a_failure() {
    struct Judge;

    #[async_trait]
    impl Plugin for Judge {
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            let verdict = cx.call("openai").await?.collect::<Reply>().await;
            let status = verdict.err().map(|f| f.status());
            assert_eq!(status, Some(StatusCode::FOUND));
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Judge)], Some("openai"));
    let moved = http::Response::builder()
        .status(StatusCode::FOUND)
        .body(Vec::new())
        .unwrap_or_default();
    h.mock
        .reply(OPENAI, moved)
        .reply(OPENAI, mock::json(StatusCode::OK, UNARY));

    let (status, _, _) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn a_plugins_own_failure_made_of_events_is_written_whole() {
    struct Busy;

    #[async_trait]
    impl Plugin for Busy {
        async fn on_client(&self, _cx: &mut Context) -> Result<Option<Response>, Fault> {
            let events = [Ok(Reply::text(0, "later"))];
            let resp = http::Response::builder()
                .status(StatusCode::TOO_MANY_REQUESTS)
                .body(futures::stream::iter(events))?;
            Ok(Some(Response::stream(resp)))
        }
    }

    let h = Harness::new(vec![Arc::new(Busy)], None);

    // Though the client asked to stream.
    let (status, headers, body) = h.send(chat()).await;

    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(headers["content-type"], "application/json");
    // A plugin's own answer is not a failed request.
    assert_eq!(h.log.only().end, Ok(()));
    let body: serde_json::Value =
        serde_json::from_slice(&body.unwrap_or_default()).unwrap_or_default();
    assert_eq!(body["choices"][0]["message"]["content"], "later", "{body}");
}

#[tokio::test]
async fn a_failure_labelled_as_a_stream_is_still_a_failure() {
    let h = Harness::new(Vec::new(), Some("openai"));
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
    h.mock.reply(OPENAI, failed());

    let (status, _, body) = h.send(chat()).await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body.ok().as_deref(), Some(STREAM.concat().as_bytes()));
    let end = h.log.only().end.map_err(|f| f.status());
    assert_eq!(end, Err(StatusCode::SERVICE_UNAVAILABLE));
}

#[tokio::test]
async fn a_redirect_answering_a_calls_own_input_is_a_bad_gateway() {
    struct Trim;

    #[async_trait]
    impl Plugin for Trim {
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            let own = Conversation::new([Message::new(Role::User, "hi")]);
            Ok(Some(cx.call("openai").input(own).await?))
        }
    }

    let h = Harness::new(vec![Arc::new(Trim)], Some("openai"));
    let moved = http::Response::builder()
        .status(StatusCode::FOUND)
        .header("location", "http://elsewhere/")
        .body(Vec::new())
        .unwrap_or_default();
    h.mock.reply(OPENAI, moved);

    let (status, headers, _) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(headers.get("location").is_none());
}

#[tokio::test]
async fn a_success_framed_as_an_event_is_the_providers_failure_on_every_path() {
    let odd = "data: {\"id\":\"c2\",\"choices\":[{\"message\":{\"content\":\"hi\"}}]}\n\n";
    for plugins in [Vec::new(), vec![Arc::new(Shout) as Arc<dyn Plugin>]] {
        let h = Harness::new(plugins, Some("openai"));
        h.mock.reply(OPENAI, mock::json(StatusCode::OK, odd));

        let (status, _, body) = h.send(unary_chat()).await;

        // Relayed as it came and counted a provider failure, plugin or not.
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.ok().as_deref(), Some(odd.as_bytes()));
        let end = h.log.only().end.map_err(|f| f.code().to_owned());
        assert_eq!(end, Err(codes::UPSTREAM.to_owned()));
    }
}

#[tokio::test]
async fn a_failure_of_several_events_keeps_the_providers_words() {
    struct Ask;

    #[async_trait]
    impl Plugin for Ask {
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            let answer = cx.call("openai").await?.collect::<Reply>().await;
            let fault = answer.err();
            assert_eq!(fault.as_ref().map(Fault::message), Some("busy"));
            assert_eq!(fault.as_ref().map(Fault::code), Some("overloaded_error"));
            assert_eq!(fault.map(|f| f.status().as_u16()), Some(529));
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
    h.mock
        .reply(OPENAI, failed)
        .reply(OPENAI, mock::json(StatusCode::OK, UNARY));

    let (status, _, _) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn a_failure_answering_a_calls_own_input_keeps_its_status_and_retry_after() {
    struct Trim;

    #[async_trait]
    impl Plugin for Trim {
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            let own = Conversation::new([Message::new(Role::User, "hi")]);
            Ok(Some(cx.call("openai").input(own).await?))
        }
    }

    let h = Harness::new(vec![Arc::new(Trim)], Some("openai"));
    let mut limited = mock::json(
        StatusCode::TOO_MANY_REQUESTS,
        r#"{"error":{"message":"slow"}}"#,
    );
    limited
        .headers_mut()
        .insert("retry-after", HeaderValue::from_static("7"));
    limited
        .headers_mut()
        .insert("x-request-id", HeaderValue::from_static("req_1"));
    h.mock.reply(OPENAI, limited);

    let (status, headers, _) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(headers["retry-after"], "7");
    assert!(headers.get("x-request-id").is_none());
}

#[tokio::test]
async fn a_plugins_own_redirect_goes_out_as_it_made_it() {
    struct Moved;

    #[async_trait]
    impl Plugin for Moved {
        async fn on_client(&self, _cx: &mut Context) -> Result<Option<Response>, Fault> {
            let moved = http::Response::builder()
                .status(StatusCode::FOUND)
                .header("location", "http://elsewhere/")
                .body(String::new())?;
            Ok(Some(moved.into()))
        }
    }

    let h = Harness::new(vec![Arc::new(Moved)], None);

    let (status, headers, _) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::FOUND);
    assert_eq!(headers["location"], "http://elsewhere/");
}

#[tokio::test]
async fn an_error_in_a_success_says_what_the_provider_answered() {
    let h = Harness::new(Vec::new(), Some("openai"));
    h.mock
        .reply(
            OPENAI,
            mock::json(StatusCode::OK, r#"{"error":{"code":"x"}}"#),
        )
        .reply(
            OPENAI,
            mock::sse(&["data: {\"error\":\"boom\",\"error_type\":\"overloaded\"}\n\n"]),
        );

    let _ = h.send(unary_chat()).await;
    let _ = h.send(chat()).await;

    let ends: Vec<_> = h
        .log
        .all()
        .into_iter()
        .map(|o| {
            o.end
                .map_err(|f| (f.status(), f.code().to_owned(), f.message().to_owned()))
        })
        .collect();
    assert_eq!(
        ends,
        [
            Err((
                StatusCode::BAD_GATEWAY,
                "x".to_owned(),
                "the provider answered 200".to_owned()
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
    h.mock.reply(OPENAI, mock::sse(STREAM));

    let req = chat();
    let resp = h.gw.handle(req).await;
    let closed = Closed::of(&resp);
    drop(h);
    let (_, _, body) = collect(resp).await;

    assert_eq!(body.ok().as_deref(), Some(STREAM.concat().as_bytes()));
    assert_eq!(closed.await.map(|o| o.end), Some(Ok(())));
}

#[tokio::test]
async fn a_providers_trailers_are_not_passed_on() {
    /// A provider whose whole answer ends in a trailer.
    struct Trailed;

    #[async_trait]
    impl respan_gateway_core::Transport for Trailed {
        async fn send(
            &self,
            _req: http::Request<respan_gateway_core::Body>,
        ) -> Result<http::Response<respan_gateway_core::Body>, Fault> {
            let mut trailers = http::HeaderMap::new();
            trailers.insert("x-trailer", HeaderValue::from_static("1"));
            let frames = [
                Ok(http_body::Frame::data(bytes::Bytes::from_static(
                    UNARY.as_bytes(),
                ))),
                Ok(http_body::Frame::trailers(trailers)),
            ];
            let body = http_body_util::StreamBody::new(futures::stream::iter(frames));
            let mut resp = http::Response::new(http_body_util::BodyExt::boxed_unsync(body));
            resp.headers_mut()
                .insert("content-type", HeaderValue::from_static("application/json"));
            Ok(resp)
        }
    }

    let log = Outcomes::default();
    let gw = gateway(
        [Provider::new("openai", OpenAiChat, Trailed)],
        vec![Arc::new(log.clone())],
        Some("openai"),
    )
    .unwrap_or_else(|e| panic!("{e}"));

    let req = unary_chat();
    let resp = gw.handle(req).await;
    let (_, body) = resp.into_parts();
    let collected = http_body_util::BodyExt::collect(body).await;
    gw.idle().await;

    let collected = collected.unwrap_or_else(|e| panic!("{e}"));
    assert!(collected.trailers().is_none());
    assert_eq!(&collected.to_bytes()[..], UNARY.as_bytes());
    assert_eq!(log.only().end, Ok(()));
}

#[tokio::test]
async fn a_plugins_own_bytes_can_be_sent_not_read() {
    struct Reader;

    #[async_trait]
    impl Plugin for Reader {
        async fn on_client(&self, _cx: &mut Context) -> Result<Option<Response>, Fault> {
            let own: Response = http::Response::new(UNARY.to_owned()).into();
            let read = own.collect::<Reply>().await.err();
            assert_eq!(
                read.map(|f| f.code().to_owned()),
                Some(codes::CONFIG.to_owned())
            );
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Reader)], Some("openai"));
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));

    let (status, _, _) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn a_providers_error_event_in_a_stream_goes_out_as_it_came() {
    let h = relay();
    let error = "data: {\"error\":{\"message\":\"boom\",\"type\":\"server_error\",\"param\":null,\"code\":null}}\n\n";
    h.mock
        .reply(OPENAI, mock::sse(&[STREAM[0], "lo\"}}]}\n\n", error]));

    let (_, _, body) = h.send(chat()).await;

    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert!(body.ends_with(error), "{body}");
    let end = h.log.only().end.map_err(|f| f.code().to_owned());
    assert_eq!(end, Err("server_error".to_owned()));
}

#[tokio::test]
async fn a_guard_refuses_what_it_cannot_read() {
    struct Guard;

    #[async_trait]
    impl Plugin for Guard {
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            let chat = cx.input::<Conversation>()?;
            if !chat.messages.iter().all(Message::is_plain) {
                let why = "content the guard cannot read";
                return Err(Fault::new(StatusCode::FORBIDDEN, "unreadable", why));
            }
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Guard)], Some("openai"));
    let image = r#"{"model":"m","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"x"}}]}]}"#;

    let (status, _, _) = h.send(call(CHAT, image)).await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(h.mock.sent().is_empty());
}

#[tokio::test]
async fn a_providers_error_after_its_finish_keeps_both_and_ends_last() {
    let error = "data: {\"error\":{\"message\":\"boom\",\"type\":\"server_error\",\"param\":null,\"code\":null}}\n\n";
    let content =
        "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hi\"}}]}\n\n";
    let finish = "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n";
    for held in [false, true] {
        // A filter that holds the finish and adds a tail at the end.
        let h = match held {
            true => filtered(Footer(0, None)),
            false => relay(),
        };
        h.mock.reply(OPENAI, mock::sse(&[content, finish, error]));

        let (_, _, body) = h.send(chat()).await;

        let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
        assert!(body.contains("\"content\":\"Hi\""), "held {held}: {body}");
        // A provider's finish is written. One a filter held past a failed
        // end is not, as it would claim the answer complete.
        let finished = body.contains("\"finish_reason\":\"stop\"");
        assert_eq!(finished, !held, "{body}");
        assert!(body.ends_with(error), "held {held}: {body}");
        assert_eq!(body.contains("[footer]"), held, "{body}");
        let end = h.log.only().end.map_err(|f| f.code().to_owned());
        assert_eq!(end, Err("server_error".to_owned()));
    }
}

#[tokio::test]
async fn a_providers_error_object_mid_stream_keeps_the_fields_its_format_has() {
    let h = relay();
    let error = "data: {\"error\":{\"code\":502,\"message\":\"Provider returned error\",\"metadata\":{\"raw\":\"x\"}}}\n\n";
    let content =
        "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hi\"}}]}\n\n";
    h.mock.reply(OPENAI, mock::sse(&[content, error]));

    let (_, _, body) = h.send(chat()).await;

    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    let last = body.trim_end().rsplit("\n\n").next().unwrap_or_default();
    let sent: serde_json::Value =
        serde_json::from_str(last.trim_start_matches("data: ")).unwrap_or_default();
    // OpenAI's `code` is a string, so the fault's replaces the number.
    assert!(sent["error"]["code"].is_string(), "{body}");
    assert_eq!(sent["error"]["type"], sent["error"]["code"], "{body}");
    assert_eq!(sent["error"]["metadata"]["raw"], "x", "{body}");
    assert_eq!(
        sent["error"]["message"], "Provider returned error",
        "{body}"
    );
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

    /// A provider that answers 200 and streams forever, holding a guard.
    struct Endless(Arc<AtomicBool>);

    #[async_trait]
    impl respan_gateway_core::Transport for Endless {
        async fn send(
            &self,
            _req: http::Request<respan_gateway_core::Body>,
        ) -> Result<http::Response<respan_gateway_core::Body>, Fault> {
            let guard = Dropped(self.0.clone());
            let never = futures::StreamExt::inspect(futures::stream::pending(), move |_| {
                let _ = &guard;
            });
            let body = http_body_util::StreamBody::new(never);
            let mut resp = http::Response::new(http_body_util::BodyExt::boxed_unsync(body));
            resp.headers_mut().insert(
                "content-type",
                HeaderValue::from_static("text/event-stream"),
            );
            Ok(resp)
        }
    }

    /// Calls the provider, drops its response and answers itself.
    struct Discard;

    #[async_trait]
    impl Plugin for Discard {
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            drop(cx.call("openai").await?);
            Cached.on_client(cx).await
        }
    }

    let dropped = Arc::new(AtomicBool::new(false));
    let log = Outcomes::default();
    let gw = gateway(
        [Provider::new(
            "openai",
            OpenAiChat,
            Endless(dropped.clone()),
        )],
        vec![Arc::new(log.clone()), Arc::new(Discard)],
        None,
    )
    .unwrap_or_else(|e| panic!("{e}"));

    let req = chat();
    let (status, _, _) = collect(gw.handle(req).await).await;
    gw.idle().await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(log.only().end, Ok(()));
    assert!(dropped.load(Ordering::SeqCst));
}

// The provider edge: hooks once per call, whoever makes it.

/// Bills on the provider edge: each call, as `target/model`, when its
/// `on_provider` runs and when it ends; `client` if its `on_client` runs.
#[derive(Clone, Default)]
struct Bill {
    started: Arc<Mutex<Vec<String>>>,
    ended: Arc<Mutex<Vec<(String, Outcome)>>>,
}

impl Bill {
    fn started(&self) -> Vec<String> {
        self.started.lock().map(|s| s.clone()).unwrap_or_default()
    }

    /// Each call's outcome, by call.
    fn ended(&self) -> Vec<(String, Outcome)> {
        let mut ended = self.ended.lock().map(|e| e.clone()).unwrap_or_default();
        ended.sort_by(|a, b| a.0.cmp(&b.0));
        ended
    }
}

fn call_name(px: &ProviderContext) -> String {
    format!("{}/{}", px.target().name(), px.model().unwrap_or_default())
}

#[async_trait]
impl Plugin for Bill {
    async fn on_client(&self, _cx: &mut Context) -> Result<Option<Response>, Fault> {
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

    async fn on_provider_end(&self, px: &ProviderContext, outcome: &Outcome) {
        let ended = (call_name(px), outcome.clone());
        self.ended.lock().map(|mut e| e.push(ended)).ok();
    }
}

fn input_tokens(outcome: &Outcome) -> f64 {
    outcome.usage.get(units::INPUT_TOKENS)
}

#[tokio::test]
async fn every_call_a_smart_router_makes_is_billed_once_on_the_provider_edge() {
    /// Asks a judge model which provider should answer, then targets it.
    struct SmartRouter;

    #[async_trait]
    impl Plugin for SmartRouter {
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            let ask = Conversation::new([Message::new(Role::User, "openai or backup?")]);
            let judge = cx.call("openai").model("judge-1").input(ask);
            let verdict = judge.await?.collect::<Reply>().await?;
            let pick = verdict.iter().find_map(|e| match e {
                Reply::Text { text, .. } => Some(text.clone()),
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
    h.mock
        .reply(OPENAI, mock::json(StatusCode::OK, &verdict.to_string()))
        .reply(BACKUP, mock::json(StatusCode::OK, UNARY));

    let (status, _) = h.text(unary_chat()).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(bill.started(), ["openai/judge-1", "client", "backup/gpt-x"]);
    let ended = bill.ended();
    let billed: Vec<_> = ended
        .iter()
        .map(|(call, o)| (call.as_str(), o.end.is_ok(), input_tokens(o)))
        .collect();
    assert_eq!(
        billed,
        [("backup/gpt-x", true, 4.0), ("openai/judge-1", true, 50.0)]
    );
    // The client edge keeps the committed answer's usage.
    assert_eq!(input_tokens(&h.log.only()), 4.0);
}

#[tokio::test]
async fn a_call_refused_on_the_provider_edge_refuses_the_request_before_sending() {
    struct Deny;

    #[async_trait]
    impl Plugin for Deny {
        async fn on_provider(&self, _px: &mut ProviderContext) -> Result<(), Fault> {
            Err(Fault::new(StatusCode::FORBIDDEN, "denied", "no calls"))
        }
    }

    let bill = Bill::default();
    let h = Harness::new(vec![Arc::new(Deny), Arc::new(bill.clone())], Some("openai"));

    let (status, body) = h.text(unary_chat()).await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(body.contains(r#""code":"denied""#), "{body}");
    assert!(h.mock.sent().is_empty());
    // Refused before the plugins after it ran on the provider edge.
    assert_eq!(bill.started(), ["client"]);
    assert!(matches!(h.log.only().end, Err(f) if f.code() == "denied"));
}

#[tokio::test]
async fn a_calls_own_copy_of_the_input_changes_only_that_call() {
    /// Cools only calls to `openai`; copies every call's event.
    struct CoolOpenai;

    #[async_trait]
    impl Plugin for CoolOpenai {
        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            let openai = px.target().name() == "openai";
            let chat = px.input_mut::<Conversation>()?;
            if openai {
                chat.temperature = Some(0.0);
            }
            Ok(())
        }
    }

    let chain: Vec<Arc<dyn Plugin>> = vec![
        Arc::new(CoolOpenai),
        Arc::new(Fallback(vec!["openai", "backup"])),
    ];
    let h = Harness::new(chain, None);
    h.mock
        .reply(OPENAI, mock::json(StatusCode::SERVICE_UNAVAILABLE, "{}"))
        .reply(BACKUP, mock::json(StatusCode::OK, UNARY));

    assert_eq!(h.text(unary_chat()).await.0, StatusCode::OK);

    let sent = h.mock.sent();
    let cooled: serde_json::Value = serde_json::from_slice(sent[0].body()).unwrap_or_default();
    assert_eq!(cooled["temperature"], 0.0);
    assert_eq!(sent[1].body(), UNARY_REQ.as_bytes());
}

#[tokio::test]
async fn provider_edge_filters_run_before_client_edge_ones_each_last_added_first() {
    /// Marks the text on both edges.
    struct Mark(&'static str);

    fn mark(tag: String) -> impl FnMut(&mut Reply) -> Result<(), Fault> + Send + 'static {
        move |e| {
            if let Reply::Text { text, .. } = e {
                text.push_str(&tag);
            }
            Ok(())
        }
    }

    #[async_trait]
    impl Plugin for Mark {
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
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
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));

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
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            drop(cx.call("backup").await?);
            Ok(None)
        }
    }

    let bill = Bill::default();
    let h = Harness::new(vec![Arc::new(Peek), Arc::new(bill.clone())], Some("openai"));
    h.mock
        .reply(BACKUP, mock::sse(STREAM))
        .reply(OPENAI, mock::json(StatusCode::OK, UNARY));

    assert_eq!(h.text(unary_chat()).await.0, StatusCode::OK);

    let ended = bill.ended();
    assert_eq!(ended.len(), 2);
    let (call, dropped) = &ended[0];
    assert_eq!(call, "backup/gpt-x");
    assert_eq!(dropped.status, Some(StatusCode::OK));
    assert!(matches!(&dropped.end, Err(f) if f.code() == codes::ABANDONED));
    assert_eq!(ended[1].1.end, Ok(()));
}

#[tokio::test]
async fn each_attempt_of_a_fallback_gets_the_provider_edge() {
    let bill = Bill::default();
    let chain: Vec<Arc<dyn Plugin>> = vec![
        Arc::new(Fallback(vec!["openai", "backup"])),
        Arc::new(bill.clone()),
    ];
    let h = Harness::new(chain, None);
    h.mock
        .reply(OPENAI, mock::json(StatusCode::SERVICE_UNAVAILABLE, "{}"))
        .reply(BACKUP, mock::sse(STREAM));

    assert_eq!(h.body(chat()).await, STREAM.concat());

    // The fallback answered, so the plugin after it had no client edge,
    // yet saw both calls.
    assert_eq!(bill.started(), ["openai/gpt-x", "backup/gpt-x"]);
    let ended = bill.ended();
    let (failed, answered) = (&ended[1].1, &ended[0].1);
    assert_eq!(failed.status, Some(StatusCode::SERVICE_UNAVAILABLE));
    assert!(failed.end.is_err());
    assert_eq!(
        (answered.end.clone(), input_tokens(answered)),
        (Ok(()), 5.0)
    );
}

/// Runs `f` on the provider edge of every call.
struct OnProvider(fn(&mut ProviderContext) -> Result<(), Fault>);

#[async_trait]
impl Plugin for OnProvider {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        (self.0)(px)
    }
}

fn ended_with(bill: &Bill, code: &str) -> bool {
    let ended = bill.ended();
    ended.len() == 1 && matches!(&ended[0].1.end, Err(f) if f.code() == code)
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
    assert!(h.mock.sent().is_empty());
    assert!(ended_with(&bill, codes::INTERNAL));
}

#[tokio::test]
async fn a_panic_in_a_provider_filter_ends_the_call_as_internal() {
    let bill = Bill::default();
    let panics = OnProvider(|px| px.filter(|_: &mut Reply| panic!("a buggy filter")));
    let h = Harness::new(
        vec![Arc::new(bill.clone()), Arc::new(panics)],
        Some("openai"),
    );
    h.mock.reply(OPENAI, mock::sse(STREAM));

    assert_eq!(h.text(chat()).await.0, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(ended_with(&bill, codes::INTERNAL));
    assert!(matches!(h.log.only().end, Err(f) if f.code() == codes::INTERNAL));
}

#[tokio::test]
async fn a_client_that_leaves_mid_stream_ends_the_call_abandoned() {
    let bill = Bill::default();
    let h = Harness::new(vec![Arc::new(bill.clone())], Some("openai"));
    h.mock.reply(OPENAI, mock::sse(STREAM));

    let req = chat();
    let (_, mut body) = h.gw.handle(req).await.into_parts();
    let _first = http_body_util::BodyExt::frame(&mut body).await;
    drop(body);
    h.gw.idle().await;

    assert!(ended_with(&bill, codes::ABANDONED));
    assert_eq!(bill.ended()[0].1.status, Some(StatusCode::OK));
}

#[tokio::test]
async fn a_call_the_transport_fails_ends_with_its_fault() {
    let bill = Bill::default();
    let h = Harness::new(vec![Arc::new(bill.clone())], Some("openai"));

    assert_eq!(h.text(chat()).await.0, StatusCode::BAD_GATEWAY);
    assert!(ended_with(&bill, codes::UPSTREAM));
    assert_eq!(bill.ended()[0].1.status, None);
}

#[tokio::test]
async fn a_refused_call_ends_for_the_refusing_plugin_and_those_before_it() {
    /// Bills the call, then refuses it.
    struct Deny(Bill);

    #[async_trait]
    impl Plugin for Deny {
        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            self.0.on_provider(px).await?;
            Err(Fault::new(StatusCode::FORBIDDEN, "denied", "no calls"))
        }

        async fn on_provider_end(&self, px: &ProviderContext, outcome: &Outcome) {
            self.0.on_provider_end(px, outcome).await;
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
        if px.target().name() == "backup" {
            px.set_model("cheap");
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
    h.mock
        .reply(OPENAI, mock::json(StatusCode::SERVICE_UNAVAILABLE, "{}"))
        .reply(BACKUP, mock::json(StatusCode::OK, UNARY));

    assert_eq!(h.text(unary_chat()).await.0, StatusCode::OK);

    let models: Vec<serde_json::Value> = h
        .mock
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
            // The request's extensions are not this call's.
            assert_eq!(px.client().head().method, http::Method::POST);
            let request = &px.client().head().extensions;
            assert!(request.get::<Reservation>().is_none());
            Ok(())
        }

        async fn on_provider_end(&self, px: &ProviderContext, _outcome: &Outcome) {
            let held = px.extensions().get::<Reservation>().map(|r| r.0);
            self.0.lock().map(|mut s| s.push(held)).ok();
        }

        async fn on_client_end(&self, cx: &Context, _outcome: &Outcome) {
            let held = cx.head().extensions.get::<Reservation>().map(|r| r.0);
            self.0.lock().map(|mut s| s.push(held)).ok();
        }
    }

    let reserve = Reserve::default();
    let h = Harness::new(vec![Arc::new(reserve.clone())], Some("openai"));
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));

    assert_eq!(h.text(unary_chat()).await.0, StatusCode::OK);

    let mut seen = reserve.0.lock().map(|s| s.clone()).unwrap_or_default();
    seen.sort();
    assert_eq!(seen, [None, Some("r1")]);
}

#[tokio::test]
async fn a_calls_request_cannot_call() {
    #[derive(Clone, Default)]
    struct Nested(Arc<Mutex<Vec<Fault>>>);

    impl Nested {
        async fn try_call(&self, px: &ProviderContext) {
            if let Err(f) = px.client().call("openai").await {
                self.0.lock().map(|mut s| s.push(f)).ok();
            }
        }
    }

    #[async_trait]
    impl Plugin for Nested {
        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            self.try_call(px).await;
            Ok(())
        }

        async fn on_provider_end(&self, px: &ProviderContext, _outcome: &Outcome) {
            self.try_call(px).await;
        }
    }

    let nested = Nested::default();
    let h = Harness::new(vec![Arc::new(nested.clone())], Some("openai"));
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));

    assert_eq!(h.text(unary_chat()).await.0, StatusCode::OK);
    let faults = nested.0.lock().map(|s| s.clone()).unwrap_or_default();
    assert_eq!(faults.len(), 2);
    for f in faults {
        assert_eq!(f.code(), codes::INTERNAL);
        assert_eq!(f.message(), "providers can be called only from on_client");
    }
    assert_eq!(h.mock.sent().len(), 1);
}

#[tokio::test]
async fn a_calls_request_is_as_it_was_when_the_call_was_made() {
    /// Calls twice, changing the request's model in between.
    #[derive(Clone, Default)]
    struct Twice(Arc<Mutex<Vec<String>>>);

    #[async_trait]
    impl Plugin for Twice {
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            cx.set_model("a");
            drop(cx.call("openai").await?);
            cx.set_model("b");
            cx.call("openai").await.map(Some)
        }

        async fn on_provider_end(&self, px: &ProviderContext, _outcome: &Outcome) {
            let model = px.client().model().map(str::to_owned);
            self.0.lock().map(|mut s| s.extend(model)).ok();
        }
    }

    let twice = Twice::default();
    let h = Harness::new(vec![Arc::new(twice.clone())], Some("openai"));
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));

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
        async fn on_provider_end(&self, _px: &ProviderContext, _outcome: &Outcome) {
            tokio::time::sleep(Duration::from_millis(50)).await;
            self.0.store(true, Ordering::SeqCst);
        }
    }

    let slow = Slow::default();
    let h = Harness::new(vec![Arc::new(slow.clone())], Some("openai"));
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));

    // `send` returns once `idle` does.
    assert_eq!(h.text(unary_chat()).await.0, StatusCode::OK);
    assert!(slow.0.load(Ordering::SeqCst));
}

#[tokio::test]
async fn reading_an_undecodable_input_does_not_stop_its_relay() {
    /// Reads the input on both edges and changes nothing.
    struct Read;

    #[async_trait]
    impl Plugin for Read {
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            let _ = cx.input::<Conversation>();
            Ok(None)
        }

        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            let _ = px.input::<Conversation>();
            Ok(())
        }
    }

    let h = Harness::new(vec![Arc::new(Read)], Some("openai"));
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));
    let request = r#"["not an object"]"#;

    assert_eq!(h.text(call(CHAT, request)).await.0, StatusCode::OK);
    assert_eq!(h.mock.sent()[0].body(), request.as_bytes());
}

/// OpenAI, sending the key a provider-edge plugin left in the extensions,
/// as a per-tenant key or a signature would be.
struct Keyed;

#[derive(Clone)]
struct TenantKey(&'static str);

impl Format for Keyed {
    type Input = Conversation;
    type Output = Reply;

    fn name(&self) -> &str {
        OpenAiChat.name()
    }

    fn accepts(&self, head: &http::request::Parts) -> bool {
        OpenAiChat.accepts(head)
    }

    fn model(&self, head: &http::request::Parts, body: &Doc) -> Option<String> {
        OpenAiChat.model(head, body)
    }

    fn decode(&self, head: &http::request::Parts, body: &Doc) -> Result<Conversation, Fault> {
        OpenAiChat.decode(head, body)
    }

    fn encode(
        &self,
        input: &Conversation,
        original: Option<(&Doc, &Conversation)>,
    ) -> Result<Doc, Fault> {
        OpenAiChat.encode(input, original)
    }

    fn prepare(&self, body: Doc, client: ClientView<'_>) -> Result<http::Request<Doc>, Fault> {
        let mut req = OpenAiChat.prepare(body, client)?;
        if let Some(key) = client.head().extensions.get::<TenantKey>() {
            let key = HeaderValue::from_static(key.0);
            req.headers_mut().insert("x-tenant-key", key);
        }
        Ok(req)
    }

    fn decoder(&self, head: &http::response::Parts, client: ClientView<'_>) -> Box<dyn Decoder> {
        OpenAiChat.decoder(head, client)
    }

    fn encoder(
        &self,
        head: &mut http::response::Parts,
        client: ClientView<'_>,
        relayed: bool,
    ) -> Box<dyn Encoder> {
        OpenAiChat.encoder(head, client, relayed)
    }
}

#[tokio::test]
async fn extensions_set_in_on_provider_reach_the_formats_prepare() {
    let key = OnProvider(|px| {
        px.extensions_mut().insert(TenantKey("k1"));
        Ok(())
    });
    let mock = MockTransport::new();
    let gw = Gateway::new([Provider::new("openai", Keyed, mock.at(OPENAI))])
        .and_then(|g| g.serve(Keyed)?.plugin("key", key)?.target("openai"))
        .unwrap_or_else(|e| panic!("gateway: {e}"));
    mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));
    let req = unary_chat();

    assert_eq!(collect(gw.handle(req).await).await.0, StatusCode::OK);
    let sent = mock.sent();
    let key = sent[0].headers().get("x-tenant-key");
    assert_eq!(key.and_then(|k| k.to_str().ok()), Some("k1"));
}

/// Sends the tenant key a provider-edge plugin left in the call's
/// extensions.
struct TenantTransport(MockAt);

#[async_trait]
impl Transport for TenantTransport {
    async fn send(&self, mut req: http::Request<Body>) -> Result<http::Response<Body>, Fault> {
        if let Some(key) = req.extensions().get::<TenantKey>().cloned() {
            let key = HeaderValue::from_static(key.0);
            req.headers_mut().insert("x-tenant-key", key);
        }
        self.0.send(req).await
    }
}

#[tokio::test]
async fn extensions_set_in_on_provider_reach_the_transport() {
    let key = OnProvider(|px| {
        px.extensions_mut().insert(TenantKey("k1"));
        Ok(())
    });
    let mock = MockTransport::new();
    let gw = gateway(
        [Provider::new(
            "openai",
            OpenAiChat,
            TenantTransport(mock.at(OPENAI)),
        )],
        vec![Arc::new(key)],
        Some("openai"),
    )
    .unwrap_or_else(|e| panic!("gateway: {e}"));
    mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));
    let req = unary_chat();

    assert_eq!(collect(gw.handle(req).await).await.0, StatusCode::OK);
    let sent = mock.sent();
    let key = sent[0].headers().get("x-tenant-key");
    assert_eq!(key.and_then(|k| k.to_str().ok()), Some("k1"));
}

#[tokio::test]
async fn usage_a_plugin_adds_to_a_call_is_not_the_calls() {
    /// Sends the call's answer with usage of its own.
    struct Surcharge;

    #[async_trait]
    impl Plugin for Surcharge {
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            let mut extra = Usage::default();
            extra.add(units::INPUT_TOKENS, 100.0);
            Ok(Some(cx.call("openai").await?.with_usage(extra)))
        }
    }

    let bill = Bill::default();
    let h = Harness::new(vec![Arc::new(bill.clone()), Arc::new(Surcharge)], None);
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));

    assert_eq!(h.text(unary_chat()).await.0, StatusCode::OK);
    assert_eq!(input_tokens(&h.log.only()), 104.0);
    assert_eq!(input_tokens(&bill.ended()[0].1), 4.0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_calls_provider_end_runs_before_the_clients_end() {
    /// Records each end hook, the provider edge's slowly.
    #[derive(Clone, Default)]
    struct Order(Arc<Mutex<Vec<&'static str>>>);

    #[async_trait]
    impl Plugin for Order {
        async fn on_provider_end(&self, _px: &ProviderContext, _outcome: &Outcome) {
            tokio::time::sleep(Duration::from_millis(20)).await;
            self.0.lock().map(|mut o| o.push("provider")).ok();
        }

        async fn on_client_end(&self, _cx: &Context, _outcome: &Outcome) {
            self.0.lock().map(|mut o| o.push("client")).ok();
        }
    }

    /// Calls a provider and drops its answer unread.
    struct Peek;

    #[async_trait]
    impl Plugin for Peek {
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            drop(cx.call("backup").await?);
            Ok(None)
        }
    }

    let order = Order::default();
    let h = Harness::new(
        vec![Arc::new(order.clone()), Arc::new(Peek)],
        Some("openai"),
    );
    h.mock
        .reply(BACKUP, mock::sse(STREAM))
        .reply(OPENAI, mock::json(StatusCode::OK, UNARY));

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
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            let ask = Conversation::new([Message::new(Role::User, "which?")]);
            let judge = cx.call("openai").model("judge-1").input(ask);
            let verdict = judge.await?.collect::<Reply>().await?;
            let pick = verdict.iter().find_map(|e| match e {
                Reply::Text { text, .. } => Some(text.clone()),
                _ => None,
            });
            cx.set_target(pick.unwrap_or_default());
            Ok(None)
        }
    }

    // Overrules the judge on the provider edge.
    let overrule = OnProvider(|px| match px.model() {
        Some("judge-1") => px.filter(|r: &mut Reply| {
            if let Reply::Text { text, .. } = r {
                *text = text.replace("openai", "backup");
            }
            Ok(())
        }),
        _ => Ok(()),
    });
    let h = Harness::new(vec![Arc::new(overrule), Arc::new(Pick)], None);
    let verdict = json!({"id": "j", "choices": [{"message": {"content": "openai"}}]});
    h.mock
        .reply(OPENAI, mock::json(StatusCode::OK, &verdict.to_string()))
        .reply(BACKUP, mock::json(StatusCode::OK, UNARY));

    assert_eq!(h.text(unary_chat()).await.0, StatusCode::OK);
    assert!(h.mock.sent()[1].uri().to_string().starts_with(BACKUP));
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
    async fn on_provider_end(&self, px: &ProviderContext, outcome: &Outcome) {
        let end = outcome.end.as_ref().err().map_or("ok", |f| f.code());
        let end = format!("{} {end}", px.target().name());
        self.0.lock().map(|mut e| e.push(end)).ok();
    }

    async fn on_client_end(&self, _cx: &Context, _outcome: &Outcome) {
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
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            let kept = Kept(Arc::new(Mutex::new(Some(cx.call("backup").await?))));
            cx.head_mut().extensions.insert(kept);
            Ok(None)
        }
    }

    let ends = Ends::default();
    let h = Harness::new(
        vec![Arc::new(ends.clone()), Arc::new(Stash)],
        Some("openai"),
    );
    h.mock
        .reply(BACKUP, mock::sse(STREAM))
        .reply(OPENAI, mock::json(StatusCode::OK, UNARY));

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
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            let (first, loser) = (cx.call("openai"), cx.call("backup"));
            let (first, loser) = futures::join!(first.into_future(), loser.into_future());
            *self.0.0.lock().map_err(|_| Fault::internal("poisoned"))? = Some(loser?);
            first.map(Some)
        }
    }

    let (ends, kept) = (Ends::default(), Kept::default());
    let h = Harness::new(
        vec![Arc::new(ends.clone()), Arc::new(Hedge(kept.clone()))],
        None,
    );
    h.mock
        .reply(OPENAI, mock::json(StatusCode::OK, UNARY))
        .reply(BACKUP, mock::sse(STREAM));

    assert_eq!(closes(&h, unary_chat()).await, StatusCode::OK);
    let mut ends = ends.all();
    assert_eq!(ends.pop().as_deref(), Some("client"));
    ends.sort();
    assert_eq!(ends, ["backup abandoned", "openai ok"]);
    let read = kept.take().expect("kept").collect::<Reply>().await;
    assert_eq!(
        read.err().map(|f| f.code().to_owned()),
        Some(codes::ABANDONED.into())
    );
}

#[tokio::test]
async fn a_client_edge_failure_ends_the_committed_call_with_it() {
    let bill = Bill::default();
    let guard = Guard(|e| matches!(e, Reply::Text { text, .. } if text.contains("world")));
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
    h.mock.reply(OPENAI, mock::sse(&chunks));

    let _ = h.send(chat()).await;

    let ended = bill.ended();
    assert_eq!(ended.len(), 1);
    let outcome = &ended[0].1;
    assert_eq!(
        outcome.end.as_ref().err().map(|f| f.code()),
        Some("blocked")
    );
    assert_eq!(input_tokens(outcome), 5.0);
}

#[tokio::test]
async fn a_transport_header_of_several_values_sends_them_all() {
    let mut headers = HeaderMap::new();
    headers.append("openai-beta", HeaderValue::from_static("a"));
    headers.append("openai-beta", HeaderValue::from_static("b"));
    let mock = MockTransport::new();
    let gw = gateway(
        [Provider::new(
            "openai",
            OpenAiChat,
            mock.at(OPENAI).with_headers(headers),
        )],
        Vec::new(),
        Some("openai"),
    )
    .expect("builds");
    mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));
    let mut req = post(CHAT, UNARY_REQ);
    let client = HeaderValue::from_static("client");
    req.headers_mut().insert("openai-beta", client);

    let _ = collect(gw.handle(req).await).await;

    let sent = mock.sent();
    let beta: Vec<_> = sent[0].headers().get_all("openai-beta").iter().collect();
    assert_eq!(beta, ["a", "b"]);
}

/// Sets its flag when dropped.
struct Dropped(Arc<AtomicBool>);

impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// A provider that answers 200 and never ends its stream, holding a guard
/// that marks when the provider's body is dropped.
struct Endless(Arc<AtomicBool>);

#[async_trait]
impl Transport for Endless {
    async fn send(&self, _req: http::Request<Body>) -> Result<http::Response<Body>, Fault> {
        let guard = Dropped(self.0.clone());
        let never = futures::StreamExt::inspect(futures::stream::pending(), move |_| {
            let _ = &guard;
        });
        let body = http_body_util::StreamBody::new(never);
        let mut resp = http::Response::new(http_body_util::BodyExt::boxed_unsync(body));
        let sse = HeaderValue::from_static("text/event-stream");
        resp.headers_mut().insert("content-type", sse);
        Ok(resp)
    }
}

#[tokio::test]
async fn a_response_goes_only_to_the_request_whose_call_it_answers() {
    /// On a request marked `x-keep`, keeps a call's response where the
    /// next request sends it.
    struct Swap(Kept);

    #[async_trait]
    impl Plugin for Swap {
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            match cx.head().headers.contains_key("x-keep") {
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
    let mock = MockTransport::new();
    let gw = gateway(
        [
            provider(&mock, "openai", OPENAI, Some("Bearer sk-test")),
            provider(&mock, "backup", BACKUP, None),
        ],
        vec![Arc::new(ends.clone()), swap],
        Some("openai"),
    )
    .expect("builds");
    mock.reply(BACKUP, mock::sse(STREAM))
        .reply(OPENAI, mock::json(StatusCode::OK, UNARY));

    // Request `a` is still open, its answer unread, when `b` runs.
    let mut a = post(CHAT, UNARY_REQ);
    a.headers_mut()
        .insert("x-keep", HeaderValue::from_static("1"));
    let a = gw.handle(a).await;
    let (status, _, body) = collect(gw.handle(post(CHAT, UNARY_REQ)).await).await;
    let _ = collect(a).await;
    gw.idle().await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let body = String::from_utf8_lossy(&body.unwrap_or_default()).into_owned();
    assert!(body.contains(codes::INTERNAL), "{body}");
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
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            let reading = tokio::spawn(cx.call("slow").await?.collect::<Reply>());
            *self.0.lock().map_err(|_| Fault::internal("poisoned"))? = Some(reading);
            Ok(None)
        }
    }

    let (dropped, reader) = (Arc::new(AtomicBool::new(false)), Arc::default());
    let mock = MockTransport::new();
    let gw = gateway(
        [
            provider(&mock, "openai", OPENAI, Some("Bearer sk-test")),
            Provider::new("slow", OpenAiChat, Endless(dropped.clone())),
        ],
        vec![Arc::new(Watch(Arc::clone(&reader)))],
        Some("openai"),
    )
    .expect("builds");
    mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));

    let (status, _, _) = collect(gw.handle(post(CHAT, UNARY_REQ)).await).await;
    let reading = reader.lock().ok().and_then(|mut r| r.take()).expect("read");
    let read = tokio::time::timeout(Duration::from_secs(5), reading).await;
    gw.idle().await;

    assert_eq!(status, StatusCode::OK);
    let read = read.expect("the reader woke").expect("the reader ran");
    assert_eq!(
        read.err().map(|f| f.code().to_owned()),
        Some(codes::ABANDONED.into())
    );
    assert!(dropped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn filters_of_a_call_abandoned_at_close_get_their_end() {
    /// Keeps a call's answer, unread, in the request.
    struct Stash;

    #[async_trait]
    impl Plugin for Stash {
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            let kept = Kept(Arc::new(Mutex::new(Some(cx.call("backup").await?))));
            cx.head_mut().extensions.insert(kept);
            Ok(None)
        }
    }

    /// Notes how each call to `backup` ended, as its filter learns it.
    #[derive(Clone, Default)]
    struct Note(Arc<Mutex<Vec<String>>>);

    struct Noted(Arc<Mutex<Vec<String>>>);

    impl Filter<Reply> for Noted {
        fn event(&mut self, event: Reply, out: &mut Vec<Reply>) -> Result<(), Fault> {
            out.push(event);
            Ok(())
        }

        fn end(&mut self, outcome: &Outcome, _out: &mut Vec<Reply>) -> Result<(), Fault> {
            let end = outcome.end.as_ref().err().map_or("ok", |f| f.code());
            self.0.lock().map(|mut n| n.push(end.to_owned())).ok();
            Ok(())
        }
    }

    #[async_trait]
    impl Plugin for Note {
        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            match px.target().name() {
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
    h.mock
        .reply(BACKUP, mock::sse(STREAM))
        .reply(OPENAI, mock::json(StatusCode::OK, UNARY));

    assert_eq!(closes(&h, unary_chat()).await, StatusCode::OK);
    let noted = note.0.lock().map(|n| n.clone()).unwrap_or_default();
    assert_eq!(noted, [codes::ABANDONED]);
}

#[tokio::test]
async fn a_calls_own_key_keeps_the_clients_credentials_off_it() {
    let tenant = OnProvider(|px| {
        let key = HeaderValue::from_static("Bearer tenant");
        let headers = HeaderMap::from_iter([(http::header::AUTHORIZATION, key)]);
        px.extensions_mut().insert(CallHeaders(headers));
        Ok(())
    });
    let h = Harness::new(vec![Arc::new(tenant)], Some("backup"));
    h.mock.reply(BACKUP, mock::json(StatusCode::OK, UNARY));
    let mut req = unary_chat();
    let headers = req.headers_mut();
    headers.insert("authorization", HeaderValue::from_static("Bearer client"));
    headers.insert("x-api-key", HeaderValue::from_static("client"));
    *req.uri_mut() = http::Uri::from_static("/v1/chat/completions?key=client");

    assert_eq!(h.text(req).await.0, StatusCode::OK);

    let sent = h.mock.sent();
    let auth: Vec<_> = sent[0].headers().get_all("authorization").iter().collect();
    assert_eq!(auth, ["Bearer tenant"]);
    assert!(sent[0].headers().get("x-api-key").is_none());
    assert_eq!(sent[0].uri().query(), None);
}

#[tokio::test]
async fn a_changed_body_is_sent_without_the_clients_content_encoding() {
    for (chain, kept) in [
        (Vec::new(), true),
        (vec![Arc::new(Cool) as Arc<dyn Plugin>], false),
    ] {
        let h = Harness::new(chain, Some("openai"));
        h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));
        let mut req = unary_chat();
        let identity = HeaderValue::from_static("identity");
        req.headers_mut().insert("content-encoding", identity);

        assert_eq!(h.text(req).await.0, StatusCode::OK);
        let sent = h.mock.sent();
        assert_eq!(sent[0].headers().contains_key("content-encoding"), kept);
    }
}

#[tokio::test]
async fn a_transport_with_its_own_key_drops_credentials_from_the_query() {
    let query = "key=a&api_key=b&access_token=c&api-version=1&beta=true";
    // `openai` has a key of its own; `backup` has none.
    for (target, base, sent) in [
        ("openai", OPENAI, Some("api-version=1&beta=true")),
        ("backup", BACKUP, Some(query)),
    ] {
        let h = Harness::new(Vec::new(), Some(target));
        h.mock.reply(base, mock::json(StatusCode::OK, UNARY));
        let mut req = unary_chat();
        *req.uri_mut() = format!("{CHAT}?{query}").parse().unwrap_or_default();

        assert_eq!(h.text(req).await.0, StatusCode::OK);
        assert_eq!(h.mock.sent()[0].uri().query(), sent, "{target}");
    }
}

#[tokio::test]
async fn a_relay_passes_the_clients_end_to_end_headers() {
    // To a provider with no headers of its own, credentials pass too.
    let h = Harness::new(Vec::new(), Some("backup"));
    h.mock.reply(BACKUP, mock::json(StatusCode::OK, UNARY));
    let mut req = post(CHAT, UNARY_REQ);
    let denied = [
        "cookie",
        "accept-encoding",
        "forwarded",
        "via",
        "x-real-ip",
        "x-forwarded-for",
        "x-forwarded-host",
        "x-http-method-override",
        "x-method-override",
        "if-none-match",
        "range",
    ];
    let passed = [
        ("user-agent", "OpenAI/Python 1.0"),
        ("x-app", "notes"),
        ("x-stainless-lang", "python"),
        ("idempotency-key", "k1"),
        ("x-max-tokens", "10"),
        ("x-monkey", "m"),
        ("x-tokenizer", "t"),
        ("x-api-key", "k"),
        ("x-auth-token", "k"),
        ("x-client-secret", "k"),
        ("x-goog-api-key", "k"),
        ("cf-aig-authorization", "k"),
        ("x-password", "k"),
    ];
    for (name, value) in denied.map(|name| (name, "v")).iter().chain(&passed) {
        let value = HeaderValue::from_static(value);
        req.headers_mut().insert(*name, value);
    }

    assert_eq!(h.text(req).await.0, StatusCode::OK);

    let sent = h.mock.sent();
    let headers = sent[0].headers();
    for (name, value) in passed {
        assert_eq!(headers[name], value);
    }
    for name in denied {
        assert!(headers.get(name).is_none(), "{name}");
    }
    assert_eq!(headers.get_all("content-type").iter().count(), 1);
}

#[tokio::test]
async fn a_plugin_removes_the_gateways_own_credential() {
    /// Checks the gateway's key and removes it, so no format passes it on.
    struct Auth;

    #[async_trait]
    impl Plugin for Auth {
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            match cx.head_mut().headers.remove("x-gateway-auth") {
                Some(key) if key == "secret" => Ok(None),
                _ => Err(Fault::new(StatusCode::UNAUTHORIZED, "unauthorized", "no")),
            }
        }
    }

    let h = Harness::new(vec![Arc::new(Auth)], Some("backup"));
    h.mock.reply(BACKUP, mock::json(StatusCode::OK, UNARY));
    let mut req = unary_chat();
    let key = HeaderValue::from_static("secret");
    req.headers_mut().insert("x-gateway-auth", key);

    assert_eq!(h.text(req).await.0, StatusCode::OK);
    assert!(h.mock.sent()[0].headers().get("x-gateway-auth").is_none());
}

// How a response begins and ends, whatever goes wrong around it.

/// The end's code, or `ok`.
fn code(end: &Result<(), Fault>) -> &str {
    end.as_ref().err().map_or("ok", |f| f.code())
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

/// A chunk of text `text`.
fn text_chunk(text: &str) -> String {
    chunk(json!({"id": "c1", "choices": [{"index": 0, "delta": {"content": text}}]}))
}

/// OpenAI Chat whose model panics on an empty body and whose encoder
/// writes part of the text `bad`, then fails.
struct Fragile;

impl Format for Fragile {
    type Input = Conversation;
    type Output = Reply;

    fn name(&self) -> &str {
        OpenAiChat.name()
    }

    fn accepts(&self, head: &http::request::Parts) -> bool {
        OpenAiChat.accepts(head)
    }

    fn model(&self, head: &http::request::Parts, body: &Doc) -> Option<String> {
        assert!(!body.to_bytes().is_empty(), "the model read of no body");
        OpenAiChat.model(head, body)
    }

    fn decode(&self, head: &http::request::Parts, body: &Doc) -> Result<Conversation, Fault> {
        OpenAiChat.decode(head, body)
    }

    fn encode(
        &self,
        input: &Conversation,
        original: Option<(&Doc, &Conversation)>,
    ) -> Result<Doc, Fault> {
        OpenAiChat.encode(input, original)
    }

    fn prepare(&self, body: Doc, client: ClientView<'_>) -> Result<http::Request<Doc>, Fault> {
        OpenAiChat.prepare(body, client)
    }

    fn decoder(&self, head: &http::response::Parts, client: ClientView<'_>) -> Box<dyn Decoder> {
        OpenAiChat.decoder(head, client)
    }

    fn encoder(
        &self,
        head: &mut http::response::Parts,
        client: ClientView<'_>,
        relayed: bool,
    ) -> Box<dyn Encoder> {
        Box::new(Garbling(OpenAiChat.encoder(head, client, relayed)))
    }
}

struct Garbling(Box<dyn Encoder>);

impl Encoder for Garbling {
    fn encode(
        &mut self,
        part: &respan_gateway_core::Part,
        out: &mut bytes::BytesMut,
    ) -> Result<(), Fault> {
        let bad = |e: &Reply| matches!(e, Reply::Text { text, .. } if text == "bad");
        if let respan_gateway_core::Part::Item(item) = part
            && item.events::<Reply>().is_ok_and(|e| e.iter().any(bad))
        {
            out.extend_from_slice(b"GARBAGE");
            return Err(Fault::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "unwritable",
                "cannot write it",
            ));
        }
        self.0.encode(part, out)
    }
}

/// A gateway serving `Fragile` to the providers of [`Harness`], answered
/// by `openai`.
fn fragile(mock: &MockTransport, plugins: Vec<Arc<dyn Plugin>>) -> Gateway {
    let providers = [provider(mock, "openai", OPENAI, None)];
    let gw = Gateway::new(providers).and_then(|g| g.serve(Fragile));
    let gw = gw.and_then(|g| with_plugins(g, plugins)?.target("openai"));
    gw.unwrap_or_else(|e| panic!("gateway: {e}"))
}

#[tokio::test]
async fn a_clients_connection_header_names_only_its_own_headers() {
    let h = Harness::new(Vec::new(), Some("backup"));
    h.mock.reply(BACKUP, mock::json(StatusCode::OK, UNARY));
    let mut req = unary_chat();
    let headers = req.headers_mut();
    let named = HeaderValue::from_static("content-type, x-hop");
    headers.insert("connection", named);
    headers.insert("x-hop", HeaderValue::from_static("1"));

    assert_eq!(h.text(req).await.0, StatusCode::OK);

    // The format's content type stays; the client's hop's header goes.
    let sent = h.mock.sent();
    assert_eq!(sent[0].headers()["content-type"], "application/json");
    assert!(sent[0].headers().get("x-hop").is_none());
}

#[tokio::test]
async fn a_response_whose_last_bytes_were_read_ends_complete() {
    let h = relay();
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));

    let (_, mut body) = h.gw.handle(unary_chat()).await.into_parts();
    let frame = http_body_util::BodyExt::frame(&mut body).await;
    let data = frame.and_then(|f| f.ok()?.into_data().ok());
    assert_eq!(data.as_deref(), Some(UNARY.as_bytes()));
    // The host stops at the last bytes, without polling for the end.
    drop(body);
    h.gw.idle().await;

    assert_eq!(h.log.only().end, Ok(()));
}

#[tokio::test]
async fn a_bodiless_response_ends_as_its_head_goes_out() {
    /// Answers 204 with a stream, which the client's format writes as a
    /// whole body that no host sends.
    struct NoContent;

    #[async_trait]
    impl Plugin for NoContent {
        async fn on_client(&self, _cx: &mut Context) -> Result<Option<Response>, Fault> {
            let resp = http::Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body(futures::stream::iter([Ok(Reply::text(0, "hi"))]))?;
            Ok(Some(Response::stream(resp)))
        }
    }

    let h = Harness::new(vec![Arc::new(NoContent)], None);

    let resp = h.gw.handle(unary_chat()).await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    // A host never polls a 204's body.
    drop(resp);
    h.gw.idle().await;

    let o = h.log.only();
    assert_eq!((o.status, o.end), (Some(StatusCode::NO_CONTENT), Ok(())));
}

/// Changes the head with an edit that panics.
struct PanicEdit;

#[async_trait]
impl Plugin for PanicEdit {
    async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
        cx.map_head(|_| panic!("a buggy head edit"))?;
        Ok(None)
    }
}

#[tokio::test]
async fn a_head_edit_that_panics_refuses_with_the_other_edits() {
    let bill = Bill::default();
    let chain: Vec<Arc<dyn Plugin>> =
        vec![Arc::new(bill.clone()), Arc::new(Stamp), Arc::new(PanicEdit)];
    let h = Harness::new(chain, Some("openai"));
    h.mock.reply(OPENAI, mock::sse(STREAM));

    let (status, headers, _) = h.send(chat()).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(headers["x-served-by"], "respan");
    assert_eq!(code(&h.log.only().end), codes::INTERNAL);
    // The call ends with the refusal, not as if the client had left.
    let ended = bill.ended();
    assert_eq!(ended[0].1.status, Some(StatusCode::OK));
    assert_eq!(code(&ended[0].1.end), codes::INTERNAL);
}

#[tokio::test]
async fn a_refusal_keeps_its_status_unless_an_edit_makes_it_another_error() {
    /// Sets the status, then refuses or panics.
    struct EditThenFail(StatusCode, bool);

    #[async_trait]
    impl Plugin for EditThenFail {
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            let status = self.0;
            cx.map_head(move |h| h.status = status)?;
            match self.1 {
                true => panic!("a buggy plugin"),
                false => Err(Fault::new(StatusCode::FORBIDDEN, "denied", "no")),
            }
        }
    }

    for (edit, panics, sent) in [
        (StatusCode::OK, true, StatusCode::INTERNAL_SERVER_ERROR),
        (StatusCode::OK, false, StatusCode::FORBIDDEN),
        (
            StatusCode::TOO_MANY_REQUESTS,
            false,
            StatusCode::TOO_MANY_REQUESTS,
        ),
    ] {
        let h = Harness::new(vec![Arc::new(EditThenFail(edit, panics))], Some("openai"));

        let (status, _) = h.text(unary_chat()).await;

        assert_eq!(status, sent, "{edit} {panics}");
        assert_eq!(h.log.only().status, Some(sent));
    }
}

#[tokio::test]
async fn a_panic_before_any_bytes_is_refused_whatever_the_status() {
    /// Answers 429 through a filter that panics.
    struct Busy;

    #[async_trait]
    impl Plugin for Busy {
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            cx.filter(|_: &mut Reply| -> Result<(), Fault> { panic!("a buggy filter") })?;
            let resp = http::Response::builder()
                .status(StatusCode::TOO_MANY_REQUESTS)
                .body(futures::stream::iter([Ok(Reply::text(0, "later"))]))?;
            Ok(Some(Response::stream(resp)))
        }
    }

    let h = Harness::new(vec![Arc::new(Busy)], None);

    let (status, body) = h.text(unary_chat()).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(body.contains(codes::INTERNAL), "{body}");
    let o = h.log.only();
    assert_eq!(o.status, Some(StatusCode::INTERNAL_SERVER_ERROR));
    assert_eq!(code(&o.end), codes::INTERNAL);
}

#[tokio::test]
async fn a_panic_after_bytes_went_out_keeps_those_written_before_it() {
    let h = Harness::new(
        vec![Arc::new(Guard(
            |e| matches!(e, Reply::Text { text, .. } if text == "boom" && panic!("a buggy guard")),
        ))],
        Some("openai"),
    );
    let both = format!("{}{}", text_chunk(" world"), text_chunk("boom"));
    h.mock
        .reply(OPENAI, mock::sse(&[HI, &both, "data: [DONE]\n\n"]));

    let (text, fault) = frames(h.gw.handle(chat()).await).await;
    h.gw.idle().await;

    // What was written before the panic in the same batch is sent.
    assert!(
        text.contains("\"Hi\"") && text.contains("\" world\""),
        "{text}"
    );
    assert_eq!(fault.as_ref().map(Fault::code), Some(codes::INTERNAL));
}

#[tokio::test]
async fn a_failed_encode_writes_nothing() {
    let mock = MockTransport::new();
    let gw = fragile(&mock, Vec::new());
    let stream = [HI, &text_chunk("bad"), "data: [DONE]\n\n"];
    mock.reply(OPENAI, mock::sse(&stream));

    let (_, _, body) = collect(gw.handle(chat()).await).await;

    let body = String::from_utf8_lossy(&body.unwrap_or_default()).into_owned();
    assert!(
        body.contains("\"Hi\"") && body.contains("unwritable"),
        "{body}"
    );
    assert!(!body.contains("GARBAGE"), "{body}");
}

#[tokio::test]
async fn another_requests_call_cannot_be_read() {
    /// Keeps a call's response from a request marked `x-keep`, and reads
    /// it from the next request.
    #[derive(Clone, Default)]
    struct Swap(Kept, Arc<Mutex<Option<Result<usize, Fault>>>>);

    #[async_trait]
    impl Plugin for Swap {
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            if cx.head().headers.contains_key("x-keep") {
                let kept = cx.call("backup").await?;
                self.0.0.lock().map(|mut k| *k = Some(kept)).ok();
            } else if let Some(kept) = self.0.take() {
                let read = kept.collect::<Reply>().await.map(|events| events.len());
                self.1.lock().map(|mut r| *r = Some(read)).ok();
            }
            Ok(None)
        }
    }

    let swap = Swap::default();
    let h = Harness::new(vec![Arc::new(swap.clone())], Some("openai"));
    h.mock
        .reply(BACKUP, mock::sse(STREAM))
        .reply(OPENAI, mock::json(StatusCode::OK, UNARY))
        .reply(OPENAI, mock::json(StatusCode::OK, UNARY));
    let mut keeping = unary_chat();
    keeping
        .headers_mut()
        .insert("x-keep", HeaderValue::from_static("1"));

    // The first request is still open, its body unread.
    let first = h.gw.handle(keeping).await;
    let second = collect(h.gw.handle(unary_chat()).await).await;
    assert_eq!(second.0, StatusCode::OK);
    let _ = collect(first).await;

    let read = swap.1.lock().ok().and_then(|mut r| r.take());
    let code = read.and_then(|r| r.err()).map(|f| f.code().to_owned());
    assert_eq!(code.as_deref(), Some(codes::INTERNAL));
}

#[tokio::test]
async fn an_empty_stream_of_another_kind_is_refused() {
    struct Wrong;

    #[async_trait]
    impl Plugin for Wrong {
        async fn on_client(&self, _cx: &mut Context) -> Result<Option<Response>, Fault> {
            let none = futures::stream::empty::<Result<String, Fault>>();
            Ok(Some(Response::stream(http::Response::new(none))))
        }
    }

    let h = Harness::new(vec![Arc::new(Wrong)], None);

    let (status, body) = h.text(unary_chat()).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(body.contains(codes::WRONG_KIND), "{body}");
    assert_eq!(code(&h.log.only().end), codes::WRONG_KIND);
}

#[tokio::test]
async fn a_calls_end_does_not_depend_on_how_its_bytes_were_split() {
    let world = text_chunk(" world");
    let done = "data: [DONE]\n\n";
    let together = format!("{world}{done}");
    for chunks in [vec![HI, &*world, done], vec![HI, &*together]] {
        let bill = Bill::default();
        let guard = Guard(|e| matches!(e, Reply::Text { text, .. } if text.contains("world")));
        let h = Harness::new(
            vec![Arc::new(bill.clone()), Arc::new(guard)],
            Some("openai"),
        );
        h.mock.reply(OPENAI, mock::sse(&chunks));

        let body = h.body(chat()).await;

        assert!(body.contains("blocked"), "{body}");
        let ended = bill.ended();
        assert_eq!(code(&ended[0].1.end), "blocked", "{chunks:?}");
    }
}

#[tokio::test]
async fn a_translated_304_passes_as_its_status() {
    const ANTHROPIC: &str = "http://anthropic.test";
    let mock = MockTransport::new();
    let p = Provider::new("anthropic", AnthropicMessages, mock.at(ANTHROPIC));
    let gw = Gateway::new([p]).and_then(|g| g.serve(OpenAiChat)?.target("anthropic"));
    let gw = gw.unwrap_or_else(|e| panic!("gateway: {e}"));
    let not_modified = http::Response::builder().status(StatusCode::NOT_MODIFIED);
    mock.reply(ANTHROPIC, not_modified.body(Vec::new()).unwrap_or_default());
    let req = call(
        CHAT,
        r#"{"model":"claude-x","messages":[{"role":"user","content":"hi"}]}"#,
    );

    let (status, _, body) = collect(gw.handle(req).await).await;

    assert_eq!(status, StatusCode::NOT_MODIFIED);
    assert_eq!(body.ok().as_deref(), Some(&b""[..]));
}

#[tokio::test]
async fn idle_waits_for_a_response_still_streaming() {
    let h = relay();
    h.mock.reply(OPENAI, mock::sse(STREAM));

    let resp = h.gw.handle(chat()).await;
    let waited = tokio::time::timeout(Duration::from_millis(50), h.gw.idle()).await;
    assert!(waited.is_err(), "idle returned with a response unread");

    let _ = collect(resp).await;
    h.gw.idle().await;
    assert_eq!(h.log.only().end, Ok(()));
}

#[tokio::test]
async fn a_gateway_changed_after_cloning_waits_only_for_its_own_requests() {
    let h = Harness::new(vec![Arc::new(Hang(Outcomes::default()))], Some("openai"));
    let changed = h.gw.clone().plugin("other", Outcomes::default());
    let changed = changed.unwrap_or_else(|e| panic!("{e}"));

    // A request of the original hangs in its plugin.
    let pending = tokio::spawn({
        let gw = h.gw.clone();
        async move { gw.handle(unary_chat()).await }
    });
    tokio::time::sleep(Duration::from_millis(10)).await;

    let own = tokio::time::timeout(Duration::from_millis(50), h.gw.idle()).await;
    let other = tokio::time::timeout(Duration::from_millis(50), changed.idle()).await;
    assert!(own.is_err() && other.is_ok());
    pending.abort();
}

#[tokio::test]
async fn plugins_run_in_the_order_after_gives() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let named = |name| Named(name, log.clone());
    let mock = MockTransport::new();
    let gw = Gateway::new([provider(&mock, "openai", OPENAI, None)])
        .and_then(|g| g.serve(OpenAiChat)?.plugin("a", named("a")))
        .and_then(|g| g.plugin("b", named("b"))?.plugin("c", named("c")))
        .and_then(|g| g.after("a", ["c"]))
        .unwrap_or_else(|e| panic!("gateway: {e}"));

    let _ = collect(gw.handle(unary_chat()).await).await;

    let log = log.lock().map(|l| l.clone()).unwrap_or_default();
    assert_eq!(log, ["b client", "c client", "a client"]);
}

#[tokio::test]
async fn json_nested_too_deeply_is_refused() {
    let h = relay();
    let deep = format!("{}{}", "[".repeat(200), "]".repeat(200));
    let body = format!(r#"{{"model":"gpt-x","messages":[],"x":{deep}}}"#);

    let (status, text) = h.text(call(CHAT, body)).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(text.contains(codes::BAD_REQUEST), "{text}");
    assert!(h.mock.sent().is_empty());
}

#[tokio::test]
async fn a_body_that_broke_off_is_refused_without_its_model_read() {
    let mock = MockTransport::new();
    let gw = fragile(&mock, Vec::new());
    let too_large = Fault::new(StatusCode::PAYLOAD_TOO_LARGE, "too_large", "too large");
    let broken = futures::stream::iter([Err::<http_body::Frame<bytes::Bytes>, _>(too_large)]);
    let mut req = http::Request::new(http_body_util::StreamBody::new(broken));
    *req.method_mut() = http::Method::POST;
    *req.uri_mut() = http::Uri::from_static(CHAT);

    let resp = gw.handle(req).await;
    let closed = Closed::of(&resp);
    let (status, headers, _) = collect(resp).await;

    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(headers["content-type"], "application/json");
    let end = closed.await.map(|o| o.end.map_err(|f| f.code().to_owned()));
    assert_eq!(end, Some(Err("too_large".to_owned())));
}

#[tokio::test]
async fn a_response_given_usage_is_not_read() {
    /// Adds usage to a call's response, then reads it.
    #[derive(Clone, Default)]
    struct Read(Arc<Mutex<Option<Fault>>>);

    #[async_trait]
    impl Plugin for Read {
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            let mut usage = Usage::default();
            usage.add(units::INPUT_TOKENS, 1.0);
            let read = cx.call("openai").await?.with_usage(usage);
            let fault = read.collect::<Reply>().await.err();
            self.0.lock().map(|mut f| *f = fault).ok();
            Ok(None)
        }
    }

    let read = Read::default();
    let h = Harness::new(vec![Arc::new(read.clone())], Some("backup"));
    h.mock
        .reply(OPENAI, mock::json(StatusCode::OK, UNARY))
        .reply(BACKUP, mock::json(StatusCode::OK, UNARY));

    assert_eq!(h.text(unary_chat()).await.0, StatusCode::OK);

    let fault = read.0.lock().ok().and_then(|mut f| f.take());
    assert_eq!(fault.as_ref().map(Fault::code), Some(codes::CONFIG));
}

#[tokio::test]
async fn a_providers_te_header_does_not_reach_the_client() {
    let h = relay();
    let mut reply = mock::json(StatusCode::OK, UNARY);
    reply
        .headers_mut()
        .insert("te", HeaderValue::from_static("trailers"));
    h.mock.reply(OPENAI, reply);

    let (_, headers, _) = h.send(unary_chat()).await;

    assert!(headers.get("te").is_none());
}

#[tokio::test]
async fn a_panicking_call_leaves_its_sibling_abandoned() {
    /// Never answers.
    struct Never;

    #[async_trait]
    impl Transport for Never {
        async fn send(&self, _req: http::Request<Body>) -> Result<http::Response<Body>, Fault> {
            std::future::pending().await
        }
    }

    /// Panics sending.
    struct Boom;

    #[async_trait]
    impl Transport for Boom {
        async fn send(&self, _req: http::Request<Body>) -> Result<http::Response<Body>, Fault> {
            panic!("a buggy transport")
        }
    }

    /// Calls both at once.
    struct Race;

    #[async_trait]
    impl Plugin for Race {
        async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            let (never, boom) = (cx.call("never"), cx.call("boom"));
            let _ = futures::join!(never.into_future(), boom.into_future());
            Ok(None)
        }
    }

    let ends = Ends::default();
    let providers = [
        Provider::new("never", OpenAiChat, Never),
        Provider::new("boom", OpenAiChat, Boom),
    ];
    let plugins: Vec<Arc<dyn Plugin>> = vec![Arc::new(ends.clone()), Arc::new(Race)];
    let gw = gateway(providers, plugins, None).unwrap_or_else(|e| panic!("gateway: {e}"));

    let (status, _, _) = collect(gw.handle(unary_chat()).await).await;
    gw.idle().await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let mut all = ends.all();
    all.sort();
    assert_eq!(all, ["boom internal_error", "client", "never abandoned"]);
}

#[tokio::test]
async fn a_client_leaving_mid_stream_ends_the_call_without_its_filters_end() {
    /// Notes each provider-edge filter end.
    #[derive(Clone, Default)]
    struct Note(Arc<Mutex<Vec<String>>>);

    struct Noted(Arc<Mutex<Vec<String>>>);

    impl Filter<Reply> for Noted {
        fn event(&mut self, event: Reply, out: &mut Vec<Reply>) -> Result<(), Fault> {
            out.push(event);
            Ok(())
        }

        fn end(&mut self, outcome: &Outcome, _out: &mut Vec<Reply>) -> Result<(), Fault> {
            let end = code(&outcome.end).to_owned();
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
    h.mock
        .reply(OPENAI, mock::sse(&[HI, &world, "data: [DONE]\n\n"]));

    let (_, mut body) = h.gw.handle(chat()).await.into_parts();
    let _ = http_body_util::BodyExt::frame(&mut body).await;
    drop(body);
    h.gw.idle().await;

    assert!(note.0.lock().map(|n| n.is_empty()).unwrap_or_default());
    assert_eq!(ends.all(), ["openai abandoned", "client"]);
}
