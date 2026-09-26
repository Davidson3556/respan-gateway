//! The kernel's promises, driven through the OpenAI format.

use respan_gateway_core::{bytes, futures, http, http_body, http_body_util, serde_json};
use std::future::IntoFuture;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use http::{HeaderMap, HeaderValue, StatusCode};
use respan_gateway_core::async_trait;
use respan_gateway_core::{
    Closed, Context, Fault, Filter, Gateway, Outcome, Plugin, Provider, Response, Route, Usage,
    codes,
};
use respan_gateway_formats::OpenAiChat;
use respan_gateway_formats::conversation::{Conversation, Event, Finish, Message, Role, units};
use respan_gateway_testkit::{MockTransport, Outcomes, collect, mock, post};

mod common;
use common::{
    Call, Disclaimer, Flip, Footer, Harness, HoldAll, Nameless, OPENAI, Shout, Silence, Unmask,
    call,
};
use serde_json::json;

const BACKUP: &str = "http://backup.test";

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
    call("chat", STREAM_REQ)
}

fn unary_chat() -> Call {
    call("chat", UNARY_REQ)
}

/// An output guardrail: fails the response with 403 `blocked` at the first
/// event it catches.
struct Guard(fn(&Event) -> bool);

#[async_trait]
impl Plugin for Guard {
    async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
        let catches = self.0;
        cx.filter(move |e: &mut Event| {
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
    async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
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
    async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
        let text = self.0.clone();
        cx.filter(move |e: &mut Event| {
            if let (Event::Text { text: event, .. }, Ok(mut t)) = (e, text.lock()) {
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
    async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
        cx.request_mut::<Conversation>()?.temperature = Some(0.0);
        Ok(None)
    }
}

struct Refuse(Fault);

#[async_trait]
impl Plugin for Refuse {
    async fn on_request(&self, _cx: &mut Context) -> Result<Option<Response>, Fault> {
        Err(self.0.clone())
    }
}

/// Never finishes its request hook.
struct Hang(Outcomes);

#[async_trait]
impl Plugin for Hang {
    async fn on_request(&self, _cx: &mut Context) -> Result<Option<Response>, Fault> {
        std::future::pending().await
    }

    async fn on_close(&self, cx: &Context, outcome: &Outcome) {
        self.0.on_close(cx, outcome).await;
    }
}

struct PanicOnClose;

#[async_trait]
impl Plugin for PanicOnClose {
    async fn on_close(&self, _cx: &Context, _outcome: &Outcome) {
        panic!("a buggy plugin");
    }
}

/// Answers from the first provider that succeeds.
struct Fallback(Vec<&'static str>);

#[async_trait]
impl Plugin for Fallback {
    async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
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
    async fn on_request(&self, _cx: &mut Context) -> Result<Option<Response>, Fault> {
        let answer = [Event::start("cached", "gpt-x"), Event::text(0, "hi")];
        let mut cost = Usage::default();
        cost.add(units::OUTPUT_TOKENS, 1.0);
        let answer = futures::stream::iter(answer.map(Ok));
        Ok(Some(
            Response::stream(http::Response::new(answer)).with_usage(cost),
        ))
    }
}

/// A provider with its own key, or with only a header that is not one.
fn provider(name: &str, base_url: &str, key: Option<&'static str>) -> Provider {
    let mut headers = HeaderMap::new();
    match key {
        Some(k) => headers.insert("authorization", HeaderValue::from_static(k)),
        None => headers.insert("x-team", HeaderValue::from_static("t")),
    };
    Provider::new(name, OpenAiChat, base_url).with_headers(headers)
}

fn route(chain: Vec<Arc<dyn Plugin>>, target: Option<&str>) -> Route {
    let route = chain
        .into_iter()
        .fold(Route::new("chat", OpenAiChat), Route::with_plugin);
    match target {
        Some(t) => route.with_target(t),
        None => route,
    }
}

impl Harness {
    /// A `chat` route: an [`Outcomes`] recorder, then `chain`, then `target`.
    fn new(chain: Vec<Arc<dyn Plugin>>, target: Option<&str>) -> Self {
        let log = Outcomes::default();
        let mock = MockTransport::new();
        let mut plugins: Vec<Arc<dyn Plugin>> = vec![Arc::new(log.clone())];
        plugins.extend(chain);
        let gw = Gateway::new(
            mock.clone(),
            [
                provider("openai", OPENAI, Some("Bearer sk-test")),
                provider("backup", BACKUP, None),
            ],
            [route(plugins, target)],
        )
        .unwrap_or_else(|e| panic!("gateway: {e}"));
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
    // A provider's own credentials may use other header names, so the
    // client's never join them; feature headers always go. A provider with
    // none configured gets the client's, as a local subscription proxy needs.
    for (target, base, auth, org) in [
        ("openai", OPENAI, Some("Bearer sk-test"), None),
        (
            "backup",
            BACKUP,
            Some("Bearer client-key"),
            Some("client-org"),
        ),
    ] {
        let h = Harness::new(Vec::new(), Some(target));
        let mut req = post(UNARY_REQ);
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
        let (status, _, _) = h.send(("chat", req)).await;
        assert_eq!(status, StatusCode::OK, "{target}");
        let sent = h.mock.sent();
        let got = |name| sent[0].headers().get(name).and_then(|v| v.to_str().ok());
        assert_eq!(got("authorization"), auth, "{target}");
        assert_eq!(got("openai-organization"), org, "{target}");
        assert_eq!(got("openai-beta"), Some("assistants=v2"), "{target}");
    }
}

#[tokio::test]
async fn a_plugin_asks_a_model_its_own_question_and_reads_the_answer() {
    /// Refuses what a judge model calls unsafe.
    struct Judge;

    #[async_trait]
    impl Plugin for Judge {
        async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            let mut prompt = cx.request::<Conversation>()?.clone();
            let rule = Message::new(Role::System, "Answer SAFE or UNSAFE.");
            prompt.messages.insert(0, rule);
            let call = cx.call("openai").model("judge-1").request(prompt);
            let verdict = call.await?.collect::<Event>().await?;
            let unsafe_ = |e: &Event| matches!(e, Event::Text { text, .. } if text == "UNSAFE");
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

    impl Filter<Event> for Seen {
        fn event(&mut self, event: Event, out: &mut Vec<Event>) -> Result<(), Fault> {
            out.push(event);
            Ok(())
        }

        fn end(&mut self, outcome: &Outcome, _out: &mut Vec<Event>) -> Result<(), Fault> {
            if let Ok(mut seen) = self.0.lock() {
                *seen = outcome.status;
            }
            Ok(())
        }
    }

    #[async_trait]
    impl Plugin for Mask {
        async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
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
    // A filter's `end` sees the provider's status; `on_close` the one sent.
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
        async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            let seen = std::cell::Cell::new(0);
            cx.filter(move |e: &mut Event| {
                if let Event::Text { text, .. } = e {
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
async fn a_client_that_leaves_mid_response_is_closed_once() {
    let h = relay();
    h.mock.reply(OPENAI, mock::sse(STREAM));

    let (_, mut body) = {
        let (route, req) = chat();
        h.gw.handle(route, req)
    }
    .await
    .into_parts();
    let _first = http_body_util::BodyExt::frame(&mut body).await;
    drop(body);
    h.gw.idle().await;

    let o = h.log.only();
    assert_eq!(o.status, Some(StatusCode::OK));
    assert!(matches!(o.end, Err(f) if f.code() == codes::ABANDONED));
}

#[tokio::test]
async fn a_client_that_leaves_before_a_response_is_closed_once() {
    let hang = Outcomes::default();
    let h = Harness::new(vec![Arc::new(Hang(hang.clone()))], Some("openai"));

    let gave_up = tokio::time::timeout(Duration::from_millis(10), {
        let (route, req) = chat();
        h.gw.handle(route, req)
    })
    .await;
    assert!(gave_up.is_err());
    h.gw.idle().await;

    let o = h.log.only();
    assert_eq!(o.status, None);
    assert!(matches!(o.end, Err(f) if f.code() == codes::ABANDONED));
    assert_eq!(hang.all().len(), 1, "the plugin cut short is closed too");
}

#[tokio::test]
async fn only_plugins_that_ran_are_closed() {
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
async fn a_panicking_close_hook_does_not_skip_the_others() {
    let after = Outcomes::default();
    let h = Harness::new(
        vec![Arc::new(PanicOnClose), Arc::new(after.clone())],
        Some("openai"),
    );
    h.mock.reply(OPENAI, mock::sse(STREAM));

    let _ = h.send(chat()).await;

    assert_eq!(after.only().end, Ok(()));
}

#[tokio::test]
async fn unknown_routes_are_404() {
    let h = relay();
    let (status, headers, _) = h.send(call("nope", "{}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(headers["content-type"], "text/plain; charset=utf-8");
    assert!(h.log.all().is_empty());
}

#[tokio::test]
async fn routes_that_cannot_work_fail_at_construction() {
    let openai = || provider("openai", OPENAI, None);
    let cases = [
        (
            vec![],
            vec![route(Vec::new(), Some("missing"))],
            "route 'chat': unknown provider 'missing'",
        ),
        (
            vec![],
            vec![route(Vec::new(), None)],
            "route 'chat' has no plugins and no target",
        ),
        (
            vec![openai(), openai()],
            vec![],
            "provider 'openai' is defined twice",
        ),
        (
            vec![openai()],
            vec![
                route(Vec::new(), Some("openai")),
                route(Vec::new(), Some("openai")),
            ],
            "route 'chat' is defined twice",
        ),
        (
            vec![openai().with_headers(HeaderMap::from_iter([(
                http::header::CONTENT_LENGTH,
                HeaderValue::from_static("4"),
            )]))],
            vec![],
            "provider 'openai': header 'content-length' is the connection's",
        ),
    ];
    for (providers, routes, expected) in cases {
        let err = Gateway::new(MockTransport::new(), providers, routes).err();
        assert_eq!(err.as_ref().map(Fault::message), Some(expected));
    }
    // A base URL may hold a key, so it is not said back.
    let bad = "provider 'p': bad base URL: an absolute one its transport reaches, with no query, fragment or `@` (credentials go in its headers)";
    for url in [
        "api.openai.com",
        "https://h.test?api-version=1",
        "https://h.test/#f",
        "https://user:ab/cd@h.test",
        "https://key:sec ret@h.test",
        "https://key:secret@h.test",
        "https://:443",
    ] {
        let err = Gateway::new(MockTransport::new(), [provider("p", url, None)], []).err();
        assert_eq!(err.as_ref().map(Fault::message), Some(bad), "{url}");
    }
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

    let body = h.body(call("chat", request)).await;

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

    let body = h.body(call("chat", request)).await;

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

    let body = h.body(call("chat", request)).await;

    assert_eq!(body, filter("") + "data: [DONE]\n\n");
}

#[tokio::test]
async fn caller_values_reach_plugins_by_type() {
    #[derive(Clone)]
    struct Tenant(&'static str);

    struct NeedsTenant;

    #[async_trait]
    impl Plugin for NeedsTenant {
        async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            match cx.extensions().get::<Tenant>() {
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
    let mut req = post(UNARY_REQ);
    req.extensions_mut().insert(Tenant("acme"));
    let (acme, _, _) = h.send(("chat", req)).await;

    assert_eq!((anonymous, acme), (StatusCode::FORBIDDEN, StatusCode::OK));
}

#[tokio::test]
async fn a_panic_in_a_filter_is_refused_as_internal() {
    struct Explode;

    #[async_trait]
    impl Plugin for Explode {
        async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            cx.filter(|_: &mut Event| panic!("a buggy filter"))?;
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
        async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
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

    let (status, headers, body) = h.send(("chat", http::Request::new(body))).await;

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
        async fn on_request(&self, _cx: &mut Context) -> Result<Option<Response>, Fault> {
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

    let (status, _, _) = h.send(call("chat", request.to_string())).await;

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
        async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            cx.filter(|e: &mut Event| {
                if let Event::Start { id, .. } = e {
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
        async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            cx.filter(|_: &mut Event| Ok(()))?;
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

    let world = |e: &Event| matches!(e, Event::Text { text, .. } if text.contains("world"));
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

    /// Changes the model and records what `on_close` finds in the
    /// extensions.
    #[derive(Clone, Default)]
    struct Redirect(Arc<Mutex<Option<&'static str>>>);

    #[async_trait]
    impl Plugin for Redirect {
        async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            assert_eq!(cx.head().method, http::Method::POST);
            assert_eq!(
                cx.body()?.json().map(|v| v["model"].clone()),
                Some(json!("gpt-x"))
            );
            let said = cx.request::<Conversation>()?.messages[0].text();
            cx.extensions_mut()
                .insert(Seen(if said == "hi" { "hi" } else { "other" }));
            cx.set_model("gpt-small");
            Ok(None)
        }

        async fn on_close(&self, cx: &Context, _outcome: &Outcome) {
            if let Ok(mut seen) = self.0.lock() {
                *seen = cx.extensions().get::<Seen>().map(|s| s.0);
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
        async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            cx.filter(|e: &mut Event| {
                match e {
                    Event::Text { text, .. } => *text = text.to_uppercase(),
                    Event::Finish(f) => *f = Finish::Length,
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
        async fn on_request(&self, _cx: &mut Context) -> Result<Option<Response>, Fault> {
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
    let text = |e: &Event| matches!(e, Event::Text { .. });
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
async fn close_hooks_cannot_call_providers() {
    #[derive(Clone, Default)]
    struct Late(Arc<Mutex<Option<String>>>);

    #[async_trait]
    impl Plugin for Late {
        async fn on_close(&self, cx: &Context, _outcome: &Outcome) {
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
        Some("providers cannot be called once the request has ended")
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
    let gw = Gateway::new(
        Silent,
        [provider("openai", OPENAI, None)],
        [route(vec![Arc::new(log.clone())], Some("openai"))],
    )
    .unwrap_or_else(|e| panic!("{e}"));

    let gave_up = tokio::time::timeout(Duration::from_millis(10), {
        let (route, req) = chat();
        gw.handle(route, req)
    })
    .await;
    assert!(gave_up.is_err());
    gw.idle().await;

    assert_eq!(log.only().status, None);
}

#[test]
fn close_hooks_run_without_a_tokio_runtime() {
    let mock = MockTransport::new();
    let log = Outcomes::default();
    let gw = Gateway::new(
        mock.clone(),
        [provider("openai", OPENAI, None)],
        [route(vec![Arc::new(log.clone())], Some("openai"))],
    )
    .unwrap_or_else(|e| panic!("{e}"));
    mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));

    futures::executor::block_on(async {
        let _ = collect(
            {
                let (route, req) = unary_chat();
                gw.handle(route, req)
            }
            .await,
        )
        .await;
        gw.idle().await;
    });

    assert_eq!(log.only().end, Ok(()));
}

#[tokio::test]
async fn a_router_can_set_the_target_and_let_the_chain_run_on() {
    struct Pick;

    #[async_trait]
    impl Plugin for Pick {
        async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
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
        async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
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

    impl Filter<Event> for Fill {
        fn event(&mut self, event: Event, out: &mut Vec<Event>) -> Result<(), Fault> {
            if let Event::Text { text, .. } = &event {
                self.0.push_str(text);
            }
            out.push(event);
            Ok(())
        }

        fn end(&mut self, outcome: &Outcome, _out: &mut Vec<Event>) -> Result<(), Fault> {
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
        async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
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

    impl Filter<Event> for Split {
        fn event(&mut self, event: Event, out: &mut Vec<Event>) -> Result<(), Fault> {
            match event {
                Event::Text { block, text, .. } if text.len() > 1 => {
                    let (a, b) = text.split_at(1);
                    out.extend([Event::text(block, a), Event::text(block, b)]);
                }
                event => out.push(event),
            }
            Ok(())
        }
    }

    struct Splitter;

    #[async_trait]
    impl Plugin for Splitter {
        async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
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
        async fn on_request(&self, _cx: &mut Context) -> Result<Option<Response>, Fault> {
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
    struct Footer(Option<Event>);

    impl Filter<Event> for Footer {
        fn event(&mut self, event: Event, out: &mut Vec<Event>) -> Result<(), Fault> {
            match event {
                Event::Finish(_) => self.0 = Some(event),
                event => out.push(event),
            }
            Ok(())
        }

        fn end(&mut self, _outcome: &Outcome, out: &mut Vec<Event>) -> Result<(), Fault> {
            out.push(Event::text(0, " [footer]"));
            out.extend(self.0.take());
            Ok(())
        }
    }

    struct Foot;

    #[async_trait]
    impl Plugin for Foot {
        async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
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

fn filtered<F: Filter<Event> + Clone + Sync>(filter: F) -> Harness {
    struct Adds<F>(F);

    #[async_trait]
    impl<F: Filter<Event> + Clone + Sync> Plugin for Adds<F> {
        async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
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
    let h = filtered(|e: &mut Event| {
        if let Event::Start { model, .. } = e {
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
    let (route, req) = chat();

    let resp = h.gw.handle(route, req).await;
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

    let resp = h.gw.handle("nowhere", post(UNARY_REQ)).await;

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

    let resp = h.gw.handle("chat", post(UNARY_REQ)).await;

    assert_eq!(resp.version(), http::Version::default());
}

#[tokio::test]
async fn a_plugin_races_two_calls_and_keeps_the_first() {
    struct Hedge;

    #[async_trait]
    impl Plugin for Hedge {
        async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
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

    impl Filter<Event> for Plain {
        fn event(&mut self, event: Event, out: &mut Vec<Event>) -> Result<(), Fault> {
            if !matches!(event, Event::Other { .. }) {
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
async fn the_clients_key_stays_off_a_provider_keyed_by_another_header() {
    for sensitive in [true, false] {
        let mut key = HeaderValue::from_static("azure-secret");
        key.set_sensitive(sensitive);
        // `x-custom-key` is no known key name: only the mark flags it.
        let name = if sensitive { "x-custom-key" } else { "api-key" };
        let mut headers = HeaderMap::new();
        headers.insert(http::HeaderName::from_static(name), key);
        let mock = MockTransport::new();
        let gw = Gateway::new(
            mock.clone(),
            [Provider::new("azure", OpenAiChat, OPENAI).with_headers(headers)],
            [route(Vec::new(), Some("azure"))],
        )
        .expect("builds");
        mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));
        let mut req = post(UNARY_REQ);
        let client_key = HeaderValue::from_static("Bearer client-key");
        req.headers_mut().insert("authorization", client_key);

        let _ = collect(gw.handle("chat", req).await).await;

        let sent = mock.sent();
        assert!(sent[0].headers().get("authorization").is_none(), "{name}");
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
        async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            let got = cx.call("openai").await?.collect::<String>().await;
            assert_eq!(
                got.err().map(|f| f.code().to_owned()),
                Some(codes::WRONG_KIND.to_owned())
            );
            // Its own kind collects, though empty.
            let got = cx.call("openai").await?.collect::<Event>().await;
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
async fn a_plugin_shared_by_routes_knows_which_it_runs_on() {
    struct Seen(Arc<Mutex<Vec<String>>>);

    #[async_trait]
    impl Plugin for Seen {
        async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            if let Ok(mut seen) = self.0.lock() {
                seen.push(cx.route().to_owned());
            }
            Ok(None)
        }
    }

    let seen = Arc::new(Mutex::new(Vec::new()));
    let shared = Arc::new(Seen(seen.clone()));
    let mock = MockTransport::new();
    let gw = Gateway::new(
        mock.clone(),
        [provider("openai", OPENAI, Some("Bearer sk-test"))],
        ["a", "b"].map(|name| {
            Route::new(name, OpenAiChat)
                .with_plugin(shared.clone())
                .with_target("openai")
        }),
    )
    .expect("builds");
    for name in ["a", "b"] {
        mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));
        let _ = collect(gw.handle(name, post(UNARY_REQ)).await).await;
    }

    assert_eq!(
        seen.lock().map(|s| s.clone()).ok(),
        Some(vec!["a".to_owned(), "b".to_owned()])
    );
}

#[tokio::test]
async fn the_clients_credentials_go_once() {
    let h = Harness::new(Vec::new(), Some("backup"));
    let mut req = post(UNARY_REQ);
    let headers = req.headers_mut();
    headers.insert(
        "authorization",
        HeaderValue::from_static("Bearer client-key"),
    );
    headers.insert("openai-organization", HeaderValue::from_static("org-1"));
    h.mock.reply(BACKUP, mock::json(StatusCode::OK, UNARY));

    let _ = h.send(("chat", req)).await;

    let sent = h.mock.sent();
    for name in ["authorization", "openai-organization"] {
        assert_eq!(sent[0].headers().get_all(name).iter().count(), 1, "{name}");
    }
}

#[tokio::test]
async fn a_plugins_own_request_keeps_what_the_client_sent_in_its_format() {
    /// Asks again with a trimmed copy of the client's request.
    struct Trim;

    #[async_trait]
    impl Plugin for Trim {
        async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            let mut trimmed = cx.request::<Conversation>()?.clone();
            trimmed.messages.truncate(1);
            cx.call("openai").request(trimmed).await.map(Some)
        }
    }

    let h = Harness::new(vec![Arc::new(Trim)], None);
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));
    let body = json!({"model": "gpt-x", "tools": [{"type": "function", "function": {"name": "f"}}],
        "messages": [{"role": "user", "content": "a"}, {"role": "user", "content": "b"}]});

    let (status, _, _) = h.send(call("chat", body.to_string())).await;

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
        async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            let said = cx.request::<Conversation>()?.messages[0].text();
            let prompt = Conversation::new([
                Message::new(Role::System, "Answer SAFE or UNSAFE."),
                Message::new(Role::User, said),
            ]);
            let _verdict = cx
                .call("openai")
                .model("judge-1")
                .request(prompt)
                .await?
                .collect::<Event>()
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

    let (status, _, _) = h.send(call("chat", body.to_string())).await;

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
        async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            let prompt = Conversation::new([Message::new(Role::User, "safe?")]);
            let verdict = cx
                .call("openai")
                .request(prompt)
                .await?
                .collect::<Event>()
                .await?;
            assert!(
                verdict
                    .iter()
                    .any(|e| matches!(e, Event::Text { text, .. } if text == "hi"))
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
async fn a_whole_answer_to_a_plugins_own_request_reaches_a_streaming_client_as_a_stream() {
    struct Trim;

    #[async_trait]
    impl Plugin for Trim {
        async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            let own = Conversation::new([Message::new(Role::User, "hi")]);
            Ok(Some(cx.call("openai").request(own).await?))
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

    let body = h.body(call("chat", quiet)).await;

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
        async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            let verdict = cx.call("openai").await?.collect::<Event>().await;
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
        async fn on_request(&self, _cx: &mut Context) -> Result<Option<Response>, Fault> {
            let events = [Ok(Event::text(0, "later"))];
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
async fn a_redirect_answering_a_plugins_own_request_is_a_bad_gateway() {
    struct Trim;

    #[async_trait]
    impl Plugin for Trim {
        async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            let own = Conversation::new([Message::new(Role::User, "hi")]);
            Ok(Some(cx.call("openai").request(own).await?))
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
        async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            let answer = cx.call("openai").await?.collect::<Event>().await;
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
async fn a_failure_answering_a_plugins_own_request_keeps_its_status_and_retry_after() {
    struct Trim;

    #[async_trait]
    impl Plugin for Trim {
        async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            let own = Conversation::new([Message::new(Role::User, "hi")]);
            Ok(Some(cx.call("openai").request(own).await?))
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
        async fn on_request(&self, _cx: &mut Context) -> Result<Option<Response>, Fault> {
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

    let (route, req) = chat();
    let resp = h.gw.handle(route, req).await;
    let closed = Closed::of(&resp);
    drop(h);
    let (_, _, body) = collect(resp).await;

    assert_eq!(body.ok().as_deref(), Some(STREAM.concat().as_bytes()));
    assert_eq!(closed.await.map(|o| o.end), Some(Ok(())));
}

#[tokio::test]
async fn a_providers_trailers_are_not_passed_on_yet() {
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
    let gw = Gateway::new(
        Trailed,
        [provider("openai", OPENAI, None)],
        [route(vec![Arc::new(log.clone())], Some("openai"))],
    )
    .unwrap_or_else(|e| panic!("{e}"));

    let (route, req) = unary_chat();
    let resp = gw.handle(route, req).await;
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
        async fn on_request(&self, _cx: &mut Context) -> Result<Option<Response>, Fault> {
            let own: Response = http::Response::new(UNARY.to_owned()).into();
            let read = own.collect::<Event>().await.err();
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
        async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            let chat = cx.request::<Conversation>()?;
            if !chat.messages.iter().all(Message::is_plain) {
                let why = "content the guard cannot read";
                return Err(Fault::new(StatusCode::FORBIDDEN, "unreadable", why));
            }
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Guard)], Some("openai"));
    let image = r#"{"model":"m","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"x"}}]}]}"#;

    let (status, _, _) = h.send(call("chat", image)).await;

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
        async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            drop(cx.call("openai").await?);
            Cached.on_request(cx).await
        }
    }

    let dropped = Arc::new(AtomicBool::new(false));
    let log = Outcomes::default();
    let gw = Gateway::new(
        Endless(dropped.clone()),
        [provider("openai", OPENAI, None)],
        [route(vec![Arc::new(log.clone()), Arc::new(Discard)], None)],
    )
    .unwrap_or_else(|e| panic!("{e}"));

    let (route, req) = chat();
    let (status, _, _) = collect(gw.handle(route, req).await).await;
    gw.idle().await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(log.only().end, Ok(()));
    assert!(dropped.load(Ordering::SeqCst));
}
