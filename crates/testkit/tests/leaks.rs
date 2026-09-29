//! Every request-scoped value is dropped once its request ends and the
//! gateway is idle, and a dropped gateway drops its plugins.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::{StreamExt, TryStreamExt, stream};
use http::StatusCode;
use respan_gateway_core::{
    Body, ClientContext, Exchange, Fault, Gateway, Plugin, Response, Transport,
};
use respan_gateway_core::{async_trait, bytes::Bytes, futures};
use respan_gateway_formats::OpenAiChat;
use respan_gateway_formats::conversation::{Finish, Reply};
use respan_gateway_http::http;
use respan_gateway_testkit::{mock, post, read_whole, serve};

const CHAT: &str = "/v1/chat/completions";
const WHOLE_REQ: &str = r#"{"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}"#;
const STREAM_REQ: &str =
    r#"{"model":"gpt-x","stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
const WHOLE: &str = r#"{"id":"c2","choices":[{"message":{"content":"hi"}}],"usage":{"prompt_tokens":4,"completion_tokens":1}}"#;
const STREAM: &[&str] = &[
    "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hel\"}}]}\n\n",
    "data: {\"id\":\"c1\",\"choices\":[],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2}}\n\n",
    "data: [DONE]\n\n",
];

/// Counts its drops.
#[derive(Clone, Default)]
struct Drops(Arc<AtomicUsize>);

impl Drops {
    fn count(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

/// A request-scoped value, such as an RAII guard; dropped, it counts.
struct Probe(Drops);

impl Drop for Probe {
    fn drop(&mut self) {
        self.0.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// How the plugin answers.
#[derive(Clone, Copy, Debug)]
enum Answer {
    /// Passes, so the gateway calls its target.
    Target,
    /// Calls `openai`, drops its response read to its content, and falls back to `backup`.
    Fallback,
    /// Answers with outputs of its own.
    Own,
}

/// Puts a probe in each request's extensions and answers as told; its own
/// drop counts in `plugin`.
struct Probing {
    request: Drops,
    plugin: Drops,
    answer: Answer,
}

impl Drop for Probing {
    fn drop(&mut self) {
        self.plugin.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl Plugin for Probing {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let probe = Arc::new(Probe(self.request.clone()));
        cx.extensions_mut().insert(probe);
        match self.answer {
            Answer::Target => Ok(None),
            Answer::Fallback => {
                let first = cx.call("openai").await?.until_content().await?;
                drop(first);
                Ok(Some(cx.call("backup").await?.until_content().await?))
            }
            Answer::Own => {
                let own = [Reply::text(0, "hi"), Reply::finished(Finish::Stop)];
                Ok(Some(Response::stream(stream::iter(
                    own.map(Ok::<_, Fault>),
                ))))
            }
        }
    }
}

/// Scripted chunks as a body.
fn body(chunks: Vec<Result<Bytes, Fault>>) -> Body {
    stream::iter(chunks).boxed()
}

/// A provider's answer, whole or streamed.
fn reply(whole: bool) -> mock::Reply {
    match whole {
        true => mock::json(StatusCode::OK, WHOLE),
        false => mock::sse(STREAM),
    }
}

/// A provider answering each call with `reply(whole)`, keeping nothing of
/// the call. `MockTransport` keeps each call, its extensions included, so
/// it would hold request-scoped values itself.
struct Scripted {
    whole: bool,
}

#[async_trait]
impl Transport for Scripted {
    type Ask = respan_gateway_http::Ask;
    type Answer = respan_gateway_http::Answer;

    async fn send(&self, call: Exchange<Self::Ask>) -> Result<Exchange<Self::Answer>, Fault> {
        let _: Vec<Bytes> = call.body.try_collect().await?;
        let (head, chunks) = reply(self.whole).into_parts();
        Ok(Exchange::new(head, body(chunks)))
    }
}

/// Run one request answered as `answer`, whole or streamed, its response
/// read to its end if `read`, else dropped unread: once it ended and the
/// gateway is idle, every request-scoped value is dropped, and once the
/// gateway is dropped, its plugin.
async fn check(answer: Answer, whole: bool, read: bool) {
    let case = format!("{answer:?}, whole: {whole}, read: {read}");
    let (request, plugin) = (Drops::default(), Drops::default());
    let probing = Probing {
        request: request.clone(),
        plugin: plugin.clone(),
        answer,
    };
    let gw = Gateway::new()
        .provider("openai", OpenAiChat, Scripted { whole })
        .and_then(|g| g.provider("backup", OpenAiChat, Scripted { whole }))
        .and_then(|g| g.serve(OpenAiChat))
        .and_then(|g| g.plugin("probing", probing))
        .and_then(|g| g.target("openai"))
        .unwrap_or_else(|f| panic!("{f}"));
    let req = post(CHAT, if whole { WHOLE_REQ } else { STREAM_REQ });
    let (resp, outcome) = serve(&gw, req).await;
    assert_eq!(resp.status(), StatusCode::OK, "{case}");
    if read {
        let (_, _, body) = read_whole(resp).await;
        assert!(body.is_ok(), "{case}");
    } else {
        drop(resp);
    }
    drop(outcome.await);
    gw.idle().await;
    assert_eq!(request.count(), 1, "the request leaked: {case}");
    assert_eq!(plugin.count(), 0, "{case}");
    drop(gw);
    assert_eq!(plugin.count(), 1, "the gateway leaked: {case}");
}

/// Every answer, whole or streamed, read or dropped unread.
async fn check_all() {
    for answer in [Answer::Target, Answer::Fallback, Answer::Own] {
        for (whole, read) in [(true, true), (false, true), (true, false), (false, false)] {
            check(answer, whole, read).await;
        }
    }
}

#[tokio::test]
async fn a_calls_whole_response_read_drops_its_request() {
    check(Answer::Target, true, true).await;
}

#[tokio::test]
async fn a_calls_streamed_response_read_drops_its_request() {
    check(Answer::Target, false, true).await;
}

#[tokio::test]
async fn a_calls_whole_response_dropped_unread_drops_its_request() {
    check(Answer::Target, true, false).await;
}

#[tokio::test]
async fn a_calls_streamed_response_dropped_unread_drops_its_request() {
    check(Answer::Target, false, false).await;
}

#[tokio::test]
async fn a_fallback_drops_its_request_and_the_call_it_dropped() {
    for (whole, read) in [(true, true), (false, true), (true, false), (false, false)] {
        check(Answer::Fallback, whole, read).await;
    }
}

#[tokio::test]
async fn a_plugins_own_answer_drops_its_request() {
    for (whole, read) in [(true, true), (false, true), (true, false), (false, false)] {
        check(Answer::Own, whole, read).await;
    }
}

/// End hooks run on other threads, so `idle` must not resolve before what
/// they hold is dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn requests_on_many_threads_drop_theirs_by_idle() {
    for _ in 0..50 {
        check_all().await;
    }
}
