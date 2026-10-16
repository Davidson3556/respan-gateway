//! Every request-scoped value is dropped once its request ends and the
//! gateway is idle, and a dropped gateway drops its plugins.

use respan_gateway_conversation::Input;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::{TryStreamExt, stream};
use http::StatusCode;
use respan_gateway_conversation::{Block, Finish, Output};
use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_core::{ClientContext, Fault, Gateway, Plugin, Provider, Response};
use respan_gateway_core::{async_trait, futures};
use respan_gateway_http::Body;
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::http;
#[allow(dead_code, unused_imports)]
#[path = "../../../../tests/support/http.rs"]
mod http_helpers;
use http_helpers::{LIMIT, post, read_whole, serve};

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
                let mut first = cx.call("openai").await?;
                first.buffer_until(Output::is_content).await?;
                drop(first);
                let mut backup = cx.call("backup").await?;
                backup.buffer_until(Output::is_content).await?;
                Ok(Some(backup))
            }
            Answer::Own => {
                let own = [
                    Output::block_start(0, Block::Text),
                    Output::text(0, "hi"),
                    Output::block_end(0),
                    Output::finished(Finish::Stop),
                ];
                Ok(Some(Response::stream(stream::iter(
                    own.map(Ok::<_, Fault>),
                ))))
            }
        }
    }
}

/// A provider answering each call whole or streamed, from exact in-memory
/// chunks, retaining neither request metadata nor extensions: ready chunks
/// isolate the lifecycle under test from socket buffering and scheduling.
async fn scripted(call: http::Request<Body>, whole: bool) -> respan_gateway_http::Sent {
    let _: Vec<Bytes> = call.into_body().try_collect().await?;
    let (content_type, chunks): (_, Vec<_>) = match whole {
        true => ("application/json", vec![WHOLE]),
        false => ("text/event-stream", STREAM.to_vec()),
    };
    let chunks = chunks
        .into_iter()
        .map(|c| Ok(Bytes::from_static(c.as_bytes())));
    let mut response = http::Response::new(Body::stream(stream::iter(chunks)));
    let content_type = http::HeaderValue::from_static(content_type);
    response
        .headers_mut()
        .insert(http::header::CONTENT_TYPE, content_type);
    Ok((response, None))
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
    let send = move |request| scripted(request, whole);
    let provider =
        |name| Provider::new(name, OpenAiChat, respan_gateway_http::connect(send, LIMIT));
    let gw = Gateway::new()
        .provider(provider("openai"))
        .and_then(|g| g.provider(provider("backup")))
        .and_then(|g| g.client::<Input, _, _>(OpenAiChat))
        .map(|g| g.plugin(probing))
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

/// The target's answer, a fallback's (which drops the call it falls back
/// from) and a plugin's own, each whole or streamed, read or dropped.
#[tokio::test]
async fn every_answer_drops_its_request_and_the_gateway_its_plugin() {
    check_all().await;
}

/// End hooks run on other threads, so `idle` must not resolve before what
/// they hold is dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn requests_on_many_threads_drop_theirs_by_idle() {
    for _ in 0..50 {
        check_all().await;
    }
}
