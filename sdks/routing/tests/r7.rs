//! Probes of the round-7 core: a response with no body to wait for goes
//! out at once; does `ready()` agree?

mod common;

use std::time::Duration;

use common::*;
use respan_gateway_core::bytes::Bytes;
use respan_gateway_core::futures::stream;
use respan_gateway_core::http::{self, StatusCode};
use respan_gateway_core::http_body::Frame;
use respan_gateway_core::http_body_util::{BodyExt, StreamBody};
use respan_gateway_core::{
    Body, Decoder, Doc, Encoder, Fault, Format, Gateway, Transport, async_trait,
};
use respan_gateway_formats::{OpenAiChat, bytes_decoder, bytes_encoder, pass_headers};
use respan_gateway_routing::fallback::Fallback;
use respan_gateway_routing::hedge::Hedge;
use respan_gateway_routing::retry::Backoff;
use respan_gateway_testkit::MockTransport;

fn no_content() -> respan_gateway_testkit::mock::Reply {
    let mut reply = http::Response::new(Vec::new());
    *reply.status_mut() = StatusCode::NO_CONTENT;
    reply
}

fn gateway(mock: &MockTransport, plugin: impl respan_gateway_core::Plugin) -> Gateway {
    Gateway::new()
        .provider("openai", OpenAiChat, mock.at(OPENAI))
        .and_then(|g| g.provider("backup", OpenAiChat, mock.at(BACKUP)))
        .and_then(|g| g.serve(OpenAiChat)?.plugin("p", plugin))
        .and_then(|g| g.target("openai"))
        .expect("gateway")
}

/// With no plugin, a provider's 204 is relayed at once: its body is never
/// read (F7-1).
#[tokio::test]
async fn a_204_is_relayed_at_once() {
    let mock = MockTransport::new();
    let gw = gateway(&mock, Log::default());
    mock.reply(OPENAI, no_content());

    let (status, _, body) = send(&gw, chat("gpt-x", false, "hi")).await;

    assert_eq!(
        (status, body.ok().as_deref()),
        (StatusCode::NO_CONTENT, Some(""))
    );
}

/// Y7-1, fixed by Z8-2: the same 204 behind the fallback. `ready()` now
/// shares `send`'s no-body rule, so the 204 is ready at once and is the
/// answer; the backup is never asked.
#[tokio::test]
async fn a_204_behind_the_fallback_is_the_answer() {
    let mock = MockTransport::new();
    let fallback = Fallback::new(Backoff::once()).then("backup", None);
    let gw = gateway(&mock, fallback);
    mock.reply(OPENAI, no_content())
        .reply(BACKUP, json_reply(StatusCode::OK, &openai_json("late")));

    let (status, _, _) = send(&gw, chat("gpt-x", false, "hi")).await;

    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(mock.sent().len(), 1);
}

/// The same, raced: the primary's 204 wins the hedge (fixed by Z8-2).
#[tokio::test]
async fn a_204_wins_the_hedge() {
    let mock = MockTransport::new();
    let hedge = Hedge::new("openai", "backup", Duration::from_secs(5));
    let gw = gateway(&mock, hedge);
    mock.reply(OPENAI, no_content())
        .reply(BACKUP, json_reply(StatusCode::OK, &openai_json("late")));

    let (status, _, _) = send(&gw, chat("gpt-x", false, "hi")).await;

    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(mock.sent().len(), 1);
}

/// A format of no kind at `/any` that relays the client's method.
struct Any;

impl Format for Any {
    type Input = ();
    type Output = ();

    fn name(&self) -> &str {
        "any"
    }

    fn accepts(&self, head: &http::request::Parts) -> bool {
        head.uri.path() == "/any"
    }

    fn decode_input(&self, _: &http::request::Parts, _: &Doc) -> Result<(), Fault> {
        Ok(())
    }

    fn encode_input(&self, _: &(), _: Option<(&Doc, &())>) -> Result<Doc, Fault> {
        Ok(Doc::default())
    }

    fn prepare(
        &self,
        body: Doc,
        _: Option<&()>,
        client: &http::request::Parts,
        relayed: bool,
    ) -> Result<http::Request<Doc>, Fault> {
        let mut req = http::Request::new(body);
        *req.method_mut() = client.method.clone();
        *req.uri_mut() = http::Uri::from_static("/any");
        pass_headers(&mut req, relayed.then_some(client));
        Ok(req)
    }

    fn decoder(&self, head: &http::response::Parts, _: &http::Request<Doc>) -> Box<dyn Decoder> {
        bytes_decoder(head)
    }

    fn encoder(
        &self,
        head: &mut http::response::Parts,
        _: &http::Request<Doc>,
        relayed: bool,
    ) -> Box<dyn Encoder> {
        bytes_encoder(head, relayed)
    }
}

/// A 200 with a length whose body never comes, as a transport that does
/// not know the request was a `HEAD` would give.
#[derive(Debug, Clone)]
struct HeadOnly;

#[async_trait]
impl Transport for HeadOnly {
    async fn send(&self, _call: http::Request<Body>) -> Result<http::Response<Body>, Fault> {
        let never = stream::pending::<Result<Frame<Bytes>, Fault>>();
        let mut resp = http::Response::new(StreamBody::new(never).boxed_unsync());
        resp.headers_mut()
            .insert(http::header::CONTENT_LENGTH, 42.into());
        Ok(resp)
    }
}

fn head_gateway(plugin: impl respan_gateway_core::Plugin) -> Gateway {
    Gateway::new()
        .provider("a", Any, HeadOnly)
        .and_then(|g| g.provider("b", Any, HeadOnly))
        .and_then(|g| g.serve(Any)?.plugin("p", plugin))
        .and_then(|g| g.target("a"))
        .expect("gateway")
}

fn head() -> http::Request<Body> {
    let mut req = http::Request::new(Body::from(Doc::default()));
    *req.method_mut() = http::Method::HEAD;
    *req.uri_mut() = http::Uri::from_static("/any");
    req
}

/// A `HEAD` with no plugin goes out at once, its body never read (F7-1).
#[tokio::test]
async fn a_head_is_answered_at_once() {
    let gw = head_gateway(Log::default());

    let answered = tokio::time::timeout(Duration::from_secs(1), send(&gw, head())).await;

    assert_eq!(answered.map(|(s, ..)| s).ok(), Some(StatusCode::OK));
}

/// Y7-1 again, fixed by Z8-2: behind the fallback, `ready()` no longer
/// waits for the first byte of a body a `HEAD` never has.
#[tokio::test]
async fn a_head_behind_the_fallback_is_answered_at_once() {
    let gw = head_gateway(Fallback::new(Backoff::once()).then("b", None));

    let answered = tokio::time::timeout(Duration::from_secs(1), send(&gw, head())).await;

    assert_eq!(answered.map(|(s, ..)| s).ok(), Some(StatusCode::OK));
}
