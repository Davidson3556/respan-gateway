//! Probes of what changed in the core since round 2: responses of no kind
//! and `ready()`, calls of another kind, `owned()`, and framed requests.

mod common;

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use common::*;
use respan_gateway_core::bytes::{Bytes, BytesMut};
use respan_gateway_core::futures::{StreamExt, TryStreamExt, stream};
use respan_gateway_core::http::header::UPGRADE;
use respan_gateway_core::http::{self, HeaderValue, StatusCode};
use respan_gateway_core::http_body::Frame;
use respan_gateway_core::http_body_util::{BodyExt, StreamBody};
use respan_gateway_core::serde_json::{self, Value, json};
use respan_gateway_core::{
    Body, ClientContext, Decoder, Doc, Encoder, Fault, Format, Gateway, Item, Part, Plugin,
    ProviderContext, Response, Transport, async_trait,
};
use respan_gateway_formats::conversation::{Conversation, Message, Reply};
use respan_gateway_formats::{OpenAiChat, bytes_decoder, bytes_encoder, posts_to};
use respan_gateway_routing::fallback::Fallback;
use respan_gateway_routing::retry::Backoff;
use respan_gateway_testkit::{MockTransport, post, read_whole, streamed};

// ---------------------------------------------------------------- no kind

/// A format of no kind at `/raw`.
struct Raw;

impl Format for Raw {
    type Input = ();
    type Output = ();

    fn name(&self) -> &str {
        "raw"
    }

    fn accepts(&self, head: &http::request::Parts) -> bool {
        posts_to(head, "/raw")
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
        _: &http::request::Parts,
        _: bool,
    ) -> Result<http::Request<Doc>, Fault> {
        let mut req = http::Request::new(body);
        *req.method_mut() = http::Method::POST;
        *req.uri_mut() = http::Uri::from_static("/raw");
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

/// A 200 whose body breaks before its first byte.
#[derive(Debug, Clone)]
struct BreaksAtOnce;

#[async_trait]
impl Transport for BreaksAtOnce {
    async fn send(&self, _call: http::Request<Body>) -> Result<http::Response<Body>, Fault> {
        let broken = stream::iter([Err::<Frame<Bytes>, _>(Fault::upstream("reset"))]);
        Ok(http::Response::new(StreamBody::new(broken).boxed_unsync()))
    }
}

/// A 200 of these bytes.
#[derive(Debug, Clone)]
struct Says(&'static str);

#[async_trait]
impl Transport for Says {
    async fn send(&self, _call: http::Request<Body>) -> Result<http::Response<Body>, Fault> {
        Ok(http::Response::new(Body::from(Doc::from(Bytes::from(
            self.0,
        )))))
    }
}

/// E-3 fixed: the same `Fallback` works on traffic of no kind, as `ready()`
/// counts a response's first bytes as its content.
#[tokio::test]
async fn a_no_kind_fallback_falls_back_from_a_200_that_breaks_before_its_first_byte() {
    let log = Log::default();
    let gw = Gateway::new()
        .provider("raw-a", Raw, BreaksAtOnce)
        .and_then(|g| g.provider("raw-b", Raw, Says("from b")))
        .and_then(|g| g.serve(Raw)?.plugin("log", log.clone()))
        .and_then(|g| {
            g.plugin(
                "fallback",
                Fallback::new(Backoff::once()).then("raw-b", None),
            )
        })
        .and_then(|g| g.target("raw-a"))
        .expect("gateway");

    let (status, _, body) = send(&gw, post("/raw", "x")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.expect("whole"), "from b");
    let calls: Vec<_> = log.calls().into_iter().map(|c| (c.edge, c.end)).collect();
    assert_eq!(
        calls,
        [
            ("raw-a".into(), "upstream".into()),
            ("raw-b".into(), "ok".into())
        ]
    );
}

// ------------------------------------------------------ another kind

/// An embeddings kind: a text in, for a model, a vector out.
#[derive(Debug, Clone, PartialEq)]
struct Embed {
    input: String,
    model: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
struct Vector(Vec<f64>);

struct Embeddings;

impl Format for Embeddings {
    type Input = Embed;
    type Output = Vector;

    fn name(&self) -> &str {
        "embeddings"
    }

    fn accepts(&self, head: &http::request::Parts) -> bool {
        posts_to(head, "/embeddings")
    }

    fn decode_input(&self, _: &http::request::Parts, body: &Doc) -> Result<Embed, Fault> {
        let text = body.json().and_then(|v| v["input"].as_str());
        let model = body
            .json()
            .and_then(|v| v["model"].as_str())
            .map(Into::into);
        text.map(|t| Embed {
            input: t.into(),
            model,
        })
        .ok_or_else(|| Fault::bad_request("not an embeddings request"))
    }

    fn encode_input(&self, input: &Embed, _: Option<(&Doc, &Embed)>) -> Result<Doc, Fault> {
        let mut body = json!({ "input": input.input });
        if let Some(model) = &input.model {
            body["model"] = model.as_str().into();
        }
        Ok(Doc::from(body))
    }

    fn prepare(
        &self,
        body: Doc,
        _: Option<&Embed>,
        _: &http::request::Parts,
        _: bool,
    ) -> Result<http::Request<Doc>, Fault> {
        let mut req = http::Request::new(body);
        *req.method_mut() = http::Method::POST;
        *req.uri_mut() = http::Uri::from_static("/embeddings");
        Ok(req)
    }

    fn decoder(&self, _: &http::response::Parts, _: &http::Request<Doc>) -> Box<dyn Decoder> {
        Box::new(WholeVector(BytesMut::new()))
    }

    fn encoder(
        &self,
        _: &mut http::response::Parts,
        _: &http::Request<Doc>,
        _: bool,
    ) -> Box<dyn Encoder> {
        Box::new(WriteVector)
    }
}

struct WholeVector(BytesMut);

impl Decoder for WholeVector {
    fn decode(&mut self, bytes: Bytes, _: &mut Vec<Part>) {
        self.0.extend_from_slice(&bytes);
    }

    fn end(&mut self, out: &mut Vec<Part>) {
        let raw = self.0.split().freeze();
        let v: Value = serde_json::from_slice(&raw).unwrap_or_default();
        let Some(vector) = v["embedding"].as_array() else {
            out.push(Part::End(Err(Fault::upstream("not embeddings"))));
            return;
        };
        let vector = Vector(vector.iter().filter_map(Value::as_f64).collect());
        out.push(Item::decoded([vector], raw).into());
        out.push(Part::End(Ok(())));
    }
}

struct WriteVector;

impl Encoder for WriteVector {
    fn encode(&mut self, part: &Part, out: &mut BytesMut) -> Result<(), Fault> {
        if let Part::Item(item) = part {
            for v in item.outputs::<Vector>()? {
                out.extend_from_slice(json!({ "embedding": v.0 }).to_string().as_bytes());
            }
        }
        Ok(())
    }
}

/// Routes by the embedding of the last message: close to `[1, 0]` goes to
/// `backup`.
struct EmbeddingRouter;

#[async_trait]
impl Plugin for EmbeddingRouter {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        if !cx.is_kind::<Conversation>() {
            return Ok(None);
        }
        let text = cx
            .input::<Conversation>()?
            .messages
            .last()
            .map(Message::text);
        let call = cx.call("embedder").with_input(Embed {
            input: text.unwrap_or_default(),
            model: Some("embed-x".into()),
        });
        let vectors: Vec<Vector> = call.await?.outputs().try_collect().await?;
        if vectors.first().is_some_and(|v| v.0.first() > Some(&0.5)) {
            cx.set_target("backup");
        }
        Ok(None)
    }
}

/// Records which kind each call is of, as a chat-only provider plugin
/// must check before reading the call's input.
#[derive(Default, Clone)]
struct KindsSeen(Arc<Mutex<Vec<(String, bool)>>>);

#[async_trait]
impl Plugin for KindsSeen {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        let chat = px.is_kind::<Conversation>();
        let mut seen = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        seen.push((px.provider().to_owned(), chat));
        Ok(())
    }
}

/// Calls of another kind (0006): a chat request routed by an embeddings
/// call, whose provider edge sees the embeddings kind.
#[tokio::test]
async fn a_router_embeds_the_question_with_a_call_of_another_kind() {
    let (mock, log, kinds) = (MockTransport::new(), Log::default(), KindsSeen::default());
    let gw = Gateway::new()
        .provider("openai", OpenAiChat, mock.at(OPENAI))
        .and_then(|g| g.provider("backup", OpenAiChat, mock.at(BACKUP)))
        .and_then(|g| g.provider("embedder", Embeddings, mock.at(TEXT_ONLY)))
        .and_then(|g| g.serve(OpenAiChat)?.plugin("log", log.clone()))
        .and_then(|g| {
            g.plugin("kinds", kinds.clone())?
                .plugin("router", EmbeddingRouter)
        })
        .and_then(|g| g.target("openai"))
        .expect("gateway");
    mock.reply(
        TEXT_ONLY,
        json_reply(StatusCode::OK, r#"{"embedding":[0.9,0.1]}"#),
    )
    .reply(
        BACKUP,
        json_reply(StatusCode::OK, &openai_json("from backup")),
    );

    let (status, _, body) = send(&gw, chat("gpt-x", false, "hi")).await;

    assert_eq!(status, StatusCode::OK, "{body:?}");
    assert_eq!(openai_text(&body.expect("whole")), "from backup");
    let sent = mock.sent();
    assert_eq!(
        sent_json(&sent[0]),
        json!({"input": "hi", "model": "embed-x"})
    );
    let kinds = kinds
        .0
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    assert_eq!(kinds, [("embedder".into(), false), ("backup".into(), true)]);
}

// ------------------------------------------------- calls after on_client

/// Streams the target's outputs; if the stream breaks after content, goes
/// on with `backup`, after what was said.
struct Resume;

#[async_trait]
impl Plugin for Resume {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        if !cx.is_kind::<Conversation>() {
            return Ok(None);
        }
        let first = cx.call("openai").await?.ready().await?;
        // The chat, model and all, is the backup's own input.
        let backup = cx.call("backup");
        let chat = cx.input::<Conversation>()?.clone();
        let resumed = stream::unfold(
            (
                Some(first.outputs::<Reply>()),
                Some((backup, chat, String::new())),
            ),
            |(mut outputs, mut rest)| async move {
                loop {
                    if let Some(out) = outputs.as_mut() {
                        match out.next().await {
                            Some(Ok(reply)) => {
                                if let (Reply::Text { text, .. }, Some((_, _, said))) =
                                    (&reply, rest.as_mut())
                                {
                                    said.push_str(text);
                                }
                                return Some((Ok(reply), (outputs, rest)));
                            }
                            None => return None,
                            Some(Err(f)) if rest.is_none() => return Some((Err(f), (None, None))),
                            Some(Err(_)) => {}
                        }
                    }
                    let (backup, mut chat, said) = rest.take()?;
                    chat.messages.push(Message::new(
                        respan_gateway_formats::conversation::Role::Assistant,
                        said,
                    ));
                    match backup.with_input(chat).await {
                        Ok(resp) => outputs = Some(resp.outputs::<Reply>()),
                        Err(f) => return Some((Err(f), (None, None))),
                    }
                }
            },
        );
        Ok(Some(Response::stream(resumed)))
    }
}

/// A `Call` is 'static, so a response stream can send one after
/// `on_client` returned: a stream cut mid-answer goes on at the backup. It
/// has to rebuild the chat by hand, as the kind's model is outside the
/// core; the model comes with the chat, as it is part of the input.
#[tokio::test]
async fn a_stream_cut_mid_answer_resumes_on_the_backup_from_a_call_made_early() {
    let (mock, log) = (MockTransport::new(), Log::default());
    let gw = Gateway::new()
        .provider("openai", OpenAiChat, mock.at(OPENAI))
        .and_then(|g| g.provider("backup", OpenAiChat, mock.at(BACKUP)))
        .and_then(|g| g.serve(OpenAiChat)?.plugin("log", log.clone()))
        .and_then(|g| g.plugin("resume", Resume))
        .expect("gateway");
    let mut cut = openai_stream(&["Hello", " wor"]);
    cut.truncate(3);
    mock.reply(OPENAI, sse(&cut))
        .reply(BACKUP, sse(&openai_stream(&["ld"])));

    let (status, _, body) = send(&gw, chat("gpt-x", true, "hi")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(openai_text(&body.expect("whole")), "Hello world");
    let sent = mock.sent();
    assert_eq!(sent_json(&sent[1])["messages"][1]["content"], "Hello wor");
    assert_eq!(sent_json(&sent[1])["model"], "gpt-x");
    let calls: Vec<_> = log.calls().into_iter().map(|c| (c.edge, c.end)).collect();
    assert_eq!(
        calls,
        [
            ("openai".into(), "truncated".into()),
            ("backup".into(), "ok".into())
        ]
    );
    assert_eq!(log.client().end, "ok");
}

// ----------------------------------------------------- framed requests

#[derive(Debug, Clone, PartialEq)]
struct Say(String);

#[derive(Debug, Clone, PartialEq)]
struct Heard(String);

/// JSON messages at `/talk`, one per data frame once upgraded.
struct Talk;

fn upgrades(head: &http::request::Parts) -> bool {
    head.headers.contains_key(UPGRADE)
}

impl Format for Talk {
    type Input = Say;
    type Output = Heard;

    fn name(&self) -> &str {
        "talk"
    }

    fn accepts(&self, head: &http::request::Parts) -> bool {
        head.uri.path().ends_with("/talk")
    }

    fn decode_input(&self, _: &http::request::Parts, body: &Doc) -> Result<Say, Fault> {
        let say = body.json().and_then(|v| v["say"].as_str());
        say.map(|s| Say(s.into()))
            .ok_or_else(|| Fault::bad_request("not talk"))
    }

    fn encode_input(&self, say: &Say, _: Option<(&Doc, &Say)>) -> Result<Doc, Fault> {
        Ok(Doc::from(json!({ "say": say.0 })))
    }

    fn prepare(
        &self,
        body: Doc,
        _: Option<&Say>,
        client: &http::request::Parts,
        _: bool,
    ) -> Result<http::Request<Doc>, Fault> {
        let mut call = http::Request::new(body);
        *call.uri_mut() = http::Uri::from_static("/talk");
        if upgrades(client) {
            let websocket = HeaderValue::from_static("websocket");
            call.headers_mut().insert(UPGRADE, websocket);
        }
        Ok(call)
    }

    fn decoder(&self, head: &http::response::Parts, _: &http::Request<Doc>) -> Box<dyn Decoder> {
        match head.status.is_client_error() || head.status.is_server_error() {
            true => Box::new(Failed(head.status)),
            false => Box::new(Messages),
        }
    }

    fn encoder(
        &self,
        head: &mut http::response::Parts,
        client: &http::Request<Doc>,
        _: bool,
    ) -> Box<dyn Encoder> {
        if client.headers().contains_key(UPGRADE) && head.status == StatusCode::OK {
            head.status = StatusCode::SWITCHING_PROTOCOLS;
            head.headers
                .insert(UPGRADE, HeaderValue::from_static("websocket"));
        }
        Box::new(WriteTalk)
    }
}

struct Failed(StatusCode);

impl Decoder for Failed {
    fn decode(&mut self, _: Bytes, _: &mut Vec<Part>) {}

    fn end(&mut self, out: &mut Vec<Part>) {
        let fault = Fault::new(self.0, "failed", "the provider failed");
        out.push(Part::End(Err(fault)));
    }
}

struct Messages;

impl Decoder for Messages {
    fn decode(&mut self, bytes: Bytes, out: &mut Vec<Part>) {
        let heard = serde_json::from_slice::<Value>(&bytes).ok();
        match heard.as_ref().and_then(|v| v["heard"].as_str()) {
            Some(h) => out.push(Item::decoded([Heard(h.into())], bytes).into()),
            None => out.push(Part::End(Err(Fault::upstream("not talk")))),
        }
    }

    fn end(&mut self, out: &mut Vec<Part>) {
        out.push(Part::End(Ok(())));
    }
}

struct WriteTalk;

impl Encoder for WriteTalk {
    fn encode(&mut self, part: &Part, out: &mut BytesMut) -> Result<(), Fault> {
        match part {
            Part::Item(item) => match item.unedited() {
                Some(raw) => out.extend_from_slice(raw),
                None => {
                    for heard in item.outputs::<Heard>()? {
                        out.extend_from_slice(json!({ "heard": heard.0 }).to_string().as_bytes());
                    }
                }
            },
            Part::Bytes(bytes) => out.extend_from_slice(bytes),
            Part::End(Err(f)) => {
                out.extend_from_slice(json!({ "error": f.message() }).to_string().as_bytes());
            }
            _ => {}
        }
        Ok(())
    }
}

/// Echoes each message as it arrives, noting what it got.
#[derive(Clone, Default)]
struct Echo(Arc<Mutex<Vec<String>>>);

#[async_trait]
impl Transport for Echo {
    async fn send(&self, call: http::Request<Body>) -> Result<http::Response<Body>, Fault> {
        let (head, body) = call.into_parts();
        let seen = self.0.clone();
        let echoed = body.into_data_stream().map_ok(move |frame| {
            let text = String::from_utf8_lossy(&frame).into_owned();
            seen.lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(text);
            let v: Value = serde_json::from_slice(&frame).unwrap_or_default();
            Frame::data(Bytes::from(json!({ "heard": v["say"] }).to_string()))
        });
        let mut resp = http::Response::new(StreamBody::new(echoed).boxed_unsync());
        if head.headers.contains_key(UPGRADE) {
            *resp.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
            resp.headers_mut()
                .insert(UPGRADE, HeaderValue::from_static("websocket"));
        }
        Ok(resp)
    }
}

/// A provider that cannot be reached: it never reads the call's body.
#[derive(Clone, Default)]
struct Unreachable;

#[async_trait]
impl Transport for Unreachable {
    async fn send(&self, _call: http::Request<Body>) -> Result<http::Response<Body>, Fault> {
        Err(Fault::upstream("connection refused"))
    }
}

fn talk_gateway(first: impl Transport, echo: &Echo, plugin: impl Plugin, log: &Log) -> Gateway {
    Gateway::new()
        .provider("talk-a", Talk, first)
        .and_then(|g| g.provider("talk-b", Talk, echo.clone()))
        .and_then(|g| g.serve(Talk)?.plugin("log", log.clone()))
        .and_then(|g| g.plugin("p", plugin)?.target("talk-a"))
        .expect("gateway")
}

fn upgrade() -> (
    respan_gateway_core::futures::channel::mpsc::UnboundedSender<Bytes>,
    http::Request<Body>,
) {
    let (send, mut req) = streamed("/v1/talk");
    req.headers_mut()
        .insert(UPGRADE, HeaderValue::from_static("websocket"));
    (send, req)
}

/// N7: a call whose transport never read its body gives an upgrading
/// request's inputs back, so a fallback's retry after an unreachable
/// provider gets them.
#[tokio::test]
async fn a_retry_after_an_unreachable_provider_gets_the_framed_inputs() {
    let (echo, log) = (Echo::default(), Log::default());
    let fallback = Fallback::new(Backoff::once()).then("talk-b", None);
    let gw = talk_gateway(Unreachable, &echo, fallback, &log);
    let (send, req) = upgrade();
    send.unbounded_send(Bytes::from(r#"{"say":"a"}"#)).ok();
    drop(send);

    let (_, _, body) = read_whole(gw.handle(req).await.0).await;

    assert_eq!(body.expect("whole"), r#"{"heard":"a"}"#);
}

/// Waits for the target's first content, then sends it, as the fallback
/// idiom does for a success.
struct ReadyFirst;

#[async_trait]
impl Plugin for ReadyFirst {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let target = cx.target().unwrap_or_default().to_owned();
        Ok(Some(cx.call(target).await?.ready().await?))
    }
}

/// N8: `ready()` on a relayed WebSocket's 101 returns at once, as the
/// kernel sends a 101 at once: a client-first protocol sends nothing until
/// it hears the 101.
#[tokio::test]
async fn ready_on_a_switching_protocols_response_returns_at_once() {
    let (echo, log) = (Echo::default(), Log::default());
    let gw = talk_gateway(echo.clone(), &echo, ReadyFirst, &log);
    let (_send, req) = upgrade();

    let handled = tokio::time::timeout(Duration::from_secs(2), gw.handle(req)).await;

    let (resp, _) = handled.expect("ready() deadlocked on the 101");
    assert_eq!(resp.status(), StatusCode::SWITCHING_PROTOCOLS);
}

/// The last attempt's 200-then-overload: `ready()` hands the fallback the
/// fault, which it returns as a refusal: a 502 in each client's format, the
/// Anthropic client getting Anthropic's own error body (`Fault::with_raw`),
/// and the usage of the dropped `message_start` still billed on the
/// provider edge.
#[tokio::test]
async fn the_last_overload_after_ready_is_refused_in_the_clients_format() {
    for (client, req) in [
        ("openai", chat("gpt-x", true, "hi")),
        ("anthropic", messages("claude-x", true, "hi")),
    ] {
        let (mock, log) = (MockTransport::new(), Log::default());
        let gw = Gateway::new()
            .provider(
                "anthropic",
                respan_gateway_formats::AnthropicMessages,
                mock.at(ANTHROPIC),
            )
            .and_then(|g| {
                g.serve(OpenAiChat)?
                    .serve(respan_gateway_formats::AnthropicMessages)
            })
            .and_then(|g| g.plugin("log", log.clone()))
            .and_then(|g| g.plugin("fallback", Fallback::new(Backoff::once())))
            .and_then(|g| g.target("anthropic"))
            .expect("gateway");
        mock.reply(ANTHROPIC, sse(&anthropic_overloaded_stream()));
        let (status, headers, body) = send(&gw, req).await;
        let body = body.expect("whole");
        assert_eq!(status, StatusCode::BAD_GATEWAY, "{client}");
        assert_eq!(headers["content-type"], "application/json");
        assert!(body.contains("overloaded_error"), "{client}: {body}");
        if client == "anthropic" {
            assert!(body.starts_with(r#"{"type":"error""#), "{body}");
        }
        let calls = log.calls();
        assert_eq!(
            (calls[0].end.as_str(), calls[0].output_tokens),
            ("overloaded_error", 1.0)
        );
    }
}
