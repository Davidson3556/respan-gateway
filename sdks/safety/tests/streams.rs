//! Safety on requests whose inputs stream after the head, as a WebSocket's
//! messages do: a conversation-kind protocol of one JSON message per data
//! frame, relayed to a provider that echoes each message as it gets it.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use common::plugin;
use respan_gateway_core::bytes::{Bytes, BytesMut};
use respan_gateway_core::futures::channel::mpsc::UnboundedSender;
use respan_gateway_core::futures::{StreamExt, TryStreamExt};
use respan_gateway_core::http::header::UPGRADE;
use respan_gateway_core::http::{self, HeaderValue, StatusCode};
use respan_gateway_core::http_body::Frame;
use respan_gateway_core::http_body_util::{BodyExt, StreamBody};
use respan_gateway_core::serde_json::{self, Value, json};
use respan_gateway_core::{
    Body, ClientContext, Decoder, Doc, Encoder, Fault, Filter, Format, Gateway, Item, Outcome,
    Part, Plugin, Response, Transport, async_trait, codes,
};
use respan_gateway_formats::conversation::{Conversation, Message, Reply, Role};
use respan_gateway_testkit::{post, streamed};
use respan_safety::guard::{BLOCKED, Guard, Policy};
use respan_safety::pii::{Pii, Redactor};

/// Conversations over `/chat-ws`: each message `{"say": text}` is one input
/// of one user message; each output `{"heard": text}`. Framed once
/// upgraded, else one message each way.
struct ChatWs;

fn upgrades(head: &http::request::Parts) -> bool {
    head.headers.contains_key(UPGRADE)
}

impl Format for ChatWs {
    type Input = Conversation;
    type Output = Reply;

    fn name(&self) -> &str {
        "chat-ws"
    }

    fn accepts(&self, head: &http::request::Parts) -> bool {
        head.uri.path().ends_with("/chat-ws")
    }

    fn decode_input(&self, _: &http::request::Parts, body: &Doc) -> Result<Conversation, Fault> {
        let say = body.json().and_then(|v| v["say"].as_str());
        let say = say.ok_or_else(|| Fault::bad_request("not chat-ws"))?;
        Ok(Conversation::new([Message::new(Role::User, say)]))
    }

    fn encode_input(
        &self,
        chat: &Conversation,
        original: Option<(&Doc, &Conversation)>,
    ) -> Result<Doc, Fault> {
        let mut v = original.and_then(|(doc, _)| doc.json().cloned());
        let v = v.get_or_insert_with(|| json!({}));
        let said: String = chat.messages.iter().map(Message::text).collect();
        v["say"] = said.into();
        Ok(Doc::from(v.take()))
    }

    fn prepare(
        &self,
        body: Doc,
        _: Option<&Conversation>,
        client: &http::request::Parts,
        relayed: bool,
    ) -> Result<http::Request<Doc>, Fault> {
        let mut call = http::Request::new(body);
        *call.uri_mut() = http::Uri::from_static("/chat-ws");
        // A call of its own input is one message each way.
        if relayed && upgrades(client) {
            let websocket = HeaderValue::from_static("websocket");
            call.headers_mut().insert(UPGRADE, websocket);
        }
        Ok(call)
    }

    fn decoder(&self, _: &http::response::Parts, _: &http::Request<Doc>) -> Box<dyn Decoder> {
        Box::new(Heard)
    }

    fn encoder(
        &self,
        head: &mut http::response::Parts,
        client: &http::Request<Doc>,
        _relayed: bool,
    ) -> Box<dyn Encoder> {
        let (client, _) = client.clone().into_parts();
        if upgrades(&client) && head.status == StatusCode::OK {
            head.status = StatusCode::SWITCHING_PROTOCOLS;
            head.headers
                .insert(UPGRADE, HeaderValue::from_static("websocket"));
        }
        Box::new(Write)
    }
}

/// One `{"heard": ...}` per data frame.
struct Heard;

impl Decoder for Heard {
    fn decode(&mut self, bytes: Bytes, out: &mut Vec<Part>) {
        let v = serde_json::from_slice::<Value>(&bytes).ok();
        match v.as_ref().and_then(|v| v["heard"].as_str()) {
            Some(h) => out.push(Item::decoded([Reply::text(0, h)], bytes).into()),
            None => out.push(Part::End(Err(Fault::upstream("not chat-ws")))),
        }
    }

    fn end(&mut self, out: &mut Vec<Part>) {
        out.push(Part::End(Ok(())));
    }
}

struct Write;

impl Encoder for Write {
    fn encode(&mut self, part: &Part, out: &mut BytesMut) -> Result<(), Fault> {
        let write = |out: &mut BytesMut, v: Value| out.extend_from_slice(v.to_string().as_bytes());
        match part {
            Part::Item(item) => match item.unedited() {
                Some(raw) => out.extend_from_slice(raw),
                None => {
                    let text = item.outputs::<Reply>()?.iter().filter_map(|r| match r {
                        Reply::Text { text, .. } => Some(text.as_str()),
                        _ => None,
                    });
                    write(out, json!({ "heard": text.collect::<String>() }));
                }
            },
            Part::Bytes(bytes) => out.extend_from_slice(bytes),
            Part::End(Err(f)) => write(out, json!({ "error": f.code() })),
            _ => {}
        }
        Ok(())
    }
}

/// A chat-ws provider echoing each message as it gets it, noting the
/// messages.
#[derive(Clone, Default)]
struct Echo(Arc<Mutex<Vec<String>>>);

impl Echo {
    fn got(&self) -> Vec<String> {
        self.0.lock().map(|g| g.clone()).unwrap_or_default()
    }
}

#[async_trait]
impl Transport for Echo {
    async fn send(&self, call: http::Request<Body>) -> Result<http::Response<Body>, Fault> {
        let (head, body) = call.into_parts();
        let got = self.0.clone();
        let echoed = body.into_data_stream().map_ok(move |frame| {
            let text = String::from_utf8_lossy(&frame).into_owned();
            got.lock().map(|mut g| g.push(text)).ok();
            let v: Value = serde_json::from_slice(&frame).unwrap_or_default();
            Frame::data(Bytes::from(json!({ "heard": v["say"] }).to_string()))
        });
        let mut resp = http::Response::new(StreamBody::new(echoed).boxed_unsync());
        if upgrades(&head) {
            *resp.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
            resp.headers_mut()
                .insert(UPGRADE, HeaderValue::from_static("websocket"));
        }
        Ok(resp)
    }
}

fn gateway(echo: &Echo, plugins: Vec<Arc<dyn Plugin>>) -> Gateway {
    let gw = Gateway::new()
        .provider("echo", ChatWs, echo.clone())
        .and_then(|g| g.serve(ChatWs));
    let mut named = plugins.into_iter().enumerate();
    let gw = gw.and_then(|g| named.try_fold(g, |g, (i, p)| g.plugin(format!("p{i}"), p)));
    gw.and_then(|g| g.target("echo"))
        .unwrap_or_else(|e| panic!("{e}"))
}

fn upgrade() -> (UnboundedSender<Bytes>, http::Request<Body>) {
    let (send, mut req) = streamed("/v1/chat-ws");
    req.headers_mut()
        .insert(UPGRADE, HeaderValue::from_static("websocket"));
    (send, req)
}

fn say(send: &UnboundedSender<Bytes>, text: &str) {
    let message = json!({ "say": text }).to_string();
    send.unbounded_send(Bytes::from(message))
        .unwrap_or_else(|e| panic!("{e}"));
}

/// The response's data frames, and the fault that broke it off, if any.
async fn frames(resp: http::Response<Body>) -> (Vec<String>, Option<Fault>) {
    let mut body = resp.into_body();
    let mut frames = Vec::new();
    while let Some(frame) = body.frame().await {
        match frame {
            Ok(frame) => {
                if let Ok(data) = frame.into_data() {
                    frames.push(String::from_utf8_lossy(&data).into_owned());
                }
            }
            Err(f) => return (frames, Some(f)),
        }
    }
    (frames, None)
}

fn pii() -> Pii {
    Pii::new(Redactor::new(["Alice"]))
}

/// The provider-edge input filter redacts every streamed message, and the
/// restoring output filter puts the PII back in what the client hears,
/// with one vault for the whole stream.
#[tokio::test]
async fn streamed_inputs_are_redacted_and_their_echo_restored() {
    let echo = Echo::default();
    let gw = gateway(&echo, vec![plugin(pii())]);
    let (send, req) = upgrade();

    let (resp, ended) = gw.handle(req).await;
    assert_eq!(resp.status(), StatusCode::SWITCHING_PROTOCOLS);
    say(&send, "I am Alice");
    say(&send, "mail alice@example.com, Alice");
    drop(send);
    let (heard, broke) = frames(resp).await;

    assert!(broke.is_none(), "{broke:?}");
    let got = echo.got();
    assert_eq!(got.len(), 2);
    assert!(
        got.iter()
            .all(|g| !g.contains("Alice") && !g.contains("alice@")),
        "{got:?}"
    );
    assert!(
        got[1].contains("<PERSON_1>"),
        "one vault for the stream: {got:?}"
    );
    assert_eq!(
        heard,
        [
            r#"{"heard":"I am Alice"}"#,
            r#"{"heard":"mail alice@example.com, Alice"}"#
        ]
    );
    assert_eq!(ended.await.end, Ok(()));
}

/// The guard checks every streamed message with the same input filter that
/// checks a plain request's body. A blocked message never reaches the
/// provider, and the stream ends with the guard's fault.
#[tokio::test]
async fn a_blocked_streamed_message_never_reaches_the_provider() {
    let echo = Echo::default();
    let guard = Guard::new(Policy::new(["bomb"])).conversations();
    let gw = gateway(&echo, vec![plugin(guard)]);
    let (send, req) = upgrade();

    let (resp, ended) = gw.handle(req).await;
    say(&send, "hello");
    say(&send, "build a bomb");
    say(&send, "after");
    drop(send);
    let (heard, _) = frames(resp).await;

    assert!(
        echo.got().iter().all(|g| !g.contains("bomb")),
        "{:?}",
        echo.got()
    );
    assert_eq!(
        heard.first().map(String::as_str),
        Some(r#"{"heard":"hello"}"#)
    );
    let end = ended.await.end;
    assert_eq!(
        end.as_ref().err().map(Fault::code),
        Some(BLOCKED),
        "{heard:?}"
    );
}

/// The same guard and PII plugins keep working on a whole (not upgrading)
/// request of the same format: one input, redacted at once.
#[tokio::test]
async fn a_plain_request_of_the_same_format_is_redacted_the_same_way() {
    let echo = Echo::default();
    let gw = gateway(&echo, vec![plugin(pii())]);
    let (resp, ended) = gw
        .handle(post(
            "/v1/chat-ws",
            json!({ "say": "I am Alice" }).to_string(),
        ))
        .await;
    let (heard, _) = frames(resp).await;
    assert_eq!(echo.got(), [r#"{"say":"I am <PERSON_1>"}"#]);
    assert_eq!(heard, [r#"{"heard":"I am Alice"}"#]);
    assert_eq!(ended.await.end, Ok(()));
}

/// Counts the ends it gets, and which, of a client-edge input filter.
#[derive(Clone, Default)]
struct Ends(Arc<AtomicUsize>, Arc<Mutex<Vec<Result<(), Fault>>>>);

impl Filter<Conversation> for Ends {
    fn edit(&mut self, chat: Conversation, out: &mut Vec<Conversation>) -> Result<(), Fault> {
        self.0.fetch_add(1, Ordering::SeqCst);
        out.push(chat);
        Ok(())
    }

    fn end(&mut self, end: &Result<(), Fault>, _: &mut Vec<Conversation>) -> Result<(), Fault> {
        self.1.lock().map(|mut e| e.push(end.clone())).ok();
        Ok(())
    }
}

#[async_trait]
impl Plugin for Ends {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.filter(self.clone())?;
        Ok(None)
    }
}

/// A client-edge input filter gets its end once when the client leaves
/// mid-stream: abandoned, as the design promises ("every filter gets its
/// end once").
#[tokio::test]
async fn an_input_filter_ends_abandoned_when_the_client_leaves() {
    let echo = Echo::default();
    let ends = Ends::default();
    let gw = gateway(&echo, vec![plugin(ends.clone())]);
    let (send, req) = upgrade();

    let (resp, ended) = gw.handle(req).await;
    say(&send, "hello");
    let mut body = resp.into_body();
    let first = body.frame().await;
    assert!(first.is_some());
    // The client leaves: its body and the response are dropped.
    drop(body);
    drop(send);
    let outcome: Outcome = ended.await;
    gw.idle().await;

    assert_eq!(
        outcome.end.as_ref().err().map(Fault::code),
        Some(codes::ABANDONED)
    );
    let got = ends.1.lock().map(|e| e.clone()).unwrap_or_default();
    assert_eq!(got.len(), 1, "{got:?}");
}

/// Drops every input: a moderation filter that removes a message rather
/// than refusing the stream.
struct DropAll;

impl Filter<Conversation> for DropAll {
    fn edit(&mut self, _: Conversation, _: &mut Vec<Conversation>) -> Result<(), Fault> {
        Ok(())
    }
}

struct Drops;

#[async_trait]
impl Plugin for Drops {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.filter(DropAll)?;
        Ok(None)
    }
}

/// On an upgrading request a filter may drop a message; on a plain request of
/// the same format the same filter is a 500 `internal` (documented: the
/// input that came with the head "must stay one input"), so a filter
/// written for streams must know which kind of request it is on.
#[tokio::test]
async fn a_dropping_input_filter_works_on_streams_and_is_a_bug_on_plain_requests() {
    let echo = Echo::default();
    let gw = gateway(&echo, vec![plugin(Drops)]);

    let (send, req) = upgrade();
    let (resp, ended) = gw.handle(req).await;
    say(&send, "dropped");
    drop(send);
    let (heard, _) = frames(resp).await;
    assert!(heard.is_empty() && echo.got().is_empty(), "{heard:?}");
    assert_eq!(ended.await.end, Ok(()));

    let (resp, ended) = gw
        .handle(post("/v1/chat-ws", json!({ "say": "x" }).to_string()))
        .await;
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    // Read, or the outcome says only `abandoned`.
    frames(resp).await;
    assert_eq!(
        ended.await.end.as_ref().err().map(Fault::code),
        Some(codes::INTERNAL)
    );
}

/// A bridge answering per message by calling the provider once per turn,
/// through `take_inputs()` and a clone of one unsent `Call` per turn.
struct Bridge;

#[async_trait]
impl Plugin for Bridge {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        if !cx.is_kind::<Conversation>() {
            return Ok(None);
        }
        let (inputs, call) = (cx.take_inputs::<Conversation>(), cx.call("plain"));
        let turns = inputs.then(move |turn| {
            let call = call.clone();
            async move {
                let resp = call.with_input(turn?).await?;
                resp.outputs::<Reply>().try_collect::<Vec<_>>().await
            }
        });
        let replies =
            turns.map_ok(|r| respan_gateway_core::futures::stream::iter(r.into_iter().map(Ok)));
        Ok(Some(Response::stream(replies.try_flatten())))
    }
}

/// PII on the provider edge covers a bridge's per-turn calls too: each
/// turn is a call with its own input, redacted by its own filter.
#[tokio::test]
async fn a_bridges_per_turn_calls_are_redacted() {
    let echo = Echo::default();
    let gw = Gateway::new()
        .provider("echo", ChatWs, echo.clone())
        .and_then(|g| g.provider("plain", ChatWs, echo.clone()))
        .and_then(|g| g.serve(ChatWs))
        .and_then(|g| g.plugin("pii", pii()))
        .and_then(|g| g.plugin("bridge", Bridge))
        .and_then(|g| g.target("echo"))
        .unwrap_or_else(|e| panic!("{e}"));
    let (send, req) = upgrade();

    let (resp, ended) = gw.handle(req).await;
    say(&send, "I am Alice");
    say(&send, "still Alice");
    drop(send);
    let (heard, broke) = frames(resp).await;

    assert!(broke.is_none(), "{broke:?}");
    assert_eq!(echo.got().len(), 2, "{:?}", echo.got());
    assert!(
        echo.got().iter().all(|g| !g.contains("Alice")),
        "{:?}",
        echo.got()
    );
    assert_eq!(
        heard,
        [r#"{"heard":"I am Alice"}"#, r#"{"heard":"still Alice"}"#]
    );
    assert_eq!(ended.await.end, Ok(()));
}

// ------------------------------------------ round 10: a late filter's end

/// Reads every streamed input in `on_client`, whatever they end with, and
/// answers nothing: the inputs have ended before later plugins run.
struct Drain(Arc<Mutex<Vec<Result<Conversation, Fault>>>>);

#[async_trait]
impl Plugin for Drain {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let all: Vec<_> = cx.take_inputs::<Conversation>().collect().await;
        self.0.lock().map(|mut g| g.extend(all)).ok();
        Ok(None)
    }
}

fn codes_of(r: &Result<(), Fault>) -> Option<&str> {
    r.as_ref().err().map(Fault::code)
}

/// What a late input filter was given, and what adding it returned.
async fn late(
    plugins: Vec<Arc<dyn Plugin>>,
    ends: &Ends,
) -> (usize, Vec<Result<(), Fault>>, Option<Result<(), Fault>>) {
    let echo = Echo::default();
    let gw = gateway(&echo, plugins);
    let (send, req) = upgrade();
    say(&send, "hello");
    say(&send, "build a bomb");
    drop(send);
    let (resp, ended) = gw.handle(req).await;
    let _ = frames(resp).await;
    let end = ended.await.end;
    gw.idle().await;
    let got = ends.1.lock().map(|e| e.clone()).unwrap_or_default();
    (ends.0.load(Ordering::SeqCst), got, Some(end))
}

/// T10-1: an input filter added once the inputs ended sees none and gets
/// the stored end at once, `Ok` here; the request goes on.
#[tokio::test]
async fn a_filter_added_after_the_inputs_ended_gets_their_end() {
    let (drained, ends) = (Arc::default(), Ends::default());
    let (edits, got, end) = late(
        vec![plugin(Drain(Arc::clone(&drained))), plugin(ends.clone())],
        &ends,
    )
    .await;
    assert_eq!(drained.lock().map(|d| d.len()).unwrap_or(0), 2);
    assert_eq!((edits, got), (0, vec![Ok(())]));
    assert_eq!(end, Some(Ok(())));
}

/// T10-1: a guard ended the inputs with its fault; the late filter gets
/// that stored end, and adding it returns it, so the request fails with
/// the guard's code rather than going on as if the inputs were fine.
#[tokio::test]
async fn a_late_filter_gets_the_stored_failure() {
    let guard = Guard::new(Policy::new(["bomb"])).conversations();
    let (drained, ends) = (Arc::default(), Ends::default());
    let (edits, got, end) = late(
        vec![
            plugin(guard),
            plugin(Drain(Arc::clone(&drained))),
            plugin(ends.clone()),
        ],
        &ends,
    )
    .await;
    assert_eq!(edits, 0);
    assert_eq!(
        got.iter().map(codes_of).collect::<Vec<_>>(),
        [Some(BLOCKED)]
    );
    assert_eq!(end.as_ref().and_then(codes_of), Some(BLOCKED));
}

/// Pinned, by design (the F2 family): a guard ordered after a plugin that
/// read the inputs is a late filter; it checks nothing and adding it
/// succeeds, so the drained "bomb" passed unchecked and the request ends
/// `Ok`. The stored end tells it the inputs ended, not that it missed
/// them.
#[tokio::test]
async fn a_guard_after_a_drain_checks_nothing_and_passes() {
    let echo = Echo::default();
    let guard = Guard::new(Policy::new(["bomb"])).conversations();
    let drained = Arc::new(Mutex::new(Vec::new()));
    let gw = gateway(
        &echo,
        vec![plugin(Drain(Arc::clone(&drained))), plugin(guard)],
    );
    let (send, req) = upgrade();
    say(&send, "build a bomb");
    drop(send);
    let (resp, ended) = gw.handle(req).await;
    let _ = frames(resp).await;
    assert_eq!(ended.await.end, Ok(()));
    let drained = drained.lock().map(|d| d.clone()).unwrap_or_default();
    assert!(matches!(&drained[..], [Ok(c)] if format!("{c:?}").contains("bomb")));
}
