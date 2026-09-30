//! Plugins over a session's events: `JsonEvents` keeps unedited events
//! byte-exact, lets filters edit, drop and count them in either direction,
//! and a failing filter closes the client with its code. The events are
//! mockllm's OpenAI Realtime; the SDK knows none of their names.
#![allow(clippy::unwrap_used)]

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use mockllm::{Answer, FakeOpenAi};
use respan_gateway_core::{
    ClientContext, Fault, Filter, Gateway, Plugin, ProviderContext, Response, async_trait,
};
use respan_gateway_http::Codec;
use respan_gateway_http::bytes::{Bytes, BytesMut};
use respan_gateway_ws::{ClientEvent, CloseCode, JsonEvents, ProviderEvent, WsTransport};
use serde_json::{Value, json};
use support::*;

const PATH: &str = "/v1/realtime?model=gpt-realtime";
const KEY: &[(&str, &str)] = &[("authorization", "Bearer sk-test")];

fn events() -> JsonEvents {
    JsonEvents::new("realtime", "/v1/realtime")
}

fn kind(text: &str) -> String {
    let v: Value = serde_json::from_str(text).unwrap();
    v["type"].as_str().unwrap_or_default().to_owned()
}

fn is_done(text: &str) -> bool {
    kind(text) == "response.done"
}

/// A gateway relaying JSON events to `base`, recorders first, then `plugins`.
fn gateway_of(base: &str, plugins: Vec<(&str, Arc<dyn Plugin>)>) -> (Gateway, Outcomes, Calls) {
    let (ends, calls) = (Outcomes::default(), Calls::default());
    let mut gateway = Gateway::new()
        .client::<ClientEvent, _, _>(events())
        .unwrap()
        .provider::<ClientEvent, _, _>(
            "p",
            events(),
            respan_gateway_http::connect(WsTransport::new(base).unwrap()),
        )
        .unwrap()
        .plugin("ends", ends.clone())
        .unwrap()
        .plugin("calls", calls.clone())
        .unwrap();
    for (name, plugin) in plugins {
        gateway = gateway.plugin(name, plugin).unwrap();
    }
    (gateway.target("p").unwrap(), ends, calls)
}

fn ws_base(openai: &FakeOpenAi) -> String {
    let base = openai.base_url().replace("http://", "ws://");
    base.trim_end_matches("/v1").to_owned()
}

/// Send `turn` (a `response.create`) and hear its events.
async fn turn(ws: &mut Ws) -> Vec<String> {
    say(ws, r#"{"type":"response.create"}"#).await;
    hear_until(ws, is_done).await
}

/// A plugin that adds the filters `add` makes on the client edge.
struct OnClient<F>(F);

#[async_trait]
impl<F: Fn(&mut ClientContext) -> Result<(), Fault> + Send + Sync + 'static> Plugin
    for OnClient<F>
{
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        (self.0)(cx)?;
        Ok(None)
    }
}

// An unedited event is written as the bytes it came in; an edited one as its
// JSON, and only that one.
#[tokio::test(flavor = "multi_thread")]
async fn only_an_edited_event_is_rewritten() {
    let openai = FakeOpenAi::start().await;
    openai.answer(Answer::text("Hi there"));
    let tap = Tap::to(ws_base(&openai)).await;
    let hide = OnClient(|cx: &mut ClientContext| {
        cx.filter(|e: &mut ProviderEvent| {
            if e.json["type"]
                .as_str()
                .is_some_and(|t| t.ends_with(".delta"))
            {
                e.json["delta"] = json!("[hidden]");
            }
            Ok(())
        })
    });
    let (gateway, ends, _) = gateway_of(&tap.base(), vec![("hide", Arc::new(hide))]);
    let server = serve(gateway).await;
    let mut ws = connect(server.addr, PATH, KEY).await.unwrap();
    let mut heard = vec![heard_text(&mut ws).await];
    heard.extend(turn(&mut ws).await);

    let sent = tap.sent_by_provider();
    assert_eq!(heard.len(), sent.len());
    let mut edited = 0;
    for (got, came) in heard.iter().zip(&sent) {
        if kind(came).ends_with(".delta") {
            let mut want: Value = serde_json::from_str(came).unwrap();
            want["delta"] = json!("[hidden]");
            assert_eq!(serde_json::from_str::<Value>(got).unwrap(), want);
            edited += 1;
        } else {
            assert_eq!(got, came, "an event nobody edited is byte-exact");
        }
    }
    assert!(edited >= 1, "the session had deltas");
    close(&mut ws, 1000, "").await;
    idle(&server.gateway).await;
    assert_eq!(ends.only(), Ok(()));
}

/// A filter that drops the events its `drops` says, in either direction.
struct Drop<F>(F);

impl<T: Send + 'static, F: Fn(&T) -> bool + Send + 'static> Filter<T> for Drop<F> {
    fn edit(&mut self, value: T, out: &mut Vec<T>) -> Result<(), Fault> {
        if !(self.0)(&value) {
            out.push(value);
        }
        Ok(())
    }
}

// Events a filter drops never reach the other side; the rest are exact, and
// a dropped client event never reaches the provider.
#[tokio::test(flavor = "multi_thread")]
async fn a_filter_can_drop_events_either_way() {
    let openai = FakeOpenAi::start().await;
    openai.answer(Answer::text("Hi there"));
    let tap = Tap::to(ws_base(&openai)).await;
    let drop_items = OnClient(|cx: &mut ClientContext| {
        cx.filter(Drop(|e: &ProviderEvent| {
            e.json["type"]
                .as_str()
                .is_some_and(|t| t.starts_with("conversation.item."))
        }))?;
        cx.filter(Drop(|e: &ClientEvent| e.json["type"] == "session.update"))
    });
    let (gateway, _, _) = gateway_of(&tap.base(), vec![("drop", Arc::new(drop_items))]);
    let server = serve(gateway).await;
    let mut ws = connect(server.addr, PATH, KEY).await.unwrap();
    let created = heard_text(&mut ws).await;
    assert_eq!(kind(&created), "session.created");
    // Dropped: the provider never hears it, so never answers `session.updated`.
    say(
        &mut ws,
        r#"{"type":"session.update","session":{"type":"realtime"}}"#,
    )
    .await;
    let heard = turn(&mut ws).await;

    assert_eq!(tap.sent_by_gateway(), [r#"{"type":"response.create"}"#]);
    let kept: Vec<String> = tap
        .sent_by_provider()
        .into_iter()
        .filter(|t| !kind(t).starts_with("conversation.item."))
        .collect();
    let mut all = vec![created];
    all.extend(heard);
    assert_eq!(all, kept);
    assert!(kept.iter().any(|t| kind(t) == "response.output_item.added"));
}

// A client event a filter edits is what the provider reads: the edit is
// written into the event, not appended.
#[tokio::test(flavor = "multi_thread")]
async fn a_filter_can_edit_what_the_provider_reads() {
    let openai = FakeOpenAi::start().await;
    let force_text = OnClient(|cx: &mut ClientContext| {
        cx.filter(|e: &mut ClientEvent| {
            if e.json["type"] == "session.update" {
                e.json["session"]["output_modalities"] = json!(["text"]);
            }
            Ok(())
        })
    });
    let (gateway, ..) = gateway_of(&ws_base(&openai), vec![("force", Arc::new(force_text))]);
    let server = serve(gateway).await;
    let mut ws = connect(server.addr, PATH, KEY).await.unwrap();
    heard_text(&mut ws).await;
    let wants_audio = json!({"type":"session.update","session":{"type":"realtime","output_modalities":["audio"]}});
    say(&mut ws, &wants_audio.to_string()).await;
    let updated: Value = serde_json::from_str(&heard_text(&mut ws).await).unwrap();
    assert_eq!(updated["type"], "session.updated");
    assert_eq!(updated["session"]["output_modalities"], json!(["text"]));
}

/// Counts the turns of each session, at the provider edge: a call's filter
/// counts what its provider says, and its end hook keeps the total.
#[derive(Clone, Default)]
struct Turns {
    seen: Arc<AtomicUsize>,
    settled: Arc<std::sync::Mutex<Vec<usize>>>,
}

#[async_trait]
impl Plugin for Turns {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        let seen = self.seen.clone();
        px.filter(move |e: &mut ProviderEvent| {
            if e.json["type"] == "response.done" {
                seen.fetch_add(1, Ordering::SeqCst);
            }
            Ok(())
        })
    }

    async fn on_provider_end(&self, _px: &ProviderContext, _end: &Result<(), Fault>) {
        let mut settled = self
            .settled
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        settled.push(self.seen.load(Ordering::SeqCst));
    }
}

// A provider-edge plugin sees every event of the session and settles once,
// with the turns it counted, when the session ends.
#[tokio::test(flavor = "multi_thread")]
async fn a_provider_edge_plugin_counts_turns() {
    let openai = FakeOpenAi::start().await;
    let turns = Turns::default();
    let (gateway, ends, calls) =
        gateway_of(&ws_base(&openai), vec![("turns", Arc::new(turns.clone()))]);
    let server = serve(gateway).await;
    let mut ws = connect(server.addr, PATH, KEY).await.unwrap();
    heard_text(&mut ws).await;
    for _ in 0..3 {
        turn(&mut ws).await;
    }
    assert_eq!(turns.seen.load(Ordering::SeqCst), 3);
    // Not settled while the session lasts.
    assert!(turns.settled.lock().unwrap().is_empty());
    close(&mut ws, 1000, "").await;
    assert!(matches!(
        hear(&mut ws).await,
        Heard::Closed(_) | Heard::Broken
    ));
    idle(&server.gateway).await;
    assert_eq!(*turns.settled.lock().unwrap(), [3]);
    assert_eq!(ends.only(), Ok(()));
    assert_eq!(calls.all(), [Ok(())]);
}

// A filter that fails ends the session: the client is closed with the
// fault's `CloseCode` and its code as the reason, never its message.
#[tokio::test(flavor = "multi_thread")]
async fn a_failing_filter_closes_the_client_with_its_code_and_no_message() {
    let openai = FakeOpenAi::start().await;
    let policy = OnClient(|cx: &mut ClientContext| {
        cx.filter(
            |e: &mut ProviderEvent| match e.json["type"] == "response.created" {
                true => {
                    Err(Fault::new("policy", "the word swordfish was said").with(CloseCode(1008)))
                }
                false => Ok(()),
            },
        )
    });
    let (gateway, ends, calls) = gateway_of(&ws_base(&openai), vec![("policy", Arc::new(policy))]);
    let server = serve(gateway).await;
    let mut ws = connect(server.addr, PATH, KEY).await.unwrap();
    heard_text(&mut ws).await;
    say(&mut ws, r#"{"type":"response.create"}"#).await;
    assert_eq!(
        hear(&mut ws).await,
        Heard::Closed(Some((1008, "policy".into())))
    );
    idle(&server.gateway).await;
    assert_eq!(code(&ends.only()), "policy");
    assert_eq!(calls.all().len(), 1);
}

// A frame that is not a JSON event fails the session it is in: the
// provider's as `upstream`, the client's as a client that left.
#[tokio::test(flavor = "multi_thread")]
async fn a_frame_that_is_not_json_fails_the_session() {
    let probe = Probe::start().await;
    let (gateway, ends, _) = gateway_of(&probe.base(), vec![]);
    let server = serve(gateway).await;
    let mut ws = connect(server.addr, PATH, &[]).await.unwrap();
    probe.connected(0).await;
    probe.tell(0, Cmd::Send("not json".into()));
    assert_eq!(
        hear(&mut ws).await,
        Heard::Closed(Some((1011, "upstream".into())))
    );
    idle(&server.gateway).await;
    assert_eq!(code(&ends.only()), "upstream");

    let mut ws = connect(server.addr, PATH, &[]).await.unwrap();
    say(&mut ws, "not json either").await;
    assert_eq!(
        hear(&mut ws).await,
        Heard::Closed(Some((1011, "bad_request".into())))
    );
    idle(&server.gateway).await;
    // Nothing that is not JSON reached the provider.
    assert!(probe.texts().is_empty());
}

// The format accepts a WebSocket upgrade at its path and nothing else.
#[tokio::test(flavor = "multi_thread")]
async fn it_serves_only_its_path() {
    let probe = Probe::start().await;
    let (gateway, ..) = gateway_of(&probe.base(), vec![]);
    let server = serve(gateway).await;
    let elsewhere = connect(server.addr, "/v1/other", &[]).await;
    let Err(tokio_tungstenite::tungstenite::Error::Http(resp)) = elsewhere else {
        panic!("expected an HTTP answer")
    };
    assert_eq!(resp.status(), 404);
    assert_eq!(probe.total.load(Ordering::SeqCst), 0);
}

// A call the format writes itself is an upgrade request to its path; one
// that carries no session cannot be written.
#[test]
fn a_call_of_its_own_is_an_upgrade_to_its_path_and_needs_a_session() {
    let format = events();
    let mut body = Bytes::new();
    let mut encoder = format.request_encoder(None, &mut body, true);
    let head = encoder.head(None);
    assert_eq!(head.method, "GET");
    assert_eq!(head.uri.path(), "/v1/realtime");
    assert_eq!(head.headers["upgrade"], "websocket");
    let mut out = BytesMut::new();
    assert_eq!(encoder.open(&mut out), Ok(false));
    encoder
        .encode(&ClientEvent::new(json!({ "type": "x", "n": 1 })), &mut out)
        .unwrap();
    assert_eq!(&out[..], br#"{"type":"x","n":1}"#);

    let mut encoder = format.request_encoder(None, &mut body, false);
    let refused = encoder.open(&mut BytesMut::new()).unwrap_err();
    assert_eq!(refused.code(), "config");
}

// A plugin refuses a session before it is upgraded: the client gets an
// ordinary HTTP answer under the fault's status, and the provider is never
// reached.
#[tokio::test(flavor = "multi_thread")]
async fn a_plugin_can_refuse_a_session_before_the_upgrade() {
    use respan_gateway_http::Status;
    let probe = Probe::start().await;
    let refuse = OnClient(|_: &mut ClientContext| {
        Err(Fault::new("over_limit", "too many sessions").with(Status(429)))
    });
    let (gateway, ends, calls) = gateway_of(&probe.base(), vec![("refuse", Arc::new(refuse))]);
    let server = serve(gateway).await;
    let refused = connect(server.addr, PATH, &[]).await;
    let Err(tokio_tungstenite::tungstenite::Error::Http(resp)) = refused else {
        panic!("expected an HTTP answer")
    };
    assert_eq!(resp.status(), 429);
    assert_eq!(resp.body().clone().unwrap(), b"too many sessions");
    idle(&server.gateway).await;
    assert_eq!(code(&ends.only()), "over_limit");
    assert!(calls.all().is_empty());
    assert_eq!(probe.total.load(Ordering::SeqCst), 0);
}

// A filter that refuses a client's input ends the session as it ends any
// other: the client is closed with the fault's `CloseCode` and its code, and
// the hooks are told that code, not that the client left.
#[tokio::test(flavor = "multi_thread")]
async fn a_filter_refusing_an_input_closes_the_client_with_its_code() {
    let openai = FakeOpenAi::start().await;
    let policy = OnClient(|cx: &mut ClientContext| {
        cx.filter(|e: &mut ClientEvent| match e.json["type"] == "forbidden" {
            true => Err(Fault::new("policy", "the input is refused").with(CloseCode(1008))),
            false => Ok(()),
        })
    });
    let (gateway, ends, calls) = gateway_of(&ws_base(&openai), vec![("policy", Arc::new(policy))]);
    let server = serve(gateway).await;
    let mut ws = connect(server.addr, PATH, KEY).await.unwrap();
    heard_text(&mut ws).await;
    say(&mut ws, r#"{"type":"forbidden"}"#).await;
    assert_eq!(
        hear(&mut ws).await,
        Heard::Closed(Some((1008, "policy".into())))
    );
    idle(&server.gateway).await;
    assert_eq!(code(&ends.only()), "policy");
    assert_eq!(calls.all().iter().map(code).collect::<Vec<_>>(), ["policy"]);
}
