//! Budgets on a request whose inputs stream after its head (a WebSocket's
//! messages), relayed to a provider as one call.

mod common;

use std::sync::{Arc, Mutex};

use common::{KEY, bank, money};
use respan_gateway_core::bytes::{Bytes, BytesMut};
use respan_gateway_core::futures::{StreamExt, TryStreamExt};
use respan_gateway_core::http::header::UPGRADE;
use respan_gateway_core::http::{self, HeaderValue, StatusCode};
use respan_gateway_core::http_body::Frame;
use respan_gateway_core::http_body_util::{BodyExt, StreamBody};
use respan_gateway_core::serde_json::{Value, json};
use respan_gateway_core::{
    Body, Decoder, Doc, Encoder, Fault, Format, Gateway, Item, Outcome, Part, Plugin,
    ProviderContext, Transport, Usage, async_trait,
};
use respan_gateway_formats::conversation::{Conversation, Message, Reply, Role, units};
use respan_gateway_testkit::{Calls, streamed};
use respan_money::{Bank, Basis, HoldId};

/// Chat over a socket: `{"say": ...}` in, `{"text": ..., "in": n, "out": m}`
/// out, one message per data frame; the model in the query.
struct SocketChat;

impl Format for SocketChat {
    type Input = Conversation;
    type Output = Reply;

    fn name(&self) -> &str {
        "socket-chat"
    }

    fn accepts(&self, head: &http::request::Parts) -> bool {
        head.uri.path() == "/socket"
    }

    fn decode_input(&self, head: &http::request::Parts, body: &Doc) -> Result<Conversation, Fault> {
        let say = body.json().and_then(|v| v["say"].as_str());
        let say = say.ok_or_else(|| Fault::bad_request("not socket chat"))?;
        let mut chat = Conversation::new([Message::new(Role::User, say)]);
        chat.model = model_of(head).map(str::to_owned);
        Ok(chat)
    }

    fn encode_input(
        &self,
        chat: &Conversation,
        _: Option<(&Doc, &Conversation)>,
    ) -> Result<Doc, Fault> {
        let text = chat.messages.last().map(|m| m.text()).unwrap_or_default();
        Ok(Doc::from(json!({ "say": text })))
    }

    fn prepare(
        &self,
        body: Doc,
        _: Option<&Conversation>,
        client: &http::request::Parts,
        _: bool,
    ) -> Result<http::Request<Doc>, Fault> {
        // The model is in the head, which comes before any input: a frame's
        // `Conversation::model` has nowhere to go.
        let mut call = http::Request::new(body);
        let uri = format!("/socket?model={}", model_of(client).unwrap_or_default());
        *call.uri_mut() = uri.parse().map_err(|_| Fault::bad_request("bad model"))?;
        call.headers_mut()
            .insert(UPGRADE, HeaderValue::from_static("websocket"));
        Ok(call)
    }

    fn decoder(&self, _: &http::response::Parts, _: &http::Request<Doc>) -> Box<dyn Decoder> {
        Box::new(Messages)
    }

    fn encoder(
        &self,
        head: &mut http::response::Parts,
        _: &http::Request<Doc>,
        _: bool,
    ) -> Box<dyn Encoder> {
        if head.status == StatusCode::OK {
            head.status = StatusCode::SWITCHING_PROTOCOLS;
            head.headers
                .insert(UPGRADE, HeaderValue::from_static("websocket"));
        }
        Box::new(Write)
    }
}

fn model_of(head: &http::request::Parts) -> Option<&str> {
    head.uri.query()?.strip_prefix("model=")
}

struct Messages;

impl Decoder for Messages {
    fn decode(&mut self, bytes: Bytes, out: &mut Vec<Part>) {
        let v: Value = respan_gateway_core::serde_json::from_slice(&bytes).unwrap_or_default();
        let text = v["text"].as_str().unwrap_or_default().to_owned();
        let mut usage = Usage::default();
        usage.add(units::INPUT_TOKENS, v["in"].as_f64().unwrap_or(0.0));
        usage.add(units::OUTPUT_TOKENS, v["out"].as_f64().unwrap_or(0.0));
        out.push(Item::decoded([Reply::text(0, text)], bytes).into());
        out.push(usage.into());
    }

    fn end(&mut self, out: &mut Vec<Part>) {
        out.push(Part::End(Ok(())));
    }
}

struct Write;

impl Encoder for Write {
    fn encode(&mut self, part: &Part, out: &mut BytesMut) -> Result<(), Fault> {
        match part {
            Part::Item(item) => match item.unedited() {
                Some(raw) => out.extend_from_slice(raw),
                None => {
                    for reply in item.outputs::<Reply>()? {
                        if let Reply::Text { text, .. } = reply {
                            out.extend_from_slice(json!({ "text": text }).to_string().as_bytes());
                        }
                    }
                }
            },
            Part::End(Err(f)) => {
                out.extend_from_slice(json!({ "error": f.message() }).to_string().as_bytes());
            }
            _ => {}
        }
        Ok(())
    }
}

/// Answers each message as it comes: its text back, 4 tokens in, 1 out.
struct Echo;

#[async_trait]
impl Transport for Echo {
    async fn send(&self, call: http::Request<Body>) -> Result<http::Response<Body>, Fault> {
        let answers = call.into_body().into_data_stream().map_ok(|frame| {
            let v: Value = respan_gateway_core::serde_json::from_slice(&frame).unwrap_or_default();
            let answer = json!({ "text": v["say"], "in": 4, "out": 1 });
            Frame::data(Bytes::from(answer.to_string()))
        });
        let mut resp = http::Response::new(StreamBody::new(answers).boxed_unsync());
        *resp.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
        resp.headers_mut()
            .insert(UPGRADE, HeaderValue::from_static("websocket"));
        Ok(resp)
    }
}

fn gateway(plugins: Vec<Arc<dyn Plugin>>) -> (Gateway, Calls) {
    let calls = Calls::default();
    let mut gw = Gateway::new()
        .provider("socket", SocketChat, Echo)
        .and_then(|gw| gw.serve(SocketChat))
        .and_then(|gw| gw.plugin("calls", calls.clone()))
        .unwrap_or_else(|e| panic!("{e}"));
    for (i, p) in plugins.into_iter().enumerate() {
        gw = gw
            .plugin(format!("p{i}"), p)
            .unwrap_or_else(|e| panic!("{e}"));
    }
    let gw = gw.target("socket").unwrap_or_else(|e| panic!("{e}"));
    (gw, calls)
}

/// Sends `says` as the socket's messages, then closes; returns the status
/// and the messages heard.
async fn talk(gw: &Gateway, says: &[&str]) -> (StatusCode, Vec<String>) {
    let (send, mut req) = streamed("/socket?model=gpt-x");
    let auth = HeaderValue::from_str(&format!("Bearer {KEY}")).unwrap_or_else(|e| panic!("{e}"));
    req.headers_mut().insert("authorization", auth);
    req.headers_mut()
        .insert(UPGRADE, HeaderValue::from_static("websocket"));
    let (resp, _ended) = gw.handle(req).await;
    for say in says {
        let msg = json!({ "say": say }).to_string();
        send.unbounded_send(Bytes::from(msg)).ok();
    }
    drop(send);
    let status = resp.status();
    let heard = resp.into_body().into_data_stream();
    let heard: Vec<_> = heard
        .map(|f| {
            f.map(|b| String::from_utf8_lossy(&b).into_owned())
                .unwrap_or_else(|e| format!("broken: {e}"))
        })
        .collect()
        .await;
    gw.idle().await;
    (status, heard)
}

/// The SDK's budget holds on the call's first input, and an upgrading
/// request has none with its head, so the budget refuses every socket. It
/// must ask the `Upgrade` header first: reading `input()` on a stream is
/// INTERNAL (a 500), the plugin's bug.
#[tokio::test]
async fn the_budget_refuses_a_stream_it_cannot_see_an_input_of() {
    let bank = bank(10_000_000);
    let (gw, _) = gateway(vec![Arc::new(money(&bank))]);

    let (status, heard) = talk(&gw, &["hi"]).await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{heard:?}");
    assert!(heard[0].contains("no cost bound: a stream"), "{heard:?}");
}

/// A budget that holds per input through an input filter: one code path
/// for a plain request (the filter runs on its one input at once) and a
/// stream (on each message). What it cannot do is settle per message: the
/// relay is one call, with one `on_provider_end`.
#[derive(Clone)]
struct PerInput {
    bank: Bank,
    per_input: i64,
    holds: Arc<Mutex<Vec<HoldId>>>,
}

#[async_trait]
impl Plugin for PerInput {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        if !px.is_kind::<Conversation>() {
            return Ok(());
        }
        let (bank, amount, holds) = (self.bank.clone(), self.per_input, self.holds.clone());
        px.filter(move |_: &mut Conversation| {
            let hold = bank
                .hold(KEY, amount)
                .map_err(|_| Fault::new(StatusCode::PAYMENT_REQUIRED, "funds", "budget spent"))?;
            holds.lock().map(|mut h| h.push(hold)).ok();
            Ok(())
        })
    }

    async fn on_provider_end(&self, _: &ProviderContext, outcome: &Outcome) {
        let holds = std::mem::take(&mut *self.holds.lock().unwrap_or_else(|e| e.into_inner()));
        let spent = outcome.usage.get(units::INPUT_TOKENS).unwrap_or(0.0) as i64;
        // One settlement for the whole stream: charge it all to the first
        // hold, release the rest.
        for (i, hold) in holds.into_iter().enumerate() {
            let charged = if i == 0 { spent } else { 0 };
            self.bank.settle(hold, charged, Basis::Reported);
        }
    }
}

#[tokio::test]
async fn a_budget_holds_per_streamed_input_and_refuses_the_one_it_cannot_cover() {
    let bank = bank(250);
    let plugin = PerInput {
        bank: bank.clone(),
        per_input: 100,
        holds: Arc::default(),
    };
    let (gw, calls) = gateway(vec![Arc::new(plugin)]);

    let (status, heard) = talk(&gw, &["one", "two", "three"]).await;

    assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
    // Two messages were covered and answered; the third was refused, and
    // the refusal ended the stream in its framing.
    assert_eq!(heard.len(), 3, "{heard:?}");
    assert!(heard[2].contains("budget spent"), "{heard:?}");
    // One call, settled once, with the two answered messages' usage.
    let all = calls.all();
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].usage.get(units::INPUT_TOKENS), Some(8.0));
    assert_eq!(bank.open_holds(), 0);
    assert_eq!(bank.spent(KEY), 8);
}

/// A budget that holds per input and settles per usage increment, through
/// a usage filter on the provider edge: a hold is released as soon as the
/// message it covered reports its usage, so holds do not pile up until the
/// socket closes (M-3).
#[derive(Clone)]
struct PerMessage {
    bank: Bank,
    per_input: i64,
    open: Arc<Mutex<std::collections::VecDeque<HoldId>>>,
}

#[async_trait]
impl Plugin for PerMessage {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        if !px.is_kind::<Conversation>() {
            return Ok(());
        }
        let (bank, amount, open) = (self.bank.clone(), self.per_input, self.open.clone());
        px.filter(move |_: &mut Conversation| {
            let hold = bank
                .hold(KEY, amount)
                .map_err(|_| Fault::new(StatusCode::PAYMENT_REQUIRED, "funds", "budget spent"))?;
            open.lock()
                .unwrap_or_else(|e| e.into_inner())
                .push_back(hold);
            Ok(())
        })?;
        let (bank, open) = (self.bank.clone(), self.open.clone());
        px.filter(move |usage: &mut Usage| {
            let hold = open.lock().unwrap_or_else(|e| e.into_inner()).pop_front();
            if let Some(hold) = hold {
                let spent = usage.get(units::INPUT_TOKENS).unwrap_or(0.0) as i64;
                bank.settle(hold, spent, Basis::Reported);
            }
            Ok(())
        })
    }

    async fn on_provider_end(&self, _: &ProviderContext, _: &Outcome) {
        let left = std::mem::take(&mut *self.open.lock().unwrap_or_else(|e| e.into_inner()));
        for hold in left {
            self.bank.settle(hold, 0, Basis::Released);
        }
    }
}

#[tokio::test]
async fn a_usage_filter_settles_each_streamed_message_as_its_usage_arrives() {
    let bank = bank(10_000);
    let plugin = PerMessage {
        bank: bank.clone(),
        per_input: 100,
        open: Arc::default(),
    };
    let (gw, calls) = gateway(vec![Arc::new(plugin)]);

    let (status, heard) = talk(&gw, &["one", "two", "three"]).await;

    assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
    assert_eq!(heard.len(), 3, "{heard:?}");
    assert_eq!(calls.all()[0].usage.get(units::INPUT_TOKENS), Some(12.0));
    assert_eq!(bank.open_holds(), 0);
    assert_eq!(bank.spent(KEY), 12);
}
