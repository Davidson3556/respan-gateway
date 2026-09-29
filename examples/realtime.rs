//! Use case 7, a two-way session: OpenAI Realtime over WebSocket through the
//! gateway, with two plugins over its events.
//!
//! - `Pii`: an e-mail address the user says is hidden behind a placeholder
//!   before the provider reads it (`conversation.item.create`,
//!   `session.update` and `response.create` text), and restored in every
//!   text the provider sends back, a placeholder split across deltas
//!   included (the text SDK's `Placeholders`, with its `Rewrite`; its
//!   `Holdback` filters the chat kind, so a session holds its own).
//! - `Meter`: each turn is metered when its `response.done` passes, not when
//!   the session ends, so a session of hours is billed as it goes; the session's
//!   end is recorded once.
//!
//! The SDK (`respan-gateway-ws`) carries the session: `WsTransport` to the
//! provider, `serve_ws` for the client, `JsonEvents` as the format, one JSON
//! event per text frame. Everything about Realtime's events is here, none in
//! the SDK.
//!
//! Covered: text events both ways; a placeholder split across deltas; usage per
//! turn; the session's end recorded as `ok`, `abandoned` (the client left),
//! `truncated` (the provider's connection broke) or `upstream`.
//!
//! Not covered: audio (`input_audio_buffer.append` events are text, and pass
//! unread, but the placeholders and the meter read text turns only); a turn cut
//! before its `response.done` is not metered (a ledger would estimate it, as
//! `respan-gateway-ledger` does for chat); a limit on a session's length or a
//! drain of long sessions at shutdown; Gemini Live and Responses over
//! WebSocket, which only need another `JsonEvents` and plugins that know their
//! events; PII other than e-mail addresses (see `pii`).
//!
//! `cargo run -p respan-gateway-examples --example realtime`

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use respan_gateway_core::{
    ClientContext, Fault, Filter, Gateway, Plugin, ProviderContext, Response, async_trait,
};
use respan_gateway_testkit::Outcomes;
use respan_gateway_text::{Placeholders, Rewrite};
use respan_gateway_ws::{ClientEvent, JsonEvents, ProviderEvent, WsTransport};
use serde_json::{Value, json};

/// The Realtime format: served and sent at `/v1/realtime`.
fn realtime() -> JsonEvents {
    JsonEvents::new("openai-realtime", "/v1/realtime")
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// `text` with every e-mail address hidden in `vault`. A plain word scan; put
/// a real detector here (see `pii`).
fn hide_emails(text: &str, vault: &Placeholders) -> String {
    let words = text.split_inclusive(char::is_whitespace);
    words
        .map(|word| {
            let core = word.trim_matches(|c: char| !c.is_alphanumeric());
            match core.split_once('@') {
                Some((user, domain)) if !user.is_empty() && domain.contains('.') => {
                    word.replacen(core, &vault.hide("EMAIL", core), 1)
                }
                _ => word.to_owned(),
            }
        })
        .collect()
}

/// Every string an event says in `keys`, at any depth, edited by `edit`.
fn edit_texts(event: &mut Value, keys: &[&str], edit: &mut dyn FnMut(&mut String)) {
    match event {
        Value::Object(fields) => {
            for (key, value) in fields.iter_mut() {
                match (value, keys.contains(&key.as_str())) {
                    (Value::String(text), true) => edit(text),
                    (value, _) => edit_texts(value, keys, edit),
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(|i| edit_texts(i, keys, edit)),
        _ => {}
    }
}

/// What the user says: text of items and instructions of sessions and responses.
const SAID: &[&str] = &["text", "instructions"];
/// What the provider says whole, and what it says in tool arguments.
const WRITTEN: &[&str] = &["text", "transcript", "arguments", "instructions"];

/// Restores placeholders in the provider's events. A delta is held back where
/// a placeholder may begin, per item; a `*.done` event, or the end, releases
/// what is held first, as deltas of its own.
struct Restore {
    vault: Placeholders,
    /// Per item: text held back, and the last delta event of the item, to
    /// make a delta of it from.
    held: HashMap<String, (String, Value)>,
}

impl Restore {
    fn new(vault: Placeholders) -> Self {
        Self {
            vault,
            held: HashMap::new(),
        }
    }

    /// The events for what is still held: each item's, restored.
    fn flush(&mut self, out: &mut Vec<ProviderEvent>) {
        let mut vault = self.vault.clone();
        for (_, (text, mut template)) in self.held.drain() {
            if let Ok(text) = vault.rewrite(&text) {
                template["delta"] = text.into();
                out.push(ProviderEvent::new(template));
            }
        }
    }
}

impl Filter<ProviderEvent> for Restore {
    fn edit(
        &mut self,
        mut event: ProviderEvent,
        out: &mut Vec<ProviderEvent>,
    ) -> Result<(), Fault> {
        let kind = event.json["type"].as_str().unwrap_or_default().to_owned();
        let mut vault = self.vault.clone();
        let item = event.json["item_id"].as_str().map(str::to_owned);
        if let (true, Some(item), Some(delta)) =
            (kind.ends_with(".delta"), item, event.json["delta"].as_str())
        {
            let held = self.held.remove(&item).map(|(text, _)| text);
            let mut text = held.unwrap_or_default() + delta;
            let mut at = text.len() - vault.pending(&text);
            while !text.is_char_boundary(at) {
                at -= 1;
            }
            let kept = text.split_off(at);
            event.json["delta"] = vault.rewrite(&text)?.into();
            if !kept.is_empty() {
                self.held.insert(item, (kept, event.json.clone()));
            }
        } else {
            if kind.ends_with(".done") {
                self.flush(out);
            }
            let mut failed = None;
            edit_texts(
                &mut event.json,
                WRITTEN,
                &mut |text| match vault.rewrite(text) {
                    Ok(restored) => *text = restored,
                    Err(fault) => failed = Some(fault),
                },
            );
            if let Some(fault) = failed {
                return Err(fault);
            }
        }
        out.push(event);
        Ok(())
    }

    fn end(&mut self, _end: &Result<(), Fault>, out: &mut Vec<ProviderEvent>) -> Result<(), Fault> {
        self.flush(out);
        Ok(())
    }
}

/// One vault per session: an address said is hidden before the provider
/// reads it and restored in what the provider says. Register it before any
/// plugin that adds text to the session.
struct Pii;

#[async_trait]
impl Plugin for Pii {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let vault = Placeholders::default();
        let hide = vault.clone();
        cx.filter(move |event: &mut ClientEvent| {
            edit_texts(&mut event.json, SAID, &mut |text| {
                *text = hide_emails(text, &hide)
            });
            Ok(())
        })?;
        cx.filter(Restore::new(vault))?;
        Ok(None)
    }
}

/// What one turn used, as its `response.done` said.
#[derive(Debug, Clone, PartialEq)]
pub struct Turn {
    /// Which session, in the order they began.
    pub session: u64,
    pub status: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
}

/// How a session ended and how many turns it had metered.
#[derive(Debug, Clone, PartialEq)]
pub struct Ended {
    pub session: u64,
    pub code: String,
    pub turns: usize,
}

/// Meters each turn as its `response.done` passes, into an embedder's tally
/// (a ledger, a billing store). Clones share it.
#[derive(Debug, Clone, Default)]
struct Meter {
    began: Arc<AtomicU64>,
    turns: Arc<Mutex<Vec<Turn>>>,
    ended: Arc<Mutex<Vec<Ended>>>,
}

impl Meter {
    fn turns(&self) -> Vec<Turn> {
        lock(&self.turns).clone()
    }

    fn ended(&self) -> Vec<Ended> {
        lock(&self.ended).clone()
    }
}

/// A session's number and the turns it metered, kept with its call.
#[derive(Clone)]
struct Metered(u64, Arc<AtomicUsize>);

#[async_trait]
impl Plugin for Meter {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        let session = self.began.fetch_add(1, Ordering::SeqCst) + 1;
        let count = Arc::new(AtomicUsize::new(0));
        px.extensions_mut().insert(Metered(session, count.clone()));
        let turns = self.turns.clone();
        px.filter(move |event: &mut ProviderEvent| {
            let response = &event.json["response"];
            if event.json["type"] == "response.done" {
                let used = |name: &str| response["usage"][name].as_u64().unwrap_or_default();
                lock(&turns).push(Turn {
                    session,
                    status: response["status"].as_str().unwrap_or_default().to_owned(),
                    input_tokens: used("input_tokens"),
                    output_tokens: used("output_tokens"),
                });
                count.fetch_add(1, Ordering::SeqCst);
            }
            Ok(())
        })
    }

    async fn on_provider_end(&self, px: &ProviderContext, end: &Result<(), Fault>) {
        if let Some(Metered(session, turns)) = px.extensions().get::<Metered>() {
            let code = end.as_ref().err().map_or("ok", Fault::code).to_owned();
            let turns = turns.load(Ordering::SeqCst);
            lock(&self.ended).push(Ended {
                session: *session,
                code,
                turns,
            });
        }
    }
}

/// The gateway: sessions served and relayed to the provider at `provider`
/// (`ws://host`). For real use, the transport is
/// `WsTransport::new("wss://api.openai.com")?.with_headers(key)?`, or without
/// `with_headers`, so each client's own key is relayed.
fn gateway(provider: &str, meter: &Meter, ends: &Outcomes) -> Result<Gateway, Fault> {
    Gateway::new()
        .serve(realtime())?
        .provider("openai", realtime(), WsTransport::new(provider)?)?
        .plugin("ends", ends.clone())?
        .plugin("pii", Pii)?
        .plugin("meter", meter.clone())?
        .target("openai")
}

/// Serve `gateway` to WebSocket clients on a free port.
async fn listen(gateway: &Gateway) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a port");
    let addr = listener.local_addr().expect("an address");
    let app = respan_gateway_ws::router(gateway.clone());
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    addr
}

type Client =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// A Realtime client of the gateway at `addr`, with a key.
async fn client(addr: std::net::SocketAddr) -> Client {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let mut req = format!("ws://{addr}/v1/realtime?model=gpt-realtime")
        .into_client_request()
        .expect("a request");
    req.headers_mut()
        .insert("authorization", "Bearer sk-demo".parse().expect("a header"));
    tokio_tungstenite::connect_async(req)
        .await
        .expect("a session")
        .0
}

async fn say(ws: &mut Client, event: Value) {
    use respan_gateway_core::futures::SinkExt;
    let text = event.to_string().into();
    ws.send(tokio_tungstenite::tungstenite::Message::Text(text))
        .await
        .expect("sent");
}

/// The next event the client hears; `None` at a Close, or a break.
async fn hear(ws: &mut Client) -> Option<Value> {
    use respan_gateway_core::futures::StreamExt;
    use tokio_tungstenite::tungstenite::Message;
    let next = tokio::time::timeout(std::time::Duration::from_secs(5), ws.next()).await;
    match next.expect("an event in time") {
        Some(Ok(Message::Text(text))) => serde_json::from_str(&text).ok(),
        _ => None,
    }
}

/// Events up to and including a `response.done`.
async fn turn(ws: &mut Client) -> Vec<Value> {
    let mut events = Vec::new();
    while let Some(event) = hear(ws).await {
        let done = event["type"] == "response.done";
        events.push(event);
        if done {
            break;
        }
    }
    events
}

/// What the client heard the assistant say in `events`: its text deltas.
fn said(events: &[Value]) -> String {
    let deltas = events
        .iter()
        .filter(|e| e["type"] == "response.output_text.delta");
    deltas.filter_map(|e| e["delta"].as_str()).collect()
}

/// A provider's scripted response that says `deltas` as text, with the
/// events Realtime sends around them and `usage` in its `response.done`.
fn script(deltas: &[&str], usage: Value) -> Vec<Value> {
    let (response, item) = ("resp_1", "item_1");
    let full: String = deltas.concat();
    let mut events = vec![
        json!({ "type": "response.created", "response": { "id": response, "status": "in_progress" } }),
    ];
    for delta in deltas {
        events.push(
            json!({ "type": "response.output_text.delta", "response_id": response,
            "item_id": item, "output_index": 0, "content_index": 0, "delta": delta }),
        );
    }
    events.push(
        json!({ "type": "response.output_text.done", "response_id": response,
        "item_id": item, "output_index": 0, "content_index": 0, "text": full }),
    );
    let item_done = json!({ "id": item, "type": "message", "role": "assistant",
        "content": [{ "type": "output_text", "text": full }] });
    events.push(
        json!({ "type": "response.output_item.done", "response_id": response,
        "output_index": 0, "item": item_done }),
    );
    events.push(
        json!({ "type": "response.done", "response": { "id": response,
        "status": "completed", "output": [item_done], "usage": usage } }),
    );
    events
}

/// The user says a sentence with an address in it.
fn user_says(text: &str) -> Value {
    json!({ "type": "conversation.item.create", "item": { "type": "message", "role": "user",
        "content": [{ "type": "input_text", "text": text }] } })
}

#[tokio::main]
async fn main() -> Result<(), Fault> {
    use mockllm::{Answer, FakeOpenAi};
    let openai = FakeOpenAi::start().await;
    let usage = json!({ "input_tokens": 40, "output_tokens": 9, "total_tokens": 49 });
    let reply = script(&["Noted: <EMA", "IL_1> is on the list."], usage);
    openai.answer(Answer::events(reply));
    let provider = openai.base_url().replace("http://", "ws://");
    let (meter, ends) = (Meter::default(), Outcomes::default());
    let gw = gateway(provider.trim_end_matches("/v1"), &meter, &ends)?;
    let addr = listen(&gw).await;

    let mut ws = client(addr).await;
    hear(&mut ws).await;
    say(&mut ws, user_says("Remember bob@x.io for the invoice.")).await;
    hear(&mut ws).await;
    say(&mut ws, json!({ "type": "response.create" })).await;
    let events = turn(&mut ws).await;
    let sent: Vec<Value> = openai
        .requests()
        .iter()
        .filter(|r| r.method == "WS")
        .map(|r| r.body.clone())
        .collect();
    println!("provider read: {}", sent[0]["item"]["content"][0]["text"]);
    println!("client heard:  {}", said(&events));
    println!("metered:       {:?}", meter.turns());
    use respan_gateway_core::futures::SinkExt;
    let close = tokio_tungstenite::tungstenite::Message::Close(None);
    let _ = ws.send(close).await;
    gw.idle().await;
    println!("session ended: {:?}", meter.ended());
    Ok(())
}

#[cfg(test)]
#[path = "tests/realtime.rs"]
mod tests;
