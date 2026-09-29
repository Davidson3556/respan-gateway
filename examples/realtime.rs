//! Use case 7, a two-way session: OpenAI Realtime over WebSocket through the
//! gateway, with plugins over its events.
//!
//! - `Pii`: an e-mail address the user says is hidden behind a placeholder
//!   before the provider reads it (`conversation.item.create`,
//!   `session.update` and `response.create` text), and restored in every
//!   text the provider sends back, a placeholder split across deltas
//!   included (the text SDK's `Placeholders`, with its `Rewrite`; its
//!   `Holdback` filters the chat kind, so a session holds its own).
//! - `MaxLength`: a session is cut at a deadline a plugin chose, with a normal
//!   close (1000, reason `max_length`); the turns metered before it stay
//!   metered. It wraps the provider's events (`ClientContext::wrap`), so it
//!   makes no call: the relay, and the provider's head with its subprotocol,
//!   stay, and it composes with every other plugin.
//! - `Reserve` (use case 9): before each `response.create` reaches the
//!   provider, a turn's worst case is held on the client's wallet (the ledger
//!   SDK), and the turn is refused, closing the client with 1008, when the
//!   wallet cannot cover it; its `response.done` settles what it cost. Turns
//!   of parallel sessions cannot overspend one wallet.
//! - `Vet` (use case 3 on a session): a remote classifier judges each thing
//!   the client says before the provider reads it; a refusal closes the
//!   session with the fault's close code. Both are input wraps: async work
//!   on each input, which a filter cannot do.
//! - `Meter`: each turn is metered when its `response.done` passes, not when
//!   the session ends, so a session of hours is billed as it goes; a turn cut
//!   before its `response.done` (or before its first event) is metered from
//!   an estimate of the session's text, both ways, and never for less than
//!   `HOLD` output tokens (use case 9: a cut is never billed as $0); the
//!   session's end is recorded once.
//!
//! The SDK (`respan-gateway-ws`) carries the session: `WsTransport` to the
//! provider, `serve_ws` for the client, `JsonEvents` as the format, one JSON
//! event per text frame. Everything about Realtime's events is here, none in
//! the SDK.
//!
//! Covered: text events both ways; a wallet held per turn; a classifier per input; a placeholder split across deltas; usage per
//! turn; the session's end recorded as `ok`, `abandoned` (the client left),
//! `truncated` (the provider's connection broke) or `upstream`.
//!
//! Not covered: audio (`input_audio_buffer.append` events are text, and pass
//! unread, but the placeholders and the meter read text turns only); draining sessions at shutdown (the SDK's `serve_ws_until`, given a
//! future per session, or `MaxLength` with a shared signal); Gemini Live and
//! Responses over WebSocket, which only need another `JsonEvents` and plugins
//! that know their events; PII other than e-mail addresses (see `pii`).
//!
//! `cargo run -p respan-gateway-examples --example realtime`

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use respan_gateway_core::futures::{Stream, StreamExt, TryStreamExt, stream};
use respan_gateway_core::{
    ClientContext, Events, Fault, Filter, Gateway, Plugin, ProviderContext, Response, async_trait,
};
use respan_gateway_http::{Credential, Status};
use respan_gateway_ledger::{Amount, HoldId, Ledger, MemoryLedger};
use respan_gateway_testkit::Outcomes;
use respan_gateway_text::{Placeholders, Rewrite};
use respan_gateway_ws::{ClientEvent, CloseCode, JsonEvents, ProviderEvent, WsTransport};
use serde_json::{Value, json};

/// The longest a session may last, unless a plugin says otherwise.
const HOUR: Duration = Duration::from_secs(3600);

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

/// The output tokens a cut turn is charged at least: it may have been
/// thinking, and cost all it was allowed, before it said a word.
const HOLD: u64 = 256;

/// What a session's client has said, for its input estimate.
#[derive(Default)]
struct Said {
    /// Bytes of text. Every turn reads the whole context, so this only grows.
    bytes: AtomicU64,
    /// A `response.create` is waiting for its `response.done`.
    asked: AtomicBool,
}

/// Meters one session's turns: each at its `response.done`, and a turn cut
/// before it (the session ended first) from an estimate, so a cut is never
/// billed as $0.
struct TurnMeter {
    session: u64,
    count: Arc<AtomicUsize>,
    turns: Arc<Mutex<Vec<Turn>>>,
    said: Arc<Said>,
    /// The output tokens estimated so far in the open turn, if one is open.
    open: Option<u64>,
}

impl TurnMeter {
    fn record(&self, status: &str, input_tokens: u64, output_tokens: u64) {
        lock(&self.turns).push(Turn {
            session: self.session,
            status: status.to_owned(),
            input_tokens,
            output_tokens,
        });
        self.count.fetch_add(1, Ordering::SeqCst);
    }
}

impl Filter<ProviderEvent> for TurnMeter {
    fn edit(&mut self, event: ProviderEvent, out: &mut Vec<ProviderEvent>) -> Result<(), Fault> {
        let kind = event.json["type"].as_str().unwrap_or_default();
        if kind == "response.created" {
            self.open = Some(0);
        } else if kind.ends_with(".delta")
            && let Some(estimate) = &mut self.open
        {
            // About four bytes a token: an estimate, never zero for a delta.
            let delta = event.json["delta"].as_str().unwrap_or_default();
            *estimate += delta.len() as u64 / 4 + 1;
        } else if kind == "response.done" {
            self.open = None;
            self.said.asked.store(false, Ordering::SeqCst);
            let response = &event.json["response"];
            let used = |name: &str| response["usage"][name].as_u64().unwrap_or_default();
            let status = response["status"].as_str().unwrap_or_default();
            self.record(status, used("input_tokens"), used("output_tokens"));
        }
        out.push(event);
        Ok(())
    }

    fn end(
        &mut self,
        _end: &Result<(), Fault>,
        _out: &mut Vec<ProviderEvent>,
    ) -> Result<(), Fault> {
        // A turn asked for and not done is cut, even before its first event.
        let asked = self.said.asked.swap(false, Ordering::SeqCst);
        if let Some(estimate) = self.open.take().or(asked.then_some(0)) {
            let input = self.said.bytes.load(Ordering::SeqCst) / 4 + 1;
            self.record("cut", input, estimate.max(HOLD));
        }
        Ok(())
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
        let said = Arc::new(Said::default());
        let heard = said.clone();
        px.filter(move |event: &mut ClientEvent| {
            let mut bytes = 0;
            edit_texts(&mut event.json, SAID, &mut |text| bytes += text.len());
            heard.bytes.fetch_add(bytes as u64, Ordering::SeqCst);
            if event.json["type"] == "response.create" {
                heard.asked.store(true, Ordering::SeqCst);
            }
            Ok(())
        })?;
        px.filter(TurnMeter {
            session,
            count,
            turns: self.turns.clone(),
            said,
            open: None,
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

/// Caps a session's length: its events are cut at a deadline chosen in
/// `on_client`, so a plugin decides per session (a tenant's plan, a drain
/// signal: any future works). The client gets Close 1000 with the reason
/// `max_length`, and the request's end (`on_client_end`) says `max_length`,
/// where a client that left says `abandoned`. The call's own end (a
/// plugin's `on_provider_end`, as `Meter`'s) still says `abandoned`: the
/// call is dropped unread when its outputs are cut.
struct MaxLength(Duration);

/// `events` until `limit` has passed, then a fault that closes normally.
fn until(
    limit: Duration,
    events: Events<ProviderEvent>,
) -> impl Stream<Item = Result<ProviderEvent, Fault>> {
    let state = Some((events, Box::pin(tokio::time::sleep(limit))));
    stream::unfold(state, |state| async {
        let (mut events, mut timer) = state?;
        tokio::select! {
            () = &mut timer => {
                let over = Fault::new("max_length", "the session reached its maximum length");
                Some((Err(over.with(CloseCode(1000))), None))
            }
            next = events.next() => Some((next?, Some((events, timer)))),
        }
    })
}

#[async_trait]
impl Plugin for MaxLength {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let limit = self.0;
        cx.wrap(move |events| until(limit, events))?;
        Ok(None)
    }
}

/// The most a turn can cost, in tokens: held before it starts.
const TURN: Amount = 1_000;

/// A session's wallet: the client's key names its account in the ledger, and
/// the holds of turns asked for and not yet done wait in order.
struct Wallet<L> {
    ledger: L,
    key: String,
    holds: Mutex<VecDeque<HoldId>>,
}

impl<L: Ledger> Wallet<L> {
    /// A `response.create` holds a turn's worst case first, or is refused.
    async fn before(&self, event: ClientEvent) -> Result<ClientEvent, Fault> {
        if event.json["type"] == "response.create" {
            let hold = HoldId::new();
            let key = [self.key.clone()];
            let held = self.ledger.reserve(&hold, &key, TURN, HOUR).await;
            held.map_err(|fault| fault.with(CloseCode(1008)))?;
            lock(&self.holds).push_back(hold);
        }
        Ok(event)
    }

    /// A `response.done` settles the oldest hold at what the turn used.
    async fn after(&self, event: ProviderEvent) -> Result<ProviderEvent, Fault> {
        let hold = match event.json["type"] == "response.done" {
            true => lock(&self.holds).pop_front(),
            false => None,
        };
        if let Some(hold) = hold {
            let used = &event.json["response"]["usage"]["total_tokens"];
            self.ledger
                .settle(&hold, used.as_i64().unwrap_or(TURN))
                .await?;
        }
        Ok(event)
    }

    /// A turn cut before its `response.done` may have cost all it was
    /// held for: it is charged that, never $0.
    async fn end(&self) {
        let cut: Vec<HoldId> = lock(&self.holds).drain(..).collect();
        for hold in cut {
            let _ = self.ledger.settle(&hold, TURN).await;
        }
    }
}

/// Holds each turn on the client's wallet before the provider sees it.
struct Reserve<L>(L);

#[async_trait]
impl<L: Ledger + Clone> Plugin for Reserve<L> {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let key = cx
            .extensions()
            .get::<Credential>()
            .and_then(Credential::bearer);
        let key = key.ok_or_else(|| Fault::new("no_key", "no key").with(Status(401)))?;
        let wallet = Arc::new(Wallet {
            ledger: self.0.clone(),
            key: key.to_owned(),
            holds: Mutex::default(),
        });
        let asks = wallet.clone();
        cx.wrap(move |events: Events<ClientEvent>| {
            events.and_then(move |event| {
                let wallet = asks.clone();
                async move { wallet.before(event).await }
            })
        })?;
        let answers = wallet.clone();
        cx.wrap(move |events: Events<ProviderEvent>| {
            events.and_then(move |event| {
                let wallet = answers.clone();
                async move { wallet.after(event).await }
            })
        })?;
        cx.extensions_mut().insert(wallet);
        Ok(None)
    }

    async fn on_client_end(&self, cx: &ClientContext, _end: &Result<(), Fault>) {
        if let Some(wallet) = cx.extensions().get::<Arc<Wallet<L>>>() {
            wallet.end().await;
        }
    }
}

/// A remote classifier's verdict on one thing the client said. The wait
/// stands for its network call; a real one is `client.post(..).await`.
async fn classify(event: &ClientEvent) -> Result<(), Fault> {
    tokio::time::sleep(Duration::from_millis(2)).await;
    match event.json.to_string().contains("evil") {
        true => Err(Fault::new("flagged", "the classifier flagged an input").with(CloseCode(1008))),
        false => Ok(()),
    }
}

/// Every event the client says waits for the classifier, and the session
/// ends with its refusal.
struct Vet;

#[async_trait]
impl Plugin for Vet {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.wrap(|events: Events<ClientEvent>| {
            events.and_then(|event| async move { classify(&event).await.map(|()| event) })
        })?;
        Ok(None)
    }
}

/// The gateway: sessions served and relayed to the provider at `provider`
/// (`ws://host`). For real use, the transport is
/// `WsTransport::new("wss://api.openai.com")?.with_headers(key)?`, or without
/// `with_headers`, so each client's own key is relayed.
fn gateway(
    provider: &str,
    meter: &Meter,
    ends: &Outcomes,
    wallets: &MemoryLedger,
    longest: Duration,
) -> Result<Gateway, Fault> {
    Gateway::new()
        .serve(realtime())?
        .provider("openai", realtime(), WsTransport::new(provider)?)?
        .target("openai")?
        .plugin("ends", ends.clone())?
        .plugin("pii", Pii)?
        .plugin("meter", meter.clone())?
        .plugin("vet", Vet)?
        .plugin("reserve", Reserve(wallets.clone()))?
        .plugin("max", MaxLength(longest))
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
    let wallets = MemoryLedger::new();
    wallets.open("sk-demo", None, 100_000);
    let gw = gateway(
        provider.trim_end_matches("/v1"),
        &meter,
        &ends,
        &wallets,
        HOUR,
    )?;
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
    println!("wallet spent:  {}", wallets.spent("sk-demo"));
    Ok(())
}

#[cfg(test)]
#[path = "tests/realtime.rs"]
mod tests;
