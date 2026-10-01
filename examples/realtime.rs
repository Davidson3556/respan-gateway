//! Use case 7, a two-way session: OpenAI Realtime over WebSocket through the
//! gateway, with plugins over its events.
//!
//! - `Pii`: an e-mail address the user says is hidden behind a placeholder
//!   before the provider reads it (`conversation.item.create`,
//!   `session.update` and `response.create` text), and restored in every
//!   text the provider sends back, a placeholder split across deltas
//!   included (the private text support's `Placeholders`, with its `Rewrite`; its
//!   `Holdback` filters the chat kind, so a session holds its own).
//! - `MaxLength`: a session is cut at a deadline a plugin chose, with a normal
//!   close (1000, reason `max_length`); the turns metered before it stay
//!   metered. It wraps the provider's events (`ClientContext::wrap`), so it
//!   makes no call: the relay, and the provider's head with its subprotocol,
//!   stay, and it composes with every other plugin.
//! - `Reserve` (use case 9): every turn is bounded, held and settled on the
//!   client's wallet (private example bookkeeping). The session is capped before the
//!   client says anything (an input wrap opens it with a `session.update`
//!   of `max_output_tokens`, and caps the client's own), and a
//!   `response.create` is rewritten to ask for at most `TURN` output tokens.
//!   A turn holds everything the session sends or receives, with validated
//!   audio bounded by duration and text counted in bytes (a
//!   token of text is at least a byte) and `IMAGE` tokens per `input_image`
//!   (an image is billed by pixels, not bytes), plus that cap; it is
//!   refused, closing the client with 1008, when the
//!   wallet cannot cover it, and its `response.done` settles what it cost. A
//!   turn the provider starts on its own (server voice detection) is held
//!   when its `response.created` passes, and cut with 1008 if the wallet
//!   cannot cover it. A `response.create` the provider refuses with an
//!   `error` is released. So parallel sessions cannot overspend one wallet,
//!   provided the provider honours `max_output_tokens` and the gateway saw
//!   the context (what the provider adds on its own, such as default
//!   instructions, must be included in a deployment quote). The
//!   key may come in any credential form.
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
//! The integration (`respan-gateway-ws`) carries the session: `WsTransport` to the
//! provider, `serve_ws` for the client, `JsonEvents` as the format, one JSON
//! event per text frame. Everything about Realtime's events is here, none in
//! the integration.
//!
//! Covered: text events both ways; a wallet that caps, holds and settles each turn; a classifier per input; a placeholder split across deltas; usage per
//! turn; the session's end recorded as `ok`, `abandoned` (the client left),
//! `truncated` (the provider's connection broke) or `upstream`.
//!
//! Audio requires explicit `gateway_with_audio(..., true)`: valid PCM24k/G.711
//! payloads are preserved and included in reservations and per-turn usage. Capped
//! sessions reject separately billed transcription and disable automatic VAD responses:
//! commit input audio, then send response.create to reserve before generation.
//! The default text-PII policy
//! rejects opaque audio; it does not pretend a transcript filter redacts speech.
//! Gemini Live and Responses WebSocket flows have their own examples. Shared
//! shutdown signals use `serve_ws_until`; no connection is silently resumed.
//! Sources: https://developers.openai.com/api/docs/guides/voice-latency-cost
//!
//! `cargo run -p respan-gateway-examples --example realtime`

#[path = "realtime/audio.rs"]
mod audio;

#[path = "support/text.rs"]
mod text;

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use respan_gateway_http::{Credential, Status};
use respan_gateway_sdk::futures::{Stream, StreamExt, TryStreamExt, stream};
use respan_gateway_sdk::{
    ClientContext, EventStream, Fault, Filter, Gateway, Plugin, ProviderContext, Response,
    async_trait,
};
#[path = "billing/ledger.rs"]
mod ledger;
use ledger::{Amount, HoldId, Ledger, MemoryLedger};
use respan_gateway_ws::{ClientEvent, CloseCode, JsonEvents, ProviderEvent, WsTransport};
use serde_json::{Value, json};
use text::{Placeholders, Rewrite};

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
            if audio::contains_audio(&event.json) {
                return Err(Fault::new(
                    "audio_requires_redaction",
                    "text PII policy cannot inspect audio",
                )
                .with(CloseCode(1008)));
            }
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
    pub input_audio_tokens: u64,
    pub output_audio_tokens: u64,
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
    audio: AtomicU64,
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
    fn record(
        &self,
        status: &str,
        input_tokens: u64,
        output_tokens: u64,
        input_audio_tokens: u64,
        output_audio_tokens: u64,
    ) {
        lock(&self.turns).push(Turn {
            session: self.session,
            status: status.to_owned(),
            input_tokens,
            output_tokens,
            input_audio_tokens,
            output_audio_tokens,
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
            *estimate += if kind.ends_with("audio.delta") {
                audio::tokens(delta)? as u64
            } else {
                delta.len() as u64 / 4 + 1
            };
        } else if kind == "response.done" {
            self.open = None;
            self.said.asked.store(false, Ordering::SeqCst);
            let response = &event.json["response"];
            let used = |name: &str| response["usage"][name].as_u64().unwrap_or_default();
            let status = response["status"].as_str().unwrap_or_default();
            let usage = &response["usage"];
            self.record(
                status,
                used("input_tokens"),
                used("output_tokens"),
                usage["input_token_details"]["audio_tokens"]
                    .as_u64()
                    .unwrap_or_default(),
                usage["output_token_details"]["audio_tokens"]
                    .as_u64()
                    .unwrap_or_default(),
            );
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
            let audio = self.said.audio.load(Ordering::SeqCst);
            self.record(
                "cut",
                input.saturating_add(audio),
                estimate.max(HOLD),
                audio,
                0,
            );
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
            heard
                .audio
                .fetch_add(audio_in(&event.json)? as u64, Ordering::SeqCst);
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
/// where a client that left says `abandoned`; so does the call's own end
/// (a plugin's `on_provider_end`, as `Meter`'s), which the cut's fault
/// ends too.
struct MaxLength(Duration);

/// `events` until `limit` has passed, then a fault that closes normally.
fn until(
    limit: Duration,
    events: EventStream<ProviderEvent>,
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

/// The most a turn may say, in output tokens: a `response.create` that asks
/// for more, or for no limit, is rewritten to it. A plan would set it per
/// tenant.
const TURN: Amount = 1_000;

/// A session's wallet: the client's key names its account in the ledger.
struct Wallet<L> {
    ledger: L,
    key: String,
    turns: Mutex<Turns>,
}

#[derive(Default)]
struct Turns {
    /// Bound on the tokens of everything the session sent or received
    /// including audio: see `context`.
    bytes: Amount,
    /// Holds of turns not yet done, oldest first, with what each holds.
    holds: VecDeque<(HoldId, Amount)>,
    /// The holds of `response.create`s that have not begun (no
    /// `response.created` yet), oldest first, by the event's id.
    unbegun: VecDeque<(String, HoldId)>,
    /// The holds whose `response.create` has not yet left `before`: the
    /// provider cannot have read it.
    unsent: Vec<HoldId>,
    /// The holds of turns begun (a `response.created`) and not done, in the
    /// order they began: a `response.done` settles the first.
    begun: VecDeque<HoldId>,
}

impl<L: Ledger> Wallet<L> {
    /// Holds a turn's worst case: what it reads (all the session so far) and
    /// the most it may say. Refused, closing the client, if the wallet cannot
    /// cover it. The hold is recorded before the ledger is asked (a client's
    /// as unsent), so a client that leaves during the wait still has it
    /// settled (to no effect if it never landed).
    async fn hold(&self, asked: bool) -> Result<HoldId, Fault> {
        let amount = lock(&self.turns).bytes + TURN;
        let hold = HoldId::new();
        {
            let mut turns = lock(&self.turns);
            turns.holds.push_back((hold, amount));
            if asked {
                turns.unsent.push(hold);
            }
        }
        let held = self
            .ledger
            .reserve(&hold, std::slice::from_ref(&self.key), amount, HOUR)
            .await;
        held.map_err(|fault| fault.with(CloseCode(1008)))?;
        Ok(hold)
    }

    /// What the client says is counted; a `response.create` is
    /// capped at `TURN` output tokens and holds a turn first; a
    /// `session.update` is capped the same, for the turns the provider starts.
    async fn before(self: Arc<Self>, mut event: ClientEvent) -> Result<ClientEvent, Fault> {
        let said = context(&event.json)?;
        {
            let mut turns = lock(&self.turns);
            turns.bytes = turns.bytes.saturating_add(said);
            if turns.bytes > 128_000 {
                return Err(
                    Fault::new("context_limit", "session context exceeds its bound")
                        .with(CloseCode(1008)),
                );
            }
        }
        if event.json["type"] == "response.create" {
            let response = &mut event.json["response"];
            if !response.is_object() {
                *response = json!({});
            }
            let cap = &mut response["max_output_tokens"];
            if cap.as_i64().is_none_or(|n| !(1..=TURN).contains(&n)) {
                *cap = TURN.into();
            }
            // The provider's `error` names the event it refuses by this id.
            let id = match event.json["event_id"].as_str() {
                Some(id) => id.to_owned(),
                None => format!("reserve_{}", HoldId::new()),
            };
            event.json["event_id"] = id.clone().into();
            let hold = self.hold(true).await?;
            // Begun-able only once held: a turn the provider starts itself
            // meanwhile must not take this entry.
            let mut turns = lock(&self.turns);
            turns.unsent.retain(|held| *held != hold);
            turns.unbegun.push_back((id, hold));
        } else if event.json["type"] == "session.update" {
            // The cap of the turns the provider starts on its own.
            let cap = &mut event.json["session"]["max_output_tokens"];
            if cap.as_i64().is_none_or(|n| !(1..=TURN).contains(&n)) {
                *cap = TURN.into();
            }
        }
        Ok(event)
    }

    /// A turn the client did not ask for (the provider's voice detection
    /// starts them) is held when it begins, and cut if the wallet cannot
    /// cover it. What the provider says is counted. A `response.done`
    /// settles the hold of the oldest begun turn at what it used; the hold leaves the
    /// list only once settled, so a client that leaves meanwhile settles it.
    async fn after(self: Arc<Self>, event: ProviderEvent) -> Result<ProviderEvent, Fault> {
        let kind = event.json["type"].as_str().unwrap_or_default();
        if kind == "response.created" {
            let asked = lock(&self.turns).unbegun.pop_front();
            let hold = match asked {
                Some((_, hold)) => hold,
                None => self.hold(false).await?,
            };
            lock(&self.turns).begun.push_back(hold);
        } else if kind == "error" {
            // A `response.create` the provider refused never ran: its hold
            // is released, not charged at the session's end.
            let id = event.json["error"]["event_id"].as_str().unwrap_or_default();
            let refused = lock(&self.turns)
                .unbegun
                .iter()
                .find(|(asked, _)| asked == id)
                .cloned();
            if let Some((_, hold)) = refused {
                // Settled before it leaves the lists: a client that leaves
                // meanwhile still has it settled by `end`.
                self.ledger.settle(&hold, 0).await?;
                let mut turns = lock(&self.turns);
                turns.unbegun.retain(|(_, held)| *held != hold);
                turns.holds.retain(|(held, _)| *held != hold);
            }
        } else if kind == "response.done" {
            let done = {
                let turns = lock(&self.turns);
                let begun = turns.begun.front().copied();
                begun.and_then(|b| turns.holds.iter().find(|(h, _)| *h == b).copied())
            };
            if let Some((hold, amount)) = done {
                let used = event.json["response"]["usage"]["total_tokens"].as_i64();
                self.ledger.settle(&hold, used.unwrap_or(amount)).await?;
                let mut turns = lock(&self.turns);
                turns.begun.pop_front();
                turns.holds.retain(|(h, _)| *h != hold);
            }
        }
        let seen = context(&event.json)?;
        let mut turns = lock(&self.turns);
        turns.bytes = turns.bytes.saturating_add(seen);
        Ok(event)
    }

    /// A turn cut before its `response.done` may have cost all it was
    /// held for: it is charged that, never $0. One whose `response.create`
    /// never left `before` cost nothing: it is settled at 0. A hold
    /// already settled is settled again to no effect.
    async fn end(&self) {
        let (cut, unsent) = {
            let mut turns = lock(&self.turns);
            turns.unbegun.clear();
            turns.begun.clear();
            (
                turns.holds.drain(..).collect::<Vec<_>>(),
                std::mem::take(&mut turns.unsent),
            )
        };
        for (hold, amount) in cut {
            let owed = if unsent.contains(&hold) { 0 } else { amount };
            let _ = self.ledger.settle(&hold, owed).await;
        }
    }
}

/// The account a credential names: the token of a bearer, of a key in the
/// query (`api_key=sk-..`) or of a subprotocol (`openai-insecure-api-key.sk-..`);
/// a real one looks the key up.
fn account(credential: &Credential) -> Option<String> {
    let values = credential.headers().iter();
    let values = values.filter_map(|(_, value)| value.to_str().ok());
    let value = values.chain(credential.query()).next()?;
    let token = value.rsplit([' ', '.', '=']).next();
    Some(token.unwrap_or(value).to_owned())
}

/// The most tokens a provider bills for one image, whatever its bytes:
/// OpenAI's tile rule at its largest (2048x768 at high detail, 8 tiles).
/// A model on another rule (patches) can bill more, so a real deployment
/// sets this per model.
const IMAGE: Amount = 1_445;

/// What an event adds to the context, as a bound on its tokens: its bytes
/// (a token of text is at least a byte), an audio-duration bound, and
/// `IMAGE` for each image.
fn audio_in(event: &Value) -> Result<Amount, Fault> {
    match event {
        Value::Object(fields) => {
            let mut amount = 0_i64;
            for (key, value) in fields {
                if key == "audio" && value.is_string() {
                    amount =
                        amount.saturating_add(audio::tokens(value.as_str().unwrap_or_default())?);
                } else {
                    amount = amount.saturating_add(audio_in(value)?);
                }
            }
            if fields
                .get("type")
                .and_then(Value::as_str)
                .is_some_and(|kind| kind.ends_with("audio.delta"))
            {
                amount = amount
                    .saturating_add(audio::tokens(event["delta"].as_str().unwrap_or_default())?);
            }
            Ok(amount)
        }
        Value::Array(items) => items
            .iter()
            .try_fold(0_i64, |sum, item| Ok(sum.saturating_add(audio_in(item)?))),
        _ => Ok(0),
    }
}

fn context(event: &Value) -> Result<Amount, Fault> {
    fn strip(v: &mut Value, images: &mut Amount) {
        match v {
            Value::Object(fields) => {
                if fields.get("audio").is_some_and(Value::is_string) {
                    fields.remove("audio");
                }
                if fields
                    .get("type")
                    .and_then(Value::as_str)
                    .is_some_and(|kind| kind.ends_with("audio.delta"))
                {
                    fields.remove("delta");
                }
                *images += Amount::from(fields.get("type").is_some_and(|t| t == "input_image"));
                fields.values_mut().for_each(|v| strip(v, images));
            }
            Value::Array(items) => items.iter_mut().for_each(|v| strip(v, images)),
            _ => {}
        }
    }
    let audio = audio_in(event)?;
    let (mut event, mut images) = (event.clone(), 0);
    strip(&mut event, &mut images);
    let bytes = Amount::try_from(event.to_string().len()).unwrap_or(Amount::MAX);
    Ok(bytes
        .saturating_add(images.saturating_mul(IMAGE))
        .saturating_add(audio))
}

/// Holds each turn on the client's wallet before the provider sees it. The
/// hold reads the whole session, so a turn cannot cost more than its hold
/// as long as the provider keeps to the cap and the gateway saw the context
/// (a deployment must also quote context the provider adds on its own).
struct Reserve<L>(L);

#[async_trait]
impl<L: Ledger + Clone> Plugin for Reserve<L> {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let key = cx.extensions().get::<Credential>().and_then(account);
        let key = key.ok_or_else(|| Fault::new("no_key", "no key").with(Status(401)))?;
        let wallet = Arc::new(Wallet {
            ledger: self.0.clone(),
            key,
            turns: Mutex::default(),
        });
        let asks = wallet.clone();
        let cap = format!("cap_{}", HoldId::new());
        let capped = cap.clone();
        cx.wrap(move |events: EventStream<ClientEvent>| {
            // The session is capped before the client says anything, so the
            // turns the provider starts (voice detection) are bounded too.
            let open = json!({ "type": "session.update", "event_id": capped,
                "session": { "type": "realtime" } });
            let open = stream::once(async move { Ok(ClientEvent::new(open)) });
            open.chain(events)
                .and_then(move |event| asks.clone().before(event))
        })?;
        let answers = wallet.clone();
        cx.wrap(move |events: EventStream<ProviderEvent>| {
            // The provider's answer to the session's cap is not the client's:
            // its `session.updated` is the first (the client speaks after the
            // cap), and an `error` naming the cap ends a session that cannot
            // be capped, rather than let it run uncapped.
            let mut ours = true;
            let events = events.try_filter_map(move |event| {
                let kind = event.json["type"].as_str().unwrap_or_default();
                let refused = kind == "error" && event.json["error"]["event_id"] == cap;
                let mine = ours && kind == "session.updated";
                ours &= !mine;
                std::future::ready(match (refused, mine) {
                    (true, _) => Err(Fault::new("cap_refused", "the session cannot be capped")
                        .with(CloseCode(1011))),
                    (_, mine) => Ok((!mine).then_some(event)),
                })
            });
            events.and_then(move |event| answers.clone().after(event))
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
        cx.wrap(|events: EventStream<ClientEvent>| {
            events.and_then(|event| async move { classify(&event).await.map(|()| event) })
        })?;
        Ok(None)
    }
}

/// A local probe for session-end outcomes; production records these in its
/// own telemetry instead of depending on a testkit plugin.
#[derive(Clone, Default)]
struct Outcomes(Arc<Mutex<Vec<Result<(), Fault>>>>);

impl Outcomes {
    #[cfg(test)]
    fn only(&self) -> Result<(), Fault> {
        let mut all = lock(&self.0).clone();
        assert_eq!(all.len(), 1, "expected exactly one end: {all:?}");
        all.remove(0)
    }
}

#[async_trait]
impl Plugin for Outcomes {
    async fn on_client_end(&self, _cx: &ClientContext, end: &Result<(), Fault>) {
        lock(&self.0).push(end.clone());
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
    gateway_with_audio(provider, meter, ends, wallets, longest, false)
}

/// Audio permission is explicit: a text-redaction policy must never silently pass audio.
fn gateway_with_audio(
    provider: &str,
    meter: &Meter,
    ends: &Outcomes,
    wallets: &MemoryLedger,
    longest: Duration,
    allow_audio: bool,
) -> Result<Gateway, Fault> {
    let gw = Gateway::new()
        .client::<ClientEvent, _, _>(realtime())?
        .provider::<ClientEvent, _, _>("openai", realtime(), WsTransport::new(provider)?.connect())?
        .target("openai")?
        .plugin("ends", ends.clone())?
        .plugin("audio", audio::Validate(allow_audio))?;
    let gw = if allow_audio {
        gw
    } else {
        gw.plugin("pii", Pii)?
    };
    gw.plugin("meter", meter.clone())?
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
    let app = respan_gateway_axum::ws::router(gateway.clone());
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
    use respan_gateway_sdk::futures::SinkExt;
    let text = event.to_string().into();
    ws.send(tokio_tungstenite::tungstenite::Message::Text(text))
        .await
        .expect("sent");
}

/// The next event the client hears; `None` at a Close, or a break.
async fn hear(ws: &mut Client) -> Option<Value> {
    use respan_gateway_sdk::futures::StreamExt;
    use tokio_tungstenite::tungstenite::Message;
    let next = tokio::time::timeout(std::time::Duration::from_secs(5), ws.next()).await;
    match next.expect("an event in time") {
        Some(Ok(Message::Text(text))) => serde_json::from_str(&text).ok(),
        _ => None,
    }
}

/// EventStream up to and including a `response.done`.
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
    use respan_gateway_sdk::futures::SinkExt;
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
