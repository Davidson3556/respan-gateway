//! The budget plugin: holds on the provider edge, settled from each call's
//! own outcome.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use respan_gateway_core::http::StatusCode;
use respan_gateway_core::http::header::UPGRADE;
use respan_gateway_core::serde_json::json;
use respan_gateway_core::{
    ClientContext, Fault, Outcome, Plugin, ProviderContext, Response, async_trait, codes,
};
use respan_gateway_formats::conversation::{Conversation, units};

use crate::bank::{Bank, Basis, HoldId, Micros, Refusal};
use crate::price::{Price, Prices, Tally, estimate_input};

/// How to charge a call given up after its head arrived, with no usage
/// reported: a hedge's loser, a client that left, a panic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Abandoned {
    /// The estimated input and the output seen so far.
    Estimate,
    /// The whole hold: the provider may have generated up to the cap.
    Reserved,
}

type Who = dyn Fn(&ClientContext) -> Option<String> + Send + Sync;

struct Inner {
    /// Tells this plugin's per-call state from another instance's.
    id: u64,
    bank: Bank,
    prices: Prices,
    who: Box<Who>,
    cap: u64,
    abandoned: Abandoned,
    bill_answers: bool,
}

/// Budgets and billing. Every call a request makes, a judge's and a
/// fallback's included, holds its worst-case cost on the provider edge
/// before it is sent and settles exactly once, from its own usage, when
/// it ends. Register it first, so its client edge sees every request; or
/// [`split`](Money::split) it so its provider edge runs last.
#[derive(Clone)]
pub struct Money(Arc<Inner>);

impl std::fmt::Debug for Money {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Money")
            .field("id", &self.0.id)
            .finish_non_exhaustive()
    }
}

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// The bearer token of the client's `Authorization` header.
fn bearer(cx: &ClientContext) -> Option<String> {
    let auth = cx.head().headers.get("authorization")?.to_str().ok()?;
    auth.strip_prefix("Bearer ").map(str::to_owned)
}

impl Money {
    /// Charge `bank` at `prices`, the account being the client's bearer
    /// token. Requests with no `max_tokens` are capped at 4096 output
    /// tokens.
    pub fn new(bank: Bank, prices: Prices) -> Self {
        Self(Arc::new(Inner {
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
            bank,
            prices,
            who: Box::new(bearer),
            cap: 4096,
            abandoned: Abandoned::Estimate,
            bill_answers: false,
        }))
    }

    fn inner(&mut self) -> &mut Inner {
        Arc::get_mut(&mut self.0).expect("configured before it is shared")
    }

    /// Find the account from the request, such as from a value an auth
    /// plugin put in its extensions.
    pub fn who(
        mut self,
        who: impl Fn(&ClientContext) -> Option<String> + Send + Sync + 'static,
    ) -> Self {
        self.inner().who = Box::new(who);
        self
    }

    /// Cap every call at `tokens` of output, the cost bound of its hold.
    pub fn cap(mut self, tokens: u64) -> Self {
        self.inner().cap = tokens;
        self
    }

    /// How to charge a call given up after its head arrived.
    pub fn abandoned(mut self, how: Abandoned) -> Self {
        self.inner().abandoned = how;
        self
    }

    /// Also charge usage a plugin answered with
    /// ([`Response::with_usage`]).
    pub fn bill_answers(mut self, yes: bool) -> Self {
        self.inner().bill_answers = yes;
        self
    }

    /// Its two edges as separate plugins: register the client edge first
    /// and the provider edge last, so holds price the call as every other
    /// plugin left it (FINDINGS F4).
    pub fn split(self) -> (ClientEdge, ProviderEdge) {
        (ClientEdge(self.0.clone()), ProviderEdge(self.0))
    }
}

/// [`Money`]'s client edge, from [`Money::split`].
#[derive(Clone)]
pub struct ClientEdge(Arc<Inner>);

/// [`Money`]'s provider edge, from [`Money::split`].
#[derive(Clone)]
pub struct ProviderEdge(Arc<Inner>);

impl std::fmt::Debug for ClientEdge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientEdge").finish_non_exhaustive()
    }
}

impl std::fmt::Debug for ProviderEdge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProviderEdge").finish_non_exhaustive()
    }
}

/// One call's hold, in its extensions, by plugin instance.
#[derive(Debug, Clone, Default)]
struct Calls(HashMap<u64, Arc<CallState>>);

#[derive(Debug)]
struct CallState {
    reserved: Micros,
    /// Taken by the one settlement.
    hold: Mutex<Option<HoldId>>,
    tally: Tally,
    /// Handed to the transport: the provider may have received it.
    sent: AtomicBool,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

fn no_bound(why: &str) -> Fault {
    Fault::bad_request(format!("no cost bound: {why}")).with_source("respan-money")
}

fn refused(r: Refusal) -> Fault {
    match r {
        Refusal::Unknown(_) => {
            Fault::new(StatusCode::UNAUTHORIZED, "no_account", "unknown account")
        }
        Refusal::Short { ledger, available } => Fault::new(
            StatusCode::PAYMENT_REQUIRED,
            "insufficient_funds",
            "the budget cannot cover this call",
        )
        .with_details(json!({ "ledger": ledger, "available_micros": available })),
    }
}

impl Inner {
    fn account(&self, cx: &ClientContext) -> Result<String, Fault> {
        let account = (self.who)(cx).filter(|a| self.bank.knows(a));
        account.ok_or_else(|| refused(Refusal::Unknown(String::new())))
    }

    /// The price of the model `chat` asks for.
    fn price(&self, chat: Option<&Conversation>) -> Option<Price> {
        self.prices.get(chat?.model.as_deref()?)
    }

    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let account = self.account(cx)?;
        // Refuse an empty wallet before any plugin spends on it.
        if self.bank.available(&account) <= 0 {
            let available = self.bank.available(&account);
            return Err(refused(Refusal::Short {
                ledger: account,
                available,
            }));
        }
        Ok(None)
    }

    async fn on_client_end(&self, cx: &ClientContext, outcome: &Outcome) {
        // The client edge's usage is only what no call reported; each call's
        // own was settled on its provider edge.
        if !self.bill_answers || outcome.usage.is_empty() {
            return;
        }
        let (Ok(account), Some(price)) = (self.account(cx), self.price(cx.input().ok())) else {
            return;
        };
        self.bank
            .charge(&account, price.of(&outcome.usage), Basis::Answer);
    }

    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        let account = self.account(px.client())?;
        if !px.is_kind::<Conversation>() {
            return Err(no_bound("requests of this format cannot be priced"));
        }
        // An upgrading request has no first input to price, and reading one
        // is the plugin's bug (INTERNAL), so the budget asks first.
        if px.client().head().headers.contains_key(UPGRADE) {
            return Err(no_bound("a stream's inputs come after its head"));
        }
        // The model is in the input: a body that does not decode fails here.
        let chat = px.input::<Conversation>()?;
        let model = chat.model.as_deref().ok_or_else(|| no_bound("no model"))?;
        self.prices
            .get(model)
            .ok_or_else(|| no_bound("the model has no price"))?;
        let cap = self.cap;
        px.filter(move |chat: &mut Conversation| {
            chat.max_tokens = Some(chat.max_tokens.map_or(cap, |n| n.min(cap)));
            Ok(())
        })?;
        let chat = px.input::<Conversation>()?;
        // A later filter may have changed the model; price what goes out.
        let price = self
            .price(Some(chat))
            .ok_or_else(|| no_bound("the model has no price"))?;
        let max_output = chat.max_tokens.unwrap_or(cap);
        let reserved = price.tokens(estimate_input(chat), max_output);
        let state = Arc::new(CallState {
            reserved,
            hold: Mutex::new(None),
            tally: Tally::default(),
            sent: AtomicBool::new(false),
        });
        // Recorded before the hold, so a hook cancelled mid-way (a hedge
        // dropping the call) still reaches its end with what it did.
        let mut calls = px.extensions().get::<Calls>().cloned().unwrap_or_default();
        calls.0.insert(self.id, state.clone());
        px.extensions_mut().insert(calls);
        px.filter(state.tally.filter())?;
        let hold = self.bank.hold(&account, reserved).map_err(refused)?;
        *lock(&state.hold) = Some(hold);
        let sent = state.clone();
        px.edit_call(move |_| {
            sent.sent.store(true, Ordering::Relaxed);
            Ok(())
        });
        Ok(())
    }

    async fn on_provider_end(&self, px: &ProviderContext, outcome: &Outcome) {
        let state = px
            .extensions()
            .get::<Calls>()
            .and_then(|c| c.0.get(&self.id));
        let Some(state) = state.cloned() else { return };
        let Some(hold) = lock(&state.hold).take() else {
            return;
        };
        // Priced as the call went out: its final model and input.
        let chat = px.input::<Conversation>().ok();
        let price = self.price(chat);
        let input = chat.map(estimate_input).unwrap_or(0);
        let (charged, basis) = match price {
            Some(price) => self.charge(&state, price, input, outcome),
            None => (state.reserved, Basis::Reserved),
        };
        self.bank.settle(hold, charged, basis);
    }

    /// What a call cost: its reported usage, topped up with estimates for
    /// units it never reported; or estimated, or nothing.
    fn charge(
        &self,
        state: &CallState,
        price: Price,
        input: u64,
        outcome: &Outcome,
    ) -> (Micros, Basis) {
        let output = state.tally.tokens();
        if !outcome.usage.is_empty() {
            let mut usage = outcome.usage.clone();
            let mut basis = Basis::Reported;
            // Usage counts as decoded, so a call that did not end `Ok` may
            // have reported only part of it, such as a stream's opening
            // input and placeholder output: never charge less than seen.
            let partial = outcome.end.is_err();
            for (unit, estimate) in [(units::INPUT_TOKENS, input), (units::OUTPUT_TOKENS, output)] {
                let reported = usage.get(unit);
                let short = reported.is_none_or(|r| partial && r < estimate as f64);
                if short {
                    usage.add(unit, estimate as f64 - reported.unwrap_or(0.0));
                    basis = Basis::ToppedUp;
                }
            }
            return (price.of(&usage), basis);
        }
        let estimate = (price.tokens(input, output), Basis::Estimated);
        let abandoned = match self.abandoned {
            Abandoned::Estimate => estimate,
            Abandoned::Reserved => (state.reserved, Basis::Reserved),
        };
        let given_up = matches!(&outcome.end, Err(f) if f.code() == codes::ABANDONED);
        match outcome.head.as_ref().map(|h| h.status) {
            // Sent and given up before its head: the provider may bill it.
            None if given_up && state.sent.load(Ordering::Relaxed) => abandoned,
            // Never sent, or the transport failed.
            None => (0, Basis::Released),
            Some(s) if !s.is_success() => (0, Basis::Released),
            Some(_) if outcome.end.is_ok() => estimate,
            Some(_) => abandoned,
        }
    }
}

#[async_trait]
impl Plugin for Money {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        self.0.on_client(cx).await
    }

    async fn on_client_end(&self, cx: &ClientContext, outcome: &Outcome) {
        self.0.on_client_end(cx, outcome).await;
    }

    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        self.0.on_provider(px).await
    }

    async fn on_provider_end(&self, px: &ProviderContext, outcome: &Outcome) {
        self.0.on_provider_end(px, outcome).await;
    }
}

#[async_trait]
impl Plugin for ClientEdge {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        self.0.on_client(cx).await
    }

    async fn on_client_end(&self, cx: &ClientContext, outcome: &Outcome) {
        self.0.on_client_end(cx, outcome).await;
    }
}

#[async_trait]
impl Plugin for ProviderEdge {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        self.0.on_provider(px).await
    }

    async fn on_provider_end(&self, px: &ProviderContext, outcome: &Outcome) {
        self.0.on_provider_end(px, outcome).await;
    }
}
