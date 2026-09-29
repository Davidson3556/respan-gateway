//! The plugin: reserve before every call, settle once when it ends.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::Duration;

use respan_gateway_core::{Fault, Plugin, ProviderContext, async_trait, codes};
use respan_gateway_http::Status;

use crate::{Amount, HoldId, Ledger, NO_ACCOUNT};

type QuoteFn = dyn Fn(&mut ProviderContext, Tally) -> Result<Quote, Fault> + Send + Sync;

/// What one call may cost, and who pays: from [`Accounting::new`]'s quote.
#[derive(Debug)]
pub struct Quote {
    accounts: Vec<String>,
    reserve: Amount,
}

impl Quote {
    /// Hold `reserve`, the most the call can cost, on `accounts`.
    pub fn new(accounts: impl IntoIterator<Item = impl Into<String>>, reserve: Amount) -> Self {
        let accounts = accounts.into_iter().map(Into::into).collect();
        Self { accounts, reserve }
    }
}

/// One call's cost as your code prices it, reported while the call runs,
/// such as by a filter of the call's outputs that prices each usage
/// increment its provider reports. [`Accounting`] settles the call from it.
/// Clones share it.
#[derive(Debug, Clone, Default)]
pub struct Tally(Arc<Counted>);

#[derive(Debug, Default)]
struct Counted {
    spent: AtomicI64,
    reported: AtomicBool,
}

impl Tally {
    /// Add `amount` to what the call cost, saturating; the call's cost now
    /// counts as reported, even if `amount` is 0.
    pub fn add(&self, amount: Amount) {
        let spent = &self.0.spent;
        let add = |now: Amount| Some(now.saturating_add(amount));
        let _ = spent.fetch_update(Ordering::Relaxed, Ordering::Relaxed, add);
        self.0.reported.store(true, Ordering::Relaxed);
    }

    /// What was added so far.
    pub fn spent(&self) -> Amount {
        self.0.spent.load(Ordering::Relaxed)
    }

    /// Whether anything was added.
    pub fn is_reported(&self) -> bool {
        self.0.reported.load(Ordering::Relaxed)
    }
}

/// Holds each call's quote on a [`Ledger`] before it is sent, and settles
/// it exactly once when it ends, from its [`Tally`]. A call with no quote
/// (its `Err`) is refused before anything is sent. A call the provider may
/// have billed without its cost reported in full (a tally never added to,
/// a stream cut off, or a call sent that failed before its head) is
/// charged at least its reserve; one the provider answered with an error
/// (a refusal: [`ProviderContext::is_head_only`]) is charged nothing. It uses the provider edge alone: register
/// it last, so it prices the call as every other plugin left it, and once
/// per gateway. A filter the quote adds is then the call's last, so it
/// sees each output first, before another plugin's filter can fail on it.
pub struct Accounting<L> {
    ledger: L,
    quote: Box<QuoteFn>,
    ttl: Duration,
}

impl<L> std::fmt::Debug for Accounting<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Accounting")
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
}

impl<L: Ledger> Accounting<L> {
    /// Quote each call on `ledger`, with the call's [`Tally`] to report
    /// its cost into, such as from a filter the quote adds to the call.
    /// Holds last 15 minutes unless settled.
    pub fn new(
        ledger: L,
        quote: impl Fn(&mut ProviderContext, Tally) -> Result<Quote, Fault> + Send + Sync + 'static,
    ) -> Self {
        Self {
            ledger,
            quote: Box::new(quote),
            ttl: Duration::from_secs(15 * 60),
        }
    }

    /// Release a hold never settled after `ttl`, such as one whose
    /// settlement was lost; longer than any call lasts.
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    /// What a call cost, as far as the gateway can tell.
    fn charge(&self, hold: &Hold, px: &ProviderContext, end: &Result<(), Fault>) -> Amount {
        let spent = hold.tally.spent();
        match end {
            // It never left the gateway.
            _ if !px.is_sent() => 0,
            // The provider answered with an error before any content: its
            // refusal, not a response cut off or a client that left.
            Err(fault) if px.is_head_only() && !is_cut(fault) => 0,
            Ok(()) if hold.tally.is_reported() => spent,
            // Cut off, never reported, or sent and given up, such as timed
            // out waiting for its head: it may have reached the provider,
            // and its cost may be partial.
            _ => spent.max(hold.reserve),
        }
    }
}

/// Whether `fault` says a response was cut off or its client left, which the
/// provider may still bill, not that it refused.
fn is_cut(fault: &Fault) -> bool {
    [codes::ABANDONED, codes::TRUNCATED, codes::INTERNAL].contains(&fault.code())
}

/// One call's hold, in the call's extensions.
struct Hold {
    id: HoldId,
    reserve: Amount,
    tally: Tally,
    /// Taken by the one settlement.
    settled: AtomicBool,
}

#[async_trait]
impl<L: Ledger> Plugin for Accounting<L> {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        let tally = Tally::default();
        let Quote { accounts, reserve } = (self.quote)(px, tally.clone())?;
        if accounts.is_empty() {
            let msg = "no account to charge";
            return Err(Fault::new(NO_ACCOUNT, msg).with(Status(401)));
        }
        let hold = Arc::new(Hold {
            id: HoldId::new(),
            reserve,
            tally,
            settled: AtomicBool::new(false),
        });
        // Stored before the first await: this hook may stop at any, and
        // `on_provider_end` still runs and settles what was held.
        px.extensions_mut().insert(hold.clone());
        let ttl = self.ttl;
        self.ledger.reserve(&hold.id, &accounts, reserve, ttl).await
    }

    async fn on_provider_end(&self, px: &ProviderContext, end: &Result<(), Fault>) {
        let Some(hold) = px.extensions().get::<Arc<Hold>>().cloned() else {
            return;
        };
        if hold.settled.swap(true, Ordering::Relaxed) {
            return;
        }
        let spent = self.charge(&hold, px, end);
        // A failure is the ledger's to retry; the hold's TTL is the backstop.
        let _ = self.ledger.settle(&hold.id, spent).await;
    }
}
