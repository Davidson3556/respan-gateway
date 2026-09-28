//! The plugin: reserve before every call, settle once when it ends.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use respan_gateway_core::http::StatusCode;
use respan_gateway_core::{Fault, Outcome, Plugin, ProviderContext, Usage, async_trait};

use crate::{Amount, HoldId, Ledger, NO_ACCOUNT};

type Price = Box<dyn FnOnce(&Usage) -> Amount + Send>;
type QuoteFn = dyn Fn(&ProviderContext) -> Result<Quote, Fault> + Send + Sync;

/// What one call may cost, and who pays: from [`Accounting::new`]'s quote.
pub struct Quote {
    accounts: Vec<String>,
    reserve: Amount,
    price: Price,
}

impl Quote {
    /// Hold `reserve`, the most the call can cost, on `accounts`; at its
    /// end, charge `price` of its usage.
    pub fn new(
        accounts: impl IntoIterator<Item = impl Into<String>>,
        reserve: Amount,
        price: impl FnOnce(&Usage) -> Amount + Send + 'static,
    ) -> Self {
        let accounts = accounts.into_iter().map(Into::into).collect();
        let price = Box::new(price);
        Self {
            accounts,
            reserve,
            price,
        }
    }
}

impl std::fmt::Debug for Quote {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Quote")
            .field("accounts", &self.accounts)
            .field("reserve", &self.reserve)
            .finish_non_exhaustive()
    }
}

/// Holds each call's quote on a [`Ledger`] before it is sent, and settles
/// it exactly once when it ends, from its [`Outcome::usage`]. A call with
/// no quote (its `Err`) is refused before anything is sent. A call the
/// provider may have billed without reporting its usage in full (a stream
/// cut off, or a call sent that failed before its head) is charged at least
/// its reserve. It uses the provider edge alone: register it last, so it
/// prices the call as every other plugin left it, and once per gateway.
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
    /// Quote each call on `ledger`. Holds last 15 minutes unless settled.
    pub fn new(
        ledger: L,
        quote: impl Fn(&ProviderContext) -> Result<Quote, Fault> + Send + Sync + 'static,
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
    fn charge(&self, hold: &Hold, price: Price, outcome: &Outcome) -> Amount {
        let usage = &outcome.usage;
        let Some(head) = &outcome.head else {
            // No answer. Sent, it may have reached the provider, such as a
            // call given up or timed out waiting for its head; else it never
            // left the gateway.
            if !hold.sent.load(Ordering::Relaxed) {
                return 0;
            }
            return price(usage).max(hold.reserve);
        };
        if !head.status.is_success() {
            return 0; // the provider refused it
        }
        if outcome.end.is_ok() && !usage.is_empty() {
            return price(usage);
        }
        // Cut off: its usage may be partial.
        price(usage).max(hold.reserve)
    }
}

/// One call's hold, in the call's extensions.
struct Hold {
    id: HoldId,
    reserve: Amount,
    /// Taken by the one settlement.
    price: Mutex<Option<Price>>,
    /// Handed to the transport.
    sent: AtomicBool,
}

#[async_trait]
impl<L: Ledger> Plugin for Accounting<L> {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        let Quote {
            accounts,
            reserve,
            price,
        } = (self.quote)(px)?;
        if accounts.is_empty() {
            let msg = "no account to charge";
            return Err(Fault::new(StatusCode::UNAUTHORIZED, NO_ACCOUNT, msg));
        }
        let hold = Arc::new(Hold {
            id: HoldId::new(),
            reserve,
            price: Mutex::new(Some(price)),
            sent: AtomicBool::new(false),
        });
        // Stored before the first await: this hook may stop at any, and
        // `on_provider_end` still runs and settles what was held.
        px.extensions_mut().insert(hold.clone());
        let sent = hold.clone();
        px.edit_call(move |_| {
            sent.sent.store(true, Ordering::Relaxed);
            Ok(())
        });
        let ttl = self.ttl;
        self.ledger.reserve(&hold.id, &accounts, reserve, ttl).await
    }

    async fn on_provider_end(&self, px: &ProviderContext, outcome: &Outcome) {
        let Some(hold) = px.extensions().get::<Arc<Hold>>().cloned() else {
            return;
        };
        let price = hold
            .price
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        let Some(price) = price else { return };
        let spent = self.charge(&hold, price, outcome);
        // A failure is the ledger's to retry; the hold's TTL is the backstop.
        let _ = self.ledger.settle(&hold.id, spent).await;
    }
}
