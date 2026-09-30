//! The store: a [`Ledger`] trait, and [`MemoryLedger`] for one process.

use std::collections::HashMap;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use respan_gateway_core::{Fault, async_trait};
use respan_gateway_http::Status;
use respan_gateway_messages::Details;
use serde_json::json;
use tokio::time::Instant;

use crate::{INSUFFICIENT_FUNDS, UNKNOWN_ACCOUNT};

/// Money, or any unit you charge in, in the smallest step you count.
pub type Amount = i64;

/// A hold's id: unique across processes, so a shared store can key on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HoldId(u128);

impl HoldId {
    /// A new id.
    pub fn new() -> Self {
        static COUNT: AtomicU64 = AtomicU64::new(0);
        let random = RandomState::new().build_hasher().finish();
        let count = COUNT.fetch_add(1, Ordering::Relaxed);
        Self(u128::from(random) << 64 | u128::from(count))
    }
}

impl Default for HoldId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for HoldId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:032x}", self.0)
    }
}

/// Where holds and spending are kept. Keep both methods fast: a request's
/// `on_client_end` waits for its calls' settlements, and a call's hold is
/// held until its settlement, so a fallback's next attempt may see the
/// last one's hold still in place.
#[async_trait]
pub trait Ledger: Send + Sync + 'static {
    /// Hold `amount` on every account in `accounts`, or on none, until
    /// settled or until `ttl` passes, when the hold is released but its
    /// accounts are kept for a late settlement for one more `ttl`. A known
    /// `id`, including one already settled, is a no-op until that deadline.
    /// After it is forgotten, the same `id` can reserve again.
    async fn reserve(
        &self,
        id: &HoldId,
        accounts: &[String],
        amount: Amount,
        ttl: Duration,
    ) -> Result<(), Fault>;

    /// Release hold `id` and charge its accounts `spent`, which may exceed
    /// it, even once its `ttl` has passed. An unknown, settled or forgotten
    /// `id` is a no-op.
    async fn settle(&self, id: &HoldId, spent: Amount) -> Result<(), Fault>;
}

/// Accounts in memory, each with a limit and an optional parent that is
/// held and charged with it. Cloning shares them.
#[derive(Debug, Clone, Default)]
pub struct MemoryLedger(Arc<Mutex<Books>>);

#[derive(Debug, Default)]
struct Books {
    accounts: HashMap<String, Account>,
    holds: HashMap<HoldId, Hold>,
}

#[derive(Debug, Default)]
struct Account {
    parent: Option<String>,
    limit: Amount,
    spent: Amount,
    held: Amount,
}

#[derive(Debug)]
struct Hold {
    chain: Vec<String>,
    /// Held on each account in `chain`; 0 once expired or settled.
    amount: Amount,
    expires: Instant,
    /// When this id is forgotten, settled or not: one `ttl` after expiry.
    forget: Instant,
}

impl MemoryLedger {
    /// No accounts.
    pub fn new() -> Self {
        Self::default()
    }

    /// Open `account` under `parent` with `limit`; reopening one changes
    /// both and keeps what it spent and holds.
    pub fn open(&self, account: &str, parent: Option<&str>, limit: Amount) -> &Self {
        let mut books = self.books();
        let a = books.accounts.entry(account.to_owned()).or_default();
        (a.parent, a.limit) = (parent.map(str::to_owned), limit);
        drop(books);
        self
    }

    /// What `account` has left, holds deducted; 0 if unknown.
    pub fn available(&self, account: &str) -> Amount {
        let books = self.books();
        let a = books.accounts.get(account);
        a.map_or(0, |a| {
            a.limit.saturating_sub(a.spent).saturating_sub(a.held)
        })
    }

    /// What `account` has been charged; 0 if unknown.
    pub fn spent(&self, account: &str) -> Amount {
        self.books().accounts.get(account).map_or(0, |a| a.spent)
    }

    /// Books with expired holds released. An expired hold stays for a late
    /// settlement, as expiry releases what it held, not what the call
    /// spent, until it is forgotten.
    fn books(&self) -> MutexGuard<'_, Books> {
        let mut books = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let now = Instant::now();
        let Books { accounts, holds } = &mut *books;
        for hold in holds.values_mut().filter(|h| h.expires <= now) {
            for name in &hold.chain {
                if let Some(a) = accounts.get_mut(name) {
                    a.held = a.held.saturating_sub(hold.amount);
                }
            }
            hold.amount = 0;
        }
        holds.retain(|_, h| h.forget > now);
        books
    }
}

impl Books {
    /// Each account and its ancestors, once each.
    fn chain(&self, accounts: &[String]) -> Result<Vec<String>, Fault> {
        let mut chain: Vec<String> = Vec::new();
        for account in accounts {
            let mut at = Some(account);
            while let Some(name) = at.filter(|n| !chain.contains(n)) {
                let a = self.accounts.get(name).ok_or_else(|| {
                    let msg = format!("unknown account '{name}'");
                    Fault::new(UNKNOWN_ACCOUNT, msg).with(Status(401))
                })?;
                chain.push(name.clone());
                at = a.parent.as_ref();
            }
        }
        Ok(chain)
    }

    fn close(&mut self, id: &HoldId, spent: Amount) {
        let Some(hold) = self.holds.get_mut(id) else {
            return;
        };
        let amount = std::mem::take(&mut hold.amount);
        for name in std::mem::take(&mut hold.chain) {
            if let Some(a) = self.accounts.get_mut(&name) {
                a.held = a.held.saturating_sub(amount);
                a.spent = a.spent.saturating_add(spent);
            }
        }
    }
}

#[async_trait]
impl Ledger for MemoryLedger {
    async fn reserve(
        &self,
        id: &HoldId,
        accounts: &[String],
        amount: Amount,
        ttl: Duration,
    ) -> Result<(), Fault> {
        let mut books = self.books();
        if books.holds.contains_key(id) {
            return Ok(());
        }
        let chain = books.chain(accounts)?;
        let amount = amount.max(0);
        for name in &chain {
            let a = &books.accounts[name];
            let available = a.limit.saturating_sub(a.spent).saturating_sub(a.held);
            if available < amount {
                let msg = format!("account '{name}' cannot cover this call");
                let details = json!({ "account": name, "available": available });
                let fault = Fault::new(INSUFFICIENT_FUNDS, msg).with(Status(402));
                return Err(fault.with(Details(details)));
            }
        }
        for name in &chain {
            if let Some(a) = books.accounts.get_mut(name) {
                a.held = a.held.saturating_add(amount);
            }
        }
        // A century stands for "never": `Duration::MAX` would overflow.
        let ttl = ttl.min(Duration::from_secs(100 * 365 * 24 * 3600));
        let expires = Instant::now() + ttl;
        let hold = Hold {
            chain,
            amount,
            expires,
            forget: expires + ttl,
        };
        books.holds.insert(*id, hold);
        Ok(())
    }

    async fn settle(&self, id: &HoldId, spent: Amount) -> Result<(), Fault> {
        self.books().close(id, spent.max(0));
        Ok(())
    }
}
