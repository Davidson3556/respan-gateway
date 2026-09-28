//! Nested ledgers with atomic holds, settled idempotently.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

/// Money in millionths of a dollar. Never `f64`.
pub type Micros = i64;

/// A hold's id, unique within its [`Bank`].
pub type HoldId = u64;

/// How a settlement's amount was found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Basis {
    /// From the usage the provider reported.
    Reported,
    /// From reported usage, with the units it never reported estimated.
    ToppedUp,
    /// Estimated: the provider reported nothing.
    Estimated,
    /// The whole hold, for a call given up with nothing to estimate from.
    Reserved,
    /// Nothing: the call cost nothing, as far as the gateway can tell.
    Released,
    /// Usage a plugin answered with, charged on the client edge.
    Answer,
}

/// One line of the journal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// The hold settled, or `None` for a charge with no hold.
    pub hold: Option<HoldId>,
    /// The account charged (its ancestors are charged the same).
    pub account: String,
    /// What was held.
    pub held: Micros,
    /// What was charged.
    pub charged: Micros,
    /// How the charge was found.
    pub basis: Basis,
}

/// Why a hold was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// No such account.
    Unknown(String),
    /// `ledger` (the account or one of its ancestors) has too little.
    Short {
        /// The ledger that is short.
        ledger: String,
        /// What it has left, holds deducted.
        available: Micros,
    },
}

#[derive(Debug)]
struct Ledger {
    parent: Option<String>,
    funds: Micros,
    spent: Micros,
    held: Micros,
}

impl Ledger {
    fn available(&self) -> Micros {
        self.funds - self.spent - self.held
    }
}

#[derive(Debug)]
struct Hold {
    account: String,
    amount: Micros,
}

#[derive(Debug, Default)]
struct Books {
    ledgers: HashMap<String, Ledger>,
    holds: HashMap<HoldId, Hold>,
    next: HoldId,
    journal: Vec<Entry>,
}

impl Books {
    /// `account` and its ancestors, nearest first.
    fn chain(&self, account: &str) -> Result<Vec<String>, Refusal> {
        let mut chain = Vec::new();
        let mut at = Some(account.to_owned());
        while let Some(name) = at {
            let ledger = self
                .ledgers
                .get(&name)
                .ok_or_else(|| Refusal::Unknown(name.clone()))?;
            at = ledger.parent.clone();
            chain.push(name);
        }
        Ok(chain)
    }

    fn apply(&mut self, chain: &[String], f: impl Fn(&mut Ledger)) {
        for name in chain {
            if let Some(l) = self.ledgers.get_mut(name) {
                f(l);
            }
        }
    }
}

/// Ledgers nested account within team within customer, as deep as needed.
/// A hold is taken on an account and all its ancestors at once, or on none.
/// Cloning shares the books.
#[derive(Debug, Clone, Default)]
pub struct Bank(Arc<Mutex<Books>>);

impl Bank {
    /// A bank with no ledgers.
    pub fn new() -> Self {
        Self::default()
    }

    fn books(&self) -> MutexGuard<'_, Books> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Open ledger `name` under `parent` with `funds`. Reopening one resets
    /// its funds and keeps what it spent.
    pub fn open(&self, name: &str, parent: Option<&str>, funds: Micros) -> &Self {
        let mut books = self.books();
        let ledger = books.ledgers.entry(name.to_owned()).or_insert(Ledger {
            parent: None,
            funds: 0,
            spent: 0,
            held: 0,
        });
        ledger.parent = parent.map(str::to_owned);
        ledger.funds = funds;
        drop(books);
        self
    }

    /// Whether `account` exists.
    pub fn knows(&self, account: &str) -> bool {
        self.books().ledgers.contains_key(account)
    }

    /// Hold `amount` on `account` and each ancestor, atomically.
    pub fn hold(&self, account: &str, amount: Micros) -> Result<HoldId, Refusal> {
        let mut books = self.books();
        let chain = books.chain(account)?;
        for name in &chain {
            let available = books.ledgers[name].available();
            if available < amount {
                let ledger = name.clone();
                return Err(Refusal::Short { ledger, available });
            }
        }
        books.apply(&chain, |l| l.held += amount);
        books.next += 1;
        let id = books.next;
        let account = account.to_owned();
        books.holds.insert(id, Hold { account, amount });
        Ok(id)
    }

    /// Settle hold `id` for `charged`, which may exceed it: usage already
    /// incurred is charged even into overdraft. Idempotent: `false` if it
    /// was settled already.
    pub fn settle(&self, id: HoldId, charged: Micros, basis: Basis) -> bool {
        let mut books = self.books();
        let Some(hold) = books.holds.remove(&id) else {
            return false;
        };
        let chain = books.chain(&hold.account).unwrap_or_default();
        books.apply(&chain, |l| {
            l.held -= hold.amount;
            l.spent += charged;
        });
        books.journal.push(Entry {
            hold: Some(id),
            account: hold.account,
            held: hold.amount,
            charged,
            basis,
        });
        true
    }

    /// Charge `account` and its ancestors `amount` with no hold, such as for
    /// usage a plugin answered with. `false` for an unknown account.
    pub fn charge(&self, account: &str, amount: Micros, basis: Basis) -> bool {
        let mut books = self.books();
        let Ok(chain) = books.chain(account) else {
            return false;
        };
        books.apply(&chain, |l| l.spent += amount);
        books.journal.push(Entry {
            hold: None,
            account: account.to_owned(),
            held: 0,
            charged: amount,
            basis,
        });
        true
    }

    /// What `ledger` has left, holds deducted.
    pub fn available(&self, ledger: &str) -> Micros {
        self.books()
            .ledgers
            .get(ledger)
            .map_or(0, Ledger::available)
    }

    /// What `ledger` has spent.
    pub fn spent(&self, ledger: &str) -> Micros {
        self.books().ledgers.get(ledger).map_or(0, |l| l.spent)
    }

    /// What `ledger` has on hold.
    pub fn held(&self, ledger: &str) -> Micros {
        self.books().ledgers.get(ledger).map_or(0, |l| l.held)
    }

    /// Holds not yet settled.
    pub fn open_holds(&self) -> usize {
        self.books().holds.len()
    }

    /// Every settlement and charge, in order.
    pub fn journal(&self) -> Vec<Entry> {
        self.books().journal.clone()
    }
}
