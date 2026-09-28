#![warn(missing_docs)]
//! Reserve before a call and settle once from its usage, against a store
//! and prices you supply. Neutral: amounts are whatever unit you count,
//! and the quote decides who pays, the most a call can cost, and its price.
//!
//! ```ignore
//! let ledger = MemoryLedger::new();
//! ledger.open("acme", None, 1_000_000).open("sk-1", Some("acme"), 100_000);
//! let accounting = Accounting::new(ledger.clone(), |px| {
//!     let key = my_key(px.client())?; // your auth
//!     let (bound, rate) = my_bound(px)?; // Err: no cost bound, refused
//!     Ok(Quote::new([key], bound, move |usage| rate.of(usage)))
//! });
//! let gw = gw.plugin("accounting", accounting)?; // last
//! ```

mod accounting;
mod ledger;

pub use accounting::{Accounting, Quote, Unreported};
pub use ledger::{Amount, HoldId, Ledger, MemoryLedger};

/// A 402: an account cannot cover a call's hold.
pub const INSUFFICIENT_FUNDS: &str = "insufficient_funds";
/// A 401: a quote named an account the ledger does not know.
pub const UNKNOWN_ACCOUNT: &str = "unknown_account";
/// A 401: a quote named no account to charge.
pub const NO_ACCOUNT: &str = "no_account";
