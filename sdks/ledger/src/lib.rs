#![warn(missing_docs)]
//! Reserve before a call and settle once from what it cost, against a
//! store and prices you supply. Neutral: amounts are whatever unit you
//! count, the quote decides who pays and the most a call can cost, and your
//! code reports what the call cost into its [`Tally`], metering it however
//! its kind reports usage.
//!
//! ```ignore
//! let ledger = MemoryLedger::new();
//! ledger.open("acme", None, 1_000_000).open("sk-1", Some("acme"), 100_000);
//! let accounting = Accounting::new(ledger.clone(), |px, tally| {
//!     let key = my_key(px.client())?; // your auth
//!     let (bound, rate) = my_bound(px)?; // Err: no cost bound, refused
//!     // The chat kind: price each usage increment the provider reports.
//!     px.filter(move |reply: &mut Reply| {
//!         if !reply.usage.is_empty() {
//!             tally.add(rate.of(&reply.usage));
//!         }
//!         Ok(())
//!     })?;
//!     Ok(Quote::new([key], bound))
//! });
//! let gw = gw.plugin("accounting", accounting)?; // last
//! ```

mod accounting;
mod ledger;

pub use accounting::{Accounting, Quote, Tally};
pub use ledger::{Amount, HoldId, Ledger, MemoryLedger};

/// A 402: an account cannot cover a call's hold.
pub const INSUFFICIENT_FUNDS: &str = "insufficient_funds";
/// A 401: a quote named an account the ledger does not know.
pub const UNKNOWN_ACCOUNT: &str = "unknown_account";
/// A 401: a quote named no account to charge.
pub const NO_ACCOUNT: &str = "no_account";
