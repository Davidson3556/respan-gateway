#![warn(missing_docs)]
//! First-party policies built on public core and SDK APIs.
//! [`Accounting`] uses the ledger SDK's holds and [`respan_gateway_ledger::Tally`]
//! to charge provider calls; [`ModelRouter`] selects a model target.
//!
//! Accounting leaves authentication, cost bounds and pricing to the application:
//!
//! ```ignore
//! use respan_gateway_ledger::MemoryLedger;
//! use respan_gateway_messages::conversation::Reply;
//! use respan_gateway_plugins::{Accounting, Quote};
//!
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
mod router;

pub use accounting::{Accounting, Quote};
pub use router::ModelRouter;

/// A 401: an accounting quote named no account to charge.
pub const NO_ACCOUNT: &str = "no_account";
