#![warn(missing_docs)]
//! Ledger storage and neutral metering primitives. [`Ledger`] reserves and
//! settles holds; [`MemoryLedger`] stores nested accounts in one process.
//! [`Tally`] shares a saturating cost counter across its clones. Amounts are
//! whatever unit the application counts. The charging policy, including
//! quotes and provider-call lifecycle, belongs to
//! `respan_gateway_plugins::Accounting`, not this SDK.

mod ledger;
mod tally;

pub use ledger::{Amount, HoldId, Ledger, MemoryLedger};
pub use tally::Tally;

/// A 402: an account cannot cover a call's hold.
pub const INSUFFICIENT_FUNDS: &str = "insufficient_funds";
/// A 401: a quote named an account the ledger does not know.
pub const UNKNOWN_ACCOUNT: &str = "unknown_account";
