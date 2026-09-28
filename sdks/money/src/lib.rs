#![warn(missing_docs)]
//! Budgets and billing for respan-gateway, on the public core API alone.
//!
//! ```ignore
//! let bank = Bank::new();
//! bank.open("acme", None, 50_000_000).open("sk-1", Some("acme"), 5_000_000);
//! let prices = Prices::new().model("gpt-x", Price::new(10, 30));
//! let gw = Gateway::new(providers)?
//!     .serve(OpenAiChat)?
//!     .plugin("money", Money::new(bank, prices))? // first
//!     .plugin("router", router)?
//!     .target("openai")?;
//! ```
//!
//! Its real purpose is to find where the core falls short: see
//! `FINDINGS.md`.

mod bank;
mod plugin;
mod price;

pub use bank::{Bank, Basis, Entry, HoldId, Micros, Refusal};
pub use plugin::{Abandoned, ClientEdge, Money, ProviderEdge};
pub use price::{Price, Prices, Tally, estimate_input};
