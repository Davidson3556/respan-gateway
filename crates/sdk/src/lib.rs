//! Optional helpers built only on the public kernel API. Nothing here may use
//! private kernel access; if a helper needs it, the kernel API is missing
//! something.
//!
//! Planned modules: `route` (fallback, hedge, split, translate), `text`
//! (rewriting with held tails), `memo`, `ledger` (wallets, reserve/settle,
//! job holds), `limit`, `admission`, `watch` (stall), `files`.
