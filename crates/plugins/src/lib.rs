#![warn(missing_docs)]
//! First-party policies built on public core APIs.
//! [`Routing`] targets each request at a provider by [`Routes`]: by its
//! model's `provider/` prefix, `models` tables, claimed prefixes, or its
//! kind's default.
//! [`agent::Agent`] runs tools inside chat requests, for every chat format.
//! Billing policies and reservation stores live in application/example code.

mod router;

/// The agent loop: the gateway runs tools inside a chat request.
pub mod agent;

/// Attempt orchestration, fallback, races and caller-classified retries.
pub mod routing;

pub use router::{Kind, Model, Route, Routes, Routing, Speaks};
