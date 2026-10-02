#![warn(missing_docs)]
//! First-party policies built on public core APIs.
//! [`ModelRouter`] selects a provider target from the input model.
//! Billing policies and reservation stores live in application/example code.

mod router;

/// Attempt orchestration, fallback, races and caller-classified retries.
pub mod routing;

pub use router::ModelRouter;

#[cfg(feature = "http-relay")]
pub mod relay;
