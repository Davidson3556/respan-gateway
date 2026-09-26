#![warn(missing_docs)]
//! First-party plugins. They use only the public kernel API, like any other.

mod router;

pub use router::ModelRouter;
