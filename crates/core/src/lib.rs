//! The respan-gateway kernel.
//!
//! Six concepts: [`Turn`], [`Part`], [`Plugin`], [`Store`], [`Format`] and
//! [`Policy`]. See `docs/ARCHITECTURE.md`. Everything here is a signature
//! stub; nothing is implemented yet.

mod fault;
mod format;
mod part;
mod plugin;
mod policy;
mod store;
mod turn;

pub use fault::Fault;
pub use format::Format;
pub use part::{Delta, DeltaKind, Part};
pub use plugin::{Covers, Plugin};
pub use policy::Policy;
pub use store::Store;
pub use turn::{Attempt, Call, Req, Turn, Upstream};
