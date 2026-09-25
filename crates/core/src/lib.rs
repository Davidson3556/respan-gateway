//! The respan-gateway kernel.
//!
//! A [`Gateway`] takes a client request, runs the route's [`Plugin`] chain,
//! sends [`ProviderRequest`]s through the route's source, and streams the
//! committed response back. A response nobody edits is relayed byte for byte.
//! Every request closes into exactly one [`Record`]. See
//! `docs/ARCHITECTURE.md`.

mod config;
mod edits;
mod fault;
mod format;
mod gateway;
mod part;
mod plugin;
mod providers;
mod record;
mod request;
mod store;
mod transport;

pub use config::{AuthConfig, Builder, Config, EntryConfig, ProviderConfig, RouteConfig};
pub use edits::{Edits, Filter, PartRx, PartTx};
pub use fault::{Fault, FaultKind};
pub use format::{Decoder, Encoder, Format};
pub use gateway::Gateway;
pub use part::{Delta, DeltaKind, Doc, End, Part, RequestHead, ResponseHead, Usage, units};
pub use plugin::Plugin;
pub use providers::{ClientResponse, ProviderResponse, Providers, Target};
pub use record::{AttemptEnd, AttemptRecord, Outcome, Record};
pub use request::{ClientRequest, Principal, ProviderRequest, Session};
pub use store::{Applied, Class, Key, MemoryStore, Op, Scope, Store};
#[cfg(feature = "reqwest")]
pub use transport::HttpTransport;
pub use transport::{Body, Transport};
