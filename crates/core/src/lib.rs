//! The respan-gateway kernel.
//!
//! A [`Gateway`] takes [`Inbound`] work, runs the route's [`Plugin`] chain on
//! the resulting [`ClientRequest`] until one responds (or the route's target
//! does), and streams the committed response back. A response
//! nobody edits is relayed byte for byte. Close hooks run exactly once. See
//! `docs/ARCHITECTURE.md`.

mod config;
mod edits;
mod fault;
mod format;
mod gateway;
mod inbound;
mod outcome;
mod part;
mod plugin;
mod providers;
mod request;
mod store;
mod transport;

pub use config::{
    AuthConfig, Builder, Config, EntryConfig, HttpRouteConfig, ProviderConfig, RouteConfig,
};
pub use edits::{Edits, Filter, PartRx, PartTx};
pub use fault::{Fault, codes};
pub use format::{Decoder, Encoder, Format};
pub use gateway::{Gateway, plain_error};
pub use inbound::{Credential, Envelope, Extensions, HttpHead, Inbound};
pub use outcome::{Outcome, ProviderEnd, ProviderOutcome};
pub use part::{Delta, DeltaKind, Doc, End, Part, ResponseHead, Usage, units};
pub use plugin::{Flow, Plugin};
pub use providers::{ClientResponse, ProviderResponse, Target};
pub use request::{ClientRequest, Principal, ProviderRequest, Session};
pub use store::{Applied, Class, Key, MemoryStore, Op, Scope, Store};
#[cfg(feature = "reqwest")]
pub use transport::HttpTransport;
pub use transport::{Body, Transport};
