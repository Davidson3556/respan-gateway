//! The respan-gateway kernel.
//!
//! A [`Gateway`] takes [`Inbound`] work and runs the route's [`Plugin`] chain
//! with a [`Context`] and the client's [`Request`], until a plugin responds
//! or the route's target does. The committed [`Response`] streams back; one
//! nobody edits is relayed byte for byte. Close hooks run exactly once. See
//! `docs/ARCHITECTURE.md`.

mod config;
mod context;
mod edits;
mod fault;
mod format;
mod gateway;
mod inbound;
mod part;
mod plugin;
mod request;
mod response;
mod store;
mod target;
mod transport;

pub use config::{
    AuthConfig, Builder, Config, EntryConfig, HttpRouteConfig, ProviderConfig, RouteConfig,
};
pub use context::Context;
pub use edits::{Edits, Filter, PartRx, PartTx};
pub use fault::{Fault, codes};
pub use format::{Decoder, Encoder, Format};
pub use gateway::{Gateway, plain_error};
pub use inbound::{Credential, Envelope, Extensions, HttpHead, Inbound};
pub use part::{Delta, DeltaKind, Doc, End, Part, ResponseHead, Usage, units};
pub use plugin::{Flow, Plugin};
pub use request::{Peer, Principal, Request, Session};
pub(crate) use response::Closed;
pub use response::{Response, ResponseEnd};
pub use store::{Applied, Class, Key, MemoryStore, Op, Scope, Store};
pub use target::Target;
#[cfg(feature = "reqwest")]
pub use transport::HttpTransport;
pub use transport::{Body, Transport};
