#![warn(missing_docs)]
//! The respan-gateway kernel: a [`Gateway`] runs each request through its
//! route's [`Plugin`] chain and streams the committed [`Response`] back.
//! `docs/ARCHITECTURE.md` describes the pipeline.

mod context;
mod fault;
mod format;
mod gateway;
mod part;
mod plugin;
mod response;
mod transport;

pub use context::{Call, Context};
pub use fault::{Fault, codes};
pub use format::{Decoder, Encoder, Format, Kind, KindType, RequestView};
pub use gateway::{Closed, Gateway, Provider, Route, Target};
pub use part::{Doc, Item, Part, Usage};
pub use plugin::{Filter, Outcome, Plugin};
pub use response::Response;
pub use transport::{Body, Transport};

/// The crates whose types the kernel's contracts use, so a format or plugin
/// builds against the same versions.
pub use {async_trait::async_trait, bytes, futures, http, http_body, http_body_util, serde_json};
