#![warn(missing_docs)]
//! The respan-gateway kernel: a [`Gateway`] runs each request through its
//! [`Plugin`]s and streams the [`Response`] back.
//! `docs/ARCHITECTURE.md` describes the pipeline.

mod call;
mod context;
mod deliver;
mod fault;
mod format;
mod gateway;
mod input;
mod lifecycle;
mod plugin;
mod provider;
mod response;
mod transport;
mod usage;

pub use call::Call;
pub use context::ClientContext;
pub use fault::{Fault, codes};
pub use format::{Decoder, Encoder, Format};
pub use gateway::Gateway;
pub use plugin::{Filter, Outcome, Plugin};
pub use provider::ProviderContext;
pub use response::Response;
pub use transport::{Body, Transport};
pub use usage::Usage;

/// The crates whose types the kernel's contracts use, so a format or plugin
/// builds against the same versions.
pub use {async_trait::async_trait, bytes, futures, http, http_body, http_body_util, serde_json};

/// Calling and reading inputs take `&mut ClientContext`, which `on_client`
/// alone gets: end hooks and a call's hooks see the request by `&`.
///
/// ```
/// # use respan_gateway_core::ClientContext;
/// fn on_client(cx: &mut ClientContext) {
///     let _ = (cx.call("p"), cx.take_inputs::<String>());
/// }
/// ```
///
/// ```compile_fail
/// # use respan_gateway_core::ClientContext;
/// fn on_client_end(cx: &ClientContext) {
///     let _ = cx.call("p");
/// }
/// ```
///
/// ```compile_fail
/// # use respan_gateway_core::ProviderContext;
/// fn on_provider(px: &mut ProviderContext) {
///     let _ = px.client().take_inputs::<String>();
/// }
/// ```
#[cfg(doctest)]
struct OnlyOnClientCalls;
