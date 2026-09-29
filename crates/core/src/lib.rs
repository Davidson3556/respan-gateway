#![warn(missing_docs)]
//! The respan-gateway kernel: a [`Gateway`] runs each request through its
//! [`Plugin`]s and streams the [`Response`] back (`docs/ARCHITECTURE.md`).
//!
//! The kernel routes events: a client edge and a provider edge, each a
//! [`Dialect`] over a connection, turn bytes and heads into a kind's events
//! and back. [`Format`] and [`Transport`] are served as dialects by the byte
//! driver the kernel had (decision 0020, until its flip).

mod call;
mod context;
mod dialect;
mod extensions;
mod fault;
mod gateway;
mod leg;
mod legacy;
mod lifecycle;
mod opening;
mod plugin;
mod provider;
mod response;
mod session;

pub use call::Call;
pub use context::ClientContext;
pub use dialect::{Dialect, Dialed, Ended, Events, Half, Origin, Reply};
pub use extensions::Extensions;
pub use fault::{Fault, codes};
pub use gateway::Gateway;
pub use legacy::{Body, Decoder, Encoder, Exchange, Format, Head, Transport};
pub use plugin::{Filter, Plugin};
pub use provider::ProviderContext;
pub use response::Response;

/// The crates the kernel's contracts use, at the kernel's versions.
pub use {async_trait::async_trait, bytes, futures, serde_json};

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
