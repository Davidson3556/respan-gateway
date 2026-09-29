#![warn(missing_docs)]
//! The respan-gateway kernel: a [`Gateway`] runs each request through its
//! [`Plugin`]s and streams the [`Response`] back (`docs/ARCHITECTURE.md`).
//!
//! The kernel routes events and never sees a byte, a head or a key: a
//! client edge and a provider edge, each a [`Format`] over a connection,
//! turn bytes and heads into a kind's events and back (decision 0020). The
//! byte machinery of HTTP's formats lives in `respan-gateway-http`.

mod call;
mod context;
mod extensions;
mod fault;
mod format;
mod gateway;
mod leg;
mod lifecycle;
mod opening;
mod plugin;
mod provider;
mod response;
mod session;

pub use call::Call;
pub use context::ClientContext;
pub use extensions::Extensions;
pub use fault::{Fault, codes};
pub use format::{Dialed, Ended, Events, Format, Half, Origin};
pub use gateway::Gateway;
pub use plugin::{Filter, Plugin};
pub use provider::ProviderContext;
pub use response::Response;

/// The crates the kernel's contracts use, at the kernel's versions.
pub use {async_trait::async_trait, futures};

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
