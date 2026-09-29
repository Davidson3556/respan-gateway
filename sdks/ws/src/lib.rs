#![warn(missing_docs)]
//! WebSocket sessions on the gateway, as a neutral building block.
//!
//! A WebSocket is an HTTP Upgrade exchange on the HTTP wire: the call's head
//! is the upgrade request, the answer's head is the `101`, and the two bodies
//! are streams of frames, **one body item per text frame**. This crate is
//! the two edges and a format for it:
//!
//! - [`WsTransport`]: an outbound [`Transport`](respan_gateway_core::Transport)
//!   that connects to a `ws://` or `wss://` provider.
//! - [`serve_ws`] and [`router`]: an axum adapter that accepts the client's
//!   upgrade and pumps frames both ways.
//! - [`JsonEvents`]: a [`Format`](respan_gateway_core::Format) whose kind is
//!   one JSON event per frame ([`ClientEvent`] in, [`ProviderEvent`] out),
//!   for plugins that filter a session's events.
//!
//! Frames are text only; a binary frame is a fault. Nothing here knows a
//! provider's events.
//!
//! # How a session ends
//!
//! | The end | Input stream (client to provider) | Output stream (provider to client) |
//! |---|---|---|
//! | Client sends Close | ends clean | the provider is sent Close 1000; its end, however it comes, is clean |
//! | Client drops without Close | ends `Err(bad_request)` | dropped, so calls and request end `abandoned` |
//! | Client sends a binary frame | ends `Err(bad_request)` | the client is sent Close 1003 |
//! | Provider sends Close 1000, or none | drop | ends clean; the client is sent Close 1000 |
//! | Provider sends Close with another code | drop | ends `Err(upstream)` with a [`CloseCode`]; the client is sent that code |
//! | Provider drops without Close | drop | ends `Err(truncated)`; the client is sent Close 1011 |
//! | Provider sends a binary frame | drop | ends `Err(upstream)`; the client is sent Close 1011 |
//! | A fault ends the response | cut, `abandoned` | the client is sent Close 1011 (or the fault's [`CloseCode`]) with the fault's code as reason |
//!
//! A fault's message is never sent to the client, only its code.

use std::time::Duration;

mod events;
mod serve;
mod transport;

pub use events::{ClientEvent, JsonEvents, ProviderEvent};
pub use serve::{router, serve_ws};
pub use transport::WsTransport;

/// How long a side waits for the other's part of a close: the gateway for a
/// provider's Close after it sent the client's, and for the response's end
/// after the client sent Close.
pub const CLOSE_WAIT: Duration = Duration::from_secs(5);

/// The largest message either edge accepts, 16 MiB.
pub const MAX_MESSAGE: usize = 16 * 1024 * 1024;

/// The WebSocket close code a fault asks for, a hint ([`Fault::with`]) that
/// the client edge sends as its Close, in place of 1011: a plugin's
/// `Fault::new("over_limit", msg).with(CloseCode(1008))`, or the provider's
/// own, which [`WsTransport`] puts on the fault of a Close with a code
/// other than 1000. A code that may not be sent (1004 to 1006, 1015, and
/// any outside 1000 to 4999) is sent as 1011.
///
/// [`Fault::with`]: respan_gateway_core::Fault::with
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CloseCode(pub u16);

impl CloseCode {
    /// The code to send: this one if a Close may carry it, else 1011.
    pub(crate) fn sendable(self) -> u16 {
        match self.0 {
            1000..=1003 | 1007..=1014 | 3000..=4999 => self.0,
            _ => 1011,
        }
    }
}
