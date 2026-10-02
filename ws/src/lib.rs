#![warn(missing_docs)]
//! WebSocket sessions on the gateway, as a neutral building block.
//!
//! A WebSocket is an HTTP Upgrade exchange on the HTTP wire: the call's head
//! is the upgrade request, the answer's head is the `101`, and the two bodies
//! are streams of frames, **one body item per text frame**. This integration provides
//! the outbound transport, a format, and the contract of a session's close;
//! hosting a client's session is an adapter's (`respan_gateway_axum::ws`,
//! which documents how each end of a session reaches the client):
//!
//! - [`WsTransport`]: an outbound native HTTP sender
//!   that connects to a `ws://` or `wss://` provider.
//! - [`JsonEvents`]: a [`Format`](respan_gateway_sdk::Format) whose kind is
//!   one JSON event per frame ([`ClientEvent`] in, [`ProviderEvent`] out),
//!   for plugins that filter a session's events.
//! - [`CloseCode`] and [`ProviderClose`]: the code a client is closed with,
//!   and the one a provider closed with.
//! - [`Limits`]: what either edge takes of a session, in bytes and time.
//!
//! Frames are text only; a binary frame is a fault. Nothing here knows a
//! provider's events.

use std::time::Duration;

mod events;
mod transport;

pub use events::{ClientEvent, JsonEvents, ProviderEvent};
pub use transport::WsTransport;

/// What an edge of a session takes: the provider edge's ([`WsTransport`])
/// and the client edge's (an adapter's). [`Default`] gives each its
/// default, with its reason.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// The largest message the edge accepts, and of a provider's refusal of
    /// the upgrade, its body: by default 16 MiB, above the largest event a
    /// provider takes (OpenAI Realtime's `input_audio_buffer.append`, up to
    /// 15 MiB). A larger one is a fault.
    pub max_message: usize,
    /// How long a side waits for the other's part of a close: the gateway
    /// for a provider's Close after it sent one, and an adapter for the
    /// response's end after its client sent Close. By default 5 s: a
    /// provider answers a Close behind the frames it was already sending,
    /// such as the rest of a turn, and one that does not answer is not
    /// waited on longer.
    pub close_wait: Duration,
    /// How long a Close is given to leave, to either side: by default 1 s,
    /// as a Close is under 130 bytes, so a peer that has not taken it in
    /// that time has stopped reading, and waiting on it would keep the
    /// session alive past its end.
    pub close_send: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_message: 16 << 20,
            close_wait: Duration::from_secs(5),
            close_send: Duration::from_secs(1),
        }
    }
}

/// The WebSocket close code a fault asks for, a fact ([`Fault::with`]) that
/// the client edge sends as its Close in place of 1011: a plugin's
/// `Fault::new(msg).with(CloseCode(1008))`. A provider's own code is a
/// [`ProviderClose`], which reaches the client only as this: set by a
/// plugin, or by [`WsTransport`] on a call relayed with
/// [`RawErrors`](respan_gateway_http::RawErrors), as
/// [`Relay`](respan_gateway_http::relay::Relay) relays one.
///
/// [`Fault::with`]: respan_gateway_sdk::Fault::with
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CloseCode(pub u16);

impl CloseCode {
    /// This code if a Close frame may carry it (RFC 6455 7.4), else 1011:
    /// 1004 to 1006, 1015, and any outside 1000 to 4999 may not be sent.
    pub fn sendable(self) -> u16 {
        match self.0 {
            1000..=1003 | 1007..=1014 | 3000..=4999 => self.0,
            _ => 1011,
        }
    }
}

/// The code a provider closed its session with, other than 1000: a fact on
/// the `upstream` fault [`WsTransport`] ends the answer with. It says why
/// the provider ended; whether the client is told is a plugin's choice
/// ([`CloseCode`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProviderClose(pub u16);
