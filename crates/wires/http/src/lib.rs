#![warn(missing_docs)]
#![doc = include_str!("../README.md")]
//! HTTP at the gateway's edges: its formats and its rules.
//!
//! Formats implement the kernel's `Format` directly over [`ClientIo`] and
//! [`ProviderIo`]: a format gives each edge an [`Encoder`] of the values it
//! writes and a [`Decoder`] of those it reads, and the binding drives them
//! over the body.
//!
//! Its rules live at the four places that own them:
//!
//! - **The adapter** ([`handle`], [`ClientIo::accept`]) strips the
//!   connection's headers from a request, lifts the client's credentials
//!   into a [`Credential`], publishes the request as it came ([`Original`](crate::raw::Original)),
//!   writes what no format can in plain text, and strips what a hop must not
//!   carry from the response.
//! - **The bindings** ([`ClientIo::bind`], [`ProviderIo::bind`]) write no
//!   body where HTTP has none ([`bodiless`](crate::rules::bodiless)), write a fault's status and
//!   `Retry-After`, read a refused answer whole into a fault, and strip
//!   from every call what a proxy must not pass on, whatever its transport.
//! - **An outbound transport** can bound its provider head and polled body
//!   frame gaps with [`watchdog`](crate::call::watchdog). HTTP retry classification is explicitly
//!   selected by a routing caller through [`retryable`](respan_gateway_http_faults::retryable); it is not built into
//!   routing's timing-only backoff.
//! - **A transport** decides whose keys reach the provider ([`credentials`](crate::call::credentials),
//!   [`CallHeaders`](crate::call::CallHeaders)), joins its path to its base URL ([`base_url`](crate::call::base_url),
//!   [`prepare_call`](crate::call::prepare_call)), and strips a provider origin's own headers from its
//!   response ([`strip_origin_headers`](crate::call::strip_origin_headers)).
//!
//! The crate by job; the root holds the wire itself, `edge`'s:
//!
//! - `edge`: how HTTP meets the kernel, [`ClientIo`] and [`handle`] for a
//!   server, [`ProviderIo`] and [`connect`] for a transport, and [`Body`].
//! - `codec`: [`Encoder`] and [`Decoder`], and the framings [`Sse`] and
//!   [`Whole`].
//! - [`rules`]: HTTP's semantics, a fault's [`status`](respan_gateway_http_faults::status) and [`error_code`](respan_gateway_http_faults::error_code),
//!   [`retryable`](respan_gateway_http_faults::retryable), [`bodiless`](crate::rules::bodiless), and the headers a hop must not carry.
//! - [`call`]: an outbound call, its URL, keys and headers ([`prepare_call`](crate::call::prepare_call),
//!   [`credentials`](crate::call::credentials)) and its [`watchdog`](crate::call::watchdog).
//! - [`raw`]: the client's request and the provider's answer as they came
//!   ([`Original`](crate::raw::Original), [`RawReply`](crate::raw::RawReply)), and the mark of a call relayed to a provider of the client's own format ([`Relayed`](crate::raw::Relayed)), which `respan-gateway-plugin-relay`'s `Relay` sets.
//! - [`framing`]: what formats build on: JSON of a body or event, and whether
//!   a response is a stream.
//!
//! Hosting a gateway is `respan-gateway-serve`'s, not a wire's.

extern crate self as respan_gateway_http;

pub mod framing;

pub mod call;
mod codec;
mod credential;
mod edge;
pub mod raw;
pub mod rules;

/// The crates HTTP formats and transports use, at these versions.
pub use {bytes, http, http_body, http_body_util};

pub use codec::{Decoder, Encoder, Sse, Whole};
pub use credential::Credential;
pub use edge::{Body, ClientIo, Completion, Frames, Framing, ProviderIo, Reply, connect, handle};
