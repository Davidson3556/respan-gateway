#![warn(missing_docs)]
#![doc = include_str!("../README.md")]
//! HTTP at the gateway's edges: its formats and its rules.
//!
//! Formats implement the kernel's `Format` directly over [`ClientIo`] and
//! [`ProviderIo`]. Native HTTP messages meet standard event streams and sinks;
//! [`Encoder`] supplies synchronous stateful framing where needed.
//!
//! Its rules live at the four places that own them:
//!
//! - **The adapter** ([`handle`], [`ClientIo::accept`]) strips the
//!   connection's headers from a request, lifts the client's credentials
//!   into a [`Credential`], publishes the request as it came ([`Wire`]),
//!   writes what no format can in plain text, and strips what a hop must not
//!   carry from the response.
//! - **The bindings** ([`ClientIo::bind`], [`ProviderIo::bind`]) write no
//!   body where HTTP has none ([`bodiless`]), write a fault's status and
//!   `Retry-After`, read a refused answer whole into a fault, and strip
//!   from every call what a proxy must not pass on, whatever its transport.
//! - **An outbound transport** can bound its provider head and polled body
//!   frame gaps with [`watchdog`]. HTTP retry classification is explicitly
//!   selected by a routing caller through [`retryable`]; it is not built into
//!   routing's timing-only backoff.
//! - **A transport** decides whose keys reach the provider ([`credentials`],
//!   [`CallHeaders`]), joins its path to its base URL ([`base_url`],
//!   [`prepare_call`]), and strips a provider origin's own headers from its
//!   response ([`strip_origin_headers`]).
//!
//! The crate by job:
//!
//! - `edge`: how HTTP meets the kernel, [`ClientIo`] and [`handle`] for a
//!   server, [`ProviderIo`] and [`connect`] for a transport, [`Body`] and
//!   [`Encoder`] between them.
//! - `rules`: HTTP's semantics, a fault's [`status`] and [`error_code`],
//!   [`retryable`], [`bodiless`], and the headers a hop must not carry.
//! - `call`: an outbound call, its URL, keys and headers ([`prepare_call`],
//!   [`credentials`]) and its [`watchdog`].
//! - `raw`: the client's request and the provider's answer as they came
//!   ([`Wire`], [`SendRaw`], [`RawErrors`]) and the [`Relay`] that forwards them.
//! - [`framing`] and [`passthrough`]: what formats build on, and the format of
//!   no kind.
//!
//! Hosting a gateway is `respan-gateway-serve`'s, not a wire's.

extern crate self as respan_gateway_http;

pub mod framing;
pub mod passthrough;

mod call;
mod credential;
mod edge;
mod raw;
mod rules;
#[cfg(feature = "reqwest")]
mod transport;

#[cfg(feature = "reqwest")]
pub use reqwest;
/// The crates HTTP formats and transports use, at these versions.
pub use {bytes, http, http_body, http_body_util};

pub use call::{
    CallHeaders, Completion, Frames, base_url, credentials, is_connection_header, prepare_call,
    strip_origin_headers, watchdog,
};
pub use credential::Credential;
pub use edge::{
    Body, ClientIo, Encoder, Framing, ProviderIo, Reply, connect, encode_all, handle, messages,
};
pub use raw::{ClientHead, ProviderError, ProviderHead, RawErrors, RawReply, Relay, SendRaw, Wire};
pub use rules::{
    Status, bodiless, built_wrong, error_code, header_tokens, is_token, percent_decode, plain,
    provider_fault, retryable, status,
};
#[cfg(feature = "reqwest")]
pub use transport::HttpTransport;
