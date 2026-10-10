#![warn(missing_docs)]
//! Chat formats over HTTP: JSON requests, JSON or SSE responses, of the
//! conversation kind. [`openai`] is OpenAI Chat Completions and its vendor
//! dialects, [`anthropic`] is Anthropic Messages; each is usable on either
//! edge, and they translate through the kind's events.
//!
//! Another such format implements [`Codec`]: its names, how it reads and
//! writes a request and a part, its response [`Reader`], whole-body and
//! stream encoders, and error body. The HTTP binding around it, at both
//! edges, is the trait's provided methods ([`Codec::bind_client`],
//! [`Codec::bind_provider`]).

// Some helpers serve one vendor; without both, they go unused.
#[cfg_attr(
    not(all(feature = "openai", feature = "anthropic")),
    allow(dead_code, unused_imports)
)]
mod codec;

#[cfg(feature = "anthropic")]
pub mod anthropic;
#[cfg(feature = "openai")]
pub mod openai;

pub use codec::{Asked, Codec, Reader, StreamResponse, merge_details};

#[cfg(all(test, feature = "openai", feature = "anthropic"))]
mod tests;
