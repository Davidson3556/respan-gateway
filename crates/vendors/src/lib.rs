#![warn(missing_docs)]
//! The vendors, one module each: where a vendor is, how it takes a key, the
//! formats it speaks, and its quirks, each a [`Vendor`]. [`ALL`] lists them.
//! [`Vendor::provider`] makes a provider of one ([`VendorProvider`]), which
//! registers a core provider of each of its formats and lists its models.
//!
//! A quirk is a vendor's deployment departing from a spec it speaks; its
//! module handles it, as data or as a plugin each of its core providers
//! carries ([`Vendor::plugin`]), and tests it in `tests/<id>.rs` by a test
//! named for it.

use respan_gateway_media_http::openai::OpenAiVideos;
use respan_gateway_passthrough_http::Passthrough;

pub use respan_gateway_vendor_kit::{
    Auth, Base, Body, BoxFuture, Connect, HttpTransport, ListModels, Model, Models, Part, Pricing,
    Quirk, Reach, Speaks, Vendor, VendorProvider, Wire, built_wrong, http, json, models,
};

mod chat;

pub mod anthropic;
pub mod azure;
pub mod cloudflare;
pub mod deepseek;
pub mod fireworks;
pub mod gemini;
pub mod groq;
pub mod mistral;
pub mod ollama;
pub mod openai;
pub mod openrouter;
pub mod together;
pub mod typesafe;
pub mod vllm;
pub mod xai;

/// Every vendor. A format's own vendor, whose spec it is, comes before any
/// other that speaks it: a provider of no vendor takes its key and sits as
/// that vendor's does.
pub const ALL: &[&Vendor] = &[
    &openai::VENDOR,
    &anthropic::VENDOR,
    &gemini::VENDOR,
    &groq::VENDOR,
    &deepseek::VENDOR,
    &openrouter::VENDOR,
    &xai::VENDOR,
    &mistral::VENDOR,
    &together::VENDOR,
    &fireworks::VENDOR,
    &ollama::VENDOR,
    &azure::VENDOR,
    &vllm::VENDOR,
    &typesafe::VENDOR,
    &cloudflare::VENDOR,
];

/// The vendor `id`.
pub fn find(id: &str) -> Option<&'static Vendor> {
    ALL.iter().copied().find(|p| p.id == id)
}

/// Format `name` as `vendor` speaks it, else as a provider of no vendor
/// does: as the format's own vendor, the first in [`ALL`] that speaks it,
/// else as its spec has it ([`UNSPOKEN`]). `None` for a format of neither.
pub fn speaks(vendor: Option<&Vendor>, name: &str) -> Option<&'static dyn Wire> {
    let own = vendor.into_iter().flat_map(|v| v.formats);
    let spoken = own.chain(ALL.iter().flat_map(|v| v.formats));
    spoken.chain(UNSPOKEN).copied().find(|f| f.name() == name)
}

/// The formats no vendor speaks, as a provider of no vendor speaks them:
/// passthrough at the origin, and OpenAI's Videos, which OpenAI no longer
/// serves, at the base URL as its spec has it, for a provider that does.
pub const UNSPOKEN: &[&dyn Wire] = &[
    &Speaks::http(Passthrough).at(Base::Origin),
    &Speaks::http(OpenAiVideos),
];
