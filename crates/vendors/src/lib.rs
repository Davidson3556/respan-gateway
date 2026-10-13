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

use respan_gateway_passthrough_http::Passthrough;

pub use respan_gateway_vendor_kit::{
    Auth, Base, Format, Part, Quirk, Speaks, Vendor, VendorProvider,
};

mod chat;

pub mod anthropic;
pub mod azure;
pub mod deepseek;
pub mod fireworks;
pub mod gemini;
pub mod groq;
pub mod mistral;
pub mod ollama;
pub mod openai;
pub mod openrouter;
pub mod together;
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
];

/// The vendor `id`.
pub fn find(id: &str) -> Option<&'static Vendor> {
    ALL.iter().copied().find(|p| p.id == id)
}

/// Format `name` as `vendor` speaks it, else as a provider of no vendor
/// does: as the format's own vendor, the first in [`ALL`] that speaks it;
/// passthrough at the origin. `None` for a format no vendor speaks.
pub fn speaks(vendor: Option<&Vendor>, name: &str) -> Option<Speaks> {
    let own = vendor.into_iter().flat_map(|v| v.formats);
    let spoken = own.chain(ALL.iter().flat_map(|v| v.formats));
    match spoken.clone().find(|s| s.format.name() == name) {
        Some(speaks) => Some(speaks.clone()),
        None => (name == "passthrough")
            .then(|| Speaks::new(Format::Passthrough(Passthrough)).at(Base::Origin)),
    }
}
