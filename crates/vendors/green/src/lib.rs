#![warn(missing_docs)]
//! The green vendors, one module each: maintained, and verified end to end
//! against their live APIs by the validation package's live tests
//! (`validation/tests/live_*.rs`), which a test here checks each has. A
//! vendor is green only while those tests run against it.

pub mod anthropic;
pub mod gemini;
pub mod openai;
pub mod typesafe;

use respan_gateway_vendor_kit::Vendor;

/// Every green vendor.
pub const ALL: &[&Vendor] = &[
    &openai::VENDOR,
    &anthropic::VENDOR,
    &gemini::VENDOR,
    &typesafe::VENDOR,
];
