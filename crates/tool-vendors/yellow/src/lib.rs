#![warn(missing_docs)]
//! The yellow tool vendors, one module each: maintained, but not verified
//! end to end against their live APIs. A vendor moves to green once it is.

pub mod apify;
pub mod browserbase;
pub mod cloudinary;
pub mod context7;
pub mod daytona;
pub mod e2b;
pub mod elevenlabs;
pub mod reducto;
pub mod tavily;
pub mod tinyfish;
pub mod zep;

use respan_gateway_tool_vendor_kit::ToolVendor;

/// Every yellow tool vendor.
pub const ALL: &[&ToolVendor] = &[
    &apify::VENDOR,
    &browserbase::VENDOR,
    &cloudinary::VENDOR,
    &context7::VENDOR,
    &daytona::VENDOR,
    &e2b::VENDOR,
    &elevenlabs::VENDOR,
    &reducto::VENDOR,
    &tavily::VENDOR,
    &tinyfish::VENDOR,
    &zep::VENDOR,
];
