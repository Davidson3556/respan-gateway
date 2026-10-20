#![warn(missing_docs)]
//! The green tool vendors, one module each: maintained, and verified end
//! to end against their live APIs. A yellow vendor moves here once it is.

pub mod apify;
pub mod browserbase;
pub mod cloudinary;
pub mod context7;
pub mod daytona;
pub mod linkup;
pub mod reducto;
pub mod tavily;
pub mod tinyfish;
pub mod zep;

use respan_gateway_tool_vendor_kit::ToolVendor;

/// Every green tool vendor.
pub const ALL: &[&ToolVendor] = &[
    &apify::VENDOR,
    &browserbase::VENDOR,
    &cloudinary::VENDOR,
    &context7::VENDOR,
    &daytona::VENDOR,
    &linkup::VENDOR,
    &reducto::VENDOR,
    &tavily::VENDOR,
    &tinyfish::VENDOR,
    &zep::VENDOR,
];
