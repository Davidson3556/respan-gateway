#![warn(missing_docs)]
//! The green tool vendors, one module each: maintained, and verified end
//! to end against their live APIs. A yellow vendor moves here once it is.

pub mod linkup;

use respan_gateway_tool_vendor_kit::ToolVendor;

/// Every green tool vendor.
pub const ALL: &[&ToolVendor] = &[&linkup::VENDOR];
