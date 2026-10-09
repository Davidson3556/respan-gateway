#![warn(missing_docs)]
//! General plugin-author facade: core contracts, usage and fault facts.
//!
//! Core contracts are re-exported as their original types. [`Usage`] records
//! integer amounts of named units; typed fault facts describe failures for
//! each protocol edge to render.
//!
//! Input requests and reply events live in `respan_gateway_conversation`.
//! HTTP bindings live in `respan_gateway_http`.
//!
//! ```
//! use respan_gateway_sdk::{
//!     ClientContext, Fault, Filter, Plugin, Response, Usage,
//! };
//! ```

pub use respan_gateway_core::*;

mod fault;
mod native;
mod usage;

pub use fault::{BadRequest, Details, ErrorCode, RetryAfter, Truncated, Upstream};
pub use native::{FormatId, NativeFields, NativeValue};
pub use usage::Usage;
