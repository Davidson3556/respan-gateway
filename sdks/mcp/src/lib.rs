#![warn(missing_docs)]
//! MCP for respan-gateway, written only against the public API of its
//! crates, as a user outside the repository would. Its purpose is to find
//! where the kernel's contracts make a real protocol impossible, clumsy or
//! silently wrong; `FINDINGS.md` records what it found.
//!
//! - [`kind`]: the MCP kind, [`ToServer`] in and [`ToClient`] out.
//! - [`StreamableHttp`]: MCP over streamable HTTP as a format.
//! - [`ToolAllowList`]: a plugin that hides and refuses tools.
//! - [`StdioTransport`]: a child process speaking JSON-RPC lines.
//! - [`Aggregate`]: several MCP servers behind one endpoint.
//! - [`Passthrough`]: a relay-only format of no kind.

pub mod aggregate;
pub mod allow;
pub mod format;
pub mod kind;
pub mod passthrough;
pub mod stdio;

pub use aggregate::Aggregate;
pub use allow::ToolAllowList;
pub use format::{SESSION, StreamableHttp};
pub use kind::{Message, ToClient, ToServer, Verb};
pub use passthrough::Passthrough;
pub use stdio::StdioTransport;
