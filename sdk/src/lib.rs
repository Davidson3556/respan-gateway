#![warn(missing_docs)]
//! The plugin-author facade: the core contracts and the shared LLM vocabulary.
//!
//! - The core contracts are re-exported as they are, not wrapped.
//! - The conversation kind: a [`Conversation`] in, [`Reply`] events out (one
//!   event of a reply each), [`Assembled`] folding them, and [`Extra`] for what
//!   a format has that the kind does not.
//! - [`Usage`]: integer amounts of named units, kind-neutral.
//! - Fault facts: [`BadRequest`], [`Upstream`], [`Truncated`], [`RetryAfter`],
//!   [`ErrorCode`] and [`Details`].
//!
//! HTTP bindings live in `respan_gateway_http`.
//!
//! ```
//! use respan_gateway_sdk::{
//!     ClientContext, Conversation, Fault, Filter, Plugin, Reply, Response, Usage,
//! };
//! ```

pub use respan_gateway_core::*;

pub mod conversation;
mod fault;
mod usage;

pub use conversation::{
    Assembled, Block, Content, Conversation, Delta, Extra, Finish, Message, Reply, Role, Start,
    Tool, ToolChoice, set_model,
};
pub use fault::{BadRequest, Details, ErrorCode, RetryAfter, Truncated, Upstream};
pub use usage::Usage;
