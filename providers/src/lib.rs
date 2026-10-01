#![warn(missing_docs)]
//! First-party conversation codecs. Shared message types live in
//! [`respan_gateway_sdk`]; reusable wire helpers in [`respan_gateway_http::formats`].
//!
//! Unchanged same-format values retain their original bytes. Edited SSE values
//! retain their envelope fields, and edited usage increments are accumulated
//! back into the provider's cumulative wire counters. Unsupported nonzero usage
//! units and orphan tool continuations fail explicitly instead of disappearing.
//!
//! Request content edits preserve matching parts' vendor metadata, including
//! cache markers. Ambiguous matching is bounded to 262,144 matrix cells; larger
//! unchanged sequences and unambiguous single-part replacements use linear
//! paths. Other oversized edits fail rather than allocate an unbounded matrix
//! or guess which duplicate part owns the metadata.

mod anthropic;
mod common;
mod openai;
mod tools;

pub use anthropic::AnthropicMessages;
pub use openai::OpenAiChat;

use respan_gateway_http::formats;
use respan_gateway_sdk::{Usage, conversation};
