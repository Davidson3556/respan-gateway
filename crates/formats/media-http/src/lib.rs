#![warn(missing_docs)]
//! The media kinds over HTTP:
//!
//! - [`openai`]: OpenAI's media endpoints, as OpenAI and compatible vendors
//!   serve them, one format per kind.
//! - [`gemini`]: Google's Veo, as the Gemini API serves it, a provider of
//!   the video kind.

mod edge;
pub mod gemini;
pub mod openai;
