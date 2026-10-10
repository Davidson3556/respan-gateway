#![warn(missing_docs)]
//! The media kinds: each operation is a kind of its own, an `Input` in and
//! an `Output` out, in a module of its name.
//!
//! - [`image`]: images generated from a prompt, or edits of source images.
//! - [`transcription`]: speech to text, or a translation of it to English.
//! - [`speech`]: text to speech.
//! - [`video`]: video generation, a job created, polled and downloaded.
//!
//! Media are the conversation kind's [`Media`] and usage its [`Usage`]; a
//! kind has a type of its own only where neither says it, such as a
//! [`transcription::Transcript`]. What a kind has no form for stays as the
//! format that decoded it has it ([`NativeFields`], [`NativeValue`]), and
//! only that format writes it. Values are plain data: they say what they
//! hold and compare by it, and keep nothing of the bytes they came as.
//!
//! The formats, OpenAI's endpoints as OpenAI and compatible vendors serve
//! them, are in [`openai`], behind the default `openai` feature, and Google's
//! Veo in [`gemini`], behind the default `gemini` feature. Without them the
//! crate is the kinds' types alone, with no I/O of its own.
//!
//! [`NativeFields`]: respan_gateway_core::NativeFields
//! [`NativeValue`]: respan_gateway_core::NativeValue

pub mod image;
pub mod speech;
pub mod transcription;
pub mod video;

#[cfg(feature = "gemini")]
pub mod gemini;
#[cfg(feature = "openai")]
pub mod openai;

pub use respan_gateway_conversation::{Media, MediaSource, Usage};
