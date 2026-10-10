//! The image kind: images generated from a prompt, or edits of source
//! images. A request with source images is an edit.
//!
//! A response is a sequence of events, the same for a stream and a whole
//! body: a [`Output::Start`], then for each image any [`Output::Partial`]
//! previews (in a stream that asked for them) and its [`Output::Image`],
//! with [`Output::Usage`] and [`Output::Native`] among them.

use respan_gateway_conversation::{Media, Usage};
use respan_gateway_core::{FormatId, NativeFields, NativeValue};
use serde_json::Value;

/// The kind's input: a request for images.
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct Input {
    /// The model asked for, if any; without one, the provider's default.
    pub model: Option<String>,
    /// What to draw, or how to change the source images.
    pub prompt: String,
    /// The source images of an edit; none for a generation.
    pub images: Vec<Media>,
    /// Where an edit may change its first image: the transparent areas.
    pub mask: Option<Media>,
    /// How many images, if not one.
    pub count: Option<u32>,
    /// Their size, as the provider names it, such as `1024x1024`.
    pub size: Option<String>,
    /// Their quality, as the provider names it, such as `high`.
    pub quality: Option<String>,
    /// Their file format, such as `png`.
    pub output_format: Option<String>,
    /// The end user's id, as the client names them to the provider.
    pub user: Option<String>,
    /// The request's fields the kind has no place for, such as a background.
    pub native: Option<NativeFields>,
}

impl Input {
    /// A generation of `prompt` alone.
    pub fn new(prompt: impl Into<String>) -> Self {
        Self {
            prompt: prompt.into(),
            ..Self::default()
        }
    }

    /// Whether it is an edit: it has source images.
    pub fn is_edit(&self) -> bool {
        !self.images.is_empty()
    }
}

/// The kind's output: one event of a response, streamed or not.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Output {
    /// The response begins.
    Start(Start),
    /// A preview of the image being drawn, superseded by the next one and by
    /// the image itself. A whole body has none.
    Partial(Partial),
    /// An image, in the order the provider gave them.
    Image(Image),
    /// Usage the provider reports, as an increment.
    Usage(Usage),
    /// An event the kind has no form for, as `format` has it. Sent only in
    /// that format.
    Native(NativeValue),
}

impl Output {
    /// An event of `format` the kind has no form for.
    pub fn native(format: impl Into<FormatId>, raw: impl Into<Value>) -> Self {
        Output::Native(NativeValue::new(format, raw))
    }
}

/// Which response the events belong to.
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct Start {
    /// When the provider made it, in seconds since the Unix epoch.
    pub created: Option<u64>,
    /// The response's fields the kind has no place for, such as the size or
    /// background the provider chose.
    pub native: Option<NativeFields>,
}

impl Start {
    /// The start of a response made at `created`.
    pub fn new(created: Option<u64>) -> Self {
        Self {
            created,
            native: None,
        }
    }
}

/// A preview of an image being drawn.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Partial {
    /// Which preview it is, from 0.
    pub index: u32,
    /// The preview.
    pub media: Media,
    /// Its fields the kind has no place for.
    pub native: Option<NativeFields>,
}

impl Partial {
    /// The preview `index`.
    pub fn new(index: u32, media: Media) -> Self {
        Self {
            index,
            media,
            native: None,
        }
    }
}

/// An image a provider made.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Image {
    /// The image: its bytes, or a URL the provider keeps it at.
    pub media: Media,
    /// The prompt the provider drew from, if it rewrote the one asked.
    pub revised_prompt: Option<String>,
    /// Its fields the kind has no place for.
    pub native: Option<NativeFields>,
}

impl Image {
    /// The image `media`.
    pub fn new(media: Media) -> Self {
        Self {
            media,
            revised_prompt: None,
            native: None,
        }
    }
}
