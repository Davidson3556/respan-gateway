#![warn(missing_docs)]
//! The vendors, one module each: where a vendor is, how it takes a key, the
//! models it serves, the formats it speaks, and its quirks. [`ALL`] lists
//! them.
//!
//! A format is a spec and names no vendor. Where a vendor's deployment
//! departs from a spec, its module says so: as data, such as the header a
//! key goes in ([`Speaks`]) or what its model list puts before an id, or as
//! a [`Plugin`] that turns what the vendor sends beyond the spec into the
//! kind's events and back ([`Provider::plugin`]). Each is described as a
//! [`Quirk`].

use respan_gateway_conversation_http::Schema;
use respan_gateway_conversation_http::anthropic::AnthropicMessages;
use respan_gateway_conversation_http::gemini::GeminiGenerate;
use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_conversation_http::responses::OpenAiResponses;
use respan_gateway_core::Plugin;
use respan_gateway_media_http::gemini::GeminiVeo;
use respan_gateway_media_http::openai::{
    OpenAiEmbeddings, OpenAiImages, OpenAiSpeech, OpenAiTranscriptions, OpenAiVideos,
};
use respan_gateway_realtime_ws::{GeminiLive, OpenAiRealtime, ResponsesWs};

mod chat;

pub mod anthropic;
pub mod azure;
pub mod deepseek;
pub mod fireworks;
pub mod gemini;
pub mod groq;
pub mod mistral;
pub mod ollama;
pub mod openai;
pub mod openrouter;
pub mod together;
pub mod vllm;
pub mod xai;

/// Every vendor. A format's own vendor, whose spec it is, comes before any
/// other that speaks it: a provider of no vendor takes its key and sits as
/// that vendor's does.
pub const ALL: &[&Provider] = &[
    &openai::PROVIDER,
    &anthropic::PROVIDER,
    &gemini::PROVIDER,
    &groq::PROVIDER,
    &deepseek::PROVIDER,
    &openrouter::PROVIDER,
    &xai::PROVIDER,
    &mistral::PROVIDER,
    &together::PROVIDER,
    &fireworks::PROVIDER,
    &ollama::PROVIDER,
    &azure::PROVIDER,
    &vllm::PROVIDER,
];

/// The vendor `id`.
pub fn find(id: &str) -> Option<&'static Provider> {
    ALL.iter().copied().find(|p| p.id == id)
}

/// A vendor.
pub struct Provider {
    /// Its id, as a config names it: `{ vendor: <id> }`.
    pub id: &'static str,
    /// The base URL its SDKs take, if it has one of its own.
    pub base_url: Option<&'static str>,
    /// The environment variables its key is read from, the first set; none
    /// for a vendor that needs no key.
    pub key_env: &'static [&'static str],
    /// Model names routed to it without a `provider/` prefix; of two that
    /// match, the longer wins.
    pub prefixes: &'static [&'static str],
    /// A model to show in examples.
    pub example_model: &'static str,
    /// The formats it speaks, as it speaks them.
    pub formats: &'static [Speaks],
    /// What its model list puts before each model's id.
    pub model_id_prefix: Option<&'static str>,
    /// How it departs from the specs it speaks, each tested in
    /// `tests/<id>.rs` by a test named for it.
    pub quirks: &'static [Quirk],
    /// The plugin of its quirks, run on its providers' calls alone, before
    /// any other plugin sees their events.
    pub plugin: Option<&'static dyn Plugin>,
}

impl std::fmt::Debug for Provider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Provider")
            .field("id", &self.id)
            .field("formats", &self.formats)
            .field("quirks", &self.quirks)
            .finish_non_exhaustive()
    }
}

impl Provider {
    /// A model's id as its list names it, without
    /// [`model_id_prefix`](Self::model_id_prefix).
    pub fn model_id<'a>(&self, listed: &'a str) -> &'a str {
        let prefix = self.model_id_prefix.unwrap_or_default();
        listed.strip_prefix(prefix).unwrap_or(listed)
    }
}

/// A way a vendor's deployment departs from a spec it speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Quirk {
    /// Its name, such as `x_groq_usage`.
    pub name: &'static str,
    /// What the vendor does.
    pub description: &'static str,
}

/// A format a vendor speaks, as it speaks it.
#[derive(Debug, Clone)]
pub struct Speaks {
    /// The format.
    pub format: Format,
    /// How it takes the key.
    pub auth: Auth,
    /// Where it sits relative to the vendor's base URL.
    pub base: Base,
}

impl Speaks {
    /// `format` at the base URL, its key a bearer token.
    pub const fn new(format: Format) -> Self {
        Self {
            format,
            auth: Auth::Bearer,
            base: Base::Same,
        }
    }

    /// Its key in the header `name`.
    pub const fn key_in(mut self, name: &'static str) -> Self {
        self.auth = Auth::Header(name);
        self
    }

    /// Where it sits.
    pub const fn at(mut self, base: Base) -> Self {
        self.base = base;
        self
    }
}

/// How a format takes a provider's key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Auth {
    /// `Authorization: Bearer <key>`.
    Bearer,
    /// The key alone, in this header.
    Header(&'static str),
}

/// Where a format sits relative to the base URL of a provider with several
/// formats. A WebSocket format takes `ws`/`wss` in place of `http`/`https`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Base {
    /// The base URL itself.
    Same,
    /// Its origin, `scheme://host[:port]`.
    Origin,
    /// The base URL without this suffix, if it ends in it.
    Without(&'static str),
}

/// A format, as its value.
#[derive(Debug, Clone)]
pub enum Format {
    /// OpenAI Chat Completions.
    OpenAiChat(OpenAiChat),
    /// OpenAI Responses.
    OpenAiResponses(OpenAiResponses),
    /// Anthropic Messages.
    AnthropicMessages(AnthropicMessages),
    /// Google Gemini's `generateContent`.
    GeminiGenerate(GeminiGenerate),
    /// OpenAI Images.
    OpenAiImages(OpenAiImages),
    /// OpenAI Audio Transcriptions.
    OpenAiTranscriptions(OpenAiTranscriptions),
    /// OpenAI Speech.
    OpenAiSpeech(OpenAiSpeech),
    /// OpenAI Embeddings.
    OpenAiEmbeddings(OpenAiEmbeddings),
    /// OpenAI Videos.
    OpenAiVideos(OpenAiVideos),
    /// Google's Veo.
    GeminiVeo(GeminiVeo),
    /// OpenAI Realtime.
    OpenAiRealtime(OpenAiRealtime),
    /// Gemini Live.
    GeminiLive(GeminiLive),
    /// OpenAI's Responses over WebSocket.
    ResponsesWs(ResponsesWs),
}

impl Format {
    /// Its name, as a config names it.
    pub fn name(&self) -> &str {
        match self {
            Format::OpenAiChat(_) => OpenAiChat::NAME,
            Format::OpenAiResponses(_) => OpenAiResponses::NAME,
            Format::AnthropicMessages(_) => AnthropicMessages::NAME,
            Format::GeminiGenerate(_) => GeminiGenerate::NAME,
            Format::OpenAiImages(_) => OpenAiImages::NAME,
            Format::OpenAiTranscriptions(_) => OpenAiTranscriptions::NAME,
            Format::OpenAiSpeech(_) => OpenAiSpeech::NAME,
            Format::OpenAiEmbeddings(_) => OpenAiEmbeddings::NAME,
            Format::OpenAiVideos(_) => OpenAiVideos::NAME,
            Format::GeminiVeo(_) => GeminiVeo::NAME,
            Format::OpenAiRealtime(_) => OpenAiRealtime::NAME,
            Format::GeminiLive(_) => GeminiLive::NAME,
            Format::ResponsesWs(_) => ResponsesWs::NAME,
        }
    }
}
