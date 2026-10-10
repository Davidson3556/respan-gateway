#![warn(missing_docs)]
//! The vendors, one module each: where a vendor is, how it takes a key, the
//! models it serves, and the formats it speaks, each a value set with the
//! vendor's quirks. [`ALL`] lists them.
//!
//! Formats name no vendor: a format offers knobs, such as
//! [`ChatQuirks`](respan_gateway_conversation_http::openai::ChatQuirks), and
//! a vendor's module sets them, describing each as a [`Quirk`].

use respan_gateway_conversation_http::anthropic::AnthropicMessages;
use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_media_http::gemini::GeminiVeo;
use respan_gateway_media_http::openai::{
    OpenAiEmbeddings, OpenAiImages, OpenAiSpeech, OpenAiTranscriptions, OpenAiVideos,
};
use respan_gateway_realtime_ws::JsonEvents;

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

/// Every vendor. A format is reached as the first that speaks it says,
/// for a provider of no vendor too.
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
#[derive(Debug)]
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
    /// Its quirks, one per knob its values set ([`Provider::knobs`]).
    pub quirks: &'static [Quirk],
}

impl Provider {
    /// A model's id as its list names it, without
    /// [`model_id_prefix`](Self::model_id_prefix).
    pub fn model_id<'a>(&self, listed: &'a str) -> &'a str {
        let prefix = self.model_id_prefix.unwrap_or_default();
        listed.strip_prefix(prefix).unwrap_or(listed)
    }

    /// The names of the knobs its values set, which its quirks describe.
    pub fn knobs(&self) -> Vec<&'static str> {
        let mut knobs: Vec<&'static str> =
            self.formats.iter().flat_map(|s| s.format.knobs()).collect();
        knobs.extend(self.model_id_prefix.map(|_| "model_id_prefix"));
        knobs.sort_unstable();
        knobs.dedup();
        knobs
    }
}

/// A way a vendor departs from the formats it speaks, which a knob handles.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Quirk {
    /// The knob's name, such as `usage_under`.
    pub name: &'static str,
    /// What the vendor does.
    pub description: &'static str,
}

/// A format a vendor speaks, as it speaks it.
#[derive(Debug, Clone)]
pub struct Speaks {
    /// The format, set with the vendor's quirks.
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
    /// Chat Completions, in a dialect.
    OpenAiChat(OpenAiChat),
    /// Anthropic Messages.
    AnthropicMessages(AnthropicMessages),
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
    /// A WebSocket session of JSON events.
    JsonEvents(JsonEvents),
}

impl Format {
    /// Its name, as a config names it.
    pub fn name(&self) -> &str {
        match self {
            Format::OpenAiChat(f) => f.name(),
            Format::AnthropicMessages(f) => f.name(),
            Format::OpenAiImages(_) => OpenAiImages::NAME,
            Format::OpenAiTranscriptions(_) => OpenAiTranscriptions::NAME,
            Format::OpenAiSpeech(_) => OpenAiSpeech::NAME,
            Format::OpenAiEmbeddings(_) => OpenAiEmbeddings::NAME,
            Format::OpenAiVideos(_) => OpenAiVideos::NAME,
            Format::GeminiVeo(_) => GeminiVeo::NAME,
            Format::JsonEvents(f) => f.name(),
        }
    }

    /// The names of the knobs its value sets.
    pub fn knobs(&self) -> Vec<&'static str> {
        match self {
            Format::OpenAiChat(f) => f.quirks().names(),
            Format::JsonEvents(f) => {
                let knobs = [
                    ("protocol", f.subprotocol().is_some()),
                    ("binary_text", f.binary_text()),
                ];
                knobs
                    .into_iter()
                    .filter(|(_, on)| *on)
                    .map(|(k, _)| k)
                    .collect()
            }
            _ => Vec::new(),
        }
    }
}
