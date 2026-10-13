#![warn(missing_docs)]
//! A vendor as a template: where it is, how it takes a key, the formats it
//! speaks, and its quirks ([`Vendor`]); and a provider of it, at a base URL
//! with a key, which registers a core provider of each of its formats and
//! lists its models ([`VendorProvider`]). The vendors themselves are
//! `respan-gateway-vendors`'.
//!
//! A format is a spec and names no vendor. Where a vendor's deployment
//! departs from a spec, its vendor says so: as data, such as the header a
//! key goes in ([`Speaks`]) or what its model list puts before an id, or as
//! a [`Plugin`] that turns what the vendor sends beyond the spec into the
//! kind's events and back. Each is described as a [`Quirk`].

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
use respan_gateway_passthrough_http::Passthrough;
use respan_gateway_realtime_ws::{GeminiLive, OpenAiRealtime, ResponsesWs};

mod provider;

pub use provider::{Part, VendorProvider};

/// A vendor: a template of the providers of it ([`Vendor::provider`]).
pub struct Vendor {
    /// Its id, as the gateway's config names it: `vendor = "<id>"`.
    pub id: &'static str,
    /// The base URL its SDKs take, if it has one of its own.
    pub base_url: Option<&'static str>,
    /// The environment variables its key is read from, the first set; none
    /// for a vendor that needs no key.
    pub key_env: &'static [&'static str],
    /// A model to show in examples.
    pub example_model: &'static str,
    /// The formats it speaks, as it speaks them.
    pub formats: &'static [Speaks],
    /// What its model list puts before each model's id.
    pub model_id_prefix: Option<&'static str>,
    /// How it departs from the specs it speaks.
    pub quirks: &'static [Quirk],
    /// The plugin of its quirks, a plugin of each of its providers
    /// ([`Provider::plugin`](respan_gateway_core::Provider::plugin)): run on
    /// their calls alone, after the gateway's plugins, so those see the
    /// kind's events. Not optional: a provider of this vendor needs it.
    pub plugin: Option<&'static dyn Plugin>,
}

impl std::fmt::Debug for Vendor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Vendor")
            .field("id", &self.id)
            .field("formats", &self.formats)
            .field("quirks", &self.quirks)
            .finish_non_exhaustive()
    }
}

impl Vendor {
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

    /// Its base URL, for a provider at `base_url`: a WebSocket format's
    /// with `ws`/`wss`.
    pub fn base_url(&self, base_url: &str) -> String {
        let base_url = base_url.trim_end_matches('/');
        let base = match self.base {
            Base::Same => base_url,
            Base::Origin => origin(base_url),
            Base::Without(suffix) => base_url.strip_suffix(suffix).unwrap_or(base_url),
        };
        let swap = match self.format.websocket() {
            true => [("https://", "wss://"), ("http://", "ws://")],
            false => [("wss://", "https://"), ("ws://", "http://")],
        };
        swap.iter()
            .find_map(|(from, to)| base.strip_prefix(from).map(|rest| format!("{to}{rest}")))
            .unwrap_or_else(|| base.to_owned())
    }
}

/// `scheme://host[:port]` of `url`.
fn origin(url: &str) -> &str {
    let after_scheme = url.find("://").map_or(0, |i| i + 3);
    match url[after_scheme..].find('/') {
        Some(slash) => &url[..after_scheme + slash],
        None => url,
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
    /// Any request, relayed as it is.
    Passthrough(Passthrough),
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
            Format::Passthrough(_) => "passthrough",
        }
    }

    /// Whether it is a WebSocket session's.
    pub fn websocket(&self) -> bool {
        matches!(
            self,
            Format::OpenAiRealtime(_) | Format::GeminiLive(_) | Format::ResponsesWs(_)
        )
    }
}
