//! Every format this binary speaks, and what the rest of the binary needs
//! to know of each: its kind, its endpoints, and how routing reads a
//! request's model. How a provider of it is reached is its vendor's
//! ([`respan_gateway_vendors::VendorProvider`]).

use respan_gateway_conversation as conversation;
use respan_gateway_core::{Fault, Format, Gateway};
use respan_gateway_http::{ClientIo, ProviderIo, raw::Original};
use respan_gateway_media::{embedding, image, speech, transcription, video};
use respan_gateway_passthrough_http::Passthrough;
use respan_gateway_plugin_routing::{Kind, Model};
use respan_gateway_realtime as realtime;
use respan_gateway_realtime_ws::{
    GeminiLive, OpenAiRealtime, ResponsesWs, SessionModel, requested_model,
};
use respan_gateway_vendors::{ALL, Base, Format as Spoken, Speaks};

/// The kinds of traffic this binary carries, as the admin API and routing
/// name them.
pub mod kind {
    use respan_gateway_plugin_routing::Kind;

    pub const CHAT: Kind = Kind {
        label: "chat",
        translates: true,
    };
    pub const REALTIME: Kind = Kind {
        label: "realtime",
        translates: true,
    };
    /// Responses over WebSocket: a session of many requests and replies,
    /// which no other format speaks.
    pub const RESPONSES: Kind = Kind {
        label: "responses",
        translates: false,
    };
    pub const PASSTHROUGH: Kind = Kind {
        label: "passthrough",
        translates: false,
    };
    pub const IMAGE: Kind = Kind {
        label: "image",
        translates: true,
    };
    pub const TRANSCRIPTION: Kind = Kind {
        label: "transcription",
        translates: true,
    };
    pub const SPEECH: Kind = Kind {
        label: "speech",
        translates: true,
    };
    pub const VIDEO: Kind = Kind {
        label: "video",
        translates: true,
    };
    pub const EMBEDDING: Kind = Kind {
        label: "embedding",
        translates: true,
    };
    /// MCP: tools and resources over streamable HTTP, served by the `mcp`
    /// section, which no other format translates.
    pub const MCP: Kind = Kind {
        label: "mcp",
        translates: false,
    };
    /// The kinds of the media crate, whose client formats are served by
    /// default once a provider speaks the kind.
    pub const MEDIA: [Kind; 5] = [IMAGE, TRANSCRIPTION, SPEECH, VIDEO, EMBEDDING];
}

/// The first of `items` of each kind, in order.
pub fn first_of_each_kind<T>(items: impl IntoIterator<Item = (Kind, T)>) -> Vec<(Kind, T)> {
    let mut firsts: Vec<(Kind, T)> = Vec::new();
    for (kind, item) in items {
        if !firsts.iter().any(|(k, _)| *k == kind) {
            firsts.push((kind, item));
        }
    }
    firsts
}

type Serve = dyn Fn(Gateway) -> Result<Gateway, Fault> + Send + Sync;

/// A format compiled into the binary.
pub struct KnownFormat {
    pub name: String,
    pub kind: Kind,
    /// What its clients send, as `METHOD /path`.
    pub endpoints: Vec<String>,
    pub websocket: bool,
    /// What its video jobs' ids start with, which routes a follow-up.
    pub job_ids: Option<&'static str>,
    /// None for a format only providers speak.
    serve: Option<Box<Serve>>,
    /// How its requests name their model.
    pub model: Model,
}

impl std::fmt::Debug for KnownFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KnownFormat")
            .field("name", &self.name)
            .field("kind", &self.kind.label)
            .finish_non_exhaustive()
    }
}

impl KnownFormat {
    /// `format` of `kind`, its clients sending to `endpoints`.
    pub fn of<F, I, O>(kind: Kind, endpoints: &[&str], format: F) -> Self
    where
        F: Clone + Send + Sync + 'static + Format<ClientIo, I, O> + Format<ProviderIo, O, I>,
        I: Clone + Send + Sync + 'static,
        O: Clone + Send + Sync + 'static,
    {
        let client = format.clone();
        let mut known = Self::provider_only(kind, &format);
        known.endpoints = endpoints.iter().map(|e| (*e).to_owned()).collect();
        known.serve = Some(Box::new(move |gateway| {
            gateway.client::<I, O, _>(client.clone())
        }));
        known
    }

    /// `format` of `kind`, which only providers speak.
    pub fn provider_only<F, I, O>(kind: Kind, format: &F) -> Self
    where
        F: Format<ProviderIo, O, I>,
    {
        Self {
            name: format.name().to_owned(),
            kind,
            endpoints: Vec::new(),
            websocket: false,
            job_ids: None,
            serve: None,
            model: Model::none(),
        }
    }

    /// Registers what it serves with `serve` in place of its client format.
    pub fn serving(
        mut self,
        serve: impl Fn(Gateway) -> Result<Gateway, Fault> + Send + Sync + 'static,
    ) -> Self {
        self.serve = Some(Box::new(serve));
        self
    }

    fn websocket(mut self) -> Self {
        self.websocket = true;
        self
    }

    pub fn model(mut self, model: Model) -> Self {
        self.model = model;
        self
    }

    fn job_ids(mut self, prefix: &'static str) -> Self {
        self.job_ids = Some(prefix);
        self
    }

    /// Whether clients may speak it.
    pub fn servable(&self) -> bool {
        self.serve.is_some()
    }

    pub fn serve(&self, gateway: Gateway) -> Result<Gateway, Fault> {
        match &self.serve {
            Some(serve) => serve(gateway),
            None => Err(Fault::config(format!(
                "formats: {} is spoken only by providers; no client sends it",
                self.name
            ))),
        }
    }
}

/// What clients of each format send, as `METHOD /path`.
mod endpoints {
    pub const CHAT_COMPLETIONS: &[&str] = &["POST /v1/chat/completions"];
    pub const RESPONSES: &[&str] = &["POST /v1/responses"];
    pub const MESSAGES: &[&str] = &["POST /v1/messages"];
    pub const GENERATE_CONTENT: &[&str] = &[
        "POST /v1beta/models/{model}:generateContent",
        "POST /v1beta/models/{model}:streamGenerateContent",
    ];
    pub const IMAGES: &[&str] = &["POST /v1/images/generations", "POST /v1/images/edits"];
    pub const TRANSCRIPTIONS: &[&str] = &[
        "POST /v1/audio/transcriptions",
        "POST /v1/audio/translations",
    ];
    pub const SPEECH: &[&str] = &["POST /v1/audio/speech"];
    pub const EMBEDDINGS: &[&str] = &["POST /v1/embeddings"];
    pub const VIDEOS: &[&str] = &[
        "POST /v1/videos",
        "GET /v1/videos",
        "GET /v1/videos/{id}",
        "DELETE /v1/videos/{id}",
        "GET /v1/videos/{id}/content",
        "POST /v1/videos/{id}/remix",
    ];
    pub const PASSTHROUGH: &[&str] = &["ANY /* (every other request)"];
}

/// Every format this binary speaks: each format a vendor speaks
/// ([`ALL`]), and passthrough.
pub fn formats() -> Vec<KnownFormat> {
    let mut known: Vec<KnownFormat> = Vec::new();
    for speaks in ALL.iter().flat_map(|p| p.formats) {
        if !known.iter().any(|k| k.name == speaks.format.name()) {
            known.push(known_format(speaks));
        }
    }
    known.push(KnownFormat::of(
        kind::PASSTHROUGH,
        endpoints::PASSTHROUGH,
        Passthrough,
    ));
    known
}

/// The format `speaks`: its kind, endpoints and how its model is named.
fn known_format(speaks: &Speaks) -> KnownFormat {
    match speaks.format.clone() {
        Spoken::OpenAiChat(f) => {
            KnownFormat::of(kind::CHAT, endpoints::CHAT_COMPLETIONS, f).model(chat())
        }
        Spoken::OpenAiResponses(f) => {
            KnownFormat::of(kind::CHAT, endpoints::RESPONSES, f).model(chat())
        }
        Spoken::AnthropicMessages(f) => {
            KnownFormat::of(kind::CHAT, endpoints::MESSAGES, f).model(chat())
        }
        Spoken::GeminiGenerate(f) => {
            KnownFormat::of(kind::CHAT, endpoints::GENERATE_CONTENT, f).model(chat())
        }
        Spoken::OpenAiImages(f) => KnownFormat::of(kind::IMAGE, endpoints::IMAGES, f)
            .model(Model::of(image::Input::model, image::Input::set_model)),
        Spoken::OpenAiTranscriptions(f) => {
            KnownFormat::of(kind::TRANSCRIPTION, endpoints::TRANSCRIPTIONS, f).model(Model::of(
                transcription::Input::model,
                transcription::Input::set_model,
            ))
        }
        Spoken::OpenAiSpeech(f) => KnownFormat::of(kind::SPEECH, endpoints::SPEECH, f)
            .model(Model::of(speech::Input::model, speech::Input::set_model)),
        Spoken::OpenAiEmbeddings(f) => KnownFormat::of(kind::EMBEDDING, endpoints::EMBEDDINGS, f)
            .model(Model::of(
                embedding::Input::model,
                embedding::Input::set_model,
            )),
        // Only a `Create` names a model; a follow-up goes by its job's id
        // ([`crate::jobs`]).
        Spoken::OpenAiVideos(f) => KnownFormat::of(kind::VIDEO, endpoints::VIDEOS, f)
            .job_ids("video_")
            .model(Model::of(video::Input::model, video::Input::set_model)),
        Spoken::GeminiVeo(f) => KnownFormat::provider_only(kind::VIDEO, &f).job_ids("veo_"),
        Spoken::OpenAiRealtime(f) => {
            session(kind::REALTIME, f, OpenAiRealtime::PATH, speaks.base).model(realtime())
        }
        Spoken::GeminiLive(f) => {
            session(kind::REALTIME, f, GeminiLive::PATH, speaks.base).model(realtime())
        }
        Spoken::ResponsesWs(f) => session(kind::RESPONSES, f, ResponsesWs::PATH, speaks.base),
        Spoken::Passthrough(f) => KnownFormat::of(kind::PASSTHROUGH, endpoints::PASSTHROUGH, f),
    }
}

/// How the model of a chat request is named.
fn chat() -> Model {
    Model::of(conversation::Input::model, conversation::Input::set_model)
}

/// How a realtime session names its model: at `?model=` on its handshake,
/// as its first input comes only once it is routed (a Gemini Live client
/// without it goes to the default). The model routing chose is dialed
/// ([`SessionModel`]) and asked for by the session's opening input.
fn realtime() -> Model {
    Model::new(
        |cx| requested_model(cx.extensions().get::<Original>()?),
        |cx, model| {
            cx.extensions_mut().insert(SessionModel(model.clone()));
            cx.filter(realtime::set_model(model))
        },
    )
}

/// A session `format` of `kind`, its clients sending to its `path`: after
/// `/v1`, as an SDK adds it to the gateway's base URL, unless it sits at the
/// origin.
fn session<F>(kind: Kind, format: F, path: &str, base: Base) -> KnownFormat
where
    F: Clone
        + Send
        + Sync
        + 'static
        + Format<ClientIo, realtime::Input, realtime::Output>
        + Format<ProviderIo, realtime::Output, realtime::Input>,
{
    let endpoint = match base {
        Base::Origin => format!("GET {path} (WebSocket)"),
        _ => format!("GET /v1{path} (WebSocket)"),
    };
    KnownFormat::of(kind, &[&endpoint], format).websocket()
}
