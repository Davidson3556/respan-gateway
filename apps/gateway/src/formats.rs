//! Every format this binary speaks, and what the rest of the binary needs
//! to know of each: its kind, its endpoints, how a provider of it
//! authenticates and is reached, and how routing reads a request's model.

use std::sync::Arc;

use respan_gateway_conversation::anthropic::AnthropicMessages;
use respan_gateway_conversation::openai::OpenAiChat;
use respan_gateway_core::{ClientContext, Extensions, Fault, Format, Gateway, Upstream};
use respan_gateway_http::formats::opaque::{self, Passthrough};
use respan_gateway_http::http::HeaderMap;
use respan_gateway_http::{ClientIo, HttpTransport, ProviderIo, reqwest};
use respan_gateway_media::gemini::GeminiVeo;
use respan_gateway_media::openai::{
    OpenAiImages, OpenAiSpeech, OpenAiTranscriptions, OpenAiVideos,
};
use respan_gateway_media::{image, speech, transcription, video};
use respan_gateway_realtime::JsonEvents;

use crate::presets::origin;

/// What sort of traffic a format carries, as the admin API and routing
/// name it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Kind {
    pub label: &'static str,
    /// Whether a client of one format of the kind may be answered by a
    /// provider of another; otherwise only a provider of its own format.
    pub translates: bool,
}

impl Kind {
    pub const CHAT: Kind = Kind {
        label: "chat",
        translates: true,
    };
    pub const REALTIME: Kind = Kind {
        label: "realtime",
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
    /// The media kinds, whose client formats are served by default once a
    /// provider speaks the kind.
    pub const MEDIA: [Kind; 4] = [Kind::IMAGE, Kind::TRANSCRIPTION, Kind::SPEECH, Kind::VIDEO];
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

/// How a provider of a format takes the key configured for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyHeader {
    Bearer,
    Header(&'static str),
}

/// Where a format sits relative to the base URL of a provider with several
/// formats, or a vendor preset: the base itself, or its origin. A
/// WebSocket format takes `ws`/`wss` in place of `http`/`https`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Base {
    Same,
    Origin,
    /// The base without this suffix, if it ends in it: Gemini's own API is
    /// its OpenAI-compatible base without `/openai`.
    Without(&'static str),
}

/// A request's input, as routing reads and rewrites its model.
pub trait Routed: Clone + Send + Sync + 'static {
    fn model(&self) -> Option<&str> {
        None
    }
    fn set_model(&mut self, _model: String) {}
}

impl Routed for respan_gateway_conversation::Input {
    fn model(&self) -> Option<&str> {
        self.model.as_deref()
    }
    fn set_model(&mut self, model: String) {
        self.model = Some(model);
    }
}

macro_rules! routed_by_model {
    ($($input:ty),*) => {$(
        impl Routed for $input {
            fn model(&self) -> Option<&str> {
                self.model.as_deref()
            }
            fn set_model(&mut self, model: String) {
                self.model = Some(model);
            }
        }
    )*};
}

routed_by_model!(image::Input, transcription::Input, speech::Input);

/// Only a `Create` names a model; a follow-up goes by its job's id
/// ([`crate::jobs`]).
impl Routed for video::Input {
    fn model(&self) -> Option<&str> {
        video::Input::model(self)
    }
    fn set_model(&mut self, model: String) {
        if let video::Input::Create(create) = self {
            create.model = Some(model);
        }
    }
}

impl Routed for respan_gateway_realtime::Input {}

impl<F: 'static> Routed for opaque::Input<F> {}

type AddProvider = dyn Fn(Gateway, String, &str, HeaderMap) -> Result<Gateway, Fault> + Send + Sync;
type Serve = dyn Fn(Gateway) -> Result<Gateway, Fault> + Send + Sync;
pub type ReadModel = dyn Fn(&ClientContext) -> Option<String> + Send + Sync;
pub type SetModel = dyn Fn(&mut ClientContext, String) -> Result<(), Fault> + Send + Sync;

/// A format compiled into the binary.
pub struct KnownFormat {
    pub name: String,
    pub kind: Kind,
    /// What its clients send, as `METHOD /path`.
    pub endpoints: &'static [&'static str],
    pub key: KeyHeader,
    pub base: Base,
    pub websocket: bool,
    /// What its video jobs' ids start with, which routes a follow-up.
    pub job_ids: Option<&'static str>,
    provider: Box<AddProvider>,
    /// None for a format only providers speak.
    serve: Option<Box<Serve>>,
    model: Arc<ReadModel>,
    set_model: Arc<SetModel>,
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
    /// `format` of `kind`, its clients sending to `endpoints`, its
    /// providers reached by `reach`, given one's name, base URL and auth
    /// headers.
    pub fn of<F, I, O, T>(
        kind: Kind,
        endpoints: &'static [&'static str],
        format: F,
        reach: impl Fn(&str, &str, HeaderMap) -> Result<T, Fault> + Send + Sync + 'static,
    ) -> Self
    where
        F: Clone + Send + Sync + 'static + Format<ClientIo, I, O> + Format<ProviderIo, O, I>,
        I: Routed,
        O: Clone + Send + Sync + 'static,
        T: Fn(Extensions) -> ProviderIo + Send + Sync + 'static,
    {
        let client = format.clone();
        let mut known = Self::provider_only(kind, format, reach);
        known.endpoints = endpoints;
        known.serve = Some(Box::new(move |gateway| {
            gateway.client::<I, O, _>(client.clone())
        }));
        known
    }

    /// `format` of `kind`, which only providers speak.
    pub fn provider_only<F, I, O, T>(
        kind: Kind,
        format: F,
        reach: impl Fn(&str, &str, HeaderMap) -> Result<T, Fault> + Send + Sync + 'static,
    ) -> Self
    where
        F: Clone + Send + Sync + 'static + Format<ProviderIo, O, I>,
        I: Routed,
        O: Clone + Send + Sync + 'static,
        T: Fn(Extensions) -> ProviderIo + Send + Sync + 'static,
    {
        Self {
            name: <F as Format<ProviderIo, O, I>>::name(&format).to_owned(),
            kind,
            endpoints: &[],
            key: KeyHeader::Bearer,
            base: Base::Same,
            websocket: false,
            job_ids: None,
            provider: Box::new(move |gateway, name, base_url, auth| {
                // A core provider is named `provider` or `provider:format`.
                let provider = name.split(':').next().unwrap_or_default();
                let reached = reach(provider, base_url, auth)?;
                gateway.provider::<I, O, _>(name, format.clone(), reached)
            }),
            serve: None,
            model: Arc::new(|cx| cx.input::<I>().ok()?.model().map(str::to_owned)),
            set_model: Arc::new(|cx, model| {
                cx.filter(move |input: &mut I| {
                    input.set_model(model.clone());
                    Ok(())
                })
            }),
        }
    }

    fn key(mut self, key: KeyHeader) -> Self {
        self.key = key;
        self
    }

    fn base(mut self, base: Base) -> Self {
        self.base = base;
        self
    }

    fn websocket(mut self) -> Self {
        self.websocket = true;
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

    pub fn add_provider(
        &self,
        gateway: Gateway,
        name: String,
        base_url: &str,
        auth: HeaderMap,
    ) -> Result<Gateway, Fault> {
        (self.provider)(gateway, name, base_url, auth)
    }

    pub fn serve(&self, gateway: Gateway) -> Result<Gateway, Fault> {
        match &self.serve {
            Some(serve) => serve(gateway),
            None => Err(Fault::config(format!(
                "serve: {} is spoken only by providers; no client sends it",
                self.name
            ))),
        }
    }

    pub fn model_reader(&self) -> Arc<ReadModel> {
        self.model.clone()
    }

    pub fn model_writer(&self) -> Arc<SetModel> {
        self.set_model.clone()
    }
}

const CHAT_COMPLETIONS: &[&str] = &["POST /v1/chat/completions"];
const VIDEOS: &[&str] = &[
    "POST /v1/videos",
    "GET /v1/videos",
    "GET /v1/videos/{id}",
    "DELETE /v1/videos/{id}",
    "GET /v1/videos/{id}/content",
    "POST /v1/videos/{id}/remix",
];

/// Every format this binary speaks: OpenAI Chat as OpenAI and each vendor
/// with a dialect serve it, Anthropic Messages and passthrough, OpenAI's
/// media endpoints and Google's Veo, their providers reached over HTTP,
/// reading up to `limit` bytes of an answer at once; and the sessions of
/// OpenAI Realtime, Responses over WebSocket and Gemini Live, their
/// providers reached over WebSocket. A new format is one line here.
pub fn formats(limit: usize) -> Vec<KnownFormat> {
    let http = move |provider: &str, base_url: &str, auth| -> Result<_, Fault> {
        let client = HttpTransport::new(base_url)?.with_headers(auth)?;
        Ok(explained(client, provider, base_url, limit))
    };
    let google = move |provider: &str, base_url: &str, auth| -> Result<_, Fault> {
        let client = HttpTransport::new(base_url)?.with_client(google_storage()?);
        Ok(explained(
            client.with_headers(auth)?,
            provider,
            base_url,
            limit,
        ))
    };
    let ws = |format: JsonEvents, endpoints| {
        KnownFormat::of(
            Kind::REALTIME,
            endpoints,
            format.clone(),
            move |_: &str, base_url: &str, auth| {
                Ok(format.transport(base_url)?.with_headers(auth)?.connect())
            },
        )
        .websocket()
    };
    let chat = |format: OpenAiChat| KnownFormat::of(Kind::CHAT, CHAT_COMPLETIONS, format, http);
    let goog_key = KeyHeader::Header("x-goog-api-key");
    vec![
        chat(OpenAiChat::OPENAI),
        chat(OpenAiChat::GROQ),
        chat(OpenAiChat::DEEPSEEK),
        chat(OpenAiChat::VLLM),
        chat(OpenAiChat::OPENROUTER),
        chat(OpenAiChat::AZURE),
        KnownFormat::of(Kind::CHAT, &["POST /v1/messages"], AnthropicMessages, http)
            .key(KeyHeader::Header("x-api-key")),
        KnownFormat::of(
            Kind::IMAGE,
            &["POST /v1/images/generations", "POST /v1/images/edits"],
            OpenAiImages,
            http,
        ),
        KnownFormat::of(
            Kind::TRANSCRIPTION,
            &["POST /v1/audio/transcriptions", "POST /v1/audio/translations"],
            OpenAiTranscriptions,
            http,
        ),
        KnownFormat::of(Kind::SPEECH, &["POST /v1/audio/speech"], OpenAiSpeech, http),
        KnownFormat::of(Kind::VIDEO, VIDEOS, OpenAiVideos, http).job_ids("video_"),
        KnownFormat::provider_only(Kind::VIDEO, GeminiVeo, google)
            .job_ids("veo_")
            .key(goog_key)
            .base(Base::Without("/openai")),
        KnownFormat::of(Kind::PASSTHROUGH, &["ANY /* (every other request)"], Passthrough, http)
            .base(Base::Origin),
        ws(JsonEvents::OPENAI_REALTIME, &["GET /v1/realtime (WebSocket)"]),
        ws(JsonEvents::RESPONSES_WS, &["GET /v1/responses (WebSocket)"]),
        ws(
            JsonEvents::GEMINI_LIVE,
            &["GET /ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent (WebSocket)"],
        )
        .key(goog_key)
        .base(Base::Origin),
    ]
}

/// `client`'s calls, a provider that cannot be reached named in the fault
/// with its host and the cause: the transport's own message names neither.
fn explained(
    client: HttpTransport,
    provider: &str,
    base_url: &str,
    limit: usize,
) -> impl Fn(Extensions) -> ProviderIo + Send + Sync + use<> {
    let client = Arc::new(client);
    let host = origin(base_url)
        .split_once("://")
        .map_or("", |(_, host)| host);
    let who: Arc<str> = format!("provider '{provider}' at {host}").into();
    respan_gateway_http::connect(
        move |call| {
            let (client, who) = (client.clone(), who.clone());
            async move {
                client.send(call).await.map_err(|fault| {
                    let Some(e) = fault.cause::<reqwest::Error>() else {
                        return fault;
                    };
                    let message = format!("{who} {}", unreachable(e));
                    Upstream::fault(message).with_source(fault)
                })
            }
        },
        limit,
    )
}

/// Why a provider could not be reached: the transport's own cause, which,
/// unlike the error itself, names no URL.
pub fn unreachable(e: &reqwest::Error) -> String {
    let mut cause = None;
    let mut next = std::error::Error::source(e);
    while let Some(e) = next {
        cause = Some(e.to_string());
        next = e.source();
    }
    let cause = match (cause, e.is_timeout()) {
        (Some(cause), _) => cause,
        (None, true) => "timed out".to_owned(),
        (None, false) => "the connection failed".to_owned(),
    };
    format!("could not be reached: {cause}")
}

/// A client for the Gemini API, which serves a video's bytes by redirecting
/// to Google's storage: it follows redirects to Google's hosts over HTTPS
/// alone, as each is sent the key too.
fn google_storage() -> Result<reqwest::Client, Fault> {
    let policy = reqwest::redirect::Policy::custom(|attempt| {
        let host = attempt.url().host_str().unwrap_or_default();
        let google = [".googleapis.com", ".googleusercontent.com"];
        let trusted =
            attempt.url().scheme() == "https" && google.iter().any(|domain| host.ends_with(domain));
        match trusted && attempt.previous().len() < 5 {
            true => attempt.follow(),
            false => attempt.stop(),
        }
    });
    reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(10))
        .redirect(policy)
        .build()
        .map_err(|e| Fault::internal("the HTTP client could not be built").with_source(e))
}
