//! Every format this binary speaks, and what the rest of the binary needs
//! to know of each: its kind, its endpoints, how a provider of it
//! authenticates and is reached, and how routing reads a request's model.

use std::sync::Arc;

use respan_gateway_conversation as conversation;
use respan_gateway_core::{Extensions, Fault, Format, Gateway, Upstream};
use respan_gateway_http::http::{HeaderMap, Uri};
use respan_gateway_http::passthrough::Passthrough;
use respan_gateway_http::{ClientIo, HttpTransport, ProviderIo, Wire, reqwest};
use respan_gateway_media::{embedding, image, speech, transcription, video};
use respan_gateway_plugins::{Kind, Model};
use respan_gateway_providers::{ALL, Auth, Base, Format as Spoken};
use respan_gateway_realtime as realtime;
use respan_gateway_realtime_ws::{
    GeminiLive, OpenAiRealtime, ResponsesWs, SessionModel, requested_model,
};
use respan_gateway_ws::WsTransport;

use crate::presets::origin;

/// The kinds of traffic this binary carries, as the admin API and routing
/// name them.
pub mod kind {
    use respan_gateway_plugins::Kind;

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

type AddProvider = dyn Fn(Gateway, String, &str, HeaderMap) -> Result<Gateway, Fault> + Send + Sync;
type Serve = dyn Fn(Gateway) -> Result<Gateway, Fault> + Send + Sync;

/// A format compiled into the binary.
pub struct KnownFormat {
    pub name: String,
    pub kind: Kind,
    /// What its clients send, as `METHOD /path`.
    pub endpoints: Vec<String>,
    /// How a provider of no vendor takes a key, and where it sits beside a
    /// base URL: as the format's own vendor, the first in [`ALL`] that
    /// speaks it, does.
    pub key: Auth,
    pub base: Base,
    pub websocket: bool,
    /// What its video jobs' ids start with, which routes a follow-up.
    pub job_ids: Option<&'static str>,
    provider: Box<AddProvider>,
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
    /// `format` of `kind`, its clients sending to `endpoints`, its
    /// providers reached by `reach`, given one's name, base URL and auth
    /// headers.
    pub fn of<F, I, O, T>(
        kind: Kind,
        endpoints: &[&str],
        format: F,
        reach: impl Fn(&str, &str, HeaderMap) -> Result<T, Fault> + Send + Sync + 'static,
    ) -> Self
    where
        F: Clone + Send + Sync + 'static + Format<ClientIo, I, O> + Format<ProviderIo, O, I>,
        I: Clone + Send + Sync + 'static,
        O: Clone + Send + Sync + 'static,
        T: Fn(Extensions) -> ProviderIo + Send + Sync + 'static,
    {
        let client = format.clone();
        let mut known = Self::provider_only(kind, format, reach);
        known.endpoints = endpoints.iter().map(|e| (*e).to_owned()).collect();
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
        I: Clone + Send + Sync + 'static,
        O: Clone + Send + Sync + 'static,
        T: Fn(Extensions) -> ProviderIo + Send + Sync + 'static,
    {
        Self {
            name: <F as Format<ProviderIo, O, I>>::name(&format).to_owned(),
            kind,
            endpoints: Vec::new(),
            key: Auth::Bearer,
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
            model: Model::none(),
        }
    }

    fn key(mut self, key: Auth) -> Self {
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

    fn model(mut self, model: Model) -> Self {
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
                "formats: {} is spoken only by providers; no client sends it",
                self.name
            ))),
        }
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

/// Every format this binary speaks: each format a vendor speaks
/// ([`ALL`]), reached as its own vendor's is, and passthrough. Its
/// providers are reached over HTTP, reading up to `limit` bytes of an answer
/// at once, or over WebSocket for a session.
pub fn formats(limit: usize) -> Vec<KnownFormat> {
    let http = move |provider: &str, base_url: &str, auth| -> Result<_, Fault> {
        let client = HttpTransport::new(base_url)?.with_headers(auth)?;
        Ok(explained(client, provider, base_url, limit))
    };
    let google = move |provider: &str, base_url: &str, auth| -> Result<_, Fault> {
        let client = HttpTransport::new(base_url)?
            .with_connect_timeout(std::time::Duration::from_secs(10))?
            .follow_redirects(google_storage);
        Ok(explained(
            client.with_headers(auth)?,
            provider,
            base_url,
            limit,
        ))
    };
    let chat = || Model::of(conversation::Input::model, conversation::Input::set_model);
    // Only a `Create` names a model; a follow-up goes by its job's id
    // ([`crate::jobs`]).
    let video = || Model::of(video::Input::model, video::Input::set_model);
    let mut known: Vec<KnownFormat> = Vec::new();
    for speaks in ALL.iter().flat_map(|p| p.formats) {
        if known.iter().any(|k| k.name == speaks.format.name()) {
            continue;
        }
        let format = match speaks.format.clone() {
            Spoken::OpenAiChat(f) => {
                KnownFormat::of(kind::CHAT, CHAT_COMPLETIONS, f, http).model(chat())
            }
            Spoken::AnthropicMessages(f) => {
                KnownFormat::of(kind::CHAT, &["POST /v1/messages"], f, http).model(chat())
            }
            Spoken::OpenAiImages(f) => KnownFormat::of(
                kind::IMAGE,
                &["POST /v1/images/generations", "POST /v1/images/edits"],
                f,
                http,
            )
            .model(Model::of(image::Input::model, image::Input::set_model)),
            Spoken::OpenAiTranscriptions(f) => KnownFormat::of(
                kind::TRANSCRIPTION,
                &[
                    "POST /v1/audio/transcriptions",
                    "POST /v1/audio/translations",
                ],
                f,
                http,
            )
            .model(Model::of(
                transcription::Input::model,
                transcription::Input::set_model,
            )),
            Spoken::OpenAiSpeech(f) => {
                KnownFormat::of(kind::SPEECH, &["POST /v1/audio/speech"], f, http)
                    .model(Model::of(speech::Input::model, speech::Input::set_model))
            }
            Spoken::OpenAiEmbeddings(f) => {
                KnownFormat::of(kind::EMBEDDING, &["POST /v1/embeddings"], f, http).model(
                    Model::of(embedding::Input::model, embedding::Input::set_model),
                )
            }
            Spoken::OpenAiVideos(f) => KnownFormat::of(kind::VIDEO, VIDEOS, f, http)
                .job_ids("video_")
                .model(video()),
            Spoken::GeminiVeo(f) => {
                KnownFormat::provider_only(kind::VIDEO, f, google).job_ids("veo_")
            }
            Spoken::OpenAiRealtime(f) => session(
                kind::REALTIME,
                f,
                OpenAiRealtime::PATH,
                speaks.base,
                |f, base| f.transport(base),
            )
            .model(realtime()),
            Spoken::GeminiLive(f) => session(
                kind::REALTIME,
                f,
                GeminiLive::PATH,
                speaks.base,
                |f, base| f.transport(base),
            )
            .model(realtime()),
            Spoken::ResponsesWs(f) => session(
                kind::RESPONSES,
                f,
                ResponsesWs::PATH,
                speaks.base,
                |f, base| f.transport(base),
            ),
        };
        known.push(format.key(speaks.auth).base(speaks.base));
    }
    known.push(
        KnownFormat::of(
            kind::PASSTHROUGH,
            &["ANY /* (every other request)"],
            Passthrough,
            http,
        )
        .base(Base::Origin),
    );
    known
}

/// How a realtime session names its model: at `?model=` on its handshake,
/// as its first input comes only once it is routed (a Gemini Live client
/// without it goes to the default). The model routing chose is dialed
/// ([`SessionModel`]) and asked for by the session's opening input.
fn realtime() -> Model {
    Model::new(
        |cx| requested_model(cx.extensions().get::<Wire>()?),
        |cx, model| {
            cx.extensions_mut().insert(SessionModel(model.clone()));
            cx.filter(realtime::set_model(model))
        },
    )
}

/// A session `format` of `kind`, its clients sending to its `path`: after
/// `/v1`, as an SDK adds it to the gateway's base URL, unless it sits at the
/// origin. Its providers are reached by `transport`.
fn session<F>(
    kind: Kind,
    format: F,
    path: &str,
    base: Base,
    transport: fn(&F, &str) -> Result<WsTransport, Fault>,
) -> KnownFormat
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
    KnownFormat::of(
        kind,
        &[&endpoint],
        format.clone(),
        move |_: &str, base_url: &str, auth| {
            Ok(transport(&format, base_url)?.with_headers(auth)?.connect())
        },
    )
    .websocket()
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

/// The redirects the Gemini API's calls follow: it serves a video's bytes by
/// redirecting to Google's storage, so Google's hosts over HTTPS alone. The
/// wire leaves the key behind once a call leaves the API's origin.
fn google_storage(uri: &Uri) -> bool {
    let host = uri.host().unwrap_or_default();
    let google = [".googleapis.com", ".googleusercontent.com"];
    uri.scheme_str() == Some("https") && google.iter().any(|domain| host.ends_with(domain))
}

#[cfg(test)]
mod storage_tests {
    use super::*;

    #[test]
    fn only_googles_hosts_over_https_are_followed() {
        let follows = |uri: &str| google_storage(&uri.parse().unwrap_or_default());
        assert!(follows("https://storage.googleapis.com/v/1"));
        assert!(follows("https://x.googleusercontent.com/v"));
        assert!(!follows("http://storage.googleapis.com/v"));
        assert!(!follows("https://evil.test/storage.googleapis.com"));
        assert!(!follows("https://googleapis.com.evil.test/v"));
    }
}
