//! Every format this binary speaks, and what the rest of the binary needs
//! to know of each: its kind, its endpoints, how a provider of it
//! authenticates and is reached, and how routing reads a request's model.

use std::sync::Arc;

use respan_gateway_conversation::anthropic::AnthropicMessages;
use respan_gateway_conversation::openai::OpenAiChat;
use respan_gateway_core::{ClientContext, Extensions, Fault, Format, Gateway};
use respan_gateway_http::formats::opaque::{self, Passthrough};
use respan_gateway_http::http::HeaderMap;
use respan_gateway_http::{ClientIo, HttpTransport, ProviderIo};
use respan_gateway_realtime::JsonEvents;

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

impl Routed for respan_gateway_realtime::Input {}

impl<F: 'static> Routed for opaque::Input<F> {}

type AddProvider = dyn Fn(Gateway, String, &str, HeaderMap) -> Result<Gateway, Fault> + Send + Sync;
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
    provider: Box<AddProvider>,
    serve: Box<dyn Fn(Gateway) -> Result<Gateway, Fault> + Send + Sync>,
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
    /// `format` of `kind`, its providers reached by `reach`, given one's
    /// base URL and auth headers.
    pub fn of<F, I, O, T>(
        kind: Kind,
        endpoints: &'static [&'static str],
        format: F,
        reach: impl Fn(&str, HeaderMap) -> Result<T, Fault> + Send + Sync + 'static,
    ) -> Self
    where
        F: Clone + Send + Sync + 'static + Format<ClientIo, I, O> + Format<ProviderIo, O, I>,
        I: Routed,
        O: Clone + Send + Sync + 'static,
        T: Fn(Extensions) -> ProviderIo + Send + Sync + 'static,
    {
        let client = format.clone();
        Self {
            name: <F as Format<ClientIo, I, O>>::name(&format).to_owned(),
            kind,
            endpoints,
            key: KeyHeader::Bearer,
            base: Base::Same,
            websocket: false,
            provider: Box::new(move |gateway, name, base_url, auth| {
                gateway.provider::<I, O, _>(name, format.clone(), reach(base_url, auth)?)
            }),
            serve: Box::new(move |gateway| gateway.client::<I, O, _>(client.clone())),
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
        (self.serve)(gateway)
    }

    pub fn model_reader(&self) -> Arc<ReadModel> {
        self.model.clone()
    }

    pub fn model_writer(&self) -> Arc<SetModel> {
        self.set_model.clone()
    }
}

const CHAT_COMPLETIONS: &[&str] = &["POST /v1/chat/completions"];

/// Every format this binary speaks: OpenAI Chat as OpenAI and each vendor
/// with a dialect serve it, Anthropic Messages and passthrough, their
/// providers reached over HTTP, reading up to `limit` bytes of an answer
/// at once; and the sessions of OpenAI Realtime, Responses over WebSocket
/// and Gemini Live, their providers reached over WebSocket. A new format
/// is one line here.
pub fn formats(limit: usize) -> Vec<KnownFormat> {
    let http = move |base_url: &str, auth| -> Result<_, Fault> {
        let client = HttpTransport::new(base_url)?.with_headers(auth)?;
        Ok(client.with_limit(limit).connect())
    };
    let ws = |format: JsonEvents, endpoints| {
        KnownFormat::of(
            Kind::REALTIME,
            endpoints,
            format.clone(),
            move |base_url, auth| Ok(format.transport(base_url)?.with_headers(auth)?.connect()),
        )
        .websocket()
    };
    let chat = |format: OpenAiChat| KnownFormat::of(Kind::CHAT, CHAT_COMPLETIONS, format, http);
    vec![
        chat(OpenAiChat::OPENAI),
        chat(OpenAiChat::GROQ),
        chat(OpenAiChat::DEEPSEEK),
        chat(OpenAiChat::VLLM),
        chat(OpenAiChat::OPENROUTER),
        chat(OpenAiChat::AZURE),
        KnownFormat::of(Kind::CHAT, &["POST /v1/messages"], AnthropicMessages, http)
            .key(KeyHeader::Header("x-api-key")),
        KnownFormat::of(Kind::PASSTHROUGH, &["ANY /* (every other request)"], Passthrough, http)
            .base(Base::Origin),
        ws(JsonEvents::OPENAI_REALTIME, &["GET /v1/realtime (WebSocket)"]),
        ws(JsonEvents::RESPONSES_WS, &["GET /v1/responses (WebSocket)"]),
        ws(
            JsonEvents::GEMINI_LIVE,
            &["GET /ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent (WebSocket)"],
        )
        .key(KeyHeader::Header("x-goog-api-key"))
        .base(Base::Origin),
    ]
}
