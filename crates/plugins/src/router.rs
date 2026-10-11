//! Which provider a request goes to. A request goes, of the providers
//! serving its kind, to the one its model names as `provider/model` (sent
//! as `model`) if `provider` is configured, else, with the whole name as
//! the model, to the one a `models` table gives, else the one claiming the
//! longest prefix of the name, else the kind's default: the target if it
//! serves the kind, else the first provider that does.
//!
//! One provider is one core provider per format it speaks. A request goes
//! to its provider's core provider of the client's own format, else, for a
//! kind that translates, its first of the kind.

use std::collections::HashMap;
use std::sync::Arc;

use respan_gateway_core::{ClientContext, Fault, Plugin, Response, async_trait};

/// What sort of traffic a format carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Kind {
    /// Its name, such as `chat`.
    pub label: &'static str,
    /// Whether a client of one format of the kind may be answered by a
    /// provider of another; otherwise only by a provider of its own format.
    pub translates: bool,
}

/// A format a provider speaks, and its core provider of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Speaks {
    /// The format's name.
    pub format: String,
    /// The format's kind.
    pub kind: Kind,
    /// The core provider registered for it.
    pub internal: String,
}

/// Who serves what, and the routes to them.
#[derive(Debug, Default)]
pub struct Routes {
    /// Each provider and its formats, in the order added.
    providers: Vec<(String, Vec<Speaks>)>,
    /// The `models` tables, in the order added: a later one's match wins.
    tables: Vec<Vec<(String, String)>>,
    /// The claimed model prefixes, longest first, then in the order added.
    claims: Vec<(String, String)>,
    target: Option<String>,
}

/// Where a request goes.
#[derive(Debug, PartialEq, Eq)]
pub struct Route {
    /// The provider, as added.
    pub provider: String,
    /// The core provider of the format it is sent in.
    pub internal: String,
    /// The model to send instead, its `provider/` prefix taken off.
    pub model: Option<String>,
}

impl Routes {
    /// No providers and no routes.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds provider `name`, speaking each of `speaks`.
    pub fn provider(mut self, name: impl Into<String>, speaks: Vec<Speaks>) -> Self {
        self.providers.push((name.into(), speaks));
        self
    }

    /// Adds a `models` table of `(prefix, provider)`, tried in order, and
    /// before the tables added earlier.
    pub fn models(mut self, table: Vec<(String, String)>) -> Self {
        self.tables.push(table);
        self
    }

    /// Lets `provider` claim the models starting with `prefix`; the longest
    /// claim wins.
    pub fn claim(mut self, prefix: impl Into<String>, provider: impl Into<String>) -> Self {
        self.claims.push((prefix.into(), provider.into()));
        // `deepseek-ai/` is Together's, though `deepseek` is DeepSeek's.
        self.claims
            .sort_by_key(|(prefix, _)| std::cmp::Reverse(prefix.len()));
        self
    }

    /// Makes `provider` the default of each kind it serves.
    pub fn target(mut self, provider: impl Into<String>) -> Self {
        self.target = Some(provider.into());
        self
    }

    /// Where a request of `kind`, from a client of `format`, asking for
    /// `model`, goes; `None` if no provider serves it.
    pub fn route(&self, kind: Kind, format: &str, model: Option<&str>) -> Option<Route> {
        let to = |provider: &str, model: Option<&str>| {
            let internal = self.internal(provider, kind, format)?;
            Some(Route {
                provider: provider.to_owned(),
                internal: internal.to_owned(),
                model: model.map(str::to_owned),
            })
        };
        if let Some(model) = model {
            let named = model.split_once('/');
            if let Some(route) = named.and_then(|(provider, rest)| to(provider, Some(rest))) {
                return Some(route);
            }
            let matches = |(prefix, _): &&(String, String)| model.starts_with(prefix.as_str());
            for table in self.tables.iter().rev() {
                if let Some(route) = table.iter().find(matches).and_then(|(_, p)| to(p, None)) {
                    return Some(route);
                }
            }
            let claimed = self.claims.iter().filter(matches);
            if let Some(route) = claimed.into_iter().find_map(|(_, p)| to(p, None)) {
                return Some(route);
            }
        }
        self.target
            .iter()
            .chain(self.providers.iter().map(|(name, _)| name))
            .find_map(|provider| to(provider, None))
    }

    /// The core provider of `provider` that takes a client of `format`:
    /// one of that format, else, for a kind that translates, its first of
    /// the kind.
    fn internal(&self, provider: &str, kind: Kind, format: &str) -> Option<&str> {
        let (_, speaks) = self.providers.iter().find(|(name, _)| name == provider)?;
        let same = speaks.iter().find(|s| s.format == format);
        let translated = || speaks.iter().find(|s| kind.translates && s.kind == kind);
        same.or_else(translated).map(|s| s.internal.as_str())
    }

    /// The default provider of each kind, as `(kind, provider)`.
    pub fn defaults(&self) -> Vec<(&'static str, String)> {
        let mut kinds: Vec<&Speaks> = Vec::new();
        for speaks in self.providers.iter().flat_map(|(_, speaks)| speaks) {
            if !kinds.iter().any(|s| s.kind == speaks.kind) {
                kinds.push(speaks);
            }
        }
        kinds
            .into_iter()
            .filter_map(|s| Some((s.kind.label, self.route(s.kind, &s.format, None)?.provider)))
            .collect()
    }

    /// The routes by model prefix, as `(prefix, provider)`, in the order
    /// tried.
    pub fn prefixes(&self) -> Vec<(String, String)> {
        let tables = self.tables.iter().rev().flatten();
        tables.chain(&self.claims).cloned().collect()
    }
}

type Read = dyn Fn(&ClientContext) -> Result<Option<String>, Fault> + Send + Sync;
type Write = dyn Fn(&mut ClientContext, String) -> Result<(), Fault> + Send + Sync;

/// How a format's requests name their model.
#[derive(Clone)]
pub struct Model {
    read: Arc<Read>,
    write: Arc<Write>,
}

impl std::fmt::Debug for Model {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Model").finish_non_exhaustive()
    }
}

impl Model {
    /// The model of inputs of type `I`, read by `read` and rewritten by
    /// `write`, such as `Model::of(Input::model, Input::set_model)`. A
    /// request whose input is not an `I` is refused when routed, not
    /// routed to the default.
    pub fn of<I: Clone + Send + Sync + 'static>(
        read: fn(&I) -> Option<&str>,
        write: fn(&mut I, String),
    ) -> Self {
        Self {
            read: Arc::new(move |cx| Ok(read(cx.input::<I>()?).map(str::to_owned))),
            write: Arc::new(move |cx, model| {
                cx.filter(move |input: &mut I| {
                    write(input, model.clone());
                    Ok(())
                })
            }),
        }
    }

    /// The model `read` reads of a request, and `write` asks for, for a
    /// format whose inputs do not name it at once, such as a session that
    /// names it in its handshake.
    pub fn new(
        read: impl Fn(&ClientContext) -> Option<String> + Send + Sync + 'static,
        write: impl Fn(&mut ClientContext, String) -> Result<(), Fault> + Send + Sync + 'static,
    ) -> Self {
        Self {
            read: Arc::new(move |cx| Ok(read(cx))),
            write: Arc::new(write),
        }
    }

    /// For requests that name no model.
    pub fn none() -> Self {
        Self {
            read: Arc::new(|_| Ok(None)),
            write: Arc::new(|_, _| Ok(())),
        }
    }

    /// The model `cx`'s request asks for, if any.
    pub fn read(&self, cx: &ClientContext) -> Option<String> {
        (self.read)(cx).ok().flatten()
    }
}

/// The plugin that targets each request of a format it routes by
/// [`Routes`]. A request of another format keeps its target, and so does
/// one a plugin before it targeted; a gateway with a default target
/// ([`Gateway::target`]) tells it with [`Routing::over`], else that
/// default would pass for a plugin's choice.
///
/// [`Gateway::target`]: respan_gateway_core::Gateway::target
///
/// ```
/// use std::sync::Arc;
/// use respan_gateway_conversation::Input;
/// use respan_gateway_plugins::{Kind, Model, Routes, Routing, Speaks};
///
/// const CHAT: Kind = Kind { label: "chat", translates: true };
/// let speaks = |format: &str, internal: &str| Speaks {
///     format: format.into(),
///     kind: CHAT,
///     internal: internal.into(),
/// };
/// let routes = Routes::new()
///     .provider("openai", vec![speaks("openai-chat", "openai")])
///     .provider("anthropic", vec![speaks("anthropic-messages", "anthropic")])
///     .claim("claude", "anthropic")
///     .target("openai");
/// let route = routes.route(CHAT, "openai-chat", Some("claude-sonnet-4-5"));
/// assert_eq!(route.map(|r| r.internal).as_deref(), Some("anthropic"));
/// let routing = Routing::new(Arc::new(routes))
///     .format("openai-chat", CHAT, Model::of(Input::model, Input::set_model));
/// ```
#[derive(Debug)]
pub struct Routing {
    routes: Arc<Routes>,
    formats: HashMap<String, (Kind, Model)>,
    default: Option<String>,
}

impl Routing {
    /// Routing by `routes`, of no format yet.
    pub fn new(routes: Arc<Routes>) -> Self {
        Self {
            routes,
            formats: HashMap::new(),
            default: None,
        }
    }

    /// Tells it `provider` is the gateway's default target, not a
    /// plugin's choice.
    pub fn over(mut self, provider: impl Into<String>) -> Self {
        self.default = Some(provider.into());
        self
    }

    /// Routes the requests of format `name`, of `kind`, by their `model`.
    pub fn format(mut self, name: impl Into<String>, kind: Kind, model: Model) -> Self {
        self.formats.insert(name.into(), (kind, model));
        self
    }
}

#[async_trait]
impl Plugin for Routing {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let Some((kind, model)) = self.formats.get(cx.format()) else {
            return Ok(None);
        };
        let asked = (model.read)(cx)?;
        let Some(route) = self.routes.route(*kind, cx.format(), asked.as_deref()) else {
            return Err(Fault::not_found(format!(
                "no provider serves {} requests; add one to the config",
                kind.label
            )));
        };
        if cx.target().is_none() || cx.target() == self.default.as_deref() {
            cx.set_target(route.internal);
        }
        if let Some(sent) = route.model {
            (model.write)(cx, sent)?;
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CHAT: Kind = Kind {
        label: "chat",
        translates: true,
    };
    const REALTIME: Kind = Kind {
        label: "realtime",
        translates: false,
    };
    const PASSTHROUGH: Kind = Kind {
        label: "passthrough",
        translates: false,
    };

    fn speaks(format: &str, kind: Kind, internal: &str) -> Speaks {
        Speaks {
            format: format.into(),
            kind,
            internal: internal.into(),
        }
    }

    fn routes(target: Option<&str>, tables: &[&[(&str, &str)]]) -> Routes {
        let mut routes = Routes::new()
            .provider("groq", vec![speaks("openai-chat", CHAT, "groq")])
            .provider(
                "openai",
                vec![
                    speaks("openai-chat", CHAT, "openai"),
                    speaks("openai-realtime", REALTIME, "openai:openai-realtime"),
                ],
            )
            .provider(
                "anthropic",
                vec![speaks("anthropic-messages", CHAT, "anthropic")],
            )
            .claim("gpt", "openai")
            .claim("claude", "anthropic");
        for table in tables {
            let table = table.iter().map(|(a, b)| ((*a).into(), (*b).into()));
            routes = routes.models(table.collect());
        }
        match target {
            Some(target) => routes.target(target),
            None => routes,
        }
    }

    fn to(routes: &Routes, format: &str, model: &str) -> (String, String, Option<String>) {
        let kind = match format {
            "openai-realtime" => REALTIME,
            _ => CHAT,
        };
        let route = routes.route(kind, format, Some(model));
        let route = route.unwrap_or_else(|| panic!("no route for {model}"));
        (route.provider, route.internal, route.model)
    }

    #[test]
    fn a_provider_prefix_names_the_provider_and_is_taken_off() {
        let routes = routes(None, &[]);
        let groq = to(&routes, "openai-chat", "groq/llama-3.3-70b-versatile");
        assert_eq!(
            groq,
            (
                "groq".into(),
                "groq".into(),
                Some("llama-3.3-70b-versatile".into())
            )
        );
        // Not a provider: the whole name is the model.
        let other = to(&routes, "openai-chat", "meta-llama/x");
        assert_eq!(other.0, "groq", "the first provider of the kind");
        assert_eq!(other.2, None);
    }

    #[test]
    fn models_tables_then_claims_then_the_default() {
        let routes = routes(
            Some("anthropic"),
            &[&[("gpt-4", "groq")], &[("gpt-4o", "anthropic")]],
        );
        assert_eq!(
            to(&routes, "openai-chat", "gpt-4o-mini").0,
            "anthropic",
            "the later table"
        );
        assert_eq!(to(&routes, "openai-chat", "gpt-4.1").0, "groq");
        assert_eq!(
            to(&routes, "anthropic-messages", "gpt-3.5").0,
            "openai",
            "a claim"
        );
        assert_eq!(
            to(&routes, "openai-chat", "llama").0,
            "anthropic",
            "the target"
        );
        let untargeted = super::tests::routes(None, &[]);
        assert_eq!(
            to(&untargeted, "openai-chat", "llama").0,
            "groq",
            "the first"
        );
    }

    #[test]
    fn the_longest_claim_wins() {
        let routes = Routes::new()
            .provider("deepseek", vec![speaks("openai-chat", CHAT, "deepseek")])
            .provider("together", vec![speaks("openai-chat", CHAT, "together")])
            .claim("deepseek", "deepseek")
            .claim("deepseek-ai/", "together");
        assert_eq!(to(&routes, "openai-chat", "deepseek-ai/r1").0, "together");
        assert_eq!(to(&routes, "openai-chat", "deepseek-chat").0, "deepseek");
    }

    #[test]
    fn a_client_is_sent_in_its_own_format_else_translated() {
        let routes = routes(Some("anthropic"), &[]);
        let (_, internal, _) = to(&routes, "anthropic-messages", "gpt-4o");
        assert_eq!(internal, "openai", "OpenAI's first chat format");
        let (_, internal, _) = to(&routes, "openai-chat", "anthropic/claude");
        assert_eq!(internal, "anthropic");
    }

    #[test]
    fn only_providers_of_the_kind_are_candidates() {
        let routes = routes(Some("anthropic"), &[]);
        // Anthropic serves no realtime, so the target is passed over.
        let realtime = to(&routes, "openai-realtime", "anthropic/gpt-realtime");
        assert_eq!(
            realtime,
            ("openai".into(), "openai:openai-realtime".into(), None)
        );
        assert_eq!(routes.route(PASSTHROUGH, "passthrough", None), None);
    }

    #[test]
    fn a_session_crosses_formats_only_in_a_kind_that_translates() {
        const LIVE: Kind = Kind {
            label: "realtime",
            translates: true,
        };
        const RESPONSES: Kind = Kind {
            label: "responses",
            translates: false,
        };
        let openai = vec![
            speaks("openai-realtime", LIVE, "openai:openai-realtime"),
            speaks("responses-ws", RESPONSES, "openai:responses-ws"),
        ];
        let routes = Routes::new()
            .provider("openai", openai)
            .provider("gemini", vec![speaks("gemini-live", LIVE, "gemini")])
            .claim("gpt", "openai")
            .claim("gemini", "gemini")
            .target("gemini");
        let internal = |kind, format, model| routes.route(kind, format, model).map(|r| r.internal);
        let to_openai = internal(LIVE, "gemini-live", Some("gpt-realtime"));
        assert_eq!(to_openai.as_deref(), Some("openai:openai-realtime"));
        let to_gemini = internal(LIVE, "openai-realtime", Some("gemini-live"));
        assert_eq!(to_gemini.as_deref(), Some("gemini"));
        assert_eq!(
            internal(LIVE, "gemini-live", None).as_deref(),
            Some("gemini")
        );
        // Gemini speaks no Responses, so even a model it claims stays with
        // the provider that does.
        let responses = internal(RESPONSES, "responses-ws", Some("gemini-live"));
        assert_eq!(responses.as_deref(), Some("openai:responses-ws"));
    }

    #[test]
    fn each_kind_has_its_default() {
        let routes = routes(Some("anthropic"), &[]);
        assert_eq!(
            routes.defaults(),
            [
                ("chat", "anthropic".to_owned()),
                ("realtime", "openai".to_owned())
            ]
        );
    }

    #[test]
    fn prefixes_list_the_tables_latest_first_then_the_claims() {
        let routes = routes(None, &[&[("a", "groq")], &[("b", "openai")]]);
        let prefixes = routes.prefixes();
        let prefixes: Vec<(&str, &str)> = prefixes
            .iter()
            .map(|(p, to)| (p.as_str(), to.as_str()))
            .collect();
        assert_eq!(
            prefixes,
            [
                ("b", "openai"),
                ("a", "groq"),
                ("claude", "anthropic"),
                ("gpt", "openai")
            ]
        );
    }
}
