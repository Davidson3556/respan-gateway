//! The binary's one routing step. A request goes, of the providers serving
//! its kind, to the one its model names as `provider/model` (sent as
//! `model`), else the one a `models` route gives, else the one whose
//! preset claims its model's prefix, else the kind's default: `target` if
//! it serves the kind, else the first configured that does.

use std::collections::HashMap;
use std::sync::Arc;

use respan_gateway_core::{ClientContext, Fault, Plugin, Response, async_trait};

use crate::compose::Upstream;
use crate::formats::{Kind, KnownFormat, ReadModel, SetModel};

/// A format a provider speaks: its name, kind and core provider.
type Speaks = (String, Kind, String);

/// Who serves what, and the routes to them, in the order they apply.
#[derive(Debug, Default)]
pub struct Routes {
    /// Each provider's formats, as `(format, kind, core provider)`, in
    /// config order.
    providers: Vec<(String, Vec<Speaks>)>,
    /// The `models` plugins' routes, in the order they run: a later one's
    /// match wins, as its target was set last.
    tables: Vec<Vec<(String, String)>>,
    /// The presets' model prefixes, in config order.
    prefixes: Vec<(String, String)>,
    target: Option<String>,
}

/// Where a request goes.
#[derive(Debug, PartialEq, Eq)]
pub struct Route {
    /// The provider, as configured.
    pub provider: String,
    /// The core provider of the format it is sent in.
    pub internal: String,
    /// The model to send instead, its `provider/` prefix taken off.
    pub model: Option<String>,
}

impl Routes {
    pub fn new(
        upstreams: &[Upstream],
        tables: Vec<Vec<(String, String)>>,
        target: Option<String>,
    ) -> Self {
        let providers = upstreams
            .iter()
            .map(|u| {
                let entries = u.entries.iter();
                let entries = entries.map(|e| (e.format.clone(), e.kind, e.internal.clone()));
                (u.name.clone(), entries.collect())
            })
            .collect();
        let prefixes = upstreams
            .iter()
            .flat_map(|u| {
                let prefixes = u.preset.map(|p| p.prefixes).unwrap_or_default();
                prefixes
                    .iter()
                    .map(|prefix| ((*prefix).to_owned(), u.name.clone()))
            })
            .collect();
        Self {
            providers,
            tables,
            prefixes,
            target,
        }
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
            let claimed = self.prefixes.iter().filter(matches);
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
        let (_, entries) = self.providers.iter().find(|(name, _)| name == provider)?;
        let same = entries.iter().find(|(f, ..)| f == format);
        let translated = || {
            entries
                .iter()
                .find(|(_, k, _)| kind.translates && *k == kind)
        };
        same.or_else(translated)
            .map(|(.., internal)| internal.as_str())
    }

    /// The default provider of each kind, as `(kind, provider)`.
    pub fn defaults(&self) -> Vec<(&'static str, String)> {
        let mut kinds: Vec<(Kind, &str)> = Vec::new();
        for (_, entries) in &self.providers {
            for (format, kind, _) in entries {
                if !kinds.iter().any(|(k, _)| k == kind) {
                    kinds.push((*kind, format));
                }
            }
        }
        kinds
            .into_iter()
            .filter_map(|(kind, format)| {
                Some((kind.label, self.route(kind, format, None)?.provider))
            })
            .collect()
    }

    /// The routes by model prefix, as `(prefix, provider)`, in the order tried.
    pub fn prefixes(&self) -> Vec<(String, String)> {
        let tables = self.tables.iter().rev().flatten();
        tables.chain(&self.prefixes).cloned().collect()
    }
}

/// How routing reads and rewrites the requests of one format.
struct FormatRouting {
    kind: Kind,
    model: Arc<ReadModel>,
    set_model: Arc<SetModel>,
}

/// The plugin that routes each request by [`Routes`].
pub struct Routing {
    routes: Arc<Routes>,
    formats: HashMap<String, FormatRouting>,
}

impl std::fmt::Debug for Routing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Routing")
            .field("routes", &self.routes)
            .finish()
    }
}

impl Routing {
    pub fn new(routes: Arc<Routes>, formats: &[KnownFormat]) -> Self {
        let formats = formats
            .iter()
            .map(|f| {
                let routing = FormatRouting {
                    kind: f.kind,
                    model: f.model_reader(),
                    set_model: f.model_writer(),
                };
                (f.name.clone(), routing)
            })
            .collect();
        Self { routes, formats }
    }
}

#[async_trait]
impl Plugin for Routing {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let Some(format) = self.formats.get(cx.format()) else {
            return Ok(None);
        };
        let model = (format.model)(cx);
        let Some(route) = self
            .routes
            .route(format.kind, cx.format(), model.as_deref())
        else {
            let kind = format.kind.label;
            return Err(Fault::not_found(format!(
                "no provider serves {kind} requests; add one to the config"
            )));
        };
        cx.set_target(route.internal);
        if let Some(model) = route.model {
            (format.set_model)(cx, model)?;
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn routes(target: Option<&str>, tables: &[&[(&str, &str)]]) -> Routes {
        let entry =
            |format: &str, kind, internal: &str| (format.to_owned(), kind, internal.to_owned());
        Routes {
            providers: vec![
                ("groq".into(), vec![entry("groq-chat", Kind::CHAT, "groq")]),
                (
                    "openai".into(),
                    vec![
                        entry("openai-chat", Kind::CHAT, "openai"),
                        entry("openai-realtime", Kind::REALTIME, "openai:openai-realtime"),
                    ],
                ),
                (
                    "anthropic".into(),
                    vec![entry("anthropic-messages", Kind::CHAT, "anthropic")],
                ),
            ],
            tables: tables
                .iter()
                .map(|t| t.iter().map(|(a, b)| ((*a).into(), (*b).into())).collect())
                .collect(),
            prefixes: vec![
                ("gpt".into(), "openai".into()),
                ("claude".into(), "anthropic".into()),
            ],
            target: target.map(str::to_owned),
        }
    }

    fn to(routes: &Routes, format: &str, model: &str) -> (String, String, Option<String>) {
        let kind = match format {
            "openai-realtime" => Kind::REALTIME,
            _ => Kind::CHAT,
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
    fn models_routes_then_presets_then_the_default() {
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
            "a preset prefix"
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
    fn only_providers_of_the_kind_are_candidates() {
        let routes = routes(Some("anthropic"), &[]);
        // Anthropic serves no realtime, so the target is passed over.
        let realtime = to(&routes, "openai-realtime", "anthropic/gpt-realtime");
        assert_eq!(
            realtime,
            ("openai".into(), "openai:openai-realtime".into(), None)
        );
        assert_eq!(routes.route(Kind::PASSTHROUGH, "passthrough", None), None);
    }

    #[test]
    fn each_kind_has_its_default() {
        let routes = routes(Some("anthropic"), &[]);
        let defaults = routes.defaults();
        assert_eq!(
            defaults,
            [
                ("chat", "anthropic".to_owned()),
                ("realtime", "openai".to_owned())
            ]
        );
    }
}
