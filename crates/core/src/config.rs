use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use http::Method;
use http::header::{AUTHORIZATION, HeaderName, HeaderValue};
use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::gateway::{Inner, Route, Source};
use crate::{Fault, Format, Gateway, MemoryStore, Plugin, Store, Target, Transport};

/// The static configuration, usually loaded from YAML.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub listen: Option<String>,
    #[serde(default)]
    pub providers: BTreeMap<String, ProviderConfig>,
    pub routes: Vec<RouteConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderConfig {
    pub format: String,
    pub base_url: String,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub auth: Option<AuthConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum AuthConfig {
    /// `Authorization: Bearer <key>`
    Bearer { key: String },
    /// `<name>: <key>`
    Header { name: String, key: String },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteConfig {
    pub name: String,
    pub format: String,
    /// How an HTTP adapter reaches this route. Routes without it are reachable
    /// only by name, e.g. from an embedding program.
    #[serde(default)]
    pub http: Option<HttpRouteConfig>,
    #[serde(default)]
    pub chain: Vec<EntryConfig>,
    /// A provider name. Exactly one of `target` and `source` is set.
    #[serde(default)]
    pub target: Option<String>,
    /// A source plugin that decides which providers to call.
    #[serde(default)]
    pub source: Option<EntryConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpRouteConfig {
    #[serde(default = "default_method")]
    pub method: String,
    pub path: String,
}

fn default_method() -> String {
    "POST".into()
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EntryConfig {
    #[serde(rename = "use")]
    pub plugin: String,
    #[serde(default)]
    pub with: serde_json::Value,
}

type Factory = Box<dyn Fn(serde_json::Value) -> Result<Arc<dyn Plugin>, Fault> + Send + Sync>;

/// Registers the compiled-in formats and plugins, then builds a [`Gateway`]
/// from a [`Config`]. Anything the config names that is not registered is a
/// build error.
pub struct Builder {
    formats: HashMap<String, Arc<dyn Format>>,
    factories: BTreeMap<String, Factory>,
    store: Option<Arc<dyn Store>>,
    transport: Option<Arc<dyn Transport>>,
}

impl std::fmt::Debug for Builder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Builder")
            .field("formats", &self.formats.keys().collect::<Vec<_>>())
            .field("plugins", &self.factories.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl Default for Builder {
    fn default() -> Self {
        Self::new()
    }
}

impl Builder {
    pub fn new() -> Self {
        Self {
            formats: HashMap::new(),
            factories: BTreeMap::new(),
            store: None,
            transport: None,
        }
    }

    pub fn format(mut self, format: impl Format) -> Self {
        self.formats
            .insert(format.name().to_owned(), Arc::new(format));
        self
    }

    /// Register a plugin under `name`. `make` receives the entry's `with:`
    /// config, deserialized into `C`.
    pub fn plugin<C, P>(
        mut self,
        name: &str,
        make: impl Fn(C) -> Result<P, Fault> + Send + Sync + 'static,
    ) -> Self
    where
        C: DeserializeOwned,
        P: Plugin,
    {
        let plugin_name = name.to_owned();
        let factory: Factory = Box::new(move |with| {
            let config: C = serde_json::from_value(with)
                .map_err(|e| Fault::config(format!("plugin '{plugin_name}': {e}")))?;
            Ok(Arc::new(make(config)?) as Arc<dyn Plugin>)
        });
        self.factories.insert(name.to_owned(), factory);
        self
    }

    pub fn store(mut self, store: impl Store) -> Self {
        self.store = Some(Arc::new(store));
        self
    }

    pub fn transport(mut self, transport: impl Transport) -> Self {
        self.transport = Some(Arc::new(transport));
        self
    }

    pub fn build(self, config: &Config) -> Result<Gateway, Fault> {
        let mut targets = HashMap::new();
        for (name, p) in &config.providers {
            if !self.formats.contains_key(&p.format) {
                return Err(self.unknown_format(&p.format, &format!("provider '{name}'")));
            }
            targets.insert(
                name.clone(),
                Target {
                    name: name.clone(),
                    format: p.format.clone(),
                    base_url: p.base_url.clone(),
                    model: p.model.clone(),
                    headers: auth_headers(name, p.auth.as_ref())?,
                },
            );
        }

        let mut routes = HashMap::new();
        for r in &config.routes {
            let name = r.name.clone();
            let format = self
                .formats
                .get(&r.format)
                .cloned()
                .ok_or_else(|| self.unknown_format(&r.format, &format!("route '{name}'")))?;
            let http = match &r.http {
                None => None,
                Some(h) => {
                    let method =
                        Method::from_bytes(h.method.to_uppercase().as_bytes()).map_err(|_| {
                            Fault::config(format!("route '{name}': bad method '{}'", h.method))
                        })?;
                    Some((method, h.path.clone()))
                }
            };
            let chain = r
                .chain
                .iter()
                .map(|e| self.instantiate(e))
                .collect::<Result<Vec<_>, _>>()?;
            let source = match (&r.target, &r.source) {
                (Some(t), None) => {
                    if !targets.contains_key(t) {
                        return Err(Fault::config(format!(
                            "route '{name}': unknown provider '{t}'"
                        )));
                    }
                    Source::Provider(t.clone())
                }
                (None, Some(e)) => Source::Plugin(self.instantiate(e)?),
                _ => {
                    return Err(Fault::config(format!(
                        "route '{name}': set exactly one of `target` and `source`"
                    )));
                }
            };
            let route = Route {
                name: name.clone(),
                http,
                format,
                chain,
                source,
            };
            if routes.insert(name.clone(), route).is_some() {
                return Err(Fault::config(format!("route '{name}' is defined twice")));
            }
        }

        let transport = match self.transport {
            Some(t) => t,
            None => default_transport()?,
        };
        Ok(Gateway {
            inner: Arc::new(Inner {
                routes,
                formats: self.formats,
                targets,
                store: self.store.unwrap_or_else(|| Arc::new(MemoryStore::new())),
                transport,
                next_id: AtomicU64::new(1),
            }),
        })
    }

    fn instantiate(&self, e: &EntryConfig) -> Result<Arc<dyn Plugin>, Fault> {
        let factory = self.factories.get(&e.plugin).ok_or_else(|| {
            let known: Vec<_> = self.factories.keys().map(String::as_str).collect();
            Fault::config(format!(
                "unknown plugin '{}'; compiled-in plugins: [{}]",
                e.plugin,
                known.join(", ")
            ))
        })?;
        factory(e.with.clone())
    }

    fn unknown_format(&self, name: &str, context: &str) -> Fault {
        let mut known: Vec<_> = self.formats.keys().map(String::as_str).collect();
        known.sort_unstable();
        Fault::config(format!(
            "{context}: unknown format '{name}'; compiled-in formats: [{}]",
            known.join(", ")
        ))
    }
}

#[cfg(feature = "reqwest")]
fn default_transport() -> Result<Arc<dyn Transport>, Fault> {
    Ok(Arc::new(crate::HttpTransport::default()))
}

#[cfg(not(feature = "reqwest"))]
fn default_transport() -> Result<Arc<dyn Transport>, Fault> {
    Err(Fault::config("no transport configured"))
}

/// `env:NAME` reads the environment variable; anything else is literal.
fn secret(provider: &str, value: &str) -> Result<String, Fault> {
    match value.strip_prefix("env:") {
        Some(var) => std::env::var(var).map_err(|_| {
            Fault::config(format!(
                "provider '{provider}': environment variable {var} is not set"
            ))
        }),
        None => Ok(value.to_owned()),
    }
}

fn auth_headers(
    provider: &str,
    auth: Option<&AuthConfig>,
) -> Result<Vec<(HeaderName, HeaderValue)>, Fault> {
    let bad = |e: String| Fault::config(format!("provider '{provider}': {e}"));
    let header = |name: HeaderName, value: String| {
        let mut v = HeaderValue::from_str(&value).map_err(|e| bad(e.to_string()))?;
        v.set_sensitive(true);
        Ok::<_, Fault>((name, v))
    };
    match auth {
        None => Ok(Vec::new()),
        Some(AuthConfig::Bearer { key }) => Ok(vec![header(
            AUTHORIZATION,
            format!("Bearer {}", secret(provider, key)?),
        )?]),
        Some(AuthConfig::Header { name, key }) => {
            let name = HeaderName::from_bytes(name.as_bytes()).map_err(|e| bad(e.to_string()))?;
            Ok(vec![header(name, secret(provider, key)?)?])
        }
    }
}
