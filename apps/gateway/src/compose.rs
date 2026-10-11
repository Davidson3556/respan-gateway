//! A checked configuration's gateway: its providers, one core provider per
//! format, and the formats it serves; and the plan the rest of the binary
//! reads, of who serves what.

use std::collections::BTreeSet;
use std::sync::Arc;

use respan_gateway_core::{Fault, Gateway};
use respan_gateway_http::http::HeaderMap;
use respan_gateway_http::http::header::{AUTHORIZATION, HeaderName, HeaderValue};

use crate::auth::Client;
use crate::config::{Auth, ClientKey, Config, PluginConfig, ProviderConfig};
use crate::formats::{KnownFormat, kind};
use crate::presets;
use respan_gateway_plugins::{Kind, Routes, Speaks};
use respan_gateway_providers::{Auth as KeyIn, Base, Provider};

/// The name of the format that relays what no other served format accepts.
pub const PASSTHROUGH: &str = "passthrough";

/// A provider as configured, with the core provider of each of its formats.
#[derive(Debug)]
pub struct Upstream {
    pub name: String,
    pub preset: Option<&'static Provider>,
    pub entries: Vec<Entry>,
    /// Whether it sends a key of its own, rather than the client's.
    pub own_key: bool,
    /// Where its key comes from, such as `env OPENAI_API_KEY` or `config`;
    /// never the key.
    pub key_from: String,
}

/// One format of a provider: the core provider registered for it.
pub struct Entry {
    pub format: String,
    pub kind: Kind,
    /// The core provider's name: the provider's for its first format,
    /// `name:format` for the others.
    pub internal: String,
    pub base_url: String,
    pub auth: HeaderMap,
}

impl std::fmt::Debug for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Entry")
            .field("format", &self.format)
            .field("internal", &self.internal)
            .field("base_url", &crate::redact::url(&self.base_url))
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct Plan {
    pub upstreams: Vec<Upstream>,
    pub serve: Vec<String>,
    pub routes: Arc<Routes>,
    pub keys: Vec<Client>,
    /// The MCP servers, set by the app that built their format.
    pub mcp: Vec<crate::mcp::McpServer>,
}

impl Plan {
    pub fn serves(&self, format: &str) -> bool {
        self.serve.iter().any(|s| s == format)
    }
}

pub fn compose(config: &Config, formats: &[KnownFormat]) -> Result<(Gateway, Plan), Fault> {
    let (gateway, upstreams) = providers(config, formats)?;
    let serve = match &config.formats {
        Some(serve) => serve.clone(),
        None => default_serve(&upstreams, formats),
    };
    let gateway = serve_formats(gateway, &serve, &upstreams, formats)?;
    let tables = model_tables(&config.plugins, &upstreams)?;
    if let Some(default) = &config.default
        && !upstreams.iter().any(|u| u.name == *default)
    {
        return Err(Fault::config(format!(
            "default: unknown provider '{default}'; providers: [{}]",
            names(&upstreams)
        )));
    }
    let keys = clients(&config.client_keys, config.admin_writes)?;
    if upstreams.is_empty() && config.mcp.is_empty() {
        return Err(Fault::config(
            "providers: none configured; add one, such as `openai: { vendor: openai }`",
        ));
    }
    let routes = Arc::new(routes(&upstreams, tables, config.default.clone()));
    let plan = Plan {
        upstreams,
        serve,
        routes,
        keys,
        mcp: Vec::new(),
    };
    Ok((gateway, plan))
}

/// The routes to `upstreams`: by `tables`, then their presets' prefixes,
/// then by default to `default`.
fn routes(
    upstreams: &[Upstream],
    tables: Vec<Vec<(String, String)>>,
    default: Option<String>,
) -> Routes {
    let mut routes = Routes::new();
    for u in upstreams {
        let speaks = u.entries.iter().map(|e| Speaks {
            format: e.format.clone(),
            kind: e.kind,
            internal: e.internal.clone(),
        });
        routes = routes.provider(&u.name, speaks.collect());
    }
    let claims = upstreams.iter().flat_map(|u| {
        let prefixes = u.preset.map(|p| p.prefixes).unwrap_or_default();
        prefixes.iter().map(|prefix| (*prefix, &u.name))
    });
    routes = claims.fold(routes, |routes, (prefix, name)| routes.claim(prefix, name));
    routes = tables.into_iter().fold(routes, Routes::models);
    match default {
        Some(default) => routes.target(default),
        None => routes,
    }
}

/// The gateway with a core provider for each format of each configured
/// provider.
fn providers(config: &Config, formats: &[KnownFormat]) -> Result<(Gateway, Vec<Upstream>), Fault> {
    let mut gateway = Gateway::new();
    let mut upstreams = Vec::new();
    for (name, p) in &config.providers {
        let upstream = upstream(name, p, formats)?;
        for entry in &upstream.entries {
            let format = format_named(formats, &entry.format, &format!("provider '{name}'"))?;
            let (internal, auth) = (entry.internal.clone(), entry.auth.clone());
            gateway = format
                .add_provider(gateway, internal, &entry.base_url, auth)
                .map_err(|e| Fault::config(format!("provider '{name}': {}", e.message())))?;
        }
        upstreams.push(upstream);
    }
    Ok((gateway, upstreams))
}

/// The gateway serving each format of `serve`, in order. A format that
/// does not translate needs exactly one provider of its own.
fn serve_formats(
    mut gateway: Gateway,
    serve: &[String],
    upstreams: &[Upstream],
    formats: &[KnownFormat],
) -> Result<Gateway, Fault> {
    let after = serve.iter().skip_while(|n| *n != PASSTHROUGH).nth(1);
    if let Some(after) = after {
        return Err(Fault::config(format!(
            "formats: '{after}' comes after '{PASSTHROUGH}', which accepts every request first; \
             serve '{PASSTHROUGH}' last"
        )));
    }
    for name in serve {
        let served = format_named(formats, name, "formats")?;
        gateway = served.serve(gateway)?;
        if served.kind.translates || served.kind == kind::MCP {
            continue;
        }
        let speakers: Vec<&str> = upstreams
            .iter()
            .filter(|u| u.entries.iter().any(|e| e.format == *name))
            .map(|u| u.name.as_str())
            .collect();
        if speakers.len() != 1 {
            let mut message = format!("formats: {name} needs exactly one provider of that format");
            if speakers.len() > 1 {
                message.push_str(&format!("; {} all speak it", speakers.join(", ")));
            }
            return Err(Fault::config(message));
        }
    }
    Ok(gateway)
}

/// Each `models` plugin's routes, as `(prefix, provider)`, in the order
/// listed. A route applies to every kind its provider serves.
fn model_tables(
    plugins: &[PluginConfig],
    upstreams: &[Upstream],
) -> Result<Vec<Vec<(String, String)>>, Fault> {
    let mut tables = Vec::new();
    for PluginConfig { models: routes } in plugins {
        for m in routes {
            if !upstreams.iter().any(|u| u.name == m.provider) {
                return Err(Fault::config(format!(
                    "plugins: models: unknown provider '{}'; providers: [{}]",
                    m.provider,
                    names(upstreams)
                )));
            }
        }
        tables.push(
            routes
                .iter()
                .map(|m| (m.prefix.clone(), m.provider.clone()))
                .collect(),
        );
    }
    Ok(tables)
}

/// The providers' names, comma separated.
fn names(upstreams: &[Upstream]) -> String {
    let names: Vec<&str> = upstreams.iter().map(|u| u.name.as_str()).collect();
    names.join(", ")
}

/// The compiled-in format `name`; `owner` says who asked for it in an error.
fn format_named<'a>(
    formats: &'a [KnownFormat],
    name: &str,
    owner: &str,
) -> Result<&'a KnownFormat, Fault> {
    formats.iter().find(|f| f.name == name).ok_or_else(|| {
        let known: Vec<_> = formats.iter().map(|f| f.name.as_str()).collect();
        Fault::config(format!(
            "{owner}: unknown format '{name}'; compiled-in formats: [{}]",
            known.join(", ")
        ))
    })
}

/// Whether `name` may name a provider: routing reads `name/model`, video
/// ids `name~id`, and core providers `name:format`.
pub(crate) fn valid_provider_name(name: &str) -> bool {
    let allowed = |c: char| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-');
    !name.is_empty() && name.chars().all(allowed)
}

/// The formats served when the config names none: OpenAI Chat, Anthropic
/// Messages, every other chat format some provider speaks, every client
/// format of a media or realtime kind some provider speaks, and every
/// format of a kind that does not translate that some provider speaks, but
/// passthrough, which takes every request.
fn default_serve(upstreams: &[Upstream], formats: &[KnownFormat]) -> Vec<String> {
    let mut serve = vec!["openai-chat".to_owned(), "anthropic-messages".to_owned()];
    let entries = || upstreams.iter().flat_map(|u| &u.entries);
    let spoken = |kind: Kind| entries().any(|e| e.kind == kind);
    let translated = formats
        .iter()
        .filter(|f| match f.kind {
            kind::CHAT => entries().any(|e| e.format == f.name),
            kind::MCP => true,
            k => (kind::MEDIA.contains(&k) || k == kind::REALTIME) && spoken(k),
        })
        .filter(|f| f.servable());
    let sessions = entries().filter(|e| !e.kind.translates && e.format != PASSTHROUGH);
    let names = translated
        .map(|f| &f.name)
        .chain(sessions.map(|e| &e.format));
    for name in names {
        if !serve.contains(name) {
            serve.push(name.clone());
        }
    }
    serve
}

/// A provider's formats, each with its base URL and credentials.
fn upstream(name: &str, p: &ProviderConfig, formats: &[KnownFormat]) -> Result<Upstream, Fault> {
    let owner = format!("provider '{name}'");
    let bad = |e: String| Fault::config(format!("{owner}: {e}"));
    if !valid_provider_name(name) {
        return Err(bad(
            "a name is letters, digits, '_', '.' and '-' alone (no '/', '~' or ':')".into(),
        ));
    }
    let preset = match &p.vendor {
        None => None,
        Some(vendor) => Some(respan_gateway_providers::find(vendor).ok_or_else(|| {
            bad(format!(
                "unknown vendor '{vendor}'; presets: [{}]",
                presets::vendors().join(", ")
            ))
        })?),
    };
    let names: Vec<String> = match (&p.formats, preset) {
        (Some(names), _) => names.clone(),
        (None, Some(preset)) => preset
            .formats
            .iter()
            .map(|s| s.format.name().to_owned())
            .collect(),
        (None, None) => return Err(bad("needs a `vendor` or `formats`".into())),
    };
    if names.is_empty() {
        return Err(bad("has no formats".into()));
    }
    let base_url = p
        .base_url
        .as_deref()
        .or(preset.and_then(|preset| preset.base_url))
        .ok_or_else(|| bad("needs a `base_url`".into()))?;
    let (key, key_from) = match (&p.key, &p.auth, preset) {
        (Some(_), Some(_), _) => return Err(bad("set `key` or `auth`, not both".into())),
        (Some(key), None, _) => (Some(secret(&owner, key)?), key_source(key)),
        (None, None, Some(preset)) if !preset.key_env.is_empty() => {
            let Some(key) = presets::key_from_env(preset.key_env) else {
                let var = preset.key_env.join(" or ");
                return Err(bad(format!(
                    "environment variable {var} is not set; export it, put it in .env, or set `key:`"
                )));
            };
            (Some(secret(&owner, &key)?), key_source(&key))
        }
        (None, Some(Auth::Bearer { key } | Auth::Header { key, .. }), _) => (None, key_source(key)),
        _ => (None, "client credentials".to_owned()),
    };
    // One explicit format sits at its base URL exactly as given.
    let exact = preset.is_none() && names.len() == 1;
    let mut entries = Vec::new();
    for (i, format) in names.iter().enumerate() {
        let known = format_named(formats, format, &owner)?;
        // As its vendor speaks it, else as the format's own vendor does.
        let spoken = preset.and_then(|v| v.formats.iter().find(|s| s.format.name() == format));
        let (key_in, base) = spoken.map_or((known.key, known.base), |s| (s.auth, s.base));
        let auth = match (&p.auth, &key) {
            (Some(auth), _) => auth_headers(name, auth)?,
            (None, Some(key)) => key_headers(name, key_in, key)?,
            (None, None) => HeaderMap::new(),
        };
        let base_url = match exact {
            true => base_url.to_owned(),
            false => base_for(known, base, base_url),
        };
        let internal = match i {
            0 => name.to_owned(),
            _ => format!("{name}:{format}"),
        };
        entries.push(Entry {
            format: format.clone(),
            kind: known.kind,
            internal,
            base_url,
            auth,
        });
    }
    let unique: BTreeSet<&String> = names.iter().collect();
    if unique.len() != names.len() {
        return Err(bad("lists a format twice".into()));
    }
    Ok(Upstream {
        name: name.to_owned(),
        preset,
        entries,
        own_key: p.auth.is_some() || key.is_some(),
        key_from,
    })
}

/// Where a configured key comes from, for a person to read: never the key.
fn key_source(value: &str) -> String {
    match value.strip_prefix("env:") {
        Some(var) => format!("env {var}"),
        None => "config".to_owned(),
    }
}

/// Where `format` sits, at `base`, for a provider at `base_url`.
fn base_for(format: &KnownFormat, base: Base, base_url: &str) -> String {
    let base_url = base_url.trim_end_matches('/');
    let base = match base {
        Base::Same => base_url,
        Base::Origin => presets::origin(base_url),
        Base::Without(suffix) => base_url.strip_suffix(suffix).unwrap_or(base_url),
    };
    presets::with_scheme(base, format.websocket)
}

/// `env:NAME` reads the environment variable; anything else is literal.
/// Either is trimmed, and must not be empty. `owner` says whose it is in
/// an error.
pub fn secret(owner: &str, value: &str) -> Result<String, Fault> {
    let (value, empty) = match value.strip_prefix("env:") {
        Some(var) => {
            let value = std::env::var(var).map_err(|_| {
                Fault::config(format!("{owner}: environment variable {var} is not set"))
            })?;
            (value, format!("environment variable {var} is empty"))
        }
        None => (value.to_owned(), "a key is empty".to_owned()),
    };
    match value.trim() {
        "" => Err(Fault::config(format!("{owner}: {empty}"))),
        trimmed => Ok(trimmed.to_owned()),
    }
}

/// The client keys, read, each with the name its requests are logged
/// under (its own, else `key-<n>` by its place in the list), whether it
/// reaches the admin API, and the tools it is limited to.
///
/// A key's `admin` is what its entry says. An entry that does not say is an
/// admin key unless some entry sets `admin: true`, when only those are.
/// `admin_writes` needs an entry that sets `admin: true`.
fn clients(entries: &[ClientKey], admin_writes: bool) -> Result<Vec<Client>, Fault> {
    let named_admins = entries.iter().any(|e| e.admin() == Some(true));
    if admin_writes && !named_admins {
        return Err(Fault::config(
            "admin_writes: needs a client key with `admin: true` \
             (`client_keys: [{ name: ops, key: ..., admin: true }]`), so that not \
             every key can rewrite the config",
        ));
    }
    let mut clients: Vec<Client> = Vec::new();
    for (i, entry) in entries.iter().enumerate() {
        let key = secret("client_keys", entry.key())?;
        let name = match entry.name().map(str::trim) {
            Some("") => return Err(Fault::config("client_keys: a name is empty")),
            Some(name) => name.to_owned(),
            None => format!("key-{}", i + 1),
        };
        if clients.iter().any(|c| c.name == name) {
            return Err(Fault::config(format!(
                "client_keys: the name '{name}' is used twice"
            )));
        }
        let admin = entry.admin().unwrap_or(!named_admins);
        let tools = entry.tools().map(<[String]>::to_vec);
        clients.push(Client {
            name,
            key,
            admin,
            tools,
        });
    }
    Ok(clients)
}

/// A key read already, sent as a format expects.
fn key_headers(provider: &str, header: KeyIn, key: &str) -> Result<HeaderMap, Fault> {
    match header {
        KeyIn::Bearer => one_header(provider, AUTHORIZATION, &format!("Bearer {key}")),
        KeyIn::Header(name) => one_header(provider, HeaderName::from_static(name), key),
    }
}

/// The header of `auth`, its key read.
pub fn auth_headers(provider: &str, auth: &Auth) -> Result<HeaderMap, Fault> {
    let read = |key: &str| secret(&format!("provider '{provider}'"), key);
    match auth {
        Auth::Bearer { key } => {
            one_header(provider, AUTHORIZATION, &format!("Bearer {}", read(key)?))
        }
        Auth::Header { name, key } => {
            let name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|e| Fault::config(format!("provider '{provider}': {e}")))?;
            one_header(provider, name, &read(key)?)
        }
    }
}

fn one_header(provider: &str, name: HeaderName, value: &str) -> Result<HeaderMap, Fault> {
    let mut value = HeaderValue::from_str(value)
        .map_err(|e| Fault::config(format!("provider '{provider}': {e}")))?;
    value.set_sensitive(true);
    let mut headers = HeaderMap::new();
    headers.insert(name, value);
    Ok(headers)
}

#[cfg(test)]
#[allow(dead_code, unused_imports)]
#[path = "../../../tests/support/http.rs"]
mod http_helpers;

#[cfg(test)]
mod tests;
