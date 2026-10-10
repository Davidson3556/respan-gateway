//! A checked configuration's gateway: its providers, one core provider per
//! format, and the formats it serves; and the plan the rest of the binary
//! reads, of who serves what.

use std::collections::BTreeSet;
use std::sync::Arc;

use respan_gateway_core::{Fault, Gateway};
use respan_gateway_http::http::HeaderMap;
use respan_gateway_http::http::header::{AUTHORIZATION, HeaderName, HeaderValue};

use crate::config::{Auth, Config, PluginConfig, ProviderConfig};
use crate::formats::{Base, KeyHeader, Kind, KnownFormat};
use crate::presets::{self, Preset};
use crate::routing::Routes;

/// The name of the format that relays what no other served format accepts.
pub const PASSTHROUGH: &str = "passthrough";

/// A provider as configured, with the core provider of each of its formats.
#[derive(Debug)]
pub struct Upstream {
    pub name: String,
    pub preset: Option<&'static Preset>,
    pub entries: Vec<Entry>,
    /// Whether it sends a key of its own, rather than the client's.
    pub own_key: bool,
}

/// One format of a provider: the core provider registered for it.
#[derive(Debug)]
pub struct Entry {
    pub format: String,
    pub kind: Kind,
    /// The core provider's name: the provider's for its first format,
    /// `name:format` for the others.
    pub internal: String,
    pub base_url: String,
    pub auth: HeaderMap,
}

#[derive(Debug)]
pub struct Plan {
    pub upstreams: Vec<Upstream>,
    pub serve: Vec<String>,
    pub routes: Arc<Routes>,
    pub keys: Vec<String>,
}

impl Plan {
    pub fn serves(&self, format: &str) -> bool {
        self.serve.iter().any(|s| s == format)
    }
}

pub fn compose(config: &Config, formats: &[KnownFormat]) -> Result<(Gateway, Plan), Fault> {
    let known = |name: &str, owner: &str| {
        formats.iter().find(|f| f.name == name).ok_or_else(|| {
            let known: Vec<_> = formats.iter().map(|f| f.name.as_str()).collect();
            Fault::config(format!(
                "{owner}: unknown format '{name}'; compiled-in formats: [{}]",
                known.join(", ")
            ))
        })
    };
    let mut gateway = Gateway::new();
    let mut upstreams = Vec::new();
    for (name, p) in &config.providers {
        let upstream = upstream(name, p, formats)?;
        for entry in &upstream.entries {
            let format = known(&entry.format, &format!("provider '{name}'"))?;
            let (internal, auth) = (entry.internal.clone(), entry.auth.clone());
            gateway = format
                .add_provider(gateway, internal, &entry.base_url, auth)
                .map_err(|e| Fault::config(format!("provider '{name}': {}", e.message())))?;
        }
        upstreams.push(upstream);
    }

    let serve = match &config.serve {
        Some(serve) => serve.clone(),
        None => default_serve(&upstreams),
    };
    let after = serve.iter().skip_while(|n| *n != PASSTHROUGH).nth(1);
    if let Some(after) = after {
        return Err(Fault::config(format!(
            "serve: '{after}' comes after '{PASSTHROUGH}', which accepts every request first; \
             serve '{PASSTHROUGH}' last"
        )));
    }
    for name in &serve {
        let served = known(name, "serve")?;
        gateway = served.serve(gateway)?;
        if served.kind.translates {
            continue;
        }
        let speakers: Vec<&str> = upstreams
            .iter()
            .filter(|u| u.entries.iter().any(|e| e.format == *name))
            .map(|u| u.name.as_str())
            .collect();
        if speakers.len() != 1 {
            let mut message = format!("serve: {name} needs exactly one provider of that format");
            if speakers.len() > 1 {
                message.push_str(&format!("; {} all speak it", speakers.join(", ")));
            }
            return Err(Fault::config(message));
        }
    }

    let mut tables = Vec::new();
    for p in ordered(&config.plugins)? {
        let bad = |e: String| Fault::config(format!("plugin '{}': {e}", p.name));
        let Some(models) = &p.models else {
            return Err(bad("no plugin given; compiled-in plugins: [models]".into()));
        };
        for m in models {
            let Some(upstream) = upstreams.iter().find(|u| u.name == m.provider) else {
                return Err(bad(format!("unknown provider '{}'", m.provider)));
            };
            if !upstream.entries.iter().any(|e| e.kind == Kind::CHAT) {
                let formats: Vec<&str> =
                    upstream.entries.iter().map(|e| e.format.as_str()).collect();
                let formats = match formats.as_slice() {
                    [one] => format!("format '{one}'"),
                    many => format!("formats [{}]", many.join(", ")),
                };
                return Err(bad(format!(
                    "provider '{}' uses {formats}, incompatible with conversation models routes",
                    m.provider
                )));
            }
        }
        tables.push(
            models
                .iter()
                .map(|m| (m.prefix.clone(), m.provider.clone()))
                .collect(),
        );
    }

    if let Some(target) = &config.target
        && !upstreams.iter().any(|u| u.name == *target)
    {
        return Err(Fault::config(format!(
            "target: unknown provider '{target}'"
        )));
    }
    let keys = config
        .keys
        .iter()
        .map(|key| secret("keys", key))
        .collect::<Result<_, _>>()?;
    let routes = Arc::new(Routes::new(&upstreams, tables, config.target.clone()));
    let plan = Plan {
        upstreams,
        serve,
        routes,
        keys,
    };
    Ok((gateway, plan))
}

/// The formats served when the config names none: OpenAI Chat, Anthropic
/// Messages, and every format of a kind that does not translate that some
/// provider speaks, but passthrough, which takes every request.
fn default_serve(upstreams: &[Upstream]) -> Vec<String> {
    let mut serve = vec!["openai-chat".to_owned(), "anthropic-messages".to_owned()];
    let others = upstreams.iter().flat_map(|u| &u.entries);
    for entry in others.filter(|e| !e.kind.translates && e.format != PASSTHROUGH) {
        if !serve.contains(&entry.format) {
            serve.push(entry.format.clone());
        }
    }
    serve
}

/// A provider's formats, each with its base URL and credentials.
fn upstream(name: &str, p: &ProviderConfig, formats: &[KnownFormat]) -> Result<Upstream, Fault> {
    let owner = format!("provider '{name}'");
    let bad = |e: String| Fault::config(format!("{owner}: {e}"));
    let preset = match &p.vendor {
        None => None,
        Some(vendor) => Some(presets::preset(vendor).ok_or_else(|| {
            bad(format!(
                "unknown vendor '{vendor}'; presets: [{}]",
                presets::vendors().join(", ")
            ))
        })?),
    };
    let names: Vec<String> = match (&p.format, preset) {
        (Some(names), _) => names.clone(),
        // A preset's formats not compiled in are left out.
        (None, Some(preset)) => preset
            .formats
            .iter()
            .filter(|f| formats.iter().any(|known| known.name == **f))
            .map(|f| (*f).to_owned())
            .collect(),
        (None, None) => return Err(bad("needs a `vendor` or a `format`".into())),
    };
    if names.is_empty() {
        return Err(bad("has no format".into()));
    }
    let base_url = p
        .base_url
        .as_deref()
        .or(preset.map(|preset| preset.base_url))
        .ok_or_else(|| bad("needs a `base_url`".into()))?;
    let key = match (&p.key, &p.auth, preset) {
        (Some(_), Some(_), _) => return Err(bad("set `key` or `auth`, not both".into())),
        (Some(key), None, _) => Some(secret(&owner, key)?),
        (None, None, Some(preset)) if !preset.key_env.is_empty() => {
            let Some(key) = presets::key_from_env(preset.key_env) else {
                let var = preset.key_env.join(" or ");
                return Err(bad(format!("environment variable {var} is not set")));
            };
            Some(secret(&owner, &key)?)
        }
        _ => None,
    };
    // One explicit format sits at its base URL exactly as given.
    let exact = preset.is_none() && names.len() == 1;
    let mut entries = Vec::new();
    for (i, format) in names.iter().enumerate() {
        let Some(known) = formats.iter().find(|f| f.name == *format) else {
            let known: Vec<_> = formats.iter().map(|f| f.name.as_str()).collect();
            return Err(bad(format!(
                "unknown format '{format}'; compiled-in formats: [{}]",
                known.join(", ")
            )));
        };
        let auth = match (&p.auth, &key) {
            (Some(auth), _) => auth_headers(name, auth)?,
            (None, Some(key)) => key_headers(name, known.key, key)?,
            (None, None) => HeaderMap::new(),
        };
        let base_url = match exact {
            true => base_url.to_owned(),
            false => base_for(known, base_url),
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
    })
}

/// Where `format` sits for a provider at `base_url`.
fn base_for(format: &KnownFormat, base_url: &str) -> String {
    let base_url = base_url.trim_end_matches('/');
    let base = match format.base {
        Base::Same => base_url,
        Base::Origin => presets::origin(base_url),
    };
    presets::with_scheme(base, format.websocket)
}

/// The plugins in the order they run: next is the earliest listed plugin
/// whose `after` plugins are all placed. `Err` for an unknown name or a
/// cycle.
pub fn ordered(plugins: &[PluginConfig]) -> Result<Vec<&PluginConfig>, Fault> {
    let index = |name: &str| plugins.iter().position(|p| p.name == name);
    // `after` names a plugin, so a name is one plugin's.
    let mut each = plugins.iter().enumerate();
    if let Some((_, p)) = each.find(|(i, p)| index(&p.name) != Some(*i)) {
        return Err(Fault::config(format!(
            "plugin '{}' is registered twice",
            p.name
        )));
    }
    for p in plugins {
        if let Some(first) = p.after.iter().find(|a| index(a).is_none()) {
            return Err(Fault::config(format!(
                "plugin '{}' runs after unknown plugin '{first}'",
                p.name
            )));
        }
    }
    let mut placed = vec![false; plugins.len()];
    let mut order = Vec::with_capacity(plugins.len());
    while order.len() < plugins.len() {
        let ready = (0..plugins.len()).find(|&i| {
            let after = |a: &String| index(a).is_some_and(|a| placed[a]);
            !placed[i] && plugins[i].after.iter().all(after)
        });
        let Some(i) = ready else {
            let stuck: Vec<_> = (0..plugins.len())
                .filter(|&i| !placed[i])
                .map(|i| format!("'{}'", plugins[i].name))
                .collect();
            return Err(Fault::config(format!(
                "plugins run after each other in a cycle: {}",
                stuck.join(", ")
            )));
        };
        placed[i] = true;
        order.push(&plugins[i]);
    }
    Ok(order)
}

/// `env:NAME` reads the environment variable; anything else is literal.
/// `owner` says whose it is in an error.
pub fn secret(owner: &str, value: &str) -> Result<String, Fault> {
    match value.strip_prefix("env:") {
        Some(var) => std::env::var(var)
            .map_err(|_| Fault::config(format!("{owner}: environment variable {var} is not set"))),
        None => Ok(value.to_owned()),
    }
}

fn key_headers(provider: &str, header: KeyHeader, key: &str) -> Result<HeaderMap, Fault> {
    let auth = match header {
        KeyHeader::Bearer => Auth::Bearer {
            key: key.to_owned(),
        },
        KeyHeader::Header(name) => Auth::Header {
            name: name.to_owned(),
            key: key.to_owned(),
        },
    };
    // The key is already read: it is literal here.
    headers(provider, &auth, |k| Ok(k.to_owned()))
}

pub fn auth_headers(provider: &str, auth: &Auth) -> Result<HeaderMap, Fault> {
    headers(provider, auth, |k| {
        secret(&format!("provider '{provider}'"), k)
    })
}

fn headers(
    provider: &str,
    auth: &Auth,
    read: impl Fn(&str) -> Result<String, Fault>,
) -> Result<HeaderMap, Fault> {
    let bad = |e: String| Fault::config(format!("provider '{provider}': {e}"));
    let (name, value) = match auth {
        Auth::Bearer { key } => (AUTHORIZATION, format!("Bearer {}", read(key)?)),
        Auth::Header { name, key } => (
            HeaderName::from_bytes(name.as_bytes()).map_err(|e| bad(e.to_string()))?,
            read(key)?,
        ),
    };
    let mut value = HeaderValue::from_str(&value).map_err(|e| bad(e.to_string()))?;
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
