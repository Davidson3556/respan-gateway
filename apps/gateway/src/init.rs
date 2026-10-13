//! `respan-gateway init`: a commented starter `gateway.yaml`, with a
//! provider for each vendor whose key is in the environment, or that was
//! asked for.

use crate::presets;
use respan_gateway_providers::{ALL, Provider};

/// The vendors the environment has keys for, by name.
pub fn detected_vendors() -> Vec<&'static str> {
    presets::detected().iter().map(|(p, ..)| p.id).collect()
}

/// A vendor `init` was asked to enable, and the key its entry names.
#[derive(Debug, Clone)]
pub struct Chosen {
    pub vendor: &'static Provider,
    /// `env:NAME`, or the literal key the user gave; `None` for a vendor
    /// that needs no key.
    pub key: Option<String>,
}

impl Chosen {
    /// `VENDOR`, whose key is read from its first variable, or `VENDOR=KEY`,
    /// with `KEY` as `env:NAME` or a literal.
    pub fn parse(spec: &str) -> Result<Self, String> {
        let (name, key) = match spec.split_once('=') {
            Some((name, key)) => (name, Some(key)),
            None => (spec, None),
        };
        let vendor = ALL.iter().copied().find(|p| p.id == name);
        let vendor = vendor.ok_or_else(|| {
            let known = presets::vendors().join(", ");
            format!("unknown vendor {name}; the vendors are {known}")
        })?;
        let key = match (key, vendor.key_env.first()) {
            (Some(""), _) => return Err(format!("{name}= names no key")),
            (Some(key), _) => Some(key.to_owned()),
            (None, Some(var)) => Some(format!("env:{var}")),
            (None, None) => None,
        };
        Ok(Self { vendor, key })
    }

    /// Whether the key is written into the file, not read from a variable.
    pub fn is_literal(&self) -> bool {
        self.key.as_deref().is_some_and(|k| !k.starts_with("env:"))
    }

    /// The variable the key is read from, if it is `env:NAME`.
    pub fn variable(&self) -> Option<&str> {
        self.key.as_deref()?.strip_prefix("env:")
    }
}

/// The starter config from the environment alone.
pub fn starter_config() -> String {
    starter(&[], None)
}

/// A new client key: `sk-gw-` and 32 url-safe characters (192 bits) from
/// the system's CSPRNG.
pub fn new_client_key() -> Result<String, getrandom::Error> {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes)?;
    // 64 divides 256, so masking a byte to 6 bits is unbiased.
    let random: String = bytes
        .iter()
        .map(|b| char::from(ALPHABET[usize::from(b & 63)]))
        .collect();
    Ok(format!("sk-gw-{random}"))
}

/// The starter config: the vendors with keys in the environment, and those
/// `chosen`, enabled, with any base URL the environment moves them to
/// (`OLLAMA_HOST`, `RESPAN_<VENDOR>_BASE_URL`), the rest commented out;
/// OpenAI enabled if there are none. `client_key` is written into
/// `client_keys:`, else a hint is. A chosen vendor's entry names its key.
pub fn starter(chosen: &[Chosen], client_key: Option<&str>) -> String {
    let detected = presets::detected();
    let found = |vendor: &str| detected.iter().find(|(p, ..)| p.id == vendor);
    let picked = |vendor: &str| chosen.iter().find(|c| c.vendor.id == vendor);
    let enabled = |vendor: &str| match detected.is_empty() && chosen.is_empty() {
        true => vendor == "openai",
        false => found(vendor).is_some() || picked(vendor).is_some(),
    };
    let client_keys = match client_key {
        Some(key) => format!("client_keys: ['{}']", key.replace('\'', "''")),
        None => {
            "# client_keys: [env:GATEWAY_KEY]   # or `respan-gateway init --client-key`".to_owned()
        }
    };
    let mut providers = String::new();
    for vendor in ALL {
        let key = match vendor.key_env {
            [] => "no key".to_owned(),
            names => format!("key: env:{}", names.join(" or env:")),
        };
        let mark = if enabled(vendor.id) { "" } else { "# " };
        let base_url = found(vendor.id).and_then(|(.., base_url)| base_url.as_deref());
        // A vendor with no base URL of its own needs one.
        let base_url = base_url.or(vendor.base_url.is_none().then_some("<base URL>"));
        let base_url = base_url.map_or(String::new(), |url| {
            format!(", base_url: '{}'", url.replace('\'', "''"))
        });
        let named = picked(vendor.id).and_then(|c| c.key.as_deref());
        let named = named.map_or(String::new(), |k| {
            format!(", key: '{}'", k.replace('\'', "''"))
        });
        let entry = format!(
            "{}: {{ vendor: {}{base_url}{named} }}",
            vendor.id, vendor.id
        );
        providers.push_str(&format!("  {mark}{entry:<36} # {key}\n"));
    }
    format!(
        "# respan-gateway configuration. Check it with `respan-gateway check`,
# run it with `respan-gateway`. Every setting: gateway.example.yaml.

listen: 127.0.0.1:8080

# Client keys: when set, every request needs one (Authorization: Bearer,
# x-api-key or x-goog-api-key). Without them, anyone who can reach the
# gateway spends your provider keys: keep it local (the gateway refuses to
# listen beyond loopback without one).
{client_keys}

# Providers, by name. A vendor preset brings its base URL, its key from the
# environment, every format the gateway speaks for it, and its model names
# (gpt-* to openai, claude-* to anthropic, ...). Any provider is also
# reached as `<name>/<model>`, such as `groq/llama-3.3-70b-versatile`.
# Override a preset's `base_url` or `key` in place (with a config file,
# RESPAN_<VENDOR>_BASE_URL is not read). The first provider
# serving a kind (chat, realtime, ...) is that kind's default.
providers:
{providers}
# Plugins, each under its name. Routing sends model names to providers,
# before the presets' own names, and a default for requests nothing else
# routes; fallback tries other providers when one refuses:
# plugins:
#   routing:
#     models:
#       - {{ prefix: llama, provider: groq }}
#     default: openai
#   fallback:
#     after: {{ openai: [openai-eu] }}

# Print each request to stderr as text or json (or off); serve the web UI
# at /ui/.
log: text
ui: true
"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ClientKey;

    fn chosen(specs: &[&str]) -> Vec<Chosen> {
        let all: Result<Vec<_>, _> = specs.iter().map(|s| Chosen::parse(s)).collect();
        all.unwrap_or_else(|e| panic!("{e}"))
    }

    #[test]
    fn the_starter_config_parses() {
        let text = starter_config();
        let config = crate::Config::parse(&text).unwrap_or_else(|e| panic!("{e}\n{text}"));
        assert!(!config.providers.is_empty(), "{text}");
        assert!(text.contains("vendor: anthropic"), "{text}");
    }

    #[test]
    fn a_chosen_vendor_names_its_key_by_variable_or_literal() {
        let picks = chosen(&["groq", "mistral=env:MY_MISTRAL", "openai=sk-'odd"]);
        assert_eq!(picks[0].variable(), Some("GROQ_API_KEY"));
        assert!(!picks[0].is_literal() && picks[2].is_literal());
        let text = starter(&picks, None);
        let config = crate::Config::parse(&text).unwrap_or_else(|e| panic!("{e}\n{text}"));
        assert!(config.providers.len() >= 3, "{text}");
        for vendor in ["groq", "mistral", "openai"] {
            assert!(text.contains(&format!("\n  {vendor}: {{")), "{text}");
        }
        assert!(
            text.contains("vendor: groq, key: 'env:GROQ_API_KEY'"),
            "{text}"
        );
        assert!(text.contains("key: 'env:MY_MISTRAL'"), "{text}");
        assert!(text.contains("key: 'sk-''odd'"), "{text}");
        assert!(text.contains("  # anthropic:"), "{text}");
    }

    #[test]
    fn a_vendor_that_needs_no_key_or_does_not_exist() {
        assert_eq!(chosen(&["ollama"])[0].key, None);
        let e = Chosen::parse("nope").err().unwrap_or_default();
        assert!(
            e.starts_with("unknown vendor nope; the vendors are "),
            "{e}"
        );
        assert!(Chosen::parse("groq=").is_err());
    }

    #[test]
    fn a_client_key_is_long_random_and_written_into_the_config() {
        let key = || new_client_key().unwrap_or_else(|e| panic!("{e}"));
        let (a, b) = (key(), key());
        assert_ne!(a, b);
        let random = a.strip_prefix("sk-gw-").unwrap_or_default();
        assert_eq!(random.len(), 32, "{a}");
        assert!(
            random
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || "-_".contains(c))
        );
        let text = starter(&[], Some(&a));
        let config = crate::Config::parse(&text).unwrap_or_else(|e| panic!("{e}\n{text}"));
        assert_eq!(config.client_keys, [ClientKey::Key(a)]);
        let hint = crate::Config::parse(&starter(&[], None)).unwrap_or_else(|e| panic!("{e}"));
        assert!(hint.client_keys.is_empty());
    }
}
