//! `respan-gateway init`: a commented starter `gateway.toml`, with a
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
    use base64::Engine;
    let mut bytes = [0u8; 24];
    getrandom::fill(&mut bytes)?;
    let random = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
    Ok(format!("sk-gw-{random}"))
}

/// The starter config: the vendors with keys in the environment, and those
/// `chosen`, enabled, with any base URL the environment moves them to
/// (`OLLAMA_HOST`, `RESPAN_<VENDOR>_BASE_URL`);
/// OpenAI enabled if there are none. `client_key` is written into
/// `client_keys`, else a hint is. A chosen vendor's entry names its key.
pub fn starter(chosen: &[Chosen], client_key: Option<&str>) -> String {
    let detected = presets::detected();
    let quote = |text: &str| toml::Value::String(text.to_owned()).to_string();
    let mut text = String::from(
        "# Respan Gateway\n\
         # Check: respan-gateway check    Run: respan-gateway\n\
         # More options: gateway.example.toml\n\n\
         # Server settings (keep these above all [tables]).\n\
         listen = \"127.0.0.1:8080\"\n\
         log = \"text\"                 # text | json | off\n\
         ui = true                    # Dashboard at /ui/\n\n\
         # Require a client key before listening beyond localhost.\n",
    );
    match client_key {
        Some(key) => text.push_str(&format!("client_keys = [{}]\n", quote(key))),
        None => text.push_str("# client_keys = [\"env:GATEWAY_KEY\"]\n"),
    }
    text.push_str("\n# Providers: presets supply URLs, formats, and model routing.\n");
    for vendor in ALL {
        let found = detected.iter().find(|(p, ..)| p.id == vendor.id);
        let picked = chosen.iter().find(|c| c.vendor.id == vendor.id);
        let enabled = found.is_some()
            || picked.is_some()
            || (detected.is_empty() && chosen.is_empty() && vendor.id == "openai");
        if !enabled {
            continue;
        }
        text.push_str(&format!(
            "\n[providers.{}]\nvendor = {}\n",
            vendor.id,
            quote(vendor.id)
        ));
        let key = picked
            .and_then(|c| c.key.clone())
            .or_else(|| found.and_then(|(_, key, _)| key.clone()))
            .or_else(|| vendor.key_env.first().map(|var| format!("env:{var}")));
        if let Some(key) = key {
            text.push_str(&format!("key = {}\n", quote(&key)));
        }
        let base_url = found
            .and_then(|(.., url)| url.as_deref())
            .or(vendor.base_url.is_none().then_some("<base URL>"));
        if let Some(url) = base_url {
            text.push_str(&format!("base_url = {}\n", quote(url)));
        }
    }
    text.push_str("\n# Add providers and optional plugins using gateway.example.toml.\n");
    text
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
        let text = starter(&[], None);
        let config = crate::Config::parse(&text).unwrap_or_else(|e| panic!("{e}\n{text}"));
        assert!(!config.providers.is_empty(), "{text}");
        assert!(text.contains("[providers."), "{text}");
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
            assert!(text.contains(&format!("[providers.{vendor}]")), "{text}");
        }
        assert_eq!(
            config.providers["groq"].key.as_deref(),
            Some("env:GROQ_API_KEY")
        );
        assert_eq!(
            config.providers["mistral"].key.as_deref(),
            Some("env:MY_MISTRAL")
        );
        assert_eq!(config.providers["openai"].key.as_deref(), Some("sk-'odd"));
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
