//! `respan-gateway init`: a commented starter `gateway.yaml`, with a
//! provider for each vendor whose key is in the environment.

use crate::presets::{self, PRESETS};

/// The starter config: the vendors with keys in the environment enabled,
/// with any base URL the environment moves them to (`OLLAMA_HOST`,
/// `RESPAN_<VENDOR>_BASE_URL`), the rest commented out; OpenAI enabled if
/// none has one.
pub fn starter_config() -> String {
    let detected = presets::detected();
    let found = |vendor: &str| detected.iter().find(|(p, ..)| p.vendor == vendor);
    let enabled = |vendor: &str| match detected.is_empty() {
        true => vendor == "openai",
        false => found(vendor).is_some(),
    };
    let mut providers = String::new();
    for preset in PRESETS {
        let key = match preset.key_env {
            [] => "no key".to_owned(),
            names => format!("key: env:{}", names.join(" or env:")),
        };
        let mark = if enabled(preset.vendor) { "" } else { "# " };
        let base_url = found(preset.vendor).and_then(|(.., base_url)| base_url.as_ref());
        let base_url = base_url.map_or(String::new(), |url| {
            format!(", base_url: '{}'", url.replace('\'', "''"))
        });
        let entry = format!(
            "{}: {{ vendor: {}{base_url} }}",
            preset.vendor, preset.vendor
        );
        providers.push_str(&format!("  {mark}{entry:<36} # {key}\n"));
    }
    format!(
        "# respan-gateway configuration. Check it with `respan-gateway check`,
# run it with `respan-gateway`. Every setting: gateway.example.yaml.

listen: 127.0.0.1:8080

# Client keys: when set, every request needs one (Authorization: Bearer,
# x-api-key or x-goog-api-key). Without them, anyone who can reach the
# gateway spends your provider keys: keep it local.
# keys: [env:GATEWAY_KEY]

# Providers, by name. A vendor preset brings its base URL, its key from the
# environment, every format the gateway speaks for it, and its model names
# (gpt-* to openai, claude-* to anthropic, ...). Any provider is also
# reached as `<name>/<model>`, such as `groq/llama-3.3-70b-versatile`.
# Override a preset's `base_url` or `key` in place (with a config file,
# RESPAN_<VENDOR>_BASE_URL is not read). The first provider
# serving a kind (chat, realtime, ...) is that kind's default.
providers:
{providers}
# Route model names to providers, before the presets' own names:
# plugins:
#   - name: models
#     models:
#       - {{ prefix: llama, provider: groq }}

# The default provider for requests nothing else routes:
# target: openai

# Print each request to stderr; serve the web UI at /ui/.
log: true
ui: true
"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_starter_config_parses() {
        let text = starter_config();
        let config = crate::Config::parse(&text).unwrap_or_else(|e| panic!("{e}\n{text}"));
        assert!(!config.providers.is_empty(), "{text}");
        assert!(text.contains("vendor: anthropic"), "{text}");
    }
}
