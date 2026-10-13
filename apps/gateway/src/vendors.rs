//! Vendors: `vendor = "openai"` is a provider as the vendor is
//! ([`respan_gateway_vendors`]): at its base URL, with its key from the
//! environment, speaking every format it speaks, and reached by its models'
//! names.

use respan_gateway_vendors::{ALL, Vendor};

/// The variable read to override `vendor`'s base URL, such as in tests.
pub fn base_url_env(vendor: &Vendor) -> String {
    format!("RESPAN_{}_BASE_URL", vendor.id.to_ascii_uppercase())
}

pub fn vendors() -> Vec<&'static str> {
    ALL.iter().map(|p| p.id).collect()
}

/// The first of `names` set in the environment, as `env:NAME`.
pub fn key_from_env(names: &[&str]) -> Option<String> {
    let set = |name: &&&str| std::env::var(name).is_ok_and(|v| !v.is_empty());
    names.iter().find(set).map(|name| format!("env:{name}"))
}

/// The vendors the environment has keys for, with a base URL of their own
/// or one the environment gives, and Ollama if `OLLAMA_HOST` is set, each
/// with its key and any base URL override.
pub fn detected() -> Vec<(&'static Vendor, Option<String>, Option<String>)> {
    ALL.iter()
        .copied()
        .filter_map(|vendor| {
            let key = key_from_env(vendor.key_env);
            let base_url = std::env::var(base_url_env(vendor)).ok();
            let ollama = || {
                let host = std::env::var("OLLAMA_HOST").ok()?;
                Some(ollama_base_url(&host))
            };
            let located = vendor.base_url.is_some() || base_url.is_some();
            match (vendor.key_env.is_empty(), key) {
                (false, Some(key)) if located => Some((vendor, Some(key), base_url)),
                (true, _) if vendor.id == "ollama" => {
                    let base_url = base_url.or_else(ollama)?;
                    Some((vendor, None, Some(base_url)))
                }
                _ => None,
            }
        })
        .collect()
}

/// Ollama's OpenAI-compatible base URL from `OLLAMA_HOST`, which may be
/// `host:port` or a URL.
fn ollama_base_url(host: &str) -> String {
    let host = host.trim_end_matches('/');
    match host.contains("://") {
        true => format!("{host}/v1"),
        false => format!("http://{host}/v1"),
    }
}

/// The models routed to a provider of `vendor` by their names alone,
/// without a `provider/` prefix: what those names start with. Of two
/// providers that match, the longer start wins.
pub fn claims(vendor: &str) -> impl Iterator<Item = &'static str> {
    let claims = match vendor {
        "openai" => {
            "gpt o1 o3 o4 chatgpt codex dall-e gpt-image whisper-1 tts- sora text-embedding"
        }
        "anthropic" => "claude",
        "gemini" => "gemini imagen veo",
        "groq" => "whisper-large distil-whisper playai-tts canopylabs/orpheus",
        "deepseek" => "deepseek",
        "openrouter" => {
            "openai/ anthropic/ google/ meta-llama/ mistralai/ qwen/ deepseek/ x-ai/ \
             moonshotai/ cohere/ nvidia/ microsoft/ amazon/ perplexity/"
        }
        "xai" => "grok",
        "mistral" => "mistral codestral magistral pixtral",
        "together" => {
            "meta-llama/ mistralai/ Qwen/ deepseek-ai/ google/ moonshotai/ zai-org/ \
             black-forest-labs/"
        }
        "fireworks" => "accounts/",
        _ => "",
    };
    claims.split_whitespace()
}

/// `scheme://host[:port]` of `url`.
pub fn origin(url: &str) -> &str {
    let after_scheme = url.find("://").map_or(0, |i| i + 3);
    match url[after_scheme..].find('/') {
        Some(slash) => &url[..after_scheme + slash],
        None => url,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls_lose_their_path() {
        assert_eq!(origin("https://a.test:1/v1beta/openai"), "https://a.test:1");
        assert_eq!(origin("https://a.test"), "https://a.test");
        assert_eq!(
            ollama_base_url("127.0.0.1:11434"),
            "http://127.0.0.1:11434/v1"
        );
        assert_eq!(ollama_base_url("http://box:1/"), "http://box:1/v1");
    }
}
