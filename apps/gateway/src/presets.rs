//! Vendor presets: `{ vendor: openai }` is a provider as the vendor is
//! ([`respan_gateway_providers`]): at its base URL, with its key from the
//! environment, speaking every format it speaks, and reached by its models'
//! names.

use respan_gateway_providers::{ALL, Provider};

/// The variable read to override `vendor`'s base URL, such as in tests.
pub fn base_url_env(vendor: &Provider) -> String {
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
pub fn detected() -> Vec<(&'static Provider, Option<String>, Option<String>)> {
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

/// `url` with `ws`/`wss` for `http`/`https`, or the reverse.
pub fn with_scheme(url: &str, websocket: bool) -> String {
    let swapped = match websocket {
        true => [("https://", "wss://"), ("http://", "ws://")],
        false => [("wss://", "https://"), ("ws://", "http://")],
    };
    for (from, to) in swapped {
        if let Some(rest) = url.strip_prefix(from) {
            return format!("{to}{rest}");
        }
    }
    url.to_owned()
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
    fn urls_change_scheme_and_lose_their_path() {
        assert_eq!(with_scheme("https://a.test/v1", true), "wss://a.test/v1");
        assert_eq!(with_scheme("http://a.test", true), "ws://a.test");
        assert_eq!(with_scheme("wss://a.test", false), "https://a.test");
        assert_eq!(with_scheme("wss://a.test", true), "wss://a.test");
        assert_eq!(origin("https://a.test:1/v1beta/openai"), "https://a.test:1");
        assert_eq!(origin("https://a.test"), "https://a.test");
        assert_eq!(
            ollama_base_url("127.0.0.1:11434"),
            "http://127.0.0.1:11434/v1"
        );
        assert_eq!(ollama_base_url("http://box:1/"), "http://box:1/v1");
    }
}
