//! Vendor presets: `{ vendor: openai }` is a provider at the vendor's base
//! URL, with its key from the environment, speaking every format of the
//! vendor this binary has compiled in, and reached by its models' names.

/// One vendor's provider.
#[derive(Debug)]
pub struct Preset {
    pub vendor: &'static str,
    pub base_url: &'static str,
    /// Its formats, by name. A name not compiled in is skipped, so a
    /// format added later is one name here.
    pub formats: &'static [&'static str],
    /// The environment variables its key is read from, the first set; none
    /// for a vendor that needs no key.
    pub key_env: &'static [&'static str],
    /// Model names routed to it without a `provider/` prefix.
    pub prefixes: &'static [&'static str],
    /// A model to show in examples.
    pub example_model: &'static str,
}

impl Preset {
    /// The variable read to override its base URL, such as in tests.
    pub fn base_url_env(&self) -> String {
        format!("RESPAN_{}_BASE_URL", self.vendor.to_ascii_uppercase())
    }
}

pub const PRESETS: &[Preset] = &[
    Preset {
        vendor: "openai",
        base_url: "https://api.openai.com/v1",
        formats: &[
            "openai-chat",
            "openai-realtime",
            "responses-ws",
            "openai-images",
            "openai-transcriptions",
            "openai-speech",
            "openai-videos",
        ],
        key_env: &["OPENAI_API_KEY"],
        prefixes: &[
            "gpt",
            "o1",
            "o3",
            "o4",
            "chatgpt",
            "codex",
            "dall-e",
            "gpt-image",
            "whisper-1",
            "tts-",
            "sora",
            "text-embedding",
        ],
        example_model: "gpt-4o-mini",
    },
    Preset {
        vendor: "anthropic",
        base_url: "https://api.anthropic.com",
        formats: &["anthropic-messages"],
        key_env: &["ANTHROPIC_API_KEY"],
        prefixes: &["claude"],
        example_model: "claude-sonnet-4-5",
    },
    Preset {
        vendor: "gemini",
        base_url: "https://generativelanguage.googleapis.com/v1beta/openai",
        formats: &["openai-chat", "gemini-live", "openai-images", "gemini-veo"],
        key_env: &["GEMINI_API_KEY", "GOOGLE_API_KEY"],
        prefixes: &["gemini", "imagen", "veo"],
        example_model: "gemini-2.5-flash",
    },
    Preset {
        vendor: "groq",
        base_url: "https://api.groq.com/openai/v1",
        formats: &["groq-chat", "openai-transcriptions", "openai-speech"],
        key_env: &["GROQ_API_KEY"],
        prefixes: &[
            "whisper-large",
            "distil-whisper",
            "playai-tts",
            "canopylabs/orpheus",
        ],
        example_model: "llama-3.3-70b-versatile",
    },
    Preset {
        vendor: "deepseek",
        base_url: "https://api.deepseek.com",
        formats: &["deepseek-chat"],
        key_env: &["DEEPSEEK_API_KEY"],
        prefixes: &["deepseek"],
        example_model: "deepseek-chat",
    },
    Preset {
        vendor: "openrouter",
        base_url: "https://openrouter.ai/api/v1",
        formats: &["openrouter-chat"],
        key_env: &["OPENROUTER_API_KEY"],
        prefixes: &[],
        example_model: "openai/gpt-4o-mini",
    },
    Preset {
        vendor: "xai",
        base_url: "https://api.x.ai/v1",
        formats: &["openai-chat", "openai-images"],
        key_env: &["XAI_API_KEY"],
        prefixes: &["grok"],
        example_model: "grok-4",
    },
    Preset {
        vendor: "mistral",
        base_url: "https://api.mistral.ai/v1",
        formats: &["openai-chat"],
        key_env: &["MISTRAL_API_KEY"],
        prefixes: &["mistral", "codestral", "magistral", "pixtral"],
        example_model: "mistral-small-latest",
    },
    Preset {
        vendor: "together",
        base_url: "https://api.together.xyz/v1",
        formats: &["openai-chat", "openai-images"],
        key_env: &["TOGETHER_API_KEY"],
        prefixes: &[],
        example_model: "meta-llama/Llama-3.3-70B-Instruct-Turbo",
    },
    Preset {
        vendor: "fireworks",
        base_url: "https://api.fireworks.ai/inference/v1",
        formats: &["openai-chat"],
        key_env: &["FIREWORKS_API_KEY"],
        prefixes: &[],
        example_model: "accounts/fireworks/models/llama-v3p3-70b-instruct",
    },
    Preset {
        vendor: "ollama",
        base_url: "http://localhost:11434/v1",
        formats: &["openai-chat"],
        key_env: &[],
        prefixes: &[],
        example_model: "llama3.2",
    },
];

pub fn preset(vendor: &str) -> Option<&'static Preset> {
    PRESETS.iter().find(|p| p.vendor == vendor)
}

pub fn vendors() -> Vec<&'static str> {
    PRESETS.iter().map(|p| p.vendor).collect()
}

/// The first of `names` set in the environment, as `env:NAME`.
pub fn key_from_env(names: &[&str]) -> Option<String> {
    let set = |name: &&&str| std::env::var(name).is_ok_and(|v| !v.is_empty());
    names.iter().find(set).map(|name| format!("env:{name}"))
}

/// The presets the environment has keys for, Ollama if `OLLAMA_HOST` is
/// set, each with its key and any base URL override.
pub fn detected() -> Vec<(&'static Preset, Option<String>, Option<String>)> {
    PRESETS
        .iter()
        .filter_map(|preset| {
            let key = key_from_env(preset.key_env);
            let base_url = std::env::var(preset.base_url_env()).ok();
            let ollama = || {
                let host = std::env::var("OLLAMA_HOST").ok()?;
                Some(ollama_base_url(&host))
            };
            match (preset.key_env.is_empty(), key) {
                (false, Some(key)) => Some((preset, Some(key), base_url)),
                (true, _) if preset.vendor == "ollama" => {
                    let base_url = base_url.or_else(ollama)?;
                    Some((preset, None, Some(base_url)))
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

    #[test]
    fn every_preset_has_a_unique_vendor() {
        let mut vendors = vendors();
        vendors.sort_unstable();
        vendors.dedup();
        assert_eq!(vendors.len(), PRESETS.len());
    }
}
