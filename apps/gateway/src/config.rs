//! The configuration: where it comes from (a YAML file, the environment or
//! `proxy`) and what it says. [`App::new`](crate::App::new) checks and
//! composes it.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use indexmap::IndexMap;
use serde::{Deserialize, Deserializer};

use crate::Error;
use crate::logging::LogFormat;
use crate::mcp::McpConfig;
use crate::mcp_aggregate::AggregateConfig;
use crate::presets;

/// A gateway's configuration, as `gateway.yaml` writes it.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Where to listen, as `host:port` (a name is resolved), `:port` or a
    /// bare port (on 127.0.0.1); `127.0.0.1:8080` if unset.
    #[serde(default, deserialize_with = "listen_address")]
    pub listen: Option<SocketAddr>,
    /// The most bytes of a client's request, and of a provider's answer
    /// held at once, past which the request fails 413 or the answer 502; a
    /// number of bytes or a size such as `32MiB`; 32 MiB if unset.
    #[serde(default, deserialize_with = "body_limit")]
    pub body_limit: Option<usize>,
    /// The largest WebSocket message of a session, from the client or the
    /// provider, past which the session ends (close 1009); a number of
    /// bytes or a size such as `16MiB`; 16 MiB if unset.
    #[serde(default, deserialize_with = "ws_message_limit")]
    pub ws_message_limit: Option<usize>,
    /// How many finished requests the in-memory request history (the web
    /// UI's) keeps; 1000 if unset.
    #[serde(default, deserialize_with = "request_history")]
    pub request_history: Option<usize>,
    /// A file each finished request is appended to as one JSON line (the
    /// request history's records: no bodies, no keys), and the last
    /// `request_history` of which are read back at start; none if unset.
    pub request_history_file: Option<PathBuf>,
    /// The size at which that file moves to `<file>.1` (replacing the one
    /// there) and a new one starts; a number of bytes or a size such as
    /// `64MiB`; 64 MiB if unset.
    #[serde(default, deserialize_with = "request_history_file_max")]
    pub request_history_file_max: Option<usize>,
    /// What models cost, by model prefix (the longest matching wins), in
    /// dollars per million tokens. Set, each request in the history gets a
    /// `cost_usd`, and the overview's totals the sum; there is no built-in
    /// price list.
    #[serde(default, deserialize_with = "prices")]
    pub prices: IndexMap<String, Price>,
    /// How long open responses and end hooks get, together, after a stop
    /// signal, before they are cut: seconds, or a duration such as `30s`,
    /// `2m` or `500ms`; 30 s if unset.
    #[serde(default, deserialize_with = "shutdown_grace")]
    pub shutdown_grace: Option<Duration>,
    /// Client keys, literal or `env:NAME`. When set, every request needs
    /// one; it is consumed by the gateway and never sent to a provider.
    #[serde(default)]
    pub client_keys: Vec<String>,
    /// How each finished request is printed to stderr: `text`, `json` or
    /// `off`.
    #[serde(default)]
    pub log: LogFormat,
    /// Whether the web UI and its admin API are served under `/ui/`.
    #[serde(default = "enabled")]
    pub ui: bool,
    /// Providers by name, in order: the first serving a kind is that
    /// kind's default.
    #[serde(default)]
    pub providers: IndexMap<String, ProviderConfig>,
    /// The formats served to clients, by name, tried in order. Unset:
    /// `openai-chat`, `anthropic-messages`, and every format of another
    /// kind some provider speaks, except `passthrough`. Serve
    /// `passthrough` last to relay every other request unread to the
    /// provider of format `passthrough`, whose `base_url` is then the
    /// vendor's origin. Clients of a format no other translates,
    /// `passthrough` or a session's, go to the one provider of that format.
    pub formats: Option<Vec<String>>,
    /// MCP servers by name, each served to clients at `/mcp/{name}`: a
    /// streamable-HTTP `url`, or a stdio `command`. Name `mcp` in `formats`
    /// if that lists formats.
    #[serde(default)]
    pub mcp: IndexMap<String, McpConfig>,
    /// One MCP endpoint, `/mcp`, that fronts the `mcp:` servers as a single
    /// server (tools and prompts as `{server}__{name}`); off if unset.
    pub mcp_aggregate: Option<AggregateConfig>,
    /// Which web pages may call the gateway from a browser; none if unset.
    pub cors: Option<CorsConfig>,
    /// Compiled-in plugins, in the order listed.
    #[serde(default)]
    pub plugins: Vec<PluginConfig>,
    /// Run tools of `mcp:` servers inside chat requests; off if unset.
    pub agent: Option<AgentConfig>,
    /// The provider of each kind it serves for requests nothing else
    /// routes.
    pub default: Option<String>,
    #[serde(skip)]
    pub(crate) source: Source,
    /// Relay each request raw, with the client's own credentials (`proxy`).
    #[serde(skip)]
    pub(crate) raw_relay: bool,
}

fn enabled() -> bool {
    true
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen: None,
            body_limit: None,
            ws_message_limit: None,
            request_history: None,
            request_history_file: None,
            request_history_file_max: None,
            prices: IndexMap::new(),
            shutdown_grace: None,
            client_keys: Vec::new(),
            log: LogFormat::Text,
            ui: true,
            providers: IndexMap::new(),
            formats: None,
            mcp: IndexMap::new(),
            mcp_aggregate: None,
            plugins: Vec::new(),
            cors: None,
            agent: None,
            default: None,
            source: Source::Inline,
            raw_relay: false,
        }
    }
}

/// One provider: a vendor preset, explicit formats, or a preset with some
/// of its settings replaced.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderConfig {
    /// A vendor preset: `openai`, `anthropic`, `gemini`, `groq`, ...
    pub vendor: Option<String>,
    /// Its format, or several; a preset's are every format of the vendor
    /// compiled in.
    #[serde(default, deserialize_with = "one_or_many")]
    pub formats: Option<Vec<String>>,
    /// The base URL its SDK takes; a preset's is the vendor's.
    pub base_url: Option<String>,
    /// Its key, literal or `env:NAME`, sent as its formats expect; a
    /// preset's is `env:<VENDOR>_API_KEY`.
    pub key: Option<String>,
    /// Its credentials as one header, in place of `key`. A provider with
    /// neither, and no preset key, passes on the client's.
    pub auth: Option<Auth>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Auth {
    /// `Authorization: Bearer <key>`
    Bearer { key: String },
    /// `<name>: <key>`
    Header { name: String, key: String },
}

/// One plugin, by the one field that configures it.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginConfig {
    /// Model routes: model prefix to provider, tried in order, for every
    /// kind the provider serves.
    pub models: Vec<ModelRoute>,
}

/// Browser access: the origins whose pages may call the gateway.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CorsConfig {
    /// Origins such as `https://app.example.com` (scheme, host and port
    /// alone), or `*` for any, which needs `client_keys`. Their preflight
    /// requests are answered without a key; their other requests need the
    /// gateway's usual one.
    pub origins: Vec<String>,
}

/// The agent loop: the gateway offers the tools of `mcp:` servers to the
/// model and runs the ones it calls, so a client that knows nothing of MCP
/// gets answers that used them.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentConfig {
    /// The `mcp:` servers whose tools the gateway offers and runs, as
    /// `<server>__<tool>`.
    pub servers: Vec<String>,
    /// Which of their tools are offered, as the model names them
    /// (`<server>__<tool>`), `*` standing for any characters, such as
    /// `github__search_*`; all of them if unset or empty.
    #[serde(default)]
    pub allow: Vec<String>,
    /// Tools never offered, named as `allow` does; it wins over `allow`.
    #[serde(default)]
    pub deny: Vec<String>,
    /// Model name prefixes the loop applies to, as the provider is sent
    /// them; every chat model if unset.
    #[serde(default)]
    pub models: Vec<String>,
    /// The most calls to the model for one request; 8 if unset.
    pub max_steps: Option<u32>,
    /// The wall-clock limit of one request's loop; 5 minutes if unset.
    #[serde(default, deserialize_with = "agent_timeout")]
    pub timeout: Option<Duration>,
    /// The limit of one tool call; 60 s if unset.
    #[serde(default, deserialize_with = "agent_tool_timeout")]
    pub tool_timeout: Option<Duration>,
    /// How long a server's tool list is kept; 5 minutes if unset.
    #[serde(default, deserialize_with = "agent_tool_list_ttl")]
    pub tool_list_ttl: Option<Duration>,
    /// The most tool calls of one step that run at once; 4 if unset.
    pub parallel: Option<usize>,
    /// The most bytes of one tool's answer the model sees; 64 KiB if unset.
    /// A tool's output is untrusted input to the model.
    #[serde(default, deserialize_with = "agent_max_result")]
    pub max_result: Option<usize>,
    /// The most bytes of tool answers the model sees in one request, over
    /// all its steps; 256 KiB if unset.
    #[serde(default, deserialize_with = "agent_max_results")]
    pub max_results: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelRoute {
    pub prefix: String,
    pub provider: String,
}

/// Where a configuration came from.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum Source {
    File(PathBuf),
    Environment,
    Proxy,
    #[default]
    Inline,
}

impl std::fmt::Display for Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Source::File(path) => write!(f, "{}", path.display()),
            Source::Environment => f.write_str("environment"),
            Source::Proxy => f.write_str("proxy"),
            Source::Inline => f.write_str("inline"),
        }
    }
}

/// The vendor `proxy` relays to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProxyVendor {
    Anthropic,
    OpenAi,
}

/// The file read when none is named.
pub const DEFAULT_FILE: &str = "gateway.yaml";

impl Config {
    /// A configuration from YAML text.
    pub fn parse(yaml: &str) -> Result<Config, Error> {
        serde_norway::from_str(yaml).map_err(|e| Error::new(explain(&e.to_string())))
    }

    /// The configuration in the file at `path`.
    pub fn load(path: impl AsRef<Path>) -> Result<Config, Error> {
        let path = path.as_ref();
        let in_file = |e: String| Error::new(format!("{}: {e}", path.display()));
        let text = std::fs::read_to_string(path).map_err(|e| in_file(e.to_string()))?;
        let mut config = Config::parse(&text).map_err(|e| in_file(e.to_string()))?;
        config.source = Source::File(path.to_owned());
        Ok(config)
    }

    /// A configuration with no file: a provider for every vendor whose key
    /// is in the environment (`OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, ...;
    /// Ollama if `OLLAMA_HOST` is set). `RESPAN_<VENDOR>_BASE_URL` moves a
    /// vendor's base URL. `Err` if there is none.
    pub fn from_env() -> Result<Config, Error> {
        let detected = presets::detected();
        if detected.is_empty() {
            return Err(Error::new(no_keys_message()));
        }
        let providers = detected
            .into_iter()
            .map(|(preset, key, base_url)| {
                let provider = ProviderConfig {
                    vendor: Some(preset.id.to_owned()),
                    key,
                    base_url,
                    ..ProviderConfig::default()
                };
                (preset.id.to_owned(), provider)
            })
            .collect();
        Ok(Config {
            providers,
            source: Source::Environment,
            ..Config::default()
        })
    }

    /// `path` if given, else `./gateway.yaml` if there is one, else
    /// [`from_env`](Self::from_env).
    pub fn discover(path: Option<&Path>) -> Result<Config, Error> {
        match path {
            Some(path) => Config::load(path),
            None if Path::new(DEFAULT_FILE).exists() => Config::load(DEFAULT_FILE),
            None => Config::from_env(),
        }
    }

    /// The configuration of `proxy`: one vendor at `url` (its public API if
    /// `None`), no keys of its own, its chat format and every other
    /// request relayed raw, byte for byte, with each client's credentials.
    pub fn proxy(vendor: ProxyVendor, url: Option<&str>) -> Config {
        let (name, format, default) = match vendor {
            ProxyVendor::Anthropic => (
                "anthropic",
                "anthropic-messages",
                "https://api.anthropic.com",
            ),
            ProxyVendor::OpenAi => ("openai", "openai-chat", "https://api.openai.com/v1"),
        };
        let url = url.unwrap_or(default).trim_end_matches('/');
        // Every call is a raw relay of the client's own path, so both
        // formats sit at the vendor's origin.
        let origin = url.strip_suffix("/v1").unwrap_or(url);
        let provider = ProviderConfig {
            formats: Some(vec![format.to_owned(), "passthrough".to_owned()]),
            base_url: Some(origin.to_owned()),
            ..ProviderConfig::default()
        };
        Config {
            providers: IndexMap::from([(name.to_owned(), provider)]),
            formats: Some(vec![format.to_owned(), "passthrough".to_owned()]),
            source: Source::Proxy,
            raw_relay: true,
            ..Config::default()
        }
    }

    /// Where it came from.
    pub fn source(&self) -> &Source {
        &self.source
    }
}

/// Keys that were renamed, with their new names; `agent.` ones are named
/// under their section.
const RENAMED: [(&str, &str); 3] = [
    ("request_log", "request_history"),
    ("tools", "agent.servers"),
    ("cache", "agent.tool_list_ttl"),
];

/// A serde error for a person: no value of the config in it, which may be a
/// secret (`invalid type: string "sk-..."`), and a hint where a key was
/// renamed.
fn explain(message: &str) -> String {
    let mut said = redacted(message);
    for (old, new) in RENAMED {
        if said.contains(&format!("unknown field `{old}`")) {
            said.push_str(&format!("\n(`{old}` is now `{new}`)"));
        }
    }
    said
}

/// `message` with the values serde echoes in it left out: a string
/// (`invalid type: string "..."`), and a variant name (`unknown variant
/// `...``), which a misplaced secret would be.
fn redacted(message: &str) -> String {
    let mut out = message.to_owned();
    for (start, end) in [
        ("string \"", "\", expected"),
        ("unknown variant `", "`, expected"),
    ] {
        let mut from = 0;
        while let Some(at) = out[from..].find(start) {
            let value = from + at + start.len();
            let Some(len) = out[value..].rfind(end) else {
                break;
            };
            out.replace_range(value..value + len, "...");
            from = value + 3;
        }
    }
    out
}

/// What to do when there is no config file and no key in the environment.
pub fn no_keys_message() -> String {
    let names: Vec<&str> = respan_gateway_providers::ALL
        .iter()
        .flat_map(|p| p.key_env.iter().copied())
        .collect();
    format!(
        "no {DEFAULT_FILE} here and no provider key in the environment.\n\
         Set one, such as `export OPENAI_API_KEY=sk-...`, and run again; the keys read are\n  {}\n\
         (and OLLAMA_HOST for Ollama). Or write a config: `respan-gateway init`.",
        names.join(", ")
    )
}

/// Where `text` says to listen: `host:port`, a name resolved, preferring
/// IPv4; `:port` or a bare port, on 127.0.0.1.
pub fn parse_listen(text: &str) -> Result<SocketAddr, String> {
    use std::net::{IpAddr, Ipv4Addr, ToSocketAddrs};
    let text = text.trim();
    let port = text.strip_prefix(':').unwrap_or(text);
    if let Ok(port) = port.parse::<u16>() {
        return Ok(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port));
    }
    if let Ok(addr) = text.parse() {
        return Ok(addr);
    }
    let usage = || format!("'{text}' is not host:port, :port or a port");
    if !text.contains(':') {
        return Err(usage());
    }
    let addrs: Vec<SocketAddr> = text
        .to_socket_addrs()
        .map_err(|e| format!("{}: {e}", usage()))?
        .collect();
    let v4 = addrs.iter().find(|a| a.is_ipv4());
    v4.or(addrs.first())
        .copied()
        .ok_or_else(|| format!("'{text}' resolves to no address"))
}

fn listen_address<'de, D: Deserializer<'de>>(d: D) -> Result<Option<SocketAddr>, D::Error> {
    let Some(text) = Option::<String>::deserialize(d)? else {
        return Ok(None);
    };
    parse_listen(&text)
        .map(Some)
        .map_err(|e| serde::de::Error::custom(format!("listen: {e}")))
}

/// A number of bytes, or a size such as `32MiB`, `512 KB` or `1GiB`, of
/// the config key `key`; at least one byte.
fn byte_size<'de, D: Deserializer<'de>>(d: D, key: &str) -> Result<Option<usize>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Size {
        Bytes(usize),
        Text(String),
    }
    let bytes = match Option::<Size>::deserialize(d)? {
        None => return Ok(None),
        Some(Size::Bytes(bytes)) => bytes,
        Some(Size::Text(text)) => {
            parse_size(&text).map_err(|e| serde::de::Error::custom(format!("{key}: {e}")))?
        }
    };
    match bytes {
        0 => Err(serde::de::Error::custom(format!("{key}: must be above 0"))),
        bytes => Ok(Some(bytes)),
    }
}

fn body_limit<'de, D: Deserializer<'de>>(d: D) -> Result<Option<usize>, D::Error> {
    byte_size(d, "body_limit")
}

fn ws_message_limit<'de, D: Deserializer<'de>>(d: D) -> Result<Option<usize>, D::Error> {
    byte_size(d, "ws_message_limit")
}

fn request_history_file_max<'de, D: Deserializer<'de>>(d: D) -> Result<Option<usize>, D::Error> {
    byte_size(d, "request_history_file_max")
}

/// Dollars per million tokens of a model prefix.
#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Price {
    pub input: f64,
    pub output: f64,
    /// For input tokens read from a cache; `input` if unset.
    pub cached_input: Option<f64>,
}

fn prices<'de, D: Deserializer<'de>>(d: D) -> Result<IndexMap<String, Price>, D::Error> {
    let prices = IndexMap::<String, Price>::deserialize(d)?;
    for (prefix, p) in &prices {
        let rates = [Some(p.input), Some(p.output), p.cached_input];
        if rates.iter().flatten().any(|r| !r.is_finite() || *r < 0.0) {
            return Err(serde::de::Error::custom(format!(
                "prices: {prefix}: a price must be a number of dollars, 0 or more"
            )));
        }
    }
    Ok(prices)
}

fn request_history<'de, D: Deserializer<'de>>(d: D) -> Result<Option<usize>, D::Error> {
    match Option::<usize>::deserialize(d)? {
        Some(0) => Err(serde::de::Error::custom("request_history: must be above 0")),
        kept => Ok(kept),
    }
}

/// A number of seconds, or a duration such as `30s`, `2m`, `1h` or `500ms`,
/// of the config key `key`.
fn span<'de, D: Deserializer<'de>>(d: D, key: &str) -> Result<Option<Duration>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Span {
        Seconds(u64),
        Text(String),
    }
    Ok(match Option::<Span>::deserialize(d)? {
        None => None,
        Some(Span::Seconds(s)) => Some(Duration::from_secs(s)),
        Some(Span::Text(text)) => Some(
            parse_duration(&text).map_err(|e| serde::de::Error::custom(format!("{key}: {e}")))?,
        ),
    })
}

fn shutdown_grace<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {
    span(d, "shutdown_grace")
}

fn agent_timeout<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {
    span(d, "agent: timeout")
}

fn agent_tool_timeout<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {
    span(d, "agent: tool_timeout")
}

fn agent_tool_list_ttl<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {
    span(d, "agent: tool_list_ttl")
}

fn agent_max_result<'de, D: Deserializer<'de>>(d: D) -> Result<Option<usize>, D::Error> {
    byte_size(d, "agent: max_result")
}

fn agent_max_results<'de, D: Deserializer<'de>>(d: D) -> Result<Option<usize>, D::Error> {
    byte_size(d, "agent: max_results")
}

/// The time `text` says: a number and a unit (`ms`, `s`, `m`, `h`),
/// seconds if none.
pub fn parse_duration(text: &str) -> Result<Duration, String> {
    let text = text.trim();
    let split = text
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(text.len());
    let (number, unit) = text.split_at(split);
    let millis: u64 = match unit.trim().to_ascii_lowercase().as_str() {
        "ms" => 1,
        "" | "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        _ => return Err(format!("'{text}' is not a duration such as 30s or 2m")),
    };
    number
        .parse::<u64>()
        .ok()
        .and_then(|n| n.checked_mul(millis))
        .map(Duration::from_millis)
        .ok_or_else(|| format!("'{text}' is not a duration such as 30s or 2m"))
}

/// The bytes `text` says: a number and a unit (`B`, `KB`, `MB`, `GB` as
/// powers of 1000, `KiB`, `MiB`, `GiB` as powers of 1024), case-blind.
pub fn parse_size(text: &str) -> Result<usize, String> {
    let text = text.trim();
    let split = text
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(text.len());
    let (number, unit) = text.split_at(split);
    let factor: usize = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "kb" => 1_000,
        "mb" => 1_000_000,
        "gb" => 1_000_000_000,
        "kib" => 1 << 10,
        "mib" => 1 << 20,
        "gib" => 1 << 30,
        _ => return Err(format!("'{text}' is not a size such as 32MiB or 33554432")),
    };
    number
        .parse::<usize>()
        .ok()
        .and_then(|n| n.checked_mul(factor))
        .ok_or_else(|| format!("'{text}' is not a size such as 32MiB or 33554432"))
}

/// A string or a list of them.
fn one_or_many<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Vec<String>>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrMany {
        One(String),
        Many(Vec<String>),
    }
    Ok(match Option::<OneOrMany>::deserialize(d)? {
        None => None,
        Some(OneOrMany::One(one)) => Some(vec![one]),
        Some(OneOrMany::Many(many)) => Some(many),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_listen_address_is_a_socket_address() {
        let bad = Config::parse("listen: 127.0.0.1").err();
        assert!(bad.is_some_and(|e| e.to_string().contains("listen")));
        let good = Config::parse("listen: 127.0.0.1:8080");
        assert!(good.is_ok_and(|c| c.listen.is_some_and(|a| a.port() == 8080)));
        let named = Config::parse("listen: localhost:8081");
        assert!(named.is_ok_and(|c| c.listen.is_some_and(|a| a.port() == 8081)));
    }

    #[test]
    fn a_listen_address_may_be_a_name_a_port_or_colon_port() {
        let local = |port| Ok(SocketAddr::from(([127, 0, 0, 1], port)));
        assert_eq!(parse_listen("8080"), local(8080));
        assert_eq!(parse_listen(":8080"), local(8080));
        assert_eq!(parse_listen("localhost:8080").map(|a| a.port()), Ok(8080));
        assert!(parse_listen("localhost:8080").is_ok_and(|a| a.ip().is_loopback()));
        assert_eq!(
            parse_listen("[::1]:9"),
            Ok(SocketAddr::from((std::net::Ipv6Addr::LOCALHOST, 9)))
        );
        assert!(parse_listen("0.0.0.0:80").is_ok_and(|a| a.ip().is_unspecified()));
        assert!(parse_listen("localhost").is_err());
        assert!(parse_listen("99999").is_err());
    }

    #[test]
    fn presets_lists_keys_and_switches_parse() {
        let yaml = "client_keys: [env:GATEWAY_KEY, sk-local]
log: off
ui: false
providers:
  openai: { vendor: openai }
  claude: { vendor: anthropic, key: env:MY_KEY, base_url: 'http://x.test' }
  both: { formats: [openai-chat, anthropic-messages], base_url: 'http://y.test' }
  one: { formats: openai-chat, base_url: 'http://z.test' }
";
        let config = Config::parse(yaml).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(config.client_keys, ["env:GATEWAY_KEY", "sk-local"]);
        assert!(config.log == LogFormat::Off && !config.ui);
        assert!(config.formats.is_none());
        let names: Vec<&str> = config.providers.keys().map(String::as_str).collect();
        assert_eq!(
            names,
            ["openai", "claude", "both", "one"],
            "in config order"
        );
        let both = &config.providers["both"];
        assert_eq!(
            both.formats.as_deref(),
            Some(&["openai-chat".to_owned(), "anthropic-messages".to_owned()][..])
        );
        assert_eq!(
            config.providers["one"].formats.as_ref().map(Vec::len),
            Some(1)
        );
        assert_eq!(
            config.providers["claude"].key.as_deref(),
            Some("env:MY_KEY")
        );
        assert_eq!(config.providers["openai"].vendor.as_deref(), Some("openai"));
    }

    #[test]
    fn a_body_limit_is_bytes_or_a_size() {
        let limit = |text: &str| Config::parse(text).map(|c| c.body_limit);
        assert_eq!(limit("body_limit: 1000"), Ok(Some(1000)));
        assert_eq!(limit("body_limit: 32MiB"), Ok(Some(32 << 20)));
        assert_eq!(limit("body_limit: 2 kb"), Ok(Some(2000)));
        let bad = limit("body_limit: lots").err().map(|e| e.to_string());
        assert!(bad.is_some_and(|e| e.contains("body_limit")));
    }

    #[test]
    fn limits_are_sizes_counts_and_durations() {
        let config =
            Config::parse("ws_message_limit: 1MiB\nrequest_history: 50\nshutdown_grace: 2m")
                .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(config.ws_message_limit, Some(1 << 20));
        assert_eq!(config.request_history, Some(50));
        assert_eq!(config.shutdown_grace, Some(Duration::from_secs(120)));
        let grace = |text: &str| Config::parse(text).map(|c| c.shutdown_grace);
        assert_eq!(
            grace("shutdown_grace: 30s"),
            Ok(Some(Duration::from_secs(30)))
        );
        assert_eq!(
            grace("shutdown_grace: 45"),
            Ok(Some(Duration::from_secs(45)))
        );
        assert_eq!(
            grace("shutdown_grace: 500ms"),
            Ok(Some(Duration::from_millis(500)))
        );
        let defaults = Config::parse("{}").unwrap_or_else(|e| panic!("{e}"));
        assert!(defaults.request_history.is_none() && defaults.shutdown_grace.is_none());
        for (text, key) in [
            ("shutdown_grace: soon", "shutdown_grace"),
            ("shutdown_grace: 5 days", "shutdown_grace"),
            ("request_history: 0", "request_history"),
            ("request_history: many", "request_history"),
            ("ws_message_limit: 0", "ws_message_limit"),
            ("ws_message_limit: big", "ws_message_limit"),
            ("body_limit: 0", "body_limit"),
        ] {
            let said = Config::parse(text).err().map(|e| e.to_string());
            assert!(said.is_some_and(|e| e.contains(key)), "{text}");
        }
    }

    #[test]
    fn old_names_are_refused() {
        let old = [
            "serve: []",
            "target: p",
            "keys: []",
            "providers: { p: { format: x } }",
        ];
        for text in old {
            assert!(Config::parse(text).is_err(), "{text}");
        }
    }

    #[test]
    fn defaults_serve_the_ui_and_log() {
        let config = Config::parse("{}").unwrap_or_else(|e| panic!("{e}"));
        assert!(config.log == LogFormat::Text && config.ui && config.client_keys.is_empty());
        assert_eq!(config.source(), &Source::Inline);
    }

    #[test]
    fn an_error_does_not_echo_a_value_that_may_be_a_secret() {
        for text in [
            "client_keys: sk-secret-1",
            "providers: { p: { vendor: openai, key: [sk-secret-1] } }",
            "providers: { p: { vendor: openai, auth: sk-secret-1 } }",
            "mcp: { m: { command: [x], env: sk-secret-1 } }",
            "mcp: { m: { url: 'http://h.test', key: { a: 1 }, forward_auth: sk-secret-1 } }",
            "log: sk-secret-1",
            "providers: { p: { vendor: openai, auth: { type: sk-secret-1, key: k } } }",
        ] {
            let said = Config::parse(text)
                .err()
                .map(|e| e.to_string())
                .unwrap_or_default();
            assert!(!said.is_empty(), "{text}");
            assert!(!said.contains("sk-secret-1"), "{text}: {said}");
        }
        let said = Config::parse("client_keys: sk-secret-1")
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default();
        assert!(said.contains("string \"...\""), "{said}");
    }

    #[test]
    fn renamed_keys_are_refused_with_their_new_names() {
        let said = |text: &str| Config::parse(text).err().map(|e| e.to_string());
        assert!(said("request_log: 5").is_some_and(|e| e.contains("`request_history`")));
        let agent = "agent: { servers: [a], tools: [a] }";
        assert!(said(agent).is_some_and(|e| e.contains("agent.servers")));
        let cache = "agent: { servers: [a], cache: 5 }";
        assert!(said(cache).is_some_and(|e| e.contains("agent.tool_list_ttl")));
    }

    #[test]
    fn agent_and_cors_keys_parse() {
        let yaml = "agent:
  servers: [github]
  allow: ['github__*']
  deny: ['github__delete_*']
  tool_list_ttl: 1m
  parallel: 2
  max_result: 8KiB
  max_results: 64KiB
cors: { origins: ['https://app.example.com'] }";
        let config = Config::parse(yaml).unwrap_or_else(|e| panic!("{e}"));
        let agent = config.agent.unwrap_or_else(|| panic!("agent"));
        assert_eq!(agent.servers, ["github"]);
        assert_eq!(agent.allow, ["github__*"]);
        assert_eq!(agent.deny, ["github__delete_*"]);
        assert_eq!(agent.tool_list_ttl, Some(Duration::from_secs(60)));
        assert_eq!(agent.parallel, Some(2));
        assert_eq!(agent.max_results, Some(64 << 10));
        assert!(config.cors.is_some_and(|c| c.origins.len() == 1));
    }

    #[test]
    fn unknown_fields_are_refused() {
        let said = Config::parse("providers: { p: { vendor: openai, bogus: 1 } }")
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default();
        assert!(said.contains("bogus"), "{said}");
    }

    #[test]
    fn proxy_is_one_provider_of_the_chat_format_and_passthrough_at_the_origin() {
        let config = Config::proxy(ProxyVendor::OpenAi, Some("http://h.test/v1/"));
        let provider = &config.providers["openai"];
        assert_eq!(provider.base_url.as_deref(), Some("http://h.test"));
        assert_eq!(
            provider.formats.as_deref(),
            Some(&["openai-chat".to_owned(), "passthrough".to_owned()][..])
        );
        assert!(config.raw_relay);
        assert_eq!(config.source().to_string(), "proxy");
    }
}
