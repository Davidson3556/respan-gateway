//! The configuration: where it comes from (a TOML file, the environment or
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
use crate::vendors;

/// A client key in the config: the key alone, or with the name its requests
/// are logged under and what it may do.
#[derive(Clone, PartialEq, Eq)]
pub enum ClientKey {
    Key(String),
    Named(NamedKey),
}

#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamedKey {
    pub name: String,
    pub key: String,
    /// Whether the key reaches the admin API. When no key sets it, every
    /// key does; once one sets `true`, only those do.
    #[serde(default)]
    pub admin: Option<bool>,
    /// The tool loop and aggregate tools (`server__tool` globs) the key's
    /// requests may use, within the global `allow` and `deny`; all if unset.
    #[serde(default)]
    pub tools: Option<Vec<String>>,
}

// A key is a secret: a debug print shows its name alone.
impl std::fmt::Debug for ClientKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Key(_) => f.write_str("ClientKey(..)"),
            Self::Named(named) => write!(f, "ClientKey({})", named.name),
        }
    }
}

impl std::fmt::Debug for NamedKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NamedKey")
            .field("name", &self.name)
            .field("admin", &self.admin)
            .field("tools", &self.tools)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for ClientKey {
    fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        let value = serde_json::Value::deserialize(de)?;
        match value {
            serde_json::Value::String(key) => Ok(Self::Key(key)),
            serde_json::Value::Object(_) => serde_json::from_value(value)
                .map(Self::Named)
                .map_err(|e| D::Error::custom(format!("a client key entry: {e}"))),
            _ => Err(D::Error::custom(
                "a client key is a string, or `{ name, key, admin?, tools? }`",
            )),
        }
    }
}

impl ClientKey {
    pub fn key(&self) -> &str {
        match self {
            Self::Key(key) => key,
            Self::Named(named) => &named.key,
        }
    }

    pub fn name(&self) -> Option<&str> {
        match self {
            Self::Key(_) => None,
            Self::Named(named) => Some(&named.name),
        }
    }

    /// What the entry says of admin access, if it does.
    pub fn admin(&self) -> Option<bool> {
        match self {
            Self::Key(_) => None,
            Self::Named(named) => named.admin,
        }
    }

    pub fn tools(&self) -> Option<&[String]> {
        match self {
            Self::Key(_) => None,
            Self::Named(named) => named.tools.as_deref(),
        }
    }
}

impl From<&str> for ClientKey {
    fn from(key: &str) -> Self {
        Self::Key(key.to_owned())
    }
}

// Secrets stay out of a debug print: a key, `env:NAME` or not, is not shown.
impl std::fmt::Debug for ProviderConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProviderConfig")
            .field("vendor", &self.vendor)
            .field("formats", &self.formats)
            .field(
                "base_url",
                &self.base_url.as_deref().map(crate::redact::url),
            )
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for Auth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bearer { .. } => f.write_str("Auth::Bearer(..)"),
            Self::Header { name, .. } => write!(f, "Auth::Header({name}: ..)"),
        }
    }
}

/// A gateway's configuration, as `gateway.toml` writes it.
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
    /// Client keys, literal or `env:NAME`, each alone or as
    /// `{ name, key }`. When set, every request needs one; it is consumed
    /// by the gateway and never sent to a provider. A request is logged
    /// under its key's name (`key-1`, `key-2`... for a key without one).
    #[serde(default)]
    pub client_keys: Vec<ClientKey>,
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
    /// Which web pages may call the gateway from a browser; none if unset.
    pub cors: Option<CorsConfig>,
    /// The compiled-in plugins, each configured under its name. The gateway
    /// runs them in an order of its own, whatever the order here.
    #[serde(default)]
    pub plugins: PluginsConfig,
    #[serde(skip)]
    pub(crate) source: Source,
    /// Relay each request to one vendor, with the client's own credentials
    /// (`proxy`).
    #[serde(skip)]
    pub(crate) relay: bool,
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
            prices: IndexMap::new(),
            shutdown_grace: None,
            client_keys: Vec::new(),
            log: LogFormat::Text,
            ui: true,
            providers: IndexMap::new(),
            formats: None,
            mcp: IndexMap::new(),
            plugins: PluginsConfig::default(),
            cors: None,
            source: Source::Inline,
            relay: false,
        }
    }
}

/// One provider: a vendor, explicit formats, or a vendor with some
/// of its settings replaced.
#[derive(Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderConfig {
    /// A vendor: `openai`, `anthropic`, `gemini`, `groq`, ...
    pub vendor: Option<String>,
    /// Its format, or several; a vendor's are every format it speaks
    /// compiled in.
    #[serde(default, deserialize_with = "one_or_many")]
    pub formats: Option<Vec<String>>,
    /// The base URL its SDK takes; a vendor's is its own.
    pub base_url: Option<String>,
    /// Its key, literal or `env:NAME`, sent as its formats expect; a
    /// vendor's is `env:<VENDOR>_API_KEY`.
    pub key: Option<String>,
    /// Its credentials as one header, in place of `key`. A provider with
    /// neither, and no vendor key, passes on the client's.
    pub auth: Option<Auth>,
}

#[derive(Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Auth {
    /// `Authorization: Bearer <key>`
    Bearer { key: String },
    /// `<name>: <key>`
    Header { name: String, key: String },
}

/// The compiled-in plugins. Each is configured under its own name, and the
/// gateway registers them in a fixed order: the request log, routing, the
/// MCP aggregate, video jobs, the tool loop, hedge, then fallback. Video
/// jobs are always on; each vendor's quirks are a plugin of its providers,
/// not of the gateway.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginsConfig {
    /// Which provider each request goes to; always on.
    #[serde(default)]
    pub routing: RoutingConfig,
    /// Retry a request's target, then fall back to other providers; off if
    /// unset.
    pub fallback: Option<FallbackConfig>,
    /// Race a request's target against other providers started a little
    /// later; off if unset.
    pub hedge: Option<HedgeConfig>,
    /// Run tools of `mcp` servers inside chat requests; off if unset.
    pub tool_loop: Option<ToolLoopConfig>,
    /// Keep Responses translated to another provider's format, so a
    /// client's `previous_response_id` works with it; off if unset.
    pub responses: Option<ResponsesConfig>,
    /// One MCP endpoint, `/mcp`, that fronts the `mcp` servers as a single
    /// server (tools and prompts as `{server}__{name}`); off if unset.
    pub mcp_aggregate: Option<AggregateConfig>,
    /// The request history the web UI and admin API read; always on.
    #[serde(default)]
    pub request_log: RequestLogConfig,
}

/// Responses the gateway keeps, for `previous_response_id` with a provider
/// of another format.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResponsesConfig {
    /// How long a response is kept; an hour if unset.
    #[serde(default, deserialize_with = "responses_keep")]
    pub keep: Option<Duration>,
}

fn responses_keep<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {
    span(d, "plugins: responses: keep")
}

/// Routing: which provider each request goes to.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutingConfig {
    /// Model routes: model prefix to provider, before the vendors' own
    /// names, for every kind the provider serves.
    #[serde(default)]
    pub models: Vec<ModelRoute>,
    /// The provider of each kind it serves for requests nothing else
    /// routes; the first provider serving the kind if unset.
    pub default: Option<String>,
}

/// Fallback: a request to a provider with a chain is retried as `tries`
/// allows, then sent to each provider of the chain in turn.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FallbackConfig {
    /// Each provider's chain: the providers tried after it, in order. An
    /// empty chain retries the provider alone.
    pub after: IndexMap<String, Vec<String>>,
    /// Tries per provider, retries included; 1 if unset.
    pub tries: Option<u32>,
    /// The first wait before a retry, doubled after each; 250 ms if unset.
    #[serde(default, deserialize_with = "fallback_backoff")]
    pub backoff: Option<Duration>,
    /// The longest wait before a retry; a `Retry-After` longer than it means
    /// no retry. 10 s if unset.
    #[serde(default, deserialize_with = "fallback_max_backoff")]
    pub max_backoff: Option<Duration>,
    /// How far an answer is read before it counts as a success.
    #[serde(default)]
    pub until: Commit,
}

/// Hedge: a request to a provider with a chain also goes to each provider
/// of the chain, `stagger` after the one before; the first success wins.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HedgeConfig {
    /// Each provider's chain: the providers raced against it, started in
    /// order.
    pub after: IndexMap<String, Vec<String>>,
    /// The wait before starting the next provider; 200 ms if unset.
    #[serde(default, deserialize_with = "hedge_stagger")]
    pub stagger: Option<Duration>,
    /// How far an answer is read before it counts as a success.
    #[serde(default)]
    pub until: Commit,
}

/// How far an attempt's answer is read before it counts as a success, and
/// nothing is retried after.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Commit {
    /// Its head: a success status. The default.
    #[default]
    Head,
    /// Its whole answer, held until it ends cleanly.
    End,
}

/// The request log: the history of finished requests.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestLogConfig {
    /// How many finished requests the in-memory history (the web UI's)
    /// keeps; 1000 if unset.
    #[serde(default, deserialize_with = "request_log_history")]
    pub history: Option<usize>,
    /// A file each finished request is appended to as one JSON line (the
    /// history's records: no bodies, no keys), and the last `history` of
    /// which are read back at start; none if unset.
    pub file: Option<PathBuf>,
    /// The size at which that file moves to `<file>.1` (replacing the one
    /// there) and a new one starts; a number of bytes or a size such as
    /// `64MiB`; 64 MiB if unset.
    #[serde(default, deserialize_with = "request_log_file_max")]
    pub file_max: Option<usize>,
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

/// The tool loop: the gateway offers the tools of `mcp` servers to the
/// model and runs the ones it calls, so a client that knows nothing of MCP
/// gets answers that used them.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolLoopConfig {
    /// The `mcp` servers whose tools the gateway offers and runs, as
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
    #[serde(default, deserialize_with = "tool_loop_timeout")]
    pub timeout: Option<Duration>,
    /// The limit of one tool call; 60 s if unset.
    #[serde(default, deserialize_with = "tool_loop_tool_timeout")]
    pub tool_timeout: Option<Duration>,
    /// How long a server's tool list is kept; 5 minutes if unset.
    #[serde(default, deserialize_with = "tool_loop_tool_list_ttl")]
    pub tool_list_ttl: Option<Duration>,
    /// The most tool calls of one step that run at once; 4 if unset.
    pub parallel: Option<usize>,
    /// The most bytes of one tool's answer the model sees; 64 KiB if unset.
    /// A tool's output is untrusted input to the model.
    #[serde(default, deserialize_with = "tool_loop_max_result")]
    pub max_result: Option<usize>,
    /// The most bytes of tool answers the model sees in one request, over
    /// all its steps; 256 KiB if unset.
    #[serde(default, deserialize_with = "tool_loop_max_results")]
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
pub const DEFAULT_FILE: &str = "gateway.toml";

impl Config {
    /// A configuration from TOML text. Errors omit source excerpts, which may
    /// contain credentials, while retaining the line and column.
    pub fn parse(text: &str) -> Result<Config, Error> {
        toml::from_str(text).map_err(|e| Error::new(toml_error(text, &e)))
    }

    /// Structured configuration for language bindings, with secret-safe errors.
    pub fn from_value(value: serde_json::Value) -> Result<Config, Error> {
        serde_json::from_value(value).map_err(|e| Error::new(redacted(&e.to_string())))
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
        let detected = vendors::detected();
        if detected.is_empty() {
            return Err(Error::new(no_keys_message()));
        }
        let providers = detected
            .into_iter()
            .map(|(vendor, key, base_url)| {
                let provider = ProviderConfig {
                    vendor: Some(vendor.id.to_owned()),
                    key,
                    base_url,
                    ..ProviderConfig::default()
                };
                (vendor.id.to_owned(), provider)
            })
            .collect();
        Ok(Config {
            providers,
            source: Source::Environment,
            ..Config::default()
        })
    }

    /// `path` if given, else `./gateway.toml`, else
    /// [`from_env`](Self::from_env).
    pub fn discover(path: Option<&Path>) -> Result<Config, Error> {
        match path {
            Some(path) => Config::load(path),
            None if Path::new(DEFAULT_FILE).exists() => Config::load(DEFAULT_FILE),
            None => Config::from_env(),
        }
    }

    /// The configuration of `proxy`: one vendor at `url` (its public API if
    /// `None`), no keys of its own, its chat format and every other request
    /// relayed, with each client's credentials: the chat format writes the
    /// client's request in its own spec, and passthrough relays the rest
    /// byte for byte.
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
        // The chat format writes its own path after the vendor's API base;
        // passthrough relays the client's path at the vendor's origin.
        let provider = ProviderConfig {
            formats: Some(vec![format.to_owned(), "passthrough".to_owned()]),
            base_url: Some(url.to_owned()),
            ..ProviderConfig::default()
        };
        Config {
            providers: IndexMap::from([(name.to_owned(), provider)]),
            formats: Some(vec![format.to_owned(), "passthrough".to_owned()]),
            source: Source::Proxy,
            relay: true,
            ..Config::default()
        }
    }

    /// Where it came from.
    pub fn source(&self) -> &Source {
        &self.source
    }
}

pub(crate) fn toml_error(text: &str, error: &toml::de::Error) -> String {
    let message = redacted(error.message());
    match error.span() {
        Some(span) => {
            let prefix = &text[..span.start.min(text.len())];
            let line = prefix.bytes().filter(|b| *b == b'\n').count() + 1;
            let column = prefix
                .rsplit('\n')
                .next()
                .unwrap_or_default()
                .chars()
                .count()
                + 1;
            format!("{message} at line {line} column {column}")
        }
        None => message,
    }
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
    format!(
        "no configuration found.\n\n\
         Create a config:\n  respan-gateway init\n\n\
         Or start with a provider key:\n  export OPENAI_API_KEY=sk-...\n  respan-gateway\n\n\
         Config file: {DEFAULT_FILE}\n\
         More options: respan-gateway --help"
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
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Address {
        Text(String),
        Port(u16),
    }
    let text = match Option::<Address>::deserialize(d)? {
        None => return Ok(None),
        Some(Address::Text(text)) => text,
        Some(Address::Port(port)) => port.to_string(),
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

fn request_log_file_max<'de, D: Deserializer<'de>>(d: D) -> Result<Option<usize>, D::Error> {
    byte_size(d, "plugins: request_log: file_max")
}

fn fallback_backoff<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {
    span(d, "plugins: fallback: backoff")
}

fn fallback_max_backoff<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {
    span(d, "plugins: fallback: max_backoff")
}

fn hedge_stagger<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {
    span(d, "plugins: hedge: stagger")
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

fn request_log_history<'de, D: Deserializer<'de>>(d: D) -> Result<Option<usize>, D::Error> {
    match Option::<usize>::deserialize(d)
        .map_err(|e| serde::de::Error::custom(format!("plugins.request_log.history: {e}")))?
    {
        Some(0) => Err(serde::de::Error::custom(
            "plugins: request_log: history: must be above 0",
        )),
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

fn tool_loop_timeout<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {
    span(d, "plugins: tool_loop: timeout")
}

fn tool_loop_tool_timeout<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {
    span(d, "plugins: tool_loop: tool_timeout")
}

fn tool_loop_tool_list_ttl<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {
    span(d, "plugins: tool_loop: tool_list_ttl")
}

fn tool_loop_max_result<'de, D: Deserializer<'de>>(d: D) -> Result<Option<usize>, D::Error> {
    byte_size(d, "plugins: tool_loop: max_result")
}

fn tool_loop_max_results<'de, D: Deserializer<'de>>(d: D) -> Result<Option<usize>, D::Error> {
    byte_size(d, "plugins: tool_loop: max_results")
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
    fn toml_preserves_provider_order_and_reads_readable_values() {
        let config = Config::parse(
            r#"
listen = 8080
body_limit = "2MiB"
shutdown_grace = "500ms"
client_keys = [{ name = "ops", key = "env:OPS_KEY", admin = true }]
[providers.z]
vendor = "openai"
[providers.a]
vendor = "anthropic"
"#,
        )
        .expect("TOML config");
        assert_eq!(
            config
                .providers
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            ["z", "a"]
        );
        assert_eq!(config.listen.expect("listen").port(), 8080);
        assert_eq!(config.body_limit, Some(2 << 20));
        assert_eq!(config.shutdown_grace, Some(Duration::from_millis(500)));
        assert_eq!(config.client_keys[0].name(), Some("ops"));
    }

    #[test]
    fn toml_diagnostics_have_locations_without_source_secrets() {
        for text in [
            "log = 'sk-secret-1'",
            "client_keys = 'sk-secret-1'",
            "[providers.p]\nkey = 'sk-secret-1'\nbogus = true",
            "[providers.p]\nkey = \"sk-secret-1",
        ] {
            let error = Config::parse(text).expect_err("invalid").to_string();
            assert!(error.contains("line"), "{error}");
            assert!(!error.contains("sk-secret-1"), "{error}");
        }
    }

    #[test]
    fn toml_example_and_generated_starter_parse() {
        Config::parse(include_str!("../../../gateway.example.toml")).expect("example");
        let picks = [crate::init::Chosen::parse("openai=key\"with\ncontrols").expect("vendor")];
        let text = crate::init::starter(&picks, Some("client-secret"));
        let config = Config::parse(&text).expect("starter");
        assert_eq!(
            config.providers["openai"].key.as_deref(),
            Some("key\"with\ncontrols")
        );
        assert_eq!(config.client_keys[0].key(), "client-secret");
    }

    #[test]
    fn debug_shows_no_secret() {
        let config = Config::parse(
            "client_keys = [\"bare-secret\", { name = \"ops\", key = \"named-secret\", admin = true }]\nproviders = { a = { vendor = \"openai\", key = \"prov-secret\", base_url = \"https://user:url-secret@h.test/v1?k=q-secret\" }, b = { formats = \"openai-chat\", base_url = \"http://b.test\", auth = { type = \"bearer\", key = \"auth-secret\" } } }\nmcp = { m = { command = [\"npx\", \"--token\", \"arg-secret\"], env = { T = \"env-secret\" } }, h = { url = \"https://h.test/mcp/tok-in-path\", key = \"mcp-secret\", oauth = { token_url = \"https://o.test/t\", client_id = \"cid\", client_secret = \"oauth-secret\" } } }\n",
        )
        .unwrap_or_else(|e| panic!("{e}"));
        let shown = format!("{config:?}");
        for secret in [
            "bare-secret",
            "named-secret",
            "prov-secret",
            "url-secret",
            "q-secret",
            "auth-secret",
            "arg-secret",
            "env-secret",
            "mcp-secret",
            "oauth-secret",
            "tok-in-path",
        ] {
            assert!(!shown.contains(secret), "{secret} in {shown}");
        }
    }

    #[test]
    fn a_client_key_entry_with_a_typo_is_named() {
        let said = |text: &str| Config::parse(text).err().map(|e| e.to_string());
        let bad = said("client_keys = [{ name = \"ci\", ky = \"x\" }]\n").unwrap_or_default();
        assert!(bad.contains("ky") && bad.contains("client key"), "{bad}");
        assert!(
            said(
                "client_keys = [{ name = \"ci\", key = \"x\", admin = true, tools = [\"a__*\"] }]\n"
            )
            .is_none()
        );
        assert!(said("client_keys = [3]\n").is_some());
    }

    #[test]
    fn a_listen_address_is_a_socket_address() {
        let bad = Config::parse("listen = \"127.0.0.1\"\n").err();
        assert!(bad.is_some_and(|e| e.to_string().contains("listen")));
        let good = Config::parse("listen = \"127.0.0.1:8080\"\n");
        assert!(good.is_ok_and(|c| c.listen.is_some_and(|a| a.port() == 8080)));
        let named = Config::parse("listen = \"localhost:8081\"\n");
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
    fn vendors_lists_keys_and_switches_parse() {
        let text = "client_keys = [\"env:GATEWAY_KEY\", \"sk-local\"]\nlog = \"off\"\nui = false\nproviders = { openai = { vendor = \"openai\" }, claude = { vendor = \"anthropic\", key = \"env:MY_KEY\", base_url = \"http://x.test\" }, both = { formats = [\"openai-chat\", \"anthropic-messages\"], base_url = \"http://y.test\" }, one = { formats = \"openai-chat\", base_url = \"http://z.test\" } }\n";
        let config = Config::parse(text).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(
            config.client_keys,
            [
                ClientKey::from("env:GATEWAY_KEY"),
                ClientKey::from("sk-local")
            ]
        );
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
        assert_eq!(limit("body_limit = 1000\n"), Ok(Some(1000)));
        assert_eq!(limit("body_limit = \"32MiB\"\n"), Ok(Some(32 << 20)));
        assert_eq!(limit("body_limit = \"2 kb\"\n"), Ok(Some(2000)));
        let bad = limit("body_limit = \"lots\"\n")
            .err()
            .map(|e| e.to_string());
        assert!(bad.is_some_and(|e| e.contains("body_limit")));
    }

    #[test]
    fn limits_are_sizes_counts_and_durations() {
        let config = Config::parse(
            "ws_message_limit = \"1MiB\"\nplugins = { request_log = { history = 50 } }\nshutdown_grace = \"2m\"\n",
        )
        .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(config.ws_message_limit, Some(1 << 20));
        assert_eq!(config.plugins.request_log.history, Some(50));
        assert_eq!(config.shutdown_grace, Some(Duration::from_secs(120)));
        let grace = |text: &str| Config::parse(text).map(|c| c.shutdown_grace);
        assert_eq!(
            grace("shutdown_grace = \"30s\"\n"),
            Ok(Some(Duration::from_secs(30)))
        );
        assert_eq!(
            grace("shutdown_grace = 45\n"),
            Ok(Some(Duration::from_secs(45)))
        );
        assert_eq!(
            grace("shutdown_grace = \"500ms\"\n"),
            Ok(Some(Duration::from_millis(500)))
        );
        let defaults = Config::parse("").unwrap_or_else(|e| panic!("{e}"));
        assert!(
            defaults.plugins.request_log.history.is_none() && defaults.shutdown_grace.is_none()
        );
        for (text, key) in [
            ("shutdown_grace = \"soon\"\n", "shutdown_grace"),
            ("shutdown_grace = \"5 days\"\n", "shutdown_grace"),
            ("plugins = { request_log = { history = 0 } }\n", "history"),
            (
                "plugins = { request_log = { history = \"many\" } }\n",
                "history",
            ),
            (
                "plugins = { fallback = { after = {  }, backoff = \"soon\" } }\n",
                "backoff",
            ),
            ("ws_message_limit = 0\n", "ws_message_limit"),
            ("ws_message_limit = \"big\"\n", "ws_message_limit"),
            ("body_limit = 0\n", "body_limit"),
        ] {
            let said = Config::parse(text).err().map(|e| e.to_string());
            assert!(said.is_some_and(|e| e.contains(key)), "{text}");
        }
    }

    #[test]
    fn old_names_are_refused() {
        let old = [
            "serve = []\n",
            "target = \"p\"\n",
            "keys = []\n",
            "providers = { p = { format = \"x\" } }\n",
            "request_history = 10\n",
            "tool_loop = { servers = [\"m\"] }\n",
            "mcp_aggregate = {  }\n",
            "default = \"p\"\n",
            "plugins = [{ models = [] }]\n",
            "plugins = { quirks = false }\n",
            "plugins = { video_jobs = false }\n",
        ];
        for text in old {
            assert!(Config::parse(text).is_err(), "{text}");
        }
    }

    #[test]
    fn defaults_serve_the_ui_and_log() {
        let config = Config::parse("").unwrap_or_else(|e| panic!("{e}"));
        assert!(config.log == LogFormat::Text && config.ui && config.client_keys.is_empty());
        assert_eq!(config.source(), &Source::Inline);
    }

    #[test]
    fn an_error_does_not_echo_a_value_that_may_be_a_secret() {
        for text in [
            "client_keys = \"sk-secret-1\"\n",
            "providers = { p = { vendor = \"openai\", key = [\"sk-secret-1\"] } }\n",
            "providers = { p = { vendor = \"openai\", auth = \"sk-secret-1\" } }\n",
            "mcp = { m = { command = [\"x\"], env = \"sk-secret-1\" } }\n",
            "mcp = { m = { url = \"http://h.test\", key = { a = 1 }, forward_auth = \"sk-secret-1\" } }\n",
            "log = \"sk-secret-1\"\n",
            "providers = { p = { vendor = \"openai\", auth = { type = \"sk-secret-1\", key = \"k\" } } }\n",
        ] {
            let said = Config::parse(text)
                .err()
                .map(|e| e.to_string())
                .unwrap_or_default();
            assert!(!said.is_empty(), "{text}");
            assert!(!said.contains("sk-secret-1"), "{text}: {said}");
        }
        let said = Config::parse("client_keys = \"sk-secret-1\"\n")
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default();
        assert!(said.contains("string \"...\""), "{said}");
    }

    #[test]
    fn tool_loop_and_cors_keys_parse() {
        let text = "plugins = { tool_loop = { servers = [\"github\"], allow = [\"github__*\"], deny = [\"github__delete_*\"], tool_list_ttl = \"1m\", parallel = 2, max_result = \"8KiB\", max_results = \"64KiB\" } }\ncors = { origins = [\"https://app.example.com\"] }\n";
        let config = Config::parse(text).unwrap_or_else(|e| panic!("{e}"));
        let tool_loop = config
            .plugins
            .tool_loop
            .unwrap_or_else(|| panic!("tool_loop"));
        assert_eq!(tool_loop.servers, ["github"]);
        assert_eq!(tool_loop.allow, ["github__*"]);
        assert_eq!(tool_loop.deny, ["github__delete_*"]);
        assert_eq!(tool_loop.tool_list_ttl, Some(Duration::from_secs(60)));
        assert_eq!(tool_loop.parallel, Some(2));
        assert_eq!(tool_loop.max_results, Some(64 << 10));
        assert!(config.cors.is_some_and(|c| c.origins.len() == 1));
    }

    #[test]
    fn plugins_parse_under_their_names() {
        let text = "plugins = { routing = { models = [{ prefix = \"llama\", provider = \"groq\" }], default = \"openai\" }, fallback = { after = { openai = [\"anthropic\"] }, tries = 2, backoff = \"100ms\", until = \"end\" }, hedge = { after = { openai = [\"openai-eu\"] }, stagger = \"300ms\" } }\n";
        let plugins = Config::parse(text)
            .unwrap_or_else(|e| panic!("{e}"))
            .plugins;
        assert_eq!(plugins.routing.models[0].provider, "groq");
        assert_eq!(plugins.routing.default.as_deref(), Some("openai"));
        let fallback = plugins.fallback.unwrap_or_else(|| panic!("fallback"));
        assert_eq!(fallback.after["openai"], ["anthropic"]);
        assert_eq!(fallback.tries, Some(2));
        assert_eq!(fallback.backoff, Some(Duration::from_millis(100)));
        assert_eq!(fallback.until, Commit::End);
        let hedge = plugins.hedge.unwrap_or_else(|| panic!("hedge"));
        assert_eq!(hedge.stagger, Some(Duration::from_millis(300)));
        assert_eq!(hedge.until, Commit::Head);
        let defaults = Config::parse("").unwrap_or_else(|e| panic!("{e}")).plugins;
        assert!(defaults.fallback.is_none() && defaults.hedge.is_none());
    }

    #[test]
    fn unknown_fields_are_refused() {
        let said = Config::parse("providers = { p = { vendor = \"openai\", bogus = 1 } }\n")
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default();
        assert!(said.contains("bogus"), "{said}");
    }

    #[test]
    fn proxy_is_one_provider_of_the_chat_format_and_passthrough_at_its_api_base() {
        let config = Config::proxy(ProxyVendor::OpenAi, Some("http://h.test/v1/"));
        let provider = &config.providers["openai"];
        assert_eq!(provider.base_url.as_deref(), Some("http://h.test/v1"));
        assert_eq!(
            provider.formats.as_deref(),
            Some(&["openai-chat".to_owned(), "passthrough".to_owned()][..])
        );
        assert!(config.relay);
        assert_eq!(config.source().to_string(), "proxy");
    }
}
