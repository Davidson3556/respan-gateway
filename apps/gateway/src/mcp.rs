//! MCP servers behind the gateway: `mcp:` in the config names them, and
//! clients reach each one over streamable HTTP at `/mcp/{name}`, behind the
//! gateway's client keys. An upstream is a streamable-HTTP server (`url`) or
//! a child process speaking MCP on stdio (`command`); either way its
//! credentials stay on the gateway, and a client's are not sent on unless the
//! server says `forward_auth`.
//!
//! A stdio server is not a daemon: the transport starts one child per
//! client session, when the session `initialize`s, and ends it with the
//! session (a client's `DELETE`, ten idle minutes, or the child exiting).
//! A child that dies fails its session's requests; the client starts a new
//! session and so a fresh child, as MCP asks of a client whose session ended.
//! At shutdown the children are killed with the runtime.

use std::path::{Path, PathBuf};

use indexmap::IndexMap;
use respan_gateway_core::{
    ClientContext, Extensions, Fault, Gateway, Plugin, Response, async_trait,
};
use respan_gateway_http::http::HeaderMap;
use respan_gateway_http::{HttpTransport, ProviderIo, Wire, framing};
use respan_gateway_mcp::{Input, Output};
use respan_gateway_mcp_http::{StdioTransport, StreamableHttp};
use respan_gateway_plugins::Model;
use serde::Deserialize;

use crate::compose::{auth_headers, secret, valid_provider_name};
use crate::config::Auth;
use crate::formats::{KnownFormat, explained, kind};
use crate::presets::origin;

/// The format's name, as the request log shows it.
pub const FORMAT: &str = "mcp";
/// Where clients reach a server: `/mcp/{name}`.
const MOUNT: &str = "/mcp";
/// The most client sessions, so child processes, one server holds at once
/// (the transport's limits are per owner and in all; here they are one).
const SESSIONS: usize = 32;

/// One MCP server, as `gateway.yaml` writes it under `mcp:`: `url` or
/// `command`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpConfig {
    /// A streamable-HTTP server's endpoint, such as
    /// `https://api.githubcopilot.com/mcp/`.
    pub url: Option<String>,
    /// Its key, literal or `env:NAME`, sent as `Authorization: Bearer`.
    pub key: Option<String>,
    /// Its credentials as one header, in place of `key`.
    pub auth: Option<Auth>,
    /// Send the client's own credentials to a server with no key of its own.
    #[serde(default)]
    pub forward_auth: bool,
    /// A stdio server's program and arguments, run once per client session.
    pub command: Option<Vec<String>>,
    /// Its environment, values literal or `env:NAME`. It gets nothing else
    /// of the gateway's but `PATH`.
    #[serde(default)]
    pub env: IndexMap<String, String>,
    /// The directory it runs in.
    pub cwd: Option<PathBuf>,
}

/// A checked server.
#[derive(Debug, Clone)]
pub struct McpServer {
    pub name: String,
    pub transport: Transport,
    /// Whether it sends a key of its own.
    pub own_key: bool,
    /// Where its key comes from, never the key.
    pub key_from: String,
    pub forward_auth: bool,
}

#[derive(Debug, Clone)]
pub enum Transport {
    Http {
        /// As configured.
        url: String,
        /// `scheme://host[:port]`, and the path a call is sent to.
        origin: String,
        path: String,
        auth: HeaderMap,
    },
    Stdio {
        command: Vec<String>,
        env: Vec<(String, String)>,
        cwd: Option<PathBuf>,
    },
}

impl McpServer {
    /// `http` or `stdio`.
    pub fn transport_label(&self) -> &'static str {
        match self.transport {
            Transport::Http { .. } => "http",
            Transport::Stdio { .. } => "stdio",
        }
    }

    /// What it runs or reaches, for a person to read: the URL, or the
    /// command line.
    pub fn target(&self) -> String {
        match &self.transport {
            Transport::Http { url, .. } => url.clone(),
            Transport::Stdio { command, .. } => command.join(" "),
        }
    }

    /// Where clients reach it.
    pub fn endpoint(&self) -> String {
        format!("{MOUNT}/{}", self.name)
    }
}

/// The servers `config` names, checked: names, transports, keys read, and
/// the programs found.
pub fn servers(
    config: &IndexMap<String, McpConfig>,
    providers: &[&str],
) -> Result<Vec<McpServer>, Fault> {
    config
        .iter()
        .map(|(name, c)| {
            let bad = |e: &str| Fault::config(format!("mcp '{name}': {e}"));
            if !valid_provider_name(name) || name.chars().all(|c| c == '.') {
                return Err(bad("a name is letters, digits, '_', '.' and '-' alone"));
            }
            if providers.contains(&name.as_str()) {
                return Err(bad("is also the name of a provider; names are shared"));
            }
            server(name, c)
        })
        .collect()
}

fn server(name: &str, c: &McpConfig) -> Result<McpServer, Fault> {
    let owner = format!("mcp '{name}'");
    let bad = |e: &str| Fault::config(format!("{owner}: {e}"));
    match (&c.url, &c.command) {
        (Some(_), Some(_)) => return Err(bad("set `url` or `command`, not both")),
        (None, None) => return Err(bad("needs a `url` (HTTP) or a `command` (stdio)")),
        _ => {}
    }
    let (transport, own_key, key_from) = match (&c.url, &c.command) {
        (Some(url), _) => {
            if !c.env.is_empty() || c.cwd.is_some() {
                return Err(bad("`env` and `cwd` are for a `command`"));
            }
            let (auth, key_from) = match (&c.key, &c.auth) {
                (Some(_), Some(_)) => return Err(bad("set `key` or `auth`, not both")),
                (Some(key), None) => {
                    let auth = Auth::Bearer { key: key.clone() };
                    (auth_headers(&owner, &auth)?, key_source(key))
                }
                (None, Some(a @ (Auth::Bearer { key } | Auth::Header { key, .. }))) => {
                    (auth_headers(&owner, a)?, key_source(key))
                }
                (None, None) => (HeaderMap::new(), "client credentials".to_owned()),
            };
            // Parsed as the transport will: a bad URL fails here, not on a call.
            let url = url.trim();
            HttpTransport::new(origin(url)).map_err(|e| bad(e.message()))?;
            let path = &url[origin(url).len()..];
            if path.contains(['?', '#']) {
                return Err(bad("a `url` has no query or fragment"));
            }
            let path = if path.is_empty() { "/" } else { path };
            let (origin, path) = (origin(url).to_owned(), path.to_owned());
            let own = !auth.is_empty();
            let t = Transport::Http {
                url: url.to_owned(),
                origin,
                path,
                auth,
            };
            (t, own, key_from)
        }
        (_, Some(command)) => {
            if c.key.is_some() || c.auth.is_some() || c.forward_auth {
                return Err(bad("`key`, `auth` and `forward_auth` are for a `url`"));
            }
            let Some(program) = command.first().filter(|p| !p.is_empty()) else {
                return Err(bad("`command` is empty"));
            };
            if !on_path(program) {
                return Err(bad(&format!("command '{program}' was not found on PATH")));
            }
            let env = c
                .env
                .iter()
                .map(|(k, v)| Ok((k.clone(), secret(&format!("{owner}: env {k}"), v)?)))
                .collect::<Result<_, Fault>>()?;
            let t = Transport::Stdio {
                command: command.clone(),
                env,
                cwd: c.cwd.clone(),
            };
            (t, false, "none (a child process)".to_owned())
        }
        _ => unreachable!("one of url and command is set"),
    };
    Ok(McpServer {
        name: name.to_owned(),
        transport,
        own_key,
        key_from,
        forward_auth: c.forward_auth,
    })
}

fn key_source(value: &str) -> String {
    match value.strip_prefix("env:") {
        Some(var) => format!("env {var}"),
        None => "config".to_owned(),
    }
}

/// Whether `program` is a file, or is on the gateway's `PATH`, which a
/// child is given.
fn on_path(program: &str) -> bool {
    if program.contains('/') {
        return Path::new(program).is_file();
    }
    let path = std::env::var_os("PATH").unwrap_or_default();
    std::env::split_paths(&path).any(|dir| dir.join(program).is_file())
}

/// The MCP format: clients of every server in `servers`, and the servers
/// themselves as providers, registered when it is served. `limit` caps what
/// the gateway holds of one HTTP answer.
pub fn known(servers: Vec<McpServer>, limit: usize) -> KnownFormat {
    let client = StreamableHttp::new(MOUNT).serving(servers.iter().map(|s| s.name.clone()));
    let endpoints = [
        format!("POST {MOUNT}/{{name}}"),
        format!("GET {MOUNT}/{{name}}"),
        format!("DELETE {MOUNT}/{{name}}"),
    ];
    let endpoints: Vec<&str> = endpoints.iter().map(String::as_str).collect();
    // Servers are not providers: nothing configures one under `providers:`.
    let not_a_provider =
        |_: &str, _: &str, _: HeaderMap| -> Result<fn(Extensions) -> ProviderIo, Fault> {
            Err(Fault::config(
                "mcp servers are configured under `mcp:`, not `providers:`",
            ))
        };
    let serve_client = client.clone();
    KnownFormat::of(kind::MCP, &endpoints, client, not_a_provider)
        .model(model())
        .serving(move |gateway| register(gateway, &servers, &serve_client, limit))
}

/// `gateway` with each server as a provider, and the client format.
fn register(
    mut gateway: Gateway,
    servers: &[McpServer],
    client: &StreamableHttp,
    limit: usize,
) -> Result<Gateway, Fault> {
    for s in servers {
        let name = s.name.clone();
        gateway = match &s.transport {
            Transport::Http {
                origin, path, auth, ..
            } => {
                let transport = HttpTransport::new(origin)?.with_headers(auth.clone())?;
                let io = explained(transport, &s.name, origin, limit);
                gateway.provider::<Input, Output, _>(name, StreamableHttp::new(path), io)?
            }
            Transport::Stdio { command, env, cwd } => {
                let (program, args) = command
                    .split_first()
                    .ok_or_else(|| Fault::config("empty"))?;
                let mut child = StdioTransport::new(program, args)
                    .max_sessions(SESSIONS)
                    .max_total(SESSIONS);
                for (key, value) in env {
                    child = child.env(key, value);
                }
                if let Some(dir) = cwd {
                    child = child.current_dir(dir);
                }
                gateway.provider::<Input, Output, _>(
                    name,
                    StreamableHttp::new("/"),
                    child.connect(),
                )?
            }
        };
    }
    gateway.client::<Input, Output, _>(client.clone())
}

/// What a request does, for the request log in place of a model: its
/// method, and the tool, prompt or resource it names.
fn model() -> Model {
    Model::new(|cx| cx.input::<Input>().ok().map(label), |_, _| Ok(()))
}

fn label(input: &Input) -> String {
    let Some(message) = input.messages.first() else {
        // A `GET` opens the server's stream; a `DELETE` ends the session.
        return input.method.to_string();
    };
    let Some(method) = message.method() else {
        return "response".to_owned();
    };
    let params = message.params();
    let named = |field: &str| params.and_then(|p| p.get(field)?.as_str());
    let subject = match method {
        "tools/call" | "prompts/get" => named("name"),
        "resources/read" | "resources/subscribe" | "resources/unsubscribe" => named("uri"),
        _ => None,
    };
    match subject {
        Some(subject) => format!("{method} {subject}"),
        None => method.to_owned(),
    }
}

/// Targets a client of `/mcp/{name}` at the server `name`.
#[derive(Debug)]
pub struct McpRoute;

#[async_trait]
impl Plugin for McpRoute {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        if cx.format() != FORMAT {
            return Ok(None);
        }
        let path = cx
            .extensions()
            .get::<Wire>()
            .and_then(|wire| wire.get())
            .map(|request| framing::clean_path(request.uri().path()));
        let name = path.as_deref().and_then(|p| p.rsplit('/').next());
        let name = name.ok_or_else(|| Fault::not_found("no MCP server named"))?;
        cx.set_target(name);
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(yaml: &str) -> Result<Vec<McpServer>, String> {
        let config: IndexMap<String, McpConfig> =
            serde_norway::from_str(yaml).map_err(|e| e.to_string())?;
        servers(&config, &["openai"]).map_err(|e| e.message().to_owned())
    }

    #[test]
    fn a_server_is_a_url_or_a_command() {
        let ok =
            parse("a: { url: 'https://h.test/mcp/', key: sk-1 }\nb: { command: [sh, -c, 'true'] }")
                .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(ok[0].transport_label(), "http");
        assert!(ok[0].own_key && ok[0].key_from == "config");
        assert_eq!(ok[0].endpoint(), "/mcp/a");
        assert_eq!(ok[1].transport_label(), "stdio");
        let Transport::Http { origin, path, .. } = &ok[0].transport else {
            panic!("http")
        };
        assert_eq!(
            (origin.as_str(), path.as_str()),
            ("https://h.test", "/mcp/")
        );
    }

    #[test]
    fn mistakes_are_named() {
        let said = |yaml: &str| parse(yaml).err().unwrap_or_default();
        assert!(said("a: {}").contains("needs a `url`"));
        assert!(said("a: { url: 'http://h.test', command: [x] }").contains("not both"));
        assert!(said("a: { url: 'http://h.test?x=1' }").contains("query"));
        assert!(said("a: { command: [definitely-not-a-program] }").contains("not found on PATH"));
        assert!(said("a: { command: [sh], key: k }").contains("for a `url`"));
        assert!(said("a: { url: 'http://h.test', env: { A: b } }").contains("for a `command`"));
        assert!(said("openai: { url: 'http://h.test' }").contains("provider"));
        assert!(said("a/b: { url: 'http://h.test' }").contains("name"));
        assert!(
            said("a: { url: 'http://h.test', key: 'env:NO_SUCH_VAR_X' }").contains("NO_SUCH_VAR_X")
        );
    }

    #[test]
    fn a_request_is_labelled_by_what_it_does() {
        use respan_gateway_http::http::Method;
        use respan_gateway_mcp::Message;
        let message =
            |json: serde_json::Value| Message::try_from(json).unwrap_or_else(|e| panic!("{e:?}"));
        let post = |m: Message| Input::new(Method::POST, vec![m]);
        let call = message(serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": { "name": "echo", "arguments": {} }
        }));
        assert_eq!(label(&post(call)), "tools/call echo");
        let list =
            message(serde_json::json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/list" }));
        assert_eq!(label(&post(list)), "tools/list");
        assert_eq!(label(&Input::new(Method::GET, Vec::new())), "GET");
    }
}
