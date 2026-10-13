//! MCP servers behind the gateway: `mcp` in the config names them, and
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
use std::process::Stdio;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use indexmap::IndexMap;
use respan_gateway_core::futures::future::join_all;
use respan_gateway_core::{
    ClientContext, ErrorCode, Extensions, Fault, Gateway, Plugin, Provider, Response, async_trait,
};
use respan_gateway_http::http::{self, HeaderMap, Method, StatusCode};
use respan_gateway_http::{Body, Completion, raw::Original};
use respan_gateway_http_faults::Status;
use respan_gateway_http_transport::reqwest;
use respan_gateway_http_transport::{HttpTransport, unreachable};
use respan_gateway_mcp::{Input, Output, RpcCode};
use respan_gateway_mcp_http::{Owner, SESSION, StdioTransport, StreamableHttp};
use respan_gateway_plugin_routing::Model;
use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::time::timeout;

use crate::compose::{auth_headers, secret, valid_provider_name};
use crate::config::Auth;
use crate::formats::{KnownFormat, kind};
use crate::mcp_oauth::{OAuth, OAuthConfig};
use crate::models::{Probe, Reach};
use crate::vendors::origin;

/// The format's name, as the request log shows it.
pub use respan_gateway_mcp_http::FORMAT;
/// Where clients reach a server: `/mcp/{name}`.
const MOUNT: &str = "/mcp";
/// The most client sessions, so child processes, one server holds at once
/// (the transport's limits are per owner and in all; here they are one).
const SESSIONS: usize = 32;

/// One MCP server, as `gateway.toml` writes it under `mcp`: `url` or
/// `command`.
#[derive(Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpConfig {
    /// A streamable-HTTP server's endpoint, such as
    /// `https://api.githubcopilot.com/mcp/`.
    pub url: Option<String>,
    /// Its key, literal or `env:NAME`, sent as `Authorization: Bearer`.
    pub key: Option<String>,
    /// Its credentials as one header, in place of `key`.
    pub auth: Option<Auth>,
    /// The gateway's own OAuth client for the server: it gets a token from
    /// a token endpoint, in place of `key`.
    pub oauth: Option<OAuthConfig>,
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

// Its key, auth, OAuth, arguments and environment are secrets: not printed.
impl std::fmt::Debug for McpConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("McpConfig")
            .field("forward_auth", &self.forward_auth)
            .finish_non_exhaustive()
    }
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
    /// Its token source, if it has OAuth.
    pub oauth: Option<Arc<OAuth>>,
}

#[derive(Clone)]
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
        /// The program as found: on Windows `npx` is `npx.cmd`.
        program: PathBuf,
        env: Vec<(String, String)>,
        cwd: Option<PathBuf>,
    },
}

// What it reaches and runs are not printed beyond `target`: a URL, an
// argument, the environment and the headers can hold tokens.
impl std::fmt::Debug for Transport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Http { origin, .. } => write!(f, "Http({origin})"),
            Self::Stdio { program, .. } => write!(f, "Stdio({})", program.display()),
        }
    }
}

impl McpServer {
    /// `http` or `stdio`.
    pub fn transport_label(&self) -> &'static str {
        match self.transport {
            Transport::Http { .. } => "http",
            Transport::Stdio { .. } => "stdio",
        }
    }

    /// What it runs or reaches, for a person to read and for the admin API:
    /// an HTTP server's origin (`scheme://host[:port]`) and a stdio
    /// server's program name. Never the path or the arguments, which hold
    /// tokens (`/mcp/<token>`, `--token X`), nor the environment.
    pub fn target(&self) -> String {
        match &self.transport {
            Transport::Http { origin, .. } => origin.clone(),
            Transport::Stdio { command, .. } => {
                let program = command.first().map_or("", String::as_str);
                let name = Path::new(program).file_name();
                name.map_or(program, |n| n.to_str().unwrap_or(program))
                    .to_owned()
            }
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
    let mut oauth = None;
    let (transport, own_key, key_from) = match (&c.url, &c.command) {
        (Some(url), _) => {
            if !c.env.is_empty() || c.cwd.is_some() {
                return Err(bad("`env` and `cwd` are for a `command`"));
            }
            if c.oauth.is_some() && (c.key.is_some() || c.auth.is_some()) {
                return Err(bad("set `oauth`, or `key` or `auth`; only one"));
            }
            if c.oauth.is_some() && c.forward_auth {
                return Err(bad(
                    "`oauth` and `forward_auth` are both credentials; set one",
                ));
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
            oauth = match &c.oauth {
                Some(o) => Some(Arc::new(OAuth::new(&owner, o)?)),
                None => None,
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
            let own = !auth.is_empty() || oauth.is_some();
            let key_from = match &oauth {
                Some(o) => format!("oauth {} ({})", o.grant(), o.source),
                None => key_from,
            };
            let t = Transport::Http {
                url: url.to_owned(),
                origin,
                path,
                auth,
            };
            (t, own, key_from)
        }
        (_, Some(command)) => {
            if c.key.is_some() || c.auth.is_some() || c.forward_auth || c.oauth.is_some() {
                return Err(bad(
                    "`key`, `auth`, `oauth` and `forward_auth` are for a `url`",
                ));
            }
            let Some(program) = command.first().filter(|p| !p.is_empty()) else {
                return Err(bad("`command` is empty"));
            };
            let Some(found) = find_program(program) else {
                return Err(bad(&format!("command '{program}' was not found on PATH")));
            };
            if let Some(dir) = &c.cwd
                && !dir.is_dir()
            {
                let why = match dir.exists() {
                    true => "is not a directory",
                    false => "does not exist",
                };
                return Err(bad(&format!("cwd '{}' {why}", dir.display())));
            }
            let env = c
                .env
                .iter()
                .map(|(k, v)| Ok((k.clone(), secret(&format!("{owner}: env {k}"), v)?)))
                .collect::<Result<_, Fault>>()?;
            let t = Transport::Stdio {
                command: command.clone(),
                program: found,
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
        oauth,
    })
}

fn key_source(value: &str) -> String {
    match value.strip_prefix("env:") {
        Some(var) => format!("env {var}"),
        None => "config".to_owned(),
    }
}

/// The file `program` names, or finds on the gateway's `PATH`, which a
/// child is given. On Windows a program is also found with an extension of
/// `PATHEXT` (`npx` is `npx.cmd`).
fn find_program(program: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH").unwrap_or_default();
    let dirs: Vec<PathBuf> = std::env::split_paths(&path).collect();
    let pathext = match cfg!(windows) {
        true => std::env::var("PATHEXT").ok(),
        false => None,
    };
    let exts = pathext_list(pathext.as_deref());
    resolve(program, &dirs, &exts)
}

/// The extensions `PATHEXT` lists, or the usual ones if it is unset; none
/// where it is not used (`None` is an unset one only on Windows).
fn pathext_list(pathext: Option<&str>) -> Vec<String> {
    if !cfg!(windows) && pathext.is_none() {
        return Vec::new();
    }
    let list = pathext.unwrap_or(".COM;.EXE;.BAT;.CMD");
    let exts = list.split(';').map(str::trim).filter(|e| !e.is_empty());
    exts.map(str::to_owned).collect()
}

/// `program` as a file in one of `dirs` (itself, if it has a directory), as
/// it is or with one of `exts` added, in that order.
fn resolve(program: &str, dirs: &[PathBuf], exts: &[String]) -> Option<PathBuf> {
    let candidates = |base: PathBuf| -> Option<PathBuf> {
        let name = base.as_os_str().to_owned();
        let with = exts.iter().map(|ext| {
            let mut name = name.clone();
            name.push(ext);
            PathBuf::from(name)
        });
        std::iter::once(base).chain(with).find(|p| p.is_file())
    };
    if program.contains(std::path::is_separator) {
        return candidates(PathBuf::from(program));
    }
    dirs.iter().find_map(|dir| candidates(dir.join(program)))
}

/// How long a server has to answer a probe's `initialize`.
const PROBE: Duration = Duration::from_secs(5);

/// The `initialize` a probe sends: a client that only looks.
fn hello() -> String {
    serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": {
            "protocolVersion": "2025-06-18", "capabilities": {},
            "clientInfo": {"name": "respan-gateway-check", "version": env!("CARGO_PKG_VERSION")}
        }
    })
    .to_string()
}

/// `check --online` for MCP servers: each is asked to `initialize`, as a
/// client would, and the session it opens is ended; a stdio server is
/// started for it and stopped. No key and no URL is in the result.
pub async fn probe(servers: &[McpServer]) -> Vec<Probe> {
    join_all(servers.iter().map(|s| async move {
        let started = Instant::now();
        let reach = match timeout(PROBE, probe_one(s)).await {
            Ok(reach) => reach,
            Err(_) => Reach::Unreachable("it did not answer `initialize` in time".to_owned()),
        };
        Probe {
            provider: format!("mcp/{}", s.name),
            reach,
            took: started.elapsed(),
        }
    }))
    .await
}

async fn probe_one(server: &McpServer) -> Reach {
    match &server.transport {
        Transport::Http { url, auth, .. } => probe_http(url, auth, server.oauth.as_deref()).await,
        Transport::Stdio {
            command,
            program,
            env,
            cwd,
        } => probe_stdio(command, program, env, cwd.as_deref()).await,
    }
}

async fn probe_http(url: &str, auth: &HeaderMap, oauth: Option<&OAuth>) -> Reach {
    let mut auth = auth.clone();
    if let Some(oauth) = oauth {
        let sent = async { OAuth::header(&oauth.token(None).await?) }.await;
        match sent {
            Ok(value) => {
                auth.insert(http::header::AUTHORIZATION, value);
            }
            Err(fault) => return Reach::Unreachable(fault.message().to_owned()),
        }
    }
    let client = match reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
    {
        Ok(client) => client,
        Err(e) => return Reach::Unreachable(unreachable(&e)),
    };
    let asked = client
        .post(url)
        .headers(auth.clone())
        .header("accept", "application/json, text/event-stream")
        .header("content-type", "application/json")
        .body(hello())
        .send()
        .await;
    let resp = match asked {
        Ok(resp) => resp,
        Err(e) => return Reach::Unreachable(unreachable(&e)),
    };
    let status = resp.status();
    if matches!(status.as_u16(), 401 | 403) {
        return Reach::Unauthorized(status.as_u16());
    }
    if !status.is_success() {
        return Reach::Unreachable(format!("it answered {status} to `initialize`"));
    }
    if let Some(session) = resp.headers().get("mcp-session-id") {
        // Courtesy: the session is not left to idle out. Its answer is not
        // the probe's.
        let _ = client
            .delete(url)
            .headers(auth.clone())
            .header("mcp-session-id", session)
            .send()
            .await;
    }
    Reach::Reachable
}

async fn probe_stdio(
    command: &[String],
    program: &Path,
    env: &[(String, String)],
    cwd: Option<&Path>,
) -> Reach {
    let args = command.get(1..).unwrap_or_default();
    let mut child = tokio::process::Command::new(program);
    child
        .args(args)
        .env_clear()
        .envs(env.iter().cloned())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    if let Some(path) = std::env::var_os("PATH") {
        child.env("PATH", path);
    }
    #[cfg(windows)]
    {
        // As a session starts it: its own process group, and what a
        // program cannot start without.
        child.creation_flags(0x0000_0200);
        for name in ["SystemRoot", "ComSpec", "PATHEXT", "TEMP", "TMP"] {
            if let Some(value) = std::env::var_os(name) {
                child.env(name, value);
            }
        }
    }
    if let Some(dir) = cwd {
        child.current_dir(dir);
    }
    let Ok(mut child) = child.spawn() else {
        return Reach::Unreachable("the program could not be started".to_owned());
    };
    let (Some(mut stdin), Some(stdout)) = (child.stdin.take(), child.stdout.take()) else {
        return Reach::Unreachable("the program has no stdio".to_owned());
    };
    let line = format!("{}\n", hello());
    let wrote = stdin.write_all(line.as_bytes()).await;
    let wrote = match wrote {
        Ok(()) => stdin.flush().await,
        Err(e) => Err(e),
    };
    let mut answer = String::new();
    let read = match wrote {
        Ok(()) => BufReader::new(stdout).read_line(&mut answer).await,
        Err(e) => Err(e),
    };
    // Stopped as a session's end stops it: stdin closed, then killed.
    drop(stdin);
    #[cfg(windows)]
    if let Some(pid) = child.id() {
        // The program and what it started, as a session's end stops them.
        let _ = tokio::process::Command::new("taskkill")
            .args(["/T", "/F", "/PID", &pid.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await;
    }
    let _ = child.kill().await;
    let answered = serde_json::from_str::<serde_json::Value>(&answer)
        .ok()
        .is_some_and(|m| m.get("result").is_some());
    match (read, answered) {
        (Ok(n), true) if n > 0 => Reach::Reachable,
        (Ok(0) | Err(_), _) => Reach::Unreachable("the program exited before answering".to_owned()),
        _ => Reach::Unreachable("it did not answer `initialize` with a result".to_owned()),
    }
}

/// The MCP format: clients of every server in `servers`, and the servers
/// themselves as providers, registered when it is served. `limit` caps what
/// the gateway holds of one HTTP answer.
pub fn known(servers: Vec<McpServer>, aggregate: bool, limit: usize) -> KnownFormat {
    let client = StreamableHttp::new(MOUNT).serving(servers.iter().map(|s| s.name.clone()));
    let client = if aggregate {
        client.with_root()
    } else {
        client
    };
    let mut endpoints = vec![
        format!("POST {MOUNT}/{{name}}"),
        format!("GET {MOUNT}/{{name}}"),
        format!("DELETE {MOUNT}/{{name}}"),
    ];
    if aggregate {
        endpoints.extend(["POST", "GET", "DELETE"].map(|m| format!("{m} {MOUNT} (all servers)")));
    }
    let endpoints: Vec<&str> = endpoints.iter().map(String::as_str).collect();
    let serve_client = client.clone();
    KnownFormat::of(kind::MCP, &endpoints, client)
        .model(model())
        .serving(move |gateway| register(gateway, &servers, &serve_client, limit))
}

/// `client`'s calls through `owners`, a server that cannot be reached
/// named in the fault with its host and the cause: the transport's own
/// message names neither.
fn reached(
    client: HttpTransport,
    server: &str,
    url: &str,
    limit: usize,
    owners: Arc<SessionOwners>,
) -> impl Fn(respan_gateway_core::Extensions) -> respan_gateway_http::ProviderIo + Send + Sync + use<>
{
    let client = Arc::new(client);
    let host = origin(url).split_once("://").map_or("", |(_, host)| host);
    let who: Arc<str> = format!("provider '{server}' at {host}").into();
    respan_gateway_http::connect(
        move |call| {
            let (client, who, owners) = (client.clone(), who.clone(), owners.clone());
            async move {
                let sent = owners.send(call, |call| client.send(call)).await;
                sent.map_err(|fault| match fault.cause::<reqwest::Error>() {
                    Some(e) => {
                        Fault::upstream(format!("{who} {}", unreachable(e))).with_source(fault)
                    }
                    None => fault,
                })
            }
        },
        limit,
    )
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
        let owners = Arc::new(SessionOwners::new(s.oauth.clone()));
        gateway = match &s.transport {
            Transport::Http {
                origin, path, auth, ..
            } => {
                let transport = HttpTransport::new(origin)?.with_headers(auth.clone())?;
                let io = reached(transport, &s.name, origin, limit, owners.clone());
                gateway.provider(Provider::new(name, StreamableHttp::new(path), io))?
            }
            Transport::Stdio {
                command,
                program,
                env,
                cwd,
            } => {
                let args = command.get(1..).unwrap_or_default();
                let mut child = StdioTransport::new(program, args)
                    .max_sessions(SESSIONS)
                    .max_total(SESSIONS);
                for (key, value) in env {
                    child = child.env(key, value);
                }
                if let Some(dir) = cwd {
                    child = child.current_dir(dir);
                }
                gateway.provider(Provider::new(
                    name,
                    StreamableHttp::new("/"),
                    child.connect(),
                ))?
            }
        };
    }
    gateway.client::<Input, Output, _>(client.clone())
}

/// The most sessions of one HTTP server whose owners are remembered; the
/// oldest is forgotten past it, and its client starts a new session.
const OWNED: usize = 4096;

/// Whose each session of one HTTP server is. The server holds the session;
/// the gateway remembers which client it gave the id to, by the call's
/// [`Owner`], so another client naming that id is told there is none (404),
/// as the stdio transport does for its own. A call with no owner (no client
/// keys) is not tracked. Sessions are forgotten at a reload or restart.
#[derive(Debug, Default)]
pub struct SessionOwners {
    owners: Mutex<IndexMap<String, String>>,
    /// The server's token source, if it has OAuth.
    oauth: Option<Arc<OAuth>>,
}

impl SessionOwners {
    /// For a server with `oauth` as its credentials, if any.
    pub fn new(oauth: Option<Arc<OAuth>>) -> Self {
        Self {
            owners: Mutex::default(),
            oauth,
        }
    }

    fn table(&self) -> std::sync::MutexGuard<'_, IndexMap<String, String>> {
        self.owners.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Sends `call` with `send` if its session is its owner's, and notes
    /// the session the answer starts or ends.
    pub async fn send<F>(
        &self,
        call: http::Request<Body>,
        send: impl Fn(http::Request<Body>) -> F,
    ) -> Result<(http::Response<Body>, Option<Completion>), Fault>
    where
        F: Future<Output = Result<(http::Response<Body>, Option<Completion>), Fault>>,
    {
        let owner = call
            .extensions()
            .get::<Extensions>()
            .and_then(|e| e.get::<Owner>())
            .map(|Owner(id)| id.clone());
        let Some(owner) = owner else {
            return self.authorized(call, &send).await;
        };
        let session = call
            .headers()
            .get(SESSION)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        if let Some(id) = &session
            && self.table().get(id) != Some(&owner)
        {
            let fault = Fault::new("no such session");
            let fault = fault
                .with(ErrorCode("no_session".into()))
                .with(RpcCode(-32003));
            return Err(fault.with(Status(StatusCode::NOT_FOUND)));
        }
        let ends = call.method() == Method::DELETE;
        let (resp, completion) = self.authorized(call, &send).await?;
        match (
            &session,
            resp.headers().get(SESSION).and_then(|v| v.to_str().ok()),
        ) {
            (None, Some(id)) if resp.status().is_success() => {
                let mut table = self.table();
                if table.len() >= OWNED {
                    table.shift_remove_index(0);
                }
                table.insert(id.to_owned(), owner);
            }
            (Some(id), _) if ends && resp.status().is_success() => {
                self.table().shift_remove(id);
            }
            _ => {}
        }
        Ok((resp, completion))
    }
}

impl SessionOwners {
    /// Sends `call` with `send`, with the server's OAuth token if it has
    /// one. A `401` drops the token, and the call is sent once more with a
    /// new one, if its body is whole to send again.
    async fn authorized<F>(
        &self,
        call: http::Request<Body>,
        send: &impl Fn(http::Request<Body>) -> F,
    ) -> Result<(http::Response<Body>, Option<Completion>), Fault>
    where
        F: Future<Output = Result<(http::Response<Body>, Option<Completion>), Fault>>,
    {
        let Some(oauth) = &self.oauth else {
            return send(call).await;
        };
        let (parts, body) = call.into_parts();
        let whole = body.full_bytes().cloned();
        let with = |token: &str, body: Body| -> Result<http::Request<Body>, Fault> {
            let mut call = http::Request::new(body);
            *call.method_mut() = parts.method.clone();
            *call.uri_mut() = parts.uri.clone();
            *call.version_mut() = parts.version;
            *call.headers_mut() = parts.headers.clone();
            *call.extensions_mut() = parts.extensions.clone();
            call.headers_mut()
                .insert(http::header::AUTHORIZATION, OAuth::header(token)?);
            Ok(call)
        };
        let token = oauth.token(None).await?;
        let (resp, completion) = send(with(&token, body)?).await?;
        let Some(bytes) = whole.filter(|_| resp.status() == StatusCode::UNAUTHORIZED) else {
            return Ok((resp, completion));
        };
        drop((resp, completion));
        let fresh = oauth.token(Some(&token)).await?;
        send(with(&fresh, Body::full(bytes))?).await
    }
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
            .get::<Original>()
            .and_then(|wire| wire.get())
            .map(|request| respan_gateway_http::path::clean_path(request.uri().path()));
        let name = path.as_deref().and_then(|p| p.rsplit('/').next());
        let name = name.ok_or_else(|| Fault::not_found("no MCP server named"))?;
        cx.set_target(name);
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(text: &str) -> Result<Vec<McpServer>, String> {
        let config: IndexMap<String, McpConfig> =
            toml::from_str(text).map_err(|e| e.to_string())?;
        servers(&config, &["openai"]).map_err(|e| e.message().to_owned())
    }

    #[test]
    fn a_server_is_a_url_or_a_command() {
        let ok =
            parse("a = { url = \"https://h.test/mcp/\", key = \"sk-1\" }\nb = { command = [\"sh\", \"-c\", \"true\"] }")
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
        let said = |text: &str| parse(text).err().unwrap_or_default();
        assert!(said("a = {  }").contains("needs a `url`"));
        assert!(said("a = { url = \"http://h.test\", command = [\"x\"] }").contains("not both"));
        assert!(said("a = { url = \"http://h.test?x=1\" }").contains("query"));
        assert!(
            said("a = { command = [\"definitely-not-a-program\"] }").contains("not found on PATH")
        );
        assert!(said("a = { command = [\"sh\"], key = \"k\" }").contains("for a `url`"));
        assert!(
            said("a = { url = \"http://h.test\", env = { A = \"b\" } }")
                .contains("for a `command`")
        );
        assert!(said("openai = { url = \"http://h.test\" }").contains("provider"));
        assert!(said("\"a/b\" = { url = \"http://h.test\" }").contains("name"));
        assert!(
            said("a = { url = \"http://h.test\", key = \"env:NO_SUCH_VAR_X\" }")
                .contains("NO_SUCH_VAR_X")
        );
    }

    #[test]
    fn a_program_is_found_with_an_extension_of_pathext() {
        let dir = std::env::temp_dir().join(format!("respan-pathext-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("{e}"));
        std::fs::write(dir.join("npx.cmd"), "").unwrap_or_else(|e| panic!("{e}"));
        std::fs::write(dir.join("plain"), "").unwrap_or_else(|e| panic!("{e}"));
        let dirs = [dir.clone()];
        let exts = pathext_list(Some(".com;.exe;.cmd"));
        assert_eq!(exts, [".com", ".exe", ".cmd"]);
        assert_eq!(resolve("npx", &dirs, &exts), Some(dir.join("npx.cmd")));
        assert_eq!(resolve("npx", &dirs, &[]), None);
        assert_eq!(resolve("plain", &dirs, &exts), Some(dir.join("plain")));
        assert_eq!(resolve("absent", &dirs, &exts), None);
        let whole = dir.join("npx").to_string_lossy().into_owned();
        assert_eq!(resolve(&whole, &[], &exts), Some(dir.join("npx.cmd")));
        assert_eq!(pathext_list(Some(" ;")), Vec::<String>::new());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn a_cwd_must_be_a_directory() {
        let said = |cwd: &str| parse(&format!("a = {{ command = ['sh'], cwd = '{cwd}' }}")).err();
        assert!(said("/definitely/not/here").is_some_and(|e| e.contains("does not exist")));
        assert!(said("/bin/sh").is_some_and(|e| e.contains("not a directory")));
        assert!(said("/").is_none());
    }

    #[test]
    fn a_target_shows_no_path_argument_or_environment() {
        let http = parse("a = { url = \"https://h.test:8443/mcp/tok3n/\", key = \"sk-1\" }")
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(http[0].target(), "https://h.test:8443");
        let plain =
            parse("a = { url = \"http://h.test/secret/path\" }").unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(plain[0].target(), "http://h.test");
        let stdio =
            parse("a = { command = [\"sh\", \"--token\", \"tok3n\"], env = { K = \"sk-2\" } }");
        let stdio = stdio.unwrap_or_else(|e| panic!("{e}"));
        let target = stdio[0].target();
        assert_eq!(target, "sh");
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
