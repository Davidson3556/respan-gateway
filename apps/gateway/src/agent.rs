//! The agent loop of the binary: `agent:` in the config names `mcp:` servers
//! whose tools the gateway offers to the model in chat requests and runs for
//! it ([`respan_gateway_plugins::agent`] is the loop; this is the tools).
//!
//! The tools are reached as the gateway reaches any MCP server: as its
//! providers, with the server's own credentials, never a client's. The
//! gateway is one MCP client of its own (an [`Owner`] no client key can be):
//! it keeps one session per server across requests, started at the first use
//! and started again if the server lost it, and keeps each server's tool list
//! for a while.
//!
//! When the config reloads, the old build's sessions are ended once its
//! requests are done ([`Closer`]), not left to the server's idle timeout:
//! the app sends the old gateway a request of its own, marked with an
//! extension no client can send, which `DELETE`s each session (an HTTP
//! server's, or a stdio server's child) through the same providers.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use respan_gateway_conversation::Tool;
use respan_gateway_core::futures::StreamExt;
use respan_gateway_core::futures::stream;
use respan_gateway_core::{
    Call, ClientContext, Extensions, Fault, Gateway, Plugin, Response, async_trait,
};
use respan_gateway_http::http::{self, Method};
use respan_gateway_http::{Body, Framing};
use respan_gateway_mcp::{Input, Message, Outcome as Rpc, Output, RpcError};
use respan_gateway_mcp_http::Owner;
use respan_gateway_plugins::agent::{Agent, Outcome, ToolHop, Toolbox, Tools};
use serde_json::{Value, json};

use crate::config::AgentConfig;
use crate::mcp::McpServer;

/// The protocol version the gateway speaks as an MCP client.
pub(crate) const PROTOCOL: &str = "2025-06-18";
/// The owner of the gateway's own sessions.
const OWNER: &str = "respan-gateway-agent";
/// How many pages of a server's tool list are read.
const PAGES: usize = 8;
/// The longest a tool's name may be, as every chat API has it.
const NAME: usize = 64;
/// The longest the sessions of a retired runner, or a listing, are given.
const CLOSING: Duration = Duration::from_secs(10);
/// How long a server's tool list is kept unless the config says.
const DEFAULT_CACHE: Duration = Duration::from_secs(300);

/// Checks the `agent:` section against the servers: every tool server is an
/// `mcp:` server that is served, and takes no client's credentials.
pub fn check(agent: &AgentConfig, servers: &[McpServer], serving_mcp: bool) -> Result<(), Fault> {
    let bad = |e: String| Fault::config(format!("agent: {e}"));
    if agent.servers.is_empty() {
        return Err(bad("`servers` names no mcp server".into()));
    }
    let known = || {
        let names: Vec<&str> = servers.iter().map(|s| s.name.as_str()).collect();
        names.join(", ")
    };
    for (i, name) in agent.servers.iter().enumerate() {
        if agent.servers[..i].contains(name) {
            return Err(bad(format!("servers: '{name}' is listed twice")));
        }
        let Some(server) = servers.iter().find(|s| s.name == *name) else {
            return Err(bad(format!(
                "servers: unknown mcp server '{name}'; mcp servers: [{}]",
                known()
            )));
        };
        if server.forward_auth {
            return Err(bad(format!(
                "servers: mcp server '{name}' passes on a client's credentials \
                 (`forward_auth`); the gateway runs tools with the server's own"
            )));
        }
    }
    if !serving_mcp {
        return Err(bad(
            "servers: the mcp servers are served by the `mcp` format; add it to `formats`".into(),
        ));
    }
    if agent.max_steps == Some(0) || agent.max_steps.is_some_and(|n| n > 64) {
        return Err(bad("max_steps: must be from 1 to 64".into()));
    }
    if agent.parallel == Some(0) || agent.parallel.is_some_and(|n| n > 64) {
        return Err(bad("parallel: must be from 1 to 64".into()));
    }
    for (key, patterns) in [("allow", &agent.allow), ("deny", &agent.deny)] {
        for pattern in patterns {
            let fit = |c: char| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '*');
            if pattern.is_empty() || !pattern.chars().all(fit) {
                return Err(bad(format!(
                    "{key}: '{pattern}' is not a tool name as the model has it \
                     (`<server>__<tool>`, letters, digits, '_' and '-', with '*' for any \
                     characters; any other character of a tool's name is '_')"
                )));
            }
        }
    }
    for (key, value) in [
        ("timeout", agent.timeout),
        ("tool_timeout", agent.tool_timeout),
        ("tool_list_ttl", agent.tool_list_ttl),
    ] {
        if value == Some(Duration::ZERO) {
            return Err(bad(format!("{key}: must be above 0")));
        }
    }
    Ok(())
}

/// The loop, with the tools of the servers `agent` names. Which requests it
/// applies to: chat requests of a model `agent.models` names (all, if it
/// names none), unless the client sends `x-respan-agent: off`.
pub fn plugin(
    agent: &AgentConfig,
    anthropic: impl Fn(&str) -> bool + Send + Sync + 'static,
) -> (Agent, Closer) {
    let filter = Filter {
        allow: agent.allow.clone(),
        deny: agent.deny.clone(),
    };
    let ttl = agent.tool_list_ttl.unwrap_or(DEFAULT_CACHE);
    let tools = McpTools::new(agent.servers.clone(), ttl, filter);
    let closer = Closer(tools.shared.clone());
    let prefixes = agent.models.clone();
    let mut plugin = Agent::new(tools)
        .flag_errors_for(anthropic)
        .when(move |cx, input| {
            let opted_out = opted_out(cx);
            let model = input.model().unwrap_or_default();
            let named = prefixes.is_empty() || prefixes.iter().any(|p| model.starts_with(p));
            named && !opted_out
        });
    if let Some(steps) = agent.max_steps {
        plugin = plugin.max_steps(steps);
    }
    if let Some(limit) = agent.timeout {
        plugin = plugin.timeout(limit);
    }
    if let Some(limit) = agent.tool_timeout {
        plugin = plugin.tool_timeout(limit);
    }
    if let Some(bytes) = agent.max_result {
        plugin = plugin.max_result(bytes);
    }
    if let Some(calls) = agent.parallel {
        plugin = plugin.parallel(calls);
    }
    if let Some(bytes) = agent.max_results {
        plugin = plugin.max_results(bytes);
    }
    (plugin, closer)
}

/// Which tools are offered: those `allow` names (all, if it names none) but
/// not those `deny` names, by the name the model has.
#[derive(Debug, Clone, Default)]
pub(crate) struct Filter {
    allow: Vec<String>,
    deny: Vec<String>,
}

impl Filter {
    pub(crate) fn new(allow: Vec<String>, deny: Vec<String>) -> Self {
        Self { allow, deny }
    }

    pub(crate) fn offers(&self, name: &str) -> bool {
        let named = |patterns: &[String]| patterns.iter().any(|p| glob(p, name));
        (self.allow.is_empty() || named(&self.allow)) && !named(&self.deny)
    }
}

/// Whether `text` matches `pattern`, in which `*` stands for any characters.
fn glob(pattern: &str, text: &str) -> bool {
    let Some((first, rest)) = pattern.split_once('*') else {
        return pattern == text;
    };
    let Some(mut left) = text.strip_prefix(first) else {
        return false;
    };
    let parts: Vec<&str> = rest.split('*').collect();
    let (last, middle) = parts.split_last().unwrap_or((&"", &[]));
    for part in middle {
        match left.find(part) {
            Some(at) => left = &left[at + part.len()..],
            None => return false,
        }
    }
    left.ends_with(last)
}

/// Marks the request that ends a retired runner's sessions.
#[derive(Debug, Clone, Copy)]
struct Close;

/// Marks the request that lists the tools the agent offers, into its slot.
#[derive(Debug, Clone, Default)]
struct List(Arc<Mutex<Vec<String>>>);

/// Ends the sessions the runner kept with its servers.
#[derive(Clone)]
pub struct Closer(Arc<Shared>);

impl Closer {
    /// The plugin that does it for the request [`close`](Self::close) makes.
    /// Register it first, so nothing else sees that request.
    pub fn plugin(&self) -> impl Plugin + use<> {
        Closing(self.0.clone())
    }

    /// Ends every session, through `gateway`: the one the runner was built
    /// into, once its requests are done. A session that does not end is the
    /// server's to time out.
    pub async fn close(&self, gateway: &Gateway) {
        self.send(gateway, Close).await;
    }

    /// The tools the agent offers (`<server>__<tool>`), listed now through
    /// `gateway`; a server that cannot be listed offers none.
    pub async fn offered(&self, gateway: &Gateway) -> Vec<String> {
        let list = List::default();
        self.send(gateway, list.clone()).await;
        let names = list.0.lock().unwrap_or_else(PoisonError::into_inner);
        names.clone()
    }

    /// A request of the gateway's own, marked with `mark`, which the plugin
    /// answers.
    async fn send<M: Clone + Send + Sync + 'static>(&self, gateway: &Gateway, mark: M) {
        let Some(server) = self.0.servers.first() else {
            return;
        };
        let mut request = http::Request::new(Body::full(Vec::<u8>::new()));
        *request.method_mut() = Method::DELETE;
        *request.uri_mut() = match format!("/mcp/{}", server.name).parse() {
            Ok(uri) => uri,
            Err(_) => return,
        };
        // Any session id will do: the plugin answers before it is looked up.
        request
            .headers_mut()
            .insert("mcp-session-id", http::HeaderValue::from_static("closing"));
        let mut marks = Extensions::new();
        marks.insert(mark);
        request.extensions_mut().insert(marks);
        let sent = respan_gateway_http::handle(gateway, request, Framing::Whole, 1 << 20);
        let work = async {
            let (response, done) = sent.await;
            let body = respan_gateway_http::http_body_util::BodyExt::collect(response.into_body());
            let _ = tokio::join!(body, done);
        };
        let _ = tokio::time::timeout(CLOSING, work).await;
    }
}

struct Closing(Arc<Shared>);

#[async_trait]
impl Plugin for Closing {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let list = cx.extensions().get::<List>().cloned();
        if let Some(List(slot)) = list {
            let servers = Servers::new(self.0.clone(), cx);
            let names = servers.list().await.into_iter().map(|t| t.name);
            *slot.lock().unwrap_or_else(PoisonError::into_inner) = names.collect();
        } else if cx.extensions().get::<Close>().is_some() {
            Servers::new(self.0.clone(), cx).close().await;
        } else {
            return Ok(None);
        }
        Ok(Some(Response::stream(stream::empty::<
            Result<respan_gateway_mcp::Output, Fault>,
        >())))
    }
}

/// The header a client sends to leave the loop out of one request.
pub const OPT_OUT: &str = "x-respan-agent";

fn opted_out(cx: &ClientContext) -> bool {
    use respan_gateway_http::Wire;
    let wire = cx.extensions().get::<Wire>();
    let request = wire.and_then(|wire| wire.get());
    let value = request.and_then(|r| r.headers().get(OPT_OUT)?.to_str().ok());
    value.is_some_and(|v| {
        matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "off" | "false" | "0"
        )
    })
}

/// The tools of MCP servers, run through the gateway's providers of them.
struct McpTools {
    shared: Arc<Shared>,
}

struct Shared {
    /// The servers, in the order the config names them.
    servers: Vec<Server>,
    ttl: Duration,
    filter: Filter,
    /// The next JSON-RPC id, unique across every session.
    next_id: AtomicU64,
}

struct Server {
    name: String,
    state: tokio::sync::Mutex<State>,
}

#[derive(Default)]
struct State {
    session: Option<Opened>,
    listed: Option<(Instant, Vec<Listed>)>,
}

/// A session with a server: its id (none if the server keeps none) and the
/// protocol version it answered `initialize` with.
#[derive(Debug, Clone)]
struct Opened {
    id: Option<String>,
    protocol: String,
}

impl McpTools {
    fn new(servers: Vec<String>, ttl: Duration, filter: Filter) -> Self {
        let servers = servers
            .into_iter()
            .map(|name| Server {
                name,
                state: tokio::sync::Mutex::default(),
            })
            .collect();
        Self {
            shared: Arc::new(Shared {
                servers,
                ttl,
                filter,
                next_id: AtomicU64::new(1),
            }),
        }
    }
}

impl Tools for McpTools {
    fn open(&self, cx: &mut ClientContext) -> Arc<dyn Toolbox> {
        Arc::new(Servers::new(self.shared.clone(), cx))
    }
}

/// One request's use of the servers.
struct Servers {
    shared: Arc<Shared>,
    /// A call to each server, to make copies of.
    calls: HashMap<String, Call>,
    /// The tools offered, by the name the model has: server and tool.
    names: Mutex<HashMap<String, (String, String)>>,
}

#[respan_gateway_core::async_trait]
impl Toolbox for Servers {
    async fn list(&self) -> Vec<Tool> {
        let each = self.shared.servers.iter().map(|s| self.listing(s));
        let all = respan_gateway_core::futures::future::join_all(each).await;
        let mut names = self.names.lock().unwrap_or_else(PoisonError::into_inner);
        let mut offered = Vec::new();
        for listed in all.into_iter().flatten() {
            if !names.contains_key(&listed.offered.name) {
                let (server, tool) = (listed.server, listed.tool);
                names.insert(listed.offered.name.clone(), (server, tool));
                offered.push(listed.offered);
            }
        }
        offered
    }

    async fn call(&self, name: &str, arguments: Value) -> Outcome {
        let known = {
            let names = self.names.lock().unwrap_or_else(PoisonError::into_inner);
            names.get(name).cloned()
        };
        let Some((server, tool)) = known else {
            return Outcome::error(format!("there is no tool '{name}'"));
        };
        let params = json!({ "name": tool, "arguments": arguments });
        match self.request(&server, "tools/call", params).await {
            Ok(Ok(result)) => render(&result),
            Ok(Err(e)) => Outcome::error(e.message),
            Err(fault) => Outcome::error(format!(
                "the tool server '{server}' could not be used: {}",
                fault.message()
            )),
        }
    }
}

/// A tool of a server, as offered to the model.
#[derive(Clone)]
struct Listed {
    server: String,
    /// The tool's own name.
    tool: String,
    /// What the model is offered, named `<server>__<tool>`.
    offered: Tool,
}

impl Servers {
    fn new(shared: Arc<Shared>, cx: &mut ClientContext) -> Self {
        let calls = shared
            .servers
            .iter()
            .map(|s| (s.name.clone(), cx.call(&s.name)))
            .collect();
        Self {
            shared,
            calls,
            names: Mutex::default(),
        }
    }

    /// Ends the session with each server, and forgets it.
    async fn close(&self) {
        let each = self.shared.servers.iter().map(|server| async move {
            let session = server.state.lock().await.session.take();
            let Some(Opened {
                id: Some(id),
                protocol,
            }) = session
            else {
                return;
            };
            let mut input = Input::new(Method::DELETE, Vec::new());
            input.session = Some(id);
            input.protocol_version = Some(protocol);
            let _ = self.exchange(&server.name, input).await;
        });
        respan_gateway_core::futures::future::join_all(each).await;
    }

    fn server(&self, name: &str) -> Result<&Server, Fault> {
        let found = self.shared.servers.iter().find(|s| s.name == name);
        found.ok_or_else(|| Fault::internal(format!("no agent tool server '{name}'")))
    }

    /// A server's tools: its list, kept for the cache's time; if it cannot
    /// be listed, the last list, else none.
    async fn listing(&self, server: &Server) -> Vec<Listed> {
        let kept = {
            let state = server.state.lock().await;
            state.listed.clone()
        };
        if let Some((at, tools)) = &kept
            && at.elapsed() < self.shared.ttl
        {
            return tools.clone();
        }
        match self.fetch(&server.name).await {
            Ok(tools) => {
                let mut state = server.state.lock().await;
                state.listed = Some((Instant::now(), tools.clone()));
                tools
            }
            Err(_) => kept.map(|(_, tools)| tools).unwrap_or_default(),
        }
    }

    async fn fetch(&self, server: &str) -> Result<Vec<Listed>, Fault> {
        let (mut tools, mut cursor) = (Vec::new(), None::<String>);
        for _ in 0..PAGES {
            let params = match &cursor {
                Some(cursor) => json!({ "cursor": cursor }),
                None => json!({}),
            };
            let page = self.request(server, "tools/list", params).await?;
            let page = page.map_err(|e| Fault::new(e.message))?;
            for tool in page["tools"].as_array().into_iter().flatten() {
                let Some(name) = tool["name"].as_str() else {
                    continue;
                };
                if !self.shared.filter.offers(&model_name(server, name)) {
                    continue;
                }
                let mut offered = Tool::new(
                    model_name(server, name),
                    match tool["inputSchema"].is_object() {
                        true => tool["inputSchema"].clone(),
                        false => json!({ "type": "object", "properties": {} }),
                    },
                );
                offered.description = tool["description"].as_str().map(str::to_owned);
                tools.push(Listed {
                    server: server.to_owned(),
                    tool: name.to_owned(),
                    offered,
                });
            }
            cursor = page["nextCursor"].as_str().map(str::to_owned);
            if cursor.is_none() {
                break;
            }
        }
        Ok(tools)
    }

    /// The server's session: kept, else started.
    async fn session(&self, server: &str) -> Result<Opened, Fault> {
        let mut state = self.server(server)?.state.lock().await;
        if let Some(opened) = &state.session {
            return Ok(opened.clone());
        }
        let opened = self.initialize(server).await?;
        state.session = Some(opened.clone());
        Ok(opened)
    }

    async fn forget(&self, server: &str, opened: &Opened) {
        if let Ok(s) = self.server(server) {
            let mut state = s.state.lock().await;
            if state.session.as_ref().map(|o| &o.id) == Some(&opened.id) {
                state.session = None;
            }
        }
    }

    async fn initialize(&self, server: &str) -> Result<Opened, Fault> {
        let id = self.shared.next_id.fetch_add(1, Ordering::Relaxed);
        let hello = message(json!({
            "jsonrpc": "2.0", "id": id, "method": "initialize",
            "params": {
                "protocolVersion": PROTOCOL, "capabilities": {},
                "clientInfo": { "name": "respan-gateway", "version": env!("CARGO_PKG_VERSION") }
            }
        }))?;
        let (messages, session) = self.send(server, None, hello).await?;
        let result = answer(&messages, id)?
            .map_err(|e| Fault::new(format!("it refused `initialize`: {}", e.message)))?;
        let protocol = result["protocolVersion"].as_str().unwrap_or(PROTOCOL);
        let opened = Opened {
            id: session,
            protocol: protocol.to_owned(),
        };
        let done = message(json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }))?;
        self.send(server, Some(&opened), done).await?;
        Ok(opened)
    }

    /// A request of `method` to `server`, answered with its result or the
    /// server's error. A failure to reach it, or a session it lost, is a
    /// fault, after one try with a new session.
    async fn request(
        &self,
        server: &str,
        method: &str,
        params: Value,
    ) -> Result<Result<Value, RpcError>, Fault> {
        let mut failed = None;
        for _ in 0..2 {
            let opened = self.session(server).await?;
            let id = self.shared.next_id.fetch_add(1, Ordering::Relaxed);
            let asked = message(json!({
                "jsonrpc": "2.0", "id": id, "method": method, "params": params
            }))?;
            let sent = self.send(server, Some(&opened), asked).await;
            match sent.and_then(|(messages, _)| answer(&messages, id)) {
                Ok(answered) => return Ok(answered),
                Err(fault) => {
                    self.forget(server, &opened).await;
                    failed = Some(fault);
                }
            }
        }
        Err(failed.unwrap_or_else(|| Fault::internal("no attempt was made")))
    }

    /// Posts `message` to `server` in `session`, and reads everything the
    /// server answers: its messages, and the session it names.
    async fn send(
        &self,
        server: &str,
        session: Option<&Opened>,
        message: Message,
    ) -> Result<(Vec<Message>, Option<String>), Fault> {
        let mut input = Input::new(Method::POST, vec![message]);
        if let Some(opened) = session {
            input.session.clone_from(&opened.id);
            input.protocol_version = Some(opened.protocol.clone());
        }
        self.exchange(server, input).await
    }

    /// Sends `input` to `server` as the gateway's own client, and reads
    /// everything it answers: its messages, and the session it names.
    async fn exchange(
        &self,
        server: &str,
        input: Input,
    ) -> Result<(Vec<Message>, Option<String>), Fault> {
        let call = self
            .calls
            .get(server)
            .ok_or_else(|| Fault::internal(format!("no call to '{server}'")))?;
        let call = call.clone().with_input(input).with_extension(ToolHop);
        let call = call.with_extension(Owner(OWNER.to_owned()));
        let mut outputs = call.await?.outputs::<Output>();
        let (mut messages, mut named) = (Vec::new(), None);
        while let Some(output) = outputs.next().await {
            let output = output?;
            named = named.or(output.session);
            messages.extend(output.messages);
        }
        Ok((messages, named))
    }
}

pub(crate) fn message(json: Value) -> Result<Message, Fault> {
    Message::try_from(json).map_err(Fault::internal)
}

/// The server's answer to request `id`: its result, or its error.
pub(crate) fn answer(messages: &[Message], id: u64) -> Result<Result<Value, RpcError>, Fault> {
    let wanted = json!(id);
    for m in messages {
        if let Message::Response { outcome, .. } = m
            && m.response_id().as_ref() == Some(&wanted)
        {
            return Ok(match outcome {
                Rpc::Success(result) => Ok(result.clone()),
                Rpc::Error(e) => Err(e.clone()),
            });
        }
    }
    Err(Fault::new("the server did not answer"))
}

/// The name a tool has for the model: the server and the tool, fit for every
/// chat API (letters, digits, `_` and `-`, at most 64).
fn model_name(server: &str, tool: &str) -> String {
    let fit = |c: char| match c.is_ascii_alphanumeric() || c == '-' {
        true => c,
        false => '_',
    };
    let name: String = format!("{server}__{tool}").chars().map(fit).collect();
    name[..name.len().min(NAME)].to_owned()
}

/// What a tool answered, as text for the model.
fn render(result: &Value) -> Outcome {
    let mut parts: Vec<String> = Vec::new();
    for item in result["content"].as_array().into_iter().flatten() {
        let part = match item["type"].as_str() {
            Some("text") => item["text"].as_str().unwrap_or_default().to_owned(),
            Some("resource") => match item["resource"]["text"].as_str() {
                Some(text) => text.to_owned(),
                None => format!(
                    "[resource {} not shown]",
                    item["resource"]["uri"].as_str().unwrap_or_default()
                ),
            },
            Some("resource_link") => {
                format!("[resource {}]", item["uri"].as_str().unwrap_or_default())
            }
            Some(kind @ ("image" | "audio")) => format!(
                "[{kind} of {} not shown]",
                item["mimeType"].as_str().unwrap_or("unknown type")
            ),
            _ => item.to_string(),
        };
        parts.push(part);
    }
    if parts.is_empty() && !result["structuredContent"].is_null() {
        parts.push(result["structuredContent"].to_string());
    }
    let text = parts.join("\n");
    match result["isError"].as_bool().unwrap_or(false) {
        true => Outcome::error(text),
        false => Outcome::ok(text),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_pattern_matches_with_stars() {
        assert!(glob("github__*", "github__search"));
        assert!(glob("*__delete_*", "gh__delete_repo"));
        assert!(glob("a*b*c", "aXbYc") && glob("a*b*c", "abc"));
        assert!(glob("*", "") && glob("exact", "exact"));
        assert!(!glob("github__*", "gitlab__search"));
        assert!(!glob("a*b", "ab_"));
        assert!(!glob("a*b*c", "acb"));
        assert!(!glob("exact", "exacts"));
    }

    #[test]
    fn deny_wins_over_allow_and_allow_defaults_to_all() {
        let filter = |allow: &[&str], deny: &[&str]| Filter {
            allow: allow.iter().map(|s| (*s).to_owned()).collect(),
            deny: deny.iter().map(|s| (*s).to_owned()).collect(),
        };
        let all = filter(&[], &[]);
        assert!(all.offers("gh__search"));
        let some = filter(&["gh__*"], &["gh__delete_*"]);
        assert!(some.offers("gh__search") && !some.offers("gh__delete_repo"));
        assert!(!some.offers("fs__read"));
        let denied = filter(&[], &["fs__write"]);
        assert!(denied.offers("fs__read") && !denied.offers("fs__write"));
    }

    #[test]
    fn a_tool_is_named_for_the_model_after_its_server() {
        assert_eq!(model_name("github", "search"), "github__search");
        assert_eq!(model_name("my.files", "read file"), "my_files__read_file");
        assert_eq!(model_name("s", &"t".repeat(100)).len(), 64);
    }

    #[test]
    fn a_result_is_text_for_the_model() {
        let result = json!({ "content": [
            { "type": "text", "text": "a" },
            { "type": "image", "mimeType": "image/png", "data": "AAAA" },
            { "type": "resource_link", "uri": "file:///x" },
        ], "isError": true });
        let outcome = render(&result);
        assert!(outcome.is_error);
        assert_eq!(
            outcome.text,
            "a\n[image of image/png not shown]\n[resource file:///x]"
        );
        let structured = render(&json!({ "content": [], "structuredContent": { "n": 1 } }));
        assert_eq!(structured, Outcome::ok("{\"n\":1}"));
    }
}
