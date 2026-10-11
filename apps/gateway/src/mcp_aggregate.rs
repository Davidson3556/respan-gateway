//! One MCP endpoint for every MCP server: `mcp_aggregate:` in the config
//! turns on `/mcp`, which fronts the `mcp:` servers it names (all of them if
//! it names none) as a single server. A client sees one server and one
//! session; the gateway holds one session with each server behind it, opened
//! as the client's session is and ended with it, over the same providers
//! `/mcp/{name}` uses, with each server's own credentials.
//!
//! What a client sees:
//!
//! - `initialize` answers with the capabilities of the servers merged (a
//!   capability any server has, `listChanged` and `subscribe` if any server
//!   does). A server that does not answer is left out of the session.
//! - **Tools and prompts** are named `{server}__{name}`; a call is sent to
//!   the server with the name it knows. `allow` and `deny` (patterns with
//!   `*`, as the agent's) say which tools are offered, by that name.
//! - **Resources** keep their URIs under the server's: `mcp://{server}/{uri}`
//!   (a template likewise). Nothing is looked up: the URI says the server.
//!   URIs inside tool and prompt results are given the prefix too, so what
//!   a tool links to can be read.
//! - **Lists** are the first page of every server's, merged, in the order
//!   `servers` names them. A server with more pages leaves a `nextCursor`,
//!   which holds the cursors of the servers that have more; it asks for
//!   those servers alone, until none has.
//! - **Notifications**: the `GET` stream is the servers' own, merged: their
//!   `list_changed`, `resources/updated` (URI prefixed) and log messages. A
//!   `notifications/cancelled` from the client goes to every server; progress
//!   notifications ride the answer to the call they belong to.
//! - Sampling, elicitation, roots and completion are not offered: the
//!   gateway tells its servers it is a client with no capabilities.
//!
//! The session belongs to the client key that opened it, as a server's does
//! at `/mcp/{name}`. A client that ends its session (`DELETE`) ends the
//! servers' sessions with it; one that does not leaves them to the servers'
//! own idle timeouts (the gateway forgets its side after thirty idle minutes
//! or past a thousand sessions, and at a reload).
//!
//! The request log has one entry per client request: the method and the
//! tool, prompt or resource it names, and the server a routed call went to
//! (the last one, for a list that asked several).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use indexmap::IndexMap;
use respan_gateway_core::futures::future::join_all;
use respan_gateway_core::futures::stream::{self, BoxStream, select_all};
use respan_gateway_core::futures::{StreamExt, future};
use respan_gateway_core::{
    BadRequest, Call, ClientContext, ErrorCode, Fault, Plugin, Response, Upstream, async_trait,
};
use respan_gateway_http::http::{Method, StatusCode};
use respan_gateway_http::{Status, Wire, framing};
use respan_gateway_mcp::{Input, Message, Outcome, Output, RpcCode, RpcError};
use respan_gateway_mcp_http::Owner;
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::agent::{Filter, PROTOCOL, answer, message};
use crate::mcp::{FORMAT, McpServer};

/// Where clients reach the aggregate.
pub const ROOT: &str = "/mcp";
/// The separator between a server's name and a tool's or prompt's.
const SEP: &str = "__";
/// The scheme of a resource's URI under a server's.
const SCHEME: &str = "mcp://";
/// The most client sessions held at once.
const SESSIONS: usize = 1024;
/// How long a session no client uses is kept.
const IDLE: Duration = Duration::from_secs(30 * 60);
/// The protocol versions the aggregate answers with, newest first.
const VERSIONS: [&str; 3] = [PROTOCOL, "2025-03-26", "2024-11-05"];

/// `mcp_aggregate:` in `gateway.yaml`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AggregateConfig {
    /// The `mcp:` servers behind `/mcp`, in the order lists show them. All
    /// of the servers that take no client's credentials, if unset.
    pub servers: Option<Vec<String>>,
    /// The tools offered, by their names `{server}__{tool}`, with `*` for
    /// any characters; all of them if none.
    #[serde(default)]
    pub allow: Vec<String>,
    /// The tools withheld, which win over `allow`.
    #[serde(default)]
    pub deny: Vec<String>,
}

/// The servers behind `/mcp`, checked: each is an `mcp:` server, named
/// once, and none passes on a client's credentials (the servers' sessions
/// are the gateway's own).
pub fn check(config: &AggregateConfig, servers: &[McpServer]) -> Result<Vec<String>, Fault> {
    let bad = |e: String| Fault::config(format!("mcp_aggregate: {e}"));
    let names: Vec<String> = match &config.servers {
        Some(listed) => listed.clone(),
        None => (servers.iter().filter(|s| !s.forward_auth))
            .map(|s| s.name.clone())
            .collect(),
    };
    if names.is_empty() {
        return Err(bad(
            "no mcp server to front; list some under `mcp:` (a server with \
             `forward_auth` cannot be fronted)"
                .into(),
        ));
    }
    for (i, name) in names.iter().enumerate() {
        if names[..i].contains(name) {
            return Err(bad(format!("servers: '{name}' is listed twice")));
        }
        let Some(server) = servers.iter().find(|s| s.name == *name) else {
            let known: Vec<&str> = servers.iter().map(|s| s.name.as_str()).collect();
            return Err(bad(format!(
                "servers: unknown mcp server '{name}'; mcp servers: [{}]",
                known.join(", ")
            )));
        };
        if server.forward_auth {
            return Err(bad(format!(
                "servers: mcp server '{name}' passes on a client's credentials \
                 (`forward_auth`); the aggregate's sessions use each server's own"
            )));
        }
    }
    for (key, patterns) in [("allow", &config.allow), ("deny", &config.deny)] {
        if patterns
            .iter()
            .any(|p| p.is_empty() || p.contains(char::is_whitespace))
        {
            return Err(bad(format!(
                "{key}: a pattern is a tool name, with `*` for any characters"
            )));
        }
    }
    Ok(names)
}

/// Answers the requests to `/mcp` itself.
pub struct Aggregate(Arc<Inner>);

struct Inner {
    /// The servers, in the order lists show them.
    servers: Vec<String>,
    filter: Filter,
    sessions: Mutex<IndexMap<String, Arc<Session>>>,
    /// The next JSON-RPC id of the gateway's own requests.
    next_id: AtomicU64,
}

/// One client's session.
struct Session {
    owner: Option<String>,
    /// The session with each server: none until opened, and again once the
    /// server lost it.
    upstream: HashMap<String, tokio::sync::Mutex<Option<Opened>>>,
    touched: Mutex<Instant>,
}

/// A session with a server.
#[derive(Debug, Clone)]
struct Opened {
    id: Option<String>,
    protocol: String,
    capabilities: Value,
}

impl Aggregate {
    /// Serves `servers` (checked by [`check`]) under `config`'s filter.
    pub fn new(config: &AggregateConfig, servers: Vec<String>) -> Self {
        Self(Arc::new(Inner {
            servers,
            filter: Filter::new(config.allow.clone(), config.deny.clone()),
            sessions: Mutex::default(),
            next_id: AtomicU64::new(1),
        }))
    }
}

#[async_trait]
impl Plugin for Aggregate {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        if cx.format() != FORMAT {
            return Ok(None);
        }
        let path = cx.extensions().get::<Wire>().and_then(|wire| wire.get());
        let path = path.map(|request| framing::clean_path(request.uri().path()));
        if path.as_deref() != Some(ROOT) {
            return Ok(None);
        }
        let input = cx.input::<Input>()?.clone();
        let owner = cx.extensions().get::<Owner>().map(|Owner(id)| id.clone());
        let calls = self.0.servers.iter().map(|s| (s.clone(), cx.call(s)));
        let run = Run {
            inner: self.0.clone(),
            calls: Arc::new(calls.collect()),
            owner,
        };
        let outputs = run.handle(input).await?;
        Ok(Some(Response::stream(outputs)))
    }
}

type Outputs = BoxStream<'static, Result<Output, Fault>>;

/// One client request's work.
#[derive(Clone)]
struct Run {
    inner: Arc<Inner>,
    /// A call to each server, to make copies of.
    calls: Arc<HashMap<String, Call>>,
    owner: Option<String>,
}

impl Run {
    async fn handle(self, input: Input) -> Result<Outputs, Fault> {
        match input.method {
            Method::POST => self.post(input).await,
            Method::GET => self.listen(input).await,
            Method::DELETE => self.end(input).await,
            _ => Err(BadRequest::fault("not a method of an MCP endpoint")),
        }
    }

    async fn post(self, input: Input) -> Result<Outputs, Fault> {
        let hello =
            |m: &Message| matches!(m, Message::Request { method, .. } if method == "initialize");
        if input.messages.iter().any(hello) {
            return match input.messages.as_slice() {
                [one] => self.initialize(one).await,
                _ => Err(BadRequest::fault("`initialize` is not batched")),
            };
        }
        let session = self.session(&input)?;
        let mut streams: Vec<Outputs> = Vec::new();
        for m in input.messages {
            match m {
                Message::Request {
                    id, method, params, ..
                } => streams.push(self.clone().request(&session, id.into(), &method, params)),
                Message::Notification { method, params, .. } => {
                    self.notify(&session, &method, params).await;
                }
                Message::Response { .. } => {}
            }
        }
        Ok(select_all(streams).boxed())
    }

    // The session `input` names: this client's, else no such session.
    fn session(&self, input: &Input) -> Result<Arc<Session>, Fault> {
        let missing = || BadRequest::fault("the Mcp-Session-Id header is required");
        let id = input.session.as_deref().ok_or_else(missing)?;
        let mut sessions = lock(&self.inner.sessions);
        let found = sessions.get(id).filter(|s| s.owner == self.owner);
        let Some(session) = found else {
            let fault = Fault::new("no such session");
            let fault = fault
                .with(ErrorCode("no_session".into()))
                .with(RpcCode(-32003));
            return Err(fault.with(Status(StatusCode::NOT_FOUND)));
        };
        *lock(&session.touched) = Instant::now();
        let session = session.clone();
        // Most recently used last, so the oldest goes first.
        if let Some(at) = sessions.get_index_of(id) {
            let last = sessions.len() - 1;
            sessions.move_index(at, last);
        }
        Ok(session)
    }

    /// A new session, opened with every server.
    async fn initialize(self, hello: &Message) -> Result<Outputs, Fault> {
        let Message::Request { id, params, .. } = hello else {
            return Err(BadRequest::fault("`initialize` is a request"));
        };
        let id: Value = id.clone().into();
        let asked = params.as_ref().and_then(|p| p["protocolVersion"].as_str());
        let version = VERSIONS
            .iter()
            .find(|v| Some(**v) == asked)
            .unwrap_or(&VERSIONS[0]);
        let session = Arc::new(Session {
            owner: self.owner.clone(),
            upstream: (self.inner.servers.iter())
                .map(|s| (s.clone(), tokio::sync::Mutex::new(None)))
                .collect(),
            touched: Mutex::new(Instant::now()),
        });
        let opening = self
            .inner
            .servers
            .iter()
            .map(|s| self.upstream(&session, s));
        let opened = join_all(opening).await;
        let mut capabilities = Vec::new();
        let mut why = None;
        for result in opened {
            match result {
                Ok(opened) => capabilities.push(opened.capabilities),
                Err(fault) => why = Some(fault),
            }
        }
        if capabilities.is_empty() {
            let why = why.map_or(String::new(), |f| format!(": {}", f.message()));
            return Err(Upstream::fault(format!(
                "no mcp server answered `initialize`{why}"
            )));
        }
        let name = session_id()?;
        {
            let mut sessions = lock(&self.inner.sessions);
            sessions.retain(|_, s| lock(&s.touched).elapsed() < IDLE);
            if sessions.len() >= SESSIONS {
                sessions.shift_remove_index(0);
            }
            sessions.insert(name.clone(), session);
        }
        let result = json!({
            "protocolVersion": version,
            "capabilities": merge_capabilities(&capabilities),
            "serverInfo": { "name": "respan-gateway", "version": env!("CARGO_PKG_VERSION") },
        });
        let mut output = reply(&id, Ok(result));
        output.session = Some(name);
        Ok(stream::once(future::ready(Ok(output))).boxed())
    }

    /// Ends the session and the servers' sessions with it.
    async fn end(self, input: Input) -> Result<Outputs, Fault> {
        let session = self.session(&input)?;
        let name = input.session.unwrap_or_default();
        lock(&self.inner.sessions).shift_remove(&name);
        let ending = self.inner.servers.iter().map(|server| {
            let (this, session) = (self.clone(), session.clone());
            async move {
                let Some(slot) = session.upstream.get(server) else {
                    return;
                };
                let Some(Opened {
                    id: Some(id),
                    protocol,
                    ..
                }) = slot.lock().await.take()
                else {
                    return;
                };
                let mut input = Input::new(Method::DELETE, Vec::new());
                input.session = Some(id);
                input.protocol_version = Some(protocol);
                let _ = this.exchange(server, input).await;
            }
        });
        join_all(ending).await;
        Ok(stream::empty().boxed())
    }

    /// The servers' own streams, merged: what a client is told unasked.
    async fn listen(self, input: Input) -> Result<Outputs, Fault> {
        let session = self.session(&input)?;
        let mut streams: Vec<Outputs> = Vec::new();
        for server in &self.inner.servers {
            let Some(opened) = session.opened(server).await else {
                continue;
            };
            let mut get = Input::new(Method::GET, Vec::new());
            get.session = opened.id;
            get.protocol_version = Some(opened.protocol);
            let Ok(outputs) = self.stream(server, get).await else {
                continue;
            };
            let server = server.clone();
            // A server with no stream, or one that broke, is quiet.
            let told = outputs
                .take_while(|o| future::ready(o.is_ok()))
                .filter_map(move |o| {
                    let output = o.ok().and_then(|o| told(&server, o));
                    future::ready(output.map(Ok))
                });
            streams.push(told.boxed());
        }
        Ok(select_all(streams).boxed())
    }

    /// What the client says that is no request: a cancellation goes to the
    /// servers, which know the request it names.
    async fn notify(&self, session: &Arc<Session>, method: &str, params: Option<Value>) {
        if method != "notifications/cancelled" {
            return;
        }
        let params = &params;
        let sends = self.inner.servers.iter().map(|server| async move {
            let Some(opened) = session.opened(server).await else {
                return;
            };
            let note = json!({ "jsonrpc": "2.0", "method": method, "params": params });
            let Ok(note) = message(note) else { return };
            let mut input = Input::new(Method::POST, vec![note]);
            input.session = opened.id;
            input.protocol_version = Some(opened.protocol);
            let _ = self.exchange(server, input).await;
        });
        join_all(sends).await;
    }

    /// The answer to one request.
    fn request(
        self,
        session: &Arc<Session>,
        id: Value,
        method: &str,
        params: Option<Value>,
    ) -> Outputs {
        let session = session.clone();
        let params = params.unwrap_or_else(|| json!({}));
        let method = method.to_owned();
        let once = |f: BoxFuture<Output>| stream::once(f).map(Ok).boxed();
        match method.as_str() {
            "ping" => once(Box::pin(async move { reply(&id, Ok(json!({}))) })),
            "tools/list" | "prompts/list" | "resources/list" | "resources/templates/list" => {
                once(Box::pin(async move {
                    let listed = self.list(&session, &method, &params).await;
                    reply(&id, listed)
                }))
            }
            "tools/call" => self.routed(&session, id, &method, params, by_name(true)),
            "prompts/get" => self.routed(&session, id, &method, params, by_name(false)),
            "resources/read" | "resources/subscribe" | "resources/unsubscribe" => {
                self.routed(&session, id, &method, params, by_uri)
            }
            _ => once(Box::pin(async move {
                reply(
                    &id,
                    Err((-32601, format!("method '{method}' is not offered here"))),
                )
            })),
        }
    }

    /// A `tools/list`, `prompts/list`, `resources/list` or
    /// `resources/templates/list`: the first page of every server's, or the
    /// next of those the cursor names.
    async fn list(&self, session: &Arc<Session>, method: &str, params: &Value) -> Listed {
        let (capability, field) = match method {
            "tools/list" => ("tools", "tools"),
            "prompts/list" => ("prompts", "prompts"),
            "resources/list" => ("resources", "resources"),
            _ => ("resources", "resourceTemplates"),
        };
        let cursors: Option<Map<String, Value>> = match params["cursor"].as_str() {
            Some(cursor) => match serde_json::from_str(cursor) {
                Ok(map) => Some(map),
                Err(_) => return Err((-32602, "invalid cursor".to_owned())),
            },
            None => None,
        };
        let asking = self.inner.servers.iter().filter_map(|server| {
            let cursor = match &cursors {
                Some(map) => Some(map.get(server)?.as_str()?.to_owned()),
                None => None,
            };
            Some((server.clone(), cursor))
        });
        let pages = asking.map(|(server, cursor)| {
            let session = session.clone();
            async move {
                let opened = session.opened(&server).await?;
                opened.capabilities.get(capability)?;
                let params = match &cursor {
                    Some(cursor) => json!({ "cursor": cursor }),
                    None => json!({}),
                };
                let page = self
                    .rpc(&session, &server, method, params)
                    .await
                    .ok()?
                    .ok()?;
                Some((server, page))
            }
        });
        let (mut items, mut more) = (Vec::new(), Map::new());
        for (server, page) in join_all(pages).await.into_iter().flatten() {
            for item in page[field].as_array().into_iter().flatten() {
                if let Some(item) = self.namespaced(&server, field, item.clone()) {
                    items.push(item);
                }
            }
            if let Some(next) = page["nextCursor"].as_str().filter(|c| !c.is_empty()) {
                more.insert(server, json!(next));
            }
        }
        let mut page = Map::from_iter([(field.to_owned(), Value::Array(items))]);
        if !more.is_empty() {
            page.insert("nextCursor".into(), json!(Value::Object(more).to_string()));
        }
        Ok(Value::Object(page))
    }

    /// `item` of `server`'s list as the client has it, or none if withheld.
    fn namespaced(&self, server: &str, field: &str, mut item: Value) -> Option<Value> {
        match field {
            "tools" | "prompts" => {
                let name = format!("{server}{SEP}{}", item["name"].as_str()?);
                if field == "tools" && !self.inner.filter.offers(&name) {
                    return None;
                }
                item["name"] = json!(name);
            }
            "resources" => prefix_field(server, &mut item, "uri"),
            _ => prefix_field(server, &mut item, "uriTemplate"),
        }
        Some(item)
    }

    /// A request that goes to the one server `route` finds in its params,
    /// which it rewrites for that server; the answer comes back as the
    /// server sent it, notifications on the way included, with the URIs in
    /// it given the server's prefix.
    fn routed(
        self,
        session: &Arc<Session>,
        id: Value,
        method: &str,
        mut params: Value,
        route: impl Fn(&Inner, &mut Value) -> Result<String, String>,
    ) -> Outputs {
        let (session, method) = (session.clone(), method.to_owned());
        let server = match route(&self.inner, &mut params) {
            Ok(server) => server,
            Err(why) => {
                let output = reply(&id, Err((-32602, why)));
                return stream::once(future::ready(Ok(output))).boxed();
            }
        };
        stream::once(async move {
            let mut last = None;
            for _ in 0..2 {
                match self.forward(&session, &server, &id, &method, &params).await {
                    Ok(outputs) => return outputs,
                    Err(fault) => last = Some(fault),
                }
            }
            let why = last.map_or(String::new(), |f| f.message().to_owned());
            let why = format!("the server '{server}' could not be used: {why}");
            let output = reply(&id, Err((-32603, why)));
            stream::once(future::ready(Ok(output))).boxed()
        })
        .flatten()
        .boxed()
    }

    /// One try at a routed request: its answer as a stream, once its first
    /// output is in; a session the server lost is forgotten, for the next
    /// try to open again.
    async fn forward(
        &self,
        session: &Arc<Session>,
        server: &str,
        id: &Value,
        method: &str,
        params: &Value,
    ) -> Result<Outputs, Fault> {
        let opened = self.upstream(session, server).await?;
        let asked = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
        let mut input = Input::new(Method::POST, vec![message(asked)?]);
        input.session.clone_from(&opened.id);
        input.protocol_version = Some(opened.protocol.clone());
        let tried = async {
            let mut outputs = self.stream(server, input).await?;
            let first = outputs.next().await;
            match first {
                Some(Err(fault)) => Err(fault),
                first => Ok((first, outputs)),
            }
        };
        let (first, rest) = match tried.await {
            Ok(started) => started,
            Err(fault) => {
                session.forget(server, &opened).await;
                return Err(fault);
            }
        };
        let (server, id) = (server.to_owned(), id.clone());
        let outputs = stream::iter(first).chain(rest);
        // Whatever breaks midway is an error to the request, and its end.
        let outputs = outputs.scan(false, move |broken, output| {
            if *broken {
                return future::ready(None);
            }
            let said = match output {
                Ok(output) => Ok(passed(&server, output)),
                Err(fault) => {
                    *broken = true;
                    let why = format!("the server '{server}' broke off: {}", fault.message());
                    Ok(reply(&id, Err((-32603, why))))
                }
            };
            future::ready(Some(said))
        });
        Ok(outputs.boxed())
    }

    /// The session with `server`: kept, else opened. Which `initialize`
    /// each server is sent is the gateway's own.
    async fn upstream(&self, session: &Arc<Session>, server: &str) -> Result<Opened, Fault> {
        let slot = session
            .upstream
            .get(server)
            .ok_or_else(|| Fault::internal(format!("no aggregate server '{server}'")))?;
        let mut slot = slot.lock().await;
        if let Some(opened) = &*slot {
            return Ok(opened.clone());
        }
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let hello = message(json!({
            "jsonrpc": "2.0", "id": id, "method": "initialize",
            "params": {
                "protocolVersion": PROTOCOL, "capabilities": {},
                "clientInfo": { "name": "respan-gateway", "version": env!("CARGO_PKG_VERSION") }
            }
        }))?;
        let (messages, named) = self
            .exchange(server, Input::new(Method::POST, vec![hello]))
            .await?;
        let result = answer(&messages, id)?
            .map_err(|e| Fault::new(format!("it refused `initialize`: {}", e.message)))?;
        let opened = Opened {
            id: named,
            protocol: result["protocolVersion"]
                .as_str()
                .unwrap_or(PROTOCOL)
                .to_owned(),
            capabilities: result["capabilities"].clone(),
        };
        let done = message(json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }))?;
        let mut input = Input::new(Method::POST, vec![done]);
        input.session.clone_from(&opened.id);
        input.protocol_version = Some(opened.protocol.clone());
        self.exchange(server, input).await?;
        *slot = Some(opened.clone());
        Ok(opened)
    }

    /// A request of the gateway's own to `server`: its result or its error,
    /// after one more try with a new session if the first failed.
    async fn rpc(
        &self,
        session: &Arc<Session>,
        server: &str,
        method: &str,
        params: Value,
    ) -> Result<Result<Value, RpcError>, Fault> {
        let mut failed = None;
        for _ in 0..2 {
            let opened = self.upstream(session, server).await?;
            let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
            let asked = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
            let mut input = Input::new(Method::POST, vec![message(asked)?]);
            input.session.clone_from(&opened.id);
            input.protocol_version = Some(opened.protocol.clone());
            let sent = self.exchange(server, input).await;
            match sent.and_then(|(messages, _)| answer(&messages, id)) {
                Ok(answered) => return Ok(answered),
                Err(fault) => {
                    session.forget(server, &opened).await;
                    failed = Some(fault);
                }
            }
        }
        Err(failed.unwrap_or_else(|| Fault::internal("no attempt was made")))
    }

    /// The outputs of `input` sent to `server` as the client's call.
    async fn stream(&self, server: &str, input: Input) -> Result<Outputs, Fault> {
        let call = self
            .calls
            .get(server)
            .ok_or_else(|| Fault::internal(format!("no call to '{server}'")))?;
        let outputs = call.clone().with_input(input).await?.outputs::<Output>();
        Ok(outputs.boxed())
    }

    /// Everything `server` answers to `input`: its messages, and the
    /// session it names.
    async fn exchange(
        &self,
        server: &str,
        input: Input,
    ) -> Result<(Vec<Message>, Option<String>), Fault> {
        let mut outputs = self.stream(server, input).await?;
        let (mut messages, mut named) = (Vec::new(), None);
        while let Some(output) = outputs.next().await {
            let output = output?;
            named = named.or(output.session);
            messages.extend(output.messages);
        }
        Ok((messages, named))
    }
}

type BoxFuture<T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send>>;
/// A list's page, or the error code and message to answer with.
type Listed = Result<Value, (i64, String)>;

impl Session {
    /// The session with `server`, if it is open.
    async fn opened(&self, server: &str) -> Option<Opened> {
        self.upstream.get(server)?.lock().await.clone()
    }

    /// Drops the session with `server` if it is still `opened`.
    async fn forget(&self, server: &str, opened: &Opened) {
        if let Some(slot) = self.upstream.get(server) {
            let mut slot = slot.lock().await;
            if slot.as_ref().map(|o| &o.id) == Some(&opened.id) {
                *slot = None;
            }
        }
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The server a `tools/call` (`tool`) or `prompts/get` names, and the name
/// that server knows it by, put in `params`.
fn by_name(tool: bool) -> impl Fn(&Inner, &mut Value) -> Result<String, String> {
    move |inner, params| {
        let unknown =
            |name: &str| format!("unknown {} '{name}'", if tool { "tool" } else { "prompt" });
        let name = params["name"].as_str().unwrap_or_default().to_owned();
        let (server, own) = split(&inner.servers, &name).ok_or_else(|| unknown(&name))?;
        if tool && !inner.filter.offers(&name) {
            return Err(unknown(&name));
        }
        let (server, own) = (server.to_owned(), own.to_owned());
        params["name"] = json!(own);
        Ok(server)
    }
}

/// The server a resource's URI names, and its own URI, put in `params`.
fn by_uri(inner: &Inner, params: &mut Value) -> Result<String, String> {
    let uri = params["uri"].as_str().unwrap_or_default().to_owned();
    let (server, own) = unprefix(&inner.servers, &uri)
        .ok_or_else(|| format!("unknown resource '{uri}': it is `{SCHEME}<server>/<uri>`"))?;
    let (server, own) = (server.to_owned(), own.to_owned());
    params["uri"] = json!(own);
    Ok(server)
}

/// The server a name `{server}__{name}` starts with (the longest, if
/// several do) and the rest.
fn split<'a>(servers: &'a [String], name: &'a str) -> Option<(&'a str, &'a str)> {
    let found = servers.iter().filter_map(|server| {
        let rest = name.strip_prefix(server.as_str())?.strip_prefix(SEP)?;
        (!rest.is_empty()).then_some((server.as_str(), rest))
    });
    found.max_by_key(|(server, _)| server.len())
}

/// `uri` of `server` as the client has it.
fn prefixed(server: &str, uri: &str) -> String {
    format!("{SCHEME}{server}/{uri}")
}

/// The server a prefixed URI names, and its own URI.
fn unprefix<'a>(servers: &'a [String], uri: &'a str) -> Option<(&'a str, &'a str)> {
    let (server, own) = uri.strip_prefix(SCHEME)?.split_once('/')?;
    let server = servers.iter().find(|s| *s == server)?;
    (!own.is_empty()).then_some((server.as_str(), own))
}

fn prefix_field(server: &str, value: &mut Value, key: &str) {
    if let Some(Value::String(uri)) = value.get_mut(key) {
        *uri = prefixed(server, uri);
    }
}

/// A content item of a tool's or prompt's result: the URI it links to or
/// holds, prefixed.
fn prefix_content(server: &str, item: &mut Value) {
    match item["type"].as_str() {
        Some("resource_link") => prefix_field(server, item, "uri"),
        Some("resource") => prefix_field(server, &mut item["resource"], "uri"),
        _ => {}
    }
}

/// An output of `server`'s answer to a routed request, as the client has
/// it: URIs prefixed in a result, requests of the server's own left out
/// (the client has none to answer them), and no event id of the server's.
fn passed(server: &str, mut output: Output) -> Output {
    output.session = None;
    output.event_id = None;
    output.event = None;
    let messages = std::mem::take(&mut output.messages);
    output.messages = messages
        .into_iter()
        .filter_map(|m| match m {
            Message::Request { .. } => None,
            Message::Notification { .. } => notified(server, m),
            m => Some(rewrite_result(server, m)),
        })
        .collect();
    output
}

/// A response with the URIs in its result prefixed: those of
/// `resources/read`'s `contents`, and of the content of a tool's result or
/// a prompt's messages.
fn rewrite_result(server: &str, mut m: Message) -> Message {
    if let Some(result) = m.result_mut() {
        for content in result["contents"].as_array_mut().into_iter().flatten() {
            prefix_field(server, content, "uri");
        }
        for item in result["content"].as_array_mut().into_iter().flatten() {
            prefix_content(server, item);
        }
        for message in result["messages"].as_array_mut().into_iter().flatten() {
            prefix_content(server, &mut message["content"]);
        }
    }
    m
}

/// A notification of `server`'s as the client has it: `resources/updated`
/// with the URI prefixed.
fn notified(server: &str, mut m: Message) -> Option<Message> {
    if let Message::Notification { method, params, .. } = &mut m
        && method == "notifications/resources/updated"
        && let Some(params) = params
    {
        prefix_field(server, params, "uri");
    }
    Some(m)
}

/// What a server tells on its own stream that the client is told: that its
/// lists changed, a resource changed, or a log message. The rest is meant for
/// a request of its own.
fn told(server: &str, output: Output) -> Option<Output> {
    let messages: Vec<Message> = output
        .messages
        .into_iter()
        .filter(|m| {
            matches!(
                m.method(),
                Some(
                    "notifications/tools/list_changed"
                        | "notifications/prompts/list_changed"
                        | "notifications/resources/list_changed"
                        | "notifications/resources/updated"
                        | "notifications/message"
                )
            ) && m.request_id().is_none()
        })
        .filter_map(|m| notified(server, m))
        .collect();
    (!messages.is_empty()).then(|| Output::new(messages))
}

/// The capabilities a client sees: those any server has.
fn merge_capabilities(all: &[Value]) -> Value {
    let any = |capability: &str, flag: &str| {
        all.iter()
            .any(|c| c[capability][flag].as_bool().unwrap_or(false))
    };
    let has = |capability: &str| all.iter().any(|c| c.get(capability).is_some());
    let mut merged = Map::new();
    for capability in ["tools", "prompts", "resources"] {
        if !has(capability) {
            continue;
        }
        let mut flags = Map::new();
        for flag in ["listChanged", "subscribe"] {
            let applies = flag == "listChanged" || capability == "resources";
            if applies && any(capability, flag) {
                flags.insert(flag.to_owned(), json!(true));
            }
        }
        merged.insert(capability.to_owned(), Value::Object(flags));
    }
    Value::Object(merged)
}

/// The answer to request `id`: a result, or an error code and message.
fn reply(id: &Value, outcome: Result<Value, (i64, String)>) -> Output {
    let json = match outcome {
        Ok(result) => json!({ "jsonrpc": "2.0", "id": id, "result": result }),
        Err((code, why)) => respan_gateway_mcp::error(id.clone(), code, &why),
    };
    let message = Message::try_from(json).unwrap_or_else(|_| Message::Response {
        id: None,
        outcome: Outcome::Error(RpcError {
            code: -32603,
            message: "the answer could not be built".into(),
            data: None,
            extensions: Map::new(),
        }),
        extensions: Map::new(),
    });
    Output::new(vec![message])
}

/// A session id: 128 random bits as hex, visible ASCII as MCP requires.
fn session_id() -> Result<String, Fault> {
    let mut bits = [0u8; 16];
    getrandom::fill(&mut bits)
        .map_err(|e| Fault::internal("no secure random source").with_source(e))?;
    Ok(bits.iter().map(|b| format!("{b:02x}")).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| (*s).to_owned()).collect()
    }

    #[test]
    fn a_name_belongs_to_the_longest_server_it_starts_with() {
        let servers = names(&["a", "a__b", "files"]);
        assert_eq!(split(&servers, "files__read"), Some(("files", "read")));
        assert_eq!(split(&servers, "a__b__x"), Some(("a__b", "x")));
        assert_eq!(split(&servers, "a__c"), Some(("a", "c")));
        assert_eq!(split(&servers, "a__"), None);
        assert_eq!(split(&servers, "nothing"), None);
    }

    #[test]
    fn a_resource_uri_says_its_server() {
        let servers = names(&["fs", "gh"]);
        let uri = prefixed("fs", "file:///a/b.txt");
        assert_eq!(uri, "mcp://fs/file:///a/b.txt");
        assert_eq!(unprefix(&servers, &uri), Some(("fs", "file:///a/b.txt")));
        assert_eq!(unprefix(&servers, "mcp://other/x"), None);
        assert_eq!(unprefix(&servers, "file:///x"), None);
        assert_eq!(unprefix(&servers, "mcp://fs/"), None);
    }

    #[test]
    fn capabilities_are_what_any_server_has() {
        let merged = merge_capabilities(&[
            json!({ "tools": {} }),
            json!({ "tools": { "listChanged": true }, "resources": { "subscribe": true } }),
        ]);
        assert_eq!(
            merged,
            json!({ "tools": { "listChanged": true }, "resources": { "subscribe": true } })
        );
        assert_eq!(merge_capabilities(&[json!({})]), json!({}));
    }

    #[test]
    fn a_servers_own_stream_tells_only_what_changed() {
        let note = |method: &str, params: Value| {
            let json = json!({ "jsonrpc": "2.0", "method": method, "params": params });
            Message::try_from(json).unwrap_or_else(|e| panic!("{e}"))
        };
        let output = Output::new(vec![
            note("notifications/tools/list_changed", json!({})),
            note("notifications/progress", json!({ "progress": 1 })),
            note(
                "notifications/resources/updated",
                json!({ "uri": "file:///x" }),
            ),
        ]);
        let told = told("fs", output).unwrap_or_default();
        assert_eq!(told.messages.len(), 2);
        assert_eq!(
            told.messages[1].params().map(|p| p["uri"].clone()),
            Some(json!("mcp://fs/file:///x"))
        );
    }

    #[test]
    fn uris_in_a_result_are_prefixed() {
        let json = json!({ "jsonrpc": "2.0", "id": 1, "result": {
            "content": [
                { "type": "resource_link", "uri": "file:///a" },
                { "type": "resource", "resource": { "uri": "file:///b", "text": "t" } },
                { "type": "text", "text": "file:///c" },
            ],
            "contents": [{ "uri": "file:///d", "text": "t" }],
        }});
        let m = Message::try_from(json).unwrap_or_else(|e| panic!("{e}"));
        let Message::Response {
            outcome: Outcome::Success(result),
            ..
        } = rewrite_result("s", m)
        else {
            panic!("a result")
        };
        assert_eq!(result["content"][0]["uri"], "mcp://s/file:///a");
        assert_eq!(result["content"][1]["resource"]["uri"], "mcp://s/file:///b");
        assert_eq!(result["content"][2]["text"], "file:///c");
        assert_eq!(result["contents"][0]["uri"], "mcp://s/file:///d");
    }
}
