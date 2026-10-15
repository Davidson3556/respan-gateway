//! One client request's work: the methods of an MCP endpoint, and the
//! client's session.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use respan_gateway_core::futures::future::join_all;
use respan_gateway_core::futures::stream::{self, select_all};
use respan_gateway_core::futures::{StreamExt, future};
use respan_gateway_core::{Call, ErrorCode, Fault};
use respan_gateway_http::http::{Method, StatusCode};
use respan_gateway_http_faults::Status;
use respan_gateway_mcp::{Input, Message, Output, RpcCode};
use serde_json::{Value, json};

use super::rewrite::{by_name, by_uri, merge_capabilities, reply, told};
use super::session::{Inner, Opened, Session, lock, session_id};
use super::{IDLE, Outputs, SESSIONS, VERSIONS, message};
use respan_gateway_plugin_tool_loop::ToolScope;

type BoxFuture<T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send>>;

/// One client request's work.
#[derive(Clone)]
pub(super) struct Run {
    pub(super) inner: Arc<Inner>,
    /// A call to each server, to make copies of.
    pub(super) calls: Arc<HashMap<String, Call>>,
    /// The tools the client key limits its requests to, if it does.
    pub(super) scope: Option<ToolScope>,
}

impl Run {
    pub(super) async fn handle(self, input: Input) -> Result<Outputs, Fault> {
        match input.method {
            Method::POST => self.post(input).await,
            Method::GET => self.listen(input).await,
            Method::DELETE => self.end(input).await,
            _ => Err(Fault::bad_request("not a method of an MCP endpoint")),
        }
    }

    pub(super) async fn post(self, input: Input) -> Result<Outputs, Fault> {
        let hello =
            |m: &Message| matches!(m, Message::Request { method, .. } if method == "initialize");
        if input.messages.iter().any(hello) {
            return match input.messages.as_slice() {
                [one] => self.initialize(one).await,
                _ => Err(Fault::bad_request("`initialize` is not batched")),
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

    // The session `input` names, else no such session.
    pub(super) fn session(&self, input: &Input) -> Result<Arc<Session>, Fault> {
        let missing = || Fault::bad_request("the Mcp-Session-Id header is required");
        let id = input.session.as_deref().ok_or_else(missing)?;
        let mut sessions = lock(&self.inner.sessions);
        let found = sessions.get(id);
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
    pub(super) async fn initialize(self, hello: &Message) -> Result<Outputs, Fault> {
        let Message::Request { id, params, .. } = hello else {
            return Err(Fault::bad_request("`initialize` is a request"));
        };
        let id: Value = id.clone().into();
        let asked = params.as_ref().and_then(|p| p["protocolVersion"].as_str());
        let version = VERSIONS
            .iter()
            .find(|v| Some(**v) == asked)
            .unwrap_or(&VERSIONS[0]);
        let session = Arc::new(Session {
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
            return Err(Fault::upstream(format!(
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
    pub(super) async fn end(self, input: Input) -> Result<Outputs, Fault> {
        let session = self.session(&input)?;
        let name = input.session.unwrap_or_default();
        lock(&self.inner.sessions).shift_remove(&name);
        self.hang_up(&session).await;
        Ok(stream::empty().boxed())
    }

    /// Ends the sessions `session` holds with the servers.
    pub(super) async fn hang_up(&self, session: &Arc<Session>) {
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
    }

    /// Ends every client's session, and the servers' with them.
    pub(super) async fn end_all(&self) {
        let all: Vec<Arc<Session>> = lock(&self.inner.sessions)
            .drain(..)
            .map(|(_, session)| session)
            .collect();
        join_all(all.iter().map(|session| self.hang_up(session))).await;
    }

    /// The servers' own streams, merged: what a client is told unasked.
    pub(super) async fn listen(self, input: Input) -> Result<Outputs, Fault> {
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
    pub(super) async fn notify(&self, session: &Arc<Session>, method: &str, params: Option<Value>) {
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
    pub(super) fn request(
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
}

impl Run {
    /// Whether the client may be offered, and may call, the tool `name`
    /// (`{server}__{tool}`): within `allow` and `deny` and its key's scope.
    pub(super) fn offers(&self, name: &str) -> bool {
        let scoped = self.scope.as_ref().is_none_or(|s| s.offers(name));
        scoped && self.inner.filter.offers(name)
    }
}
