//! Several MCP servers behind one endpoint. The gateway keeps its own
//! session per client, mapped to one session per server; `tools/list`
//! merges every server's tools under `{server}__{tool}`, and `tools/call`
//! goes to the server its prefix names.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use respan_gateway_core::futures::future::join_all;
use respan_gateway_core::futures::{TryStreamExt, stream};
use respan_gateway_core::http::{HeaderValue, StatusCode};
use respan_gateway_core::serde_json::{Map, Value, json};
use respan_gateway_core::{ClientContext, Fault, Plugin, ProviderContext, Response, async_trait};

use crate::allow::answer;
use crate::format::{SESSION, rpc_fault};
use crate::kind::{Message, ToClient, ToServer, Verb, rpc};

const SEPARATOR: &str = "__";

/// Upstream session ids, by server, of one gateway session.
type Upstreams = HashMap<String, HeaderValue>;

/// Aggregates the MCP servers named, which must be providers of an MCP
/// format. Register it last: it answers most requests itself.
#[derive(Debug, Clone)]
pub struct Aggregate {
    servers: Arc<Vec<String>>,
    sessions: Arc<Mutex<HashMap<String, Upstreams>>>,
    minted: Arc<AtomicU64>,
}

impl Aggregate {
    /// Aggregate these providers, each named as its tools' prefix.
    pub fn new<S: Into<String>>(servers: impl IntoIterator<Item = S>) -> Self {
        Self {
            servers: Arc::new(servers.into_iter().map(Into::into).collect()),
            sessions: Arc::default(),
            minted: Arc::default(),
        }
    }

    fn upstreams(&self, session: &str) -> Option<Upstreams> {
        let sessions = self.sessions.lock().unwrap_or_else(PoisonError::into_inner);
        sessions.get(session).cloned()
    }

    /// Every server's events for the client's request, sent as is.
    async fn fan_out(&self, cx: &mut ClientContext) -> Result<Vec<(String, Response)>, Fault> {
        let calls = self.servers.iter().map(|s| {
            let call = cx.call(s.as_str());
            async move { Ok::<_, Fault>((s.clone(), call.await?)) }
        });
        join_all(calls).await.into_iter().collect()
    }

    async fn initialize(
        &self,
        cx: &mut ClientContext,
        request: &Message,
    ) -> Result<Response, Fault> {
        let mut capabilities = Map::new();
        let mut upstreams = Upstreams::new();
        for (server, resp) in self.fan_out(cx).await? {
            if !resp.head().status.is_success() {
                let status = resp.head().status;
                return Err(Fault::upstream(format!("{server} answered {status}")));
            }
            if let Some(id) = resp.head().headers.get(SESSION) {
                upstreams.insert(server.clone(), id.clone());
            }
            for event in resp.outputs::<ToClient>().try_collect::<Vec<_>>().await? {
                let caps = &event.0.json()["result"]["capabilities"];
                for (k, v) in caps.as_object().into_iter().flatten() {
                    capabilities.entry(k.clone()).or_insert_with(|| v.clone());
                }
            }
        }
        let session = format!("agg-{}", self.minted.fetch_add(1, Ordering::Relaxed) + 1);
        let mut sessions = self.sessions.lock().unwrap_or_else(PoisonError::into_inner);
        sessions.insert(session.clone(), upstreams);
        drop(sessions);
        let version = request.params()["protocolVersion"].clone();
        let result = json!({
            "protocolVersion": version,
            "capabilities": capabilities,
            "serverInfo": { "name": "respan-aggregate", "version": "0" },
        });
        let id = request.id().cloned().unwrap_or_default();
        let session = HeaderValue::try_from(session)
            .map_err(|e| Fault::internal("bad session id").with_source(e))?;
        cx.edit_response_head(move |head| {
            head.headers.insert(SESSION, session);
            Ok(())
        });
        Ok(answer(vec![ToClient(Message::result(id, result))]))
    }

    async fn tools_list(
        &self,
        cx: &mut ClientContext,
        request: &Message,
    ) -> Result<Response, Fault> {
        let mut tools = Vec::new();
        for (server, resp) in self.fan_out(cx).await? {
            for event in resp.outputs::<ToClient>().try_collect::<Vec<_>>().await? {
                let listed = event.0.json()["result"]["tools"].as_array().cloned();
                for mut tool in listed.into_iter().flatten() {
                    let name = tool["name"].as_str().unwrap_or_default();
                    tool["name"] = format!("{server}{SEPARATOR}{name}").into();
                    tools.push(tool);
                }
            }
        }
        let id = request.id().cloned().unwrap_or_default();
        Ok(answer(vec![ToClient(Message::result(
            id,
            json!({ "tools": tools }),
        ))]))
    }

    /// An empty answer of `status`, after every server took the request.
    async fn everywhere(
        &self,
        cx: &mut ClientContext,
        status: StatusCode,
    ) -> Result<Response, Fault> {
        for (_, resp) in self.fan_out(cx).await? {
            resp.outputs::<ToClient>().try_collect::<Vec<_>>().await?;
        }
        Ok(answer(Vec::new()).with_status(status))
    }
}

#[async_trait]
impl Plugin for Aggregate {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        if !cx.is_kind::<ToServer>() {
            return Ok(None);
        }
        let input = cx.input::<ToServer>()?.clone();
        let session = cx.head().headers.get(SESSION).and_then(|v| v.to_str().ok());
        let session = session.map(str::to_owned);
        if let Some(request) = input.single().filter(|m| m.method() == Some("initialize")) {
            return self.initialize(cx, request).await.map(Some);
        }
        let Some(session) = session.filter(|s| self.upstreams(s).is_some()) else {
            let text = "unknown session: initialize again";
            return Err(rpc_fault(StatusCode::NOT_FOUND, rpc::INVALID_REQUEST, text));
        };
        match input.verb {
            // The servers' own streams, merged as they come; the format
            // opens the stream, so its head goes out though servers are quiet.
            Verb::Listen => {
                let streams = self.fan_out(cx).await?;
                let merged = stream::select_all(streams.into_iter().map(|(_, r)| r.outputs()));
                Ok(Some(Response::stream::<ToClient, _>(merged)))
            }
            Verb::End => {
                let answer = self.everywhere(cx, StatusCode::OK).await?;
                let mut sessions = self.sessions.lock().unwrap_or_else(PoisonError::into_inner);
                sessions.remove(&session);
                Ok(Some(answer))
            }
            Verb::Send if input.request_ids().next().is_none() => {
                self.everywhere(cx, StatusCode::ACCEPTED).await.map(Some)
            }
            Verb::Send => {
                let Some(request) = input.single() else {
                    let text = "batches are not aggregated";
                    return Err(rpc_fault(
                        StatusCode::BAD_REQUEST,
                        rpc::INVALID_REQUEST,
                        text,
                    ));
                };
                match request.method() {
                    Some("tools/list") => self.tools_list(cx, request).await.map(Some),
                    Some("ping") => {
                        let id = request.id().cloned().unwrap_or_default();
                        Ok(Some(answer(vec![ToClient(Message::result(id, json!({})))])))
                    }
                    Some("tools/call") => {
                        let name = request.params()["name"].as_str().unwrap_or_default();
                        let routed = name
                            .split_once(SEPARATOR)
                            .filter(|(server, _)| self.servers.iter().any(|s| s == server));
                        let Some((server, tool)) = routed else {
                            let id = request.id().cloned().unwrap_or_default();
                            let text = format!("unknown tool '{name}'");
                            let error = Message::error(id, rpc::INVALID_PARAMS, &text, None);
                            return Ok(Some(answer(vec![ToClient(error)])));
                        };
                        let (server, tool) = (server.to_owned(), Value::from(tool));
                        cx.set_target(server);
                        cx.filter(move |input: &mut ToServer| {
                            input.messages[0].json_mut()["params"]["name"] = tool.clone();
                            Ok(())
                        })?;
                        Ok(None)
                    }
                    // Anything else goes to the first server.
                    _ => {
                        let first = self.servers.first().cloned().unwrap_or_default();
                        cx.set_target(first);
                        Ok(None)
                    }
                }
            }
        }
    }

    /// Each call carries its own server's session id.
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        let session = px.client().head().headers.get(SESSION);
        let session = session.and_then(|v| v.to_str().ok());
        let upstream = session
            .and_then(|s| self.upstreams(s))
            .and_then(|u| u.get(px.provider()).cloned());
        if let Some(id) = upstream {
            px.edit_call(move |call| {
                call.headers_mut().insert(SESSION, id);
                Ok(())
            });
        }
        Ok(())
    }
}
