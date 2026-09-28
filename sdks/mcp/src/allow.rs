//! A tool allow-list: `tools/list` shows only allowed tools, and a
//! `tools/call` of any other is refused in JSON-RPC's own error shape,
//! inside a `200` as MCP clients expect. Traffic of no kind, which it
//! cannot read, is refused; other kinds pass.

use std::collections::HashSet;
use std::sync::Arc;

use respan_gateway_core::futures::stream;
use respan_gateway_core::http::StatusCode;
use respan_gateway_core::serde_json::Value;
use respan_gateway_core::{ClientContext, Fault, Filter, Plugin, Response, async_trait};

use crate::format::rpc_fault;
use crate::kind::{Message, ToClient, ToServer, rpc};

/// The code of a refusal of traffic the allow-list cannot read.
pub const UNCOVERED: &str = "mcp_uncovered";

/// Lets clients see and call only the named tools.
#[derive(Debug, Clone)]
pub struct ToolAllowList {
    allowed: Arc<HashSet<String>>,
}

impl ToolAllowList {
    /// Allow exactly `tools`.
    pub fn new<S: Into<String>>(tools: impl IntoIterator<Item = S>) -> Self {
        Self {
            allowed: Arc::new(tools.into_iter().map(Into::into).collect()),
        }
    }

    fn allows(&self, m: &Message) -> bool {
        let tool = m.params()["name"].as_str().unwrap_or_default();
        !(m.is_request() && m.method() == Some("tools/call")) || self.allowed.contains(tool)
    }
}

fn not_allowed(m: &Message) -> String {
    let tool = m.params()["name"].as_str().unwrap_or_default();
    format!("tool '{tool}' is not allowed")
}

/// The JSON-RPC error for a refused call in a batch.
fn refusal(m: &Message) -> ToClient {
    let id = m.id().cloned().unwrap_or_default();
    ToClient(Message::error(
        id,
        rpc::INVALID_PARAMS,
        &not_allowed(m),
        None,
    ))
}

/// A refusal of the whole request as a JSON-RPC error inside a `200`: the
/// client's format writes it for each of its requests.
fn refuse(m: &Message) -> Fault {
    rpc_fault(StatusCode::OK, rpc::INVALID_PARAMS, not_allowed(m))
}

/// A plugin's answer of these messages, `200 OK`.
pub fn answer(messages: Vec<ToClient>) -> Response {
    Response::stream(stream::iter(messages.into_iter().map(Ok)))
}

#[async_trait]
impl Plugin for ToolAllowList {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        // Relayed bytes could be MCP it cannot read: fail closed.
        if cx.is_kind::<()>() {
            return Err(Fault::new(
                StatusCode::FORBIDDEN,
                UNCOVERED,
                "traffic the tool allow-list cannot read",
            ));
        }
        if !cx.is_kind::<ToServer>() {
            return Ok(None);
        }
        let input = cx.input::<ToServer>()?;
        let refused: Vec<Message> = input
            .messages
            .iter()
            .filter(|m| !self.allows(m))
            .cloned()
            .collect();
        let lists: Vec<Value> = input
            .messages
            .iter()
            .filter(|m| m.is_request() && m.method() == Some("tools/list"))
            .filter_map(|m| m.id().cloned())
            .collect();
        // One refused request: refused, keeping JSON-RPC's 200.
        if let (Some(_), [m]) = (input.single(), &refused[..]) {
            return Err(refuse(m));
        }
        if !refused.is_empty() {
            if refused.len() == input.messages.len() {
                return Ok(Some(answer(refused.iter().map(refusal).collect())));
            }
            // A batch: the refused requests are dropped and answered at the
            // end of the server's reply.
            let this = self.clone();
            cx.filter(move |input: &mut ToServer| {
                input.messages.retain(|m| this.allows(m));
                Ok(())
            })?;
            cx.filter(Append(refused.iter().map(refusal).collect()))?;
        }
        if !lists.is_empty() {
            let allowed = self.allowed.clone();
            cx.filter(move |m: &mut ToClient| {
                if !m.0.id().is_some_and(|id| lists.contains(id)) {
                    return Ok(());
                }
                let shows = |t: &Value| allowed.contains(t["name"].as_str().unwrap_or_default());
                let tools = &m.0.json()["result"]["tools"];
                if tools.as_array().is_some_and(|t| !t.iter().all(shows))
                    && let Some(tools) = m.0.json_mut()["result"]["tools"].as_array_mut()
                {
                    tools.retain(shows);
                }
                Ok(())
            })?;
        }
        Ok(None)
    }
}

/// Passes every message, then adds its own at the end.
struct Append(Vec<ToClient>);

impl Filter<ToClient> for Append {
    fn edit(&mut self, message: ToClient, out: &mut Vec<ToClient>) -> Result<(), Fault> {
        out.push(message);
        Ok(())
    }

    fn end(&mut self, _end: &Result<(), Fault>, out: &mut Vec<ToClient>) -> Result<(), Fault> {
        out.append(&mut self.0);
        Ok(())
    }
}
