//! MCP proxy: MCP clients speak streamable HTTP to the gateway, which lets
//! them see and call only the tools on an allow-list.
//!
//! `cargo run -p respan-gateway-examples --example mcp_proxy` runs it
//! against a mock MCP server with tools `echo` and `wipe`. Add
//! `-- --stdio PROGRAM [ARGS...]` to run a real server as a child process
//! instead, such as
//! `-- --stdio npx -y @modelcontextprotocol/server-everything`.
//!
//! Nothing is relayed: the session the server assigns, and every header MCP
//! carries, travel in the kind ([`Input`], [`Output`]), so the
//! allow-list's filters see and edit every call.
//!
//! Not built: realtime WebSocket sessions and per-turn billing (use case 7).

use std::sync::{Arc, Mutex, PoisonError};

use respan_gateway_core::futures::stream;
use respan_gateway_core::{ClientContext, Fault, Gateway, Plugin, Response, async_trait};
use respan_gateway_http::http::{HeaderValue, StatusCode};
use serde_json::{Value, json};

use respan_gateway_http::HttpTransport;
use respan_gateway_mcp::{self as mcp, SESSION, StdioTransport, StreamableHttp};
use respan_gateway_mcp::{Input, Output};
#[allow(dead_code)]
#[path = "../../tests/support/http.rs"]
mod http_support;
use http_support::{post, read_whole, serve};

const PROTOCOL_VERSION: &str = "2025-06-18";

/// Lets clients see and call only these tools.
struct ToolAllowList(Arc<[&'static str]>);

fn allows(allowed: &[&str], tool: &Value) -> bool {
    allowed.iter().any(|a| tool == a)
}

#[async_trait]
impl Plugin for ToolAllowList {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        if !cx.is_kind::<Input>() {
            return Ok(None);
        }
        let input = cx.input::<Input>()?;
        let calls = input
            .messages
            .iter()
            .filter(|m| m.method() == Some("tools/call"));
        let refused = calls
            .filter_map(|m| m.params().map(|p| &p["name"]))
            .find(|t| !allows(&self.0, t));
        if let Some(tool) = refused {
            // Every request it posts is answered with a JSON-RPC error; the
            // server never sees it.
            let message = format!("tool {tool} is not allowed");
            let invalid_params = |id: Value| {
                mcp::error(id, -32602, &message)
                    .try_into()
                    .expect("valid RPC error")
            };
            let answer = Output::new(input.request_ids().map(invalid_params).collect());
            return Ok(Some(Response::stream(stream::iter([Ok(answer)]))));
        }
        let allowed = self.0.clone();
        cx.filter(move |out: &mut Output| {
            let lists = out.messages.iter_mut();
            let lists = lists.filter_map(|m| m.result_mut()?.get_mut("tools")?.as_array_mut());
            for tools in lists {
                tools.retain(|t| allows(&allowed, &t["name"]));
            }
            Ok(())
        })?;
        Ok(None)
    }
}

fn gateway(
    server: impl Fn(respan_gateway_core::Extensions) -> respan_gateway_http::ProviderIo
    + Send
    + Sync
    + 'static,
) -> Result<Gateway, Fault> {
    Gateway::new()
        .provider::<Input, _, _>("tools", StreamableHttp::default(), server)?
        .client::<Input, _, _>(StreamableHttp::default())?
        .plugin(ToolAllowList(Arc::from(["echo"])))
        .target("tools")
}

/// A small real MCP HTTP server for this example's three methods. Unlike a
/// transport registry, it checks the request method and session and returns
/// JSON-RPC responses derived from the received request id.
type McpRequests = Arc<Mutex<Vec<(Value, Option<String>)>>>;

struct McpServer {
    url: String,
    #[allow(dead_code)]
    seen: McpRequests,
    task: tokio::task::JoinHandle<()>,
}

impl McpServer {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind MCP provider");
        let url = format!(
            "http://{}",
            listener.local_addr().expect("MCP provider address")
        );
        let seen = Arc::new(Mutex::new(Vec::new()));
        let requests = seen.clone();
        let app = axum::Router::new().route(
            "/mcp",
            axum::routing::post(move |req: respan_gateway_http::http::Request<axum::body::Body>| {
                let requests = requests.clone();
                async move {
                    let session = req.headers().get(SESSION)
                        .and_then(|v| v.to_str().ok()).map(str::to_owned);
                    let raw = axum::body::to_bytes(req.into_body(), 1_048_576).await
                        .unwrap_or_default();
                    let body: Value = serde_json::from_slice(&raw).unwrap_or_default();
                    let method = body["method"].as_str().unwrap_or_default();
                    requests.lock().unwrap_or_else(PoisonError::into_inner).push((body.clone(), session.clone()));
                    if method == "notifications/initialized" && session.as_deref() == Some("s1") && body.get("id").is_none() {
                        return (StatusCode::ACCEPTED, [("content-type", "application/json"), ("mcp-session-id", "s1")], String::new());
                    }
                    let result = match method {
                        "initialize" if session.is_none() && body["params"]["protocolVersion"] == PROTOCOL_VERSION && body["params"]["clientInfo"]["name"].is_string() && body["params"]["capabilities"].is_object() =>
                            json!({ "protocolVersion": PROTOCOL_VERSION, "capabilities": { "tools": {} }, "serverInfo": {"name":"gateway-example", "version":"1"} }),
                        "tools/list" if session.as_deref() == Some("s1") => json!({ "tools": [
                            { "name": "echo", "inputSchema": {"type":"object", "properties":{"message":{"type":"string"}}, "required":["message"]} },
                            { "name": "wipe", "inputSchema": {"type":"object"} }
                        ] }),
                        "tools/call" if session.as_deref() == Some("s1") && body["params"]["name"] == "echo" =>
                            json!({ "content": [{ "type": "text", "text": body["params"]["arguments"]["message"] }] }),
                        _ => return (StatusCode::BAD_REQUEST, [("content-type", "application/json"), ("mcp-session-id", "s1")], "invalid method or session".to_owned()),
                    };
                    let answer = json!({ "jsonrpc": "2.0", "id": body["id"], "result": result });
                    (StatusCode::OK, [("content-type", "application/json"), ("mcp-session-id", "s1")], answer.to_string())
                }
            }),
        );
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self { url, seen, task }
    }

    #[cfg(test)]
    fn requests(&self) -> Vec<(Value, Option<String>)> {
        self.seen
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl Drop for McpServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// An MCP client's request, in `session` if given: the status, the session
/// and the matching response. Interleaved notifications are skipped.
async fn send(
    gw: &Gateway,
    session: Option<&str>,
    id: Option<u64>,
    method: &str,
    params: Value,
) -> Result<(StatusCode, Option<String>, Value), Fault> {
    let mut body = json!({ "jsonrpc": "2.0", "method": method, "params": params });
    if let Some(id) = id {
        body["id"] = json!(id);
    }
    let mut req = post("/mcp", body.to_string());
    req.headers_mut().insert(
        "accept",
        HeaderValue::from_static("application/json, text/event-stream"),
    );
    req.headers_mut().insert(
        "mcp-protocol-version",
        HeaderValue::from_static(PROTOCOL_VERSION),
    );
    if let Some(session) = session.and_then(|s| HeaderValue::from_str(s).ok()) {
        req.headers_mut().insert(SESSION, session);
    }
    let (reply, finished) = serve(gw, req).await;
    let (status, headers, body) = read_whole(reply).await;
    finished.await?;
    let session = headers
        .get(SESSION)
        .and_then(|s| s.to_str().ok())
        .map(String::from);
    let body = String::from_utf8_lossy(&body?).into_owned();
    let first = body
        .split_inclusive("\n\n")
        .filter_map(respan_gateway_http::formats::json)
        .find(|message| {
            id.is_some_and(|id| message["id"] == id) && message.get("method").is_none()
        });
    Ok((status, session, first.unwrap_or_default()))
}

/// The full MCP initialization sequence, before any tool requests.
async fn initialize(gw: &Gateway) -> Result<(StatusCode, Option<String>, Value), Fault> {
    let initialized = send(
        gw,
        None,
        Some(1),
        "initialize",
        json!({
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {},
            "clientInfo": {"name":"gateway-example-client", "version":"1"}
        }),
    )
    .await?;
    if initialized.2["result"]["protocolVersion"] != PROTOCOL_VERSION {
        return Err(respan_gateway_core::Upstream::fault(
            "the MCP server did not negotiate the requested protocol",
        ));
    }
    let (status, _, _) = send(
        gw,
        initialized.1.as_deref(),
        None,
        "notifications/initialized",
        json!({}),
    )
    .await?;
    if status != StatusCode::ACCEPTED {
        return Err(respan_gateway_core::Upstream::fault(
            "the MCP server did not accept initialization",
        ));
    }
    Ok(initialized)
}

/// A client's session: it starts one, lists the tools, calls the allowed
/// tool and then one that is not; each answer, in order.
async fn session(gw: &Gateway) -> Result<Vec<(StatusCode, Value)>, Fault> {
    let (status, session, init) = initialize(gw).await?;
    let mut answers = vec![(status, init)];
    let echo = json!({ "name": "echo", "arguments": { "message": "hi" } });
    for (id, method, params) in [
        (2, "tools/list", json!({})),
        (3, "tools/call", echo),
        (4, "tools/call", json!({ "name": "wipe" })),
    ] {
        let (status, _, answer) = send(gw, session.as_deref(), Some(id), method, params).await?;
        answers.push((status, answer));
    }
    Ok(answers)
}

#[tokio::main]
async fn main() -> Result<(), Fault> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let fixture = match args.iter().position(|a| a == "--stdio") {
        Some(_) => None,
        None => Some(McpServer::start().await),
    };
    let gw = match args.iter().position(|a| a == "--stdio") {
        Some(i) => {
            let (program, rest) = args[i + 1..]
                .split_first()
                .ok_or_else(|| Fault::config("--stdio PROGRAM"))?;
            gateway(StdioTransport::new(program, rest).connect())?
        }
        None => {
            gateway(HttpTransport::new(&fixture.as_ref().expect("started fixture").url)?.connect())?
        }
    };
    let methods = [
        "initialize",
        "tools/list",
        "tools/call echo",
        "tools/call wipe",
    ];
    for (method, (status, answer)) in methods.iter().zip(session(&gw).await?) {
        println!("{method} -> {status}: {answer}");
    }
    Ok(())
}

#[cfg(test)]
#[path = "../tests/cases/integrations/mcp_proxy.rs"]
mod tests;
