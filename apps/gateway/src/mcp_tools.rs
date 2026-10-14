//! What an MCP server offers, asked through the gateway as a client asks:
//! `initialize`, `tools/list` (every page), then the session ended. Asked
//! in process, so it takes the server's transport and credentials as any
//! call does, and, not a client's, is neither logged nor traced.

use std::time::Duration;

use respan_gateway_core::Gateway;
use respan_gateway_http::Body;
use respan_gateway_http::http::{self, HeaderValue, Method};
use respan_gateway_http::http_body_util::BodyExt;
use serde_json::{Value, json};

/// How long a server has to list its tools, a stdio server's start included.
pub const TIMEOUT: Duration = Duration::from_secs(20);
/// The most pages of tools read.
const PAGES: usize = 10;
/// The longest description kept of a tool, in characters.
const DESCRIPTION: usize = 600;

/// A tool `server` offers, as a person reads it.
pub fn tool_json(tool: &Value) -> Value {
    let description = tool["description"].as_str().unwrap_or_default();
    let description: String = description.chars().take(DESCRIPTION).collect();
    let hints = &tool["annotations"];
    json!({
        "name": tool["name"],
        "title": tool["title"].as_str().or(hints["title"].as_str()),
        "description": (!description.is_empty()).then_some(description),
        "read_only": hints["readOnlyHint"].as_bool(),
        "destructive": hints["destructiveHint"].as_bool(),
    })
}

/// The tools `server` lists now, or why it did not.
pub async fn list(gateway: &Gateway, server: &str) -> Result<Vec<Value>, String> {
    let hello = json!({
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": {
            "protocolVersion": "2025-06-18", "capabilities": {},
            "clientInfo": { "name": "respan-gateway-ui", "version": env!("CARGO_PKG_VERSION") }
        }
    });
    let (session, _) = rpc(gateway, server, None, Method::POST, Some(hello)).await?;
    let session = session.as_deref();
    let initialized = json!({ "jsonrpc": "2.0", "method": "notifications/initialized" });
    let listed = async {
        rpc(gateway, server, session, Method::POST, Some(initialized)).await?;
        let mut tools = Vec::new();
        let mut cursor: Option<String> = None;
        for page in 0..PAGES {
            let mut ask = json!({ "jsonrpc": "2.0", "id": page + 2, "method": "tools/list" });
            if let Some(cursor) = &cursor {
                ask["params"] = json!({ "cursor": cursor });
            }
            let (_, result) = rpc(gateway, server, session, Method::POST, Some(ask)).await?;
            let result = result.unwrap_or_default();
            tools.extend(result["tools"].as_array().into_iter().flatten().cloned());
            cursor = result["nextCursor"].as_str().map(str::to_owned);
            if cursor.is_none() {
                break;
            }
        }
        Ok(tools)
    }
    .await;
    if session.is_some() {
        let _ = rpc(gateway, server, session, Method::DELETE, None).await;
    }
    listed
}

/// One message to `server`: the session it names, and the result of a
/// request (one with an `id`).
async fn rpc(
    gateway: &Gateway,
    server: &str,
    session: Option<&str>,
    method: Method,
    message: Option<Value>,
) -> Result<(Option<String>, Option<Value>), String> {
    let id = message.as_ref().and_then(|m| m.get("id")).cloned();
    let body = message.map(|m| m.to_string()).unwrap_or_default();
    let mut request = http::Request::new(Body::full(body.into_bytes()));
    *request.method_mut() = method;
    *request.uri_mut() = format!("/mcp/{server}")
        .parse()
        .map_err(|_| format!("'{server}' is no server name"))?;
    let headers = request.headers_mut();
    headers.insert("content-type", HeaderValue::from_static("application/json"));
    let accept = HeaderValue::from_static("application/json, text/event-stream");
    headers.insert("accept", accept);
    headers.insert(
        "mcp-protocol-version",
        HeaderValue::from_static("2025-06-18"),
    );
    if let Some(session) = session.and_then(|s| HeaderValue::from_str(s).ok()) {
        headers.insert("mcp-session-id", session);
    }
    let (response, done) = respan_gateway_http::handle(gateway, request, 8 << 20).await;
    let status = response.status();
    let session = response.headers().get("mcp-session-id");
    let session = session.and_then(|s| s.to_str().ok()).map(str::to_owned);
    let (body, _) = tokio::join!(response.into_body().collect(), done);
    let body = body.map(|b| b.to_bytes()).unwrap_or_default();
    let text = String::from_utf8_lossy(&body);
    if !status.is_success() {
        let said: Value = serde_json::from_str(&text).unwrap_or_default();
        let said = said["error"]["message"].as_str().map(str::to_owned);
        let said = said.unwrap_or_else(|| text.chars().take(200).collect());
        return Err(match status.as_u16() {
            401 | 403 => format!("it refused the gateway's credentials ({status}): {said}"),
            _ => format!("it answered {status}: {said}"),
        });
    }
    let Some(id) = id else {
        return Ok((session, None));
    };
    // A JSON answer, or a stream of events, one of which answers `id`.
    let messages: Vec<Value> = match serde_json::from_str(&text) {
        Ok(message) => vec![message],
        Err(_) => (text.lines())
            .filter_map(|l| l.strip_prefix("data:"))
            .filter_map(|data| serde_json::from_str(data.trim()).ok())
            .collect(),
    };
    let answer = messages.into_iter().find(|m| m["id"] == id);
    let answer = answer.ok_or("it answered without a result")?;
    if let Some(message) = answer["error"]["message"].as_str() {
        return Err(format!("it refused: {message}"));
    }
    Ok((session, Some(answer["result"].clone())))
}
