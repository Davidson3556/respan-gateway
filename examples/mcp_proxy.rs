//! MCP proxy: MCP clients speak streamable HTTP to the gateway, which lets
//! them see and call only the tools on an allow-list.
//!
//! `cargo run --example mcp_proxy` runs it against a mock MCP server with
//! tools `echo` and `wipe`. Add `-- --stdio PROGRAM [ARGS...]` to run a real
//! server as a child process instead, such as
//! `-- --stdio npx -y @modelcontextprotocol/server-everything`.

use std::sync::Arc;

use respan_gateway_core::futures::stream;
use respan_gateway_core::http::{HeaderValue, StatusCode};
use respan_gateway_core::serde_json::{Value, json};
use respan_gateway_core::{
    ClientContext, Fault, Gateway, Plugin, Response, Transport, async_trait,
};
use respan_gateway_mcp::{
    self as mcp, SESSION, StdioTransport, StreamableHttp, ToClient, ToServer,
};
use respan_gateway_testkit::{MockTransport, mock, post, read_whole};

/// Lets clients see and call only these tools.
struct ToolAllowList(Arc<[&'static str]>);

fn allows(allowed: &[&str], tool: &Value) -> bool {
    allowed.iter().any(|a| tool == a)
}

#[async_trait]
impl Plugin for ToolAllowList {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        if !cx.is_kind::<ToServer>() {
            return Ok(None);
        }
        let input = cx.input::<ToServer>()?;
        let calls = input
            .messages
            .iter()
            .filter(|m| m["method"] == "tools/call");
        let refused = calls
            .map(|m| &m["params"]["name"])
            .find(|t| !allows(&self.0, t));
        if let Some(tool) = refused {
            // Every request it posts is answered with a JSON-RPC error; the
            // server never sees it.
            let message = format!("tool {tool} is not allowed");
            let invalid_params = |id: &Value| mcp::error(id.clone(), -32602, &message);
            let answer = ToClient::new(input.requests().map(invalid_params).collect());
            return Ok(Some(Response::stream(stream::iter([Ok(answer)]))));
        }
        let allowed = self.0.clone();
        cx.filter(move |out: &mut ToClient| {
            let lists = out.messages.iter_mut();
            let lists = lists.filter_map(|m| m.pointer_mut("/result/tools")?.as_array_mut());
            for tools in lists {
                tools.retain(|t| allows(&allowed, &t["name"]));
            }
            Ok(())
        })?;
        Ok(None)
    }
}

fn gateway(server: impl Transport) -> Result<Gateway, Fault> {
    Gateway::new()
        .provider("tools", StreamableHttp::default(), server)?
        .serve(StreamableHttp::default())?
        .plugin("allow", ToolAllowList(Arc::from(["echo"])))?
        .target("tools")
}

const AT: &str = "http://mcp.local";

/// A mock MCP server's answers to `initialize`, `tools/list` and a call of
/// `echo`, in order.
fn mock_server() -> MockTransport {
    let mock = MockTransport::new();
    let tools =
        json!([{ "name": "echo", "inputSchema": {} }, { "name": "wipe", "inputSchema": {} }]);
    let results = [
        json!({ "protocolVersion": "2025-06-18", "capabilities": { "tools": {} } }),
        json!({ "tools": tools }),
        json!({ "content": [{ "type": "text", "text": "hi" }] }),
    ];
    for (id, result) in (1..).zip(results) {
        let body = json!({ "jsonrpc": "2.0", "id": id, "result": result }).to_string();
        let mut reply = mock::json(StatusCode::OK, &body);
        reply
            .headers_mut()
            .insert(SESSION, HeaderValue::from_static("s1"));
        mock.reply(AT, reply);
    }
    mock
}

/// An MCP client's request, in `session` if given: the status, the session
/// and the first message answered.
async fn send(
    gw: &Gateway,
    session: Option<&str>,
    id: u64,
    method: &str,
    params: Value,
) -> Result<(StatusCode, Option<String>, Value), Fault> {
    let body = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    let mut req = post("/mcp", body.to_string());
    if let Some(session) = session.and_then(|s| HeaderValue::from_str(s).ok()) {
        req.headers_mut().insert(SESSION, session);
    }
    let (status, headers, body) = read_whole(gw.handle(req).await.0).await;
    let session = headers
        .get(SESSION)
        .and_then(|s| s.to_str().ok())
        .map(String::from);
    let body = String::from_utf8_lossy(&body?).into_owned();
    let first = body
        .split_inclusive("\n\n")
        .find_map(respan_gateway_formats::json);
    Ok((status, session, first.unwrap_or_default()))
}

/// A client's session: it starts one, lists the tools, calls the allowed
/// tool and then one that is not; each answer, in order.
async fn session(gw: &Gateway) -> Result<Vec<(StatusCode, Value)>, Fault> {
    let (status, session, init) = send(gw, None, 1, "initialize", json!({})).await?;
    let mut answers = vec![(status, init)];
    let echo = json!({ "name": "echo", "arguments": { "text": "hi" } });
    for (id, method, params) in [
        (2, "tools/list", json!({})),
        (3, "tools/call", echo),
        (4, "tools/call", json!({ "name": "wipe" })),
    ] {
        let (status, _, answer) = send(gw, session.as_deref(), id, method, params).await?;
        answers.push((status, answer));
    }
    Ok(answers)
}

#[tokio::main]
async fn main() -> Result<(), Fault> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let gw = match args.iter().position(|a| a == "--stdio") {
        Some(i) => {
            let (program, rest) = args[i + 1..]
                .split_first()
                .ok_or_else(|| Fault::config("--stdio PROGRAM"))?;
            gateway(StdioTransport::new(program, rest))?
        }
        None => gateway(mock_server().at(AT))?,
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
mod tests {
    use super::*;

    #[tokio::test]
    async fn clients_see_and_call_only_the_allowed_tools() {
        let mock = mock_server();
        let gw = gateway(mock.at(AT)).unwrap_or_else(|e| panic!("{e}"));
        let answers = session(&gw).await.unwrap_or_else(|e| panic!("{e}"));
        let [_, (_, list), (_, called), (status, refused)] = &answers[..] else {
            panic!("{answers:?}")
        };
        assert_eq!(
            list["result"]["tools"],
            json!([{ "name": "echo", "inputSchema": {} }])
        );
        assert_eq!(called["result"]["content"][0]["text"], "hi");
        assert_eq!(
            (*status, &refused["id"], &refused["error"]["code"]),
            (StatusCode::OK, &json!(4), &json!(-32602))
        );
        // The refused call never reached the server; the others kept the session.
        let sent = mock.sent();
        assert_eq!(sent.len(), 3);
        assert_eq!(sent[2].headers()[SESSION], "s1");
    }
}
