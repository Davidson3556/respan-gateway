//! Interoperability with the official MCP server over stdio and real HTTP.
//! Opt-in: npm downloads the pinned server; no provider key is needed.

#[allow(dead_code)]
#[path = "../../tests/support/http.rs"]
mod http_support;

use respan_gateway_http::{
    Body, HttpClient,
    http::{self, Method, StatusCode},
    http_body_util::BodyExt,
};
use respan_gateway_mcp::{SESSION, StdioTransport, StreamableHttp, ToServer};
use respan_gateway_sdk::{Fault, Gateway};
use serde_json::{Value, json};
use std::process::Stdio;
use std::time::Duration;
use tokio::process::Command;

const SERVER: &str = "@modelcontextprotocol/server-everything@2026.8.31";

async fn send(
    gw: &Gateway,
    method: Method,
    body: Value,
    session: Option<&str>,
) -> (StatusCode, Option<String>, Value) {
    let mut request = http::Request::builder()
        .method(method)
        .uri("/mcp")
        .header("accept", "application/json")
        .header("mcp-protocol-version", "2025-06-18")
        .header("content-type", "application/json");
    if let Some(session) = session {
        request = request.header(SESSION, session);
    }
    let request = request.body(Body::full(body.to_string())).expect("request");
    tokio::time::timeout(Duration::from_secs(45), async {
        let (reply, finished) = http_support::serve(gw, request).await;
        let status = reply.status();
        let session = reply
            .headers()
            .get(SESSION)
            .map(|s| s.to_str().expect("session text").to_owned());
        let bytes = reply
            .into_body()
            .collect()
            .await
            .expect("response body")
            .to_bytes();
        finished.await.expect("successful exchange");
        let body = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).expect("RPC reply")
        };
        (status, session, body)
    })
    .await
    .expect("MCP exchange deadline")
}

#[tokio::test]
#[ignore = "downloads and runs the official MCP reference server with npx"]
async fn official_stdio_server_initializes_lists_calls_and_deletes() -> Result<(), Fault> {
    let transport = StdioTransport::new("npx", ["--yes", SERVER]).timeout(Duration::from_secs(30));
    let gw = Gateway::new()
        .client::<ToServer, _, _>(StreamableHttp::default())?
        .provider::<ToServer, _, _>("mcp", StreamableHttp::default(), transport.connect())?
        .target("mcp")?;
    exercise(gw).await;
    Ok(())
}

#[tokio::test]
#[ignore = "downloads and runs the official MCP HTTP reference server with npx/node"]
async fn official_http_server_initializes_lists_calls_and_deletes() -> Result<(), Fault> {
    // Resolve the pinned entry point, then launch Node directly so the
    // test owns the server process and can kill/reap it on completion.
    let resolved = tokio::time::timeout(
        Duration::from_secs(45),
        Command::new("npx")
            .args([
                "--yes",
                "--package",
                SERVER,
                "-c",
                "command -v mcp-server-everything",
            ])
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("npm resolution deadline")
    .expect("npm available");
    assert!(
        resolved.status.success(),
        "npm failed to resolve the MCP server"
    );
    let entry = std::str::from_utf8(&resolved.stdout)
        .expect("server path")
        .trim();
    let entry = std::fs::canonicalize(entry).expect("pinned server entry point");
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("available port");
    let addr = listener.local_addr().expect("port");
    drop(listener);
    let mut child = Command::new("node")
        .arg(entry)
        .arg("streamableHttp")
        .env("PORT", addr.port().to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("Node available");
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            assert!(
                child.try_wait().expect("server status").is_none(),
                "MCP HTTP server exited"
            );
            if tokio::net::TcpStream::connect(addr).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("MCP HTTP server startup deadline");
    let gw = Gateway::new()
        .client::<ToServer, _, _>(StreamableHttp::default())?
        .provider::<ToServer, _, _>(
            "mcp",
            StreamableHttp::default(),
            HttpClient::new(format!("http://{addr}"))?.connect(),
        )?
        .target("mcp")?;
    exercise(gw).await;
    child.start_kill().expect("stop reference server");
    child.wait().await.expect("reap reference server");
    Ok(())
}

async fn exercise(gw: Gateway) {
    let (status,session,init) = send(&gw,Method::POST,json!({"jsonrpc":"2.0","id":"init","method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"gateway-interop","version":"1"}}}),None).await;
    assert_eq!(status, StatusCode::OK, "{init}");
    assert_eq!(init["id"], "init");
    assert_eq!(init["result"]["protocolVersion"], "2025-06-18");
    assert!(init["result"]["serverInfo"]["name"].is_string(), "{init}");
    let session = session.expect("assigned session");
    let (status, _, _) = send(
        &gw,
        Method::POST,
        json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
        Some(&session),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let (_, _, list) = send(
        &gw,
        Method::POST,
        json!({"jsonrpc":"2.0","id":0,"method":"tools/list"}),
        Some(&session),
    )
    .await;
    assert_eq!(list["id"], 0, "{list}");
    assert!(
        list["result"]["tools"]
            .as_array()
            .expect("tools")
            .iter()
            .any(|t| t["name"] == "echo"),
        "{list}"
    );
    let (_,_,answer) = send(&gw,Method::POST,json!({"jsonrpc":"2.0","id":"call","method":"tools/call","params":{"name":"echo","arguments":{"message":"雪😀\nquote: \""}}}),Some(&session)).await;
    assert_eq!(answer["id"], "call");
    assert!(
        answer["result"]["content"][0]["text"]
            .as_str()
            .expect("tool text")
            .contains("雪😀\nquote: \""),
        "{answer}"
    );
    let (status, _, _) = send(&gw, Method::DELETE, Value::Null, Some(&session)).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    gw.idle().await;
}
