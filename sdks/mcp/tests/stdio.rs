//! An MCP server over stdio, reached through a custom `Transport`.

mod common;

use std::sync::Arc;

use common::*;
use respan_gateway_core::http::{Method, StatusCode};
use respan_gateway_core::http_body_util::BodyExt;
use respan_gateway_core::serde_json::json;
use respan_gateway_core::{Gateway, Plugin};
use respan_gateway_mcp::{StdioTransport, StreamableHttp, ToolAllowList};

fn echo() -> StdioTransport {
    StdioTransport::new(env!("CARGO_BIN_EXE_mcp-echo"), [])
}

fn over_stdio(transport: StdioTransport, plugins: Vec<Arc<dyn Plugin>>) -> Gateway {
    let p = Gateway::new().provider("echo", StreamableHttp::default(), transport);
    gateway(p, StreamableHttp::default(), plugins, "echo")
}

/// Initializes a session and returns its id.
async fn initialize(gw: &Gateway) -> String {
    let init = rpc(1, "initialize", json!({ "protocolVersion": "2025-06-18" }));
    let (status, headers, body, _) = send(gw, post(&init)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(messages(&body)[0]["result"]["serverInfo"]["name"], "echo");
    let id = headers.get("mcp-session-id").and_then(|v| v.to_str().ok());
    id.unwrap_or_else(|| panic!("no session id")).to_owned()
}

#[tokio::test]
async fn a_session_over_stdio_lists_calls_notifies_and_ends() {
    let transport = echo();
    let gw = over_stdio(transport.clone(), Vec::new());
    let session = initialize(&gw).await;
    let with = [("mcp-session-id", session.as_str())];

    let note = r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#;
    let (status, _, body, _) = send(&gw, mcp(Method::POST, note, &with)).await;
    assert_eq!((status, body.as_str()), (StatusCode::ACCEPTED, ""));

    let (_, _, body, _) = send(
        &gw,
        mcp(Method::POST, &rpc(2, "tools/list", json!({})), &with),
    )
    .await;
    assert_eq!(
        messages(&body)[0]["result"]["tools"]
            .as_array()
            .map(Vec::len),
        Some(3)
    );

    // The progress notification comes first, on the call's own stream.
    let (_, _, body, outcome) = send(&gw, mcp(Method::POST, &call_tool(3, "echo"), &with)).await;
    let got = messages(&body);
    assert_eq!(got[0]["method"], "notifications/progress");
    assert_eq!(got[1]["result"]["content"][0]["text"], "hi");
    assert!(outcome.is_some_and(|o| o.end.is_ok()));

    let (status, _, _, _) = send(&gw, mcp(Method::DELETE, "", &with)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(transport.sessions(), 0);

    // The session is gone: the client must initialize again.
    let (status, _, _, _) = send(&gw, mcp(Method::POST, &rpc(4, "ping", json!({})), &with)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn the_allow_list_guards_a_stdio_server_like_any_other() {
    let gw = over_stdio(echo(), vec![Arc::new(ToolAllowList::new(["echo"]))]);
    let session = initialize(&gw).await;
    let with = [("mcp-session-id", session.as_str())];

    let (_, _, body, _) = send(
        &gw,
        mcp(Method::POST, &rpc(2, "tools/list", json!({})), &with),
    )
    .await;
    let tools = &messages(&body)[0]["result"]["tools"];
    assert_eq!(tools.as_array().map(Vec::len), Some(1));

    let (_, _, body, _) = send(&gw, mcp(Method::POST, &call_tool(3, "wipe"), &with)).await;
    assert_eq!(messages(&body)[0]["error"]["code"], -32602);
}

#[tokio::test]
async fn a_get_stream_receives_what_the_server_says_unasked() {
    let gw = over_stdio(echo(), Vec::new());
    let session = initialize(&gw).await;
    let with = [("mcp-session-id", session.as_str())];

    OPEN_STREAMS.with(|o| o.set(true));
    // The head comes at once, with the format's opening comment (F1).
    let get = mcp(Method::GET, "", &with);
    let (resp, _) = tokio::time::timeout(PROMPTLY, gw.handle(get))
        .await
        .unwrap_or_else(|_| panic!("no head"));
    assert_eq!(resp.status(), StatusCode::OK);
    let mut body = resp.into_body();
    let mut frame = async || {
        let frame = tokio::time::timeout(PROMPTLY, body.frame()).await;
        let data = frame.ok().flatten().and_then(|f| f.ok()?.into_data().ok());
        String::from_utf8_lossy(data.as_deref().unwrap_or_default()).into_owned()
    };
    assert_eq!(frame().await, ": stream\n\n");

    let (_, _, said, _) = send(&gw, mcp(Method::POST, &call_tool(2, "announce"), &with)).await;
    assert_eq!(messages(&said)[0]["id"], 2);

    let text = frame().await;
    assert_eq!(messages(&text)[0]["method"], "notifications/message");
}
