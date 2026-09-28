//! The gateway as a library (use case 14): `handle` and `Ended`, no
//! server.

mod common;

use common::*;
use respan_gateway_core::Gateway;
use respan_gateway_core::bytes::Bytes;
use respan_gateway_core::codes;
use respan_gateway_core::http::{self, Method, StatusCode};
use respan_gateway_core::http_body_util::{BodyExt, Full};
use respan_gateway_core::serde_json::json;
use respan_gateway_mcp::{StdioTransport, StreamableHttp};

fn embedded() -> respan_gateway_core::Gateway {
    let stdio = StdioTransport::new(env!("CARGO_BIN_EXE_mcp-echo"), []);
    let p = Gateway::new().provider("echo", StreamableHttp::default(), stdio);
    gateway(p, StreamableHttp::default(), Vec::new(), "echo")
}

/// A request as another Rust service builds it, with a body of its own
/// type.
fn request(body: String, session: Option<&str>) -> http::Request<Full<Bytes>> {
    let mut req = http::Request::post("/mcp")
        .header("accept", "application/json, text/event-stream")
        .header("content-type", "application/json");
    if let Some(s) = session {
        req = req.header("mcp-session-id", s);
    }
    req.body(Full::new(Bytes::from(body)))
        .unwrap_or_else(|e| panic!("{e}"))
}

#[tokio::test]
async fn an_embedder_gets_the_response_and_then_its_outcome() {
    let gw = embedded();
    let init = rpc(1, "initialize", json!({"protocolVersion":"2025-06-18"}));

    let (resp, ended) = gw.handle(request(init, None)).await;
    let session = resp.headers().get("mcp-session-id").cloned();
    let body = resp.into_body().collect().await.map(|b| b.to_bytes());
    let outcome = ended.await;

    assert!(session.is_some());
    assert!(body.is_ok_and(|b| !b.is_empty()));
    assert_eq!(outcome.head.map(|h| h.status), Some(StatusCode::OK));
    assert!(outcome.end.is_ok());
}

#[tokio::test]
async fn an_embedder_dropping_a_stream_ends_it_abandoned() {
    let gw = embedded();
    let init = rpc(1, "initialize", json!({"protocolVersion":"2025-06-18"}));
    let (resp, _) = gw.handle(request(init, None)).await;
    let session = resp
        .headers()
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let _ = resp.into_body().collect().await;
    let session = session.unwrap_or_default();

    // A GET stream the server keeps open; the embedder reads one event.
    OPEN_STREAMS.with(|o| o.set(true));
    let listen = {
        let gw = gw.clone();
        let get = mcp(Method::GET, "", &[("mcp-session-id", &session)]);
        tokio::spawn(async move { gw.handle(get).await })
    };
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let _ = send(
        &gw,
        mcp(
            Method::POST,
            &call_tool(2, "announce"),
            &[("mcp-session-id", &session)],
        ),
    )
    .await;
    let (resp, ended) = listen.await.unwrap_or_else(|e| panic!("{e}"));
    let mut body = resp.into_body();
    let first = body.frame().await;
    assert!(first.is_some_and(|f| f.is_ok()));
    drop(body);

    let end = tokio::time::timeout(PROMPTLY, ended)
        .await
        .ok()
        .map(|o| o.end);
    assert!(
        matches!(&end, Some(Err(f)) if f.code() == codes::ABANDONED),
        "{end:?}"
    );
}

#[tokio::test]
async fn a_request_no_format_accepts_has_the_outcome_of_its_404() {
    let gw = embedded();
    let (resp, ended) = gw
        .handle(respan_gateway_testkit::post("/elsewhere", "{}"))
        .await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let outcome = ended.await;
    assert_eq!(outcome.head.map(|h| h.status), Some(StatusCode::NOT_FOUND));
    assert!(outcome.end.is_err_and(|f| f.code() == codes::NOT_FOUND));
}
