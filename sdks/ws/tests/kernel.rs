//! What a session shows of the kernel and the HTTP adapter, pinned, and the
//! bugs it exposed (target/review/BUGS.md, W1).
#![allow(clippy::unwrap_used)]

mod support;

use respan_gateway_core::Fault;
use respan_gateway_http::Status;
use respan_gateway_http::http::StatusCode;
use respan_gateway_testkit::{read_whole, serve as serve_http, streamed};
use support::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// A server that refuses every upgrade with `403` and a body.
async fn refusing() -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut s, _)) = listener.accept().await {
            let mut buf = [0u8; 2048];
            let _ = s.read(&mut buf).await;
            let body = r#"{"error":"no room"}"#;
            let head = format!(
                "HTTP/1.1 403 Forbidden\r\ncontent-length: {}\r\n\r\n{body}",
                body.len()
            );
            s.write_all(head.as_bytes()).await.unwrap();
        }
    });
    addr
}

/// A streaming request as the HTTP adapter gets it: a `GET` with `Upgrade`.
fn upgrade() -> respan_gateway_http::http::Request<respan_gateway_http::Body> {
    let (_tx, mut req) = streamed("/v1/realtime");
    req.headers_mut()
        .insert("upgrade", "websocket".parse().unwrap());
    // The client's frames never come: the sender is dropped, ending the body.
    req
}

// A provider that refuses the upgrade answers with its status and a body.
// The gateway sends a streaming request's head at once, so the client should
// get that head with the body it goes with; what it gets is the status with a
// body that then FAILS, which an HTTP server writes as a broken response
// (the connection is aborted).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "BUGS.md: W1, a provider's refusal of a streaming request is a head with a failing body"]
async fn a_refused_upgrade_is_a_complete_answer() {
    let refusing = refusing().await;
    let (gateway, ..) = relay(&format!("ws://{refusing}"));
    let (resp, _) = serve_http(&gateway, upgrade()).await;
    let (status, _, body) = read_whole(resp).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(body.is_ok(), "the body failed: {body:?}");
}

// What it is today, pinned: the head, then a body that ends `Err` with the
// provider's fault, which `serve_ws` turns into a whole plain-text answer.
#[tokio::test(flavor = "multi_thread")]
async fn today_a_refused_upgrade_is_a_head_and_a_failing_body() {
    let refusing = refusing().await;
    let (gateway, ends, _) = relay(&format!("ws://{refusing}"));
    let (resp, _) = serve_http(&gateway, upgrade()).await;
    let (status, _, body) = read_whole(resp).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let fault: Fault = body.unwrap_err();
    assert_eq!(fault.get::<Status>(), Some(&Status(403)));
    idle(&gateway).await;
    assert_eq!(ends.all().len(), 1);
}

// The same refusal through the adapter of this crate is a whole answer
// under the provider's status.
#[tokio::test(flavor = "multi_thread")]
async fn serve_ws_writes_a_refused_upgrade_whole() {
    let refusing = refusing().await;
    let (gateway, ends, calls) = relay(&format!("ws://{refusing}"));
    let server = serve(gateway).await;
    let refused = connect(server.addr, "/v1/realtime", &[]).await;
    let Err(tokio_tungstenite::tungstenite::Error::Http(resp)) = refused else {
        panic!("expected an HTTP answer to the handshake")
    };
    assert_eq!(resp.status(), 403);
    idle(&server.gateway).await;
    assert_eq!(ends.all().len(), 1);
    assert_eq!(calls.all().len(), 1);
}
