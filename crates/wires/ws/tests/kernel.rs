//! What a session shows of the kernel and the HTTP adapter, pinned: a
//! provider's refusal of the upgrade is a complete answer.
#![allow(clippy::unwrap_used)]

mod support;

use respan_gateway_core::Fault;
use respan_gateway_http::http::StatusCode;
use respan_gateway_http_faults::Status;
#[allow(dead_code, unused_imports)]
#[path = "../../../../tests/support/http.rs"]
mod http_helpers;
use http_helpers::{read_whole, serve_live as serve_http, streamed};
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
// The call is refused at its head, so the client gets that head with the
// body it goes with, not a head and then a body that fails, which an HTTP
// server writes as a broken response.
#[tokio::test(flavor = "multi_thread")]
async fn a_refused_upgrade_is_a_complete_answer() {
    let refusing = refusing().await;
    let (gateway, ..) = relay(&format!("ws://{refusing}"));
    let gateway = gateway.plugin(Relay);
    let (resp, _) = serve_http(&gateway, upgrade()).await;
    let (status, _, body) = read_whole(resp).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(body.is_ok(), "the body failed: {body:?}");
}

// The provider's refusal goes as it came, head and body, not as a head and
// then a body that ends `Err` with its fault; the request ends once, with
// that fault.
#[tokio::test(flavor = "multi_thread")]
async fn a_refused_upgrade_is_the_providers_answer_as_it_came() {
    let refusing = refusing().await;
    let (gateway, ends, _) = relay(&format!("ws://{refusing}"));
    let gateway = gateway.plugin(Relay);
    let (resp, _) = serve_http(&gateway, upgrade()).await;
    let (status, _, body) = read_whole(resp).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(&body.unwrap()[..], br#"{"error":"no room"}"#);
    idle(&gateway).await;
    let ends = ends.all();
    assert_eq!(ends.len(), 1);
    let fault: Fault = ends[0].clone().unwrap_err();
    assert_eq!(fault.get::<Status>(), Some(&Status(StatusCode::FORBIDDEN)));
}

// The same refusal through the adapter of this crate is a whole answer
// under the provider's status.
#[tokio::test(flavor = "multi_thread")]
async fn the_router_writes_a_refused_upgrade_whole() {
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

// A refusal is read whole only up to `max_message`: one larger is the
// gateway's fault, a 500 with its error code.
#[tokio::test(flavor = "multi_thread")]
async fn a_refusal_over_max_message_is_a_500() {
    let refusing = refusing().await;
    let (gateway, ..) = relay(&format!("ws://{refusing}"));
    let limits = respan_gateway_ws::Limits {
        max_message: 8,
        ..Default::default()
    };
    let server = serve_with(gateway.plugin(Relay), limits, std::future::pending()).await;
    let refused = connect(server.addr, "/v1/realtime", &[]).await;
    let Err(tokio_tungstenite::tungstenite::Error::Http(resp)) = refused else {
        panic!("expected an HTTP answer to the handshake")
    };
    assert_eq!(resp.status(), 500);
    assert_eq!(resp.body().as_deref(), Some(&b"internal"[..]));
}
