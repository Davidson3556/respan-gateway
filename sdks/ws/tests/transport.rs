//! `WsTransport` on its own, without a gateway: what its answer's body does
//! for each thing that can happen on the connection.
#![allow(clippy::unwrap_used)]

mod support;

use futures::StreamExt;
use futures::channel::mpsc;
use respan_gateway_core::Fault;
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::{Exchange, Transport};
use respan_gateway_ws::WsTransport;
use std::sync::atomic::Ordering;
use support::*;

/// A call carrying `frames` sent through the returned sender, which ends
/// the body when dropped (or `Err` if given one).
fn call(
    path: &str,
) -> (
    mpsc::UnboundedSender<Result<Bytes, Fault>>,
    Exchange<respan_gateway_http::RequestHead>,
) {
    let (tx, rx) = mpsc::unbounded();
    let mut head = respan_gateway_http::fresh_request_head();
    head.uri = path.parse().unwrap();
    head.headers.insert("upgrade", "websocket".parse().unwrap());
    let mut call = Exchange::new(head, rx.boxed());
    call.streams = true;
    (tx, call)
}

fn text(t: &'static str) -> Result<Bytes, Fault> {
    Ok(Bytes::from_static(t.as_bytes()))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_101_answer_carries_the_providers_frames_in_order() {
    let probe = Probe::echoing().await;
    let (tx, call) = call("/x");
    let mut answer = WsTransport::new(probe.base())
        .unwrap()
        .send(call)
        .await
        .unwrap();
    assert_eq!(answer.head.status, 101);
    assert!(answer.streams);
    // The handshake's own headers are not the answer's.
    assert!(answer.head.headers.get("sec-websocket-accept").is_none());
    for t in ["a", "b", "c"] {
        tx.unbounded_send(text(t)).unwrap();
    }
    for t in ["echo:a", "echo:b", "echo:c"] {
        assert_eq!(answer.body.next().await.unwrap().unwrap(), t.as_bytes());
    }
    // A clean end sends Close, and the provider's part of it ends the answer clean.
    drop(tx);
    assert!(answer.body.next().await.is_none());
    assert_eq!(probe.seen().last(), Some(&Seen::Close(Some(1000))));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_call_that_ends_in_an_err_drops_the_connection_and_the_answer_ends_abandoned() {
    let probe = Probe::start().await;
    let (tx, call) = call("/x");
    let mut answer = WsTransport::new(probe.base())
        .unwrap()
        .send(call)
        .await
        .unwrap();
    tx.unbounded_send(text("a")).unwrap();
    tx.unbounded_send(Err(Fault::bad_request("client left")))
        .unwrap();
    let end = answer.body.next().await.unwrap().unwrap_err();
    assert_eq!(end.code(), "abandoned");
    eventually("the connection dropped", || {
        probe.live.load(Ordering::SeqCst) == 0
    })
    .await;
    assert_eq!(
        probe.seen(),
        [Seen::Text("a".into()), Seen::Ended],
        "no Close"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn dropping_the_answer_drops_the_connection() {
    let probe = Probe::start().await;
    let (_tx, call) = call("/x");
    let answer = WsTransport::new(probe.base())
        .unwrap()
        .send(call)
        .await
        .unwrap();
    probe.connected(0).await;
    assert_eq!(probe.live.load(Ordering::SeqCst), 1);
    drop(answer);
    eventually("the connection dropped", || {
        probe.live.load(Ordering::SeqCst) == 0
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_frame_that_is_not_text_fails_the_call() {
    let probe = Probe::start().await;
    let (tx, call) = call("/x");
    let mut answer = WsTransport::new(probe.base())
        .unwrap()
        .send(call)
        .await
        .unwrap();
    tx.unbounded_send(Ok(Bytes::from_static(&[0xff, 0xfe])))
        .unwrap();
    let end = answer.body.next().await.unwrap().unwrap_err();
    assert_eq!(end.code(), "config");
    assert!(probe.texts().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_message_over_the_limit_breaks_the_answer() {
    let probe = Probe::start().await;
    let (_tx, call) = call("/x");
    let mut answer = WsTransport::new(probe.base())
        .unwrap()
        .max_message(1024)
        .send(call)
        .await
        .unwrap();
    probe.connected(0).await;
    probe.tell(0, Cmd::Send("x".repeat(4096)));
    let end = answer.body.next().await.unwrap().unwrap_err();
    assert_eq!(end.code(), "upstream");
    assert!(end.message().contains("too large"), "{end}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_provider_that_refuses_the_upgrade_is_an_answer_not_an_error() {
    // A server that answers 403 to the handshake, with a body.
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 2048];
        let _ = s.read(&mut buf).await;
        let body = "no room";
        let head = format!(
            "HTTP/1.1 403 Forbidden\r\ncontent-length: {}\r\nx-why: full\r\n\r\n{body}",
            body.len()
        );
        s.write_all(head.as_bytes()).await.unwrap();
    });
    let (_tx, call) = call("/x");
    let mut answer = WsTransport::new(format!("ws://{addr}"))
        .unwrap()
        .send(call)
        .await
        .unwrap();
    assert_eq!(answer.head.status, 403);
    assert_eq!(answer.head.headers["x-why"], "full");
    assert!(!answer.streams);
    assert_eq!(answer.body.next().await.unwrap().unwrap(), "no room");
}

// The connector for `wss` builds and fails as a connection failure does: a
// TLS handshake with a server that is not TLS is `upstream`, never a panic.
#[tokio::test(flavor = "multi_thread")]
async fn wss_fails_cleanly_against_a_server_that_is_not_tls() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((s, _)) = listener.accept().await {
            drop(s);
        }
    });
    let (_tx, call) = call("/x");
    let refused = WsTransport::new(format!("wss://{addr}"))
        .unwrap()
        .send(call)
        .await
        .unwrap_err();
    assert_eq!(refused.code(), "upstream");
    assert!(!refused.message().contains(&addr.to_string()));
}
