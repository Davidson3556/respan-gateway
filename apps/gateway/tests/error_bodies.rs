//! Slow, oversized and abandoned provider error bodies are bounded.
#![allow(clippy::unwrap_used)]

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use support::Gateway;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// A vendor answering `status` with a body trickled forever, 1 KiB each
/// `gap`; `closed` is set once a write fails (the gateway hung up).
async fn trickler(status: &'static str, gap: Duration, closed: Arc<AtomicBool>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let closed = closed.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 65536];
                let mut got = Vec::new();
                while !got.windows(4).any(|w| w == b"\r\n\r\n") {
                    let n = sock.read(&mut buf).await.unwrap_or(0);
                    if n == 0 {
                        return;
                    }
                    got.extend_from_slice(&buf[..n]);
                }
                let head = format!(
                    "HTTP/1.1 {status}\r\ncontent-type: text/plain\r\ntransfer-encoding: chunked\r\n\r\n"
                );
                if sock.write_all(head.as_bytes()).await.is_err() {
                    closed.store(true, Ordering::SeqCst);
                    return;
                }
                let chunk = format!("400\r\n{}\r\n", "e".repeat(1024));
                loop {
                    if sock.write_all(chunk.as_bytes()).await.is_err() {
                        closed.store(true, Ordering::SeqCst);
                        return;
                    }
                    tokio::time::sleep(gap).await;
                }
            });
        }
    });
    format!("http://{addr}")
}

async fn gateway(base: &str) -> Gateway {
    Gateway::start(&format!(
        "providers:
  any: {{ formats: passthrough, base_url: '{base}' }}
formats: [passthrough]
default: any
"
    ))
    .await
}

/// A client that leaves while the gateway waits on a slow error body: the
/// gateway hangs up on the vendor, not reading it on for nobody.
#[tokio::test]
async fn a_client_that_leaves_while_an_error_is_read_frees_the_vendor() {
    let closed = Arc::new(AtomicBool::new(false));
    let gw = gateway(
        &trickler(
            "503 Service Unavailable",
            Duration::from_millis(20),
            closed.clone(),
        )
        .await,
    )
    .await;
    let addr = gw.base.trim_start_matches("http://").to_owned();
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    sock.write_all(b"GET /files/x HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer sk\r\n\r\n")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    drop(sock);
    let start = Instant::now();
    while !closed.load(Ordering::SeqCst) {
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "the gateway kept reading the vendor's error for a client that left"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// An error body that never ends but trickles fast: the limit ends it, and
/// the client gets the gateway's 502 rather than waiting forever.
#[tokio::test]
async fn an_endless_error_is_answered_at_the_limit() {
    let closed = Arc::new(AtomicBool::new(false));
    let base = trickler("500 Internal Server Error", Duration::ZERO, closed.clone()).await;
    let gw = Gateway::start(&format!(
        "body_limit: 1048576
providers:
  any: {{ formats: passthrough, base_url: '{base}' }}
formats: [passthrough]
default: any
"
    ))
    .await;
    let addr = gw.base.trim_start_matches("http://").to_owned();
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    sock.write_all(b"GET /files/x HTTP/1.1\r\nhost: localhost\r\nauthorization: Bearer sk\r\nconnection: close\r\n\r\n").await.unwrap();
    let mut out = Vec::new();
    let r = tokio::time::timeout(Duration::from_secs(20), sock.read_to_end(&mut out)).await;
    assert!(r.is_ok(), "no end within 20 s ({} bytes)", out.len());
    let head = String::from_utf8_lossy(&out[..out.len().min(200)]).into_owned();
    assert!(head.starts_with("HTTP/1.1 502"), "{head}");
    let n = out.iter().filter(|b| **b == b'e').count();
    assert!(n < 1024, "{n} bytes relayed");
}
