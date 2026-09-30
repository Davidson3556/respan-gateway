//! Loop 8 audit: the provider edge's `decide` under a slow, huge or
//! abandoned error body.
#![allow(clippy::unwrap_used)]

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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
  any: {{ format: passthrough, base_url: '{base}' }}
serve: [passthrough]
target: any
"
    ))
    .await
}

/// A client that leaves while the gateway waits on a slow error body: the
/// gateway hangs up on the vendor, not reading it on for nobody.
#[tokio::test]
async fn l8_a_client_that_leaves_while_an_error_is_read_frees_the_vendor() {
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
    sock.write_all(b"GET /files/x HTTP/1.1\r\nhost: x\r\nauthorization: Bearer sk\r\n\r\n")
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

/// An error body that never ends but trickles fast: the cap ends it, and the
/// client gets a refusal (at most ~1 MiB) rather than waiting forever.
#[tokio::test]
async fn l8_an_endless_error_is_answered_at_the_cap() {
    let closed = Arc::new(AtomicBool::new(false));
    let gw =
        gateway(&trickler("500 Internal Server Error", Duration::ZERO, closed.clone()).await).await;
    let addr = gw.base.trim_start_matches("http://").to_owned();
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    sock.write_all(b"GET /files/x HTTP/1.1\r\nhost: x\r\nauthorization: Bearer sk\r\nconnection: close\r\n\r\n").await.unwrap();
    let mut out = Vec::new();
    let r = tokio::time::timeout(Duration::from_secs(20), sock.read_to_end(&mut out)).await;
    assert!(r.is_ok(), "no end within 20 s ({} bytes)", out.len());
    let head = String::from_utf8_lossy(&out[..out.len().min(200)]).into_owned();
    assert!(head.starts_with("HTTP/1.1 500"), "{head}");
    let n = out.iter().filter(|b| **b == b'e').count();
    assert!(n <= (1 << 20) + 70_000, "{n} bytes relayed");
    let _ = AtomicUsize::new(0);
}
