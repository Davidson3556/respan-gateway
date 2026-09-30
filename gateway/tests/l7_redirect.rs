//! Loop 7 audit: a 3xx is relayed through passthrough as an answer, not a
//! refusal, so it streams whole as a 200 does, never cut at the cap that
//! bounds a refusal's body.
#![allow(clippy::unwrap_used)]

mod support;

use std::time::{Duration, Instant};

use support::Gateway;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const BIG: usize = 3 << 20;

/// A vendor answering every request with `status` and a body of `BIG` bytes
/// of `x`, sent in 64 KiB chunks `gap` apart.
async fn vendor(status: &'static str, gap: Duration) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
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
                    "HTTP/1.1 {status}\r\nlocation: /elsewhere\r\ncontent-type: text/plain\r\ncontent-length: {BIG}\r\n\r\n"
                );
                if sock.write_all(head.as_bytes()).await.is_err() {
                    return;
                }
                let chunk = vec![b'x'; 65536];
                for _ in 0..BIG / chunk.len() {
                    if sock.write_all(&chunk).await.is_err() {
                        return;
                    }
                    tokio::time::sleep(gap).await;
                }
            });
        }
    });
    format!("http://{addr}")
}

/// Reads one raw response to `GET /files/x`: its status line, the time to
/// its first byte, and how many body bytes came before the connection ended.
async fn fetch(gw: &Gateway) -> (String, Duration, usize) {
    let addr = gw.base.trim_start_matches("http://").to_owned();
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    let start = Instant::now();
    sock.write_all(b"GET /files/x HTTP/1.1\r\nhost: x\r\nauthorization: Bearer sk\r\nconnection: close\r\n\r\n").await.unwrap();
    let mut first = [0u8; 1];
    sock.read_exact(&mut first).await.unwrap();
    let ttfb = start.elapsed();
    let mut out = first.to_vec();
    let _ = tokio::time::timeout(Duration::from_secs(20), sock.read_to_end(&mut out)).await;
    let at = out.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
    let text = String::from_utf8_lossy(&out[..at]).into_owned();
    let body = &out[at..];
    // Chunked or not, count the payload bytes.
    let payload = body.iter().filter(|b| **b == b'x').count();
    (
        text.lines().next().unwrap().to_owned() + " | " + &text.replace("\r\n", " / "),
        ttfb,
        payload,
    )
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

/// A 200 of 3 MiB relays whole: the baseline.
#[tokio::test]
async fn l7_a_big_200_relays_whole() {
    let gw = gateway(&vendor("200 OK", Duration::ZERO).await).await;
    let (head, _, n) = fetch(&gw).await;
    assert_eq!(n, BIG, "{head}");
}

/// A 3xx is an answer passthrough relays as it came, so its body comes
/// whole too: it was read under the 1 MiB error-body cap and ended clean,
/// so the client got a cut body it could not tell from a whole one.
#[tokio::test]
async fn l7_a_big_redirect_relays_whole() {
    for status in [
        "302 Found",
        "300 Multiple Choices",
        "307 Temporary Redirect",
    ] {
        let gw = gateway(&vendor(status, Duration::ZERO).await).await;
        let (head, _, n) = fetch(&gw).await;
        assert_eq!(n, BIG, "{status}: {head}");
    }
}

/// A 3xx relayed as it came streams as a 200 does; it was held until its
/// whole body (or the cap) was read.
#[tokio::test]
async fn l7_a_slow_redirect_starts_as_a_slow_200_does() {
    let gap = Duration::from_millis(40);
    let gw = gateway(&vendor("200 OK", gap).await).await;
    let (_, ok_first, _) = fetch(&gw).await;
    let gw = gateway(&vendor("302 Found", gap).await).await;
    let (head, redirect_first, _) = fetch(&gw).await;
    assert!(
        redirect_first < ok_first + Duration::from_millis(500),
        "a 200's first byte came in {ok_first:?}, a 302's in {redirect_first:?} ({head})"
    );
}
