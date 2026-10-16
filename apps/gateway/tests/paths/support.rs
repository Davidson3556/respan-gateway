//! Local wire fixtures shared by the gateway path suites.
#![allow(clippy::unwrap_used)]

use parking_lot::Mutex;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

pub const CHAT_REPLY: &str = r#"{"id":"x","object":"chat.completion","created":1,"model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}"#;
const CHAT_REQUEST: &str =
    r#"{"model":"gpt-4.1-mini","messages":[{"role":"user","content":"hi"}]}"#;

pub async fn record_lines(body: &'static str) -> (String, Arc<Mutex<Vec<String>>>) {
    record(body, true).await
}

pub async fn record_heads(body: &'static str) -> (String, Arc<Mutex<Vec<String>>>) {
    record(body, false).await
}

pub async fn send_post(addr: &str, target: &str) -> String {
    send_request(addr, "POST", target, "").await
}

pub async fn send_request(addr: &str, method: &str, target: &str, extra: &str) -> String {
    send(addr, method, target, extra, CHAT_REQUEST).await
}

pub async fn send_proxy_request(addr: &str, method: &str, target: &str) -> String {
    let body = r#"{"model":"m","max_tokens":5,"messages":[{"role":"user","content":"hi"}]}"#;
    send(
        addr,
        method,
        target,
        "x-api-key: sk-client\r\nanthropic-version: 2023-06-01\r\n",
        body,
    )
    .await
}

async fn record(body: &'static str, lines_only: bool) -> (String, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let s = seen.clone();
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let s = s.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 65536];
                let mut got = Vec::new();
                loop {
                    let n = sock.read(&mut buf).await.unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    got.extend_from_slice(&buf[..n]);
                    if got.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                let head = String::from_utf8_lossy(&got).into_owned();
                s.lock().push(if lines_only {
                    head.lines().next().unwrap_or_default().to_owned()
                } else {
                    head
                });
                let _ = sock.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}", body.len()).as_bytes()).await;
            });
        }
    });
    (format!("http://{addr}"), seen)
}

async fn send(addr: &str, method: &str, target: &str, extra: &str, body: &str) -> String {
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "{method} {target} HTTP/1.1\r\nhost: localhost\r\n{extra}authorization: Bearer sk-client\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    sock.write_all(req.as_bytes()).await.unwrap();
    let mut out = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(5), sock.read_to_end(&mut out)).await;
    String::from_utf8_lossy(&out)
        .lines()
        .next()
        .unwrap_or_default()
        .to_owned()
}
