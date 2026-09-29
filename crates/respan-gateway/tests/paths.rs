//! Loop 4 audit: spellings of the chat path, against the real binary with
//! `serve: [openai-chat, passthrough]`. A spelling the vendor's URL handling
//! resolves to its chat endpoint must be taken by the chat format.
#![allow(clippy::unwrap_used)]

mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use support::Gateway;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

async fn recorder() -> (String, Arc<Mutex<Vec<String>>>) {
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
                s.lock()
                    .unwrap()
                    .push(head.lines().next().unwrap_or_default().to_owned());
                let body = r#"{"id":"x","object":"chat.completion","created":1,"model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}"#;
                let _ = sock.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}", body.len()).as_bytes()).await;
            });
        }
    });
    (format!("http://{addr}"), seen)
}

async fn send_raw(addr: &str, target: &str) -> String {
    let body = r#"{"model":"gpt-4.1-mini","messages":[{"role":"user","content":"hi"}]}"#;
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "POST {target} HTTP/1.1\r\nhost: x\r\nauthorization: Bearer sk-client\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
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

#[tokio::test]
async fn l4_no_spelling_of_the_chat_path_reaches_the_vendor_chat_endpoint_through_passthrough() {
    let (base, seen) = recorder().await;
    let gw = Gateway::start(&format!(
        "providers:
  p: {{ format: openai-chat, base_url: '{base}/chatv1' }}
  any: {{ format: passthrough, base_url: '{base}/pt' }}
serve: [openai-chat, passthrough]
target: p
"
    ))
    .await;
    let addr = gw.base.trim_start_matches("http://").to_owned();
    let mut bypass = Vec::new();
    let mut table = Vec::new();
    for target in [
        "/v1/chat/completions",
        "/v1/chat/./completions",
        "/v1/chat\\completions",
        "/v1\\chat\\completions",
        "/v1/chat/.\\completions",
        "/v1/chat/x\\..\\completions",
        "/v1/chat/completions\\",
        "/v1/chat/completions%5C",
        "/v1/chat/%2e%2e/chat/completions",
        "/v1/chat/completions%09",
        "/v1/chat/completions/%2e",
        "http://x/v1/chat/./completions",
        "/v1/chat/completions#x",
        "/v1/chat/completions?#",
        "/v1/chat/completions%00",
        "/v1/chat/completions%3F",
        "/v1/chat/completions%23",
    ] {
        seen.lock().unwrap().clear();
        let status = send_raw(&addr, target).await;
        let got = seen.lock().unwrap().clone();
        table.push(format!("{target:40} -> {status:30} vendor {got:?}"));
        if got
            .iter()
            .any(|l| l.starts_with("POST /pt/v1/chat/completions "))
        {
            bypass.push(target);
        }
    }
    eprintln!("{}", table.join("\n"));
    assert!(
        bypass.is_empty(),
        "these spellings skipped the chat format and reached the vendor's chat endpoint: {bypass:?}"
    );
}

/// Passthrough forwards the path it matched, cleaned, and the query as it
/// came: whatever the spelling, the vendor gets one path.
#[tokio::test]
async fn passthrough_forwards_the_canonical_path() {
    let (base, seen) = recorder().await;
    let gw = Gateway::start(&format!(
        "providers:
  p: {{ format: openai-chat, base_url: '{base}/chatv1' }}
  any: {{ format: passthrough, base_url: '{base}/pt' }}
serve: [openai-chat, passthrough]
target: p
"
    ))
    .await;
    let addr = gw.base.trim_start_matches("http://").to_owned();
    for (sent, forwarded) in [
        ("/v1\\models", "/pt/v1/models"),
        ("/v1/./models", "/pt/v1/models"),
        ("/v1/x/../models?a=1", "/pt/v1/models?a=1"),
        ("/v1/%6Dodels", "/pt/v1/models"),
        ("/v1/files/file-1?limit=2", "/pt/v1/files/file-1?limit=2"),
        ("/v1/a%20b", "/pt/v1/a%20b"),
    ] {
        seen.lock().unwrap().clear();
        send_raw(&addr, sent).await;
        let got = seen.lock().unwrap().clone();
        assert_eq!(got, [format!("POST {forwarded} HTTP/1.1")], "sent {sent}");
    }
}
