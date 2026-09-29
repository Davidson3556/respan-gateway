//! Loop 5 audit: what passthrough forwards.
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

/// Passthrough forwards a vendor's path as the vendor needs it: a `%2F`
/// inside one segment (a Bedrock inference-profile ARN, a GCS object
/// name) stays one segment, a Vertex/Gemini custom method keeps its `:`,
/// and bytes that are not UTF-8 are not rewritten.
#[tokio::test]
async fn l5_passthrough_keeps_a_vendors_path_segments() {
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
    let mut wrong = Vec::new();
    for sent in [
        "/model/arn%3Aaws%3Abedrock%3Aus-east-1%3A1%3Aapplication-inference-profile%2Fabc/invoke",
        "/storage/v1/b/bkt/o/dir%2Ffile.txt",
        "/v1beta/models/gemini-2.0-flash:generateContent",
        "/v1/projects/p/locations/l/publishers/anthropic/models/claude@2024:rawPredict",
        "/v1/files/%FF",
        "/v1/x;v=1/y",
    ] {
        seen.lock().unwrap().clear();
        send_raw(&addr, sent).await;
        let got = seen.lock().unwrap().clone();
        let want = format!("POST /pt{sent} HTTP/1.1");
        if got != [want.clone()] {
            wrong.push(format!("sent {sent}\n   vendor got {got:?}"));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
