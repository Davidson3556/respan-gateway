//! Loop 7 audit: what passthrough forwards vs what matching read.
//! `serve: [openai-chat, passthrough]`. A path readers read differently is
//! no format's (a 404): passthrough takes only a path every reader reads one
//! way, so a spelling a vendor resolves to its chat endpoint never reaches it.
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

use respan_gateway_http::percent_decode as decode;

fn dots(segs: impl Iterator<Item = String>) -> String {
    let mut kept: Vec<String> = Vec::new();
    for s in segs {
        match s.as_str() {
            "" | "." => {}
            ".." => drop(kept.pop()),
            _ => kept.push(s),
        }
    }
    kept.iter().fold(String::new(), |o, s| o + "/" + s)
}

/// A vendor behind a front end that decodes then resolves dots (nginx's
/// normalized `$uri`), or a servlet container that cuts `;params` from each
/// raw segment first, reads these forwarded paths as its chat endpoint;
/// matching read them as not chat, so passthrough carried a chat call past
/// the chat format and its plugins.
#[tokio::test]
async fn l7_no_front_end_reads_a_passthrough_call_as_chat() {
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
        "/v1/chat/completions/%3B/%2F..",
        "/v1/chat/completions/x%5C../%2F..",
        "/v1/chat/completions;x%2F..",
        "/v1/chat/completions;x%2Fy",
    ] {
        seen.lock().unwrap().clear();
        let status = send_raw(&addr, sent).await;
        let got = seen.lock().unwrap().clone();
        if !status.contains(" 404 ") {
            wrong.push(format!("sent {sent}: {status}, not a 404"));
        }
        for line in &got {
            let target = line.split(' ').nth(1).unwrap_or_default();
            let Some(path) = target.strip_prefix("/pt") else {
                continue;
            };
            let nginx = dots(decode(path).split('/').map(str::to_owned));
            let servlet = dots(
                path.split('/')
                    .map(|s| decode(s.split(';').next().unwrap_or_default())),
            );
            if nginx.ends_with("/chat/completions") || servlet.ends_with("/chat/completions") {
                wrong.push(format!("sent {sent} ({status}): passthrough sent {target}, read {nginx:?} / {servlet:?}"));
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
