//! Loop 8 audit (copied helpers from loop 7): what passthrough forwards vs what matching read.
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
                s.lock().unwrap().push(head.clone());
                let body = r#"{"id":"x","object":"chat.completion","created":1,"model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}"#;
                let _ = sock.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}", body.len()).as_bytes()).await;
            });
        }
    });
    (format!("http://{addr}"), seen)
}

async fn send_req(addr: &str, method: &str, target: &str, extra: &str) -> String {
    let body = r#"{"model":"gpt-4.1-mini","messages":[{"role":"user","content":"hi"}]}"#;
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "{method} {target} HTTP/1.1\r\nhost: x\r\n{extra}authorization: Bearer sk-client\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
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

/// Readers of the forwarded request that match case-insensitively (ASP.NET
/// Core and Express route so by default; ASP.NET compares methods so too)
/// read these as the chat endpoint, which the chat format never matched:
/// passthrough carries a chat call past it and its plugins.
#[tokio::test]
async fn l8_no_case_insensitive_vendor_reads_a_passthrough_call_as_chat() {
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
    for (method, target, extra) in [
        ("POST", "/v1/Chat/Completions", ""),
        ("POST", "/V1/CHAT/COMPLETIONS", ""),
        ("post", "/v1/chat/completions", ""),
        ("Post", "/v1/chat/completions", ""),
        ("POST", "/v1/chat/completions%00", ""),
        ("POST", "/v1/chat/completions.", ""),
        ("POST", "/v1/chat/completions%20", ""),
        (
            "PUT",
            "/v1/chat/completions",
            "x-http-method-override: POST\r\n",
        ),
    ] {
        seen.lock().unwrap().clear();
        let status = send_req(&addr, method, target, extra).await;
        let got = seen.lock().unwrap().clone();
        for head in &got {
            let line = head.lines().next().unwrap_or_default();
            if line
                .split(' ')
                .nth(1)
                .unwrap_or_default()
                .starts_with("/pt")
            {
                let over = head.to_ascii_lowercase().contains("x-http-method-override");
                if method == "PUT" && !over {
                    continue; // a PUT is no chat call once the override is gone
                }
                wrong.push(format!(
                    "{method} {target} ({status}): passthrough sent `{line}`{}",
                    if over {
                        " with x-http-method-override"
                    } else {
                        ""
                    }
                ));
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
