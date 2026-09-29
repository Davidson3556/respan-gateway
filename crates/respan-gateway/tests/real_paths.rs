//! The passthrough path allowlist must not refuse the paths real clients
//! (Claude Code, Codex, Gemini, Vertex, Bedrock, Azure) send through
//! `respan-gateway proxy` (use case 12).
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
                let body = r#"{"ok":true}"#;
                let _ = sock.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}", body.len()).as_bytes()).await;
            });
        }
    });
    (format!("http://{addr}"), seen)
}

async fn send_raw(addr: &str, method: &str, target: &str) -> String {
    let body = r#"{"model":"m","max_tokens":5,"messages":[{"role":"user","content":"hi"}]}"#;
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "{method} {target} HTTP/1.1\r\nhost: x\r\nx-api-key: sk-client\r\nauthorization: Bearer sk-client\r\nanthropic-version: 2023-06-01\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
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

async fn check(vendor: &str, paths: &[(&str, &str)]) -> Vec<String> {
    let (base, seen) = recorder().await;
    let gw = Gateway::run(&["proxy", vendor, "--url", &base, "--listen", "127.0.0.1:0"]).await;
    let addr = gw.base.trim_start_matches("http://").to_owned();
    let mut report = Vec::new();
    for (method, path) in paths {
        seen.lock().unwrap().clear();
        let status = send_raw(&addr, method, path).await;
        let got = seen.lock().unwrap().clone();
        let upstream = got
            .first()
            .cloned()
            .unwrap_or_else(|| "(nothing sent)".into());
        let exact = upstream == format!("{method} {path} HTTP/1.1");
        report.push(format!(
            "{} {vendor} {method} {path} -> client {status} | upstream {upstream}",
            if status.contains(" 200 ") && exact {
                "OK  "
            } else {
                "FAIL"
            }
        ));
    }
    report
}

#[tokio::test]
async fn real_client_paths_pass_through_proxy() {
    let anthropic = [
        // /v1/messages is the chat format's (e2e covers it); this mock answers no message.
        ("POST", "/v1/messages/count_tokens?beta=true"),
        ("GET", "/v1/models?limit=1000"),
        ("GET", "/v1/models/claude-sonnet-4-5-20250929"),
        ("POST", "/v1/messages/batches"),
        (
            "GET",
            "/v1/messages/batches/msgbatch_01HkcTjaV5uDC8jWR4ZsDV8d/results",
        ),
        ("GET", "/v1/files/file_011CNha8iCJcU1wXNR6q4V8w/content"),
        ("GET", "/v1/skills/skill_01AbCdEf/versions/1759178010641129"),
        ("POST", "/api/event_logging/batch"),
        ("GET", "/api/oauth/claude_cli/roles"),
        ("POST", "/v1/complete"),
    ];
    let openai = [
        ("POST", "/v1/responses"),
        ("POST", "/v1/responses/compact"),
        ("POST", "/v1/responses/input_tokens"),
        (
            "GET",
            "/v1/responses/resp_68af4030592c81938ec0a5fbab4a3e9f/input_items?limit=20",
        ),
        ("POST", "/backend-api/codex/responses"),
        ("POST", "/v1/embeddings"),
        ("POST", "/v1/audio/transcriptions"),
        (
            "GET",
            "/v1/models/ft:gpt-4o-mini-2024-07-18:org:custom:9x8y7z",
        ),
        ("GET", "/v1/vector_stores/vs_abc/files?filter=completed"),
        (
            "POST",
            "/v1beta/models/gemini-2.5-pro:streamGenerateContent?alt=sse",
        ),
        (
            "POST",
            "/v1/projects/p-1/locations/us-east5/publishers/anthropic/models/claude-sonnet-4@20250514:streamRawPredict",
        ),
        (
            "POST",
            "/model/arn%3Aaws%3Abedrock%3Aus-east-1%3A123456789012%3Aapplication-inference-profile%2Fabc123/converse-stream",
        ),
        (
            "POST",
            "/model/us.anthropic.claude-3-7-sonnet-20250219-v1%3A0/invoke-with-response-stream",
        ),
        (
            "POST",
            "/openai/deployments/gpt-4.1/responses?api-version=2025-04-01-preview",
        ),
        ("GET", "/api/tags"),
    ];
    let mut report = check("--anthropic", &anthropic).await;
    report.extend(check("--openai", &openai).await);
    let text = report.join("\n");
    println!("{text}");
    assert!(!text.contains("FAIL"), "{text}");
}
