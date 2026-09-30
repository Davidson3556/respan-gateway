//! Loop 10 audit: helpers copied from l8_paths.rs.
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

fn gateway_yaml(base: &str) -> String {
    format!(
        "providers:
  p: {{ format: openai-chat, base_url: '{base}/chatv1' }}
  any: {{ format: passthrough, base_url: '{base}/pt' }}
serve: [openai-chat, passthrough]
target: p
"
    )
}

async fn vendor_saw(
    addr: &str,
    seen: &Arc<Mutex<Vec<String>>>,
    method: &str,
    target: &str,
) -> (String, Vec<String>) {
    seen.lock().unwrap().clear();
    let status = send_req(addr, method, target, "").await;
    let lines = seen
        .lock()
        .unwrap()
        .iter()
        .map(|h| h.lines().next().unwrap_or_default().to_owned())
        .collect();
    (status, lines)
}

/// SUSPECTED (exotic reader): a router with suffix-pattern matching (Spring
/// MVC <= 5.2 `useSuffixPatternMatch`, Rails `(.:format)`, Grape) reads
/// `/v1/chat/completions.json` as `/v1/chat/completions`. The chat format
/// does not match it and passthrough relays it unread.
#[tokio::test]
async fn l10_a_suffix_pattern_reader_reads_no_passthrough_call_as_chat() {
    let (base, seen) = recorder().await;
    let gw = Gateway::start(&gateway_yaml(&base)).await;
    let addr = gw.base.trim_start_matches("http://").to_owned();
    let mut wrong = Vec::new();
    for target in ["/v1/chat/completions.json", "/v1/chat/completions.x"] {
        let (status, lines) = vendor_saw(&addr, &seen, "POST", target).await;
        if lines.iter().any(|l| l.contains(" /pt/")) {
            wrong.push(format!("POST {target} ({status}): vendor got {lines:?}"));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// More real client paths (SDKs beyond the pinned 27) pass through, byte for
/// byte.
#[tokio::test]
async fn l10_more_real_client_paths_pass() {
    let (base, seen) = recorder().await;
    let gw = Gateway::start(&gateway_yaml(&base)).await;
    let addr = gw.base.trim_start_matches("http://").to_owned();
    let mut wrong = Vec::new();
    for (method, target) in [
        ("GET", "/v1beta/files/abc-123:download?alt=media"),
        (
            "POST",
            "/v1beta/tunedModels/my-model-abc123:generateContent",
        ),
        (
            "POST",
            "/upload/v1beta/files?upload_id=AbC&upload_protocol=resumable",
        ),
        ("GET", "/v1beta/cachedContents/xyz"),
        ("POST", "/v1internal:streamGenerateContent?alt=sse"),
        ("GET", "/v1/organization/projects/proj_abc/api_keys/key_abc"),
        (
            "GET",
            "/v1/organizations/usage_report/messages?starting_at=2025-01-01T00:00:00Z",
        ),
        (
            "POST",
            "/model/anthropic.claude-3-5-sonnet-20240620-v1%3A0/invoke",
        ),
        ("POST", "/model/my%21model/invoke"),
        (
            "POST",
            "/agents/AG1/agentAliases/TST/sessions/s.1:a-b_c/text",
        ),
        ("GET", "/v1/containers/cntr_1/files/cfile_1/content"),
        ("DELETE", "/v1/models/ft:gpt-3.5-turbo:my-org::AbC123"),
        ("GET", "/api/blobs/sha256:29fdb92e57cf082e"),
        ("POST", "/openai/v1/responses"),
        ("GET", "/backend-api/codex/models?client_version=0.40.0"),
        ("POST", "/v1/mcp/mcpsrv_01AbC"),
        ("GET", "/api/claude_cli_profile?account_uuid=0f0c"),
        (
            "POST",
            "/hf-inference/models/Qwen/Qwen2.5-7B-Instruct/v1/embeddings",
        ),
    ] {
        let (status, lines) = vendor_saw(&addr, &seen, method, target).await;
        let want = format!("{method} /pt{target} HTTP/1.1");
        if lines != [want.clone()] {
            wrong.push(format!(
                "{method} {target} ({status}): vendor got {lines:?}"
            ));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
