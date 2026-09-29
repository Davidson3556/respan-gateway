//! Loop 9 audit: helpers copied from l8_paths.rs.
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

/// A front end that decodes the path and passes the result on as a URL
/// (Apache mod_rewrite `[P]` with a captured `$1`, CVE-2023-25690) reads a
/// decoded `?` or `#` as the start of the query or fragment: it reads these
/// as the chat endpoint, which the chat format never matched.
#[tokio::test]
async fn l9_no_decode_then_reparse_reader_reads_a_passthrough_call_as_chat() {
    let (base, seen) = recorder().await;
    let gw = Gateway::start(&gateway_yaml(&base)).await;
    let addr = gw.base.trim_start_matches("http://").to_owned();
    let mut wrong = Vec::new();
    for target in [
        "/v1/chat/completions%3Fx=1",
        "/v1/chat/completions%3F",
        "/v1/chat/completions%23",
        "/v1/chat/completions%23frag",
    ] {
        seen.lock().unwrap().clear();
        let status = send_req(&addr, "POST", target, "").await;
        for head in seen.lock().unwrap().iter() {
            let line = head.lines().next().unwrap_or_default();
            if line
                .split(' ')
                .nth(1)
                .unwrap_or_default()
                .starts_with("/pt")
            {
                wrong.push(format!(
                    "POST {target} ({status}): passthrough sent `{line}`"
                ));
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// Matching lowercases, forwarding must not: a vendor's case-sensitive ids
/// (file ids, base64 ids, a Bedrock ARN, lower- and upper-case escapes) reach
/// it byte for byte.
#[tokio::test]
async fn l9_passthrough_forwards_case_sensitive_paths_byte_for_byte() {
    let (base, seen) = recorder().await;
    let gw = Gateway::start(&gateway_yaml(&base)).await;
    let addr = gw.base.trim_start_matches("http://").to_owned();
    let mut wrong = Vec::new();
    for (method, target) in [
        ("GET", "/v1/files/file-AbC123XyZ/content"),
        ("GET", "/v1/responses/resp_AbCdEf%2B%2FgH=="),
        (
            "POST",
            "/model/arn%3Aaws%3Abedrock%3Aus-east-1%3A123%3Aapplication-inference-profile%2FAbC9/invoke",
        ),
        ("POST", "/model/arn%3aaws%3aBedrock%2fX/Invoke"),
        (
            "POST",
            "/v1beta/models/Gemini-2.0-Flash:generateContent?key=AbC",
        ),
        ("GET", "/v1/Models/GPT-4o"),
    ] {
        seen.lock().unwrap().clear();
        let status = send_req(&addr, method, target, "").await;
        let got = seen.lock().unwrap().clone();
        let lines: Vec<String> = got
            .iter()
            .map(|h| h.lines().next().unwrap_or_default().to_owned())
            .collect();
        let want = format!("{method} /pt{target} HTTP/1.1");
        if lines != [want.clone()] {
            wrong.push(format!(
                "{method} {target} ({status}): want `{want}`, vendor got {lines:?}"
            ));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// Passthrough takes only ASCII letters, digits and `/ - _ . ~ : @ + = , !`
/// after one decode: any other character is refused (404) and never reaches
/// the vendor, however a reader might treat it.
#[tokio::test]
async fn l9_a_path_outside_the_allowed_set_is_refused_before_the_vendor() {
    let (base, seen) = recorder().await;
    let gw = Gateway::start(&gateway_yaml(&base)).await;
    let addr = gw.base.trim_start_matches("http://").to_owned();
    let mut wrong = Vec::new();
    for target in [
        "/v1/files/%FF",
        "/v1/completion%C5%BF",
        "/v1/my%20file.jsonl",
        "/v1/a%22b",
        "/v1/a%2Ab",
        "/v1/a%00b",
        "/v1/a%3Bb",
        "/v1/a%5Cb",
        "/v1/a%25b",
        "/v1/a%3Fb",
        "/v1/a%23b",
    ] {
        seen.lock().unwrap().clear();
        let status = send_req(&addr, "GET", target, "").await;
        if !status.contains(" 404 ") || !seen.lock().unwrap().is_empty() {
            wrong.push(format!(
                "GET {target}: {status}, vendor got {:?}",
                seen.lock().unwrap()
            ));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
