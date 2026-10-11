//! Decoded delimiters, case preservation and the passthrough character allowlist.
#![allow(clippy::unwrap_used)]

use crate::path_support::{CHAT_REPLY, record_heads, send_request};
use crate::support::Gateway;

fn gateway_yaml(base: &str) -> String {
    format!(
        "providers:
  p: {{ formats: openai-chat, base_url: '{base}/chatv1' }}
  any: {{ formats: passthrough, base_url: '{base}/pt' }}
formats: [openai-chat, passthrough]
default: p
"
    )
}

/// A front end that decodes the path and passes the result on as a URL
/// (Apache mod_rewrite `[P]` with a captured `$1`, CVE-2023-25690) reads a
/// decoded `?` or `#` as the start of the query or fragment: it reads these
/// as the chat endpoint, which the chat format never matched.
#[tokio::test]
async fn no_decode_then_reparse_reader_reads_a_passthrough_call_as_chat() {
    let (base, seen) = record_heads(CHAT_REPLY).await;
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
        let status = send_request(&addr, "POST", target, "").await;
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
async fn passthrough_forwards_case_sensitive_paths_byte_for_byte() {
    let (base, seen) = record_heads(CHAT_REPLY).await;
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
        let status = send_request(&addr, method, target, "").await;
        let got = seen.lock().unwrap().clone();
        let lines: Vec<String> = got
            .iter()
            .map(|h| h.lines().next().unwrap_or_default().to_owned())
            .collect();
        let target = target.split("?key=").next().unwrap();
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
async fn a_path_outside_the_allowed_set_is_refused_before_the_vendor() {
    let (base, seen) = record_heads(CHAT_REPLY).await;
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
        let status = send_request(&addr, "GET", target, "").await;
        if !status.contains(" 404 ") || !seen.lock().unwrap().is_empty() {
            wrong.push(format!(
                "GET {target}: {status}, vendor got {:?}",
                seen.lock().unwrap()
            ));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
