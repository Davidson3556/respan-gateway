//! Chat endpoint spellings cannot bypass format selection.
#![allow(clippy::unwrap_used)]

use crate::path_support::{CHAT_REPLY, record_lines, send_post};
use crate::support::Gateway;

#[tokio::test]
async fn no_spelling_of_the_chat_path_reaches_the_vendor_chat_endpoint_through_passthrough() {
    let (base, seen) = record_lines(CHAT_REPLY).await;
    let gw = Gateway::start(&format!(
        "providers = {{ p = {{ formats = \"openai-chat\", base_url = \"{base}/chatv1\" }}, any = {{ formats = \"passthrough\", base_url = \"{base}/pt\" }} }}\nformats = [\"openai-chat\", \"passthrough\"]\nplugins = {{ routing = {{ default = \"p\" }} }}\n"
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
        seen.lock().clear();
        let status = send_post(&addr, target).await;
        let got = seen.lock().clone();
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

/// Passthrough forwards a path's segments as spelled, and the query as it
/// came; a path readers could read two ways (a dot-segment, `;`, `\`, a
/// `%25` or a space, raw or escaped) is no format's, a 404 that reaches no vendor.
#[tokio::test]
async fn passthrough_forwards_the_matched_segments() {
    let (base, seen) = record_lines(CHAT_REPLY).await;
    let gw = Gateway::start(&format!(
        "providers = {{ p = {{ formats = \"openai-chat\", base_url = \"{base}/chatv1\" }}, any = {{ formats = \"passthrough\", base_url = \"{base}/pt\" }} }}\nformats = [\"openai-chat\", \"passthrough\"]\nplugins = {{ routing = {{ default = \"p\" }} }}\n"
    ))
    .await;
    let addr = gw.base.trim_start_matches("http://").to_owned();
    for (sent, forwarded) in [
        ("/v1/%6Dodels", "/pt/v1/%6Dodels"),
        ("/v1/files/file-1?limit=2", "/pt/v1/files/file-1?limit=2"),
        ("/v1/a%2Bb%2F=", "/pt/v1/a%2Bb%2F="),
        ("/v1//files/?a=../b;c", "/pt/v1/files?a=../b;c"),
    ] {
        seen.lock().clear();
        send_post(&addr, sent).await;
        let got = seen.lock().clone();
        assert_eq!(got, [format!("POST {forwarded} HTTP/1.1")], "sent {sent}");
    }
    for sent in [
        "/v1\\models",
        "/v1/a%20b",
        "/v1/./models",
        "/v1/x/../models?a=1",
        "/v1/%2e%2e/models",
        "/v1/a/%2E%2e/%2e/b;x=1//c",
        "/v1/.;x/models",
        "/v1/x%5Cy",
        "/v1/100%25",
    ] {
        seen.lock().clear();
        let status = send_post(&addr, sent).await;
        assert!(status.contains(" 404 "), "sent {sent}: {status}");
        assert!(seen.lock().is_empty(), "sent {sent}");
    }
}
