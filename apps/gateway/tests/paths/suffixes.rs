//! Suffix matching and additional vendor endpoint paths.
#![allow(clippy::unwrap_used)]

use std::sync::{Arc, Mutex};

use crate::path_support::{CHAT_REPLY, record_heads, send_request};
use crate::support::Gateway;

fn gateway_yaml(base: &str) -> String {
    format!(
        "providers:
  p: {{ formats: openai-chat, base_url: '{base}/chatv1' }}
  any: {{ formats: passthrough, base_url: '{base}/pt' }}
formats: [openai-chat, passthrough]
plugins: {{ routing: {{ default: p }} }}
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
    let status = send_request(addr, method, target, "").await;
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
async fn a_suffix_pattern_reader_reads_no_passthrough_call_as_chat() {
    let (base, seen) = record_heads(CHAT_REPLY).await;
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
async fn more_real_client_paths_pass() {
    let (base, seen) = record_heads(CHAT_REPLY).await;
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
