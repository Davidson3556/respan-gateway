//! Paths used by real clients pass through a locally hosted proxy.
#![allow(clippy::unwrap_used)]

use crate::path_support::{record_lines, send_proxy_request};
use crate::support::Gateway;

async fn check(vendor: &str, paths: &[(&str, &str)]) -> Vec<String> {
    let (base, seen) = record_lines(r#"{"ok":true}"#).await;
    let gw = Gateway::run(&["proxy", vendor, "--url", &base, "--listen", "127.0.0.1:0"]).await;
    let addr = gw.base.trim_start_matches("http://").to_owned();
    let mut report = Vec::new();
    for (method, path) in paths {
        seen.lock().unwrap().clear();
        let status = send_proxy_request(&addr, method, path).await;
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
