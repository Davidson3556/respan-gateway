//! Vendor path segments retain their encoded boundaries.
#![allow(clippy::unwrap_used)]

use crate::path_support::{CHAT_REPLY, record_lines, send_post};
use crate::support::Gateway;

/// Passthrough forwards a vendor's path as the vendor needs it: a `%2F`
/// inside one segment (a Bedrock inference-profile ARN, a GCS object
/// name) stays one segment, a Vertex/Gemini custom method keeps its `:`,
/// and bytes that are not UTF-8 are not rewritten.
#[tokio::test]
async fn passthrough_keeps_a_vendors_path_segments() {
    let (base, seen) = record_lines(CHAT_REPLY).await;
    let gw = Gateway::start(&format!(
        "providers = {{ p = {{ formats = \"openai-chat\", base_url = \"{base}/chatv1\" }}, any = {{ formats = \"passthrough\", base_url = \"{base}/pt\" }} }}\nformats = [\"openai-chat\", \"passthrough\"]\nplugins = {{ routing = {{ default = \"p\" }} }}\n"
    ))
    .await;
    let addr = gw.base.trim_start_matches("http://").to_owned();
    let mut wrong = Vec::new();
    for sent in [
        "/model/arn%3Aaws%3Abedrock%3Aus-east-1%3A1%3Aapplication-inference-profile%2Fabc/invoke",
        "/storage/v1/b/bkt/o/dir%2Ffile.txt",
        "/v1beta/models/gemini-2.0-flash:generateContent",
        "/v1/projects/p/locations/l/publishers/anthropic/models/claude@2024:rawPredict",
        "/v1/files/file-AbC123%2B%2F==",
    ] {
        seen.lock().unwrap().clear();
        send_post(&addr, sent).await;
        let got = seen.lock().unwrap().clone();
        let want = format!("POST /pt{sent} HTTP/1.1");
        if got != [want.clone()] {
            wrong.push(format!("sent {sent}\n   vendor got {got:?}"));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
