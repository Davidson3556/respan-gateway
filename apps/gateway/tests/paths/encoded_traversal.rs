//! Encoded separators and parent segments cannot bypass chat matching.
#![allow(clippy::unwrap_used)]

use crate::path_support::{CHAT_REPLY, record_lines, send_post};
use crate::support::Gateway;

/// A `..` after a segment holding `%2F`: matching (`clean_path`) reads the
/// `%2F` as a slash and pops only the part after it, so it sees
/// `/v1/chat/completions/x` (not chat) and hands the request to passthrough;
/// the forwarded path pops the whole raw segment and sends
/// `/v1/chat/completions`. A chat request reaches the vendor's chat
/// endpoint through passthrough, past the chat format and its plugins.
#[tokio::test]
async fn a_chat_request_never_escapes_into_passthrough() {
    let (base, seen) = record_lines(CHAT_REPLY).await;
    let gw = Gateway::start(&format!(
        "providers:
  p: {{ formats: openai-chat, base_url: '{base}/chatv1' }}
  any: {{ formats: passthrough, base_url: '{base}/pt' }}
formats: [openai-chat, passthrough]
default: p
"
    ))
    .await;
    let addr = gw.base.trim_start_matches("http://").to_owned();
    let mut wrong = Vec::new();
    for sent in [
        "/v1/chat/completions/x%2Fy/..",
        "/v1/chat/completions/x%5Cy/..",
        "/v1/chat/completions/x%2F..%2Fy/..",
    ] {
        seen.lock().unwrap().clear();
        let status = send_post(&addr, sent).await;
        let got = seen.lock().unwrap().clone();
        if got
            .iter()
            .any(|l| l.starts_with("POST /pt/v1/chat/completions "))
        {
            wrong.push(format!("sent {sent} ({status})\n   vendor got {got:?}"));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
