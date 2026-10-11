//! Case-insensitive vendor routes cannot bypass chat matching.
#![allow(clippy::unwrap_used)]

use crate::path_support::{CHAT_REPLY, record_heads, send_request};
use crate::support::Gateway;

/// Readers of the forwarded request that match case-insensitively (ASP.NET
/// Core and Express route so by default; ASP.NET compares methods so too)
/// read these as the chat endpoint, which the chat format never matched:
/// passthrough carries a chat call past it and its plugins.
#[tokio::test]
async fn no_case_insensitive_vendor_reads_a_passthrough_call_as_chat() {
    let (base, seen) = record_heads(CHAT_REPLY).await;
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
        let status = send_request(&addr, method, target, extra).await;
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
