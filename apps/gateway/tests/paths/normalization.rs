//! Different frontend normalization rules cannot turn passthrough into chat.
#![allow(clippy::unwrap_used)]

use crate::path_support::{CHAT_REPLY, record_lines, send_post};
use crate::support::Gateway;

/// `s` with its `%XX` escapes decoded, as a front end decodes a path.
fn decode(s: &str) -> String {
    percent_encoding::percent_decode_str(s)
        .decode_utf8_lossy()
        .into_owned()
}

fn dots(segs: impl Iterator<Item = String>) -> String {
    let mut kept: Vec<String> = Vec::new();
    for s in segs {
        match s.as_str() {
            "" | "." => {}
            ".." => drop(kept.pop()),
            _ => kept.push(s),
        }
    }
    kept.iter().fold(String::new(), |o, s| o + "/" + s)
}

/// A vendor behind a front end that decodes then resolves dots (nginx's
/// normalized `$uri`), or a servlet container that cuts `;params` from each
/// raw segment first, reads these forwarded paths as its chat endpoint;
/// matching read them as not chat, so passthrough carried a chat call past
/// the chat format and its plugins.
#[tokio::test]
async fn no_front_end_reads_a_passthrough_call_as_chat() {
    let (base, seen) = record_lines(CHAT_REPLY).await;
    let gw = Gateway::start(&format!(
        "providers = {{ p = {{ formats = \"openai-chat\", base_url = \"{base}/chatv1\" }}, any = {{ formats = \"passthrough\", base_url = \"{base}/pt\" }} }}\nformats = [\"openai-chat\", \"passthrough\"]\nplugins = {{ routing = {{ default = \"p\" }} }}\n"
    ))
    .await;
    let addr = gw.base.trim_start_matches("http://").to_owned();
    let mut wrong = Vec::new();
    for sent in [
        "/v1/chat/completions/%3B/%2F..",
        "/v1/chat/completions/x%5C../%2F..",
        "/v1/chat/completions;x%2F..",
        "/v1/chat/completions;x%2Fy",
    ] {
        seen.lock().unwrap().clear();
        let status = send_post(&addr, sent).await;
        let got = seen.lock().unwrap().clone();
        if !status.contains(" 404 ") {
            wrong.push(format!("sent {sent}: {status}, not a 404"));
        }
        for line in &got {
            let target = line.split(' ').nth(1).unwrap_or_default();
            let Some(path) = target.strip_prefix("/pt") else {
                continue;
            };
            let nginx = dots(decode(path).split('/').map(str::to_owned));
            let servlet = dots(
                path.split('/')
                    .map(|s| decode(s.split(';').next().unwrap_or_default())),
            );
            if nginx.ends_with("/chat/completions") || servlet.ends_with("/chat/completions") {
                wrong.push(format!("sent {sent} ({status}): passthrough sent {target}, read {nginx:?} / {servlet:?}"));
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
