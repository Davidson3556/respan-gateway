//! PII redaction before every call and restoration in the response.

mod common;

use std::sync::{Arc, Mutex, PoisonError};

use common::*;
use respan_gateway_core::http::StatusCode;
use respan_gateway_core::serde_json::json;
use respan_gateway_core::{ClientContext, Fault, Plugin, Response, async_trait};
use respan_gateway_formats::conversation::Conversation;
use respan_gateway_testkit::{mock, post};
use respan_safety::calls::{Fallback, Judge};
use respan_safety::pii::{Pii, Redactor, UNREDACTABLE, Vault};

const SECRET: &str = "I am Alice, mail me at alice@example.com";

fn pii() -> Pii {
    Pii::new(Redactor::new(["Alice"]))
}

fn assert_redacted(body: &respan_gateway_core::serde_json::Value) {
    let s = body.to_string();
    assert!(
        !s.contains("Alice") && !s.contains("alice@"),
        "raw PII sent: {s}"
    );
    assert!(s.contains("<PERSON_1>") && s.contains("<EMAIL_1>"), "{s}");
}

#[tokio::test]
async fn a_placeholder_split_across_openai_deltas_is_restored() {
    let h = Harness::new(vec![("pii", plugin(pii()))]);
    h.reply(
        OPENAI,
        oa_stream(&["Hello <PER", "SON_", "1>, I will write to <EMA", "IL_1>."]),
    );
    let (status, body, outcome) = h.text(openai_request(SECRET, true)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        text_of(&body),
        "Hello Alice, I will write to alice@example.com."
    );
    assert!(body.ends_with("data: [DONE]\n\n"), "{body}");
    assert!(outcome.end.is_ok());
    assert_redacted(&h.sent_to(OPENAI)[0]);
}

#[tokio::test]
async fn a_placeholder_split_across_anthropic_deltas_is_restored() {
    let h = Harness::build(vec![("pii", plugin(pii()))], |gw| gw.target("anthropic"));
    h.reply(ANTHROPIC, an_stream(&["Hi <PERSON", "_1>", " bye"]));
    let (status, body, _) = h.text(anthropic_request(SECRET, true)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(text_of(&body), "Hi Alice bye");
    assert!(body.contains("event: message_stop"), "{body}");
    assert_redacted(&h.sent_to(ANTHROPIC)[0]);
}

#[tokio::test]
async fn an_openai_client_of_an_anthropic_provider_gets_restored_text() {
    let h = Harness::build(vec![("pii", plugin(pii()))], |gw| gw.target("anthropic"));
    h.reply(ANTHROPIC, an_stream(&["Dear <PE", "RSON_1>"]));
    let (status, body, _) = h.text(openai_request(SECRET, true)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(text_of(&body), "Dear Alice");
    assert!(body.ends_with("data: [DONE]\n\n"), "{body}");
    assert_redacted(&h.sent_to(ANTHROPIC)[0]);
}

#[tokio::test]
async fn an_anthropic_client_of_an_openai_provider_gets_restored_text() {
    let h = Harness::new(vec![("pii", plugin(pii()))]);
    h.reply(OPENAI, oa_stream(&["Dear <", "EMAIL_1", ">!"]));
    let (status, body, _) = h.text(anthropic_request(SECRET, true)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(text_of(&body), "Dear alice@example.com!");
    assert_redacted(&h.sent_to(OPENAI)[0]);
}

#[tokio::test]
async fn a_whole_response_is_restored() {
    let h = Harness::new(vec![("pii", plugin(pii()))]);
    h.reply(OPENAI, oa_whole("Bye <PERSON_1>."));
    let (status, body, _) = h.text(openai_request(SECRET, false)).await;
    assert_eq!(status, StatusCode::OK);
    let v: respan_gateway_core::serde_json::Value =
        respan_gateway_core::serde_json::from_str(&body).expect("json");
    assert_eq!(v["choices"][0]["message"]["content"], "Bye Alice.");
}

#[tokio::test]
async fn a_tail_that_never_became_a_placeholder_is_flushed_before_the_end() {
    let h = Harness::new(vec![("pii", plugin(pii()))]);
    h.reply(OPENAI, oa_stream(&["x <PERS"]));
    let (_, body, _) = h.text(openai_request(SECRET, true)).await;
    assert_eq!(text_of(&body), "x <PERS");
    // The tail goes before the finish chunk and the terminator.
    let tail = body.find("<PERS").expect("tail");
    assert!(tail < body.find("\"finish_reason\":\"stop\"").expect("finish"));
}

#[tokio::test]
async fn a_held_tail_is_flushed_before_a_broken_stream_ends() {
    let h = Harness::new(vec![("pii", plugin(pii()))]);
    h.reply(OPENAI, oa_broken(&["Hi <PERSON_1> and <PERSON"]));
    let (status, body, outcome) = h.text(openai_request(SECRET, true)).await;
    assert_eq!(status, StatusCode::OK);
    let said = said(&body);
    assert_eq!(
        said[..2],
        [
            Said::Text("Hi Alice and ".into()),
            Said::Text("<PERSON".into())
        ]
    );
    assert!(matches!(said.last(), Some(Said::Error(_))), "{body}");
    assert!(outcome.end.is_err());
}

/// OpenAI's decoder ends at an error event without relaying its bytes, so
/// the held tail goes first. A format that relays it as a said Frame gets
/// the tail after it (F1, in `findings.rs`).
#[tokio::test]
async fn a_held_tail_is_flushed_before_a_relayed_provider_error_event() {
    let h = Harness::new(vec![("pii", plugin(pii()))]);
    h.reply(OPENAI, oa_error_event(&["Hi <PERSON"]));
    let (_, body, _) = h.text(openai_request(SECRET, true)).await;
    assert_eq!(
        said(&body),
        [
            Said::Text("Hi ".into()),
            Said::Text("<PERSON".into()),
            Said::Error(
                json!({ "message": "overloaded", "type": "server_error", "param": null, "code": null })
            ),
        ],
        "{body}"
    );
}

#[tokio::test]
async fn translated_the_held_tail_precedes_the_error_event() {
    let h = Harness::new(vec![("pii", plugin(pii()))]);
    h.reply(OPENAI, oa_error_event(&["Hi <PERSON"]));
    let (_, body, _) = h.text(anthropic_request(SECRET, true)).await;
    let said = said(&body);
    assert_eq!(
        said[..2],
        [Said::Text("Hi ".into()), Said::Text("<PERSON".into())]
    );
    assert!(matches!(said.last(), Some(Said::Error(_))), "{body}");
}

#[tokio::test]
async fn a_fallbacks_second_attempt_is_redacted_and_restored() {
    let h = Harness::new(vec![
        ("pii", plugin(pii())),
        ("fallback", plugin(Fallback(vec!["openai", "backup"]))),
    ]);
    h.reply(
        OPENAI,
        mock::json(
            StatusCode::INTERNAL_SERVER_ERROR,
            r#"{"error":{"message":"down"}}"#,
        ),
    );
    h.reply(BACKUP, oa_stream(&["ok <PERSON_", "1>"]));
    let (status, body, _) = h.text(openai_request(SECRET, true)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(text_of(&body), "ok Alice");
    assert_redacted(&h.sent_to(OPENAI)[0]);
    assert_redacted(&h.sent_to(BACKUP)[0]);
}

#[tokio::test]
async fn a_judge_ordered_before_redaction_never_sees_raw_pii() {
    let judge = Judge::new("judge", "backup");
    let h = Harness::new(vec![
        ("judge", plugin(judge.clone())),
        ("pii", plugin(pii())),
    ]);
    h.reply(JUDGE, oa_whole("ESCALATE for <PERSON_1>"));
    h.reply(BACKUP, oa_stream(&["hi <PERSON_1>"]));
    let (status, body, _) = h.text(openai_request(SECRET, true)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(text_of(&body), "hi Alice");
    assert_redacted(&h.sent_to(JUDGE)[0]);
    // The judge reads its verdict restored, by its own call's filter.
    assert_eq!(judge.verdicts(), [Ok("ESCALATE for Alice".into())]);
}

/// Redaction on the client edge, where `docs/ARCHITECTURE.md` places PII.
struct ClientEdgePii(Pii);

#[async_trait]
impl Plugin for ClientEdgePii {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        // Filters are the one way to edit inputs, so the vault is shared
        // with the restoring filter even though this one runs at once.
        let vault = Arc::new(Mutex::new(Vault::default()));
        let (pii, into) = (self.0.clone(), vault.clone());
        cx.filter(move |chat: &mut Conversation| {
            pii.redact(
                chat,
                &mut into.lock().unwrap_or_else(PoisonError::into_inner),
            )
        })?;
        cx.filter(respan_safety::pii::Restore::shared(vault, false))?;
        Ok(None)
    }
}

/// F2: on the client edge, a judge ordered before the redaction sends raw
/// PII: safety relies on plugin order.
#[tokio::test]
#[ignore = "F2: client-edge redaction relies on plugin order"]
async fn client_edge_redaction_covers_a_judge_ordered_before_it() {
    let h = Harness::new(vec![
        ("judge", plugin(Judge::new("judge", "backup"))),
        ("pii", plugin(ClientEdgePii(pii()))),
    ]);
    h.reply(JUDGE, oa_whole("FINE"));
    h.reply(OPENAI, oa_stream(&["hi"]));
    h.text(openai_request(SECRET, true)).await;
    assert_redacted(&h.sent_to(JUDGE)[0]);
}

#[tokio::test]
async fn pii_in_a_tool_calls_arguments_is_refused_not_sent() {
    let h = Harness::new(vec![("pii", plugin(pii()))]);
    let body = json!({ "model": "gpt-x", "messages": [
        { "role": "user", "content": "look up my friend" },
        { "role": "assistant", "content": null, "tool_calls": [{ "id": "t1", "type": "function",
            "function": { "name": "lookup", "arguments": "{\"who\":\"Alice\"}" } }] },
        { "role": "tool", "tool_call_id": "t1", "content": "found" },
    ] });
    let (status, body, _) = h.text(post(CHAT, body.to_string())).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains(UNREDACTABLE), "{body}");
    assert!(h.mock.sent().is_empty());
}

#[tokio::test]
async fn pii_in_a_tool_result_is_redacted_as_text() {
    let h = Harness::new(vec![("pii", plugin(pii()))]);
    h.reply(OPENAI, oa_whole("done"));
    let body = json!({ "model": "gpt-x", "messages": [
        { "role": "user", "content": "who?" },
        { "role": "assistant", "content": null, "tool_calls": [{ "id": "t1", "type": "function",
            "function": { "name": "lookup", "arguments": "{}" } }] },
        { "role": "tool", "tool_call_id": "t1", "content": "It is Alice" },
    ] });
    let (status, _, _) = h.text(post(CHAT, body.to_string())).await;
    assert_eq!(status, StatusCode::OK);
    let sent = h.sent_to(OPENAI)[0].to_string();
    assert!(
        !sent.contains("Alice") && sent.contains("<PERSON_1>"),
        "{sent}"
    );
}

fn tool_call_stream(arguments: &[&str]) -> mock::Reply {
    let mut chunks = vec![oa_start()];
    let first = json!({ "id": "c1", "object": "chat.completion.chunk", "model": "gpt-x",
        "choices": [{ "index": 0, "delta": { "tool_calls": [{ "index": 0, "id": "t1",
            "type": "function", "function": { "name": "email", "arguments": "" } }] } }] });
    chunks.push(format!("data: {first}\n\n"));
    for a in arguments {
        let v = json!({ "id": "c1", "object": "chat.completion.chunk", "model": "gpt-x",
            "choices": [{ "index": 0, "delta": { "tool_calls": [{ "index": 0,
                "function": { "arguments": a } }] } }] });
        chunks.push(format!("data: {v}\n\n"));
    }
    let fin = json!({ "id": "c1", "object": "chat.completion.chunk", "model": "gpt-x",
        "choices": [{ "index": 0, "delta": {}, "finish_reason": "tool_calls" }] });
    chunks.push(format!("data: {fin}\n\n"));
    chunks.push(OA_DONE.to_owned());
    let refs: Vec<&str> = chunks.iter().map(String::as_str).collect();
    mock::sse(&refs)
}

/// F4: a tool call's argument deltas are `Reply::Other` JSON in the
/// provider's shape; the SDK restores them by OpenAI-specific surgery.
#[tokio::test]
async fn a_placeholder_split_across_tool_call_argument_deltas_is_restored() {
    let h = Harness::new(vec![("pii", plugin(pii().restore_tool_arguments()))]);
    h.reply(OPENAI, tool_call_stream(&["{\"to\":\"<EMA", "IL_1>\"}"]));
    let (_, body, outcome) = h.text(openai_request(SECRET, true)).await;
    assert!(outcome.end.is_ok(), "{body}");
    assert!(body.contains("alice@example.com"), "{body}");
}

/// F4: the same surgery does not exist for Anthropic's `input_json_delta`,
/// so a placeholder split across its deltas reaches the client.
#[tokio::test]
#[ignore = "F4: tool-call argument deltas are not text of the kind"]
async fn a_placeholder_split_across_anthropic_tool_input_deltas_is_restored() {
    let h = Harness::build(
        vec![("pii", plugin(pii().restore_tool_arguments()))],
        |gw| gw.target("anthropic"),
    );
    h.reply(
        ANTHROPIC,
        anthropic_tool_stream(&["{\"to\":\"<EMA", "IL_1>\"}"]),
    );
    let (_, body, outcome) = h.text(anthropic_request(SECRET, true)).await;
    assert!(outcome.end.is_ok(), "{body}");
    assert!(body.contains("alice@example.com"), "{body}");
}

fn anthropic_tool_stream(parts: &[&str]) -> mock::Reply {
    let ev = |name: &str, v: respan_gateway_core::serde_json::Value| {
        format!("event: {name}\ndata: {v}\n\n")
    };
    let mut chunks = vec![
        ev(
            "message_start",
            json!({ "type": "message_start", "message": { "id": "m", "type": "message",
            "role": "assistant", "model": "claude-x", "content": [], "stop_reason": null,
            "stop_sequence": null, "usage": { "input_tokens": 1, "output_tokens": 1 } } }),
        ),
        ev(
            "content_block_start",
            json!({ "type": "content_block_start", "index": 0,
            "content_block": { "type": "tool_use", "id": "t1", "name": "email", "input": {} } }),
        ),
    ];
    for p in parts {
        chunks.push(ev(
            "content_block_delta",
            json!({ "type": "content_block_delta", "index": 0,
            "delta": { "type": "input_json_delta", "partial_json": p } }),
        ));
    }
    chunks.push(ev(
        "content_block_stop",
        json!({ "type": "content_block_stop", "index": 0 }),
    ));
    chunks.push(ev("message_delta", json!({ "type": "message_delta",
        "delta": { "stop_reason": "tool_use", "stop_sequence": null }, "usage": { "output_tokens": 3 } })));
    chunks.push(ev("message_stop", json!({ "type": "message_stop" })));
    let refs: Vec<&str> = chunks.iter().map(String::as_str).collect();
    mock::sse(&refs)
}

#[tokio::test]
async fn without_restoring_tool_arguments_the_client_gets_the_placeholder() {
    let h = Harness::new(vec![("pii", plugin(pii()))]);
    h.reply(OPENAI, tool_call_stream(&["{\"to\":\"<EMA", "IL_1>\"}"]));
    let (_, body, outcome) = h.text(openai_request(SECRET, true)).await;
    assert!(outcome.end.is_ok(), "{body}");
    assert!(body.contains("<EMA") && !body.contains("alice@"), "{body}");
}

#[tokio::test]
async fn other_kinds_are_refused_unless_let_through() {
    let h = Harness::build(vec![("pii", plugin(pii()))], |gw| gw.target("mcp"));
    let (status, _, _) = h
        .text(mcp_request("lookup", json!({ "who": "Alice" })))
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(h.mock.sent().is_empty());
    let open = Harness::build(vec![("pii", plugin(pii().pass_other_kinds()))], |gw| {
        gw.target("mcp")
    });
    open.reply(MCP, mcp_result(json!({ "ok": true })));
    let (status, _, _) = open
        .text(mcp_request("lookup", json!({ "who": "Alice" })))
        .await;
    assert_eq!(status, StatusCode::OK);
    // Let through means sent raw: the kind is not redactable by this SDK.
    assert!(open.sent_to(MCP)[0].to_string().contains("Alice"));
}

/// F9 (fixed by r2 #4): `is_kind` lets a fail-closed plugin refuse another
/// kind with its own status and code, not a 500 a client cannot tell from
/// a crash.
#[tokio::test]
async fn another_kind_is_refused_with_the_plugins_own_status() {
    let h = Harness::build(vec![("pii", plugin(pii()))], |gw| gw.target("mcp"));
    let (status, _, outcome) = h.text(mcp_request("x", json!({}))).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(
        outcome.end.as_ref().err().map(Fault::code),
        Some(UNREDACTABLE)
    );
}
