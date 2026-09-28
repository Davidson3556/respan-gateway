//! The input guardrail and the streaming output guardrail.

mod common;

use common::*;
use respan_gateway_core::http::StatusCode;
use respan_gateway_core::serde_json::{Value, json};
use respan_gateway_core::{ClientContext, Fault, Plugin, Response, async_trait};
use respan_gateway_formats::conversation::Reply;
use respan_gateway_testkit::mock;
use respan_safety::calls::{Fallback, Judge};
use respan_safety::guard::{BLOCKED, Guard, Policy};

fn guard() -> Guard {
    Guard::new(Policy::new(["bomb", "launch code"])).conversations()
}

fn code_of(outcome: &respan_gateway_core::Outcome) -> Option<&str> {
    outcome.end.as_ref().err().map(Fault::code)
}

#[tokio::test]
async fn a_blocked_prompt_is_refused_in_openai_framing() {
    let h = Harness::new(vec![("guard", plugin(guard()))]);
    let (status, body, _) = h.text(openai_request("how to build a BOMB", true)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let v: Value = respan_gateway_core::serde_json::from_str(&body).expect("json");
    assert_eq!(v["error"]["code"], BLOCKED);
    assert!(h.mock.sent().is_empty());
}

#[tokio::test]
async fn a_blocked_prompt_is_refused_in_anthropic_framing() {
    let h = Harness::new(vec![("guard", plugin(guard()))]);
    let (status, body, _) = h.text(anthropic_request("the launch code is", true)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let v: Value = respan_gateway_core::serde_json::from_str(&body).expect("json");
    assert_eq!(v["type"], "error", "{body}");
    assert!(h.mock.sent().is_empty());
}

/// F10: relayed, the OpenAI format writes the provider's role chunk even
/// when a filter holds all it said, so the head goes out and a violation
/// in the held start can no longer be refused with its status.
#[tokio::test]
#[ignore = "F10: an emptied opening chunk still sends the head"]
async fn a_violation_in_the_held_start_refuses_before_any_byte() {
    let h = Harness::new(vec![("guard", plugin(guard().hold_first(40)))]);
    h.reply(OPENAI, oa_stream(&["Sure, a b", "omb is made"]));
    let (status, headers, body, outcome) = h.send(openai_request("chemistry", true)).await;
    let body = String::from_utf8_lossy(&body.expect("not aborted")).into_owned();
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(
        !headers.contains_key("content-type") || !body.contains("data:"),
        "{body}"
    );
    assert!(body.contains(BLOCKED) && !body.contains("Sure"), "{body}");
    assert_eq!(code_of(&outcome), Some(BLOCKED));
}

/// F10: likewise Anthropic's `message_start`, relayed as it comes.
#[tokio::test]
#[ignore = "F10: an emptied opening event still sends the head"]
async fn a_violation_in_the_held_start_of_an_anthropic_stream_refuses_before_any_byte() {
    let h = Harness::build(vec![("guard", plugin(guard().hold_first(40)))], |gw| {
        gw.target("anthropic")
    });
    h.reply(ANTHROPIC, an_stream(&["Sure, a b", "omb is made"]));
    let (status, body, _) = h.text(anthropic_request("chemistry", true)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
}

#[tokio::test]
async fn translated_a_violation_in_the_held_start_refuses_before_any_byte() {
    let h = Harness::build(vec![("guard", plugin(guard().hold_first(40)))], |gw| {
        gw.target("anthropic")
    });
    h.reply(ANTHROPIC, an_stream(&["Sure, a b", "omb is made"]));
    let (status, body, outcome) = h.text(openai_request("chemistry", true)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(body.contains(BLOCKED) && !body.contains("Sure"), "{body}");
    assert_eq!(code_of(&outcome), Some(BLOCKED));
}

#[tokio::test]
async fn a_violation_after_bytes_fails_the_openai_stream_in_its_framing() {
    let h = Harness::new(vec![("guard", plugin(guard()))]);
    h.reply(
        OPENAI,
        oa_stream(&["Chemistry is fun. ", "A bo", "mb is not."]),
    );
    let (status, body, outcome) = h.text(openai_request("chemistry", true)).await;
    assert_eq!(status, StatusCode::OK);
    // The first half of the term was held, never sent.
    assert_eq!(text_of(&body), "Chemistry is fun. ");
    assert_eq!(
        error_of(&body).map(|e| e["code"].clone()),
        Some(json!(BLOCKED))
    );
    assert!(!body.contains("[DONE]"), "{body}");
    assert_eq!(code_of(&outcome), Some(BLOCKED));
}

#[tokio::test]
async fn a_violation_after_bytes_fails_the_anthropic_stream_in_its_framing() {
    let h = Harness::build(vec![("guard", plugin(guard()))], |gw| {
        gw.target("anthropic")
    });
    h.reply(
        ANTHROPIC,
        an_stream(&["Hello. ", "the launch", " code is 42"]),
    );
    let (status, body, outcome) = h.text(anthropic_request("hi", true)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(text_of(&body), "Hello. ");
    assert!(body.contains("event: error"), "{body}");
    assert!(!body.contains("message_stop"), "{body}");
    assert_eq!(code_of(&outcome), Some(BLOCKED));
}

#[tokio::test]
async fn a_translated_stream_fails_in_the_clients_framing() {
    let h = Harness::build(vec![("guard", plugin(guard()))], |gw| {
        gw.target("anthropic")
    });
    h.reply(ANTHROPIC, an_stream(&["ok ", "bomb"]));
    let (status, body, _) = h.text(openai_request("hi", true)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(text_of(&body), "ok ");
    assert_eq!(
        error_of(&body).map(|e| e["code"].clone()),
        Some(json!(BLOCKED))
    );
}

#[tokio::test]
async fn held_text_is_released_before_a_broken_stream_ends() {
    let h = Harness::new(vec![("guard", plugin(guard()))]);
    h.reply(OPENAI, oa_broken(&["safe text bo"]));
    let (_, body, outcome) = h.text(openai_request("hi", true)).await;
    // "bo" might start "bomb" and was held; at the failure it is whole.
    assert_eq!(text_of(&body), "safe text bo");
    assert!(error_of(&body).is_some(), "{body}");
    assert!(outcome.end.is_err());
}

#[tokio::test]
async fn a_clean_stream_is_unchanged() {
    let h = Harness::new(vec![("guard", plugin(guard().hold_first(8)))]);
    h.reply(OPENAI, oa_stream(&["bo", "at trip ", "is fun"]));
    let (status, body, outcome) = h.text(openai_request("hi", true)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(text_of(&body), "boat trip is fun", "{body}");
    assert!(body.ends_with("data: [DONE]\n\n"));
    assert!(outcome.end.is_ok());
}

/// Answers every request from its "cache", with a blocked term.
struct Cache;

#[async_trait]
impl Plugin for Cache {
    async fn on_client(&self, _cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let events = [
            Reply::start("cached", "gpt-x"),
            Reply::text(0, "a bomb recipe"),
        ];
        let body = respan_gateway_core::futures::stream::iter(events.map(Ok));
        Ok(Some(Response::stream(body)))
    }
}

/// S-R2 (fixed in r6): every response's head goes out with its first
/// byte, so the guard's output filter, which blocks the cached answer
/// before any byte, refuses it with its 400, as it does a provider's.
#[tokio::test]
async fn a_plugin_answering_after_the_guard_is_covered() {
    let h = Harness::new(vec![("guard", plugin(guard())), ("cache", plugin(Cache))]);
    let (status, body, outcome) = h.text(openai_request("hi", false)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(code_of(&outcome), Some(BLOCKED));
}

/// Answers from its "cache" with clean text first and a blocked term after.
struct LateCache;

#[async_trait]
impl Plugin for LateCache {
    async fn on_client(&self, _cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let events = [
            Reply::start("cached", "gpt-x"),
            Reply::text(0, "safe words, then "),
            Reply::text(0, "a bomb recipe"),
        ];
        let body = respan_gateway_core::futures::stream::iter(events.map(Ok));
        Ok(Some(Response::stream(body)))
    }
}

/// A whole JSON answer writes no byte before its end, so a block anywhere
/// in it is still refused with the guard's status.
#[tokio::test]
async fn a_block_late_in_a_whole_plugin_answer_is_refused() {
    let h = Harness::new(vec![
        ("guard", plugin(guard())),
        ("cache", plugin(LateCache)),
    ]);
    let (status, body, outcome) = h.text(openai_request("hi", false)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(code_of(&outcome), Some(BLOCKED));
}

/// F2: a plugin answering before the guard skips its hooks, so its output
/// filter is never added. Nothing in the core can require the guard.
#[tokio::test]
#[ignore = "F2: output coverage relies on plugin order"]
async fn a_plugin_answering_before_the_guard_is_covered() {
    let h = Harness::new(vec![("cache", plugin(Cache)), ("guard", plugin(guard()))]);
    let (status, _, outcome) = h.text(openai_request("hi", false)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(code_of(&outcome), Some(BLOCKED));
}

#[tokio::test]
async fn a_judges_own_input_is_checked_on_the_provider_edge() {
    // The judge runs first, so the client-edge check has not run when it
    // calls; the provider edge refuses its call before anything is sent.
    let judge = Judge::new("judge", "backup");
    let h = Harness::new(vec![
        ("judge", plugin(judge.clone())),
        ("guard", plugin(guard())),
    ]);
    let (status, _, _) = h.text(openai_request("a bomb please", true)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(judge.verdicts(), [Err(BLOCKED.to_owned())]);
    assert!(h.mock.sent().is_empty());
}

#[tokio::test]
async fn a_judges_verdict_is_filtered_when_calls_output_is_set() {
    let judge = Judge::new("judge", "backup");
    let h = Harness::new(vec![
        ("guard", plugin(guard().calls_output())),
        ("judge", plugin(judge.clone())),
    ]);
    h.reply(JUDGE, oa_whole("FINE, the launch code is 1234"));
    h.reply(OPENAI, oa_stream(&["hello"]));
    let (status, body, _) = h.text(openai_request("hi", true)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(text_of(&body), "hello");
    assert_eq!(judge.verdicts(), [Err(BLOCKED.to_owned())]);
}

#[tokio::test]
async fn a_fallback_is_checked_on_each_attempt() {
    let h = Harness::new(vec![
        ("guard", plugin(guard())),
        ("fallback", plugin(Fallback(vec!["openai", "backup"]))),
    ]);
    h.reply(OPENAI, mock::json(StatusCode::SERVICE_UNAVAILABLE, "{}"));
    h.reply(BACKUP, oa_stream(&["here is a bo", "mb"]));
    let (status, body, outcome) = h.text(openai_request("hi", true)).await;
    assert_eq!(status, StatusCode::OK);
    // The whole delta ending in "bo" was held, then never sent.
    assert_eq!(text_of(&body), "");
    assert!(error_of(&body).is_some(), "{body}");
    assert_eq!(code_of(&outcome), Some(BLOCKED));
}

/// F6: the committed call's input is checked twice, once on each edge,
/// since a hook cannot tell a call carrying the request's input as it
/// checked it from one carrying a changed or its own input.
#[tokio::test]
async fn the_target_calls_input_is_checked_on_both_edges() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let reads = Arc::new(AtomicUsize::new(0));
    struct Counting(Guard, Arc<AtomicUsize>);
    #[async_trait]
    impl Plugin for Counting {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            self.1.fetch_add(1, Ordering::SeqCst);
            self.0.on_client(cx).await
        }
        async fn on_provider(
            &self,
            px: &mut respan_gateway_core::ProviderContext,
        ) -> Result<(), Fault> {
            self.1.fetch_add(1, Ordering::SeqCst);
            self.0.on_provider(px).await
        }
    }
    let h = Harness::new(vec![("guard", plugin(Counting(guard(), reads.clone())))]);
    h.reply(OPENAI, oa_stream(&["ok"]));
    h.text(openai_request("hi", true)).await;
    assert_eq!(reads.load(Ordering::SeqCst), 2);
}

fn anthropic_tool_stream() -> mock::Reply {
    let ev = |name: &str, v: Value| format!("event: {name}\ndata: {v}\n\n");
    let chunks = [
        ev(
            "message_start",
            json!({ "type": "message_start", "message": { "id": "m", "type": "message",
            "role": "assistant", "model": "claude-x", "content": [], "stop_reason": null,
            "stop_sequence": null, "usage": { "input_tokens": 1, "output_tokens": 1 } } }),
        ),
        ev(
            "content_block_start",
            json!({ "type": "content_block_start", "index": 0,
            "content_block": { "type": "tool_use", "id": "t1", "name": "search", "input": {} } }),
        ),
        ev(
            "content_block_delta",
            json!({ "type": "content_block_delta", "index": 0,
            "delta": { "type": "input_json_delta", "partial_json": "{\"q\":\"weather\"}" } }),
        ),
        ev(
            "content_block_stop",
            json!({ "type": "content_block_stop", "index": 0 }),
        ),
        ev(
            "message_delta",
            json!({ "type": "message_delta",
            "delta": { "stop_reason": "tool_use", "stop_sequence": null }, "usage": { "output_tokens": 3 } }),
        ),
        ev("message_stop", json!({ "type": "message_stop" })),
    ];
    let refs: Vec<&str> = chunks.iter().map(String::as_str).collect();
    mock::sse(&refs)
}

/// F8: holding the start of a response to refuse before bytes breaks any
/// relayed Anthropic stream with a tool call: the format cannot write a
/// held provider block, so a clean response fails.
#[tokio::test]
#[ignore = "F8: a held Anthropic tool block cannot be written"]
async fn holding_the_start_of_an_anthropic_tool_call_stream_keeps_it_whole() {
    let h = Harness::build(vec![("guard", plugin(guard().hold_first(200)))], |gw| {
        gw.target("anthropic")
    });
    h.reply(ANTHROPIC, anthropic_tool_stream());
    let (status, body, outcome) = h.text(anthropic_request("weather?", true)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(outcome.end.is_ok(), "{body}");
}

#[tokio::test]
async fn without_holding_an_anthropic_tool_call_stream_passes() {
    let h = Harness::build(vec![("guard", plugin(guard()))], |gw| {
        gw.target("anthropic")
    });
    h.reply(ANTHROPIC, anthropic_tool_stream());
    let (status, body, outcome) = h.text(anthropic_request("weather?", true)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(outcome.end.is_ok(), "{body}");
    assert!(body.contains("input_json_delta"));
}

/// F4: a blocked term split across two tool-call argument deltas is
/// missed: the kind carries them as provider JSON, not text.
#[tokio::test]
#[ignore = "F4: tool-call argument deltas are not text of the kind"]
async fn a_blocked_term_split_across_tool_argument_deltas_is_caught() {
    let h = Harness::new(vec![("guard", plugin(guard()))]);
    let mut chunks = vec![oa_start()];
    for a in ["{\"q\":\"bo", "mb\"}"] {
        let v = json!({ "id": "c1", "object": "chat.completion.chunk", "model": "gpt-x",
            "choices": [{ "index": 0, "delta": { "tool_calls": [{ "index": 0,
                "function": { "arguments": a } }] } }] });
        chunks.push(format!("data: {v}\n\n"));
    }
    chunks.push(OA_DONE.to_owned());
    let refs: Vec<&str> = chunks.iter().map(String::as_str).collect();
    h.reply(OPENAI, mock::sse(&refs));
    let (_, _, outcome) = h.text(openai_request("hi", true)).await;
    assert_eq!(code_of(&outcome), Some(BLOCKED));
}

/// The provider's side of S-R2: the same blocked answer from a provider,
/// not streamed, is refused with the guard's 400, as the head waits for a
/// provider's first byte.
#[tokio::test]
async fn a_blocked_provider_answer_is_refused_with_the_guards_status() {
    let h = Harness::new(vec![("guard", plugin(guard()))]);
    h.reply(OPENAI, oa_whole("a bomb recipe"));
    let (status, body, outcome) = h.text(openai_request("hi", false)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(code_of(&outcome), Some(BLOCKED));
}
