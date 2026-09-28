//! A per-tenant "no retention" policy, set by an auth plugin.

mod common;

use common::*;
use respan_gateway_core::http::{self, StatusCode, header::AUTHORIZATION};
use respan_gateway_core::serde_json::json;
use respan_gateway_core::{Body, ClientContext, Fault, Plugin, Response, async_trait};
use respan_gateway_testkit::{mock, post};
use respan_safety::calls::Judge;
use respan_safety::retention::{CallLog, ContentLog, NoRetention, NoStore, TenantAuth};

fn auth() -> TenantAuth {
    TenantAuth::new()
        .tenant("k-open", "acme", false)
        .tenant("k-zdr", "bank", true)
}

fn with_key(mut req: http::Request<Body>, key: &str) -> http::Request<Body> {
    let value = format!("Bearer {key}").parse().expect("header");
    req.headers_mut().insert(AUTHORIZATION, value);
    req
}

#[tokio::test]
async fn a_no_retention_tenants_content_is_not_stored_but_billing_flows() {
    let (content, calls) = (ContentLog::default(), CallLog::default());
    let h = Harness::new(vec![
        ("auth", plugin(auth())),
        ("content", plugin(content.clone())),
        ("calls", plugin(calls.clone())),
    ]);
    h.reply(OPENAI, oa_whole("secret answer"));
    h.reply(OPENAI, oa_whole("public answer"));
    h.text(with_key(openai_request("secret prompt", false), "k-zdr"))
        .await;
    h.text(with_key(openai_request("public prompt", false), "k-open"))
        .await;
    let content = content.records();
    assert_eq!(content[0].tenant.as_deref(), Some("bank"));
    assert_eq!((&content[0].prompt, &content[0].completion), (&None, &None));
    assert!(
        content[1]
            .prompt
            .as_deref()
            .is_some_and(|p| p.contains("public prompt"))
    );
    assert_eq!(content[1].completion.as_deref(), Some("public answer"));
    let calls = calls.records();
    assert_eq!(calls[0].tenant.as_deref(), Some("bank"));
    assert_eq!(calls[0].prompt, None);
    assert!(!calls[0].usage.is_empty(), "billing still sees usage");
    assert!(calls[1].prompt.is_some());
}

#[tokio::test]
async fn an_unknown_key_is_refused_and_the_gateway_key_never_reaches_a_provider() {
    let h = Harness::new(vec![("auth", plugin(auth()))]);
    let (status, _, _) = h.text(with_key(openai_request("x", false), "nope")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    h.reply(OPENAI, oa_whole("ok"));
    h.text(with_key(openai_request("x", false), "k-open")).await;
    let sent = h.requests_to(OPENAI);
    assert_eq!(sent.len(), 1);
    assert!(!sent[0].headers().contains_key(AUTHORIZATION));
}

#[tokio::test]
async fn a_log_ordered_before_auth_still_honours_the_mark_at_the_end() {
    let content = ContentLog::default();
    let h = Harness::new(vec![
        ("content", plugin(content.clone())),
        ("auth", plugin(auth())),
    ]);
    h.reply(OPENAI, oa_whole("secret answer"));
    h.text(with_key(openai_request("secret prompt", false), "k-zdr"))
        .await;
    let r = &content.records()[0];
    assert_eq!((&r.prompt, &r.completion), (&None, &None));
}

/// F7: a call made before the auth plugin ran carries no mark, so the
/// provider edge retains a no-retention tenant's prompt.
#[tokio::test]
#[ignore = "F7: the policy mark is a plugin's extension, set in order"]
async fn a_judge_ordered_before_auth_is_covered_by_the_policy() {
    let calls = CallLog::default();
    let h = Harness::new(vec![
        ("judge", plugin(Judge::new("judge", "openai"))),
        ("auth", plugin(auth())),
        ("calls", plugin(calls.clone())),
    ]);
    h.reply(JUDGE, oa_whole("FINE"));
    h.reply(OPENAI, oa_whole("ok"));
    h.text(with_key(openai_request("secret prompt", false), "k-zdr"))
        .await;
    assert!(calls.records().iter().all(|r| r.prompt.is_none()));
}

/// Removes the mark, as a buggy or hostile plugin could.
struct Forget;

#[async_trait]
impl Plugin for Forget {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.head_mut().extensions.remove::<NoRetention>();
        Ok(None)
    }
}

/// F7: any plugin can remove the mark; the core keeps no per-request
/// policy that plugins cannot change.
#[tokio::test]
#[ignore = "F7: any plugin can remove the policy mark"]
async fn the_policy_mark_cannot_be_removed_by_a_later_plugin() {
    let calls = CallLog::default();
    let h = Harness::new(vec![
        ("auth", plugin(auth())),
        ("forget", plugin(Forget)),
        ("calls", plugin(calls.clone())),
    ]);
    h.reply(OPENAI, oa_whole("ok"));
    h.text(with_key(openai_request("secret prompt", false), "k-zdr"))
        .await;
    assert_eq!(calls.records()[0].prompt, None);
}

#[tokio::test]
async fn an_openai_provider_is_asked_not_to_store() {
    let h = Harness::new(vec![("auth", plugin(auth())), ("nostore", plugin(NoStore))]);
    h.reply(OPENAI, oa_whole("ok"));
    h.reply(OPENAI, oa_whole("ok"));
    h.text(with_key(openai_request("x", false), "k-zdr")).await;
    h.text(with_key(openai_request("x", false), "k-open")).await;
    let sent = h.sent_to(OPENAI);
    assert_eq!(sent[0]["store"], false);
    assert!(sent[1].get("store").is_none());
}

/// F14 (fixed by `edit_call`, now `edit_call`): "do not store" is set on the call as sent,
/// so it reaches an OpenAI provider of an Anthropic client too.
#[tokio::test]
async fn a_translated_call_is_asked_not_to_store() {
    let h = Harness::new(vec![("auth", plugin(auth())), ("nostore", plugin(NoStore))]);
    h.reply(OPENAI, oa_whole("ok"));
    let (status, body, _) = h
        .text(with_key(anthropic_request("x", false), "k-zdr"))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(h.sent_to(OPENAI)[0]["store"], false);
}

/// F15: a provider's error text, which may quote the prompt, reaches every
/// end hook as `Outcome.end`'s message, with nothing to tell content from
/// status and code.
#[tokio::test]
async fn a_providers_error_text_reaches_end_hooks_as_is() {
    let h = Harness::new(vec![("auth", plugin(auth()))]);
    let error = json!({ "error": { "message": "Invalid prompt: 'secret prompt'", "type": "invalid_request_error" } });
    h.reply(
        OPENAI,
        mock::json(StatusCode::BAD_REQUEST, &error.to_string()),
    );
    let (_, _, outcome) = h
        .text(with_key(openai_request("secret prompt", false), "k-zdr"))
        .await;
    let message = outcome.end.err().map(|f| f.message().to_owned());
    assert!(message.is_some_and(|m| m.contains("secret prompt")));
}

#[tokio::test]
async fn the_mark_set_by_the_adapter_reaches_every_hook() {
    // An embedder can set the mark before `handle`, ahead of every plugin.
    let calls = CallLog::default();
    let h = Harness::new(vec![
        ("judge", plugin(Judge::new("judge", "openai"))),
        ("calls", plugin(calls.clone())),
    ]);
    h.reply(JUDGE, oa_whole("FINE"));
    h.reply(OPENAI, oa_whole("ok"));
    let mut req = post(
        CHAT,
        json!({ "model": "m", "messages": [{ "role": "user", "content": "s" }] }).to_string(),
    );
    req.extensions_mut().insert(NoRetention);
    h.text(req).await;
    let records = calls.records();
    assert_eq!(records.len(), 2);
    assert!(records.iter().all(|r| r.prompt.is_none()));
}
