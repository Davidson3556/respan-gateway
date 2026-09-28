//! The smart router: a judge call picks the target, then the target answers.

mod common;

use std::collections::HashMap;

use common::*;
use respan_gateway_core::Gateway;
use respan_gateway_core::http::{HeaderMap, HeaderValue, StatusCode};
use respan_gateway_core::serde_json::json;
use respan_gateway_formats::{AnthropicMessages, OpenAiChat};
use respan_gateway_routing::smart::SmartRouter;
use respan_gateway_routing::tenant::{Tenant, Tenants};
use respan_gateway_testkit::MockTransport;

const JUDGE: &str = "http://judge.test/v1";

fn router() -> SmartRouter {
    SmartRouter::new("judge", "judge-mini")
        .route("easy", "openai", Some("gpt-mini"))
        .route("hard", "anthropic", Some("claude-x"))
}

fn gateway(mock: &MockTransport, log: &Log) -> Gateway {
    Gateway::new()
        .provider("judge", OpenAiChat, mock.at(JUDGE))
        .and_then(|g| g.provider("openai", OpenAiChat, mock.at(OPENAI)))
        .and_then(|g| g.provider("anthropic", AnthropicMessages, mock.at(ANTHROPIC)))
        .and_then(|g| g.serve(OpenAiChat)?.serve(AnthropicMessages))
        .and_then(|g| g.plugin("log", log.clone())?.plugin("smart", router()))
        .and_then(|g| g.target("openai"))
        .expect("gateway")
}

#[tokio::test]
async fn the_judge_picks_the_target_which_answers() {
    let (mock, log) = (MockTransport::new(), Log::default());
    let gw = gateway(&mock, &log);
    mock.reply(JUDGE, json_reply(StatusCode::OK, &openai_json("hard")))
        .reply(
            ANTHROPIC,
            json_reply(StatusCode::OK, &anthropic_json("a proof")),
        );

    let (status, _, body) = send(&gw, chat("auto", true, "prove Fermat")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(openai_text(&body.expect("whole")), "a proof");
    let sent = mock.sent();
    let judge = sent_json(&sent[0]);
    assert_eq!(judge["model"], "judge-mini");
    assert_eq!(judge["messages"][1]["content"], "prove Fermat");
    // The judge is asked whole while the client streams.
    assert!(judge.get("stream").is_none_or(|s| s == false), "{judge}");
    let target = sent_json(&sent[1]);
    assert_eq!(target["model"], "claude-x");
    assert_eq!(target["stream"], true);
    // Both calls ran the provider edge and count their usage there; the
    // client edge counts none.
    let calls: Vec<_> = log
        .calls()
        .into_iter()
        .map(|c| (c.edge, c.output_tokens))
        .collect();
    assert_eq!(calls, [("judge".into(), 2.0), ("anthropic".into(), 2.0)]);
    assert_eq!(log.client().output_tokens, 0.0);
}

#[tokio::test]
async fn an_anthropic_client_is_judged_by_an_openai_judge() {
    let (mock, log) = (MockTransport::new(), Log::default());
    let gw = gateway(&mock, &log);
    mock.reply(JUDGE, json_reply(StatusCode::OK, &openai_json("easy")))
        .reply(OPENAI, json_reply(StatusCode::OK, &openai_json("4")));

    let (status, _, body) = send(&gw, messages("auto", false, "2+2?")).await;

    assert_eq!(status, StatusCode::OK);
    let body: respan_gateway_core::serde_json::Value =
        respan_gateway_core::serde_json::from_str(&body.expect("whole")).expect("json");
    assert_eq!(body["content"][0]["text"], "4");
    assert_eq!(sent_json(&mock.sent()[1])["model"], "gpt-mini");
}

#[tokio::test]
async fn a_failing_judge_leaves_the_default_target() {
    let (mock, log) = (MockTransport::new(), Log::default());
    let gw = gateway(&mock, &log);
    mock.reply(JUDGE, json_reply(StatusCode::INTERNAL_SERVER_ERROR, "{}"))
        .reply(OPENAI, json_reply(StatusCode::OK, &openai_json("default")));

    let (status, _, body) = send(&gw, chat("gpt-x", false, "hi")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(openai_text(&body.expect("whole")), "default");
    assert_eq!(sent_json(&mock.sent()[1])["model"], "gpt-x");
}

#[tokio::test]
async fn a_judge_answering_nonsense_leaves_the_default_target() {
    let (mock, log) = (MockTransport::new(), Log::default());
    let gw = gateway(&mock, &log);
    mock.reply(
        JUDGE,
        json_reply(StatusCode::OK, &openai_json("I think it is hard-ish")),
    )
    .reply(OPENAI, json_reply(StatusCode::OK, &openai_json("default")));

    let (status, _, _) = send(&gw, chat("gpt-x", false, "hi")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(mock.sent()[1].uri().host(), Some("openai.test"));
}

fn tenant_gateway(mock: &MockTransport, tenants: &Tenants) -> Gateway {
    Gateway::new()
        .provider("judge", OpenAiChat, mock.at(JUDGE))
        .and_then(|g| g.provider("openai", OpenAiChat, mock.at(OPENAI)))
        .and_then(|g| g.provider("anthropic", AnthropicMessages, mock.at(ANTHROPIC)))
        .and_then(|g| g.serve(OpenAiChat))
        .and_then(|g| {
            g.plugin("tenants", tenants.clone())?
                .plugin("smart", router())
        })
        .and_then(|g| g.target("openai"))
        .expect("gateway")
}

fn keys(pairs: &[(&str, &str, &str)]) -> HashMap<String, HeaderMap> {
    pairs
        .iter()
        .map(|(provider, name, value)| {
            let mut h = HeaderMap::new();
            h.insert(
                respan_gateway_core::http::HeaderName::from_bytes(name.as_bytes()).expect("name"),
                HeaderValue::from_str(value).expect("value"),
            );
            ((*provider).to_owned(), h)
        })
        .collect()
}

#[tokio::test]
async fn the_judges_call_carries_the_tenants_key() {
    let (mock, tenants) = (MockTransport::new(), Tenants::default());
    let t = Tenant {
        keys: keys(&[
            ("judge", "authorization", "Bearer judge-key-A"),
            ("anthropic", "x-api-key", "anthropic-key-A"),
        ]),
        ..Tenant::default()
    };
    tenants.set("gw-A", t);
    let gw = tenant_gateway(&mock, &tenants);
    mock.reply(JUDGE, json_reply(StatusCode::OK, &openai_json("hard")))
        .reply(ANTHROPIC, json_reply(StatusCode::OK, &anthropic_json("ok")));
    let mut req = chat("auto", false, "hi");
    req.headers_mut()
        .insert("authorization", HeaderValue::from_static("Bearer gw-A"));

    let (status, _, _) = send(&gw, req).await;

    assert_eq!(status, StatusCode::OK);
    let sent = mock.sent();
    assert_eq!(sent[0].headers()["authorization"], "Bearer judge-key-A");
    assert_eq!(sent[1].headers()["x-api-key"], "anthropic-key-A");
    assert!(sent[1].headers().get("authorization").is_none());
}

/// F7, fixed by the plan's A10: a call with its own input is a new message
/// and carries none of the client's headers, so the client's idempotency
/// key reaches only the target.
#[tokio::test]
async fn the_clients_idempotency_key_does_not_reach_the_judge() {
    let (mock, log) = (MockTransport::new(), Log::default());
    let gw = gateway(&mock, &log);
    mock.reply(JUDGE, json_reply(StatusCode::OK, &openai_json("easy")))
        .reply(OPENAI, json_reply(StatusCode::OK, &openai_json("ok")));
    let mut req =
        chat_with(json!({"model": "auto", "messages": [{"role": "user", "content": "hi"}]}));
    req.headers_mut()
        .insert("idempotency-key", HeaderValue::from_static("k-1"));

    let _ = send(&gw, req).await;

    let sent = mock.sent();
    assert!(sent[0].headers().get("idempotency-key").is_none());
    assert_eq!(sent[1].headers()["idempotency-key"], "k-1");
}

/// F9: identity is a plugin, so its place in the order matters. A router
/// ordered before the tenant plugin calls its judge before the tenant is
/// known; the tenant plugin refuses that call (else it would go out with no
/// key, or the client's gateway key), and the router silently falls back to
/// the default target.
#[tokio::test]
async fn a_judge_ordered_before_the_tenant_plugin_is_refused_and_routing_silently_degrades() {
    let (mock, tenants) = (MockTransport::new(), Tenants::default());
    let t = Tenant {
        keys: keys(&[
            ("judge", "authorization", "Bearer judge-key-A"),
            ("openai", "authorization", "Bearer openai-key-A"),
        ]),
        ..Tenant::default()
    };
    tenants.set("gw-A", t);
    let gw = Gateway::new()
        .provider("judge", OpenAiChat, mock.at(JUDGE))
        .and_then(|g| g.provider("openai", OpenAiChat, mock.at(OPENAI)))
        .and_then(|g| g.provider("anthropic", AnthropicMessages, mock.at(ANTHROPIC)))
        .and_then(|g| g.serve(OpenAiChat))
        .and_then(|g| {
            g.plugin("smart", router())?
                .plugin("tenants", tenants.clone())
        })
        .and_then(|g| g.target("openai"))
        .expect("the gateway accepts either order");
    mock.reply(OPENAI, json_reply(StatusCode::OK, &openai_json("default")));
    let mut req = chat("auto", false, "hi");
    req.headers_mut()
        .insert("authorization", HeaderValue::from_static("Bearer gw-A"));

    let (status, _, _) = send(&gw, req).await;

    assert_eq!(status, StatusCode::OK);
    let sent = mock.sent();
    assert_eq!(sent.len(), 1, "the judge was never asked");
    assert_eq!(sent[0].headers()["authorization"], "Bearer openai-key-A");
}

/// A judge's own input is a new message (A10): none of the client's body
/// or headers reach an Anthropic judge, its end-user id and beta headers
/// included (F5, F7).
#[tokio::test]
async fn an_anthropic_judge_gets_nothing_of_an_anthropic_clients_request() {
    let mock = MockTransport::new();
    let router = SmartRouter::new("judge", "claude-judge").route("easy", "anthropic", None);
    let gw = Gateway::new()
        .provider("judge", AnthropicMessages, mock.at(JUDGE))
        .and_then(|g| g.provider("anthropic", AnthropicMessages, mock.at(ANTHROPIC)))
        .and_then(|g| {
            g.serve(AnthropicMessages)?
                .plugin("smart", router)?
                .target("anthropic")
        })
        .expect("gateway");
    mock.reply(JUDGE, json_reply(StatusCode::OK, &anthropic_json("easy")))
        .reply(ANTHROPIC, json_reply(StatusCode::OK, &anthropic_json("ok")));
    let body = json!({"model": "claude-x", "max_tokens": 900, "system": "You are Bob.",
        "tools": [{"name": "f", "input_schema": {"type": "object"}}],
        "metadata": {"user_id": "u-1"},
        "messages": [{"role": "user", "content": "hi"}]});
    let mut req = respan_gateway_testkit::post(MESSAGES, body.to_string());
    req.headers_mut()
        .insert("anthropic-beta", HeaderValue::from_static("tools-2024"));

    let (status, _, _) = send(&gw, req).await;

    assert_eq!(status, StatusCode::OK);
    let judge = sent_json(&mock.sent()[0]);
    assert!(judge.get("tools").is_none(), "{judge}");
    assert!(judge.get("metadata").is_none(), "{judge}");
    assert_ne!(judge["system"], "You are Bob.", "{judge}");
    assert_eq!(judge["max_tokens"], 5, "{judge}");
    assert!(mock.sent()[0].headers().get("anthropic-beta").is_none());
    // The target, relaying the client's message, gets them all.
    assert_eq!(mock.sent()[1].headers()["anthropic-beta"], "tools-2024");
    assert_eq!(sent_json(&mock.sent()[1])["metadata"]["user_id"], "u-1");
}

/// Writes each call's record to a slow store in its end hook.
struct SlowAudit;

#[respan_gateway_core::async_trait]
impl respan_gateway_core::Plugin for SlowAudit {
    async fn on_provider_end(
        &self,
        _px: &respan_gateway_core::ProviderContext,
        _o: &respan_gateway_core::Outcome,
    ) {
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    }
}

/// N3: D1 puts every ended call's end hooks on the request's critical path.
/// The target's call waits for the judge's audit write, a slow end hook no
/// budget needs, so every judged request is 3 s slower. Pinned as it is.
#[tokio::test(start_paused = true)]
async fn a_slow_end_hook_on_the_judge_delays_the_targets_call() {
    let (mock, log) = (MockTransport::new(), Log::default());
    let gw = gateway(&mock, &log)
        .plugin("audit", SlowAudit)
        .expect("plugin");
    mock.reply(JUDGE, json_reply(StatusCode::OK, &openai_json("easy")))
        .reply(OPENAI, json_reply(StatusCode::OK, &openai_json("ok")));

    let start = tokio::time::Instant::now();
    let resp = gw.handle(chat("auto", false, "hi")).await;
    let waited = start.elapsed();
    drop(resp);
    gw.idle().await;

    assert_eq!(mock.sent().len(), 2);
    assert!(waited >= std::time::Duration::from_secs(3), "{waited:?}");
}
