//! Fallback across providers and formats, retries and backoff.

mod common;

use std::time::Duration;

use common::*;
use respan_gateway_core::http::StatusCode;
use respan_gateway_core::serde_json::Value;
use respan_gateway_core::{ClientContext, Fault, Gateway, Plugin, Response, async_trait};
use respan_gateway_formats::conversation::Reply;
use respan_gateway_formats::{AnthropicMessages, OpenAiChat};
use respan_gateway_routing::fallback::Fallback;
use respan_gateway_routing::retry::Backoff;
use respan_gateway_testkit::MockTransport;

fn gateway(mock: &MockTransport, fallback: Fallback, log: &Log) -> Gateway {
    Gateway::new()
        .provider("openai", OpenAiChat, mock.at(OPENAI))
        .and_then(|g| g.provider("backup", OpenAiChat, mock.at(BACKUP)))
        .and_then(|g| g.provider("anthropic", AnthropicMessages, mock.at(ANTHROPIC)))
        .and_then(|g| g.serve(OpenAiChat)?.serve(AnthropicMessages))
        .and_then(|g| g.plugin("log", log.clone())?.plugin("fallback", fallback))
        .and_then(|g| g.target("openai"))
        .expect("gateway")
}

fn to_anthropic() -> Fallback {
    Fallback::new(Backoff::once()).then("anthropic", Some("claude-x"))
}

#[tokio::test]
async fn an_openai_failure_falls_back_to_anthropic_translated() {
    let (mock, log) = (MockTransport::new(), Log::default());
    let gw = gateway(&mock, to_anthropic(), &log);
    mock.reply(OPENAI, json_reply(StatusCode::SERVICE_UNAVAILABLE, "{}"))
        .reply(
            ANTHROPIC,
            json_reply(StatusCode::OK, &anthropic_json("bonjour")),
        );

    let (status, _, body) = send(&gw, chat("gpt-x", false, "hi")).await;

    assert_eq!(status, StatusCode::OK);
    let body = body.expect("whole");
    assert_eq!(openai_text(&body), "bonjour", "{body}");
    let sent = mock.sent();
    assert_eq!(
        sent[1].uri().to_string(),
        "http://anthropic.test/v1/messages"
    );
    assert_eq!(sent_json(&sent[1])["model"], "claude-x");
    // The failed call ends abandoned: its answer was dropped unread (F6).
    // The answer's call ends first: `ready()` read its whole body, and the
    // reader ends the call; the failed one ends when the fallback returns.
    let mut calls = log.calls();
    calls.reverse();
    assert_eq!(calls[0].edge, "openai");
    assert_eq!(
        (calls[0].status, calls[0].end.as_str()),
        (Some(503), "abandoned")
    );
    assert_eq!(
        (calls[1].edge.as_str(), calls[1].end.as_str()),
        ("anthropic", "ok")
    );
    // Each call's usage counts on its own edge; the client edge counts only
    // what no call reported, here nothing.
    assert_eq!((calls[0].output_tokens, calls[1].output_tokens), (0.0, 2.0));
    assert_eq!(log.client().output_tokens, 0.0);
}

#[tokio::test]
async fn an_openai_stream_falls_back_to_an_anthropic_stream() {
    let (mock, log) = (MockTransport::new(), Log::default());
    let gw = gateway(&mock, to_anthropic(), &log);
    let anthropic = vec![
        "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-x\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null,\"usage\":{\"input_tokens\":5,\"output_tokens\":1}}}\n\n".to_owned(),
        "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n".to_owned(),
        "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello\"}}\n\n".to_owned(),
        "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n".to_owned(),
        "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\",\"stop_sequence\":null},\"usage\":{\"output_tokens\":2}}\n\n".to_owned(),
        "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n".to_owned(),
    ];
    mock.reply(OPENAI, json_reply(StatusCode::INTERNAL_SERVER_ERROR, "{}"))
        .reply(ANTHROPIC, sse(&anthropic));

    let (status, _, body) = send(&gw, chat("gpt-x", true, "hi")).await;

    assert_eq!(status, StatusCode::OK);
    let body = body.expect("whole");
    assert_eq!(openai_text(&body), "Hello", "{body}");
    assert!(body.ends_with("data: [DONE]\n\n"), "{body}");
}

#[tokio::test]
async fn the_last_failure_reaches_the_client_translated_with_its_retry_after() {
    let (mock, log) = (MockTransport::new(), Log::default());
    let gw = gateway(&mock, to_anthropic(), &log);
    mock.reply(OPENAI, json_reply(StatusCode::SERVICE_UNAVAILABLE, "{}"))
        .reply(ANTHROPIC, retry_after(StatusCode::TOO_MANY_REQUESTS, 30));

    let (status, headers, body) = send(&gw, chat("gpt-x", false, "hi")).await;

    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        headers.get("retry-after").map(|v| v.as_bytes()),
        Some(&b"30"[..])
    );
    let body: Value =
        respan_gateway_core::serde_json::from_str(&body.expect("whole")).expect("json");
    // Anthropic's error, in OpenAI's shape.
    assert_eq!(body["error"]["type"], "rate_limit_error", "{body}");
    assert_eq!(body["error"]["message"], "slow down");
}

#[tokio::test]
async fn an_anthropic_overload_reaches_an_openai_client_with_its_status() {
    let (mock, log) = (MockTransport::new(), Log::default());
    let gw = gateway(&mock, to_anthropic(), &log);
    let overloaded =
        r#"{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}"#;
    mock.reply(OPENAI, json_reply(StatusCode::SERVICE_UNAVAILABLE, "{}"))
        .reply(
            ANTHROPIC,
            json_reply(StatusCode::from_u16(529).expect("status"), overloaded),
        );

    let (status, _, body) = send(&gw, chat("gpt-x", false, "hi")).await;

    assert_eq!(status.as_u16(), 529);
    let body: Value =
        respan_gateway_core::serde_json::from_str(&body.expect("whole")).expect("json");
    assert_eq!(body["error"]["type"], "overloaded_error", "{body}");
}

#[tokio::test(start_paused = true)]
async fn retries_wait_as_retry_after_says() {
    let (mock, log) = (MockTransport::new(), Log::default());
    let gw = gateway(&mock, Fallback::new(Backoff::default()), &log);
    mock.reply(OPENAI, retry_after(StatusCode::TOO_MANY_REQUESTS, 7))
        .reply(OPENAI, json_reply(StatusCode::OK, &openai_json("ok")));
    let start = tokio::time::Instant::now();

    let (status, _, _) = send(&gw, chat("gpt-x", false, "hi")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(start.elapsed().as_secs(), 7);
    assert_eq!(mock.sent().len(), 2);
}

#[tokio::test(start_paused = true)]
async fn without_retry_after_the_backoff_doubles() {
    let (mock, log) = (MockTransport::new(), Log::default());
    let backoff = Backoff {
        attempts: 3,
        base: Duration::from_secs(1),
        ..Backoff::default()
    };
    let gw = gateway(&mock, Fallback::new(backoff), &log);
    mock.reply(OPENAI, json_reply(StatusCode::BAD_GATEWAY, "{}"))
        .reply(OPENAI, json_reply(StatusCode::BAD_GATEWAY, "{}"))
        .reply(OPENAI, json_reply(StatusCode::OK, &openai_json("ok")));
    let start = tokio::time::Instant::now();

    let (status, _, _) = send(&gw, chat("gpt-x", false, "hi")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(start.elapsed().as_secs(), 1 + 2);
}

#[tokio::test(start_paused = true)]
async fn a_retry_after_too_long_moves_on_at_once() {
    let (mock, log) = (MockTransport::new(), Log::default());
    let fallback = Fallback::new(Backoff::default()).then("backup", None);
    let gw = gateway(&mock, fallback, &log);
    mock.reply(OPENAI, retry_after(StatusCode::TOO_MANY_REQUESTS, 3600))
        .reply(
            BACKUP,
            json_reply(StatusCode::OK, &openai_json("from backup")),
        );
    let start = tokio::time::Instant::now();

    let (status, _, body) = send(&gw, chat("gpt-x", false, "hi")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(openai_text(&body.expect("whole")), "from backup");
    assert_eq!(start.elapsed().as_secs(), 0);
}

#[tokio::test]
async fn a_client_error_is_neither_retried_nor_passed_to_the_fallback() {
    let (mock, log) = (MockTransport::new(), Log::default());
    let gw = gateway(
        &mock,
        Fallback::new(Backoff::default()).then("backup", None),
        &log,
    );
    let bad = r#"{"error":{"type":"invalid_request_error","message":"bad"}}"#;
    mock.reply(OPENAI, json_reply(StatusCode::BAD_REQUEST, bad));

    let (status, _, body) = send(&gw, chat("gpt-x", false, "hi")).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    // Relayed: the provider's own bytes.
    assert_eq!(body.expect("whole"), bad);
    assert_eq!(mock.sent().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn an_unreachable_provider_is_retried_then_skipped() {
    let (mock, log) = (MockTransport::new(), Log::default());
    let gw = gateway(
        &mock,
        Fallback::new(Backoff::default()).then("backup", None),
        &log,
    );
    // Nothing queued for OpenAI: every call to it fails to send.
    mock.reply(
        BACKUP,
        json_reply(StatusCode::OK, &openai_json("from backup")),
    );

    let (status, _, body) = send(&gw, chat("gpt-x", false, "hi")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(openai_text(&body.expect("whole")), "from backup");
    let openai = mock
        .sent()
        .iter()
        .filter(|r| r.uri().host() == Some("openai.test"))
        .count();
    assert_eq!(openai, 3);
    // A call that never got a response has no status on the provider edge.
    let calls = log.calls();
    assert_eq!((calls[0].status, calls[0].end.as_str()), (None, "upstream"));
}

/// Use case 6: once the response is committed, it is not retried. The
/// client gets what was sent, then an error in its own framing, and the
/// outcome is a failure, not `stop`. This is right, and the fallback never
/// learns of it.
#[tokio::test]
async fn a_stream_cut_after_bytes_is_not_retried_and_ends_in_error() {
    let (mock, log) = (MockTransport::new(), Log::default());
    let gw = gateway(&mock, to_anthropic(), &log);
    let mut cut = openai_stream(&["Hel", "lo"]);
    cut.truncate(3);
    mock.reply(OPENAI, sse(&cut));

    let (status, _, body) = send(&gw, chat("gpt-x", true, "hi")).await;

    assert_eq!(status, StatusCode::OK);
    let body = body.expect("an error event, not an abort");
    assert_eq!(openai_text(&body), "Hello");
    let last = data(&body).pop().expect("an event");
    assert_eq!(last["error"]["code"], "truncated", "{body}");
    assert_eq!(mock.sent().len(), 1, "no retry after the commit point");
    assert_eq!(log.client().end, "truncated");
}

/// F1/N2, fixed: a provider that answers `200` and then fails before any
/// content, as Anthropic does under load, is fallen back from. The decoder
/// holds `message_start` until the next item, so `ready()` sees the error.
#[tokio::test]
async fn a_200_that_fails_before_any_content_falls_back() {
    let (mock, log) = (MockTransport::new(), Log::default());
    let fallback = Fallback::new(Backoff::once())
        .then("anthropic", Some("claude-x"))
        .then("backup", None);
    let gw = gateway(&mock, fallback, &log);
    mock.reply(OPENAI, json_reply(StatusCode::SERVICE_UNAVAILABLE, "{}"))
        .reply(ANTHROPIC, sse(&anthropic_overloaded_stream()))
        .reply(BACKUP, sse(&openai_stream(&["from backup"])));

    let (status, _, body) = send(&gw, chat("gpt-x", true, "hi")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(openai_text(&body.expect("whole")), "from backup");
}

/// F3: a plugin ordered after the fallback never runs, so its filter never
/// applies and its end hook never fires. Nothing says so.
#[tokio::test]
#[ignore = "F3: a plugin that calls must answer, which silently skips the plugins after it"]
async fn a_redactor_after_the_fallback_still_redacts() {
    let (mock, log) = (MockTransport::new(), Log::default());
    let gw = gateway(&mock, to_anthropic(), &log)
        .plugin("redact", Redact)
        .and_then(|g| g.after("redact", ["fallback"]))
        .expect("the gateway accepts the order");
    mock.reply(
        OPENAI,
        json_reply(StatusCode::OK, &openai_json("my SECRET")),
    );

    let (_, _, body) = send(&gw, chat("gpt-x", false, "hi")).await;

    assert_eq!(openai_text(&body.expect("whole")), "my [redacted]");
}

/// What F3 does today.
#[tokio::test]
async fn today_a_redactor_after_the_fallback_is_skipped_silently() {
    let (mock, log) = (MockTransport::new(), Log::default());
    let gw = gateway(&mock, to_anthropic(), &log)
        .plugin("redact", Redact)
        .and_then(|g| g.after("redact", ["fallback"]))
        .expect("the gateway accepts the order");
    mock.reply(
        OPENAI,
        json_reply(StatusCode::OK, &openai_json("my SECRET")),
    );

    let (status, _, body) = send(&gw, chat("gpt-x", false, "hi")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(openai_text(&body.expect("whole")), "my SECRET");
}

struct Redact;

#[async_trait]
impl Plugin for Redact {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.filter(|r: &mut Reply| {
            if let Reply::Text { text, .. } = r {
                *text = text.replace("SECRET", "[redacted]");
            }
            Ok(())
        })?;
        Ok(None)
    }
}

#[test]
fn backoff_is_capped_and_bounded() {
    let b = Backoff {
        attempts: 5,
        base: Duration::from_secs(1),
        cap: Duration::from_secs(3),
        max_retry_after: Duration::from_secs(10),
    };
    let waits: Vec<_> = (1..5)
        .map(|n| b.wait(n, None).map(|d| d.as_secs()))
        .collect();
    assert_eq!(waits, [Some(1), Some(2), Some(3), Some(3)]);
    assert_eq!(b.wait(5, None), None);
    let mut h = respan_gateway_core::http::HeaderMap::new();
    h.insert(
        "retry-after",
        "Wed, 21 Oct 2015 07:28:00 GMT".parse().expect("value"),
    );
    assert_eq!(b.wait(1, Some(&h)), Some(Duration::from_secs(1)));
}
