//! Hedging: race a second provider, keep the first success, cancel the other.

mod common;

use std::time::Duration;

use common::*;
use respan_gateway_core::http::StatusCode;
use respan_gateway_core::{Gateway, Transport};
use respan_gateway_formats::{AnthropicMessages, OpenAiChat};
use respan_gateway_routing::hedge::Hedge;
use respan_gateway_testkit::MockTransport;

const SEC: Duration = Duration::from_secs(1);

fn gateway(primary: impl Transport, backup: impl Transport, hedge: Hedge, log: &Log) -> Gateway {
    Gateway::new()
        .provider("openai", OpenAiChat, primary)
        .and_then(|g| g.provider("backup", OpenAiChat, backup))
        .and_then(|g| g.serve(OpenAiChat)?.serve(AnthropicMessages))
        .and_then(|g| g.plugin("log", log.clone())?.plugin("hedge", hedge))
        .expect("gateway")
}

fn hedge() -> Hedge {
    Hedge::new("openai", "backup", 2 * SEC)
}

#[tokio::test(start_paused = true)]
async fn a_slow_primary_loses_to_the_backup_and_is_cancelled() {
    let (mock, log) = (MockTransport::new(), Log::default());
    let gw = gateway(
        Slow::head(mock.at(OPENAI), 5 * SEC),
        mock.at(BACKUP),
        hedge(),
        &log,
    );
    mock.reply(OPENAI, sse(&openai_stream(&["slow"])))
        .reply(BACKUP, sse(&openai_stream(&["fast"])));
    let start = tokio::time::Instant::now();

    let (status, _, body) = send(&gw, chat("gpt-x", true, "hi")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(openai_text(&body.expect("whole")), "fast");
    assert_eq!(start.elapsed().as_secs(), 2);
    // The loser was cancelled before its transport answered.
    assert!(
        mock.sent()
            .iter()
            .all(|r| r.uri().host() != Some("openai.test"))
    );
    let all = log.all();
    // Both calls end before the request's end hook starts.
    assert_eq!(all.last().map(|s| s.edge.as_str()), Some("client"));
    let loser = all
        .iter()
        .find(|s| s.edge == "openai")
        .expect("the loser ends");
    // The loser is told by its `Leg` extension (F4, fixed).
    assert_eq!((loser.status, loser.end.as_str()), (None, "abandoned"));
    let winner = all
        .iter()
        .find(|s| s.edge == "backup")
        .expect("the winner ends");
    assert_eq!((winner.end.as_str(), winner.output_tokens), ("ok", 2.0));
    // The winner's usage counts on its provider edge only.
    assert_eq!(log.client().output_tokens, 0.0);
}

#[tokio::test(start_paused = true)]
async fn a_fast_primary_never_starts_the_backup() {
    let (mock, log) = (MockTransport::new(), Log::default());
    let gw = gateway(mock.at(OPENAI), mock.at(BACKUP), hedge(), &log);
    mock.reply(OPENAI, sse(&openai_stream(&["primary"])));

    let (status, _, body) = send(&gw, chat("gpt-x", true, "hi")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(openai_text(&body.expect("whole")), "primary");
    assert_eq!(mock.sent().len(), 1);
    assert_eq!(log.calls().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_primary_that_fails_fast_starts_the_backup_at_once() {
    let (mock, log) = (MockTransport::new(), Log::default());
    let gw = gateway(mock.at(OPENAI), mock.at(BACKUP), hedge(), &log);
    mock.reply(OPENAI, json_reply(StatusCode::SERVICE_UNAVAILABLE, "{}"))
        .reply(BACKUP, json_reply(StatusCode::OK, &openai_json("backup")));
    let start = tokio::time::Instant::now();

    let (status, _, body) = send(&gw, chat("gpt-x", false, "hi")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(openai_text(&body.expect("whole")), "backup");
    assert_eq!(start.elapsed().as_secs(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_failed_backup_leaves_the_slow_primary_to_answer() {
    let (mock, log) = (MockTransport::new(), Log::default());
    let gw = gateway(
        Slow::head(mock.at(OPENAI), 5 * SEC),
        mock.at(BACKUP),
        hedge(),
        &log,
    );
    mock.reply(OPENAI, json_reply(StatusCode::OK, &openai_json("primary")))
        .reply(BACKUP, json_reply(StatusCode::INTERNAL_SERVER_ERROR, "{}"));
    let start = tokio::time::Instant::now();

    let (status, _, body) = send(&gw, chat("gpt-x", false, "hi")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(openai_text(&body.expect("whole")), "primary");
    assert_eq!(start.elapsed().as_secs(), 5);
}

#[tokio::test(start_paused = true)]
async fn the_backup_may_speak_another_format() {
    let (mock, log) = (MockTransport::new(), Log::default());
    let gw = Gateway::new()
        .provider("openai", OpenAiChat, Slow::head(mock.at(OPENAI), 5 * SEC))
        .and_then(|g| g.provider("anthropic", AnthropicMessages, mock.at(ANTHROPIC)))
        .and_then(|g| g.serve(OpenAiChat))
        .and_then(|g| g.plugin("log", log.clone()))
        .and_then(|g| {
            g.plugin(
                "hedge",
                Hedge::new("openai", "anthropic", 2 * SEC).backup_model("claude-x"),
            )
        })
        .expect("gateway");
    mock.reply(OPENAI, json_reply(StatusCode::OK, &openai_json("primary")))
        .reply(
            ANTHROPIC,
            json_reply(StatusCode::OK, &anthropic_json("claude")),
        );

    let (status, _, body) = send(&gw, chat("gpt-x", false, "hi")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(openai_text(&body.expect("whole")), "claude");
}

/// F1, fixed by `ready()`: the first to send content wins. The primary
/// here answers `200` at once but sends nothing for 10 s; the backup,
/// started at 2 s, has its whole answer by then.
#[tokio::test(start_paused = true)]
async fn the_first_to_send_content_wins() {
    let (mock, log) = (MockTransport::new(), Log::default());
    let primary = Slow::first_byte(mock.at(OPENAI), 10 * SEC);
    let gw = gateway(primary, mock.at(BACKUP), hedge(), &log);
    mock.reply(OPENAI, sse(&openai_stream(&["slow"])))
        .reply(BACKUP, sse(&openai_stream(&["fast"])));

    let (_, _, body) = send(&gw, chat("gpt-x", true, "hi")).await;

    assert_eq!(openai_text(&body.expect("whole")), "fast");
}

/// A primary whose content comes before the delay never starts the backup.
#[tokio::test(start_paused = true)]
async fn a_primary_with_content_before_the_delay_never_starts_the_backup() {
    let (mock, log) = (MockTransport::new(), Log::default());
    // Its role chunk comes at 0.5 s and is held; its first content at 1 s.
    let primary = Slow::first_byte(mock.at(OPENAI), SEC / 2);
    let gw = gateway(primary, mock.at(BACKUP), hedge(), &log);
    mock.reply(OPENAI, sse(&openai_stream(&["slow"])));

    let (_, _, body) = send(&gw, chat("gpt-x", true, "hi")).await;

    assert_eq!(openai_text(&body.expect("whole")), "slow");
    assert_eq!(mock.sent().len(), 1);
}

/// N2, fixed: OpenAI's first chunk (the assistant role, empty content) is
/// held by the decoder until the next, so a primary that sends only that
/// preamble at once loses to a backup with real content.
#[tokio::test(start_paused = true)]
async fn a_primary_sending_only_its_preamble_loses_to_the_backup() {
    let (mock, log) = (MockTransport::new(), Log::default());
    let primary = Slow {
        inner: mock.at(OPENAI),
        head: Duration::ZERO,
        gap: 10 * SEC,
        fast: 1,
    };
    let gw = gateway(primary, mock.at(BACKUP), hedge(), &log);
    mock.reply(OPENAI, sse(&openai_stream(&["slow"])))
        .reply(BACKUP, sse(&openai_stream(&["fast"])));

    let (_, _, body) = send(&gw, chat("gpt-x", true, "hi")).await;

    assert_eq!(openai_text(&body.expect("whole")), "fast");
}
