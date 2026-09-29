//! The routing SDK's promises, against a mock provider.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use respan_gateway_core::Exchange;
use respan_gateway_core::bytes::Bytes;
use respan_gateway_core::futures::StreamExt;
use respan_gateway_core::futures::stream;
use respan_gateway_core::{
    ClientContext, Fault, Gateway, Plugin, ProviderContext, Response, Transport, async_trait, codes,
};
use respan_gateway_formats::OpenAiChat;
use respan_gateway_http::http::{self, HeaderValue, StatusCode, header::RETRY_AFTER};
use respan_gateway_http::{Answer, Ask};
use respan_gateway_routing::{Attempt, Backoff, Role, Until, Watchdog, first_success, race};
use respan_gateway_testkit::{MockTransport, mock, post, read_whole, serve};
use tokio::time::Instant;

const A: &str = "http://a.test/v1";
const B: &str = "http://b.test/v1";
const REQUEST: &str = r#"{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
const ROLE: &str = "data: {\"id\":\"c\",\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"}}]}\n\n";
const HELLO: &str = "data: {\"id\":\"c\",\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello\"}}]}\n\n";
const STOP: &str = "data: {\"id\":\"c\",\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n";
const DONE: &str = "data: [DONE]\n\n";

fn ok() -> mock::Reply {
    mock::sse(&[ROLE, HELLO, STOP, DONE])
}

/// A stream cut off by the provider after `chunks`.
fn cut(chunks: &[&str]) -> mock::Reply {
    let mut reply = mock::sse(chunks);
    reply
        .body_mut()
        .push(Err(Fault::upstream("connection reset")));
    reply
}

fn too_many(seconds: &'static str) -> mock::Reply {
    let mut reply = mock::json(
        StatusCode::TOO_MANY_REQUESTS,
        r#"{"error":{"message":"slow down"}}"#,
    );
    reply
        .headers_mut()
        .insert(RETRY_AFTER, HeaderValue::from_static(seconds));
    reply
}

/// How the plugin under test answers.
enum Plan {
    First(Vec<&'static str>, Backoff, Until),
    Race(Vec<&'static str>, Duration, Until),
}

#[async_trait]
impl Plugin for Plan {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        match self {
            Plan::First(to, backoff, until) => {
                let calls: Vec<_> = to.iter().map(|p| cx.call(*p)).collect();
                first_success(calls, backoff, *until).await.map(Some)
            }
            Plan::Race(to, stagger, until) => {
                let calls: Vec<_> = to.iter().map(|p| cx.call(*p)).collect();
                race(calls, *stagger, *until).await.map(Some)
            }
        }
    }
}

/// Each call's provider, attempt and end code, as its end hooks saw them.
#[derive(Clone, Default)]
struct Record(Arc<Mutex<Vec<Entry>>>);

type Entry = (String, u32, Role, String);

impl Record {
    fn all(&self) -> Vec<Entry> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

#[async_trait]
impl Plugin for Record {
    async fn on_provider_end(&self, px: &ProviderContext, end: &Result<(), Fault>) {
        let Some(a) = px.extensions().get::<Attempt>() else {
            return;
        };
        let end = end.as_ref().err().map_or("ok", |f| f.code()).to_owned();
        let entry = (px.provider().to_owned(), a.index(), a.role(), end);
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(entry);
    }
}

/// Answers only after `.1`.
struct Slow<T>(T, Duration);

#[async_trait]
impl<T: Transport> Transport for Slow<T> {
    type Ask = T::Ask;
    type Answer = T::Answer;

    async fn send(&self, call: Exchange<T::Ask>) -> Result<Exchange<T::Answer>, Fault> {
        tokio::time::sleep(self.1).await;
        self.0.send(call).await
    }
}

/// A 200 stream that sends one empty chunk, then nothing.
struct Stall;

#[async_trait]
impl Transport for Stall {
    type Ask = Ask;
    type Answer = Answer;

    async fn send(&self, _call: Exchange<Ask>) -> Result<Exchange<Answer>, Fault> {
        let first = stream::iter([Ok(Bytes::from(ROLE))]);
        let body = first.chain(stream::pending()).boxed();
        let mut head = respan_gateway_http::fresh_answer();
        let sse = HeaderValue::from_static("text/event-stream");
        head.headers.insert(http::header::CONTENT_TYPE, sse);
        Ok(Exchange::new(head, body))
    }
}

/// A 200 stream whose content comes at once and whose end comes 5 s later.
struct Dribble;

#[async_trait]
impl Transport for Dribble {
    type Ask = Ask;
    type Answer = Answer;

    async fn send(&self, _call: Exchange<Ask>) -> Result<Exchange<Answer>, Fault> {
        let frame = |s: &'static str| Ok(Bytes::from(s));
        let first = stream::iter([frame(ROLE), frame(HELLO)]);
        let rest = stream::once(async move {
            tokio::time::sleep(secs(5)).await;
            stream::iter([frame(STOP), frame(DONE)])
        });
        let body = stream::StreamExt::chain(first, stream::StreamExt::flatten(rest));
        let mut head = respan_gateway_http::fresh_answer();
        let sse = HeaderValue::from_static("text/event-stream");
        head.headers.insert(http::header::CONTENT_TYPE, sse);
        Ok(Exchange::new(head, body.boxed()))
    }
}

/// Never sends a head.
struct Silent;

#[async_trait]
impl Transport for Silent {
    type Ask = Ask;
    type Answer = Answer;

    async fn send(&self, _call: Exchange<Ask>) -> Result<Exchange<Answer>, Fault> {
        std::future::pending().await
    }
}

/// Refuses the connection.
struct Refused;

#[async_trait]
impl Transport for Refused {
    type Ask = Ask;
    type Answer = Answer;

    async fn send(&self, _call: Exchange<Ask>) -> Result<Exchange<Answer>, Fault> {
        Err(Fault::upstream("connection refused"))
    }
}

struct Harness {
    gw: Gateway,
    mock: MockTransport,
    record: Record,
}

fn harness(plan: Plan) -> Harness {
    let (mock, record) = (MockTransport::new(), Record::default());
    let gw = Gateway::new()
        .serve(OpenAiChat)
        .and_then(|g| g.provider("a", OpenAiChat, mock.at(A)))
        .and_then(|g| g.provider("b", OpenAiChat, mock.at(B)))
        .and_then(|g| g.provider("slow", OpenAiChat, Slow(mock.at(A), secs(5))))
        .and_then(|g| g.provider("stalled", OpenAiChat, watchdog(Stall)))
        .and_then(|g| g.provider("silent", OpenAiChat, watchdog(Silent)))
        .and_then(|g| g.provider("refused", OpenAiChat, Refused))
        .and_then(|g| g.provider("dribble", OpenAiChat, Dribble))
        .and_then(|g| g.provider("b-in-1s", OpenAiChat, Slow(mock.at(B), secs(1))))
        .and_then(|g| g.provider("late", OpenAiChat, watchdog(Slow(mock.at(A), secs(3)))))
        .and_then(|g| g.plugin("record", record.clone()))
        .and_then(|g| g.plugin("plan", plan))
        .unwrap_or_else(|f| panic!("{f}"));
    Harness { gw, mock, record }
}

impl Harness {
    async fn send(&self) -> (StatusCode, http::HeaderMap, String, Result<(), Fault>) {
        self.post(REQUEST).await
    }

    async fn post(
        &self,
        request: &'static str,
    ) -> (StatusCode, http::HeaderMap, String, Result<(), Fault>) {
        let (resp, outcome) = serve(&self.gw, post("/v1/chat/completions", request)).await;
        let (status, headers, body) = read_whole(resp).await;
        let body = body.map_or_else(
            |f| format!("aborted: {f}"),
            |b| String::from_utf8_lossy(&b).into(),
        );
        let outcome = outcome.await;
        self.gw.idle().await;
        (status, headers, body, outcome)
    }

    fn sent_to(&self) -> Vec<String> {
        let sent = self.mock.sent().into_iter();
        sent.map(|r| r.uri().host().unwrap_or_default().to_owned())
            .collect()
    }
}

fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

/// Waits 5 s for a head, 1 s between body frames.
fn watchdog<T: Transport>(inner: T) -> Watchdog<T> {
    Watchdog::new(inner, secs(5), secs(1))
}

fn retries(tries: u32) -> Backoff {
    Backoff::new(tries, Duration::from_millis(100), secs(10))
}

#[tokio::test(start_paused = true)]
async fn a_stream_failing_after_its_first_content_is_never_retried() {
    let h = harness(Plan::First(vec!["a", "b"], retries(3), Until::Content));
    h.mock.reply(A, cut(&[ROLE, HELLO]));
    h.mock.reply(B, ok());

    let (status, _, body, outcome) = h.send().await;

    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("Hello") && body.contains("error"), "{body}");
    assert_eq!(h.sent_to(), ["a.test"]);
    assert!(outcome.is_err());
    let record = h.record.all();
    assert_eq!(record.len(), 1);
    assert_eq!(record[0].2, Role::Winner);
}

#[tokio::test(start_paused = true)]
async fn a_success_failing_before_content_moves_on() {
    let h = harness(Plan::First(vec!["a", "b"], Backoff::once(), Until::Content));
    h.mock.reply(A, cut(&[ROLE]));
    h.mock.reply(B, ok());

    let (status, _, body, outcome) = h.send().await;

    assert_eq!((status, outcome), (StatusCode::OK, Ok(())));
    assert!(body.contains("Hello") && !body.contains("error"), "{body}");
    assert_eq!(h.sent_to(), ["a.test", "b.test"]);
    let roles: Vec<_> = h.record.all().into_iter().map(|r| (r.1, r.2)).collect();
    assert_eq!(roles, [(0, Role::Loser), (1, Role::Winner)]);
}

#[tokio::test(start_paused = true)]
async fn retry_after_is_honoured() {
    let h = harness(Plan::First(vec!["a"], retries(2), Until::Content));
    h.mock.reply(A, too_many("3"));
    h.mock.reply(A, ok());

    let start = Instant::now();
    let (status, ..) = h.send().await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(h.sent_to(), ["a.test", "a.test"]);
    assert!(start.elapsed() >= secs(3), "{:?}", start.elapsed());
}

#[tokio::test(start_paused = true)]
async fn a_retry_after_beyond_the_max_moves_on_at_once() {
    let h = harness(Plan::First(vec!["a", "b"], retries(3), Until::Content));
    h.mock.reply(A, too_many("60"));
    h.mock.reply(B, ok());

    let start = Instant::now();
    let (status, ..) = h.send().await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(h.sent_to(), ["a.test", "b.test"]);
    assert!(start.elapsed() < secs(1));
}

#[tokio::test(start_paused = true)]
async fn with_no_success_the_client_gets_the_last_429_and_its_retry_after() {
    let h = harness(Plan::First(vec!["a", "b"], Backoff::once(), Until::Content));
    h.mock
        .reply(A, mock::json(StatusCode::SERVICE_UNAVAILABLE, "{}"));
    h.mock.reply(B, too_many("7"));

    let (status, headers, ..) = h.send().await;

    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        headers.get(RETRY_AFTER).map(|v| v.as_bytes()),
        Some(&b"7"[..])
    );
    // The 429 is a fault, which ended its call as it was read, so
    // no attempt won.
    let roles: Vec<_> = h.record.all().into_iter().map(|r| r.2).collect();
    assert_eq!(roles, [Role::Loser, Role::Loser]);
}

#[tokio::test(start_paused = true)]
async fn a_config_error_on_one_target_moves_on_to_the_next() {
    let h = harness(Plan::First(
        vec!["missing", "b"],
        retries(3),
        Until::Content,
    ));
    h.mock.reply(B, ok());

    let (status, _, body, _) = h.send().await;

    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("Hello"), "{body}");
    assert_eq!(h.sent_to(), ["b.test"]);
}

#[tokio::test(start_paused = true)]
async fn a_race_drops_its_losers_which_end_abandoned() {
    let h = harness(Plan::Race(
        vec!["slow", "b"],
        Duration::ZERO,
        Until::Content,
    ));
    h.mock.reply(A, ok());
    h.mock.reply(B, ok());

    let (status, _, body, _) = h.send().await;

    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("Hello"), "{body}");
    let mut record = h.record.all();
    record.sort_by_key(|r| r.1);
    let b = ("b".to_owned(), 1, Role::Winner, "ok".to_owned());
    let slow = (
        "slow".to_owned(),
        0,
        Role::Loser,
        codes::ABANDONED.to_owned(),
    );
    assert_eq!(record, [slow, b]);
}

#[tokio::test(start_paused = true)]
async fn a_race_starts_the_next_call_only_after_its_stagger() {
    let h = harness(Plan::Race(vec!["a", "b"], secs(2), Until::Content));
    h.mock.reply(A, ok());

    let (status, ..) = h.send().await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(h.sent_to(), ["a.test"]);
}

#[tokio::test(start_paused = true)]
async fn a_race_starts_the_next_call_at_once_when_the_others_failed() {
    let h = harness(Plan::Race(vec!["a", "b"], secs(10), Until::Content));
    h.mock
        .reply(A, mock::json(StatusCode::INTERNAL_SERVER_ERROR, "{}"));
    h.mock.reply(B, ok());

    let start = Instant::now();
    let (status, ..) = h.send().await;

    assert_eq!(status, StatusCode::OK);
    assert!(start.elapsed() < secs(10));
    assert_eq!(h.sent_to(), ["a.test", "b.test"]);
}

#[tokio::test(start_paused = true)]
async fn the_watchdog_fails_a_stalled_body_and_first_success_moves_on() {
    let h = harness(Plan::First(
        vec!["stalled", "b"],
        Backoff::once(),
        Until::Content,
    ));
    h.mock.reply(B, ok());

    let (status, _, body, _) = h.send().await;

    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("Hello"), "{body}");
    let record = h.record.all();
    assert_eq!(record[0].0, "stalled");
    assert_eq!(record[0].3, codes::UPSTREAM);
}

#[tokio::test(start_paused = true)]
async fn the_watchdog_bounds_the_wait_for_the_head_too() {
    let h = harness(Plan::First(
        vec!["silent", "b"],
        Backoff::once(),
        Until::Content,
    ));
    h.mock.reply(B, ok());

    let start = Instant::now();
    let answer = tokio::time::timeout(secs(60), h.send()).await;

    let (status, _, body, _) = answer.expect("still waiting for the head after 60 s");
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("Hello"), "{body}");
    let elapsed = start.elapsed();
    assert!(elapsed >= secs(5) && elapsed < secs(6), "{elapsed:?}");
    let record = h.record.all();
    assert_eq!(
        (record[0].0.as_str(), record[0].3.as_str()),
        ("silent", codes::UPSTREAM)
    );
}

/// A whole answer sends its head only when it is done, so a whole answer
/// slower than `idle` but within `head` is neither cut nor sent again.
#[tokio::test(start_paused = true)]
async fn the_watchdog_does_not_cut_a_slow_whole_answer() {
    const WHOLE: &str = r#"{"model":"m","messages":[{"role":"user","content":"write an essay"}]}"#;
    const REPLY: &str = r#"{"id":"c","model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"an essay"},"finish_reason":"stop"}]}"#;
    let h = harness(Plan::First(vec!["late", "b"], retries(2), Until::Content));
    h.mock.reply(A, mock::json(StatusCode::OK, REPLY));
    h.mock.reply(B, ok());

    let (status, _, body, outcome) = h.post(WHOLE).await;

    assert_eq!((status, outcome), (StatusCode::OK, Ok(())));
    assert!(body.contains("an essay"), "{body}");
    assert_eq!(h.sent_to(), ["a.test"]);
    let record = h.record.all();
    let late = ("late".to_owned(), 0, Role::Winner, "ok".to_owned());
    assert_eq!(record, [late]);
}

/// A whole body ends as it is read, after the helper set its role, so
/// its end hook never sees it pending, on any runtime.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_whole_body_attempt_ends_with_its_role_set() {
    const WHOLE: &str = r#"{"model":"m","messages":[{"role":"user","content":"hi"}]}"#;
    const REPLY: &str = r#"{"id":"c","model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}]}"#;
    for _ in 0..100 {
        let h = harness(Plan::First(vec!["a", "b"], Backoff::once(), Until::Content));
        h.mock.reply(A, too_many("1"));
        h.mock.reply(B, mock::json(StatusCode::OK, REPLY));

        let (status, ..) = h.post(WHOLE).await;

        assert_eq!(status, StatusCode::OK);
        let mut roles: Vec<_> = h.record.all().into_iter().map(|r| (r.1, r.2)).collect();
        roles.sort_by_key(|(index, _)| *index);
        assert_eq!(roles, [(0, Role::Loser), (1, Role::Winner)]);
    }
}

/// A call failing before its head ends as it fails, before the helper sees
/// the fault; it is a loser from the start, so its end hook never races.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_call_failing_before_its_head_ends_a_loser() {
    for _ in 0..100 {
        let h = harness(Plan::First(
            vec!["refused", "b"],
            Backoff::once(),
            Until::Content,
        ));
        h.mock.reply(B, ok());

        let (status, ..) = h.send().await;

        assert_eq!(status, StatusCode::OK);
        let mut record = h.record.all();
        record.sort_by_key(|r| r.1);
        let refused = (
            "refused".to_owned(),
            0,
            Role::Loser,
            codes::UPSTREAM.to_owned(),
        );
        let b = ("b".to_owned(), 1, Role::Winner, "ok".to_owned());
        assert_eq!(record, [refused, b]);
    }
}

#[tokio::test(start_paused = true)]
async fn a_race_that_never_hedges_still_falls_over() {
    let h = harness(Plan::Race(vec!["a", "b"], Duration::MAX, Until::Content));
    h.mock
        .reply(A, mock::json(StatusCode::INTERNAL_SERVER_ERROR, "{}"));
    h.mock.reply(B, ok());
    let (status, _, body, _) = h.send().await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[test]
fn the_stopping_point_is_the_first_content_by_default() {
    assert_eq!(Until::default(), Until::Content);
}

/// `a` answers 200 and fails before any content; `b` answers.
fn a_fails_before_content(h: &Harness) {
    h.mock.reply(A, cut(&[ROLE]));
    h.mock.reply(B, ok());
}

#[tokio::test(start_paused = true)]
async fn until_head_commits_on_a_success_status_however_its_body_goes() {
    let plans = [
        Plan::First(vec!["a", "b"], Backoff::once(), Until::Head),
        Plan::Race(vec!["a", "b"], secs(1), Until::Head),
    ];
    for plan in plans {
        let h = harness(plan);
        a_fails_before_content(&h);

        let (status, _, body, outcome) = h.send().await;

        // Committed on its head, so its failure before any bytes is refused.
        assert_eq!(status, StatusCode::BAD_GATEWAY, "{body}");
        assert_eq!(h.sent_to(), ["a.test"]);
        assert!(matches!(outcome, Err(f) if f.code() == codes::UPSTREAM));
        let a = ("a".to_owned(), 0, Role::Winner, codes::UPSTREAM.to_owned());
        assert_eq!(h.record.all(), [a]);
    }
}

#[tokio::test(start_paused = true)]
async fn until_content_moves_on_from_a_success_failing_before_content() {
    let plans = [
        Plan::First(vec!["a", "b"], Backoff::once(), Until::Content),
        Plan::Race(vec!["a", "b"], secs(1), Until::Content),
    ];
    for plan in plans {
        let h = harness(plan);
        a_fails_before_content(&h);

        let (status, _, body, outcome) = h.send().await;

        assert_eq!((status, outcome), (StatusCode::OK, Ok(())));
        assert_eq!(body, [ROLE, HELLO, STOP, DONE].concat());
        assert_eq!(h.sent_to(), ["a.test", "b.test"]);
    }
}

#[tokio::test(start_paused = true)]
async fn until_end_sends_the_whole_answer_as_it_came_and_moves_on_before_content() {
    let plans = [
        Plan::First(vec!["a", "b"], Backoff::once(), Until::End),
        Plan::Race(vec!["a", "b"], secs(1), Until::End),
    ];
    for plan in plans {
        let h = harness(plan);
        a_fails_before_content(&h);

        let (status, _, body, outcome) = h.send().await;

        assert_eq!((status, outcome), (StatusCode::OK, Ok(())));
        assert_eq!(body, [ROLE, HELLO, STOP, DONE].concat());
        assert_eq!(h.sent_to(), ["a.test", "b.test"]);
        let mut roles: Vec<_> = h
            .record
            .all()
            .into_iter()
            .map(|r| (r.1, r.2, r.3))
            .collect();
        roles.sort_by_key(|r| r.0);
        let ok = "ok".to_owned();
        assert_eq!(
            roles,
            [
                (0, Role::Loser, codes::UPSTREAM.to_owned()),
                (1, Role::Winner, ok)
            ]
        );
    }
}

/// `dribble` sends content at once and ends in 5 s; `b-in-1s` answers in
/// 1 s and ends then: the first to content and the first to finish differ.
#[tokio::test(start_paused = true)]
async fn a_race_until_end_picks_the_call_that_finishes_first() {
    for (until, winner, loser) in [
        (Until::Content, "dribble", "b-in-1s"),
        (Until::End, "b-in-1s", "dribble"),
    ] {
        let h = harness(Plan::Race(
            vec!["dribble", "b-in-1s"],
            Duration::ZERO,
            until,
        ));
        h.mock.reply(B, ok());

        let start = Instant::now();
        let (status, _, body, outcome) = h.send().await;

        assert_eq!((status, outcome), (StatusCode::OK, Ok(())), "{until:?}");
        assert_eq!(body, [ROLE, HELLO, STOP, DONE].concat(), "{until:?}");
        let mut record = h.record.all();
        record.sort_by_key(|r| r.2 == Role::Loser);
        let got: Vec<_> = record
            .iter()
            .map(|r| (r.0.as_str(), r.2, r.3.as_str()))
            .collect();
        assert_eq!(got[0], (winner, Role::Winner, "ok"), "{until:?}");
        // A loser dropped before it was polled never started.
        let lost = (loser, Role::Loser, codes::ABANDONED);
        assert!(got[1..].iter().all(|r| *r == lost), "{until:?}: {got:?}");
        if until == Until::End {
            assert_eq!(got.len(), 2);
        }
        // Reading to its end, the winner finished in 1 s, not 5.
        if until == Until::End {
            assert!(start.elapsed() < secs(2), "{:?}", start.elapsed());
        }
    }
}

/// Read to its end, `a` fails after its content: not a success, so `b`'s
/// answer goes out, and `a`'s ends as it ended.
#[tokio::test(start_paused = true)]
async fn until_end_a_stream_failing_after_content_is_not_a_winner() {
    let plans = [
        Plan::First(vec!["a", "b"], Backoff::once(), Until::End),
        Plan::Race(vec!["a", "b"], secs(1), Until::End),
    ];
    for plan in plans {
        let h = harness(plan);
        h.mock.reply(A, cut(&[ROLE, HELLO]));
        h.mock.reply(B, ok());

        let (status, _, body, outcome) = h.send().await;

        assert_eq!((status, outcome), (StatusCode::OK, Ok(())));
        assert_eq!(body, [ROLE, HELLO, STOP, DONE].concat());
        assert_eq!(h.sent_to(), ["a.test", "b.test"]);
        let mut roles: Vec<_> = h
            .record
            .all()
            .into_iter()
            .map(|r| (r.0, r.2, r.3))
            .collect();
        roles.sort_by(|x, y| x.0.cmp(&y.0));
        let (a, b) = ("a".to_owned(), "b".to_owned());
        let expected = [
            (a, Role::Loser, codes::UPSTREAM.to_owned()),
            (b, Role::Winner, "ok".to_owned()),
        ];
        assert_eq!(roles, expected);
    }
}

/// With retries, a stream failing after its content is retried when read
/// to its end; with none that succeed, the last answer goes out as it came.
#[tokio::test(start_paused = true)]
async fn until_end_retries_a_stream_failing_after_content_and_returns_the_last() {
    let h = harness(Plan::First(vec!["a"], retries(2), Until::End));
    h.mock.reply(A, cut(&[ROLE, HELLO]));
    h.mock.reply(A, cut(&[ROLE, HELLO]));

    let (status, _, body, outcome) = h.send().await;

    assert_eq!(status, StatusCode::OK);
    assert!(body.starts_with(&[ROLE, HELLO].concat()), "{body}");
    assert!(body.contains("\"code\":\"upstream\""), "{body}");
    assert!(matches!(outcome, Err(f) if f.code() == codes::UPSTREAM));
    assert_eq!(h.sent_to(), ["a.test", "a.test"]);
    let roles: Vec<_> = h.record.all().into_iter().map(|r| (r.1, r.2)).collect();
    assert_eq!(roles, [(0, Role::Loser), (1, Role::Winner)]);
}
