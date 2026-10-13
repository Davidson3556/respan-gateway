//! The attempt helpers' promises (retry, fallback, races), against a mock provider.

use respan_gateway_conversation::{Block, Finish, Input, Output};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use mockllm::{Answer, FakeOpenAi};
use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_core::futures::channel::oneshot;
use respan_gateway_core::futures::future::BoxFuture;
use respan_gateway_core::futures::{FutureExt, StreamExt, sink, stream};
use respan_gateway_core::{
    BadRequest, ClientContext, Connection, ErrorCode, EventSink, EventStream, Fault, Format,
    Gateway, Internal, Plugin, Provider, ProviderContext, Response, Upstream, async_trait,
};
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::http::{
    self, HeaderMap, HeaderValue, StatusCode,
    header::{AUTHORIZATION, RETRY_AFTER},
};
use respan_gateway_http::{Body, Completion};
use respan_gateway_http_faults::{error_code, retryable};
use respan_gateway_http_transport::HttpTransport;
use respan_gateway_plugin_fallback::{
    Attempt, Backoff, Fallback, Hedge, Role, Until, first_success, race,
};
/// An end by its message, for comparing: `Fault` has no equality.
fn ended(end: Result<(), Fault>) -> Result<(), String> {
    end.map_err(|f| f.message().to_owned())
}

#[allow(dead_code)] // The shared helper also exposes streamed requests, unused here.
#[path = "../../../../tests/support/http.rs"]
mod http_support;
use http_support::{LIMIT, post, read_whole, serve};
use tokio::time::Instant;

const A: &str = "http://a.test/v1";
const B: &str = "http://b.test/v1";
const REQUEST: &str = r#"{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
const ROLE: &str = "data: {\"id\":\"c\",\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"}}]}\n\n";
const HELLO: &str = "data: {\"id\":\"c\",\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello\"}}]}\n\n";
const STOP: &str = "data: {\"id\":\"c\",\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n";
const DONE: &str = "data: [DONE]\n\n";

fn sse(chunks: &[&str]) -> Answer {
    Answer::events(chunks.iter().map(|frame| frame.trim_end().into()).collect())
}

/// What a client reads from a chat stream, ignoring how the gateway frames it:
/// each data frame's id, model, delta and finish reason (a null one is none).
/// The gateway re-encodes responses, so they are compared by meaning.
fn meaning(body: &str) -> Vec<serde_json::Value> {
    use serde_json::{Value, json};
    body.split("\n\n")
        .filter_map(|frame| frame.strip_prefix("data: "))
        .map(|data| match serde_json::from_str::<Value>(data) {
            Err(_) => json!(data),
            Ok(chunk) => {
                let choice = &chunk["choices"][0];
                let finish = Some(&choice["finish_reason"]).filter(|f| !f.is_null());
                json!([chunk["id"], chunk["model"], choice["delta"], finish])
            }
        })
        .collect()
}

fn ok() -> Answer {
    sse(&[ROLE, HELLO, STOP, DONE])
}

/// A stream cut off by the provider after `chunks`.
fn cut(chunks: &[&str]) -> Answer {
    sse(chunks).cut_after(chunks.len())
}

fn json(status: StatusCode, body: &'static str) -> Answer {
    if status.is_success() {
        Answer::raw(body)
    } else {
        Answer::error(status.as_u16())
    }
}

fn too_many(seconds: &'static str) -> Answer {
    json(
        StatusCode::TOO_MANY_REQUESTS,
        r#"{"error":{"message":"slow down"}}"#,
    )
    .header("retry-after", seconds)
}

/// Retry policy selected by the plugin under test.
type Classifier = Arc<dyn Fn(&Fault, bool) -> bool + Send + Sync>;

/// How the plugin under test answers.
enum Plan {
    First(Vec<&'static str>, Backoff, Until),
    Classified(Vec<&'static str>, Backoff, Until, Classifier),
    Race(Vec<&'static str>, Duration, Until),
}

#[async_trait]
impl Plugin for Plan {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        match self {
            Plan::First(to, backoff, until) => {
                let calls: Vec<_> = to.iter().map(|p| cx.call(*p)).collect();
                first_success(calls, backoff, until.clone(), retryable)
                    .await
                    .map(Some)
            }
            Plan::Classified(to, backoff, until, classify) => {
                let calls: Vec<_> = to.iter().map(|p| cx.call(*p)).collect();
                first_success(calls, backoff, until.clone(), |fault, opened| {
                    classify(fault, opened)
                })
                .await
                .map(Some)
            }
            Plan::Race(to, stagger, until) => {
                let calls: Vec<_> = to.iter().map(|p| cx.call(*p)).collect();
                race(calls, *stagger, until.clone()).await.map(Some)
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
        let end = end
            .as_ref()
            .err()
            .map_or("ok", |f| error_code(f))
            .to_owned();
        let entry = (px.provider().to_owned(), a.index(), a.role(), end);
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(entry);
    }
}

/// Count open provider attempts while the next attempt starts. In particular,
/// a retry must release its predecessor before backoff, while a fallback
/// retains its best opened candidate until replacement or selection.
struct Active {
    count: Arc<AtomicUsize>,
    expected_before_next: usize,
}

#[async_trait]
impl Plugin for Active {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        let previous = self.count.fetch_add(1, Ordering::SeqCst);
        if px
            .extensions()
            .get::<Attempt>()
            .is_some_and(|a| a.index() > 0)
        {
            assert_eq!(previous, self.expected_before_next);
        }
        Ok(())
    }

    async fn on_provider_end(&self, _: &ProviderContext, _: &Result<(), Fault>) {
        self.count.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Native HTTP result, including independent outgoing delivery completion.
type Answered = Result<(http::Response<Body>, Option<Completion>), Fault>;

/// Answers only after `delay`, without hiding the native HTTP callback.
fn slow<F, Fut>(
    send: F,
    delay: Duration,
) -> impl Fn(http::Request<Body>) -> BoxFuture<'static, Answered> + Clone + Send + Sync
where
    F: Fn(http::Request<Body>) -> Fut + Clone + Send + Sync + 'static,
    Fut: std::future::Future<Output = Answered> + Send + 'static,
{
    move |call| {
        let send = send.clone();
        async move {
            tokio::time::sleep(delay).await;
            send(call).await
        }
        .boxed()
    }
}

/// A 200 stream that sends one empty chunk, then nothing.
async fn stall(_: http::Request<Body>) -> Answered {
    let first = stream::iter([Ok(Bytes::from(ROLE))]);
    let body = first.chain(stream::pending()).boxed();
    let mut head = respan_gateway_http::http::Response::new(()).into_parts().0;
    head.headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    Ok((
        http::Response::from_parts(head, respan_gateway_http::Body::stream(body)),
        None,
    ))
}

/// A 200 stream whose content comes at once and whose end comes 5 s later.
async fn dribble(_: http::Request<Body>) -> Answered {
    let frame = |s: &'static str| Ok(Bytes::from(s));
    let first = stream::iter([frame(ROLE), frame(HELLO)]);
    let rest = stream::once(async move {
        tokio::time::sleep(secs(5)).await;
        stream::iter([frame(STOP), frame(DONE)])
    });
    let body = first.chain(rest.flatten()).boxed();
    let mut head = respan_gateway_http::http::Response::new(()).into_parts().0;
    head.headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    Ok((
        http::Response::from_parts(head, respan_gateway_http::Body::stream(body)),
        None,
    ))
}

/// Never sends a head.
async fn silent(_: http::Request<Body>) -> Answered {
    std::future::pending().await
}

/// Refuses the connection.
async fn refused(_: http::Request<Body>) -> Answered {
    Err(Upstream::fault("connection refused"))
}

struct Fakes {
    a: FakeOpenAi,
    b: FakeOpenAi,
    sent: Arc<Mutex<Vec<String>>>,
}

impl Fakes {
    async fn start() -> Self {
        Self {
            a: FakeOpenAi::start().await,
            b: FakeOpenAi::start().await,
            sent: Arc::default(),
        }
    }

    fn reply(&self, provider: &str, answer: Answer) -> &Self {
        match provider {
            A => self.a.answer(answer),
            B => self.b.answer(answer),
            _ => panic!("unknown provider fixture {provider}"),
        }
        self
    }

    fn at(
        &self,
        provider: &str,
    ) -> impl Fn(http::Request<Body>) -> BoxFuture<'static, Answered> + Clone + Send + Sync + 'static
    {
        let (fake, label) = match provider {
            A => (&self.a, "a.test"),
            B => (&self.b, "b.test"),
            _ => panic!("unknown provider fixture {provider}"),
        };
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer sk-test"));
        let http = HttpTransport::new(fake.base_url())
            .and_then(|transport| transport.with_headers(headers))
            .unwrap_or_else(|fault| panic!("{fault}"));
        let sent = self.sent.clone();
        move |call| {
            let http = http.clone();
            let sent = sent.clone();
            async move {
                sent.lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(label.to_owned());
                http.send(call).await
            }
            .boxed()
        }
    }

    fn sent(&self) -> Vec<String> {
        self.sent
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

struct Harness {
    gw: Gateway,
    mock: Fakes,
    record: Record,
}

async fn harness(plan: impl Plugin) -> Harness {
    let (mock, record) = (Fakes::start().await, Record::default());
    let gw = Gateway::new()
        .client::<Input, _, _>(OpenAiChat)
        .and_then(|g| {
            g.provider(Provider::new(
                "a",
                OpenAiChat,
                respan_gateway_http::connect(mock.at(A), LIMIT),
            ))
        })
        .and_then(|g| {
            g.provider(Provider::new(
                "b",
                OpenAiChat,
                respan_gateway_http::connect(mock.at(B), LIMIT),
            ))
        })
        .and_then(|g| {
            g.provider(Provider::new(
                "slow",
                OpenAiChat,
                respan_gateway_http::connect(slow(mock.at(A), secs(5)), LIMIT),
            ))
        })
        .and_then(|g| {
            g.provider(Provider::new(
                "stalled",
                OpenAiChat,
                respan_gateway_http::connect(watchdog(stall), LIMIT),
            ))
        })
        .and_then(|g| {
            g.provider(Provider::new(
                "silent",
                OpenAiChat,
                respan_gateway_http::connect(watchdog(silent), LIMIT),
            ))
        })
        .and_then(|g| {
            g.provider(Provider::new(
                "refused",
                OpenAiChat,
                respan_gateway_http::connect(refused, LIMIT),
            ))
        })
        .and_then(|g| {
            g.provider(Provider::new(
                "dribble",
                OpenAiChat,
                respan_gateway_http::connect(dribble, LIMIT),
            ))
        })
        .and_then(|g| {
            g.provider(Provider::new(
                "b-in-1s",
                OpenAiChat,
                respan_gateway_http::connect(slow(mock.at(B), secs(1)), LIMIT),
            ))
        })
        .and_then(|g| {
            g.provider(Provider::new(
                "late",
                OpenAiChat,
                respan_gateway_http::connect(watchdog(slow(mock.at(A), secs(3))), LIMIT),
            ))
        })
        .map(|g| g.plugin(record.clone()))
        .map(|g| g.plugin(plan))
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
        self.mock.sent()
    }
}

fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

/// Waits 5 s for a head, 1 s between body frames.
fn watchdog<F, Fut>(
    send: F,
) -> impl Fn(http::Request<Body>) -> BoxFuture<'static, Answered> + Send + Sync
where
    F: Fn(http::Request<Body>) -> Fut + Clone + Send + Sync + 'static,
    Fut: std::future::Future<Output = Answered> + Send + 'static,
{
    let send = respan_gateway_http::call::watchdog(send, secs(5), secs(1));
    move |request| send(request).boxed()
}

fn retries(tries: u32) -> Backoff {
    Backoff::new(tries, Duration::from_millis(100), secs(10))
}

#[tokio::test]
async fn a_zero_attempt_budget_never_dispatches_or_classifies() {
    let classified = Arc::new(AtomicUsize::new(0));
    let called = classified.clone();
    let h = harness(Plan::Classified(
        vec!["a", "b"],
        Backoff::new(0, Duration::ZERO, Duration::ZERO),
        Until::Head,
        Arc::new(move |_, _| {
            called.fetch_add(1, Ordering::SeqCst);
            true
        }),
    ))
    .await;
    let (status, _, _, outcome) = h.send().await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(outcome.is_err());
    assert!(h.sent_to().is_empty());
    assert!(h.record.all().is_empty());
    assert_eq!(classified.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn retry_releases_failed_attempt_before_the_next_one_starts() {
    let active = Arc::new(AtomicUsize::new(0));
    let mut h = harness(Plan::First(vec!["a"], retries(2), Until::End)).await;
    h.gw = h.gw.plugin(Active {
        count: active.clone(),
        expected_before_next: 0,
    });
    h.mock.reply(A, too_many("0")).reply(A, ok());
    let (status, _, _, outcome) = h.send().await;
    assert_eq!((status, ended(outcome)), (StatusCode::OK, Ok(())));
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(h.sent_to(), ["a.test", "a.test"]);
}

#[tokio::test]
async fn a_retry_releases_its_opened_response_during_backoff() {
    let h = harness(Plan::First(
        vec!["a"],
        Backoff::new(2, secs(2), secs(2)),
        Until::End,
    ))
    .await;
    h.mock.reply(A, Answer::error(503)).reply(A, ok());
    let mut sending = Box::pin(h.send());
    // The first attempt must settle while the helper waits two seconds, not
    // only when the next attempt starts or the request finishes.
    let settled = tokio::time::timeout(secs(1), async {
        loop {
            if h.record.all().iter().any(|entry| entry.1 == 0) {
                return true;
            }
            tokio::select! {
                _ = &mut sending => return false,
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        }
    })
    .await;
    drop(sending);
    h.gw.idle().await;
    assert!(
        matches!(settled, Ok(true)),
        "first call stayed open through backoff"
    );
    assert_eq!(h.sent_to(), ["a.test"]);
}

#[tokio::test]
async fn fallback_retains_its_opened_candidate_until_replacement() {
    let active = Arc::new(AtomicUsize::new(0));
    let mut h = harness(Plan::First(
        vec!["a", "refused"],
        Backoff::once(),
        Until::End,
    ))
    .await;
    h.gw = h.gw.plugin(Active {
        count: active.clone(),
        expected_before_next: 1,
    });
    h.mock.reply(A, too_many("7"));
    let (status, headers, _, outcome) = h.send().await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        headers.get(RETRY_AFTER).map(|h| h.as_bytes()),
        Some(&b"7"[..])
    );
    assert!(outcome.is_err());
    assert_eq!(active.load(Ordering::SeqCst), 0);
    let record = h.record.all();
    assert!(
        record
            .iter()
            .any(|entry| entry.0 == "a" && entry.2 == Role::Winner)
    );
}

#[tokio::test]
async fn a_stream_failing_after_its_first_content_is_never_retried() {
    let h = harness(Plan::First(
        vec!["a", "b"],
        retries(3),
        Until::content(Output::is_content),
    ))
    .await;
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

#[tokio::test]
async fn a_success_failing_before_content_moves_on() {
    let h = harness(Plan::First(
        vec!["a", "b"],
        Backoff::once(),
        Until::content(Output::is_content),
    ))
    .await;
    h.mock.reply(A, cut(&[ROLE]));
    h.mock.reply(B, ok());

    let (status, _, body, outcome) = h.send().await;

    assert_eq!((status, ended(outcome)), (StatusCode::OK, Ok(())));
    assert!(body.contains("Hello") && !body.contains("error"), "{body}");
    assert_eq!(h.sent_to(), ["a.test", "b.test"]);
    let roles: Vec<_> = h.record.all().into_iter().map(|r| (r.1, r.2)).collect();
    assert_eq!(roles, [(0, Role::Loser), (1, Role::Winner)]);
}

#[tokio::test]
async fn retry_after_is_honoured() {
    let h = harness(Plan::First(
        vec!["a"],
        retries(2),
        Until::content(Output::is_content),
    ))
    .await;
    h.mock.reply(A, too_many("3"));
    h.mock.reply(A, ok());

    let start = Instant::now();
    let (status, ..) = h.send().await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(h.sent_to(), ["a.test", "a.test"]);
    assert!(start.elapsed() >= secs(3), "{:?}", start.elapsed());
}

#[tokio::test]
async fn a_retry_after_beyond_the_max_moves_on_at_once() {
    let h = harness(Plan::First(
        vec!["a", "b"],
        retries(3),
        Until::content(Output::is_content),
    ))
    .await;
    h.mock.reply(A, too_many("60"));
    h.mock.reply(B, ok());

    let start = Instant::now();
    let (status, ..) = h.send().await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(h.sent_to(), ["a.test", "b.test"]);
    assert!(start.elapsed() < secs(1));
}

#[tokio::test]
async fn with_no_success_the_client_gets_the_last_429_and_its_retry_after() {
    let h = harness(Plan::First(
        vec!["a", "b"],
        Backoff::once(),
        Until::content(Output::is_content),
    ))
    .await;
    h.mock.reply(A, json(StatusCode::SERVICE_UNAVAILABLE, "{}"));
    h.mock.reply(B, too_many("7"));

    let (status, headers, ..) = h.send().await;

    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        headers.get(RETRY_AFTER).map(|v| v.as_bytes()),
        Some(&b"7"[..])
    );
    // The final refusal is retained until selection, so its provider end
    // hook sees Winner when the buffered refusal is replayed.
    let roles: Vec<_> = h.record.all().into_iter().map(|r| r.2).collect();
    assert_eq!(roles, [Role::Loser, Role::Winner]);
}

#[tokio::test]
async fn a_fallback_that_cannot_be_made_does_not_hide_the_providers_answer() {
    for plan in [
        Plan::First(
            vec!["a", "missing"],
            Backoff::once(),
            Until::content(Output::is_content),
        ),
        Plan::Race(
            vec!["a", "missing"],
            secs(1),
            Until::content(Output::is_content),
        ),
    ] {
        let h = harness(plan).await;
        h.mock.reply(A, json(StatusCode::SERVICE_UNAVAILABLE, "{}"));

        let (status, ..) = h.send().await;

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }
}

#[tokio::test]
async fn a_config_error_on_one_target_moves_on_to_the_next() {
    let h = harness(Plan::First(
        vec!["missing", "b"],
        retries(3),
        Until::content(Output::is_content),
    ))
    .await;
    h.mock.reply(B, ok());

    let (status, _, body, _) = h.send().await;

    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("Hello"), "{body}");
    assert_eq!(h.sent_to(), ["b.test"]);
}

#[tokio::test]
async fn a_false_classifier_suppresses_preopening_upstream_retries_but_allows_fallback() {
    let observed = Arc::new(Mutex::new(Vec::new()));
    let seen = observed.clone();
    let h = harness(Plan::Classified(
        vec!["refused", "b"],
        retries(3),
        Until::Head,
        Arc::new(move |fault, opened| {
            seen.lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push((error_code(fault).to_owned(), opened));
            false
        }),
    ))
    .await;
    h.mock.reply(B, ok());
    let (status, _, body, end) = h.send().await;
    assert_eq!((status, ended(end)), (StatusCode::OK, Ok(())));
    assert!(body.contains("Hello"));
    assert_eq!(h.sent_to(), ["b.test"]);
    assert_eq!(
        *observed.lock().unwrap_or_else(PoisonError::into_inner),
        [("upstream".to_owned(), false)]
    );
    let attempts = h.record.all();
    assert_eq!(attempts.len(), 2, "no second refused attempt: {attempts:?}");
    assert_eq!(attempts[0].0, "refused");
    assert_eq!(attempts[1].0, "b");
}

#[tokio::test]
async fn classifier_sees_opened_content_and_late_terminal_failures() {
    for (until, first) in [
        (Until::content(Output::is_content), cut(&[ROLE])),
        (Until::End, cut(&[ROLE, HELLO])),
    ] {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let seen = observed.clone();
        let h = harness(Plan::Classified(
            vec!["a", "b"],
            retries(3),
            until,
            Arc::new(move |fault, opened| {
                seen.lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push((error_code(fault).to_owned(), opened));
                false
            }),
        ))
        .await;
        h.mock.reply(A, first).reply(B, ok());
        let (status, _, body, end) = h.send().await;
        assert_eq!((status, ended(end)), (StatusCode::OK, Ok(())));
        assert!(body.contains("Hello"));
        assert_eq!(h.sent_to(), ["a.test", "b.test"]);
        assert_eq!(
            *observed.lock().unwrap_or_else(PoisonError::into_inner),
            [("upstream".to_owned(), true)]
        );
    }
}

#[tokio::test]
async fn a_non_http_provider_can_retry_its_own_preopening_fault_code() {
    struct Local;
    impl Format<Arc<AtomicUsize>, Output, Input> for Local {
        fn name(&self) -> &str {
            "local"
        }

        fn bind(&self, invocations: Arc<AtomicUsize>) -> Result<Connection<Output, Input>, Fault> {
            let (send, receive) = oneshot::channel();
            let incoming = async move { receive.await.map_err(|_| Fault::abandoned()) };
            let outgoing = EventSink::preparing(move |input: &mut EventStream<Input>| {
                let _ = input
                    .value()
                    .transpose()?
                    .ok_or_else(|| BadRequest::fault("missing input"))?;
                if invocations.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err(
                        Fault::new("local model warming up").with(ErrorCode("model_busy".into()))
                    );
                }
                Ok(sink::unfold(
                    Some(send),
                    |send, item: Result<Input, Fault>| async move {
                        item?;
                        if let Some(send) = send {
                            let answer = stream::iter([
                                Ok(Output::start("local-1", "local")),
                                Ok(Output::block_start(0, Block::Text)),
                                Ok(Output::text(0, "Local answer")),
                                Ok(Output::block_end(0)),
                                Ok(Output::finished(Finish::Stop)),
                            ]);
                            let _ = send.send(EventStream::stream(answer));
                        }
                        Ok::<_, Fault>(None)
                    },
                ))
            });
            Ok((incoming.boxed(), outgoing))
        }
    }

    let invocations = Arc::new(AtomicUsize::new(0));
    let observed = Arc::new(Mutex::new(Vec::new()));
    let seen = observed.clone();
    let plan = Plan::Classified(
        vec!["local"],
        retries(2),
        Until::content(Output::is_content),
        Arc::new(move |fault, opened| {
            seen.lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push((error_code(fault).to_owned(), opened));
            fault
                .get::<ErrorCode>()
                .is_some_and(|c| c.0 == "model_busy")
                && !opened
        }),
    );
    let gw = Gateway::new()
        .client::<Input, _, _>(OpenAiChat)
        .expect("client format")
        .provider(Provider::new("local", Local, {
            let invocations = invocations.clone();
            move |_| invocations.clone()
        }))
        .expect("local provider")
        .plugin(plan);
    let (resp, outcome) = serve(&gw, post("/v1/chat/completions", REQUEST)).await;
    let (status, _, body) = read_whole(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert!(String::from_utf8_lossy(&body.expect("response body")).contains("Local answer"));
    assert_eq!(ended(outcome.await), Ok(()));
    assert_eq!(invocations.load(Ordering::SeqCst), 2);
    assert_eq!(
        *observed.lock().unwrap_or_else(PoisonError::into_inner),
        [("model_busy".to_owned(), false)]
    );
}

#[tokio::test]
async fn a_race_drops_its_losers_which_end_abandoned() {
    let h = harness(Plan::Race(
        vec!["slow", "b"],
        Duration::ZERO,
        Until::content(Output::is_content),
    ))
    .await;
    h.mock.reply(A, ok());
    h.mock.reply(B, ok());

    let (status, _, body, _) = h.send().await;

    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("Hello"), "{body}");
    let mut record = h.record.all();
    record.sort_by_key(|r| r.1);
    let b = ("b".to_owned(), 1, Role::Winner, "ok".to_owned());
    let slow = ("slow".to_owned(), 0, Role::Loser, "abandoned".to_owned());
    assert_eq!(record, [slow, b]);
}

#[tokio::test]
async fn a_race_starts_the_next_call_only_after_its_stagger() {
    let h = harness(Plan::Race(
        vec!["a", "b"],
        secs(2),
        Until::content(Output::is_content),
    ))
    .await;
    h.mock.reply(A, ok());

    let (status, ..) = h.send().await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(h.sent_to(), ["a.test"]);
}

#[tokio::test]
async fn a_race_starts_the_next_call_at_once_when_the_others_failed() {
    let h = harness(Plan::Race(
        vec!["a", "b"],
        secs(10),
        Until::content(Output::is_content),
    ))
    .await;
    h.mock
        .reply(A, json(StatusCode::INTERNAL_SERVER_ERROR, "{}"));
    h.mock.reply(B, ok());

    let start = Instant::now();
    let (status, ..) = h.send().await;

    assert_eq!(status, StatusCode::OK);
    assert!(start.elapsed() < secs(10));
    assert_eq!(h.sent_to(), ["a.test", "b.test"]);
}

#[tokio::test]
async fn the_watchdog_fails_a_stalled_body_and_first_success_moves_on() {
    let h = harness(Plan::First(
        vec!["stalled", "b"],
        Backoff::once(),
        Until::content(Output::is_content),
    ))
    .await;
    h.mock.reply(B, ok());

    let (status, _, body, _) = h.send().await;

    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("Hello"), "{body}");
    let record = h.record.all();
    assert_eq!(record[0].0, "stalled");
    assert_eq!(record[0].3, "upstream");
}

#[tokio::test]
async fn the_watchdog_bounds_the_wait_for_the_head_too() {
    let h = harness(Plan::First(
        vec!["silent", "b"],
        Backoff::once(),
        Until::content(Output::is_content),
    ))
    .await;
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
        ("silent", "upstream")
    );
}

/// A whole answer sends its head only when it is done, so a whole answer
/// slower than `idle` but within `head` is neither cut nor sent again.
#[tokio::test]
async fn the_watchdog_does_not_cut_a_slow_whole_answer() {
    const WHOLE: &str = r#"{"model":"m","messages":[{"role":"user","content":"write an essay"}]}"#;
    const REPLY: &str = r#"{"id":"c","model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"an essay"},"finish_reason":"stop"}]}"#;
    let h = harness(Plan::First(
        vec!["late", "b"],
        retries(2),
        Until::content(Output::is_content),
    ))
    .await;
    h.mock.reply(A, json(StatusCode::OK, REPLY));
    h.mock.reply(B, ok());

    let (status, _, body, outcome) = h.post(WHOLE).await;

    assert_eq!((status, ended(outcome)), (StatusCode::OK, Ok(())));
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
        let h = harness(Plan::First(
            vec!["a", "b"],
            Backoff::once(),
            Until::content(Output::is_content),
        ))
        .await;
        h.mock.reply(A, too_many("1"));
        h.mock.reply(B, json(StatusCode::OK, REPLY));

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
            Until::content(Output::is_content),
        ))
        .await;
        h.mock.reply(B, ok());

        let (status, ..) = h.send().await;

        assert_eq!(status, StatusCode::OK);
        let mut record = h.record.all();
        record.sort_by_key(|r| r.1);
        let refused = ("refused".to_owned(), 0, Role::Loser, "upstream".to_owned());
        let b = ("b".to_owned(), 1, Role::Winner, "ok".to_owned());
        assert_eq!(record, [refused, b]);
    }
}

#[tokio::test]
async fn a_race_that_never_hedges_still_falls_over() {
    let h = harness(Plan::Race(
        vec!["a", "b"],
        Duration::MAX,
        Until::content(Output::is_content),
    ))
    .await;
    h.mock
        .reply(A, json(StatusCode::INTERNAL_SERVER_ERROR, "{}"));
    h.mock.reply(B, ok());
    let (status, _, body, _) = h.send().await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

/// `a` answers 200 and fails before any content; `b` answers.
fn a_fails_before_content(h: &Harness) {
    h.mock.reply(A, cut(&[ROLE]));
    h.mock.reply(B, ok());
}

#[tokio::test]
async fn until_head_commits_on_a_success_status_however_its_body_goes() {
    let plans = [
        Plan::First(vec!["a", "b"], Backoff::once(), Until::Head),
        Plan::Race(vec!["a", "b"], secs(1), Until::Head),
    ];
    for plan in plans {
        let h = harness(plan).await;
        a_fails_before_content(&h);

        let (status, _, body, outcome) = h.send().await;

        // Committed on its head, so its failure before any bytes is refused.
        assert_eq!(status, StatusCode::BAD_GATEWAY, "{body}");
        assert_eq!(h.sent_to(), ["a.test"]);
        assert!(matches!(outcome, Err(f) if f.is::<Upstream>()));
        let a = ("a".to_owned(), 0, Role::Winner, "upstream".to_owned());
        assert_eq!(h.record.all(), [a]);
    }
}

#[tokio::test]
async fn until_head_retries_and_falls_back_from_a_refusal() {
    let plans = [
        Plan::First(vec!["a", "b"], retries(3), Until::Head),
        Plan::Race(vec!["a", "b"], secs(1), Until::Head),
    ];
    for plan in plans {
        let h = harness(plan).await;
        for _ in 0..3 {
            h.mock.reply(A, Answer::error(503));
        }
        h.mock.reply(B, ok());

        let (status, _, body, outcome) = h.send().await;

        assert_eq!((status, ended(outcome)), (StatusCode::OK, Ok(())), "{body}");
        assert_eq!(meaning(&body), meaning(&[ROLE, HELLO, STOP, DONE].concat()));
        assert_eq!(h.sent_to().last().map(String::as_str), Some("b.test"));
    }
}

#[tokio::test]
async fn until_head_asks_the_classifier_about_a_refusal() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    let h = harness(Plan::Classified(
        vec!["a", "b"],
        retries(3),
        Until::Head,
        Arc::new(move |_, opened| {
            log.lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(opened);
            false
        }),
    ))
    .await;
    h.mock.reply(A, Answer::error(503));
    h.mock.reply(B, ok());

    let (status, ..) = h.send().await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(h.sent_to(), ["a.test", "b.test"]);
    assert_eq!(*seen.lock().unwrap_or_else(PoisonError::into_inner), [true]);
}

#[tokio::test]
async fn until_content_moves_on_from_a_success_failing_before_content() {
    let plans = [
        Plan::First(
            vec!["a", "b"],
            Backoff::once(),
            Until::content(Output::is_content),
        ),
        Plan::Race(vec!["a", "b"], secs(1), Until::content(Output::is_content)),
    ];
    for plan in plans {
        let h = harness(plan).await;
        a_fails_before_content(&h);

        let (status, _, body, outcome) = h.send().await;

        assert_eq!((status, ended(outcome)), (StatusCode::OK, Ok(())));
        assert_eq!(meaning(&body), meaning(&[ROLE, HELLO, STOP, DONE].concat()));
        assert_eq!(h.sent_to(), ["a.test", "b.test"]);
    }
}

#[tokio::test]
async fn until_end_sends_the_whole_answer_as_it_came_and_moves_on_before_content() {
    let plans = [
        Plan::First(vec!["a", "b"], Backoff::once(), Until::End),
        Plan::Race(vec!["a", "b"], secs(1), Until::End),
    ];
    for plan in plans {
        let h = harness(plan).await;
        a_fails_before_content(&h);

        let (status, _, body, outcome) = h.send().await;

        assert_eq!((status, ended(outcome)), (StatusCode::OK, Ok(())));
        assert_eq!(meaning(&body), meaning(&[ROLE, HELLO, STOP, DONE].concat()));
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
                (0, Role::Loser, "upstream".to_owned()),
                (1, Role::Winner, ok)
            ]
        );
    }
}

/// `dribble` sends content at once and ends in 5 s; `b-in-1s` answers in
/// 1 s and ends then: the first to content and the first to finish differ.
#[tokio::test]
async fn a_race_until_end_picks_the_call_that_finishes_first() {
    for (until, winner, loser) in [
        (Until::content(Output::is_content), "dribble", "b-in-1s"),
        (Until::End, "b-in-1s", "dribble"),
    ] {
        let h = harness(Plan::Race(
            vec!["dribble", "b-in-1s"],
            Duration::ZERO,
            until.clone(),
        ))
        .await;
        h.mock.reply(B, ok());

        let start = Instant::now();
        let (status, _, body, outcome) = h.send().await;

        assert_eq!(
            (status, ended(outcome)),
            (StatusCode::OK, Ok(())),
            "{until:?}"
        );
        assert_eq!(
            meaning(&body),
            meaning(&[ROLE, HELLO, STOP, DONE].concat()),
            "{until:?}"
        );
        let mut record = h.record.all();
        record.sort_by_key(|r| r.2 == Role::Loser);
        let got: Vec<_> = record
            .iter()
            .map(|r| (r.0.as_str(), r.2, r.3.as_str()))
            .collect();
        assert_eq!(got[0], (winner, Role::Winner, "ok"), "{until:?}");
        // A loser dropped before it was polled never started.
        let lost = (loser, Role::Loser, "abandoned");
        assert!(got[1..].iter().all(|r| *r == lost), "{until:?}: {got:?}");
        if matches!(until, Until::End) {
            assert_eq!(got.len(), 2);
        }
        // Reading to its end, the winner finished in 1 s, not 5.
        if matches!(until, Until::End) {
            assert!(start.elapsed() < secs(2), "{:?}", start.elapsed());
        }
    }
}

/// Read to its end, `a` fails after its content: not a success, so `b`'s
/// answer goes out, and `a`'s ends as it ended.
#[tokio::test]
async fn until_end_a_stream_failing_after_content_is_not_a_winner() {
    let plans = [
        Plan::First(vec!["a", "b"], Backoff::once(), Until::End),
        Plan::Race(vec!["a", "b"], secs(1), Until::End),
    ];
    for plan in plans {
        let h = harness(plan).await;
        h.mock.reply(A, cut(&[ROLE, HELLO]));
        h.mock.reply(B, ok());

        let (status, _, body, outcome) = h.send().await;

        assert_eq!((status, ended(outcome)), (StatusCode::OK, Ok(())));
        assert_eq!(meaning(&body), meaning(&[ROLE, HELLO, STOP, DONE].concat()));
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
            (a, Role::Loser, "upstream".to_owned()),
            (b, Role::Winner, "ok".to_owned()),
        ];
        assert_eq!(roles, expected);
    }
}

/// With retries, a stream failing after its content is retried when read
/// to its end; with none that succeed, the last answer goes out as it came.
#[tokio::test]
async fn until_end_retries_a_stream_failing_after_content_and_returns_the_last() {
    let h = harness(Plan::First(vec!["a"], retries(2), Until::End)).await;
    h.mock.reply(A, cut(&[ROLE, HELLO]));
    h.mock.reply(A, cut(&[ROLE, HELLO]));

    let (status, _, body, outcome) = h.send().await;

    assert_eq!(status, StatusCode::OK);
    let heard = meaning(&body);
    assert_eq!(heard[..2], meaning(&[ROLE, HELLO].concat())[..], "{body}");
    assert!(body.contains("\"code\":\"upstream\""), "{body}");
    assert!(matches!(outcome, Err(f) if f.is::<Upstream>()));
    assert_eq!(h.sent_to(), ["a.test", "a.test"]);
    let roles: Vec<_> = h.record.all().into_iter().map(|r| (r.1, r.2)).collect();
    assert_eq!(roles, [(0, Role::Loser), (1, Role::Winner)]);
}

#[tokio::test]
async fn a_wrong_content_type_does_not_replay_a_healthy_final_response() {
    let h = harness(Plan::First(
        vec!["a"],
        Backoff::once(),
        Until::content(|_: &u32| true),
    ))
    .await;
    h.mock.reply(A, ok());
    let (status, _, body, outcome) = h.send().await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    assert!(!body.contains("Hello"), "{body}");
    assert!(matches!(outcome, Err(fault) if fault.is::<Internal>()));
    assert_eq!(h.sent_to(), ["a.test"]);
    assert_eq!(h.record.all()[0].2, Role::Loser);
}

#[tokio::test]
async fn content_policy_accepts_custom_events_and_captured_configuration() {
    let threshold = 2u32;
    let Until::Content(read) = Until::content(move |event: &u32| *event >= threshold) else {
        panic!("expected the supplied policy");
    };
    let make = || Response::stream(stream::iter([Ok(0u32), Err(Upstream::fault("cut"))]));
    assert!(read(make()).await.1.is_err());
    let Until::Content(read) = Until::content(|_: &u32| true) else {
        panic!("expected the supplied policy");
    };
    assert!(read(make()).await.1.is_ok());
}

#[tokio::test]
async fn until_end_falls_back_when_a_provider_filter_fails_at_end() {
    struct FailEnd;
    impl respan_gateway_core::Filter<Output> for FailEnd {
        fn edit(&mut self, value: Output, out: &mut Vec<Output>) -> Result<(), Fault> {
            out.push(value);
            Ok(())
        }
        fn end(&mut self, _: &Result<(), Fault>, _: &mut Vec<Output>) -> Result<(), Fault> {
            Err(Upstream::fault("failed-at-end"))
        }
    }
    struct FailFirst;
    #[async_trait]
    impl Plugin for FailFirst {
        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            if px.provider() == "a" {
                px.filter(FailEnd)?;
            }
            Ok(())
        }
    }
    for plan in [
        Plan::First(vec!["a", "b"], Backoff::once(), Until::End),
        Plan::Race(vec!["a", "b"], secs(1), Until::End),
    ] {
        let mut h = harness(plan).await;
        h.gw = h.gw.plugin(FailFirst);
        h.mock.reply(A, ok()).reply(B, ok());
        let (status, _, _, end) = h.send().await;
        assert_eq!((status, ended(end)), (StatusCode::OK, Ok(())));
        assert_eq!(h.sent_to(), ["a.test", "b.test"]);
    }
}

/// `plugin` over the harness, its default target `a`.
async fn fallback(plugin: impl Plugin) -> Harness {
    targeted(plugin, "a").await
}

/// `plugin` over the harness, its default target `target`.
async fn targeted(plugin: impl Plugin, target: &str) -> Harness {
    let mut h = harness(plugin).await;
    h.gw = h.gw.target(target).unwrap_or_else(|f| panic!("{f}"));
    h
}

#[tokio::test]
async fn the_plugin_falls_back_from_its_target_to_the_chain() {
    let h = fallback(Fallback::new(retryable).after("a", ["b"])).await;
    h.mock.reply(A, too_many("0")).reply(B, ok());
    let (status, _, body, end) = h.send().await;
    assert_eq!((status, ended(end)), (StatusCode::OK, Ok(())));
    assert!(body.contains("Hello"), "{body}");
    assert_eq!(h.sent_to(), ["a.test", "b.test"]);
    let roles: Vec<_> = h.record.all().into_iter().map(|e| (e.0, e.2)).collect();
    assert_eq!(
        roles,
        [("a".into(), Role::Loser), ("b".into(), Role::Winner)]
    );
}

#[tokio::test]
async fn the_plugin_retries_its_target_as_its_backoff_allows_before_falling_back() {
    let plugin = Fallback::new(retryable)
        .after("a", ["b"])
        .backoff(retries(2))
        .until(Until::content(Output::is_content));
    let h = fallback(plugin).await;
    h.mock
        .reply(A, too_many("0"))
        .reply(A, too_many("0"))
        .reply(B, ok());
    let (status, ..) = h.send().await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(h.sent_to(), ["a.test", "a.test", "b.test"]);
}

#[tokio::test]
async fn the_plugin_leaves_a_target_without_a_chain_alone() {
    let h = fallback(Fallback::new(retryable).after("b", ["a"])).await;
    h.mock.reply(A, too_many("0"));
    let (status, ..) = h.send().await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(h.sent_to(), ["a.test"]);
    assert!(h.record.all().is_empty(), "no attempt of the plugin's");
}

#[tokio::test]
async fn the_plugin_gives_the_client_the_last_failure_when_the_chain_fails() {
    let h = fallback(Fallback::new(retryable).after("a", ["b"])).await;
    h.mock.reply(A, too_many("0")).reply(B, too_many("0"));
    let (status, ..) = h.send().await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(h.sent_to(), ["a.test", "b.test"]);
}

#[tokio::test]
async fn the_plugin_leaves_a_request_its_condition_refuses_alone() {
    let plugin = Fallback::new(retryable).after("a", ["b"]).when(|_| false);
    let h = fallback(plugin).await;
    h.mock.reply(A, too_many("0"));
    let (status, ..) = h.send().await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(h.sent_to(), ["a.test"]);
}

#[tokio::test]
async fn the_hedge_plugin_starts_its_chain_after_the_stagger_and_takes_the_first_success() {
    let plugin = Hedge::new(Duration::from_millis(200))
        .after("slow", ["b"])
        .until(Until::content(Output::is_content));
    let h = targeted(plugin, "slow").await;
    h.mock.reply(A, ok()).reply(B, ok());
    let start = Instant::now();
    let (status, _, _, end) = h.send().await;
    assert_eq!((status, ended(end)), (StatusCode::OK, Ok(())));
    assert!(start.elapsed() < secs(4), "{:?}", start.elapsed());
    let won: Vec<_> = h
        .record
        .all()
        .into_iter()
        .filter(|e| e.2 == Role::Winner)
        .collect();
    assert_eq!(won.len(), 1);
    assert_eq!(won[0].0, "b");
}

#[tokio::test]
async fn the_hedge_plugin_leaves_a_target_without_a_chain_alone() {
    let h = fallback(Hedge::new(Duration::ZERO).after("b", ["a"])).await;
    h.mock.reply(A, ok());
    let (status, ..) = h.send().await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(h.sent_to(), ["a.test"]);
    assert!(h.record.all().is_empty(), "no attempt of the plugin's");
}
