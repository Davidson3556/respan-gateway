//! `Accounting` in a gateway: never overspend, settle exactly once, refuse
//! what has no bound, and release what a cancelled hook held.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use respan_gateway_core::futures::future::join_all;
use respan_gateway_core::{
    ClientContext, Fault, Gateway, Plugin, ProviderContext, Response, async_trait,
};
use respan_gateway_formats::conversation::{Conversation, Reply, units};
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::http::StatusCode;
use respan_gateway_ledger::{Accounting, Amount, HoldId, Ledger, MemoryLedger, Quote, Tally};
use respan_gateway_providers::OpenAiChat;
use respan_gateway_testkit::{MockTransport, mock, post, read_whole, serve};

const P: &str = "http://p.test/v1";
const CHAT: &str = "/v1/chat/completions";
/// 10 output tokens at 10 each.
const RESERVE: Amount = 100;

/// Asks for at most 10 output tokens.
fn ask(stream: bool) -> String {
    format!(
        r#"{{"model":"m","max_tokens":10,"stream":{stream},"messages":[{{"role":"user","content":"hi"}}]}}"#
    )
}

/// A whole answer reporting `output` tokens.
fn answer(output: u32) -> mock::Reply {
    let body = format!(
        r#"{{"id":"c1","object":"chat.completion","model":"m","choices":[{{"index":0,"message":{{"role":"assistant","content":"hi"}},"finish_reason":"stop"}}],"usage":{{"prompt_tokens":3,"completion_tokens":{output},"total_tokens":{}}}}}"#,
        output + 3
    );
    mock::json(StatusCode::OK, &body)
}

const DELTA: &str = "data: {\"id\":\"c1\",\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\n";

/// Output tokens at 10 each, bounded by `max_tokens`; none set is no bound.
/// Each usage increment the provider reports is priced into the tally.
fn quote(px: &mut ProviderContext, tally: Tally) -> Result<Quote, Fault> {
    let chat = px.input::<Conversation>()?;
    let max = chat
        .max_tokens
        .ok_or_else(|| Fault::bad_request("no cost bound: set max_tokens"))?;
    px.filter(move |reply: &mut Reply| {
        if !reply.usage.is_empty() {
            let output = reply.usage.get(units::OUTPUT_TOKENS).unwrap_or(0.0);
            tally.add(output.ceil() as Amount * 10);
        }
        Ok(())
    })?;
    Ok(Quote::new(["key"], max as Amount * 10))
}

fn wallet(limit: Amount) -> MemoryLedger {
    let ledger = MemoryLedger::new();
    ledger.open("key", None, limit);
    ledger
}

/// `first`, then accounting over `ledger`, calling provider `p` on `mock`.
fn gateway(mock: &MockTransport, first: impl Plugin, ledger: impl Ledger) -> Gateway {
    let accounting = Accounting::new(ledger, quote).ttl(Duration::from_millis(200));
    let gw = Gateway::new().client::<Conversation, _, _>(OpenAiChat);
    let gw = gw.and_then(|g| {
        g.provider::<Conversation, _, _>("p", OpenAiChat, respan_gateway_http::connect(mock.at(P)))
    });
    let gw = gw.and_then(|g| g.target("p"));
    let gw = gw.and_then(|g| g.plugin("first", first));
    gw.and_then(|g| g.plugin("accounting", accounting))
        .unwrap_or_else(|f| panic!("{f}"))
}

struct Nothing;
impl Plugin for Nothing {}

async fn send(gw: &Gateway, body: String) -> (StatusCode, Result<Bytes, Fault>) {
    let (status, _, body) = read_whole(serve(gw, post(CHAT, body)).await.0).await;
    gw.idle().await;
    (status, body)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn fifty_parallel_requests_never_overspend() {
    let (mock, ledger) = (MockTransport::new(), wallet(10 * RESERVE));
    for _ in 0..50 {
        mock.reply(P, answer(10));
    }
    let gw = gateway(&mock, Nothing, ledger.clone());
    let requests = (0..50).map(|_| {
        let gw = gw.clone();
        tokio::spawn(async move { send(&gw, ask(false)).await.0 })
    });
    let statuses: Vec<StatusCode> = join_all(requests).await.into_iter().flatten().collect();
    let ok = statuses.iter().filter(|s| s.is_success()).count();
    let short = statuses.iter().filter(|s| s.as_u16() == 402).count();
    assert_eq!((ok, short), (10, 40));
    assert_eq!((ledger.spent("key"), ledger.available("key")), (1000, 0));
}

/// A ledger that notes every hold and settlement.
#[derive(Clone, Default)]
struct Noted {
    inner: MemoryLedger,
    reserved: Arc<Mutex<Vec<HoldId>>>,
    settled: Arc<Mutex<Vec<(HoldId, Amount)>>>,
}

#[async_trait]
impl Ledger for Noted {
    async fn reserve(
        &self,
        id: &HoldId,
        accounts: &[String],
        amount: Amount,
        ttl: Duration,
    ) -> Result<(), Fault> {
        self.reserved
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(*id);
        self.inner.reserve(id, accounts, amount, ttl).await
    }

    async fn settle(&self, id: &HoldId, spent: Amount) -> Result<(), Fault> {
        let settled = &self.settled;
        settled
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((*id, spent));
        self.inner.settle(id, spent).await
    }
}

/// Settled once, for `charged`, whether the call succeeded, failed or was
/// given up. A call sent may have reached the provider, so a failure with
/// no answer is charged the reserve.
#[tokio::test]
async fn every_call_settles_exactly_once() {
    let cut = vec![Ok(Bytes::from(DELTA)), Err(Fault::upstream("cut"))];
    let mut cut_stream = mock::sse(&[]);
    *cut_stream.body_mut() = cut;
    let usage_later = || {
        mock::sse(&[
            DELTA,
            "data: {\"id\":\"c1\",\"choices\":[],\"usage\":{\"completion_tokens\":2}}\n\n",
            "data: [DONE]\n\n",
        ])
    };
    let refused = mock::json(
        StatusCode::INTERNAL_SERVER_ERROR,
        r#"{"error":{"message":"x"}}"#,
    );
    // (what the provider does, a stream, whether the client reads, charged)
    let cases = [
        ("reported", Some(answer(4)), false, true, 40),
        ("provider refused", Some(refused), false, true, 0),
        ("transport failed after sending", None, false, true, RESERVE),
        (
            "stream cut before usage",
            Some(cut_stream),
            true,
            true,
            RESERVE,
        ),
        // Usage alone, in the stream's last chunk, reaches the tally.
        ("streamed", Some(usage_later()), true, true, 20),
        ("client left", Some(usage_later()), true, false, RESERVE),
    ];
    for (case, reply, stream, read, charged) in cases {
        let (mock, ledger) = (MockTransport::new(), Noted::default());
        ledger.inner.open("key", None, 1000);
        if let Some(reply) = reply {
            mock.reply(P, reply);
        }
        let gw = gateway(&mock, Nothing, ledger.clone());
        let (resp, _) = serve(&gw, post(CHAT, ask(stream))).await;
        if read {
            drop(read_whole(resp).await);
        } else {
            drop(resp); // the client left
        }
        gw.idle().await;
        let reserved = ledger
            .reserved
            .lock()
            .map(|r| r.clone())
            .unwrap_or_default();
        let settled = ledger.settled.lock().map(|s| s.clone()).unwrap_or_default();
        assert_eq!(reserved.len(), 1, "{case}");
        assert_eq!(settled, [(reserved[0], charged)], "{case}");
        assert_eq!(ledger.inner.available("key"), 1000 - charged, "{case}");
    }
}

#[tokio::test]
async fn a_request_with_no_cost_bound_is_refused_before_it_is_sent() {
    let (mock, ledger) = (MockTransport::new(), wallet(1000));
    mock.reply(P, answer(4));
    let gw = gateway(&mock, Nothing, ledger.clone());
    let unbounded = r#"{"model":"m","messages":[{"role":"user","content":"hi"}]}"#;
    let (status, _) = send(&gw, unbounded.to_owned()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(mock.sent().is_empty());
    assert_eq!(ledger.available("key"), 1000);
}

/// Calls `p`, giving up after 20ms, which cancels a pending `on_provider`.
struct GivesUp;

#[async_trait]
impl Plugin for GivesUp {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        match tokio::time::timeout(Duration::from_millis(20), cx.call("p")).await {
            Ok(resp) => resp.map(Some),
            Err(_) => Err(Fault::upstream("gave up")),
        }
    }
}

/// Holds, then never answers: the acknowledgement is lost.
#[derive(Clone)]
struct LostAck(MemoryLedger);

#[async_trait]
impl Ledger for LostAck {
    async fn reserve(
        &self,
        id: &HoldId,
        a: &[String],
        n: Amount,
        t: Duration,
    ) -> Result<(), Fault> {
        self.0.reserve(id, a, n, t).await?;
        std::future::pending().await
    }

    async fn settle(&self, id: &HoldId, spent: Amount) -> Result<(), Fault> {
        self.0.settle(id, spent).await
    }
}

#[tokio::test(start_paused = true)]
async fn a_cancelled_on_provider_releases_its_hold() {
    let (mock, ledger) = (MockTransport::new(), wallet(1000));
    let gw = gateway(&mock, GivesUp, LostAck(ledger.clone()));
    let (status, _) = send(&gw, ask(false)).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(mock.sent().is_empty());
    assert_eq!((ledger.available("key"), ledger.spent("key")), (1000, 0));
}

/// Holds only after the caller gave up, as a remote store may: the
/// settlement arrives first and finds nothing.
#[derive(Clone)]
struct LateHold(MemoryLedger);

#[async_trait]
impl Ledger for LateHold {
    async fn reserve(
        &self,
        id: &HoldId,
        a: &[String],
        n: Amount,
        t: Duration,
    ) -> Result<(), Fault> {
        let (ledger, id, a) = (self.0.clone(), *id, a.to_vec());
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            ledger.reserve(&id, &a, n, t).await
        });
        std::future::pending().await
    }

    async fn settle(&self, id: &HoldId, spent: Amount) -> Result<(), Fault> {
        self.0.settle(id, spent).await
    }
}

#[tokio::test(start_paused = true)]
async fn a_hold_that_lands_after_its_settlement_expires_with_its_ttl() {
    let (mock, ledger) = (MockTransport::new(), wallet(1000));
    let gw = gateway(&mock, GivesUp, LateHold(ledger.clone()));
    let _ = send(&gw, ask(false)).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(ledger.available("key"), 1000 - RESERVE, "the hold landed");
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!((ledger.available("key"), ledger.spent("key")), (1000, 0));
}

/// Takes the call, then gives up waiting for its head after a second, as a
/// timeout does: the provider may be doing the work.
struct NoHead;

impl NoHead {
    pub async fn send(
        &self,
        _call: respan_gateway_http::Exchange<respan_gateway_http::RequestHead>,
    ) -> Result<respan_gateway_http::Exchange<respan_gateway_http::ResponseHead>, Fault> {
        tokio::time::sleep(Duration::from_secs(1)).await;
        Err(Fault::upstream("no head within 1s"))
    }
}
impl From<NoHead> for respan_gateway_http::Transport {
    fn from(value: NoHead) -> Self {
        let value = std::sync::Arc::new(value);
        Self::new(move |call| {
            let value = value.clone();
            async move { value.send(call).await }
        })
    }
}

#[tokio::test(start_paused = true)]
async fn a_call_sent_that_times_out_before_its_head_is_charged_the_reserve() {
    let ledger = wallet(1000);
    let accounting = Accounting::new(ledger.clone(), quote);
    let gw = Gateway::new().client::<Conversation, _, _>(OpenAiChat);
    let gw = gw.and_then(|g| {
        g.provider::<Conversation, _, _>("p", OpenAiChat, respan_gateway_http::connect(NoHead))
    });
    let gw = gw.and_then(|g| g.target("p"));
    let gw = gw.and_then(|g| g.plugin("accounting", accounting));
    let gw = gw.unwrap_or_else(|f| panic!("{f}"));
    let (status, _) = send(&gw, ask(false)).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(
        (ledger.spent("key"), ledger.available("key")),
        (RESERVE, 1000 - RESERVE)
    );
}
