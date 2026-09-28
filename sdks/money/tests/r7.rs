//! Probes of the round-7 core: usage counts as decoded, whoever reads the
//! response, and every filter gets exactly one end.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use common::*;
use respan_gateway_core::http::StatusCode;
use respan_gateway_core::{
    ClientContext, Fault, Filter, Plugin, ProviderContext, Response, Usage, async_trait,
};
use respan_gateway_formats::conversation::{Conversation, Message, Reply, Role};
use respan_gateway_testkit::mock;
use respan_money::Basis;

/// Reads the first provider's response to its first content, drops it, and
/// answers with the second's.
struct ReadyThenDrop(&'static str, &'static str);

#[async_trait]
impl Plugin for ReadyThenDrop {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let first = cx.call(self.0).await?.ready().await?;
        drop(first);
        cx.call(self.1).await.map(Some)
    }
}

/// Fixed by F7-2: a whole body read by `ready()` and then dropped was
/// decoded to its end, usage and all; the provider bills it, so the budget
/// must too.
#[tokio::test]
async fn a_whole_answer_read_by_ready_then_dropped_is_charged_its_usage() {
    let bank = bank(10_000_000);
    let h = H::new(
        bank.clone(),
        vec![
            Arc::new(money(&bank)),
            Arc::new(ReadyThenDrop("openai", "backup")),
        ],
        None,
    );
    h.mock
        .reply(OPENAI, mock::json(StatusCode::OK, UNARY))
        .reply(BACKUP, mock::json(StatusCode::OK, UNARY));

    assert_eq!(h.send(chat(UNARY_REQ)).await.0, StatusCode::OK);

    assert_eq!(
        h.bases(),
        [(Basis::Reported, UNARY_COST), (Basis::Reported, UNARY_COST)]
    );
}

/// An Anthropic stream reports its input in `message_start`, with a
/// placeholder output of 1.
const CLAUDE_STREAM: &[&str] = &[
    "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m\",\"model\":\"gpt-x\",\"usage\":{\"input_tokens\":7,\"output_tokens\":1}}}\n\n",
    "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
    "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"a long answer that the client never sees at all\"}}\n\n",
    "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":400}}\n\n",
    "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
];

/// Fixed by F7-3, and the new rule's price: a stream read by `ready()` to
/// its first content and then dropped has its `message_start` usage
/// counted. The call's `Outcome.usage` is then partial (input 7 and the
/// placeholder output 1), and only `outcome.end` says so. The budget must
/// not take the reported output as final for a call that did not end `Ok`.
#[tokio::test]
async fn a_stream_dropped_after_its_first_content_is_charged_at_least_what_was_seen() {
    let bank = bank(10_000_000);
    let h = H::new(
        bank.clone(),
        vec![
            Arc::new(money(&bank)),
            Arc::new(ReadyThenDrop("claude", "backup")),
        ],
        None,
    );
    h.mock
        .reply(CLAUDE, mock::sse(CLAUDE_STREAM))
        .reply(BACKUP, mock::json(StatusCode::OK, UNARY));

    assert_eq!(h.send(chat(STREAM_REQ)).await.0, StatusCode::OK);

    let journal = h.settled();
    let claude = &journal[0];
    // Input as reported; output the larger of the placeholder and the 12
    // tokens of text seen.
    assert_eq!(
        (claude.basis, claude.charged),
        (Basis::ToppedUp, 7 * 10 + 12 * 30),
        "{journal:?}"
    );
}

/// Counts each filter added and each end it gets, by what it filters.
#[derive(Clone, Default)]
struct Ends {
    added: Arc<AtomicUsize>,
    ended: Arc<AtomicUsize>,
    doubled: Arc<Mutex<Vec<&'static str>>>,
}

struct Counted<T>(Ends, &'static str, bool, std::marker::PhantomData<fn(T)>);

/// A copy the call adds: counted as added.
impl<T> Clone for Counted<T> {
    fn clone(&self) -> Self {
        self.0.added.fetch_add(1, Ordering::SeqCst);
        Counted(self.0.clone(), self.1, false, std::marker::PhantomData)
    }
}

impl<T: Send + 'static> Filter<T> for Counted<T> {
    fn edit(&mut self, value: T, out: &mut Vec<T>) -> Result<(), Fault> {
        out.push(value);
        Ok(())
    }

    fn end(&mut self, _end: &Result<(), Fault>, _out: &mut Vec<T>) -> Result<(), Fault> {
        if self.2 {
            self.0.doubled.lock().map(|mut d| d.push(self.1)).ok();
        }
        self.2 = true;
        self.0.ended.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

impl Ends {
    fn filter<T: Send + 'static>(&self, what: &'static str) -> Counted<T> {
        self.added.fetch_add(1, Ordering::SeqCst);
        Counted(self.clone(), what, false, std::marker::PhantomData)
    }

    /// A filter for `Call::with_filter`, which adds a clone per call sent.
    fn template<T: Send + 'static>(&self, what: &'static str) -> Counted<T> {
        Counted(self.clone(), what, false, std::marker::PhantomData)
    }

    fn check(&self) {
        let doubled = self.doubled.lock().map(|d| d.clone()).unwrap_or_default();
        assert!(doubled.is_empty(), "ended twice: {doubled:?}");
        assert_eq!(
            self.ended.load(Ordering::SeqCst),
            self.added.load(Ordering::SeqCst)
        );
    }
}

/// Adds an output, input and usage filter on both edges.
struct Filters(Ends);

#[async_trait]
impl Plugin for Filters {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        if cx.is_kind::<Conversation>() {
            cx.filter(self.0.filter::<Reply>("client outputs"))?;
            cx.filter(self.0.filter::<Conversation>("client inputs"))?;
            cx.filter(self.0.filter::<Usage>("client usage"))?;
        }
        Ok(None)
    }

    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        if px.is_kind::<Conversation>() {
            px.filter(self.0.filter::<Reply>("provider outputs"))?;
            px.filter(self.0.filter::<Conversation>("provider inputs"))?;
            px.filter(self.0.filter::<Usage>("provider usage"))?;
        }
        Ok(())
    }
}

/// A judge call with its own filter, read to its end, then the target;
/// a second call refused on its provider edge; a third read by `ready()`
/// and dropped; a fourth dropped before its head.
struct Busy(Ends);

#[async_trait]
impl Plugin for Busy {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        use respan_gateway_core::futures::TryStreamExt;
        let mut ask = Conversation::new([Message::new(Role::User, "ok?")]);
        ask.model = Some("judge-1".into());
        let judge = cx
            .call("openai")
            .with_input(ask)
            .with_filter(self.0.template::<Usage>("judge usage"));
        let _: Vec<Reply> = judge.await?.outputs().try_collect().await?;
        let refused = cx
            .call("opaque")
            .with_filter(self.0.template::<Usage>("refused usage"));
        assert!(refused.await.is_err());
        drop(cx.call("backup").await?.ready().await?);
        let pending = cx
            .call("slow")
            .with_filter(self.0.template::<Usage>("pending usage"));
        let _ = tokio::time::timeout(std::time::Duration::from_millis(10), pending).await;
        Ok(None)
    }
}

/// F7-6: every filter added gets exactly one end, whatever becomes of its
/// call: read to its end, refused, read then dropped, dropped pending, or
/// sent.
#[tokio::test]
async fn every_filter_gets_exactly_one_end() {
    let ends = Ends::default();
    let bank = bank(10_000_000);
    let h = H::new(
        bank.clone(),
        vec![
            Arc::new(Filters(ends.clone())),
            Arc::new(money(&bank)),
            Arc::new(Busy(ends.clone())),
        ],
        Some("openai"),
    );
    for base in [OPENAI, OPENAI, BACKUP, SLOW] {
        h.mock.reply(base, mock::json(StatusCode::OK, UNARY));
    }

    let (status, body) = h.send(chat(UNARY_REQ)).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    ends.check();
    // Three on the client edge; three on each of the five calls' provider
    // edges (judge, backup, slow, the target), and one for each of the two
    // sent calls given their own; "opaque" is refused before its edge.
    assert_eq!(ends.added.load(Ordering::SeqCst), 3 + 3 * 4 + 2);
}

/// A refusal on the client edge after a plugin added filters: they end too.
#[tokio::test]
async fn filters_of_a_refused_request_end_once() {
    let ends = Ends::default();
    let bank = bank(10_000_000);
    let h = H::new(
        bank.clone(),
        vec![Arc::new(Filters(ends.clone())), Arc::new(Guard)],
        Some("openai"),
    );

    assert_eq!(h.send(chat(UNARY_REQ)).await.0, StatusCode::FORBIDDEN);

    ends.check();
    assert_eq!(ends.added.load(Ordering::SeqCst), 3);
}

/// The `Filter::end` rustdoc: "Runs once at every end ... Not if something
/// panics." Yet when a client-edge filter panics mid-stream, every other
/// filter, on both edges, still gets its end: the exception is broader in
/// the rustdoc than in the code.
#[tokio::test]
async fn every_other_filter_ends_when_one_panics() {
    let ends = Ends::default();
    let bank = bank(10_000_000);
    let h = H::new(
        bank.clone(),
        vec![
            Arc::new(Filters(ends.clone())),
            Arc::new(money(&bank)),
            Arc::new(PanicFilter),
        ],
        Some("openai"),
    );
    h.mock.reply(OPENAI, mock::sse(STREAM));

    let _ = h.send(chat(STREAM_REQ)).await;

    let (added, ended) = (
        ends.added.load(Ordering::SeqCst),
        ends.ended.load(Ordering::SeqCst),
    );
    assert_eq!((added, ended), (6, 6), "ended {ended} of {added}");
}
