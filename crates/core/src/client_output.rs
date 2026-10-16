//! The response on its way to the client: the selected call's batches through
//! the client's output stages, and the outgoing flow built from them.

use parking_lot::Mutex;
use std::collections::VecDeque;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use futures::future::ready;
use futures::{FutureExt, StreamExt, stream};

use crate::call::SharedCall;
use crate::events::MAX_BATCH;
use crate::fault::PANICKED;
use crate::format::AnyFormat;
use crate::layers::{Layer, cut, end_layers, on_close, run, yield_ready};
use crate::lifecycle::EndHooks;
use crate::response::{Batch, Feed};
use crate::values::{AnyBatches, Value, Values};
use crate::{ClientContext, EventStream, Fault, Response, Result};

/// The client's response through its output stages, one value per item, and
/// the selected call, retained until delivery ends.
pub(crate) async fn client_flow(
    format: &Arc<dyn AnyFormat>,
    resp: Result<Response>,
    hooks: &mut EndHooks<ClientContext>,
    discarded: &Arc<Mutex<Vec<Batch>>>,
) -> (EventStream<Value>, Option<SharedCall>) {
    let cx = hooks.cx();
    // A call's response goes only to its own request; one of another kind
    // fails, even with no outputs.
    let resp = resp.and_then(|resp| match resp.feed.call() {
        Some(c) if !cx.open.holds(c) => {
            Err(Fault::internal("a response to another request was sent"))
        }
        _ if resp.output != format.kind().1 => Err(Fault::another_kind(
            "a response of another kind was sent".into(),
        )),
        _ => Ok(resp),
    });
    let mut resp = match resp {
        Ok(resp) => resp,
        Err(fault) => return (EventStream::refused(fault), None),
    };
    if resp.is_refused() {
        // Its one batch is the refusal, through the call's filters.
        let read = AssertUnwindSafe(async {
            let mut batch = match resp.ahead.pop_front() {
                Some(batch) => batch,
                None => resp.feed.next().await,
            };
            batch.read();
            batch.ending.map(|ending| ending.outcome().clone())
        });
        let fault = match read.catch_unwind().await {
            Ok(Some(Err(fault))) => fault,
            Ok(_) => Fault::internal("a refusal that did not fail"),
            Err(_) => Fault::internal(PANICKED),
        };
        return (EventStream::refused(fault), None);
    }
    let call = resp.feed.call().cloned();
    let Response { feed, ahead, .. } = resp;
    let ended = cx.ended.clone();
    let stages = match cx.stages() {
        Ok(stages) => stages,
        Err(fault) => return (EventStream::refused(fault), call),
    };
    let mut events = ToClient::new(ahead, feed, stages.latest.clone(), discarded.clone()).events();
    // Each wrap, the last added first, then the filters added before it.
    for (wrap, layer) in stages.wrapped.iter_mut().rev() {
        let Some(wrap) = wrap.take() else { continue };
        let wrap = AssertUnwindSafe(move || wrap(events));
        // A panic is an internal fault.
        let panicked = || stream::once(ready(Err(Fault::internal(PANICKED)))).boxed();
        let outputs = std::panic::catch_unwind(wrap).unwrap_or_else(|_| panicked());
        let feed = Feed::made(outputs);
        events = ToClient::new(VecDeque::new(), feed, layer.clone(), discarded.clone()).events();
    }
    // Once the request ends, its outputs are cut.
    let events = cut(events, on_close(&ended));
    let events = events.flat_map(|values| {
        stream::iter(match values {
            Ok(values) => values.split().into_iter().map(Ok).collect(),
            Err(fault) => vec![Err(fault)],
        })
    });
    let flow = EventStream::stream(events);
    (flow, call)
}

/// A response on its way to the client: what was read ahead, then its
/// feed, each batch read (its call ends at its end) and run through the
/// client's output filters, which end with it.
pub(crate) struct ToClient {
    ahead: VecDeque<Batch>,
    feed: Feed,
    layer: Layer,
    ready: VecDeque<Result<Values>>,
    done: bool,
    batches: usize,
    /// An unread end discarded by the writer waits for its delivery result.
    discarded: Arc<Mutex<Vec<Batch>>>,
}

impl ToClient {
    pub(crate) fn new(
        ahead: VecDeque<Batch>,
        feed: Feed,
        layer: Layer,
        discarded: Arc<Mutex<Vec<Batch>>>,
    ) -> Self {
        let (ready, done) = (VecDeque::new(), false);
        Self {
            ahead,
            feed,
            layer,
            ready,
            done,
            batches: 0,
            discarded,
        }
    }

    pub(crate) fn events(self) -> AnyBatches {
        stream::unfold(self, |mut o| async move { o.next().await.map(|v| (v, o)) }).boxed()
    }

    async fn next(&mut self) -> Option<Result<Values>> {
        loop {
            if let Some(item) = self.ready.pop_front() {
                return Some(item);
            }
            if self.done {
                return None;
            }
            // A panic reading or filtering ends the call and the reply.
            if AssertUnwindSafe(self.step()).catch_unwind().await.is_err() {
                let fault = Fault::internal(PANICKED);
                self.feed.finish(Err(fault.clone()));
                let _ = self.layer.lock().end(Err(fault.clone()), &mut None);
                self.ready.push_back(Err(fault));
                self.done = true;
            }
            self.batches += 1;
            // Feed::next never suspends after consuming a batch: borrowed
            // response inspection can be canceled without losing it. Yield
            // here instead, once the outgoing edge owns the buffered batch.
            if !self.done && (self.feed.is_call() || self.batches.is_multiple_of(MAX_BATCH)) {
                yield_ready().await;
            }
        }
    }

    /// The next batch, through the client's output filters; at the end, what
    /// they kept back and the end, including any filter failure.
    async fn step(&mut self) {
        let mut batch = match self.ahead.pop_front() {
            Some(batch) => batch,
            None => self.feed.next().await,
        };
        batch.read();
        let mut end = batch.ending.take().map(|ending| ending.outcome().clone());
        if let Some(mut values) = batch.values.take() {
            if let Err(fault) = run(&[&self.layer], &mut values) {
                end = Some(Err(fault));
            }
            self.ready.push_back(Ok(values));
        }
        let Some(end) = end else { return };
        self.done = true;
        self.feed.finish(end.clone());
        // Filters see every end, a failure too.
        let mut tail = None;
        let end = end_layers(&[&self.layer], end, &mut tail);
        self.ready.extend(tail.map(Ok));
        if let Err(fault) = end {
            self.ready.push_back(Err(fault));
        }
    }
}

/// Dropped before its end, unread: its call ends as its end says, if it is
/// ready; else as its source goes, abandoned, or with its request if the
/// reply holds it.
impl Drop for ToClient {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        let ahead = self.ahead.iter_mut().find(|b| b.ending.is_some());
        let batch = match ahead {
            Some(batch) => std::mem::take(batch),
            None => {
                let next = AssertUnwindSafe(|| self.feed.next().now_or_never());
                match std::panic::catch_unwind(next) {
                    Ok(Some(batch)) => batch,
                    _ => return,
                }
            }
        };
        self.discarded.lock().push(batch);
    }
}
