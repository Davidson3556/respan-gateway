use crate::Result;
use std::any::{Any, TypeId};
use std::collections::VecDeque;
use std::panic::AssertUnwindSafe;

use futures::stream::{self, BoxStream, Stream};
use futures::{FutureExt, StreamExt};

use crate::Fault;
use crate::call::{CallEdge, SharedCall};
use crate::events::MAX_BATCH;
use crate::fault::PANICKED;
use crate::leg::{cut, locked, on_close, run, yield_ready};
use crate::values::{AnyEvents, Values, erase, take, view};

/// A response for the client, from a provider or a plugin. Dropped unsent
/// and unread, its call ends abandoned.
#[must_use = "a response does nothing unless sent or read"]
pub struct Response {
    pub(crate) feed: Feed,
    /// What [`Response::buffer_until`] or [`Response::until_end`] read,
    /// to be read again first.
    pub(crate) ahead: VecDeque<Batch>,
    /// Its outputs' type, even when it has none, as a refusal.
    pub(crate) output: TypeId,
}

impl std::fmt::Debug for Response {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Response").finish_non_exhaustive()
    }
}

/// Outputs read or made, optionally carrying their terminal result.
#[derive(Default)]
pub(crate) struct Batch {
    pub(crate) values: Option<Values>,
    pub(crate) ending: Option<Ending>,
}

/// A terminal result and the obligation to finish its provider call.
/// Buffering finalizes filters but keeps this obligation. Reading or dropping
/// the ending settles it; request cancellation may settle the call first.
pub(crate) struct Ending {
    outcome: Result<()>,
    call: Option<SharedCall>,
}

impl Ending {
    pub(crate) fn new(outcome: Result<()>) -> Self {
        Self {
            outcome,
            call: None,
        }
    }

    pub(crate) fn outcome(&self) -> &Result<()> {
        &self.outcome
    }

    /// Finalize provider filters without running end hooks. Their final
    /// values and outcome must be visible to response inspection.
    fn prepare(&mut self, values: &mut Option<Values>) {
        let Some(call) = self.call.take() else {
            return;
        };
        let prepared = std::panic::catch_unwind(AssertUnwindSafe(|| {
            self.finish_outputs(&mut locked(&call), values);
        }));
        if let Err(panic) = prepared {
            locked(&call).finish(Err(Fault::internal(PANICKED)));
            std::panic::resume_unwind(panic);
        }
        self.call = Some(call);
    }

    /// Consume the obligation before running user filters or hooks, so a
    /// panic or subsequent drop cannot attempt completion again.
    fn settle(&mut self, values: &mut Option<Values>) -> std::thread::Result<()> {
        let Some(call) = self.call.take() else {
            return Ok(());
        };
        let settled = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let mut edge = locked(&call);
            self.finish_outputs(&mut edge, values);
            edge.finish(self.outcome.clone());
        }));
        if settled.is_err() {
            locked(&call).finish(Err(Fault::internal(PANICKED)));
        }
        settled
    }

    fn finish_outputs(&mut self, edge: &mut CallEdge, values: &mut Option<Values>) {
        if edge.is_finished() {
            *values = None;
            self.outcome = Err(Fault::abandoned());
            return;
        }
        let mut outputs = locked(&edge.cx().outputs);
        // Buffering may already have finalized these filters. Preserve the
        // stored outcome and never emit their final values a second time.
        if !outputs.ended() {
            self.outcome = outputs.end(self.outcome.clone(), values);
        }
    }
}

impl Drop for Ending {
    fn drop(&mut self) {
        // Discard final filter values, but still settle the provider call.
        // Drop must not propagate a user filter's panic.
        let _ = self.settle(&mut None);
    }
}

impl Batch {
    /// The end, `end`, alone.
    pub(crate) fn ending(end: Result<()>) -> Self {
        Self {
            ending: Some(Ending::new(end)),
            ..Self::default()
        }
    }

    fn is_empty(&self) -> bool {
        self.values.is_none() && self.ending.is_none()
    }

    fn prepare(&mut self) {
        if let Some(ending) = &mut self.ending {
            ending.prepare(&mut self.values);
        }
    }

    /// Reading a terminal batch settles its call before exposing its values.
    pub(crate) fn read(&mut self) {
        if let Some(ending) = &mut self.ending
            && let Err(panic) = ending.settle(&mut self.values)
        {
            std::panic::resume_unwind(panic);
        }
    }

    /// Apply provider filters and attach call ownership to the terminal result.
    fn filter_provider_outputs(mut self, call: &SharedCall) -> Self {
        let mut edge = locked(call);
        if edge.is_finished() {
            return Self::ending(Err(Fault::abandoned()));
        }
        if let Some(values) = &mut self.values
            && let Err(fault) = run(&[&edge.cx().outputs], values)
        {
            self.ending = Some(Ending::new(Err(fault)));
        }
        if let Some(ending) = &mut self.ending {
            ending.call = Some(call.clone());
        }
        self
    }
}

impl Response {
    fn new(feed: Feed, output: TypeId) -> Self {
        Self {
            feed,
            ahead: VecDeque::new(),
            output,
        }
    }

    /// Whether the provider refused it at its head: its one batch is the
    /// refusal, which [`until_end`](Response::until_end) reads as its `Err`.
    pub fn is_refused(&self) -> bool {
        matches!(self.feed, Feed::Call { refused: true, .. })
    }

    /// Whether its call carries live inputs, so its events flow only once
    /// it is sent; holding it could deadlock a peer, and is skipped.
    fn holds_live_inputs(&self) -> bool {
        matches!(
            self.feed,
            Feed::Call {
                live: true,
                refused: false,
                ..
            }
        )
    }

    /// A success of the client's outputs from `stream`, such as a merge of
    /// calls' [`outputs`](Response::outputs); the first `Err` fails it, a
    /// refusal before any output. It passes no
    /// provider edge. Outputs of another type fail it with
    /// [`Internal`](crate::Internal) when sent.
    pub fn stream<T, S>(stream: S) -> Self
    where
        T: Any + Send + Sync,
        S: Stream<Item = Result<T>> + Send + 'static,
    {
        let feed = Feed::made(erase(stream.boxed()));
        Self::new(feed, TypeId::of::<T>())
    }

    /// Buffer outputs until `predicate` matches, the stream ends, or it fails.
    /// Values are inspected after provider filters. Buffered values are replayed
    /// when sent or read, retaining call ownership.
    /// A fault before a match is returned, with the response still owned by
    /// the caller; a fault after a match stays buffered for replay.
    /// A clean end succeeds even if no value matched.
    ///
    /// Streaming-input calls return unread to avoid deadlocking duplex peers.
    /// Bound the wait and buffered size in the calling policy if needed.
    /// A repeated call checks buffered values against its new predicate.
    pub async fn buffer_until<T: Any + Send + Sync>(
        &mut self,
        mut predicate: impl FnMut(&T) -> bool + Send,
    ) -> Result<()> {
        if self.output != TypeId::of::<T>() {
            return Err(Fault::wrong_kind::<T>());
        }
        if self.holds_live_inputs() {
            return Ok(());
        }
        let mut index = 0;
        loop {
            if index == self.ahead.len() {
                let batch = self.feed.next().await;
                if batch.is_empty() {
                    continue;
                }
                self.ahead.push_back(batch);
            }
            let batch = &mut self.ahead[index];
            batch.prepare();
            let matched = std::panic::catch_unwind(AssertUnwindSafe(|| {
                let values = batch.values.as_ref().and_then(view::<T>);
                values.is_some_and(|values| values.iter().any(&mut predicate))
            }));
            match matched {
                Ok(true) => return Ok(()),
                Ok(false) => {}
                Err(panic) => {
                    self.feed.finish(Err(Fault::internal(PANICKED)));
                    std::panic::resume_unwind(panic);
                }
            }
            if let Some(ending) = &batch.ending {
                ending.outcome().clone()?;
                return Ok(());
            }
            index += 1;
            // Yield only after ownership of the batch has moved into ahead.
            // Cancellation of this borrowed hold must never lose an event.
            if self.feed.is_call() || index.is_multiple_of(MAX_BATCH) {
                yield_ready().await;
            }
        }
    }

    /// Buffer the entire response, retaining its buffered events,
    /// terminal result and call ownership even when it fails. Streaming-input
    /// calls return unread to avoid deadlocking duplex peers.
    pub async fn until_end(&mut self) -> Result<()> {
        if self.holds_live_inputs() {
            return Ok(());
        }
        if let Some(batch) = self.ahead.back_mut() {
            batch.prepare();
        }
        let mut batches = 0usize;
        loop {
            if let Some(end) = self.buffered_end() {
                return end.clone();
            }
            let mut batch = self.feed.next().await;
            batch.prepare();
            if !batch.is_empty() {
                self.ahead.push_back(batch);
                batches += 1;
                // Store the batch before yielding so cancellation can resume
                // inspection or replay without losing any provider events.
                if self.buffered_end().is_none()
                    && (self.feed.is_call() || batches.is_multiple_of(MAX_BATCH))
                {
                    yield_ready().await;
                }
            }
        }
    }

    /// The terminal result already buffered by an inspection, if any.
    /// An inspection error without a buffered end (for example a wrong
    /// predicate type) is not a provider failure that can be replayed.
    pub fn buffered_end(&self) -> Option<&Result<()>> {
        self.ahead
            .back()
            .and_then(|batch| batch.ending.as_ref())
            .map(Ending::outcome)
    }

    /// Read it rather than send it, such as a judge's verdict: every output
    /// as `T`, as the provider edge's filters left them, then an `Err` if it
    /// fails; one `Err` with [`Internal`](crate::Internal)
    /// for another type. Its call ends with the stream, or abandoned if
    /// dropped first.
    pub fn outputs<T: Any + Clone + Send>(mut self) -> BoxStream<'static, Result<T>> {
        if self.output != TypeId::of::<T>() {
            let fault = Fault::wrong_kind::<T>();
            self.feed.finish(Err(fault.clone()));
            return stream::once(async { Err(fault) }).boxed();
        }
        let read = (self, VecDeque::new(), false, 0usize);
        stream::unfold(
            read,
            |(mut resp, mut outputs, mut ended, mut batches)| async move {
                loop {
                    if let Some(output) = outputs.pop_front() {
                        return Some((output, (resp, outputs, ended, batches)));
                    }
                    if ended {
                        return None;
                    }
                    let mut batch = match resp.ahead.pop_front() {
                        Some(batch) => batch,
                        None => resp.feed.next().await,
                    };
                    batch.read();
                    let read = batch.values.map(take::<T>).transpose();
                    let end = match (read, batch.ending) {
                        (Err(fault), _) => Some(Err(fault)),
                        (Ok(read), ending) => {
                            outputs.extend(read.into_iter().flatten().map(Ok));
                            ending.map(|ending| ending.outcome().clone())
                        }
                    };
                    if let Some(end) = end {
                        resp.feed.finish(end.clone());
                        outputs.extend(end.err().map(Err));
                        ended = true;
                    }
                    batches += 1;
                    // A batch has been read into the owned unfold state. Yield
                    // even if its filters emitted no values, so an indefinitely
                    // ready provider cannot monopolize one poll.
                    if !ended && (resp.feed.is_call() || batches.is_multiple_of(MAX_BATCH)) {
                        yield_ready().await;
                    }
                }
            },
        )
        .boxed()
    }

    /// A provider's output flow through its call, cut at the call's end
    /// so a waiting reader wakes.
    pub(crate) fn of_call(events: AnyEvents, mut call: CallEdge) -> Self {
        let px = call.cx();
        let (live, refused) = (px.live, px.refused);
        let events = cut(events, on_close(&px.ended));
        let output = px.format.kind().1;
        let feed = Feed::Call {
            events,
            call: call.share(),
            live,
            refused,
        };
        Self::new(feed, output)
    }
}

/// Where a response's outputs come from: a provider's call, or a plugin.
pub(crate) enum Feed {
    Call {
        events: AnyEvents,
        call: SharedCall,
        /// Whether the call carries live inputs.
        live: bool,
        /// Whether the provider refused it at its head.
        refused: bool,
    },
    /// Outputs a plugin made.
    Made(BoxStream<'static, Batch>),
}

impl Feed {
    /// Outputs made of `events`: each a success, the first `Err` the end.
    pub(crate) fn made(events: AnyEvents) -> Self {
        let batches = events.map(|value| match value {
            Ok(value) => Batch {
                values: Some(value.batch()),
                ..Batch::default()
            },
            Err(fault) => Batch::ending(Err(fault)),
        });
        let done = stream::once(async { Batch::ending(Ok(())) });
        Feed::Made(batches.chain(done).boxed())
    }

    /// Its call, if any.
    pub(crate) fn call(&self) -> Option<&SharedCall> {
        match self {
            Feed::Call { call, .. } => Some(call),
            Feed::Made(..) => None,
        }
    }

    pub(crate) fn is_call(&self) -> bool {
        self.call().is_some()
    }

    /// The next batch, eventually one with the end, not to be read past,
    /// filtered on its call's provider edge if it has one. The call ends when the batch
    /// with its end is read ([`Batch::read`]) or dropped. A panic of the
    /// provider's stream or a filter ends the call internal
    /// and unwinds on.
    pub(crate) async fn next(&mut self) -> Batch {
        let (events, call) = match self {
            Feed::Call { events, call, .. } => (events, call),
            Feed::Made(made) => {
                let truncated =
                    || Batch::ending(Err(Fault::internal("a made feed was read past its end")));
                let batch = made.next().await.unwrap_or_else(truncated);
                return batch;
            }
        };
        let item = AssertUnwindSafe(events.next()).catch_unwind().await;
        let read = item.map(|item| {
            let mut batch = match item {
                Some(Ok(value)) => Batch {
                    values: Some(value.batch()),
                    ..Batch::default()
                },
                Some(Err(fault)) => Batch::ending(Err(fault)),
                None => Batch::ending(Ok(())),
            };
            let of = AssertUnwindSafe(|| {
                // Coalesce ready events, with provider filters seeing the
                // batch before client filters. A hot stream must still yield.
                let mut count = 1;
                while let Some(values) = &mut batch.values
                    && batch.ending.is_none()
                    && count < MAX_BATCH
                {
                    let end = match events.next().now_or_never() {
                        None => break,
                        Some(Some(Ok(more))) => match values.push(more) {
                            Ok(()) => {
                                count += 1;
                                continue;
                            }
                            Err(fault) => Err(fault),
                        },
                        Some(Some(Err(fault))) => Err(fault),
                        Some(None) => Ok(()),
                    };
                    batch.ending = Some(Ending::new(end));
                }
                batch = std::mem::take(&mut batch).filter_provider_outputs(call);
            });
            std::panic::catch_unwind(of).map(|()| batch)
        });
        match read {
            Ok(Ok(batch)) => batch,
            Ok(Err(panic)) | Err(panic) => {
                locked(call).finish(Err(Fault::internal(PANICKED)));
                std::panic::resume_unwind(panic)
            }
        }
    }

    /// Its reader stopped with `end`; its call ends so.
    pub(crate) fn finish(&mut self, end: Result<()>) {
        if let Some(call) = self.call() {
            locked(call).finish(end);
        }
    }
}
