use crate::Result;
use std::any::{Any, TypeId};
use std::collections::VecDeque;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use futures::stream::{self, BoxStream, Stream};
use futures::{FutureExt, StreamExt};

use crate::Fault;
use crate::call::{CallEdge, SharedCall};
use crate::events::Metadata;
use crate::fault::PANICKED;
use crate::format::{AnyEvents, AnyFormat, Values, erase};
use crate::leg::{cut, idle, locked, run, yield_ready};

/// Bound ready-event coalescing, including reads attempted during drop.
const MAX_BATCH_EVENTS: usize = 128;

/// A response for the client, from a provider or a plugin. Dropped unsent
/// and unread, its call ends abandoned.
#[must_use = "a response does nothing unless sent or read"]
pub struct Response {
    /// Format-owned relay metadata, absent on translated or synthetic outputs.
    pub(crate) metadata: Metadata,
    pub(crate) body: Source,
    /// Whether its call streams inputs, so its events flow only once it is
    /// sent.
    pub(crate) streams: bool,
    /// What [`Response::buffer_until`] or [`Response::until_end`] read,
    /// to be read again first.
    pub(crate) ahead: VecDeque<Batch>,
    /// Its outputs' type; `None` for a refusal, which has none.
    pub(crate) output: Option<TypeId>,
    /// Whether the provider refused it at its head: its one batch is the
    /// refusal.
    pub(crate) refused: bool,
}

impl std::fmt::Debug for Response {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Response")
            .field("relayed", &self.metadata.is_some())
            .finish_non_exhaustive()
    }
}

/// Outputs read or made, optionally carrying their terminal result.
#[derive(Default)]
pub(crate) struct Batch {
    pub(crate) values: Option<Values>,
    pub(crate) end: Option<Completion>,
}

/// A terminal result and the obligation to finish its provider call.
/// Buffering finalizes filters but keeps this obligation. Reading or dropping
/// the completion settles it; request cancellation may settle the call first.
pub(crate) struct Completion {
    outcome: Result<()>,
    call: Option<SharedCall>,
}

impl Completion {
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

impl Drop for Completion {
    fn drop(&mut self) {
        // Discard final filter values, but still settle the provider call.
        // Drop must not propagate a user filter's panic.
        let _ = self.settle(&mut None);
    }
}

impl Batch {
    pub(crate) fn end(end: Result<()>) -> Self {
        Self {
            end: Some(Completion::new(end)),
            ..Self::default()
        }
    }

    fn is_empty(&self) -> bool {
        self.values.is_none() && self.end.is_none()
    }

    fn prepare(&mut self) {
        if let Some(end) = &mut self.end {
            end.prepare(&mut self.values);
        }
    }

    /// Reading a terminal batch settles its call before exposing its values.
    pub(crate) fn read(&mut self) {
        if let Some(end) = &mut self.end
            && let Err(panic) = end.settle(&mut self.values)
        {
            std::panic::resume_unwind(panic);
        }
    }

    /// Apply provider filters and attach call ownership to the terminal result.
    fn filter_provider_outputs(mut self, call: &SharedCall) -> Self {
        let mut edge = locked(call);
        if edge.is_finished() {
            return Self::end(Err(Fault::abandoned()));
        }
        if let Some(values) = &mut self.values
            && let Err(fault) = run(&[&edge.cx().outputs], values)
        {
            self.end = Some(Completion::new(Err(fault)));
        }
        if let Some(end) = &mut self.end {
            end.call = Some(call.clone());
        }
        self
    }
}

impl Response {
    fn new(metadata: Metadata, body: Source, output: Option<TypeId>) -> Self {
        Self {
            metadata,
            body,
            streams: false,
            ahead: VecDeque::new(),
            output,
            refused: false,
        }
    }

    /// A success of the client's outputs from `stream`, such as a merge of
    /// calls' [`outputs`](Response::outputs); the first `Err` fails it, a
    /// refusal before any output. It passes no
    /// provider edge. Outputs of another type fail it with
    /// [`codes::INTERNAL`](crate::codes::INTERNAL) when sent.
    pub fn stream<T, S>(stream: S) -> Self
    where
        T: Any + Send + Sync,
        S: Stream<Item = Result<T>> + Send + 'static,
    {
        let body = Source::made(erase(stream.boxed()));
        Self::new(None, body, Some(TypeId::of::<T>()))
    }

    /// Buffer outputs until `predicate` matches, the stream ends, or it fails.
    /// Values are inspected after provider filters. Buffered values are replayed
    /// when sent or read, retaining relay metadata and call ownership.
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
        if self
            .output
            .is_some_and(|output| output != TypeId::of::<T>())
        {
            return Err(Fault::wrong_kind::<T>());
        }
        if self.streams && !self.refused {
            return Ok(());
        }
        let mut index = 0;
        loop {
            if index == self.ahead.len() {
                let batch = self.body.next().await;
                if batch.is_empty() {
                    continue;
                }
                self.ahead.push_back(batch);
            }
            let batch = &mut self.ahead[index];
            batch.prepare();
            let matched = std::panic::catch_unwind(AssertUnwindSafe(|| {
                batch
                    .values
                    .as_ref()
                    .and_then(|values| values.downcast_ref::<Vec<T>>())
                    .is_some_and(|values| values.iter().any(&mut predicate))
            }));
            match matched {
                Ok(true) => return Ok(()),
                Ok(false) => {}
                Err(panic) => {
                    self.body.end(Err(Fault::internal(PANICKED)));
                    std::panic::resume_unwind(panic);
                }
            }
            if let Some(end) = &batch.end {
                end.outcome().clone()?;
                return Ok(());
            }
            index += 1;
            // Yield only after ownership of the batch has moved into ahead.
            // Cancellation of this borrowed hold must never lose an event.
            if matches!(self.body, Source::EventStream { .. }) || index % MAX_BATCH_EVENTS == 0 {
                tokio::task::yield_now().await;
            }
        }
    }

    /// Buffer the entire response, retaining its metadata, buffered events,
    /// terminal result and call ownership even when it fails. Streaming-input
    /// calls return unread to avoid deadlocking duplex peers.
    pub async fn until_end(&mut self) -> Result<()> {
        if self.streams && !self.refused {
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
            let mut batch = self.body.next().await;
            batch.prepare();
            if !batch.is_empty() {
                self.ahead.push_back(batch);
                batches += 1;
                // Store the batch before yielding so cancellation can resume
                // inspection or replay without losing any provider events.
                if self.buffered_end().is_none()
                    && (matches!(self.body, Source::EventStream { .. })
                        || batches.is_multiple_of(MAX_BATCH_EVENTS))
                {
                    tokio::task::yield_now().await;
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
            .and_then(|batch| batch.end.as_ref())
            .map(Completion::outcome)
    }

    /// Read it rather than send it, such as a judge's verdict: every output
    /// as `T`, as the provider edge's filters left them, then an `Err` if it
    /// fails; one `Err` with [`codes::INTERNAL`](crate::codes::INTERNAL)
    /// for another type. Its call ends with the stream, or abandoned if
    /// dropped first.
    pub fn outputs<T: Any + Clone + Send>(mut self) -> BoxStream<'static, Result<T>> {
        if self.output != Some(TypeId::of::<T>()) {
            let fault = Fault::wrong_kind::<T>();
            self.body.end(Err(fault.clone()));
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
                        None => resp.body.next().await,
                    };
                    batch.read();
                    if let Some(values) = batch.values {
                        let read = values.downcast::<Vec<T>>().map(|v| *v);
                        outputs.extend(read.unwrap_or_default().into_iter().map(Ok));
                    }
                    if let Some(end) = batch.end {
                        resp.body.end(end.outcome().clone());
                        outputs.extend(end.outcome().clone().err().map(Err));
                        ended = true;
                    }
                    batches += 1;
                    // A batch has been read into the owned unfold state. Yield
                    // even if its filters emitted no values, so an indefinitely
                    // ready provider cannot monopolize one poll.
                    if !ended
                        && (matches!(resp.body, Source::EventStream { .. })
                            || batches.is_multiple_of(MAX_BATCH_EVENTS))
                    {
                        yield_ready().await;
                    }
                }
            },
        )
        .boxed()
    }

    /// A provider's output flow through its call, with metadata retained
    /// only on a relay: cut at the call's end, so a waiting reader wakes.
    pub(crate) fn of_call(
        format: &Arc<dyn AnyFormat>,
        metadata: Metadata,
        events: AnyEvents,
        mut call: CallEdge,
    ) -> Self {
        let px = call.cx();
        let (streams, refused) = (px.flowing, px.refused);
        let events = cut(events, idle(&px.cut));
        let output = Some(format.kind().1);
        let body = Source::EventStream {
            events,
            format: format.clone(),
            call: call.share(),
        };
        Self {
            streams,
            refused,
            ..Self::new(metadata, body, output)
        }
    }
}

/// A response's body: a provider's events and its call, or outputs made.
pub(crate) enum Source {
    EventStream {
        events: AnyEvents,
        format: Arc<dyn AnyFormat>,
        call: SharedCall,
    },
    /// Outputs a plugin made.
    Made(BoxStream<'static, Batch>),
}

impl Source {
    /// Outputs made of `events`: each a success, the first `Err` the end.
    pub(crate) fn made(events: AnyEvents) -> Self {
        let batches = events.map(|values| match values {
            Ok(values) => Batch {
                values: Some(values),
                ..Batch::default()
            },
            Err(fault) => Batch::end(Err(fault)),
        });
        let done = stream::once(async { Batch::end(Ok(())) });
        Source::Made(batches.chain(done).boxed())
    }

    /// Its call, if any.
    pub(crate) fn call(&self) -> Option<&SharedCall> {
        match self {
            Source::EventStream { call, .. } => Some(call),
            Source::Made(..) => None,
        }
    }

    /// The next batch, eventually one with the end, not to be read past,
    /// filtered on its call's provider edge if it has one. The call ends when the batch
    /// with its end is read ([`Batch::read`]) or dropped. A panic of the
    /// provider's stream or a filter ends the call internal
    /// and unwinds on.
    pub(crate) async fn next(&mut self) -> Batch {
        let (events, format, call) = match self {
            Source::EventStream {
                events,
                format,
                call,
            } => (events, format, call),
            Source::Made(made) => {
                let truncated = || Batch::end(Err(Fault::truncated()));
                let batch = made.next().await.unwrap_or_else(truncated);
                return batch;
            }
        };
        let item = AssertUnwindSafe(events.next()).catch_unwind().await;
        let read = item.map(|item| {
            let mut batch = match item {
                Some(Ok(values)) => Batch {
                    values: Some(values),
                    ..Batch::default()
                },
                Some(Err(fault)) => Batch::end(Err(fault)),
                None => Batch::end(Ok(())),
            };
            let of = AssertUnwindSafe(|| {
                // Coalesce ready events, with provider filters seeing the
                // batch before client filters. A hot stream must still yield.
                let mut count = 1;
                while let Some(values) = &mut batch.values
                    && batch.end.is_none()
                    && count < MAX_BATCH_EVENTS
                {
                    match events.next().now_or_never() {
                        None => break,
                        Some(Some(Ok(more))) => {
                            format.append(values, more);
                            count += 1;
                        }
                        Some(Some(Err(fault))) => batch.end = Some(Completion::new(Err(fault))),
                        Some(None) => batch.end = Some(Completion::new(Ok(()))),
                    }
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
    pub(crate) fn end(&mut self, end: Result<()>) {
        if let Some(call) = self.call() {
            locked(call).finish(end);
        }
    }
}
