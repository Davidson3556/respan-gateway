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
use crate::leg::{cut, idle, locked};

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

/// Outputs read or made, and the end if it came.
#[derive(Default)]
pub(crate) struct Batch {
    pub(crate) values: Option<Values>,
    pub(crate) end: Option<Result<()>>,
    /// The call whose end this holds, ended once it is read.
    pub(crate) unread: Unread,
}

/// A call's end read from its provider and not yet by its reader; dropped
/// unread, the call ends so.
#[derive(Default)]
pub(crate) struct Unread(Option<(SharedCall, Result<()>)>);

impl Drop for Unread {
    fn drop(&mut self) {
        if let Some((call, end)) = self.0.take() {
            let _ = settle(&call, &mut Batch::end(end));
        }
    }
}

/// Settle `call` at the end `batch` holds; a panic ends it internal.
fn settle(call: &SharedCall, batch: &mut Batch) -> std::thread::Result<()> {
    let settled = std::panic::catch_unwind(AssertUnwindSafe(|| locked(call).settle(batch)));
    if settled.is_err() {
        locked(call).finish(Err(Fault::internal(PANICKED)));
    }
    settled
}

impl Batch {
    /// Just an end.
    pub(crate) fn end(end: Result<()>) -> Self {
        Self {
            end: Some(end),
            ..Self::default()
        }
    }

    fn is_empty(&self) -> bool {
        self.values.is_none() && self.end.is_none()
    }

    /// Include final filter output and failure in a buffered end, while
    /// leaving the call's end hooks deferred until replay or abandonment.
    fn prepare(&mut self) {
        let Some((call, _)) = self.unread.0.take() else {
            return;
        };
        let prepared = std::panic::catch_unwind(AssertUnwindSafe(|| {
            locked(&call).finish_outputs(self);
        }));
        if let Err(panic) = prepared {
            locked(&call).finish(Err(Fault::internal(PANICKED)));
            std::panic::resume_unwind(panic);
        }
        if let Some(end) = &self.end {
            self.unread = Unread(Some((call, end.clone())));
        }
    }

    /// Read by whoever reads or sends the response: the end of its call, if
    /// it holds it, goes through the call's filters and ends the call. A
    /// panic ends the call internal.
    pub(crate) fn read(&mut self) {
        if let Some((call, _)) = self.unread.0.take()
            && let Err(panic) = settle(&call, self)
        {
            std::panic::resume_unwind(panic);
        }
    }

    /// Read from `call`'s provider: filtered; its end, if any, waits to be
    /// read.
    fn of(mut self, call: &SharedCall) -> Self {
        if locked(call).events(&mut self)
            && let Some(end) = &self.end
        {
            self.unread = Unread(Some((call.clone(), end.clone())));
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
    /// An error before a match is returned; an error after a match stays buffered.
    /// A clean end returns the response even if no value matched.
    ///
    /// Streaming-input calls return unread to avoid deadlocking duplex peers.
    /// Bound the wait and buffered size in the calling policy if needed.
    /// A repeated call checks buffered values against its new predicate.
    pub async fn buffer_until<T: Any + Send + Sync>(
        mut self,
        mut predicate: impl FnMut(&T) -> bool + Send,
    ) -> Result<Self> {
        if self
            .output
            .is_some_and(|output| output != TypeId::of::<T>())
        {
            return Err(Fault::wrong_kind::<T>());
        }
        if self.streams && !self.refused {
            return Ok(self);
        }
        let mut index = 0;
        loop {
            if index == self.ahead.len() {
                let batch = self.body.next().await.0;
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
                Ok(true) => return Ok(self),
                Ok(false) => {}
                Err(panic) => {
                    self.body.end(Err(Fault::internal(PANICKED)));
                    std::panic::resume_unwind(panic);
                }
            }
            if let Some(end) = &batch.end {
                end.clone()?;
                return Ok(self);
            }
            index += 1;
        }
    }

    /// Buffer the entire response, retaining its metadata and eventual result.
    /// Errors before any output are returned directly; later errors are held
    /// for replay and also returned alongside the response. Streaming-input
    /// calls return unread to avoid deadlocking duplex peers.
    pub async fn until_end(mut self) -> Result<(Self, Result<()>)> {
        if self.streams && !self.refused {
            return Ok((self, Ok(())));
        }
        if let Some(batch) = self.ahead.back_mut() {
            batch.prepare();
        }
        let mut seen = self.ahead.iter().any(|batch| batch.values.is_some());
        let mut end = self.end().cloned();
        loop {
            if let Some(end) = end {
                if !seen {
                    end.clone()?;
                }
                return Ok((self, end));
            }
            let (mut batch, has_values) = self.body.next().await;
            batch.prepare();
            seen |= has_values || batch.values.is_some();
            end = batch.end.clone();
            if !batch.is_empty() {
                self.ahead.push_back(batch);
            }
        }
    }

    /// How it ended, if read to its end.
    fn end(&self) -> Option<&Result<()>> {
        self.ahead.iter().find_map(|batch| batch.end.as_ref())
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
        let read = (self, VecDeque::new(), false);
        stream::unfold(read, |(mut resp, mut outputs, mut ended)| async move {
            loop {
                if let Some(output) = outputs.pop_front() {
                    return Some((output, (resp, outputs, ended)));
                }
                if ended {
                    return None;
                }
                let mut batch = match resp.ahead.pop_front() {
                    Some(batch) => batch,
                    None => resp.body.next().await.0,
                };
                batch.read();
                if let Some(values) = batch.values {
                    let read = values.downcast::<Vec<T>>().map(|v| *v);
                    outputs.extend(read.unwrap_or_default().into_iter().map(Ok));
                }
                if let Some(end) = batch.end {
                    resp.body.end(end.clone());
                    outputs.extend(end.err().map(Err));
                    ended = true;
                }
            }
        })
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
        let body = Source::Events {
            events,
            format: format.clone(),
            call: Some(call.share()),
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
    Events {
        events: AnyEvents,
        format: Arc<dyn AnyFormat>,
        call: Option<SharedCall>,
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
        Source::Made(
            batches
                .chain(done)
                .enumerate()
                .then(|(index, batch)| async move {
                    // Synthetic streams may be continuously ready too. Let
                    // timeouts and cancellation run without delaying a terminal end.
                    if index % MAX_BATCH_EVENTS == MAX_BATCH_EVENTS - 1 && batch.end.is_none() {
                        tokio::task::yield_now().await;
                    }
                    batch
                })
                .boxed(),
        )
    }

    /// Its call, if any.
    pub(crate) fn call(&self) -> Option<&SharedCall> {
        match self {
            Source::Events { call, .. } => call.as_ref(),
            Source::Made(..) => None,
        }
    }

    /// The next batch, eventually one with the end, not to be read past,
    /// and whether it held values before any filter, filtered on its
    /// call's provider edge if it has one. The call ends when the batch
    /// with its end is read ([`Batch::read`]) or dropped. A panic of the
    /// provider's stream or a filter ends the call internal
    /// and unwinds on.
    pub(crate) async fn next(&mut self) -> (Batch, bool) {
        let (events, format, call) = match self {
            Source::Events {
                events,
                format,
                call,
            } => (events, format, call),
            Source::Made(made) => {
                let truncated = || Batch::end(Err(Fault::truncated()));
                let batch = made.next().await.unwrap_or_else(truncated);
                let has_values = batch.values.is_some();
                return (batch, has_values);
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
                        Some(Some(Err(fault))) => batch.end = Some(Err(fault)),
                        Some(None) => batch.end = Some(Ok(())),
                    }
                }
                let has_values = batch.values.is_some();
                if let Some(call) = call {
                    batch = std::mem::take(&mut batch).of(call);
                }
                (has_values, count == MAX_BATCH_EVENTS)
            });
            std::panic::catch_unwind(of).map(|(has_values, full)| (batch, has_values, full))
        });
        match read {
            Ok(Ok((batch, has_values, full))) => {
                // A terminal batch must reach its owner without suspension:
                // its unread end is settled according to delivery's outcome.
                if full && batch.end.is_none() {
                    tokio::task::yield_now().await;
                }
                (batch, has_values)
            }
            Ok(Err(panic)) | Err(panic) => {
                if let Some(call) = call {
                    locked(call).finish(Err(Fault::internal(PANICKED)));
                }
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
