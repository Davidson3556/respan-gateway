use std::any::{Any, TypeId};
use std::collections::VecDeque;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use futures::stream::{self, BoxStream, Stream};
use futures::{FutureExt, StreamExt};

use crate::call::{CallEdge, SharedCall};
use crate::fault::PANICKED;
use crate::format::{AnyEvents, AnyFormat, Origin, Values};
use crate::leg::{cut, locked};
use crate::{Events, Fault};

/// A response for the client, from a provider or a plugin. Dropped unsent
/// and unread, its call ends abandoned.
#[must_use = "a response does nothing unless sent or read"]
pub struct Response {
    /// The provider's origin, only on a relay; none for one made.
    pub(crate) origin: Option<Origin>,
    pub(crate) body: Source,
    /// Whether its call carries the session's inputs still flowing, so its
    /// events flow only once it is sent.
    pub(crate) streams: bool,
    /// What [`Response::until_content`] or [`Response::until_end`] read,
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
            .field("relayed", &self.origin.is_some())
            .finish_non_exhaustive()
    }
}

/// Outputs read or made, and the end if it came.
#[derive(Default)]
pub(crate) struct Batch {
    pub(crate) values: Option<Values>,
    pub(crate) end: Option<Result<(), Fault>>,
    /// The call whose end this holds, ended once it is read.
    pub(crate) unread: Unread,
}

/// A call's end read from its provider and not yet by its reader; dropped
/// unread, the call ends so.
#[derive(Default)]
pub(crate) struct Unread(Option<(SharedCall, Result<(), Fault>)>);

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
    pub(crate) fn end(end: Result<(), Fault>) -> Self {
        Self {
            end: Some(end),
            ..Self::default()
        }
    }

    fn is_empty(&self) -> bool {
        self.values.is_none() && self.end.is_none()
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
    fn new(origin: Option<Origin>, body: Source, output: Option<TypeId>) -> Self {
        Self {
            origin,
            body,
            streams: false,
            ahead: VecDeque::new(),
            output,
            refused: false,
        }
    }

    /// A success of the client's outputs from `stream`, such as a merge of
    /// calls' [`outputs`](Response::outputs); the first `Err` fails it, a
    /// refusal before any output. Every output is content. It passes no
    /// provider edge. Outputs of another type fail it with
    /// [`codes::INTERNAL`](crate::codes::INTERNAL) when sent.
    pub fn stream<T, S>(stream: S) -> Self
    where
        T: Any + Send + Sync,
        S: Stream<Item = Result<T, Fault>> + Send + 'static,
    {
        let batches = stream.map(|output| match output {
            Ok(output) => Batch {
                values: Some(Box::new(vec![output])),
                ..Batch::default()
            },
            Err(fault) => Batch::end(Err(fault)),
        });
        let done = stream::once(async { Batch::end(Ok(())) });
        let body = Source::Made(batches.chain(done).boxed());
        Self::new(None, body, Some(TypeId::of::<T>()))
    }

    /// Read it until its first content
    /// ([`Format::is_content`](crate::Format::is_content), before filters),
    /// a failure or its end, and hold what was read, unsent, to decide on
    /// it or fall back. One whose call streams the request's inputs returns
    /// at once. `Err` if it fails first, such as with a provider's error
    /// answer, which a refusal sends as it came; one the provider refused
    /// at its head fails at once.
    ///
    /// A response with no content is read, and held, to its end, so bound
    /// one that may never end. A decoder that panics ends its call
    /// [`codes::INTERNAL`](crate::codes::INTERNAL) and unwinds through this
    /// future, so a plugin's own task or a race that swallows the panic
    /// loses it.
    pub async fn until_content(self) -> Result<Response, Fault> {
        self.read_until(false).await
    }

    /// Read it to its end and hold it all, a later failure too, such as to
    /// race calls for completion; sent, it goes as it would have unread.
    /// Returned with how it ended, `Ok` for one whose call streams the
    /// request's inputs, unread. Otherwise as
    /// [`until_content`](Response::until_content), a panic included: the
    /// awaiting plugin unwinds, and the request is refused
    /// [`codes::INTERNAL`](crate::codes::INTERNAL) as any plugin panic is,
    /// where a response read as it is sent aborts after its first bytes.
    pub async fn until_end(self) -> Result<(Response, Result<(), Fault>), Fault> {
        let resp = self.read_until(true).await?;
        let end = resp.end().cloned().unwrap_or(Ok(()));
        Ok((resp, end))
    }

    /// Read until its first content, or its end if `to_end`, into `ahead`.
    async fn read_until(mut self, to_end: bool) -> Result<Response, Fault> {
        // What is ahead was judged by an earlier read; a refusal is read.
        let streams = self.streams && !self.refused;
        let mut content = streams || !self.ahead.is_empty();
        // Noted as a batch is read, not found again by scanning them all.
        let mut end = self.end().cloned();
        loop {
            match &end {
                Some(Err(fault)) if !content => return Err(fault.clone()),
                Some(_) => return Ok(self),
                None if streams || (content && !to_end) => return Ok(self),
                None => {
                    let (batch, read) = self.body.next().await;
                    content |= read;
                    end = batch.end.clone();
                    if !batch.is_empty() {
                        self.ahead.push_back(batch);
                    }
                }
            }
        }
    }

    /// How it ended, if read to its end.
    fn end(&self) -> Option<&Result<(), Fault>> {
        self.ahead.iter().find_map(|batch| batch.end.as_ref())
    }

    /// Read it rather than send it, such as a judge's verdict: every output
    /// as `T`, as the provider edge's filters left them, then an `Err` if it
    /// fails; one `Err` with [`codes::INTERNAL`](crate::codes::INTERNAL)
    /// for another type. Its call ends with the stream, or abandoned if
    /// dropped first.
    pub fn outputs<T: Any + Clone + Send>(mut self) -> Events<T> {
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

    /// A provider's answer through `call`'s provider edge, its origin kept
    /// only on a relay: cut at the call's end, so a waiting reader wakes.
    pub(crate) fn of_call(
        format: &Arc<dyn AnyFormat>,
        origin: Option<Origin>,
        events: AnyEvents,
        mut call: CallEdge,
    ) -> Self {
        let px = call.cx();
        let (streams, refused) = (px.flowing, px.refused);
        let events = cut(events, &px.cut);
        let output = Some(format.kind().1);
        let body = Source::Events {
            events,
            format: format.clone(),
            call: Some(call.share()),
        };
        Self {
            streams,
            refused,
            ..Self::new(origin, body, output)
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
    /// Outputs a plugin made, each of which is content.
    Made(BoxStream<'static, Batch>),
}

impl Source {
    /// Its call, if any.
    pub(crate) fn call(&self) -> Option<&SharedCall> {
        match self {
            Source::Events { call, .. } => call.as_ref(),
            Source::Made(..) => None,
        }
    }

    /// The next batch, eventually one with the end, not to be read past,
    /// and whether it held content before any filter, filtered on its
    /// call's provider edge if it has one. The call ends when the batch
    /// with its end is read ([`Batch::read`]) or dropped. A panic of the
    /// provider's stream, `is_content` or a filter ends the call internal
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
                let content = batch.values.is_some();
                return (batch, content);
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
                // What is ready with it is one batch, as a frame's outputs
                // are: the call's filters see all of it before the client's
                // see its first.
                while let Some(values) = &mut batch.values
                    && batch.end.is_none()
                {
                    match events.next().now_or_never() {
                        None => break,
                        Some(Some(Ok(more))) => format.append(values, more),
                        Some(Some(Err(fault))) => batch.end = Some(Err(fault)),
                        Some(None) => batch.end = Some(Ok(())),
                    }
                }
                let content = (batch.values.as_ref()).is_some_and(|v| format.any_content(v));
                if let Some(call) = call {
                    batch = std::mem::take(&mut batch).of(call);
                }
                content
            });
            std::panic::catch_unwind(of).map(|content| (batch, content))
        });
        match read {
            Ok(Ok(read)) => read,
            Ok(Err(panic)) | Err(panic) => {
                if let Some(call) = call {
                    locked(call).finish(Err(Fault::internal(PANICKED)));
                }
                std::panic::resume_unwind(panic)
            }
        }
    }

    /// Its reader stopped with `end`; its call ends so.
    pub(crate) fn end(&mut self, end: Result<(), Fault>) {
        if let Some(call) = self.call() {
            locked(call).finish(end);
        }
    }
}
