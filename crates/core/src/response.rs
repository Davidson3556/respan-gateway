use std::any::{Any, TypeId};
use std::collections::VecDeque;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use futures::StreamExt;
use futures::stream::{self, BoxStream, Stream};

use crate::call::{CallEdge, SharedCall};
use crate::fault::PANICKED;
use crate::format::{AnyDecoder, AnyFormat, Values};
use crate::input::locked;
use crate::transport::Head;
use crate::{Body, Exchange, Fault};

/// A response for the client, from a provider or a plugin. Dropped unsent
/// and unread, its call ends abandoned.
#[must_use = "a response does nothing unless sent or read"]
pub struct Response {
    /// The provider's head, handed to the client's format only if the
    /// response is in it ([`Source::relayed`]); none for one made.
    pub(crate) head: Option<Head>,
    pub(crate) body: Source,
    /// Whether its call streams the request's inputs, so its body flows
    /// only once it is sent.
    pub(crate) streams: bool,
    /// What [`Response::until_content`] or [`Response::until_end`] read,
    /// to be read again first.
    pub(crate) ahead: VecDeque<Batch>,
    /// Its outputs' type; `None` for a refusal, which has none.
    pub(crate) output: Option<TypeId>,
}

impl std::fmt::Debug for Response {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Response")
            .field("relayed", &self.body.relayed())
            .finish_non_exhaustive()
    }
}

/// Outputs decoded or made, and the end if it came.
#[derive(Default)]
pub(crate) struct Batch {
    pub(crate) values: Option<Values>,
    pub(crate) end: Option<Result<(), Fault>>,
    /// The call whose end this holds, ended once it is read.
    pub(crate) unread: Unread,
}

/// A call's end decoded and not yet read; dropped unread, the call ends so.
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

    /// Decoded from `call`'s response: filtered; its end, if any, waits to
    /// be read.
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
    fn new(head: Option<Head>, body: Source, output: Option<TypeId>) -> Self {
        Self {
            head,
            body,
            streams: false,
            ahead: VecDeque::new(),
            output,
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
    /// answer, which a refusal sends as it came.
    pub async fn until_content(self) -> Result<Response, Fault> {
        self.read_until(false).await
    }

    /// Read it to its end and hold it all, a later failure too, such as to
    /// race calls for completion; sent, it goes as it would have unread.
    /// Otherwise as [`until_content`](Response::until_content).
    pub async fn until_end(self) -> Result<Response, Fault> {
        self.read_until(true).await
    }

    /// Read until its first content, or its end if `to_end`, into `ahead`.
    async fn read_until(mut self, to_end: bool) -> Result<Response, Fault> {
        // What is ahead was judged by an earlier read.
        let mut content = self.streams || !self.ahead.is_empty();
        loop {
            match self.end() {
                Some(Err(fault)) if !content => return Err(fault.clone()),
                Some(_) => return Ok(self),
                None if self.streams || (content && !to_end) => return Ok(self),
                None => {
                    let (batch, read) = self.body.next().await;
                    content |= read;
                    if !batch.is_empty() {
                        self.ahead.push_back(batch);
                    }
                }
            }
        }
    }

    /// How the response ended, once it has been read to its end
    /// ([`Response::until_end`]); `None` before that.
    pub fn end(&self) -> Option<&Result<(), Fault>> {
        self.ahead.iter().find_map(|batch| batch.end.as_ref())
    }

    /// Read it rather than send it, such as a judge's verdict: every output
    /// as `T`, as the provider edge's filters left them, then an `Err` if it
    /// fails; one `Err` with [`codes::INTERNAL`](crate::codes::INTERNAL)
    /// for another type. Its call ends with the stream, or abandoned if
    /// dropped first.
    pub fn outputs<T: Any + Clone + Send>(mut self) -> BoxStream<'static, Result<T, Fault>> {
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

    /// A kernel-made response that just ends, such as a refusal.
    pub(crate) fn made(end: Result<(), Fault>) -> Self {
        let body = Source::Made(stream::iter([Batch::end(end)]).boxed());
        Self::new(None, body, None)
    }

    /// A response of bytes in `format` to the call with head `call` and
    /// first bytes `sent`, `relayed` if in the client's format, whose body
    /// flows only once sent if `streams`.
    pub(crate) fn decoded(
        format: &Arc<dyn AnyFormat>,
        call: &dyn Any,
        sent: &bytes::Bytes,
        response: Exchange<Head>,
        relayed: bool,
        streams: bool,
    ) -> Self {
        let Exchange { head, body, .. } = response;
        let decoder = format.response_decoder(&*head, call, sent);
        let output = Some(format.kind().1);
        let body = Source::Bytes {
            body,
            decoder,
            format: format.clone(),
            relayed,
            call: None,
        };
        Self {
            streams,
            ..Self::new(Some(head), body, output)
        }
    }

    /// The same, read through `call`'s provider edge; its body is cut off at
    /// the call's end, so a waiting reader wakes.
    pub(crate) fn of_call(mut self, mut call: CallEdge) -> Self {
        if let Source::Bytes {
            body, call: slot, ..
        } = &mut self.body
        {
            let cut = call.cx().cut.idle();
            let taken = std::mem::replace(body, stream::empty().boxed());
            *body = taken.take_until(cut).boxed();
            *slot = Some(call.share());
        }
        self
    }
}

/// A response's body: bytes with their decoder and call, or outputs made.
pub(crate) enum Source {
    Bytes {
        body: Body,
        decoder: Box<dyn AnyDecoder>,
        format: Arc<dyn AnyFormat>,
        relayed: bool,
        call: Option<SharedCall>,
    },
    /// Outputs a plugin made, each of which is content.
    Made(BoxStream<'static, Batch>),
}

impl Source {
    /// Whether it is in the client's format, so its head goes to it.
    pub(crate) fn relayed(&self) -> bool {
        matches!(self, Source::Bytes { relayed: true, .. })
    }

    /// Its call, if any.
    pub(crate) fn call(&self) -> Option<&SharedCall> {
        match self {
            Source::Bytes { call, .. } => call.as_ref(),
            Source::Made(..) => None,
        }
    }

    /// The next batch, eventually one with the end, not to be read past,
    /// and whether it held content before any filter, filtered on its
    /// call's provider edge if it has one. The call ends when the batch with its end is read ([`Batch::read`]) or
    /// dropped. A panic ends the call internal.
    pub(crate) async fn next(&mut self) -> (Batch, bool) {
        let (body, decoder, format, call) = match self {
            Source::Bytes {
                body,
                decoder,
                format,
                call,
                ..
            } => (body, decoder, format, call),
            Source::Made(made) => {
                let truncated = || Batch::end(Err(Fault::truncated()));
                let batch = made.next().await.unwrap_or_else(truncated);
                let content = batch.values.is_some();
                return (batch, content);
            }
        };
        let frame = body.next().await;
        let mut batch = Batch::default();
        let mut content = false;
        let decode = AssertUnwindSafe(|| {
            let (values, end) = match frame {
                Some(Ok(bytes)) => decoder.decode(Some(bytes)),
                Some(Err(f)) => (None, Some(Err(f))),
                None => decoder.decode(None),
            };
            content = values.as_ref().is_some_and(|v| format.any_content(v));
            batch.values = values;
            batch.end = end;
            if let Some(call) = call {
                batch = std::mem::take(&mut batch).of(call);
            }
        });
        if let Err(panic) = std::panic::catch_unwind(decode) {
            if let Some(call) = call {
                locked(call).finish(Err(Fault::internal(PANICKED)));
            }
            std::panic::resume_unwind(panic);
        }
        (batch, content)
    }

    /// Its reader stops before reading on: a call's decoder is told the
    /// body ended, and the call ends `Ok` if it says the body is complete,
    /// else abandoned, as a body was left. A panic ends the call internal.
    pub(crate) fn close_unread(&mut self) {
        let Source::Bytes { decoder, call, .. } = self else {
            return;
        };
        let close = AssertUnwindSafe(|| {
            let (values, end) = decoder.decode(None);
            let end = Some(match end {
                Some(Err(_)) => Err(Fault::abandoned()),
                _ => Ok(()),
            });
            if let Some(call) = call {
                Batch {
                    values,
                    end,
                    ..Batch::default()
                }
                .of(call)
                .read();
            }
        });
        if std::panic::catch_unwind(close).is_err() {
            self.end(Err(Fault::internal(PANICKED)));
        }
    }

    /// Its reader stopped with `end`; its call ends so.
    pub(crate) fn end(&mut self, end: Result<(), Fault>) {
        if let Some(call) = self.call() {
            locked(call).finish(end);
        }
    }
}
