use std::any::{Any, TypeId};
use std::collections::VecDeque;
use std::sync::Arc;

use futures::StreamExt;
use futures::stream::{self, BoxStream, Stream};

use crate::call::{CallEdge, SharedCall};
use crate::fault::PANICKED;
use crate::format::{AnyDecoder, AnyFormat, Values};
use crate::input::locked;
use crate::transport::Head;
use crate::{Body, Exchange, Fault};

/// A response for the client, streaming from a provider or made by a
/// plugin. Dropped before it is sent or read to its end, its call ends
/// abandoned.
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
            let mut batch = Batch::end(end);
            let settle = std::panic::AssertUnwindSafe(|| locked(&call).settle(&mut batch));
            let _ = std::panic::catch_unwind(settle);
            locked(&call).finish(Err(Fault::internal(PANICKED)));
        }
    }
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
        let Some((call, _)) = self.unread.0.take() else {
            return;
        };
        let settle = std::panic::AssertUnwindSafe(|| locked(&call).settle(self));
        if let Err(panic) = std::panic::catch_unwind(settle) {
            locked(&call).finish(Err(Fault::internal(PANICKED)));
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

    /// A success of outputs of the client's kind, streamed by `stream`, such as
    /// `stream::iter(outputs.map(Ok))` or a merge of calls'
    /// [`outputs`](Response::outputs); the first `Err` fails it. Its head
    /// goes out with its first byte, so a failure before any is refused, or
    /// at once if the client's format opens it
    /// ([`Encoder::open`](crate::Encoder::open)). Outputs of another type
    /// fail it with [`codes::INTERNAL`](crate::codes::INTERNAL) when sent.
    /// It passes no provider edge: to bill a cached answer, carry what it
    /// cost in an extension. A plugin's error is a [`Fault`], not outputs.
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

    /// Read it until its first content, a failure, or its end, and hold
    /// what was read, so it is still unsent and a plugin can decide on it
    /// or fall back. Content is a provider's output its format calls
    /// content ([`Format::is_content`](crate::Format::is_content)), before
    /// any filter, or any output a plugin made. One whose call streams the
    /// request's inputs, or read this far already, returns at once. `Err`
    /// with its fault if it fails first, such as a provider's error answer
    /// ([`Fault::is_answer`]), which a refusal sends as it came.
    pub async fn until_content(self) -> Result<Response, Fault> {
        self.read_until(false).await
    }

    /// Read it to its end and hold all of it: its head as it came, every
    /// output, and its end, `Ok` or a failure, such as to race calls for
    /// completion. Sent, it is delivered as it would have been unread,
    /// byte for byte when relayed unedited. Its call ends as the batch
    /// holding its end is sent, read or dropped. One whose call streams the
    /// request's inputs, whose body flows only once sent, returns at once.
    /// `Err` as for [`until_content`](Response::until_content): a failure
    /// before any content; a later failure is held.
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

    /// Read it rather than send it, such as a judge's verdict: its outputs
    /// of `T`, the kind's output type, as the provider edge's filters left
    /// them, content or not (filter with the kind's own check), ending with an `Err` if it fails; at once
    /// with [`codes::INTERNAL`](crate::codes::INTERNAL) for another type.
    /// Its call ends with the stream, or abandoned if dropped first.
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
        (call, sent): (&(dyn Any + Send + Sync), &bytes::Bytes),
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
        let decode = std::panic::AssertUnwindSafe(|| {
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
        let close = std::panic::AssertUnwindSafe(|| {
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
