use std::any::{Any, TypeId};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use futures::StreamExt;
use futures::stream::{self, BoxStream, Stream};
use http::StatusCode;
use http_body_util::{BodyExt, BodyStream, StreamBody};

use crate::call::{CallEdge, SharedCall};
use crate::fault::PANICKED;
use crate::format::{AnyDecoder, AnyFormat, Request, Values};
use crate::input::locked;
use crate::{Body, Fault, Usage};

/// A response for the client, streaming from a provider or made by a
/// plugin. Dropped before it is sent or read to its end, its call ends
/// abandoned.
#[must_use = "a response does nothing unless sent or read"]
pub struct Response {
    pub(crate) head: http::response::Parts,
    pub(crate) body: Source,
    /// What [`Response::ready`] read, to be read again first.
    pub(crate) ahead: VecDeque<Batch>,
    /// Usage a plugin added, counted where it is sent or read.
    pub(crate) usage: Usage,
    /// Its outputs' type; `None` for a refusal, which has none.
    pub(crate) output: Option<TypeId>,
    /// Where usage no call reported counts when a plugin reads it.
    pub(crate) unreported: Option<Arc<Mutex<Usage>>>,
}

impl std::fmt::Debug for Response {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Response")
            .field("status", &self.head.status)
            .finish_non_exhaustive()
    }
}

tokio::task_local! {
    /// The client's format while its request's plugins answer, which judges
    /// what they make as delivery does.
    pub(crate) static CLIENT_FORMAT: Arc<dyn AnyFormat>;
}

/// Outputs decoded or made, the end if it came, and usage no call counted.
#[derive(Default)]
pub(crate) struct Batch {
    pub(crate) values: Option<Values>,
    pub(crate) end: Option<Result<(), Fault>>,
    pub(crate) usage: Usage,
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
        self.values.is_none() && self.end.is_none() && self.usage.is_empty()
    }
}

impl Response {
    fn new(head: http::response::Parts, body: Source, output: Option<TypeId>) -> Self {
        Self {
            head,
            body,
            ahead: VecDeque::new(),
            usage: Usage::default(),
            output,
            unreported: None,
        }
    }

    /// A 200 of outputs of the client's kind, streamed by `stream`, such as
    /// `stream::iter(outputs.map(Ok))` or a merge of calls'
    /// [`outputs`](Response::outputs); the first `Err` fails it. Its head
    /// goes out with its first byte, so a failure before any is refused, or
    /// at once if the client's format opens it
    /// ([`Encoder::open`](crate::Encoder::open)). Outputs of another type
    /// fail it with [`codes::INTERNAL`](crate::codes::INTERNAL) when sent.
    /// The usage its outputs report is shown, never counted
    /// ([`Response::with_usage`]). A plugin's error is a [`Fault`], not
    /// outputs under an error status.
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
        let format = CLIENT_FORMAT.try_with(Arc::clone).ok();
        let format = format.filter(|f| f.kind().1 == TypeId::of::<T>());
        let body = Source::Made(batches.chain(done).boxed(), format);
        Self::new(head(StatusCode::OK), body, Some(TypeId::of::<T>()))
    }

    /// Count `usage` no call reported, such as a cached response's; it is
    /// not written, so put it in the outputs to show it. It counts on the
    /// client edge of the request it is sent to, or, read through
    /// [`Response::outputs`], of the one it came from, if any.
    pub fn with_usage(mut self, usage: Usage) -> Self {
        self.usage += &usage;
        self
    }

    /// Send it with `status`, which the client's format writes for.
    pub fn with_status(mut self, status: StatusCode) -> Self {
        self.head.status = status;
        self
    }

    /// Read it to its first content or its end, keeping what was read for
    /// whoever reads or sends it next, such as to fall back or race. Content
    /// is an output that says something
    /// ([`Format::says`](crate::Format::says)): a provider's to its format,
    /// before any filter; one a plugin made in `on_client` to the client's,
    /// as delivery judges it. A bodiless response, a 101 or one already ready is ready
    /// at once. `Err` with its fault if a success fails first; an error
    /// status is the provider's answer, to send as it came.
    pub async fn ready(mut self) -> Result<Response, Fault> {
        // What is ahead was judged by an earlier `ready`.
        let switched = self.head.status == StatusCode::SWITCHING_PROTOCOLS;
        let mut content = switched || !self.ahead.is_empty();
        loop {
            let end = self.ahead.iter().find_map(|batch| batch.end.as_ref());
            match end {
                Some(Err(fault)) if !content && self.head.status.is_success() => {
                    return Err(fault.clone());
                }
                Some(_) => return Ok(self),
                None if content => return Ok(self),
                None => {
                    let (batch, read) = self.body.next().await;
                    content = read;
                    if !batch.is_empty() {
                        self.ahead.push_back(batch);
                    }
                }
            }
        }
    }

    /// Read it rather than send it, such as a judge's verdict: its outputs
    /// of `T`, the kind's output type, that say something, as the provider
    /// edge's filters left them, ending with an `Err` if it fails; at once
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
                let given = std::mem::take(&mut resp.usage);
                resp.unreported(&given);
                let batch = match resp.ahead.pop_front() {
                    Some(batch) => batch,
                    None => resp.body.next().await.0,
                };
                resp.unreported(&batch.usage);
                if let Some(values) = batch.values {
                    let read = values.downcast::<Vec<T>>().map(|v| *v);
                    let read = read.unwrap_or_default().into_iter();
                    let says = |v: &T| resp.body.says(v);
                    outputs.extend(read.filter(says).map(Ok));
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

    /// Count `usage` no call reported on the client edge it came from.
    fn unreported(&self, usage: &Usage) {
        if let Some(to) = self.unreported.as_ref().filter(|_| !usage.is_empty()) {
            *locked(to) += usage;
        }
    }

    /// A kernel-made response that just ends, such as a refusal.
    pub(crate) fn made(status: StatusCode, end: Result<(), Fault>) -> Self {
        let body = Source::Made(stream::iter([Batch::end(end)]).boxed(), None);
        Self::new(head(status), body, None)
    }

    /// A response of bytes in `format` to `request`, `relayed` if in the
    /// client's format. One with no body is at its end already.
    pub(crate) fn decoded(
        format: &Arc<dyn AnyFormat>,
        request: &Request,
        response: http::Response<Body>,
        relayed: bool,
    ) -> Self {
        let (head, body) = response.into_parts();
        let decoder = format.response_decoder(&head, request);
        let output = Some(format.kind().1);
        let to_head = request.method() == http::Method::HEAD;
        let bodiless = bodiless(head.status, to_head);
        let body = Source::Bytes {
            body,
            decoder,
            format: format.clone(),
            relayed,
            call: None,
        };
        Self {
            ahead: bodiless.then(|| Batch::end(Ok(()))).into_iter().collect(),
            ..Self::new(head, body, output)
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
            let taken = std::mem::take(body);
            *body = StreamBody::new(BodyStream::new(taken).take_until(cut)).boxed_unsync();
            self.unreported = Some(call.cx().client.unreported.clone());
            let call = call.share();
            // A bodiless response's end is decoded already.
            for batch in &mut self.ahead {
                locked(&call).events(batch);
            }
            *slot = Some(call);
        }
        self
    }

    /// Its status and headers, such as a provider's error status.
    pub fn head(&self) -> &http::response::Parts {
        &self.head
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
    /// Outputs a plugin made, and the client's format, which judges them.
    Made(BoxStream<'static, Batch>, Option<Arc<dyn AnyFormat>>),
}

impl Source {
    /// Whether it is in the client's format, so its head is relayed.
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

    /// Whether `output` says something: every output a plugin made does.
    fn says(&self, output: &dyn Any) -> bool {
        match self {
            Source::Bytes { format, .. } => format.says(output),
            Source::Made(..) => true,
        }
    }

    /// The next batch, eventually one with the end, not to be read past,
    /// and whether it held content before any filter. A call's usage counts
    /// on its provider edge; other usage comes back in the batch. A panic
    /// ends the call internal.
    pub(crate) async fn next(&mut self) -> (Batch, bool) {
        let (body, decoder, format, call) = match self {
            Source::Bytes {
                body,
                decoder,
                format,
                call,
                ..
            } => (body, decoder, format, call),
            Source::Made(made, format) => {
                let truncated = || Batch::end(Err(Fault::truncated()));
                let batch = made.next().await.unwrap_or_else(truncated);
                let says = |v: &Values| format.as_ref().is_none_or(|f| f.read(v).1);
                let content = batch.values.as_ref().is_some_and(says);
                return (batch, content);
            }
        };
        let frame = body.frame().await;
        let mut batch = Batch::default();
        let mut content = false;
        let decode = std::panic::AssertUnwindSafe(|| {
            let (values, end) = match frame {
                // Trailers are not decoded.
                Some(Ok(frame)) => match frame.into_data() {
                    Ok(bytes) => decoder.decode(Some(bytes)),
                    Err(_) => (None, None),
                },
                Some(Err(f)) => (None, Some(Err(f))),
                None => decoder.decode(None),
            };
            if let Some(values) = &values {
                (batch.usage, content) = format.read(values);
            }
            batch.values = values;
            batch.end = end;
            if let Some(call) = call {
                locked(call).events(&mut batch);
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

    /// Its reader stopped with `end`; its call ends so.
    pub(crate) fn end(&mut self, end: Result<(), Fault>) {
        if let Some(call) = self.call() {
            locked(call).finish(end);
        }
    }
}

/// Whether a response with `status`, to a `HEAD` if `to_head`, has no body
/// (RFC 9110 9.3.2, 15.3.5, 15.3.6, 15.4.5).
pub(crate) fn bodiless(status: StatusCode, to_head: bool) -> bool {
    to_head || matches!(status.as_u16(), 204 | 205 | 304)
}

/// A response head with `status` and nothing else.
pub(crate) fn head(status: StatusCode) -> http::response::Parts {
    let (mut head, ()) = http::Response::new(()).into_parts();
    head.status = status;
    head
}
