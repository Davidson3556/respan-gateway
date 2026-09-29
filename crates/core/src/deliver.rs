use std::any::Any;
use std::collections::VecDeque;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures::FutureExt;
use futures::stream::{self, StreamExt};

use crate::fault::{PANICKED, guarded};
use crate::format::{AnyEncoder, AnyFormat, Values};
use crate::lifecycle::EndHooks;
use crate::plugin::failed;
use crate::response::{Batch, Source};
use crate::transport::Head;
use crate::{ClientContext, Exchange, Fault, Response};

/// What `Gateway::handle` hands the adapter: the response, or a failure
/// no format can write.
pub(crate) type Handed = Result<Exchange<Head>, Fault>;

/// A refusal: a response that is `fault` alone, which the client's format
/// writes.
pub(crate) async fn refuse(
    format: &Arc<dyn AnyFormat>,
    fault: Fault,
    hooks: EndHooks<ClientContext>,
) -> Handed {
    let resp = Response::made(Err(fault.clone()));
    deliver(format, resp, Some(fault), hooks).await
}

/// Send `resp`, or the `refusal` it is, to the client. Its head goes out
/// with its body's first byte, so a response that fails before any is
/// refused instead, or at once for a streaming request; a refusal its
/// format cannot write goes back to the adapter.
pub(crate) async fn deliver(
    format: &Arc<dyn AnyFormat>,
    resp: Response,
    refusal: Option<Fault>,
    mut hooks: EndHooks<ClientContext>,
) -> Handed {
    let cx = hooks.cx();
    // A call's response goes only to its own request; one of another kind
    // fails, even with no outputs.
    let wrong = match resp.body.call() {
        Some(c) if !cx.open.holds(c) => {
            Some(Fault::internal("a response to another request was sent"))
        }
        _ => (resp.output.is_some_and(|o| o != format.kind().1))
            .then(|| Fault::another_kind("a response of another kind was sent".into())),
    };
    if let Some(fault) = wrong {
        drop(resp);
        return Box::pin(refuse(format, fault, hooks)).await;
    }
    let (ask, body) = (cx.head.clone(), cx.body().clone());
    let streaming = cx.later.is_some();
    let Response {
        head: answer,
        body: mut source,
        ahead,
        ..
    } = resp;
    // A head goes only to a format of the name it came in.
    let answer = answer.filter(|_| source.relayed());
    let refused = refusal.is_some();
    let build = || {
        let answer = answer.as_deref().map(|a| a as &dyn Any);
        let encoder = format.response_encoder(answer, &*ask, &body, refusal.as_ref());
        let mut encoder = encoder.ok_or_else(|| Fault::internal("a request of another wire"))?;
        let head = encoder.head(None);
        let mut opening = BytesMut::new();
        let opened = match refused {
            true => Ok(()),
            false => encoder.open(&mut opening),
        };
        opened.map(|()| (encoder, head, opening))
    };
    // A format that panics building its encoder cannot write a refusal.
    let (encoder, mut head, opening) = match std::panic::catch_unwind(AssertUnwindSafe(build)) {
        Ok(Ok(built)) => built,
        Ok(Err(fault)) if refused => return unwritable(hooks, fault),
        Ok(Err(fault)) => {
            source.end(Err(fault.clone()));
            return Box::pin(refuse(format, fault, hooks)).await;
        }
        Err(_) => return unwritable(hooks, Fault::internal(PANICKED)),
    };
    // A body done before it began reads nothing, and its head goes at once.
    let done = !refused && encoder.done();
    let mut pipe = Pipe {
        source,
        refusal: refused,
        ahead,
        encoder,
        hooks,
        out: Out {
            streaming,
            ..Out::default()
        },
        ended: None,
        abort: None,
    };
    pipe.out.push(opening);
    // A streaming request's client waits for the head before it sends.
    let first = match refused || !(streaming || done) {
        true => pipe.first(!refused).await,
        false => Ok(()),
    };
    // An edit that fails or panics fails the head before it goes out.
    let first = first.and_then(|()| {
        let edits = std::mem::take(&mut pipe.hooks.cx().edits().head);
        guarded(|| edits.into_iter().rev().try_for_each(|edit| edit(&mut head)))
    });
    match first {
        Ok(()) => {}
        Err(fault) if refused => return unwritable(pipe.hooks, fault),
        Err(fault) => {
            pipe.source.end(Err(fault.clone()));
            // A provider's words go only where its head would.
            let fault = match pipe.source.relayed() {
                true => fault,
                false => fault.unrelayed(),
            };
            return Box::pin(refuse(format, fault, pipe.hooks)).await;
        }
    }
    if done {
        pipe.ahead.clear();
        pipe.source.close_unread();
        pipe.hooks.finish(Ok(()));
        return Ok(Exchange::new(head, stream::empty().boxed()));
    }
    // With no bytes, it ends as its head goes out.
    if pipe.out.is_empty() {
        pipe.tell_end();
    }
    let body = stream::unfold(pipe, |mut p| async move { p.next().await.map(|b| (b, p)) });
    Ok(Exchange::new(head, body.boxed()))
}

/// Turns the response sent to the client into its body.
struct Pipe {
    source: Source,
    /// A refusal carries its fault alone; filters end with the request.
    refusal: bool,
    /// Batches to write before the source's: what
    /// [`Response::until_content`] or [`Response::until_end`] read.
    ahead: VecDeque<Batch>,
    encoder: Box<dyn AnyEncoder>,
    hooks: EndHooks<ClientContext>,
    /// Bytes written and not yet handed out.
    out: Out,
    /// The end once written; told as its last bytes are handed out.
    ended: Option<Result<(), Fault>>,
    /// A failure that could not be written, to abort the body with.
    abort: Option<Fault>,
}

impl Pipe {
    /// Write until the body's first bytes. `Err`, to refuse it, if it failed
    /// before any and is `refusable`, would be aborted, or panicked.
    async fn first(&mut self, refusable: bool) -> Result<(), Fault> {
        while self.out.is_empty() && self.ended.is_none() {
            let Ok(bytes_before_end) = self.step().await else {
                return Err(Fault::internal(PANICKED));
            };
            // A failure before any bytes is refused.
            if refusable
                && !bytes_before_end
                && let Some(Err(f)) = &self.ended
            {
                return Err(f.clone());
            }
            if self.out.is_empty()
                && let Some(f) = &self.abort
            {
                return Err(f.clone());
            }
        }
        Ok(())
    }

    async fn next(&mut self) -> Option<Result<Bytes, Fault>> {
        loop {
            if let Some(bytes) = self.out.pop() {
                if self.out.is_empty() {
                    self.tell_end();
                }
                return Some(Ok(bytes));
            }
            self.tell_end();
            if let Some(f) = self.abort.take() {
                return Some(Err(f));
            }
            if self.hooks.is_finished() {
                return None;
            }
            let _ = self.step().await;
        }
    }

    /// Start the end hooks, if the end is written.
    fn tell_end(&mut self) {
        if let Some(end) = self.ended.take() {
            self.hooks.finish(end);
        }
    }

    /// Write the next batch, returning whether bytes came before an end.
    /// `Err` if a decoder, filter or encoder panicked: the response ends
    /// internal and is aborted.
    async fn step(&mut self) -> Result<bool, ()> {
        match AssertUnwindSafe(self.write()).catch_unwind().await {
            Ok(bytes_before_end) => Ok(bytes_before_end),
            Err(_) => {
                let fault = Fault::internal(PANICKED);
                self.source.end(Err(fault.clone()));
                self.ended = Some(Err(fault.clone()));
                self.abort = Some(fault);
                Err(())
            }
        }
    }

    /// Write the next batch, and at the end what filters kept back and the
    /// end. Returns whether bytes came before the end.
    async fn write(&mut self) -> bool {
        let mut batch = match self.ahead.pop_front() {
            Some(batch) => batch,
            None => self.source.next().await.0,
        };
        batch.read();
        let mut end = batch.end.take();
        let mut values = batch.values.take();
        if let Some(values) = &mut values {
            let filtered = self.hooks.cx().edits().outputs.run(values);
            if let Err(f) = filtered.and(self.encode(values)) {
                end = Some(Err(f));
            }
        }
        // What filters release at a failure does not prevent its refusal.
        let bytes_before_end = !self.out.is_empty();
        if let Some(end) = end {
            self.source.end(end.clone());
            // Filters see every end, a failure too.
            let end = self.flush(end);
            self.write_end(end);
        }
        bytes_before_end
    }

    /// Write `values`, each output its own frame in a streaming request.
    fn encode(&mut self, values: &Values) -> Result<(), Fault> {
        let out = &mut self.out;
        self.encoder.encode(values, &mut |bytes| out.push(bytes))
    }

    /// Write what the filters kept back before `end`; returns the end.
    fn flush(&mut self, end: Result<(), Fault>) -> Result<(), Fault> {
        if self.refusal {
            return end;
        }
        let mut tail = None;
        let end = self.hooks.cx().edits().outputs.end(end, &mut tail);
        match tail.map(|values| self.encode(&values)) {
            Some(Err(f)) => failed(f, &end),
            _ => end,
        }
    }

    /// Write `end`, else its encoder's fault, else abort the body.
    fn write_end(&mut self, mut end: Result<(), Fault>) {
        let mut one = BytesMut::new();
        if let Err(f) = self.encoder.end(&end, &mut one) {
            end = Err(f.clone());
            one.clear();
            if self.encoder.end(&end, &mut one).is_err() {
                one.clear();
                self.abort = Some(f);
            }
        }
        self.out.push(one);
        self.ended = Some(end);
    }
}

/// Bytes not yet handed out, as data frames: one per output to a streaming
/// request, else one per batch.
#[derive(Default)]
struct Out {
    streaming: bool,
    frames: VecDeque<Bytes>,
    batch: BytesMut,
}

impl Out {
    fn push(&mut self, bytes: BytesMut) {
        match self.streaming {
            _ if bytes.is_empty() => {}
            true => self.frames.push_back(bytes.freeze()),
            false => self.batch.unsplit(bytes),
        }
    }

    fn is_empty(&self) -> bool {
        self.frames.is_empty() && self.batch.is_empty()
    }

    /// The next frame to hand out.
    fn pop(&mut self) -> Option<Bytes> {
        let batch = (!self.batch.is_empty()).then(|| self.batch.split().freeze());
        self.frames.pop_front().or(batch)
    }
}

/// A refusal its format could not write: the adapter writes it, as an
/// internal fault; the request ends with `fault`.
fn unwritable(mut hooks: EndHooks<ClientContext>, fault: Fault) -> Handed {
    let handed = Fault::internal(fault.message().to_owned()).with_source(fault.clone());
    hooks.finish(Err(fault));
    Err(handed)
}
