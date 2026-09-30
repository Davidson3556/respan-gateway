//! Events into frames: a call's later inputs and a client's answer.

use std::collections::VecDeque;
use std::panic::AssertUnwindSafe;

use bytes::{Bytes, BytesMut};
use futures::stream::{self, BoxStream};
use futures::{FutureExt, StreamExt};
use respan_gateway_core::Fault;

use super::reader::Failure;
use super::{Completion, PANICKED};
use crate::{Encoder, Frames};

/// A body written from events through an [`Encoder`], whose head was
/// asked already: framing, the end on `Err`, and panic guards. The two
/// sides differ where HTTP's do:
///
/// - **A call's body** ([`Writer::call`]) is one frame per input, an empty
///   one too, then the end if it has bytes. It ends with its first failure,
///   its own or the inputs' (the client's body breaking, or the inputs cut
///   off: `abandoned`), as an `Err`, only after the encoder was told that
///   failure as its end, so no clean end follows one (B1). A panic in the
///   encoder ends it `internal`, in the body and in its [`Failure`], which
///   the response reads, as the transport may read the body where nothing
///   unwinds to the call (B2).
/// - **A client's answer** ([`Writer::answer`]) is one frame per output to
///   a streaming request, else one per batch. A failure is written in the
///   protocol's words ([`Encoder::end`]), again as the end's own failure if
///   that fails, and the body is aborted if that fails too. the completion receiver is
///   told once its last bytes are handed out.
pub(crate) struct Writer<T, H> {
    encoder: Box<dyn Encoder<T, H>>,
    /// An answer's: told the end once its last bytes are handed out.
    /// Declared first, so dropped first (fields drop in declaration order):
    /// a client that leaves ends the request before its answer's call,
    /// which `values` holds.
    pub(crate) ended: Option<Completion>,
    /// The values.
    pub(crate) values: Option<BoxStream<'static, Result<T, Fault>>>,
    /// A call's: where its encoder panicked.
    call: Option<Failure>,
    /// An answer's one failure, a refusal, its fault alone.
    pub(crate) refusal: Option<Fault>,
    /// Bytes written and not yet handed out.
    pub(crate) out: Out,
    /// An answer's end once written; told as its last bytes are handed out.
    end: Option<Result<(), Fault>>,
    /// A failure to end the body with, once its last bytes are out.
    abort: Option<Fault>,
    /// Whether nothing more is written: a call's inputs ended or failed, an
    /// answer's end was told.
    done: bool,
}

impl<T: Send + 'static, H: Send + 'static> Writer<T, H> {
    /// A streamed call's body after its first frame: `later`, then the end.
    pub(crate) fn call(
        encoder: Box<dyn Encoder<T, H>>,
        later: BoxStream<'static, Result<T, Fault>>,
        failed: Failure,
    ) -> Self {
        let mut writer = Self::new(encoder, Some(later), true);
        writer.call = Some(failed);
        writer
    }

    /// A client's answer: `outputs` (none: cut off), framed per output if
    /// `streaming`, telling `ended` once its end is out.
    pub(crate) fn answer(
        encoder: Box<dyn Encoder<T, H>>,
        ended: Completion,
        outputs: Option<BoxStream<'static, Result<T, Fault>>>,
        streaming: bool,
    ) -> Self {
        let mut writer = Self::new(encoder, outputs, streaming);
        writer.ended = Some(ended);
        writer
    }

    fn new(
        encoder: Box<dyn Encoder<T, H>>,
        values: Option<BoxStream<'static, Result<T, Fault>>>,
        streaming: bool,
    ) -> Self {
        Self {
            encoder,
            values,
            call: None,
            ended: None,
            refusal: None,
            out: Out {
                streaming,
                ..Out::default()
            },
            end: None,
            abort: None,
            done: false,
        }
    }

    /// The body, from here.
    pub(crate) fn frames(self) -> Frames {
        stream::unfold(self, |mut w| async move { w.next().await.map(|b| (b, w)) }).boxed()
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
            if let Some(fault) = self.abort.take() {
                return Some(Err(fault));
            }
            if self.done {
                return None;
            }
            match self.call.is_some() {
                true => self.write_input().await,
                false => {
                    let _ = self.step().await;
                }
            }
        }
    }

    /// Tell an answer's end, if it is written.
    pub(crate) fn tell_end(&mut self) {
        if let Some(end) = self.end.take()
            && let Some(ended) = self.ended.take()
        {
            self.done = true;
            ended.finish(end);
        }
    }

    /// A call's next input, or its end, written.
    async fn write_input(&mut self) {
        let input = match &mut self.values {
            Some(values) => values.next().await,
            None => None,
        };
        let mut out = BytesMut::new();
        let encoder = &mut self.encoder;
        let step = std::panic::catch_unwind(AssertUnwindSafe(|| match &input {
            Some(Ok(input)) => encoder.encode(input, &mut out).map(|()| true),
            None => encoder.end(&Ok(()), &mut out).map(|()| false),
            Some(Err(fault)) => Err(fault.clone()),
        }));
        match step {
            Err(_) => self.abort = Some(self.panicked()),
            Ok(Ok(more)) => {
                self.done = !more;
                // A frame is one, an empty one too; the end, if it has bytes.
                self.out.push(out, more);
            }
            Ok(Err(fault)) => {
                self.done = true;
                // Its partial output is dropped; a failed end was told.
                out.clear();
                if input.is_some() {
                    let ended =
                        AssertUnwindSafe(|| self.encoder.end(&Err(fault.clone()), &mut out));
                    match std::panic::catch_unwind(ended) {
                        Ok(Ok(())) => {}
                        Ok(Err(_)) => out.clear(),
                        Err(_) => {
                            self.panicked();
                            out.clear();
                        }
                    }
                }
                self.abort = Some(fault);
                self.out.push(out, false);
            }
        }
    }

    /// A call's encoder panicked: the body and the call end `internal`.
    fn panicked(&mut self) -> Fault {
        self.done = true;
        let fault = Fault::internal(PANICKED);
        if let Some(failed) = &self.call {
            let _ = failed.set(fault.clone());
        }
        fault
    }

    /// Write an answer until its first bytes. `Err`, to refuse it, if it
    /// failed before any and is `refusable`, would be aborted, or panicked.
    pub(crate) async fn first(&mut self, refusable: bool) -> Result<(), Fault> {
        while self.out.is_empty() && self.end.is_none() {
            let Ok(bytes_before_end) = self.step().await else {
                return Err(Fault::internal(PANICKED));
            };
            // A failure before any bytes is refused.
            if refusable
                && !bytes_before_end
                && let Some(Err(f)) = &self.end
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

    /// Write an answer's next output, returning whether bytes came before
    /// an end. `Err` if it panicked, reading or writing: the answer ends
    /// internal and is aborted.
    async fn step(&mut self) -> Result<bool, ()> {
        match AssertUnwindSafe(self.write_output()).catch_unwind().await {
            Ok(bytes_before_end) => Ok(bytes_before_end),
            Err(_) => {
                let fault = Fault::internal(PANICKED);
                self.end = Some(Err(fault.clone()));
                self.abort = Some(fault);
                Err(())
            }
        }
    }

    /// Write an answer's next output, or its end. Returns whether bytes
    /// came before the end.
    async fn write_output(&mut self) -> bool {
        let next = match (self.refusal.take(), &mut self.values) {
            (Some(fault), _) => Some(Err(fault)),
            (None, Some(outputs)) => outputs.next().await,
            (None, None) => Some(Err(Fault::abandoned())),
        };
        let end = match next {
            Some(Ok(output)) => self.encode(&output).err().map(Err),
            Some(Err(fault)) => Some(Err(fault)),
            None => Some(Ok(())),
        };
        let bytes_before_end = !self.out.is_empty();
        if let Some(end) = end {
            self.write_end(end);
        }
        bytes_before_end
    }

    /// Write `output`, its own frame to a streaming request.
    fn encode(&mut self, output: &T) -> Result<(), Fault> {
        let mut one = BytesMut::new();
        self.encoder.encode(output, &mut one)?;
        self.out.push(one, false);
        Ok(())
    }

    /// Write an answer's `end`, else its encoder's fault, else abort it.
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
        self.out.push(one, false);
        self.end = Some(end);
    }
}

/// Bytes not yet handed out, as data frames: one per value if streaming,
/// else one per batch.
#[derive(Default)]
pub(crate) struct Out {
    streaming: bool,
    frames: VecDeque<Bytes>,
    batch: BytesMut,
}

impl Out {
    /// Add `bytes`, as a frame even if empty if `frame`.
    pub(crate) fn push(&mut self, bytes: BytesMut, frame: bool) {
        match self.streaming {
            _ if bytes.is_empty() && !frame => {}
            true => self.frames.push_back(bytes.freeze()),
            false => self.batch.unsplit(bytes),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.frames.is_empty() && self.batch.is_empty()
    }

    /// The next frame to hand out.
    fn pop(&mut self) -> Option<Bytes> {
        let batch = (!self.batch.is_empty()).then(|| self.batch.split().freeze());
        self.frames.pop_front().or(batch)
    }
}
