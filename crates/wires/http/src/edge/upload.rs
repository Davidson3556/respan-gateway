//! A call's outgoing sink: its live input encoded an item at a time, an
//! item's messages a frame each, and the transport's independent delivery.

use super::reader::Failure;
use super::writer::Writer;
use super::{end_encoder, guarded};
use crate::{Body, Completion, Encoder};
use futures::future::{BoxFuture, Shared};
use futures::{Future, Sink};
use respan_gateway_core::Fault;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

/// The call's delivery ([`ProviderIo::bind`](crate::ProviderIo::bind)).
pub(crate) type Delivery = Shared<BoxFuture<'static, Result<(), Fault>>>;

/// Live input's encoder and the byte writer a transport's body reads.
pub(crate) struct Encoding<T> {
    /// `None` once ended.
    encoder: Option<Box<dyn Encoder<T>>>,
    writer: Writer,
    failed: Failure,
}

/// Encode live input with `encoder`, its failure kept in `failed`: its
/// sink's half, the body a transport reads, and the writer's completion
/// once its reader drained it.
pub(crate) fn encoding<T: 'static>(
    encoder: Box<dyn Encoder<T>>,
    failed: Failure,
) -> (Encoding<T>, Body, Completion) {
    let (writer, frames, draining) = Writer::new();
    let mut encoding = Encoding {
        encoder: Some(encoder),
        writer,
        failed,
    };
    let mut out = Vec::new();
    let started = match &mut encoding.encoder {
        Some(encoder) => guarded(|| encoder.start(&mut out)),
        None => Ok(()),
    };
    match started {
        Ok(()) => out
            .into_iter()
            .for_each(|frame| encoding.writer.send(frame)),
        Err(fault) => encoding.finish(Err(fault)),
    }
    (encoding, Body::stream(frames), draining)
}

impl<T: 'static> Encoding<T> {
    fn push(&mut self, item: Result<T, Fault>) {
        let Some(encoder) = &mut self.encoder else {
            return;
        };
        let mut out = Vec::new();
        let encoded = match item {
            Ok(value) => guarded(|| encoder.encode(&value, &mut out)),
            Err(fault) => Err(fault),
        };
        match encoded {
            Ok(()) => out.into_iter().for_each(|frame| self.writer.send(frame)),
            Err(fault) => self.finish(Err(fault)),
        }
    }

    /// End the upload as `end` says. Any failure breaks the body off after
    /// its error framing, so no transport takes it for a whole upload.
    fn finish(&mut self, end: Result<(), Fault>) {
        let Some(mut encoder) = self.encoder.take() else {
            return;
        };
        let mut out = Vec::new();
        let (end, _) = end_encoder(encoder.as_mut(), end, &mut out);
        if let Err(fault) = &end {
            let _ = self.failed.set(fault.clone());
        }
        out.into_iter().for_each(|frame| self.writer.send(frame));
        self.writer.end(end.clone(), end.err());
    }

    /// Finish and wait until the transport took every frame. A transport
    /// that dropped the body ends it `Ok` unless the upload itself failed:
    /// the delivery says how such a call ended.
    fn close(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Fault>> {
        if self.encoder.is_some() {
            if let Err(fault) = ready!(self.writer.poll_ready(cx, false)) {
                return Poll::Ready(self.failed.get().map_or(Ok(()), |_| Err(fault)));
            }
            self.finish(Ok(()));
        }
        match self.writer.poll_close(cx) {
            Poll::Ready(Err(_)) if self.failed.get().is_none() => Poll::Ready(Ok(())),
            end => end,
        }
    }
}

/// A call's sink: its live input's [`Encoding`], or none for a call encoded
/// whole, coupled to the call's delivery. A refused answer may discard the
/// byte reader; the delivery, not that discard, decides how the call ended.
pub(crate) struct Upload<T> {
    encoding: Option<Encoding<T>>,
    delivery: Delivery,
    delivered: Option<Result<(), Fault>>,
    failed: Failure,
    closed: bool,
}

impl<T> Unpin for Upload<T> {}

impl<T: 'static> Upload<T> {
    pub(crate) fn new(encoding: Option<Encoding<T>>, delivery: Delivery, failed: Failure) -> Self {
        Self {
            encoding,
            delivery,
            delivered: None,
            failed,
            closed: false,
        }
    }

    /// The delivery's end, once it ended, a failed upload's fault in place
    /// of its `Ok`.
    fn progress(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Fault>> {
        if self.delivered.is_none() {
            self.delivered = Some(ready!(Pin::new(&mut self.delivery).poll(cx)));
        }
        let failed = self.failed.get().cloned().map_or(Ok(()), Err);
        Poll::Ready(self.delivered.clone().unwrap_or(Ok(())).and(failed))
    }

    /// The writer's readiness, or the delivery's once it has finished or
    /// the body was discarded; a failed delivery always wins. `requested`
    /// waits for the transport to ask for more.
    fn readiness(&mut self, cx: &mut Context<'_>, requested: bool) -> Poll<Result<(), Fault>> {
        let delivery = self.progress(cx);
        if matches!(delivery, Poll::Ready(Err(_))) {
            return delivery;
        }
        let Some(encoding) = &self.encoding else {
            return Poll::Ready(Ok(()));
        };
        match encoding.writer.poll_ready(cx, requested) {
            Poll::Ready(Err(_)) if self.failed.get().is_none() => delivery,
            Poll::Ready(Err(_)) => Poll::Ready(Err(self
                .failed
                .get()
                .cloned()
                .unwrap_or_else(Fault::abandoned))),
            ready if delivery.is_pending() => ready,
            _ => delivery,
        }
    }
}

impl<T: 'static> Sink<Result<T, Fault>> for Upload<T> {
    type Error = Fault;
    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Fault>> {
        self.readiness(cx, true)
    }
    fn start_send(mut self: Pin<&mut Self>, item: Result<T, Fault>) -> Result<(), Fault> {
        if let Some(encoding) = &mut self.encoding {
            encoding.push(item);
        }
        Ok(())
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Fault>> {
        self.readiness(cx, false)
    }
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Fault>> {
        if let Poll::Ready(end) = self.progress(cx) {
            return Poll::Ready(end);
        }
        if !self.closed {
            if let Some(encoding) = &mut self.encoding {
                ready!(encoding.close(cx))?;
            }
            self.closed = true;
        }
        self.progress(cx)
    }
}
