//! A call's outgoing sink: its live input encoded an item at a time, an
//! item's messages a frame each, and the transport's independent delivery.

use super::reader::Failure;
use super::writer::Writer;
use crate::{Body, Completion, Encoder};
use bytes::Bytes;
use futures::future::{BoxFuture, Shared};
use futures::{Future, Sink};
use respan_gateway_core::Fault;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

/// The call's delivery ([`ProviderIo::bind`](crate::ProviderIo::bind)).
pub(crate) type Delivery = Shared<BoxFuture<'static, Result<(), Fault>>>;

/// A writer of live input's frames with `encoder`: it, the body a
/// transport reads, and its completion once its reader drained it.
pub(crate) fn encoding<T: 'static>(encoder: Box<dyn Encoder<T>>) -> (Writer<T>, Body, Completion) {
    let (writer, frames, draining) = Writer::new(Some(encoder));
    (writer, Body::stream(frames), draining)
}

/// A call's sink: its live input's writer ([`encoding`]), or none for a
/// call encoded whole, coupled to the call's delivery; an upload's failure
/// kept in `failed`. A refused answer may discard the byte reader; the
/// delivery, not that discard, decides how the call ended.
pub(crate) struct Upload<T> {
    writer: Option<Writer<T>>,
    delivery: Delivery,
    delivered: Option<Result<(), Fault>>,
    failed: Failure,
    closed: bool,
}

impl<T> Unpin for Upload<T> {}

impl<T: 'static> Upload<T> {
    pub(crate) fn new(writer: Option<Writer<T>>, delivery: Delivery, failed: Failure) -> Self {
        let mut upload = Self {
            writer,
            delivery,
            delivered: None,
            failed,
            closed: false,
        };
        if let Some(writer) = &mut upload.writer {
            let started = writer.start();
            upload.sent(started);
        }
        upload
    }

    /// Send `frames`, or end with their fault.
    fn sent(&mut self, frames: Result<Vec<Bytes>, Fault>) {
        let Some(writer) = &self.writer else {
            return;
        };
        match frames {
            Ok(frames) => frames.into_iter().for_each(|f| writer.send(f)),
            Err(fault) => self.finish(Err(fault)),
        }
    }

    /// End the upload as `end` says. Any failure breaks the body off after
    /// its error framing, so no transport takes it for a whole upload.
    fn finish(&mut self, end: Result<(), Fault>) {
        let Some(writer) = &mut self.writer else {
            return;
        };
        let mut out = Vec::new();
        let Some((end, _)) = writer.end_encoder(end, &mut out) else {
            return;
        };
        if let Err(fault) = &end {
            let _ = self.failed.set(fault.clone());
        }
        out.into_iter().for_each(|frame| writer.send(frame));
        writer.end(end.clone(), end.err());
    }

    /// Finish and wait until the transport took every frame. A transport
    /// that dropped the body ends it `Ok` unless the upload itself failed:
    /// the delivery says how such a call ended.
    fn close_writer(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Fault>> {
        let Some(writer) = &self.writer else {
            return Poll::Ready(Ok(()));
        };
        if writer.is_open() {
            if let Err(fault) = ready!(writer.poll_ready(cx, false)) {
                return Poll::Ready(self.failed.get().map_or(Ok(()), |_| Err(fault)));
            }
            self.finish(Ok(()));
        }
        let Some(writer) = &self.writer else {
            return Poll::Ready(Ok(()));
        };
        match writer.poll_close(cx) {
            Poll::Ready(Err(_)) if self.failed.get().is_none() => Poll::Ready(Ok(())),
            end => end,
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
        let Some(writer) = &self.writer else {
            return Poll::Ready(Ok(()));
        };
        match writer.poll_ready(cx, requested) {
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
        if let Some(writer) = &mut self.writer {
            let encoded = writer.encode(item);
            self.sent(encoded);
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
            ready!(self.close_writer(cx))?;
            self.closed = true;
        }
        self.progress(cx)
    }
}
