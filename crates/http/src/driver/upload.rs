//! Per-item request encoding and independent transport completion.

use super::PANICKED;
use super::reader::Failure;
use super::writer::Writer;
use crate::{Body, Completion, Encoder};
use bytes::BytesMut;
use futures::future::{BoxFuture, Shared};
use futures::{Future, Sink};
use respan_gateway_core::{EventSink, Fault};
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::task::{Context, Poll, ready};

pub(crate) type Delivery = Shared<BoxFuture<'static, Result<(), Fault>>>;

/// Marks the failure of an encoding sink whose transport dropped its body:
/// the delivery, not the sink, says how the upload ended.
#[derive(Clone, Copy, Debug)]
struct Discarded;
pub(crate) struct Handoff {
    pub(crate) draining: Completion,
    pub(crate) failed: Failure,
    pub(crate) settled: Arc<AtomicBool>,
}

pub(crate) fn encoding<T: 'static>(encoder: Box<dyn Encoder<T>>) -> (EventSink<T>, Body) {
    let (writer, frames, draining) = Writer::new();
    let failed = Failure::default();
    let mut body = Body::stream(frames);
    let settled = Arc::new(AtomicBool::new(false));
    body.upload = Some(Handoff {
        draining,
        failed: failed.clone(),
        settled: settled.clone(),
    });
    (
        EventSink::new(Encoding {
            encoder,
            writer,
            failed,
            settled,
            ended: false,
        }),
        body,
    )
}

struct Encoding<T> {
    encoder: Box<dyn Encoder<T>>,
    writer: Writer,
    failed: Failure,
    settled: Arc<AtomicBool>,
    ended: bool,
}
impl<T> Unpin for Encoding<T> {}
impl<T: 'static> Encoding<T> {
    /// The writer's readiness: its own failure, or, once its reader is gone,
    /// that it was [`Discarded`].
    fn ready(&self, cx: &mut Context<'_>, requested: bool) -> Poll<Result<(), Fault>> {
        self.writer.poll_ready(cx, requested).map_err(|gone| {
            self.failed
                .get()
                .cloned()
                .unwrap_or_else(|| gone.with(Discarded))
        })
    }
    fn finish(&mut self, mut end: Result<(), Fault>) {
        if self.ended {
            return;
        }
        self.ended = true;
        let mut out = BytesMut::new();
        let encoded =
            std::panic::catch_unwind(AssertUnwindSafe(|| self.encoder.end(&end, &mut out)))
                .unwrap_or_else(|_| Err(Fault::internal(PANICKED)));
        if let Err(fault) = encoded {
            out.clear();
            end = Err(fault);
        }
        if let Err(fault) = &end {
            let _ = self.failed.set(fault.clone());
        }
        if !out.is_empty() {
            self.writer.send(out.freeze());
        }
        self.writer.end(end.clone(), end.err());
    }
}
impl<T: 'static> Sink<Result<T, Fault>> for Encoding<T> {
    type Error = Fault;
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Fault>> {
        self.ready(cx, true)
    }
    fn start_send(mut self: Pin<&mut Self>, item: Result<T, Fault>) -> Result<(), Fault> {
        if self.ended {
            return Ok(());
        }
        let mut out = BytesMut::new();
        let encoded = match item {
            Ok(value) => {
                std::panic::catch_unwind(AssertUnwindSafe(|| self.encoder.encode(&value, &mut out)))
                    .unwrap_or_else(|_| Err(Fault::internal(PANICKED)))
            }
            Err(fault) => Err(fault),
        };
        match encoded {
            Ok(()) if !out.is_empty() => self.writer.send(out.freeze()),
            Ok(()) => {}
            Err(fault) => self.finish(Err(fault)),
        }
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Fault>> {
        self.ready(cx, false)
    }
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Fault>> {
        if self.settled.load(Ordering::Acquire) {
            return Poll::Ready(self.failed.get().cloned().map_or(Ok(()), Err));
        }
        if !self.ended {
            if let Err(fault) = ready!(self.writer.poll_ready(cx, false)) {
                return Poll::Ready(self.failed.get().cloned().map_or(Ok(()), |_| Err(fault)));
            }
            self.finish(Ok(()));
        }
        match self.writer.poll_close(cx) {
            Poll::Ready(Err(_)) if self.failed.get().is_none() => Poll::Ready(Ok(())),
            end => end,
        }
    }
}

/// Couples an arbitrary real sink to the request's independent delivery work.
/// A refused response may discard its byte reader; transport completion, rather
/// than that discard, decides the call's final delivery result.
pub(crate) struct Upload<T> {
    sink: EventSink<T>,
    delivery: Delivery,
    delivered: Option<Result<(), Fault>>,
    failed: Failure,
    closed: bool,
}
impl<T> Unpin for Upload<T> {}
impl<T: 'static> Upload<T> {
    pub(crate) fn new(sink: EventSink<T>, delivery: Delivery, failed: Failure) -> Self {
        Self {
            sink,
            delivery,
            delivered: None,
            failed,
            closed: false,
        }
    }
    fn progress(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Fault>> {
        if self.delivered.is_none() {
            if let Poll::Ready(end) = Pin::new(&mut self.delivery).poll(cx) {
                self.delivered =
                    Some(end.and_then(|()| self.failed.get().cloned().map_or(Ok(()), Err)));
            } else {
                return Poll::Pending;
            }
        }
        Poll::Ready(self.delivered.clone().expect("delivery completed"))
    }
    fn readiness(&mut self, cx: &mut Context<'_>, flush: bool) -> Poll<Result<(), Fault>> {
        let delivery = self.progress(cx);
        if matches!(delivery, Poll::Ready(Err(_))) {
            return delivery;
        }
        let ready = if flush {
            Pin::new(&mut self.sink).poll_flush(cx)
        } else {
            Pin::new(&mut self.sink).poll_ready(cx)
        };
        match (ready, delivery) {
            // The delivery, whose waker is set, says how a discarded upload ended.
            (Poll::Ready(Err(f)), delivery) if f.get::<Discarded>().is_some() => delivery,
            (ready @ Poll::Ready(Err(_)), _) | (ready, Poll::Pending) => ready,
            (_, delivery) => delivery,
        }
    }
}
impl<T: 'static> Sink<Result<T, Fault>> for Upload<T> {
    type Error = Fault;
    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Fault>> {
        self.readiness(cx, false)
    }
    fn start_send(mut self: Pin<&mut Self>, item: Result<T, Fault>) -> Result<(), Fault> {
        Pin::new(&mut self.sink).start_send(item)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Fault>> {
        self.readiness(cx, true)
    }
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Fault>> {
        let delivery = self.progress(cx);
        if matches!(delivery, Poll::Ready(Err(_))) {
            return delivery;
        }
        if !self.closed {
            match Pin::new(&mut self.sink).poll_close(cx) {
                Poll::Ready(Ok(())) => self.closed = true,
                Poll::Ready(Err(fault)) => return Poll::Ready(Err(fault)),
                Poll::Pending => return Poll::Pending,
            }
        }
        self.progress(cx)
    }
}
