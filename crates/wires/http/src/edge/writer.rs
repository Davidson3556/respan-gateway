//! A bounded encoded-byte handoff. Events never enter this bridge: the HTTP
//! sinks encode each item themselves, and transports pull the resulting frames.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use bytes::Bytes;
use futures::channel::oneshot;
use futures::{FutureExt, Stream, StreamExt};
use respan_gateway_core::Fault;

use super::{end_encoder, guarded, locked};
use crate::{Completion, Encoder, Frames};

#[derive(Default)]
struct State {
    frames: VecDeque<Bytes>,
    abort: Option<Fault>,
    end: Option<Result<(), Fault>>,
    delivered: Option<Result<(), Fault>>,
    reader: Option<Waker>,
    writer: Option<Waker>,
    completion: Option<oneshot::Sender<Result<(), Fault>>>,
}

impl State {
    fn acknowledge(&mut self, end: Result<(), Fault>) {
        if self.delivered.is_none() {
            self.delivered = Some(end.clone());
            if let Some(completion) = self.completion.take() {
                let _ = completion.send(end);
            }
        }
        if let Some(waker) = self.writer.take() {
            waker.wake();
        }
    }

    fn drained(&mut self) {
        if self.frames.is_empty()
            && self.abort.is_none()
            && let Some(end) = self.end.clone()
        {
            self.acknowledge(end);
        }
    }
}

/// A body's writer: its encoder's frames of each item, taken only once its
/// reader drained the last item's, plus a terminal body error, which
/// follows any error framing rather than replacing it with clean EOF. What
/// a response and an upload both are. Its encoder is `None` once it ended.
pub(crate) struct Writer<T> {
    state: Arc<Mutex<State>>,
    encoder: Option<Box<dyn Encoder<T>>>,
}

impl<T: 'static> Writer<T> {
    /// A writer of `encoder`'s frames, none if it is `None`; the body its
    /// reader reads, and its completion once read.
    pub(crate) fn new(encoder: Option<Box<dyn Encoder<T>>>) -> (Self, Frames, Completion) {
        let (completion, ended) = oneshot::channel();
        let state = Arc::new(Mutex::new(State {
            completion: Some(completion),
            ..State::default()
        }));
        let ended = ended
            .map(|end| end.unwrap_or_else(|_| Err(Fault::abandoned())))
            .boxed();
        let writer = Self {
            state: state.clone(),
            encoder,
        };
        (writer, Pipe(state).boxed(), ended)
    }

    /// Whether its encoder has not ended.
    pub(crate) fn is_open(&self) -> bool {
        self.encoder.is_some()
    }

    /// End its encoder unwritten, dropped.
    pub(crate) fn drop_encoder(&mut self) {
        self.encoder = None;
    }

    /// The frames its encoder starts with; `Err` the fault to end with.
    pub(crate) fn start(&mut self) -> Result<Vec<Bytes>, Fault> {
        let mut out = Vec::new();
        match &mut self.encoder {
            Some(encoder) => guarded(|| encoder.start(&mut out)).map(|()| out),
            None => Ok(out),
        }
    }

    /// The frames of `item`, none once it ended; `Err` the fault to end
    /// with, `item`'s own if it is one.
    pub(crate) fn encode(&mut self, item: Result<T, Fault>) -> Result<Vec<Bytes>, Fault> {
        let mut out = Vec::new();
        let Some(encoder) = &mut self.encoder else {
            return Ok(out);
        };
        let value = item?;
        guarded(|| encoder.encode(&value, &mut out)).map(|()| out)
    }

    /// End its encoder as `end` says ([`end_encoder`]), its closing frames
    /// in `out`: how it ended, and the fault to break the body off with;
    /// `None` if it had.
    pub(crate) fn end_encoder(
        &mut self,
        end: Result<(), Fault>,
        out: &mut Vec<Bytes>,
    ) -> Option<(Result<(), Fault>, Option<Fault>)> {
        let mut encoder = self.encoder.take()?;
        Some(end_encoder(encoder.as_mut(), end, out))
    }

    pub(crate) fn poll_ready(
        &self,
        cx: &mut Context<'_>,
        requested: bool,
    ) -> Poll<Result<(), Fault>> {
        let mut state = locked(&self.state);
        if let Some(Err(fault)) = &state.delivered {
            return Poll::Ready(Err(fault.clone()));
        }
        if state.frames.is_empty()
            && state.abort.is_none()
            && (!requested || state.reader.is_some())
        {
            Poll::Ready(Ok(()))
        } else {
            state.writer = Some(cx.waker().clone());
            Poll::Pending
        }
    }

    pub(crate) fn send(&self, bytes: Bytes) {
        let mut state = locked(&self.state);
        state.frames.push_back(bytes);
        if let Some(waker) = state.reader.take() {
            waker.wake();
        }
    }

    pub(crate) fn end(&self, end: Result<(), Fault>, abort: Option<Fault>) {
        let mut state = locked(&self.state);
        state.end = Some(end);
        state.abort = abort;
        state.drained();
        if let Some(waker) = state.reader.take() {
            waker.wake();
        }
    }

    pub(crate) fn poll_close(&self, cx: &mut Context<'_>) -> Poll<Result<(), Fault>> {
        let mut state = locked(&self.state);
        match state.delivered.clone() {
            Some(end) => Poll::Ready(end),
            None => {
                state.writer = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

impl<T> Drop for Writer<T> {
    fn drop(&mut self) {
        let mut state = locked(&self.state);
        if state.end.is_none() {
            let fault = Fault::abandoned();
            state.end = Some(Err(fault.clone()));
            state.abort = Some(fault);
            if let Some(waker) = state.reader.take() {
                waker.wake();
            }
        }
    }
}

struct Pipe(Arc<Mutex<State>>);

impl Stream for Pipe {
    type Item = Result<Bytes, Fault>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut state = locked(&self.0);
        if let Some(bytes) = state.frames.pop_front() {
            if state.frames.is_empty() {
                state.drained();
                if let Some(waker) = state.writer.take() {
                    waker.wake();
                }
            }
            return Poll::Ready(Some(Ok(bytes)));
        }
        if let Some(fault) = state.abort.take() {
            state.drained();
            return Poll::Ready(Some(Err(fault)));
        }
        if state.end.is_some() {
            state.drained();
            Poll::Ready(None)
        } else {
            state.reader = Some(cx.waker().clone());
            if let Some(waker) = state.writer.take() {
                waker.wake();
            }
            Poll::Pending
        }
    }
}

impl Drop for Pipe {
    fn drop(&mut self) {
        let mut state = locked(&self.0);
        state.acknowledge(Err(Fault::abandoned()));
    }
}
