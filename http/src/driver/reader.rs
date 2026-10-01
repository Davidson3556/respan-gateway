//! Observe wire failure and frame boundaries independently of semantic parsing.

use crate::Body;
use bytes::Bytes;
use futures::{FutureExt, Stream, StreamExt, stream::BoxStream};
use respan_gateway_sdk::Fault;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll};

pub(crate) type Failure = Arc<OnceLock<Fault>>;
const ERROR_BODY_CAP: usize = 1 << 20;
// Yield before the generic parser's independent 128-frame limit so opening
// can distinguish cooperative work from a genuinely pending upstream body.
const MAX_READY_FRAMES: usize = 127;

#[derive(Default)]
struct State {
    probing: bool,
    yielded: bool,
    broke: bool,
    capped: bool,
    bytes: usize,
    frames: usize,
}

pub(crate) struct Observation(Arc<Mutex<State>>);
impl Observation {
    pub(crate) fn body(body: Body, failed: Failure) -> (Body, Self) {
        let state = Arc::new(Mutex::new(State::default()));
        (
            Body::stream(Observed {
                body,
                state: state.clone(),
                failed,
                apart: false,
                ended: false,
            }),
            Self(state),
        )
    }

    pub(crate) async fn open<T: Send + 'static>(
        self,
        mut stream: BoxStream<'static, Result<T, Fault>>,
        decide: bool,
    ) -> (BoxStream<'static, Result<T, Fault>>, Option<Fault>) {
        {
            let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
            state.probing = true;
            state.capped = decide;
        }
        let first = if decide {
            Some(stream.next().await)
        } else {
            loop {
                {
                    let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
                    state.yielded = false;
                }
                if let Some(item) = stream.next().now_or_never() {
                    break Some(item);
                }
                if !self.0.lock().unwrap_or_else(|e| e.into_inner()).yielded {
                    break None;
                }
                tokio::task::yield_now().await;
            }
        };
        let broke = {
            let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
            state.probing = false;
            state.capped = false;
            state.broke
        };
        match first {
            Some(Some(Err(fault))) if !broke => (futures::stream::empty().boxed(), Some(fault)),
            Some(Some(item)) => (
                futures::stream::once(futures::future::ready(item))
                    .chain(stream)
                    .boxed(),
                None,
            ),
            Some(None) => (futures::stream::empty().boxed(), None),
            None => (stream, None),
        }
    }
}

struct Observed {
    body: Body,
    state: Arc<Mutex<State>>,
    failed: Failure,
    apart: bool,
    ended: bool,
}
impl Stream for Observed {
    type Item = Result<Bytes, Fault>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.ended {
            return Poll::Ready(None);
        }
        let mut state = this.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.capped && state.bytes > ERROR_BODY_CAP {
            this.ended = true;
            return Poll::Ready(None);
        }
        if (!state.probing && this.apart) || state.frames == MAX_READY_FRAMES {
            this.apart = false;
            state.frames = 0;
            state.yielded = true;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        let frame = Pin::new(&mut this.body).poll_next(cx);
        if let Some(fault) = this.failed.get() {
            state.broke = true;
            this.ended = true;
            return Poll::Ready(Some(Err(fault.clone())));
        }
        match &frame {
            Poll::Ready(Some(Ok(bytes))) => {
                state.bytes = state.bytes.saturating_add(bytes.len());
                state.frames += 1;
                this.apart = true;
            }
            Poll::Ready(Some(Err(_))) => {
                state.broke = true;
                this.ended = true;
            }
            Poll::Ready(None) => this.ended = true,
            Poll::Pending => {
                state.frames = 0;
                state.yielded = false;
            }
        }
        frame
    }
}
