//! Observe wire failure and frame boundaries independently of semantic parsing.

use crate::Body;
use bytes::Bytes;
use futures::Stream;
use respan_gateway_core::Fault;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};

pub(crate) type Failure = Arc<OnceLock<Fault>>;

/// A provider's answer body as its reader reads it: each frame apart, as a
/// `Pending` that wakes itself at once follows each, and ending with the
/// upload's `failed` fault once one is set. `limit` is its binding's
/// ([`Body::limit`]).
///
/// The `Pending` between frames is load-bearing: without it, an answer
/// already buffered is read whole in one poll, ahead of its client, so a
/// client that leaves mid-stream would find its call already ended well.
/// One frame per poll makes a call's outcome follow its client's pace, not
/// how the provider's bytes happened to arrive (the relay lifecycle tests).
pub(crate) fn observed(body: Body, failed: Failure, limit: usize) -> Body {
    let mut body = Body::stream(Observed {
        body,
        failed,
        apart: false,
        ended: false,
    });
    body.limit = Some(limit);
    body
}

struct Observed {
    body: Body,
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
        if this.apart {
            this.apart = false;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        let frame = Pin::new(&mut this.body).poll_next(cx);
        if let Some(fault) = this.failed.get() {
            this.ended = true;
            return Poll::Ready(Some(Err(fault.clone())));
        }
        match &frame {
            Poll::Ready(Some(Ok(_))) => this.apart = true,
            Poll::Ready(Some(Err(_)) | None) => this.ended = true,
            Poll::Pending => {}
        }
        frame
    }
}
