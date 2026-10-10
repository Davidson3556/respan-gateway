//! Complete bytes and pull-driven wire bodies.

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::{Stream, StreamExt};
use respan_gateway_core::Fault;

use crate::Frames;

/// An HTTP body: complete bytes, or a stream of wire frames. Network chunking
/// does not imply a live semantic input; the incoming adapter selects that.
pub struct Body {
    data: Data,
    pub(crate) upload: Option<crate::driver::upload::Handoff>,
    /// The most bytes a reader holds of it at once, its binding's limit.
    pub(crate) limit: Option<usize>,
}

enum Data {
    Full(Option<Bytes>),
    Stream(Frames),
}

impl Body {
    /// Complete bytes, inspectable before the body is polled.
    pub fn full(bytes: impl Into<Bytes>) -> Self {
        Self {
            data: Data::Full(Some(bytes.into())),
            upload: None,
            limit: None,
        }
    }

    /// A pull-driven stream, with one wire frame per item.
    pub fn stream(stream: impl Stream<Item = Result<Bytes, Fault>> + Send + 'static) -> Self {
        Self {
            data: Data::Stream(stream.boxed()),
            upload: None,
            limit: None,
        }
    }

    /// The most bytes a reader may hold of it at once: of a provider's
    /// answer, its transport's limit ([`connect`](crate::connect)), which
    /// the readers of [`formats`](crate::formats) keep a whole body, or one
    /// event, to. `None` for a body no binding read, already in memory.
    pub fn limit(&self) -> Option<usize> {
        self.limit
    }

    /// Complete bytes, if this is an unread finite body.
    pub fn full_bytes(&self) -> Option<&Bytes> {
        match &self.data {
            Data::Full(bytes) => bytes.as_ref(),
            Data::Stream(_) => None,
        }
    }

    /// Whether its bytes come as a stream of frames rather than whole: how a
    /// call uploads. Not whether its input is live, one message per frame,
    /// which the adapter says ([`Incoming::accept`](crate::Incoming::accept)).
    pub(crate) fn is_streaming(&self) -> bool {
        matches!(self.data, Data::Stream(_))
    }
}

impl Default for Body {
    fn default() -> Self {
        Self::full(Bytes::new())
    }
}

impl std::fmt::Debug for Body {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Body")
            .field("full", &self.full_bytes())
            .finish_non_exhaustive()
    }
}

impl Stream for Body {
    type Item = Result<Bytes, Fault>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.get_mut().data {
            Data::Full(bytes) => Poll::Ready(bytes.take().filter(|b| !b.is_empty()).map(Ok)),
            Data::Stream(frames) => frames.poll_next_unpin(cx),
        }
    }
}

impl http_body::Body for Body {
    type Data = Bytes;
    type Error = Fault;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Fault>>> {
        self.poll_next(cx)
            .map(|item| item.map(|bytes| bytes.map(http_body::Frame::data)))
    }

    fn is_end_stream(&self) -> bool {
        matches!(&self.data, Data::Full(bytes) if bytes.as_ref().is_none_or(Bytes::is_empty))
    }

    fn size_hint(&self) -> http_body::SizeHint {
        match &self.data {
            Data::Full(bytes) => {
                http_body::SizeHint::with_exact(bytes.as_ref().map_or(0, |b| b.len() as u64))
            }
            Data::Stream(_) => http_body::SizeHint::default(),
        }
    }
}
