//! Complete bytes and pull-driven wire bodies.

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt};
use respan_gateway_sdk::Fault;

use crate::Frames;

/// An HTTP body: complete bytes, or a stream of wire frames. Network chunking
/// does not imply a live semantic input; the incoming adapter selects that.
pub struct Body {
    data: Data,
    pub(crate) upload: Option<crate::driver::upload::Handoff>,
}

enum Data {
    Full(Option<Bytes>),
    Stream {
        prefix: Option<Bytes>,
        frames: Frames,
    },
}

impl Body {
    /// Complete bytes, inspectable before the body is polled.
    pub fn full(bytes: impl Into<Bytes>) -> Self {
        Self {
            data: Data::Full(Some(bytes.into())),
            upload: None,
        }
    }

    /// A pull-driven stream, with one wire frame per item.
    pub fn stream(stream: impl Stream<Item = Result<Bytes, Fault>> + Send + 'static) -> Self {
        Self {
            data: Data::Stream {
                prefix: None,
                frames: stream.boxed(),
            },
            upload: None,
        }
    }

    /// Prepend opening bytes, retaining a single initial frame.
    pub fn with_prefix(mut self, bytes: Bytes) -> Self {
        match &mut self.data {
            Data::Full(body) => *body = Some(join(bytes, body.take().unwrap_or_default())),
            Data::Stream { prefix, .. } => {
                *prefix = Some(join(bytes, prefix.take().unwrap_or_default()))
            }
        }
        self
    }

    /// Complete bytes, if this is an unread finite body.
    pub fn full_bytes(&self) -> Option<&Bytes> {
        match &self.data {
            Data::Full(bytes) => bytes.as_ref(),
            Data::Stream { .. } => None,
        }
    }

    /// Edit complete bytes before sending. The caller must update stale body headers.
    pub fn full_bytes_mut(&mut self) -> Option<&mut Bytes> {
        match &mut self.data {
            Data::Full(bytes) => bytes.as_mut(),
            Data::Stream { .. } => None,
        }
    }

    pub(crate) fn initial(&self) -> Bytes {
        match &self.data {
            Data::Full(bytes) => bytes.clone().unwrap_or_default(),
            Data::Stream { prefix, .. } => prefix.clone().unwrap_or_default(),
        }
    }

    pub(crate) fn is_streaming(&self) -> bool {
        matches!(self.data, Data::Stream { .. })
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
            Data::Stream { prefix, frames } => match prefix.take().filter(|b| !b.is_empty()) {
                Some(bytes) => Poll::Ready(Some(Ok(bytes))),
                None => frames.poll_next_unpin(cx),
            },
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
            Data::Stream { .. } => http_body::SizeHint::default(),
        }
    }
}

pub(crate) fn join(first: Bytes, second: Bytes) -> Bytes {
    if first.is_empty() {
        second
    } else if second.is_empty() {
        first
    } else {
        let mut bytes = BytesMut::with_capacity(first.len() + second.len());
        bytes.extend_from_slice(&first);
        bytes.extend_from_slice(&second);
        bytes.freeze()
    }
}
