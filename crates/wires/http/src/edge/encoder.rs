//! Synchronous body encoding and lazy message input.

use crate::Body;
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use http::request::Parts as RequestHead;
use respan_gateway_core::{EventStream, Fault};

/// Stateful values-to-bytes encoding, independent of heads and transport I/O.
pub trait Encoder<T>: Send + 'static {
    /// Encode one value. Partial output is discarded on failure.
    fn encode(&mut self, value: &T, out: &mut BytesMut) -> Result<(), Fault>;
    /// Encode one value as zero or more messages, each a frame of a live
    /// body; by default, the one message [`encode`](Self::encode) writes.
    /// Partial output is discarded on failure.
    fn encode_messages(&mut self, value: &T, out: &mut Vec<Bytes>) -> Result<(), Fault> {
        let mut bytes = BytesMut::new();
        self.encode(value, &mut bytes)?;
        out.push(bytes.freeze());
        Ok(())
    }
    /// Finish success or failure framing. Delivery still reports a source fault
    /// even when this writes its protocol representation successfully.
    fn end(&mut self, end: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault> {
        let _ = out;
        end.clone()
    }
}

/// Encode finite values synchronously, including final framing.
pub fn encode_all<T: 'static>(
    mut encoder: Box<dyn Encoder<T>>,
    values: &[T],
) -> Result<Bytes, Fault> {
    let mut bytes = BytesMut::new();
    for value in values {
        encoder.encode(value, &mut bytes)?;
    }
    encoder.end(&Ok(()), &mut bytes)?;
    Ok(bytes.freeze())
}

/// Decode one finite request lazily, or one message per explicitly live frame.
/// The client binding has already collected ordinary HTTP bodies before this runs.
pub fn messages<T, F>(request: http::Request<Body>, decode: F) -> EventStream<T>
where
    T: Clone + Send + Sync + 'static,
    F: Fn(&RequestHead, Bytes) -> Result<T, Fault> + Send + Sync + 'static,
{
    let (head, body) = request.into_parts();
    match body.full_bytes().cloned() {
        Some(bytes) => EventStream::lazy(move || decode(&head, bytes.clone())),
        None => EventStream::stream(body.map(move |bytes| decode(&head, bytes?))),
    }
}
