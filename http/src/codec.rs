//! Synchronous body encoding and lazy message input.

use crate::Body;
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use http::request::Parts as RequestHead;
use respan_gateway_sdk::{EventSink, EventStream, Fault};

/// Stateful values-to-bytes encoding, independent of heads and transport I/O.
pub trait Encoder<T>: Send + 'static {
    /// Encode one value. Partial output is discarded on failure.
    fn encode(&mut self, value: &T, out: &mut BytesMut) -> Result<(), Fault>;
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

/// Prepare the actual request and per-item sink. Finite input is encoded now;
/// live input stays caller-owned. Explicit raw requests bypass this helper.
pub fn request<T: Clone + Send + Sync + 'static>(
    _name: &str,
    source: &mut EventStream<T>,
    encoder: Box<dyn Encoder<T>>,
) -> Result<(http::Request<Body>, EventSink<T>), Fault> {
    if let Some(fault) = source.refusal() {
        return Err(fault.clone());
    }
    if source.is_single() {
        let value = source
            .value()
            .transpose()?
            .ok_or_else(|| Fault::internal("a whole request has no input"))?;
        let bytes = encode_all(encoder, std::slice::from_ref(value))?;
        source.stop();
        let sink = EventSink::new(futures::sink::drain().sink_map_err(|never| match never {}));
        Ok((http::Request::new(Body::full(bytes)), sink))
    } else {
        let (sink, body) = crate::driver::upload::encoding(encoder);
        Ok((http::Request::new(body), sink))
    }
}

/// Decode one finite request lazily, or one message per explicitly live frame.
/// Incoming binding has already collected ordinary HTTP bodies before this runs.
pub fn messages<T, F>(request: http::Request<Body>, decode: F) -> Result<EventStream<T>, Fault>
where
    T: Clone + Send + Sync + 'static,
    F: Fn(&RequestHead, Bytes) -> Result<T, Fault> + Send + Sync + 'static,
{
    let (head, body) = request.into_parts();
    match body.full_bytes().cloned() {
        Some(bytes) => Ok(EventStream::lazy(move || decode(&head, bytes.clone()))),
        None => Ok(EventStream::stream(
            body.map(move |bytes| decode(&head, bytes?)),
        )),
    }
}

use futures::SinkExt;
