//! Synchronous body encoding and lazy message input.

use crate::{Body, RequestHead, ResponseHead};
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
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

/// The eligible original client head, if this is a same-format relay.
pub fn request_head<'a, T>(name: &str, source: &'a EventStream<T>) -> Option<&'a RequestHead> {
    source
        .metadata::<crate::driver::RequestMetadata>(name)
        .map(|m| &*m.head)
}

/// Original finite bytes only while the source remains eligible and lazy.
pub fn raw_body<'a, T>(name: &str, source: &'a EventStream<T>) -> Option<&'a Bytes> {
    source
        .metadata::<crate::driver::RequestMetadata>(name)
        .and_then(|m| m.body.as_ref())
        .filter(|_| source.is_lazy())
}

/// The eligible provider head, or a fresh success head for a new response.
pub fn response_head<T>(name: &str, source: &EventStream<T>) -> ResponseHead {
    source
        .metadata::<crate::driver::ResponseMetadata>(name)
        .map(|m| m.0.clone())
        .unwrap_or_else(crate::fresh_response_head)
}

/// Prepare the actual request and per-item sink. Finite input is encoded now;
/// unchanged lazy input bypasses semantic encoding. Live input stays caller-owned.
pub fn request<T: Clone + Send + Sync + 'static>(
    name: &str,
    source: &mut EventStream<T>,
    mut encoder: Box<dyn Encoder<T>>,
) -> Result<(http::Request<Body>, EventSink<T>), Fault> {
    if let Some(fault) = source.refusal() {
        return Err(fault.clone());
    }
    let mut head = request_head(name, source)
        .cloned()
        .unwrap_or_else(crate::fresh_request_head);
    let raw = raw_body(name, source).cloned();
    if raw.is_none() {
        crate::formats::body_changed(&mut head);
    }
    if source.is_single() {
        let bytes = match raw {
            Some(bytes) => {
                let mut end = BytesMut::new();
                encoder.end(&Ok(()), &mut end)?;
                crate::body::join(bytes, end.freeze())
            }
            None => {
                let value = source
                    .value()
                    .transpose()?
                    .ok_or_else(|| Fault::internal("a whole request has no input"))?;
                let mut bytes = BytesMut::new();
                encoder.encode(value, &mut bytes)?;
                encoder.end(&Ok(()), &mut bytes)?;
                bytes.freeze()
            }
        };
        source.stop();
        let sink = EventSink::new(futures::sink::drain().sink_map_err(|never| match never {}));
        Ok((http::Request::from_parts(head, Body::full(bytes)), sink))
    } else {
        let (sink, body) = crate::driver::upload::encoding(encoder);
        Ok((http::Request::from_parts(head, body), sink))
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
