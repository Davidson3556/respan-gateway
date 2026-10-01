//! Bodies read whole, as unary responses are, and written back.

use std::marker::PhantomData;

use futures::stream::BoxStream;
use respan_gateway_http::bytes::{Bytes, BytesMut};
use respan_gateway_http::http::{self, StatusCode};
use respan_gateway_http::{Body, Encoder};
use respan_gateway_sdk::Fault;

/// Read a unary response without an HTTP-specific decoder interface.
/// Non-success answers preserve their original head and bytes in a fault.
pub fn read<T: Send + 'static>(
    format: &str,
    response: http::Response<Body>,
    mut on_body: impl FnMut(Bytes, &mut Vec<T>) -> Result<(), Fault> + Send + 'static,
) -> BoxStream<'static, Result<T, Fault>> {
    let (head, body) = response.into_parts();
    let format = format.to_owned();
    crate::formats::read(
        body,
        BytesMut::new(),
        |buf, bytes, _| {
            buf.extend_from_slice(&bytes);
            Ok(())
        },
        move |buf, wire, out| {
            wire?;
            let raw = buf.split().freeze();
            if head.status.is_success() {
                on_body(raw, out)
            } else {
                Err(answer(&format, &head, raw))
            }
        },
    )
}

/// The fault of a provider's answer in `format` that is no success: its
/// code, message and fields, an error's status and `Retry-After`, else a
/// 502. Unless informational (an unasked 101), it carries the answer as
/// `http::Response<Bytes>` ([`Fault::with_answer`]), for `format` to write.
pub fn answer(format: &str, head: &http::response::Parts, body: Bytes) -> Fault {
    let v = crate::formats::sse::failure_json(&body).unwrap_or_default();
    let fault = crate::formats::errors::provider_error(head.status, &v);
    if head.status.is_informational() {
        return fault;
    }
    let fault = match respan_gateway_http::retry_after(&head.headers) {
        Some(wait) => fault.with_retry_after(wait),
        None => fault,
    };
    fault.with_answer(format, http::Response::from_parts(head.clone(), body))
}

/// Read a whole answer as a refusal, including an unsuccessful 2xx handshake.
pub fn refusal<T: Send + 'static>(
    format: &str,
    response: http::Response<Body>,
) -> BoxStream<'static, Result<T, Fault>> {
    let (head, body) = response.into_parts();
    let format = format.to_owned();
    crate::formats::read(
        body,
        BytesMut::new(),
        |buf, bytes, _| {
            buf.extend_from_slice(&bytes);
            Ok(())
        },
        move |buf, wire, _| {
            wire?;
            Err(answer(&format, &head, buf.split().freeze()))
        },
    )
}

/// Consume and discard a body, retaining wire failure and delivery lifetime.
pub fn nothing<T: Send + 'static>(body: Body) -> BoxStream<'static, Result<T, Fault>> {
    crate::formats::read(body, (), |_, _, _| Ok(()), |_, wire, _| wire)
}

/// Encode a finite body at clean EOF; terminal faults propagate unchanged.
pub fn write<T: Clone + Send + 'static>(
    write: impl FnMut(&[T]) -> Result<Bytes, Fault> + Send + 'static,
) -> Box<dyn Encoder<T>> {
    Box::new(WholeWriter {
        values: Vec::new(),
        write,
    })
}
struct WholeWriter<T, F> {
    values: Vec<T>,
    write: F,
}
impl<T: Clone + Send + 'static, F> Encoder<T> for WholeWriter<T, F>
where
    F: FnMut(&[T]) -> Result<Bytes, Fault> + Send + 'static,
{
    fn encode(&mut self, value: &T, _: &mut BytesMut) -> Result<(), Fault> {
        self.values.push(value.clone());
        Ok(())
    }
    fn end(&mut self, end: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault> {
        end.clone()?;
        out.extend_from_slice(&(self.write)(&self.values)?);
        Ok(())
    }
}

/// The one wording of a provider's failure of `status`.
pub(crate) fn answered(status: StatusCode) -> String {
    format!("the provider returned {}", status.as_u16())
}

/// Encodes outputs of `T` through `write`, which returns their bytes or
/// `Err`, and a failure through `fail` ([`Fault::answer`]). A stream is
/// written output by output; a whole body once, at its end.
pub struct Relay<T, W, F, B> {
    write: W,
    fail: F,
    /// What `write` and `fail` return, such as a `String` or `Bytes`.
    bytes: PhantomData<fn() -> B>,
    /// A whole body's outputs, kept until it ends; `None` for a stream.
    whole: Option<Vec<T>>,
}

impl<T, W, F, B> std::fmt::Debug for Relay<T, W, F, B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Relay").finish_non_exhaustive()
    }
}

impl<T, W, F, B> Relay<T, W, F, B>
where
    W: FnMut(&[T]) -> Result<B, Fault> + Send + 'static,
    F: FnMut(&Fault) -> B + Send + 'static,
    B: AsRef<[u8]> + 'static,
{
    /// An encoder for a `stream`, or for a whole body if `stream` is false.
    pub fn new(write: W, fail: F, stream: bool) -> Self {
        Self {
            write,
            fail,
            bytes: PhantomData,
            whole: (!stream).then(Vec::new),
        }
    }
}

impl<T, W, F, B> Encoder<T> for Relay<T, W, F, B>
where
    T: Clone + Send + 'static,
    W: FnMut(&[T]) -> Result<B, Fault> + Send + 'static,
    F: FnMut(&Fault) -> B + Send + 'static,
    B: AsRef<[u8]> + 'static,
{
    fn encode(&mut self, value: &T, out: &mut BytesMut) -> Result<(), Fault> {
        match &mut self.whole {
            Some(whole) => whole.push(value.clone()),
            None => out.extend_from_slice((self.write)(std::slice::from_ref(value))?.as_ref()),
        }
        Ok(())
    }

    fn end(&mut self, end: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault> {
        match (end, self.whole.take()) {
            (Err(f), _) => out.extend_from_slice((self.fail)(f).as_ref()),
            (Ok(()), Some(whole)) => out.extend_from_slice((self.write)(&whole)?.as_ref()),
            (Ok(()), None) => {}
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_whole_body_is_written_once_in_order() {
        let write = |values: &[u32]| Ok(format!("{values:?}"));
        let mut relay = Relay::new(write, |_: &Fault| String::new(), false);
        let mut out = BytesMut::new();
        for value in [1, 2] {
            assert!(relay.encode(&value, &mut out).is_ok());
        }
        assert!(out.is_empty());
        assert!(relay.end(&Ok(()), &mut out).is_ok());
        assert_eq!(&out[..], b"[1, 2]");
    }

    #[test]
    fn a_stream_is_written_value_by_value_and_its_failure_by_fail() {
        let write = |values: &[u32]| Ok(format!("{values:?}"));
        let fail = |f: &Fault| f.message().to_owned();
        let mut relay = Relay::new(write, fail, true);
        let mut out = BytesMut::new();
        assert!(relay.encode(&1, &mut out).is_ok());
        assert!(relay.end(&Err(Fault::upstream("x")), &mut out).is_ok());
        assert_eq!(&out[..], b"[1]x");
    }
}
