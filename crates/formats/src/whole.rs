//! Bodies read whole, as unary responses are, and written back.

use std::marker::PhantomData;

use respan_gateway_core::bytes::{Bytes, BytesMut};
use respan_gateway_core::http::{self, StatusCode};
use respan_gateway_core::{Decoder, Encoder, Fault};

/// A decoder for a body of `format` read whole: a success's bytes go to
/// `on_body` at the end; a failure ends `Err` with the provider's status,
/// code, message and error fields, carrying its body ([`Fault::with_raw`]).
pub fn decoder<T, F>(
    format: &str,
    head: &http::response::Parts,
    mut on_body: F,
) -> Box<dyn Decoder<T>>
where
    T: 'static,
    F: FnMut(Bytes, &mut Vec<T>) -> Result<(), Fault> + Send + 'static,
{
    let (status, format) = (head.status, format.to_owned());
    Box::new(Whole::new(
        move |raw: Bytes, out: &mut Vec<T>| match status.is_success() {
            false => {
                let v = crate::sse::failure_json(&raw).unwrap_or_default();
                let fault = crate::common::provider_error(status, &v);
                Err(fault.with_raw(&format, raw))
            }
            true => on_body(raw, out),
        },
    ))
}

/// Hands a whole body to `on_body` at its end.
struct Whole<F> {
    buf: BytesMut,
    on_body: F,
}

impl<F> Whole<F> {
    fn new(on_body: F) -> Self {
        let buf = BytesMut::new();
        Self { buf, on_body }
    }
}

impl<T, F> Decoder<T> for Whole<F>
where
    F: FnMut(Bytes, &mut Vec<T>) -> Result<(), Fault> + Send + 'static,
{
    fn decode(&mut self, bytes: Bytes, _out: &mut Vec<T>) -> Result<(), Fault> {
        self.buf.extend_from_slice(&bytes);
        Ok(())
    }

    fn end(&mut self, out: &mut Vec<T>) -> Result<(), Fault> {
        let raw = self.buf.split().freeze();
        (self.on_body)(raw, out)
    }
}

/// The one wording of a provider's failure of `status`.
pub(crate) fn answered(status: StatusCode) -> String {
    format!("the provider returned {}", status.as_u16())
}

/// Encodes outputs of `T` through `write`, which returns their bytes or
/// `Err`, and a failure through `fail` ([`Fault::raw`]). A stream is
/// written output by output; a whole body once, at its end.
pub struct Relay<T, W, F, B> {
    write: W,
    fail: F,
    /// What `write` and `fail` return, such as a `String` or `Bytes`.
    bytes: PhantomData<fn() -> B>,
    /// A whole body's outputs, kept until it ends; `None` for a stream.
    whole: Option<Vec<T>>,
    /// The body's opening bytes.
    open: Bytes,
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
            open: Bytes::new(),
        }
    }

    /// Open the body with `bytes` ([`Encoder::open`]), such as an SSE comment.
    pub fn with_open(mut self, bytes: impl Into<Bytes>) -> Self {
        self.open = bytes.into();
        self
    }
}

impl<T, W, F, B> Encoder<T> for Relay<T, W, F, B>
where
    T: Clone + Send + 'static,
    W: FnMut(&[T]) -> Result<B, Fault> + Send + 'static,
    F: FnMut(&Fault) -> B + Send + 'static,
    B: AsRef<[u8]> + 'static,
{
    fn open(&mut self, out: &mut BytesMut) {
        out.extend_from_slice(&self.open);
    }

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
    fn a_relay_opens_with_the_bytes_it_is_given() {
        let write = |_: &[u32]| Ok(String::new());
        let relay = Relay::new(write, |_: &Fault| String::new(), true);
        let mut relay = relay.with_open(": open\n\n");
        let mut out = BytesMut::new();
        relay.open(&mut out);
        assert_eq!(&out[..], b": open\n\n");
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
