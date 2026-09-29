//! Bodies read whole, as unary responses are, and written back.

use std::marker::PhantomData;

use respan_gateway_core::bytes::{Bytes, BytesMut};
use respan_gateway_core::{Decoder, Encoder, Fault};
use respan_gateway_http::http::{self, StatusCode};
use respan_gateway_http::{Answer, Ask, bodiless};

/// A decoder for a body of `format` read whole: a success's bytes go to
/// `on_body` at the end; any other answer ends `Err` ([`answer`]).
pub fn decoder<T, F>(
    format: &str,
    head: &http::response::Parts,
    mut on_body: F,
) -> Box<dyn Decoder<T>>
where
    T: 'static,
    F: FnMut(Bytes, &mut Vec<T>) -> Result<(), Fault> + Send + 'static,
{
    let (head, format) = (head.clone(), format.to_owned());
    let on_body = move |raw: Bytes, out: &mut Vec<T>| match head.status.is_success() {
        false => Err(answer(&format, &head, raw)),
        true => on_body(raw, out),
    };
    let buf = BytesMut::new();
    Box::new(Whole { buf, on_body })
}

/// The fault of a provider's answer in `format` that is no success: its
/// code, message and fields, an error's status and `Retry-After`, else a
/// 502. Unless informational (an unasked 101), it carries the answer as
/// `http::Response<Bytes>` ([`Fault::with_answer`]), for `format` to write.
pub fn answer(format: &str, head: &http::response::Parts, body: Bytes) -> Fault {
    let v = crate::sse::failure_json(&body).unwrap_or_default();
    let fault = crate::common::provider_error(head.status, &v);
    if head.status.is_informational() {
        return fault;
    }
    let fault = match respan_gateway_http::retry_after(&head.headers) {
        Some(wait) => fault.with_retry_after(wait),
        None => fault,
    };
    fault.with_answer(format, http::Response::from_parts(head.clone(), body))
}

/// A decoder of nothing, for a response with no body
/// ([`bodiless`]): it ends at once, whatever
/// bytes came.
pub fn nothing<T: 'static>() -> Box<dyn Decoder<T>> {
    Box::new(Nothing)
}

struct Nothing;

impl<T> Decoder<T> for Nothing {
    fn decode(&mut self, _bytes: Bytes, _out: &mut Vec<T>) -> Result<(), Fault> {
        Ok(())
    }

    fn end(&mut self, _out: &mut Vec<T>) -> Result<(), Fault> {
        Ok(())
    }
}

/// A request encoder for a protocol whose calls carry one body, not
/// streamed inputs: it fails to open with
/// [`codes::CONFIG`](respan_gateway_core::codes::CONFIG).
pub fn unstreamable<T: 'static>(format: &str) -> Box<dyn Encoder<T, ()>> {
    let message = format!("a call in {format} carries one body, not streamed inputs");
    Box::new(Plain {
        unopenable: Some(message),
        ..Plain::default()
    })
}

/// An encoder of a response with no body
/// ([`bodiless`]): its head goes at once and
/// nothing more is read ([`Encoder::open`]).
pub fn no_body<T: 'static>() -> Box<dyn Encoder<T, ()>> {
    Box::new(Plain {
        done: true,
        ..Plain::default()
    })
}

/// An encoder of a refusal a protocol cannot write, such as a fault whose
/// status is no error's: it fails at the end, so the adapter writes it.
pub fn unwritable<T: 'static>() -> Box<dyn Encoder<T, ()>> {
    Box::new(Plain {
        fails: true,
        ..Plain::default()
    })
}

/// Whether `status` says an error, as a refusal's must to be written.
fn is_error(status: u16) -> bool {
    (400..600).contains(&status)
}

/// An encoder that writes `body` at the end, however it ends, and no
/// value: a refusal's.
pub fn written<T: 'static>(body: Bytes) -> Box<dyn Encoder<T, ()>> {
    Box::new(Plain {
        body,
        ..Plain::default()
    })
}

/// A refusal of `fault` in `format` under `head`, to the request `ask`: the
/// provider's answer as it came (then `answered` labels the head), else
/// `words`; no body where HTTP has none; unwritable for a status no error's.
pub(crate) fn refusal<T: 'static>(
    format: &str,
    head: &mut Answer,
    ask: &Ask,
    fault: &Fault,
    answered: impl FnOnce(&mut Answer),
    words: impl FnOnce(&mut Answer) -> Bytes,
) -> Box<dyn Encoder<T, ()>> {
    let body = match respan_gateway_http::answered(format, fault, head) {
        Some(body) => {
            answered(head);
            body
        }
        None if !is_error(respan_gateway_http::status(fault).as_u16()) => return unwritable(),
        None => words(head),
    };
    match bodiless(&ask.method, head.status) {
        true => written(Bytes::new()),
        false => written(body),
    }
}

/// Writes no value: `body` at the end, or the end's failure if `fails`;
/// fails to open with `unopenable`; done before it began if `done`.
#[derive(Default)]
struct Plain {
    body: Bytes,
    unopenable: Option<String>,
    fails: bool,
    done: bool,
}

impl<T> Encoder<T, ()> for Plain {
    fn head(&mut self, _first: Option<&T>) {}

    fn open(&mut self, _out: &mut BytesMut) -> Result<bool, Fault> {
        match &self.unopenable {
            Some(message) => Err(Fault::config(message.clone())),
            None => Ok(self.done),
        }
    }

    fn encode(&mut self, _value: &T, _out: &mut BytesMut) -> Result<(), Fault> {
        Ok(())
    }

    fn end(&mut self, end: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault> {
        if self.fails {
            return end.clone();
        }
        out.extend_from_slice(&std::mem::take(&mut self.body));
        Ok(())
    }
}

/// Hands a whole body to `on_body` at its end.
struct Whole<F> {
    buf: BytesMut,
    on_body: F,
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
/// `Err`, and a failure through `fail` ([`Fault::answer`]). A stream is
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

impl<T, W, F, B> Encoder<T, ()> for Relay<T, W, F, B>
where
    T: Clone + Send + 'static,
    W: FnMut(&[T]) -> Result<B, Fault> + Send + 'static,
    F: FnMut(&Fault) -> B + Send + 'static,
    B: AsRef<[u8]> + 'static,
{
    fn head(&mut self, _first: Option<&T>) {}

    fn open(&mut self, out: &mut BytesMut) -> Result<bool, Fault> {
        out.extend_from_slice(&self.open);
        Ok(false)
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
        assert!(relay.open(&mut out).is_ok());
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
