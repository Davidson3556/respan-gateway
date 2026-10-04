//! Bodies read whole, as unary responses are, and written back.

use std::marker::PhantomData;

use futures::stream::BoxStream;
use respan_gateway_http::bytes::{Bytes, BytesMut};
use respan_gateway_http::http::{self, StatusCode};
use respan_gateway_http::{Body, Encoder, Status};
use respan_gateway_sdk::{Fault, Upstream};

/// Read a unary response without an HTTP-specific decoder interface.
/// Non-success answers preserve their original head and bytes in a fault
/// ([`answer`]). A body over its [`limit`](Body::limit) fails `upstream`.
pub fn read<T: Send + 'static>(
    format: &str,
    response: http::Response<Body>,
    mut on_body: impl FnMut(Bytes, &mut Vec<T>) -> Result<(), Fault> + Send + 'static,
) -> BoxStream<'static, Result<T, Fault>> {
    let (head, body) = response.into_parts();
    let format = format.to_owned();
    read_all(body, move |raw, out| {
        if head.status.is_success() {
            on_body(raw, out)
        } else {
            Err(answer(&format, &head, raw))
        }
    })
}

/// Read a whole answer as a refusal, including an unsuccessful 2xx handshake.
/// A body over its [`limit`](Body::limit) fails `upstream`.
pub fn refusal<T: Send + 'static>(
    format: &str,
    response: http::Response<Body>,
) -> BoxStream<'static, Result<T, Fault>> {
    let (head, body) = response.into_parts();
    let format = format.to_owned();
    read_all(body, move |raw, _| Err(answer(&format, &head, raw)))
}

/// `body` collected whole, up to its limit, for `whole` to read at its end.
fn read_all<T: Send + 'static>(
    body: Body,
    whole: impl FnOnce(Bytes, &mut Vec<T>) -> Result<(), Fault> + Send + 'static,
) -> BoxStream<'static, Result<T, Fault>> {
    let limit = body.limit();
    crate::formats::read(
        body,
        BytesMut::new(),
        move |buf, bytes, _| match limit.filter(|&l| buf.len() + bytes.len() > l) {
            Some(limit) => Err(over(limit)),
            None => {
                buf.extend_from_slice(&bytes);
                Ok(())
            }
        },
        move |buf, wire, out| {
            wire?;
            whole(buf.split().freeze(), out)
        },
    )
}

/// The fault of a provider's answer that holds more than `limit` bytes at
/// once.
pub(crate) fn over(limit: usize) -> Fault {
    Upstream::fault(format!(
        "the provider's answer is over the limit of {limit} bytes"
    ))
}

/// The fault of a provider's answer in `format` that is no success: an
/// [`Upstream`] one, saying its status, with that status if an error's,
/// else a 502, and its `Retry-After`. Unless informational (an unasked
/// 101), it carries the answer as `http::Response<Bytes>`
/// ([`crate::ProviderError`]), for a format to read its words from or to
/// write as it came. The body's meaning is the format's: this reads none.
pub fn answer(format: &str, head: &http::response::Parts, body: Bytes) -> Fault {
    let to = match head.status.is_client_error() || head.status.is_server_error() {
        true => head.status,
        false => StatusCode::BAD_GATEWAY,
    };
    let fault = Upstream::fault(answered(head.status)).with(Status(to));
    if head.status.is_informational() {
        return fault;
    }
    let fault = match respan_gateway_http::retry_after(&head.headers) {
        Some(wait) => fault.with(respan_gateway_sdk::RetryAfter(wait)),
        None => fault,
    };
    fault.with(crate::ProviderError::new(
        format,
        http::Response::from_parts(head.clone(), body),
    ))
}

/// The one wording of a provider's failure of `status`, which says no more
/// ([`answer`]).
pub fn answered(status: StatusCode) -> String {
    format!("the provider returned {}", status.as_u16())
}

/// An encoder of a whole body: its outputs of `T` kept until it ends, then
/// written at once through `write`, which returns their bytes or `Err`; a
/// failure is written through `fail`. For a stream, written output by
/// output, see [`each`](crate::formats::each).
pub fn collect<T, W, F, B>(write: W, fail: F) -> Box<dyn Encoder<T>>
where
    T: Clone + Send + 'static,
    W: FnMut(&[T]) -> Result<B, Fault> + Send + 'static,
    F: FnMut(&Fault) -> B + Send + 'static,
    B: AsRef<[u8]> + 'static,
{
    Box::new(Whole {
        write,
        fail,
        bytes: PhantomData,
        values: Vec::new(),
    })
}

struct Whole<T, W, F, B> {
    write: W,
    fail: F,
    /// What `write` and `fail` return, such as a `String` or `Bytes`.
    bytes: PhantomData<fn() -> B>,
    values: Vec<T>,
}

impl<T, W, F, B> Encoder<T> for Whole<T, W, F, B>
where
    T: Clone + Send + 'static,
    W: FnMut(&[T]) -> Result<B, Fault> + Send + 'static,
    F: FnMut(&Fault) -> B + Send + 'static,
    B: AsRef<[u8]> + 'static,
{
    fn encode(&mut self, value: &T, _: &mut BytesMut) -> Result<(), Fault> {
        self.values.push(value.clone());
        Ok(())
    }

    fn end(&mut self, end: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault> {
        match end {
            Err(f) => out.extend_from_slice((self.fail)(f).as_ref()),
            Ok(()) => out.extend_from_slice((self.write)(&self.values)?.as_ref()),
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
        let mut whole = collect(write, |_: &Fault| String::new());
        let mut out = BytesMut::new();
        for value in [1, 2] {
            assert!(whole.encode(&value, &mut out).is_ok());
        }
        assert!(out.is_empty());
        assert!(whole.end(&Ok(()), &mut out).is_ok());
        assert_eq!(&out[..], b"[1, 2]");
    }

    #[test]
    fn a_failure_is_written_by_fail_alone() {
        let write = |values: &[u32]| Ok(format!("{values:?}"));
        let fail = |f: &Fault| f.message().to_owned();
        let mut whole = collect(write, fail);
        let mut out = BytesMut::new();
        assert!(whole.encode(&1, &mut out).is_ok());
        assert!(whole.end(&Err(Upstream::fault("x")), &mut out).is_ok());
        assert_eq!(&out[..], b"x");
    }

    #[tokio::test]
    async fn a_body_over_its_limit_fails_upstream() {
        use futures::StreamExt;
        let mut body = Body::full("12345");
        body.limit = Some(4);
        let head = http::Response::new(()).into_parts().0;
        let read = read(
            "f",
            http::Response::from_parts(head, body),
            |_, _: &mut Vec<()>| Ok(()),
        );
        let got: Vec<_> = read.collect().await;
        let fault = got.into_iter().next().and_then(Result::err);
        assert!(fault.is_some_and(|f| f.is::<Upstream>()));
    }
}
