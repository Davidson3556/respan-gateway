//! Bodies read whole, as unary responses are, and written back.

use std::marker::PhantomData;

use futures::{StreamExt, TryStreamExt, stream};
use respan_gateway_core::{EventStream, Fault, Upstream};
use respan_gateway_http::Body;
use respan_gateway_http::Encoder;
use respan_gateway_http::bytes::{Bytes, BytesMut};

/// Read a body whole, then its values through `on_body`. Values `on_body`
/// pushes before failing precede its fault. A body over its
/// [`limit`](Body::limit) fails `upstream`.
pub fn read<T: Send + 'static>(
    body: Body,
    on_body: impl FnOnce(Bytes, &mut Vec<T>) -> Result<(), Fault> + Send + 'static,
) -> EventStream<T> {
    let values = async move {
        let mut out = Vec::new();
        let end = bytes(body).await.and_then(|bytes| on_body(bytes, &mut out));
        stream::iter(out.into_iter().map(Ok).chain(end.err().map(Err)))
    };
    EventStream::stream(stream::once(values).flatten())
}

/// `body` collected whole, up to its [`limit`](Body::limit).
pub(crate) async fn bytes(body: Body) -> Result<Bytes, Fault> {
    let limit = body.limit();
    let all = body.try_fold(BytesMut::new(), |mut all, frame| {
        let over = limit.filter(|&limit| all.len() + frame.len() > limit);
        if over.is_none() {
            all.extend_from_slice(&frame);
        }
        futures::future::ready(over.map_or(Ok(all), |limit| Err(self::over(limit))))
    });
    all.await.map(BytesMut::freeze)
}

/// The fault of a provider's answer that holds more than `limit` bytes at
/// once.
pub(crate) fn over(limit: usize) -> Fault {
    Upstream::fault(format!(
        "the provider's answer is over the limit of {limit} bytes"
    ))
}

/// An encoder of a whole body: its outputs of `T` kept until it ends, then
/// written at once through `write`, which returns their bytes or `Err`. A
/// failure writes nothing: it is the format's error writer's to write
/// ([`ClientIo::bind`](crate::ClientIo::bind)). For a stream, written output
/// by output, see [`each`](crate::framing::each).
pub fn collect<T, W, B>(write: W) -> Box<dyn Encoder<T>>
where
    T: Clone + Send + 'static,
    W: FnMut(&[T]) -> Result<B, Fault> + Send + 'static,
    B: AsRef<[u8]> + 'static,
{
    Box::new(Whole {
        write,
        bytes: PhantomData,
        values: Vec::new(),
    })
}

struct Whole<T, W, B> {
    write: W,
    /// What `write` returns, such as a `String` or `Bytes`.
    bytes: PhantomData<fn() -> B>,
    values: Vec<T>,
}

impl<T, W, B> Encoder<T> for Whole<T, W, B>
where
    T: Clone + Send + 'static,
    W: FnMut(&[T]) -> Result<B, Fault> + Send + 'static,
    B: AsRef<[u8]> + 'static,
{
    fn encode(&mut self, value: &T, _: &mut BytesMut) -> Result<(), Fault> {
        self.values.push(value.clone());
        Ok(())
    }

    fn end(&mut self, end: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault> {
        end.clone()?;
        out.extend_from_slice((self.write)(&self.values)?.as_ref());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_whole_body_is_written_once_in_order() {
        let mut whole = collect(|values: &[u32]| Ok(format!("{values:?}")));
        let mut out = BytesMut::new();
        for value in [1, 2] {
            assert!(whole.encode(&value, &mut out).is_ok());
        }
        assert!(out.is_empty());
        assert!(whole.end(&Ok(()), &mut out).is_ok());
        assert_eq!(&out[..], b"[1, 2]");
    }

    #[test]
    fn a_failure_writes_nothing() {
        let mut whole = collect(|values: &[u32]| Ok(format!("{values:?}")));
        let mut out = BytesMut::new();
        assert!(whole.encode(&1, &mut out).is_ok());
        assert!(whole.end(&Err(Upstream::fault("x")), &mut out).is_err());
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn a_body_over_its_limit_fails_upstream() {
        let mut body = Body::full("12345");
        body.limit = Some(4);
        let got: Vec<_> = read(body, |_, _: &mut Vec<()>| Ok(())).collect().await;
        let fault = got.into_iter().next().and_then(Result::err);
        assert!(fault.is_some_and(|f| f.is::<Upstream>()));
    }
}
