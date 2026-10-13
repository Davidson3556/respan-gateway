//! How a format's values become a body's frames and back: an [`Encoder`]
//! writes values as frames, a [`Decoder`] reads frames as values, and the
//! binding drives them. A closure is either; [`Sse`] and [`Whole`] frame
//! what another reads or writes.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context, Poll, ready};

use bytes::{Bytes, BytesMut};
use futures::{Stream, TryStreamExt};
use respan_gateway_core::{EventStream, Fault, Upstream};

use crate::Body;

/// Writes values of `T` as frames: the chunks of a body, or the messages of
/// a live session. A closure `FnMut(&T, &mut Vec<Bytes>) -> Result<(),
/// Fault>` is one.
pub trait Encoder<T>: Send + 'static {
    /// The frames of what comes before any value, such as a quiet stream's
    /// opening comment.
    fn start(&mut self, out: &mut Vec<Bytes>) -> Result<(), Fault> {
        let _ = out;
        Ok(())
    }

    /// Write `value` as zero or more frames. What it pushed before failing
    /// is not sent.
    fn encode(&mut self, value: &T, out: &mut Vec<Bytes>) -> Result<(), Fault>;

    /// The values ended as `end` says: write what closes the body, if
    /// anything. `Err` fails it in place of how it ended.
    fn end(&mut self, end: Result<(), Fault>, out: &mut Vec<Bytes>) -> Result<(), Fault> {
        let _ = out;
        end
    }
}

impl<T, F> Encoder<T> for F
where
    F: FnMut(&T, &mut Vec<Bytes>) -> Result<(), Fault> + Send + 'static,
{
    fn encode(&mut self, value: &T, out: &mut Vec<Bytes>) -> Result<(), Fault> {
        self(value, out)
    }
}

/// Reads frames as values of `T`: a body's chunks, a live session's
/// messages, or, behind [`Sse`] or [`Whole`], its events or the whole of it.
/// A closure `FnMut(Bytes, &mut Vec<T>) -> Result<(), Fault>` is one.
pub trait Decoder<T>: Send + 'static {
    /// The values of what comes before any frame, such as a head that must
    /// reach its reader before the body speaks.
    fn start(&mut self, out: &mut Vec<T>) -> Result<(), Fault> {
        let _ = out;
        Ok(())
    }

    /// The values of one frame. Those pushed before failing come before the
    /// failure.
    fn decode(&mut self, raw: Bytes, out: &mut Vec<T>) -> Result<(), Fault>;

    /// The frames ended as `end` says, a broken body's fault or `Ok`: the
    /// values of what was held, if it ended well. `Err` fails it.
    fn end(&mut self, end: Result<(), Fault>, out: &mut Vec<T>) -> Result<(), Fault> {
        let _ = out;
        end
    }

    /// The most bytes it may hold between frames, its body's limit; what
    /// holds bytes, as [`Sse`] and [`Whole`] do, fails `upstream` past it.
    fn limit(&mut self, bytes: Option<usize>) {
        let _ = bytes;
    }

    /// Whether it has read all it will, as a protocol whose answer is
    /// complete before its body ends: the rest goes unread, and the values
    /// end well.
    fn done(&self) -> bool {
        false
    }
}

impl<T, F> Decoder<T> for F
where
    F: FnMut(Bytes, &mut Vec<T>) -> Result<(), Fault> + Send + 'static,
{
    fn decode(&mut self, raw: Bytes, out: &mut Vec<T>) -> Result<(), Fault> {
        self(raw, out)
    }
}

/// Server-sent events: a [`Decoder`] that splits frames into events and
/// gives its inner decoder one event at a time. At the end, a last event of
/// whole lines is read; one cut mid-line is not.
pub struct Sse<D> {
    inner: D,
    parser: respan_gateway_sse::Parser,
}

impl<D> std::fmt::Debug for Sse<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sse").finish_non_exhaustive()
    }
}

impl<D> Sse<D> {
    /// Events of a body, each read by `inner`.
    pub fn new(inner: D) -> Self {
        Self {
            inner,
            parser: respan_gateway_sse::Parser::new(None),
        }
    }
}

impl<T, D: Decoder<T>> Decoder<T> for Sse<D> {
    fn start(&mut self, out: &mut Vec<T>) -> Result<(), Fault> {
        self.inner.start(out)
    }

    fn decode(&mut self, raw: Bytes, out: &mut Vec<T>) -> Result<(), Fault> {
        for event in self.parser.push(raw)? {
            self.inner.decode(event, out)?;
        }
        Ok(())
    }

    fn end(&mut self, end: Result<(), Fault>, out: &mut Vec<T>) -> Result<(), Fault> {
        if end.is_ok() {
            for event in self.parser.finish() {
                self.inner.decode(event, out)?;
            }
        }
        self.inner.end(end, out)
    }

    fn limit(&mut self, bytes: Option<usize>) {
        self.parser = respan_gateway_sse::Parser::new(bytes);
        self.inner.limit(bytes);
    }

    fn done(&self) -> bool {
        self.inner.done()
    }
}

/// The whole body at once, either way. As a [`Decoder`], it holds every
/// frame and gives its inner decoder the whole body when it ends; as an
/// [`Encoder`] of a function `FnMut(&[T]) -> Result<B, Fault>`, it holds
/// every value and writes them all when they end. A failure writes nothing.
pub struct Whole<X, T = Bytes> {
    inner: X,
    held: Vec<T>,
    limit: Option<usize>,
    size: usize,
}

impl<X, T> std::fmt::Debug for Whole<X, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Whole")
            .field("held", &self.held.len())
            .finish_non_exhaustive()
    }
}

impl<X, T> Whole<X, T> {
    /// Hold all of it for `inner`.
    pub fn new(inner: X) -> Self {
        Self {
            inner,
            held: Vec::new(),
            limit: None,
            size: 0,
        }
    }
}

impl<T, D: Decoder<T>> Decoder<T> for Whole<D> {
    fn start(&mut self, out: &mut Vec<T>) -> Result<(), Fault> {
        self.inner.start(out)
    }

    fn decode(&mut self, raw: Bytes, _: &mut Vec<T>) -> Result<(), Fault> {
        self.size += raw.len();
        if let Some(limit) = self.limit.filter(|&limit| self.size > limit) {
            return Err(over(limit));
        }
        self.held.push(raw);
        Ok(())
    }

    fn end(&mut self, end: Result<(), Fault>, out: &mut Vec<T>) -> Result<(), Fault> {
        end?;
        let whole = match self.held.len() {
            1 => self.held.pop().unwrap_or_default(),
            _ => Bytes::from(self.held.concat()),
        };
        self.inner.decode(whole, out)?;
        self.inner.end(Ok(()), out)
    }

    fn limit(&mut self, bytes: Option<usize>) {
        self.limit = bytes;
    }
}

impl<T, F, B> Encoder<T> for Whole<F, T>
where
    T: Clone + Send + 'static,
    F: FnMut(&[T]) -> Result<B, Fault> + Send + 'static,
    B: Into<Bytes>,
{
    fn encode(&mut self, value: &T, _: &mut Vec<Bytes>) -> Result<(), Fault> {
        self.held.push(value.clone());
        Ok(())
    }

    fn end(&mut self, end: Result<(), Fault>, out: &mut Vec<Bytes>) -> Result<(), Fault> {
        end?;
        out.push((self.inner)(&self.held)?.into());
        Ok(())
    }
}

impl<T: 'static> dyn Encoder<T> {
    /// `values` written whole, the end included, as one body.
    pub fn encode_all(&mut self, values: &[T]) -> Result<Bytes, Fault> {
        let mut frames = Vec::new();
        self.start(&mut frames)?;
        for value in values {
            self.encode(value, &mut frames)?;
        }
        self.end(Ok(()), &mut frames)?;
        Ok(match frames.len() {
            1 => frames.pop().unwrap_or_default(),
            _ => frames.concat().into(),
        })
    }
}

/// One value of a whole body, read by `decoder` when first asked for, as a
/// whole request's one input is.
pub(crate) fn decode_one<T>(bytes: Bytes, decoder: Box<dyn Decoder<T>>) -> EventStream<T>
where
    T: Clone + Send + Sync + 'static,
{
    let decoder = Mutex::new(Some(decoder));
    EventStream::lazy(move || {
        let taken = decoder.lock().unwrap_or_else(|e| e.into_inner()).take();
        let mut decoder = taken.ok_or_else(|| Fault::internal("a whole input read twice"))?;
        let mut out = Vec::new();
        decoder.start(&mut out)?;
        decoder.decode(bytes.clone(), &mut out)?;
        decoder.end(Ok(()), &mut out)?;
        match <[T; 1]>::try_from(out) {
            Ok([one]) => Ok(one),
            Err(out) => Err(Fault::internal(format!(
                "a whole request decoded to {} inputs, not one",
                out.len()
            ))),
        }
    })
}

/// The values `decoder` reads of `body`, frame by frame, its limit the
/// body's, until it is [`done`](Decoder::done). Values read before a
/// failure come before it. It reads no frame
/// ahead while values remain, and yields after [`FRAMES_PER_POLL`] frames
/// that read as nothing.
pub(crate) fn decode<T: Send + 'static>(
    body: Body,
    mut decoder: Box<dyn Decoder<T>>,
) -> EventStream<T> {
    decoder.limit(body.limit());
    let mut ready = Vec::new();
    let started = decoder.start(&mut ready);
    let mut ready: VecDeque<_> = ready.into_iter().map(Ok).collect();
    if let Err(fault) = &started {
        ready.push_back(Err(fault.clone()));
    }
    EventStream::stream(Decoding {
        body,
        decoder,
        ready,
        ended: started.is_err(),
    })
}

/// The most frames that read as nothing one poll of [`decode`] reads before
/// it yields: what tokio's cooperative budget allows one poll of a task
/// (128), so a body always ready, of comments alone say, cannot hold its
/// thread.
const FRAMES_PER_POLL: usize = 128;

struct Decoding<T> {
    body: Body,
    decoder: Box<dyn Decoder<T>>,
    ready: VecDeque<Result<T, Fault>>,
    ended: bool,
}

impl<T> Unpin for Decoding<T> {}

impl<T: Send + 'static> Stream for Decoding<T> {
    type Item = Result<T, Fault>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let mut frames = 0;
        loop {
            if let Some(value) = this.ready.pop_front() {
                return Poll::Ready(Some(value));
            }
            if this.ended {
                return Poll::Ready(None);
            }
            if frames == FRAMES_PER_POLL {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            let frame = ready!(Pin::new(&mut this.body).poll_next(cx));
            frames += 1;
            let mut values = Vec::new();
            let end = match frame {
                Some(Ok(bytes)) => match this.decoder.decode(bytes, &mut values) {
                    Err(fault) => Some(Err(fault)),
                    Ok(()) => this.decoder.done().then_some(Ok(())),
                },
                tail => {
                    let wire = tail.transpose().map(|_| ());
                    let ended = this.decoder.end(wire.clone(), &mut values);
                    Some(wire.and(ended))
                }
            };
            this.ready.extend(values.into_iter().map(Ok));
            if let Some(end) = end {
                this.ended = true;
                if let Err(fault) = end {
                    this.ready.push_back(Err(fault));
                }
            }
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[test]
    fn a_whole_body_is_written_once_in_order() {
        let mut whole = Whole::new(|values: &[u32]| Ok(format!("{values:?}")));
        let mut out = Vec::new();
        for value in [1, 2] {
            assert!(whole.encode(&value, &mut out).is_ok());
        }
        assert!(out.is_empty());
        assert!(Encoder::end(&mut whole, Ok(()), &mut out).is_ok());
        assert_eq!(out, [Bytes::from_static(b"[1, 2]")]);
    }

    #[test]
    fn a_failure_writes_nothing() {
        let mut whole = Whole::new(|values: &[u32]| Ok(format!("{values:?}")));
        let mut out = Vec::new();
        assert!(whole.encode(&1, &mut out).is_ok());
        let ended = Encoder::end(&mut whole, Err(Upstream::fault("x")), &mut out);
        assert!(ended.is_err());
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn a_body_over_its_limit_fails_upstream() {
        let mut body = Body::full("12345");
        body.limit = Some(4);
        let whole = Whole::new(|_: Bytes, _: &mut Vec<()>| Ok(()));
        let got: Vec<_> = decode(body, Box::new(whole)).collect().await;
        let fault = got.into_iter().next().and_then(Result::err);
        assert!(fault.is_some_and(|f| f.is::<Upstream>()));
    }

    #[tokio::test]
    async fn a_stream_stops_at_its_first_failure() {
        let events = Sse::new(|raw: Bytes, out: &mut Vec<Bytes>| {
            if respan_gateway_sse::event_data(&raw).is_some() {
                return Err(Upstream::fault("unreadable"));
            }
            out.push(raw);
            Ok(())
        });
        let body = Body::full(Bytes::from_static(b": ping\n\ndata: x\n\ndata: y\n\n"));
        let out: Vec<_> = decode(body, Box::new(events)).collect().await;
        assert!(out.len() == 2 && out[0].is_ok() && out[1].is_err());
    }
}
