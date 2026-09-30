//! Frames into events: a client's later inputs and a provider's outputs.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll, ready};

use bytes::Bytes;
use futures::stream::Stream;
use futures::{FutureExt, StreamExt};
use respan_gateway_core::Fault;

use crate::{Decoder, Frames};

/// Where a streamed call's request encoder panicked, if it did: the
/// transport may read the call's body where nothing unwinds to the call, so
/// the response reads it here (B2).
pub(crate) type Failure = Arc<OnceLock<Fault>>;

/// The most bytes of an error answer's body that are read before it is
/// classified. An error answer's body is only relayed or translated, and
/// provider errors are a few KB of JSON or HTML, so 1 MiB is far past any
/// real one; the cap bounds the memory a huge or never-ending body can take,
/// as a time limit is the transport's job.
pub(crate) const ERROR_BODY_CAP: usize = 1 << 20;

/// Empty or ignored frames still consume work, even when they add no bytes.
const MAX_READY_FRAMES: usize = 128;

/// Builds a decoder once the first frame comes.
type Make<T> = Box<dyn FnOnce() -> Box<dyn Decoder<T>> + Send>;

/// A body read into events through a [`Decoder`], each frame's in turn. A
/// provider's outputs say `Pending` between frames, so what the gateway finds
/// ready at once, and takes as one batch, is one frame's (rule 5).
///
/// What was decoded before a failure comes before it; the failure ends it.
pub(crate) struct Reader<T> {
    body: Frames,
    make: Option<Make<T>>,
    decoder: Option<Box<dyn Decoder<T>>>,
    /// Set if the call's request encoder panicked, which fails the body.
    failed: Option<Failure>,
    ready: VecDeque<Result<T, Fault>>,
    ended: bool,
    /// Whether its failure is the body's, broken off, not the decoder's.
    broke: bool,
    /// Whether it says `Pending` between frames.
    apart: bool,
    /// Whether it said `Pending` before reading the next frame.
    yielded: bool,
}

/// Never pinned in place: its fields are moved, not projected.
impl<T> Unpin for Reader<T> {}

impl<T: Send + 'static> Reader<T> {
    /// A streaming request's inputs: its data frames, each a message, even
    /// an empty one, through the decoder `make` builds at the first.
    pub(crate) fn inputs(body: Frames, make: Make<T>) -> Self {
        Self::new(body, Some(make), None, None, false)
    }

    /// A provider's outputs, through `decoder`, failing if the call's body
    /// did (`failed`).
    pub(crate) fn outputs(body: Frames, decoder: Box<dyn Decoder<T>>, failed: Failure) -> Self {
        Self::new(body, None, Some(decoder), Some(failed), true)
    }

    fn new(
        body: Frames,
        make: Option<Make<T>>,
        decoder: Option<Box<dyn Decoder<T>>>,
        failed: Option<Failure>,
        apart: bool,
    ) -> Self {
        Self {
            body,
            make,
            decoder,
            failed,
            ready: VecDeque::new(),
            ended: false,
            broke: false,
            apart,
            yielded: false,
        }
    }

    /// Read the body, decoding each frame as it comes, until the decoder
    /// decides: its first output (an answer relayed as it came, which then
    /// streams from that frame on), its end, a failure or a broken body.
    ///
    /// While nothing is decided, at most [`ERROR_BODY_CAP`] bytes are read:
    /// once the frames read pass it (the frame that passed it is decoded
    /// whole) the decoder is given the end, as if the provider had sent no
    /// more. It reads a shortened body as it would a whole one, so an error
    /// answer keeps its verdict; a body up to the cap is read byte for byte.
    /// An answer decided before the cap is never cut.
    pub(crate) async fn decide(&mut self) {
        let mut read = 0;
        let mut frames = 0;
        while self.ready.is_empty() && !self.ended {
            if frames == MAX_READY_FRAMES {
                tokio::task::yield_now().await;
                frames = 0;
            }
            let frame = self.body.next().await;
            frames += 1;
            read += frame
                .as_ref()
                .and_then(|f| f.as_ref().ok())
                .map_or(0, Bytes::len);
            self.read(frame);
            if read > ERROR_BODY_CAP && self.ready.is_empty() && !self.ended {
                self.read(None);
            }
        }
    }

    /// The provider's refusal at its head (rule 2), as far as its answer
    /// is ready, yielding between batches: its decoder's failure before any
    /// output, the body unbroken, as a provider's error answer is.
    pub(crate) async fn refused(&mut self) -> Option<Fault> {
        let mut frames = 0;
        while self.ready.is_empty() && !self.ended {
            if frames == MAX_READY_FRAMES {
                tokio::task::yield_now().await;
                frames = 0;
            }
            let Some(frame) = self.body.next().now_or_never() else {
                break;
            };
            self.read(frame);
            frames += 1;
        }
        match self.ready.front() {
            Some(Err(_)) if !self.broke => self.ready.pop_front().and_then(Result::err),
            _ => None,
        }
    }

    /// The decoder, built at the first frame.
    fn decoder(&mut self) -> &mut Box<dyn Decoder<T>> {
        let make = &mut self.make;
        self.decoder.get_or_insert_with(|| match make.take() {
            Some(make) => make(),
            None => Box::new(Unmade),
        })
    }

    /// Decode `frame`, or the body's end (`None`), into `ready`: what it
    /// decoded, then the end if it came.
    fn read(&mut self, mut frame: Option<Result<Bytes, Fault>>) {
        // A request encoder that panicked, unseen by the transport, fails it.
        if let Some(fault) = self.failed.as_ref().and_then(|f| f.get()) {
            frame = Some(Err(fault.clone()));
        }
        let mut values = Vec::new();
        let end = match frame {
            Some(Ok(bytes)) => self.decoder().decode(bytes, &mut values).err().map(Err),
            Some(Err(fault)) => {
                self.broke = true;
                Some(Err(fault))
            }
            None => Some(self.decoder().end(&mut values)),
        };
        self.ready.extend(values.into_iter().map(Ok));
        if let Some(end) = end {
            self.ended = true;
            if let Err(fault) = end {
                self.ready.push_back(Err(fault));
            }
        }
    }
}

impl<T: Send + 'static> Stream for Reader<T> {
    type Item = Result<T, Fault>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        let mut frames = 0;
        loop {
            if let Some(value) = this.ready.pop_front() {
                return Poll::Ready(Some(value));
            }
            if this.ended {
                return Poll::Ready(None);
            }
            if frames == MAX_READY_FRAMES {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            if this.apart && !this.yielded {
                this.yielded = true;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            let frame = ready!(this.body.poll_next_unpin(cx));
            this.yielded = false;
            this.read(frame);
            frames += 1;
        }
    }
}

/// A decoder that was never made: it fails. Not reached, as a reader has
/// either a decoder or a way to make one.
struct Unmade;

impl<T> Decoder<T> for Unmade {
    fn decode(&mut self, _: Bytes, _: &mut Vec<T>) -> Result<(), Fault> {
        Err(Fault::internal("a body read with no decoder"))
    }

    fn end(&mut self, _: &mut Vec<T>) -> Result<(), Fault> {
        Err(Fault::internal("a body read with no decoder"))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures::stream;

    use super::*;

    struct Quiet;
    impl Decoder<()> for Quiet {
        fn decode(&mut self, _: Bytes, _: &mut Vec<()>) -> Result<(), Fault> {
            Ok(())
        }
        fn end(&mut self, _: &mut Vec<()>) -> Result<(), Fault> {
            Err(Fault::upstream("refused"))
        }
    }

    fn empty_frames(read: Arc<AtomicUsize>) -> Frames {
        stream::iter(0..10_000)
            .map(move |_| {
                read.fetch_add(1, Ordering::SeqCst);
                Ok(Bytes::new())
            })
            .boxed()
    }

    #[tokio::test]
    async fn refusal_peeking_yields_without_losing_a_late_refusal() {
        let read = Arc::new(AtomicUsize::new(0));
        let mut reader = Reader::outputs(
            empty_frames(read.clone()),
            Box::new(Quiet),
            Failure::default(),
        );
        let mut refused = reader.refused().boxed();
        assert!(refused.as_mut().now_or_never().is_none());
        assert!(read.load(Ordering::SeqCst) <= MAX_READY_FRAMES);
        assert_eq!(refused.await, Some(Fault::upstream("refused")));
        assert_eq!(read.load(Ordering::SeqCst), 10_000);
    }

    #[tokio::test]
    async fn empty_error_frames_yield_even_though_the_byte_cap_is_not_reached() {
        let read = Arc::new(AtomicUsize::new(0));
        let mut reader = Reader::outputs(
            empty_frames(read.clone()),
            Box::new(Quiet),
            Failure::default(),
        );
        let mut decision = reader.decide().boxed();
        assert!(decision.as_mut().now_or_never().is_none());
        assert!(read.load(Ordering::SeqCst) <= MAX_READY_FRAMES);
        decision.await;
        assert_eq!(reader.refused().await, Some(Fault::upstream("refused")));
    }

    #[tokio::test]
    async fn ignored_incoming_frames_cannot_spin_in_one_poll() {
        let read = Arc::new(AtomicUsize::new(0));
        let mut reader = Reader::inputs(empty_frames(read.clone()), Box::new(|| Box::new(Quiet)));
        assert!(reader.next().now_or_never().is_none());
        assert!(read.load(Ordering::SeqCst) <= MAX_READY_FRAMES);
        assert_eq!(reader.next().await, Some(Err(Fault::upstream("refused"))));
    }
}
