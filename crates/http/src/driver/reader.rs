//! Frames into events: a client's later inputs and a provider's outputs.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll, ready};

use bytes::Bytes;
use futures::stream::{self, Stream};
use futures::{FutureExt, StreamExt};
use respan_gateway_core::Fault;

use crate::{Decoder, Frames};

/// Where a streamed call's request encoder panicked, if it did: the
/// transport may read the call's body where nothing unwinds to the call, so
/// the response reads it here (B2).
pub(crate) type Failure = Arc<OnceLock<Fault>>;

/// Builds a decoder once the first frame comes.
type Make<T> = Box<dyn FnOnce() -> Box<dyn Decoder<T>> + Send>;

/// A body read into events through a [`Decoder`], each frame's in turn.
///
/// - A provider's outputs say `Pending` between frames, so what the gateway
///   finds ready at once, and takes as one batch, is one frame's (rule 6).
/// - A streaming request's inputs say `Pending` to their first poll, so
///   they are never ended at once, nor read ahead (rule 1).
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
    /// Whether it was polled yet; if not, its first poll says `Pending`.
    started: bool,
}

/// Never pinned in place: its fields are moved, not projected.
impl<T> Unpin for Reader<T> {}

impl<T: Send + 'static> Reader<T> {
    /// A streaming request's inputs: its data frames, each a message, even
    /// an empty one, through the decoder `make` builds at the first.
    pub(crate) fn inputs(body: Frames, make: Make<T>) -> Self {
        Self::new(body, Some(make), None, None, false, false)
    }

    /// A provider's outputs, through `decoder`, failing if the call's body
    /// did (`failed`).
    pub(crate) fn outputs(body: Frames, decoder: Box<dyn Decoder<T>>, failed: Failure) -> Self {
        Self::new(body, None, Some(decoder), Some(failed), true, true)
    }

    fn new(
        body: Frames,
        make: Option<Make<T>>,
        decoder: Option<Box<dyn Decoder<T>>>,
        failed: Option<Failure>,
        apart: bool,
        started: bool,
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
            started,
        }
    }

    /// Read the body to its end, or its first failure, before anything is
    /// decoded: its frames are kept apart, to be decoded as they came.
    pub(crate) async fn buffer(&mut self) {
        let mut frames = Vec::new();
        while let Some(frame) = self.body.next().await {
            let broke = frame.is_err();
            frames.push(frame);
            if broke {
                break;
            }
        }
        self.body = stream::iter(frames).boxed();
    }

    /// The provider's refusal at its head (rule 3), as far as its answer
    /// is ready now, waiting for nothing: its decoder's failure before any
    /// output, the body unbroken, as a provider's error answer is.
    pub(crate) fn refused(&mut self) -> Option<Fault> {
        while self.ready.is_empty() && !self.ended {
            let Some(frame) = self.body.next().now_or_never() else {
                break;
            };
            self.read(frame);
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
        if !this.started {
            this.started = true;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        loop {
            if let Some(value) = this.ready.pop_front() {
                return Poll::Ready(Some(value));
            }
            if this.ended {
                return Poll::Ready(None);
            }
            if this.apart && !this.yielded {
                this.yielded = true;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            let frame = ready!(this.body.poll_next_unpin(cx));
            this.yielded = false;
            this.read(frame);
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
