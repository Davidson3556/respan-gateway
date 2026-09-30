//! Typed events and a sink for writing one event stream.

use std::any::Any;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{FutureExt, Sink, Stream, StreamExt};

use crate::fault::guarded;
use crate::{Fault, Result};

pub(crate) type Metadata = Option<(Arc<str>, Arc<dyn Any + Send + Sync>)>;

/// Typed events: one replayable value or a live stream. The first fault
/// ends the stream. Protocol implementations may retain private relay data.
/// Opening and writing are ordinary futures, outside this stream.
pub struct Events<T> {
    source: Source<T>,
    metadata: Metadata,
    ended: bool,
}

enum Source<T> {
    Single(Arc<Single<T>>),
    Stream(BoxStream<'static, Result<T>>),
    Refused(Fault),
    Empty,
}

/// A replayable, memoized value, independent of the live-stream variant.
pub(crate) struct Single<T> {
    decoded: OnceLock<Result<T>>,
    decode: Box<dyn Fn() -> Result<T> + Send + Sync>,
    lazy: bool,
    clone: fn(&T) -> T,
}

impl<T> Single<T> {
    pub(crate) fn value(&self) -> Result<&T> {
        self.decoded
            .get_or_init(|| guarded(|| (self.decode)()))
            .as_ref()
            .map_err(Clone::clone)
    }

    fn cloned(&self) -> Result<T> {
        self.value().map(self.clone)
    }

    pub(crate) fn into_events(self: Arc<Self>) -> Events<T> {
        Events {
            source: Source::Single(self),
            metadata: None,
            ended: false,
        }
    }
}

impl<T> fmt::Debug for Events<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = match self.source {
            Source::Single(_) => "single",
            Source::Stream(_) => "stream",
            Source::Refused(_) => "refused",
            Source::Empty => "ended",
        };
        f.debug_struct("Events")
            .field("state", &state)
            .field("format", &self.metadata.as_ref().map(|(format, _)| format))
            .finish()
    }
}

impl<T> Events<T> {
    /// A refusal classified before any events become available.
    pub fn refused(fault: Fault) -> Self {
        Self {
            source: Source::Refused(fault),
            metadata: None,
            ended: false,
        }
    }

    /// Whether these events contain a replayable single value.
    pub fn is_single(&self) -> bool {
        matches!(self.source, Source::Single(_))
    }

    /// Whether this single value came from a lazy decoder.
    /// Inspecting it keeps this property; replacing it with `single` does not.
    pub fn is_lazy(&self) -> bool {
        matches!(&self.source, Source::Single(value) if value.lazy)
    }

    /// The refusal, before the stream has been read.
    pub fn refusal(&self) -> Option<&Fault> {
        match &self.source {
            Source::Refused(fault) => Some(fault),
            _ => None,
        }
    }

    /// The single value, decoding it at most once. `None` for a live stream.
    pub fn value(&self) -> Option<Result<&T>> {
        match &self.source {
            Source::Single(value) => Some(value.value()),
            _ => None,
        }
    }

    /// Retain protocol-owned data under its format identity.
    /// Use a private metadata type in the protocol implementation. The
    /// kernel carries this data unopened and removes it across translations.
    pub fn with_metadata<M: Any + Send + Sync>(mut self, format: &str, metadata: M) -> Self {
        self.metadata = Some((Arc::from(format), Arc::new(metadata)));
        self
    }

    /// Read private data when its format and concrete type both match.
    pub fn metadata<M: Any>(&self, format: &str) -> Option<&M> {
        let (_, data) = self
            .metadata
            .as_ref()
            .filter(|(name, _)| &**name == format)?;
        data.downcast_ref()
    }

    pub(crate) fn metadata_copy(&self) -> Metadata {
        self.metadata.clone()
    }

    pub(crate) fn set_metadata(&mut self, metadata: Metadata) {
        self.metadata = metadata;
    }

    pub(crate) fn clear_metadata(&mut self) {
        self.metadata = None;
    }

    pub(crate) fn take_single(&mut self) -> Option<Arc<Single<T>>> {
        if !self.is_single() {
            return None;
        }
        match std::mem::replace(&mut self.source, Source::Empty) {
            Source::Single(value) => Some(value),
            _ => unreachable!("source checked above"),
        }
    }
}

impl<T: Clone + Send + Sync + 'static> Events<T> {
    /// A single value already available to the reader.
    pub fn single(value: T) -> Self {
        let decoded = OnceLock::new();
        let _ = decoded.set(Ok(value));
        Arc::new(Single {
            decoded,
            decode: Box::new(|| Err(Fault::internal("single value missing"))),
            lazy: false,
            clone: T::clone,
        })
        .into_events()
    }

    /// A replayable value decoded only when a reader needs it.
    /// Internal snapshots share its result, including decoding failures.
    pub fn lazy(decode: impl Fn() -> Result<T> + Send + Sync + 'static) -> Self {
        Arc::new(Single {
            decoded: OnceLock::new(),
            decode: Box::new(decode),
            lazy: true,
            clone: T::clone,
        })
        .into_events()
    }
}

impl<T> Events<T> {
    /// A live stream whose first error is its last event.
    pub fn stream(events: impl Stream<Item = Result<T>> + Send + 'static) -> Self {
        Self {
            source: Source::Stream(events.boxed()),
            metadata: None,
            ended: false,
        }
    }
}

impl<T: Send + Sync + 'static> Events<T> {
    /// Change representation without eagerly decoding a single value.
    pub(crate) fn map_values<U: Clone + Send + Sync + 'static>(
        self,
        map: impl Fn(T) -> Result<U> + Send + Sync + 'static,
    ) -> Events<U> {
        let source = match self.source {
            Source::Single(value) => {
                let lazy = value.lazy;
                Source::Single(Arc::new(Single {
                    decoded: OnceLock::new(),
                    decode: Box::new(move || value.cloned().and_then(&map)),
                    lazy,
                    clone: U::clone,
                }))
            }
            Source::Stream(events) => {
                Source::Stream(events.map(move |event| event.and_then(&map)).boxed())
            }
            Source::Refused(fault) => Source::Refused(fault),
            Source::Empty => Source::Empty,
        };
        Events {
            source,
            metadata: self.metadata,
            ended: self.ended,
        }
    }
}

impl<T> Unpin for Events<T> {}

impl<T> Stream for Events<T> {
    type Item = Result<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.ended {
            return Poll::Ready(None);
        }
        let item = match &mut this.source {
            Source::Single(value) => {
                let item = value.cloned();
                this.ended = true;
                return Poll::Ready(Some(item));
            }
            Source::Stream(events) => events.as_mut().poll_next(cx),
            Source::Refused(fault) => Poll::Ready(Some(Err(fault.clone()))),
            Source::Empty => Poll::Ready(None),
        };
        if matches!(item, Poll::Ready(None | Some(Err(_)))) {
            this.ended = true;
        }
        item
    }
}

struct Writing<T> {
    #[allow(clippy::type_complexity)]
    prepare: Option<Box<dyn FnOnce(Events<T>) -> Result<BoxFuture<'static, Result<()>>> + Send>>,
    writing: Option<BoxFuture<'static, Result<()>>>,
    outcome: Option<Result<()>>,
}

/// A sink that accepts one event stream and completes when it is written.
///
/// `start_send` runs synchronous preparation, so its failure means no write
/// started. `poll_flush` and `poll_close` drive actual delivery. A second
/// submitted stream is rejected. Dropping the sink cancels pending delivery.
pub fn write<T, F, W>(prepare: W) -> impl Sink<Events<T>, Error = Fault> + Send + Unpin
where
    W: FnOnce(Events<T>) -> Result<F> + Send + 'static,
    F: Future<Output = Result<()>> + Send + 'static,
{
    Writing {
        prepare: Some(Box::new(move |events| {
            prepare(events).map(FutureExt::boxed)
        })),
        writing: None,
        outcome: None,
    }
}

impl<T> Sink<Events<T>> for Writing<T> {
    type Error = Fault;

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(if self.prepare.is_some() {
            Ok(())
        } else {
            Err(Fault::internal("a writer accepts exactly one event stream"))
        })
    }

    fn start_send(self: Pin<&mut Self>, events: Events<T>) -> Result<()> {
        let this = self.get_mut();
        let prepare = this
            .prepare
            .take()
            .ok_or_else(|| Fault::internal("a writer accepts exactly one event stream"))?;
        match prepare(events) {
            Ok(writing) => {
                this.writing = Some(writing);
                Ok(())
            }
            Err(fault) => {
                this.outcome = Some(Err(fault.clone()));
                Err(fault)
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let this = self.get_mut();
        if let Some(writing) = &mut this.writing {
            let outcome = std::task::ready!(writing.as_mut().poll(cx));
            this.writing = None;
            this.outcome = Some(outcome);
        }
        Poll::Ready(this.outcome.clone().unwrap_or(Ok(())))
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if self.prepare.take().is_some() {
            self.outcome = Some(Ok(()));
        }
        self.poll_flush(cx)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures::channel::oneshot;
    use futures::{SinkExt, TryStreamExt, stream};

    use super::*;

    #[test]
    fn snapshots_and_mapping_share_one_lazy_decode() {
        let decodes = Arc::new(AtomicUsize::new(0));
        let seen = decodes.clone();
        let mut events = Events::lazy(move || {
            seen.fetch_add(1, Ordering::SeqCst);
            Ok(17_u8)
        });
        let Some(single) = events.take_single() else {
            panic!("single expected")
        };
        let mapped = single
            .clone()
            .into_events()
            .map_values(|value| Ok(u16::from(value)));
        assert!(mapped.is_lazy());
        assert_eq!(decodes.load(Ordering::SeqCst), 0);
        assert_eq!(single.value(), Ok(&17));
        assert_eq!(mapped.value(), Some(Ok(&17_u16)));
        assert_eq!(mapped.value(), Some(Ok(&17_u16)));
        assert_eq!(decodes.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn lazy_panic_is_memoized() {
        let decodes = Arc::new(AtomicUsize::new(0));
        let seen = decodes.clone();
        let events = Events::<u8>::lazy(move || {
            seen.fetch_add(1, Ordering::SeqCst);
            panic!("decoder broke");
        });
        for _ in 0..2 {
            let Some(Err(fault)) = events.value() else {
                panic!("fault expected")
            };
            assert_eq!(fault.code(), crate::codes::INTERNAL);
        }
        assert_eq!(decodes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn first_fault_stops_polling_until_the_owner_drops_the_source() {
        let polls = Arc::new(AtomicUsize::new(0));
        let seen = polls.clone();
        let held = Arc::new(());
        let captured = held.clone();
        let mut events = Events::stream(stream::poll_fn(move |_| {
            let _keep_alive = &captured;
            let read = seen.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(Some(if read == 0 {
                Ok(1)
            } else {
                Err(Fault::truncated())
            }))
        }));
        assert_eq!(events.next().await, Some(Ok(1)));
        assert_eq!(events.next().await, Some(Err(Fault::truncated())));
        assert_eq!(Arc::strong_count(&held), 2);
        assert_eq!(events.next().await, None);
        assert_eq!(polls.load(Ordering::SeqCst), 2);
        drop(events);
        assert_eq!(Arc::strong_count(&held), 1);
    }

    #[tokio::test]
    async fn mapping_preserves_metadata_and_single_shape() {
        let mut events = Events::single(7_u8)
            .with_metadata("toy", 201_u16)
            .map_values(|value| Ok(u16::from(value)));
        assert!(events.is_single());
        assert!(!events.is_lazy());
        assert_eq!(events.metadata::<u16>("toy"), Some(&201));
        assert_eq!(events.metadata::<u16>("other"), None);
        assert_eq!(events.metadata::<u8>("toy"), None);
        events.clear_metadata();
        assert_eq!(events.metadata::<u16>("toy"), None);
        assert_eq!(events.try_collect::<Vec<_>>().await, Ok(vec![7_u16]));
    }

    #[tokio::test]
    async fn refusal_is_yielded_once() {
        let fault = Fault::upstream("refused");
        let mut events = Events::<u8>::refused(fault.clone());
        assert_eq!(events.refusal(), Some(&fault));
        assert_eq!(events.next().await, Some(Err(fault)));
        assert_eq!(events.next().await, None);
    }

    #[test]
    fn preparation_failure_happens_in_start_send() {
        let fault = Fault::bad_request("cannot encode");
        let expected = fault.clone();
        let mut sink =
            write(move |_: Events<u8>| Err::<std::future::Ready<Result<()>>, Fault>(fault));
        assert_eq!(
            Pin::new(&mut sink).start_send(Events::single(1)),
            Err(expected)
        );
    }

    #[tokio::test]
    async fn flush_waits_for_actual_delivery_and_close_keeps_its_failure() {
        let (finish, finished) = oneshot::channel();
        let mut sink = write(move |events: Events<u8>| {
            Ok(async move {
                assert_eq!(events.try_collect::<Vec<_>>().await?, [1]);
                finished.await.map_err(|_| Fault::abandoned())?
            })
        });
        assert_eq!(Pin::new(&mut sink).start_send(Events::single(1)), Ok(()));
        assert!(sink.flush().now_or_never().is_none());
        assert!(finish.send(Err(Fault::truncated())).is_ok());
        assert_eq!(sink.flush().await, Err(Fault::truncated()));
        assert_eq!(sink.close().await, Err(Fault::truncated()));
    }

    #[tokio::test]
    async fn close_before_sending_releases_resources_and_prevents_sending() {
        let resource = Arc::new(());
        let captured = resource.clone();
        let mut sink = write(move |_: Events<u8>| {
            drop(captured);
            Ok(std::future::ready(Ok(())))
        });
        assert_eq!(Arc::strong_count(&resource), 2);
        assert_eq!(sink.close().await, Ok(()));
        assert_eq!(Arc::strong_count(&resource), 1);
        assert!(sink.send(Events::single(1)).await.is_err());
    }

    #[tokio::test]
    async fn a_writer_rejects_second_submission() {
        let mut sink = write(|_: Events<u8>| Ok(std::future::ready(Ok(()))));
        assert_eq!(sink.send(Events::single(1)).await, Ok(()));
        assert!(sink.send(Events::single(2)).await.is_err());
    }
}
