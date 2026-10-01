//! Symmetric standard streams and sinks with protocol-owned preparation.

use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{FutureExt, Sink, Stream, StreamExt};

use crate::extensions::FormatData;
use crate::fault::guarded;
use crate::{Fault, Result};

/// The most ready events one poll moves before yielding: Tokio's cooperative
/// budget per task poll, so a hot stream yields no later than the runtime
/// would make any other Tokio resource yield.
pub(crate) const MAX_BATCH: usize = 128;

pub(crate) type Metadata = Option<FormatData>;

/// Typed events: one replayable value or a live stream. The first fault
/// ends the stream. Protocol implementations may retain private relay data.
/// Opening and writing are ordinary futures, outside this stream.
pub struct EventStream<T> {
    shape: Shape<T>,
    metadata: Metadata,
    ended: bool,
}

enum Shape<T> {
    Single(Arc<Single<T>>),
    Stream(BoxStream<'static, Result<T>>),
    Refused(Fault),
    Empty,
}

/// A replayable, memoized value, independent of the live-stream variant.
pub(crate) struct Single<T> {
    value: SingleValue<T>,
    clone: fn(&T) -> T,
}

enum SingleValue<T> {
    Ready(Result<T>),
    Deferred {
        decoded: OnceLock<Result<T>>,
        decode: Box<dyn Fn() -> Result<T> + Send + Sync>,
        /// Original lazy-input provenance, preserved through mapping.
        /// Mapping a ready value defers work too, but keeps this false.
        lazy: bool,
    },
}

impl<T> Single<T> {
    pub(crate) fn ready(value: Result<T>, clone: fn(&T) -> T) -> Arc<Self> {
        Arc::new(Self {
            value: SingleValue::Ready(value),
            clone,
        })
    }

    fn deferred(
        decode: impl Fn() -> Result<T> + Send + Sync + 'static,
        lazy: bool,
        clone: fn(&T) -> T,
    ) -> Arc<Self> {
        Arc::new(Self {
            value: SingleValue::Deferred {
                decoded: OnceLock::new(),
                decode: Box::new(decode),
                lazy,
            },
            clone,
        })
    }

    fn is_lazy(&self) -> bool {
        matches!(self.value, SingleValue::Deferred { lazy: true, .. })
    }

    pub(crate) fn known_error(&self) -> Option<Fault> {
        // An original lazy decode failure does not prevent raw-byte relay.
        // Edited or mapped ready values must report their known failures.
        let result = match &self.value {
            SingleValue::Ready(value) => value,
            SingleValue::Deferred { lazy: true, .. } => return None,
            SingleValue::Deferred { decoded, .. } => decoded.get()?,
        };
        result.as_ref().err().cloned()
    }

    pub(crate) fn value(&self) -> Result<&T> {
        let result = match &self.value {
            SingleValue::Ready(value) => value,
            SingleValue::Deferred {
                decoded, decode, ..
            } => decoded.get_or_init(|| guarded(decode)),
        };
        result.as_ref().map_err(Clone::clone)
    }

    fn cloned(&self) -> Result<T> {
        self.value().map(self.clone)
    }

    pub(crate) fn into_events(self: Arc<Self>) -> EventStream<T> {
        EventStream {
            shape: Shape::Single(self),
            metadata: None,
            ended: false,
        }
    }
}

impl<T> fmt::Debug for EventStream<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = match self.shape {
            Shape::Single(_) => "single",
            Shape::Stream(_) => "stream",
            Shape::Refused(_) => "refused",
            Shape::Empty => "ended",
        };
        f.debug_struct("EventStream")
            .field("state", &state)
            .field("format", &self.metadata.as_ref().map(FormatData::format))
            .finish()
    }
}

impl<T> EventStream<T> {
    /// An empty stream with no protocol metadata.
    pub fn empty() -> Self {
        Self {
            shape: Shape::Empty,
            metadata: None,
            ended: false,
        }
    }

    /// Stop yielding events without decoding or polling the source.
    /// Retain its shape, metadata and ownership until this stream is dropped,
    /// so already-encoded or bodyless delivery can finish before releasing it.
    pub fn stop(&mut self) {
        self.ended = true;
    }

    /// A refusal classified before any events become available.
    pub fn refused(fault: Fault) -> Self {
        Self {
            shape: Shape::Refused(fault),
            metadata: None,
            ended: false,
        }
    }

    /// Whether these events contain a replayable single value.
    pub fn is_single(&self) -> bool {
        matches!(self.shape, Shape::Single(_))
    }

    /// Whether this single value came from a lazy decoder.
    /// Inspecting it keeps this property; replacing it with `single` does not.
    pub fn is_lazy(&self) -> bool {
        matches!(&self.shape, Shape::Single(value) if value.is_lazy())
    }

    /// The refusal, before the stream has been read.
    pub fn refusal(&self) -> Option<&Fault> {
        match &self.shape {
            Shape::Refused(fault) => Some(fault),
            _ => None,
        }
    }

    /// The single value, decoding it at most once. `None` for a live stream.
    pub fn value(&self) -> Option<Result<&T>> {
        match &self.shape {
            Shape::Single(value) => Some(value.value()),
            _ => None,
        }
    }

    /// Retain protocol-owned data under its format identity.
    /// Use a private metadata type in the protocol implementation. The
    /// kernel carries this data unopened and removes it across translations.
    pub fn with_metadata<M: Any + Send + Sync>(mut self, format: &str, metadata: M) -> Self {
        self.metadata = Some(FormatData::new(format, metadata));
        self
    }

    /// Read private data when its format and concrete type both match.
    pub fn metadata<M: Any>(&self, format: &str) -> Option<&M> {
        self.metadata.as_ref()?.get(format)
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
        match std::mem::replace(&mut self.shape, Shape::Empty) {
            Shape::Single(value) => Some(value),
            _ => unreachable!("source checked above"),
        }
    }
}

impl<T: Clone + Send + Sync + 'static> EventStream<T> {
    /// A single value already available to the reader.
    pub fn single(value: T) -> Self {
        Single::ready(Ok(value), T::clone).into_events()
    }

    /// A replayable value decoded only when a reader needs it.
    /// Internal snapshots share its result, including decoding failures.
    pub fn lazy(decode: impl Fn() -> Result<T> + Send + Sync + 'static) -> Self {
        Single::deferred(decode, true, T::clone).into_events()
    }
}

impl<T> EventStream<T> {
    /// A live stream whose first error is its last event.
    pub fn stream(events: impl Stream<Item = Result<T>> + Send + 'static) -> Self {
        Self {
            shape: Shape::Stream(events.boxed()),
            metadata: None,
            ended: false,
        }
    }
}

impl<T: Send + Sync + 'static> EventStream<T> {
    /// Change representation without eagerly decoding a single value.
    /// `clone` copies replayable snapshots; live values are moved.
    pub(crate) fn map_values<U: Send + Sync + 'static>(
        self,
        map: impl Fn(T) -> Result<U> + Send + Sync + 'static,
        clone: fn(&U) -> U,
    ) -> EventStream<U> {
        let shape = match self.shape {
            Shape::Single(value) => {
                let lazy = value.is_lazy();
                Shape::Single(Single::deferred(
                    move || value.cloned().and_then(&map),
                    lazy,
                    clone,
                ))
            }
            Shape::Stream(events) => {
                Shape::Stream(events.map(move |event| event.and_then(&map)).boxed())
            }
            Shape::Refused(fault) => Shape::Refused(fault),
            Shape::Empty => Shape::Empty,
        };
        EventStream {
            shape,
            metadata: self.metadata,
            ended: self.ended,
        }
    }
}

impl<T> Unpin for EventStream<T> {}

impl<T> Stream for EventStream<T> {
    type Item = Result<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.ended {
            return Poll::Ready(None);
        }
        let item = match &mut this.shape {
            Shape::Single(value) => {
                let item = value.cloned();
                this.ended = true;
                return Poll::Ready(Some(item));
            }
            Shape::Stream(events) => events.as_mut().poll_next(cx),
            Shape::Refused(fault) => Poll::Ready(Some(Err(fault.clone()))),
            Shape::Empty => Poll::Ready(None),
        };
        if matches!(item, Poll::Ready(None | Some(Err(_)))) {
            this.ended = true;
        }
        item
    }
}

type BoxSink<T> = Pin<Box<dyn Sink<Result<T>, Error = Fault> + Send>>;
type Prepare<T> = Box<dyn FnOnce(&mut EventStream<T>) -> Result<BoxSink<T>> + Send>;

enum SinkState<T> {
    Preparing(Prepare<T>),
    Ready(BoxSink<T>),
    Failed(Fault),
    Closed(Result<()>),
}

/// A standard per-event sink. Source faults are items; sink faults are delivery
/// failures. The first submitted source fault is terminal: it is passed to the
/// concrete sink, then only flushing and closing are allowed.
///
/// `poll_flush` advances delivery without ending the transfer. `poll_close`
/// completes final framing and actual delivery, including transport completion.
/// Dropping the sink cancels outstanding delivery.
pub struct EventSink<T> {
    state: SinkState<T>,
    terminal: bool,
}

impl<T> fmt::Debug for EventSink<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = match &self.state {
            SinkState::Preparing(_) => "preparing",
            SinkState::Ready(_) => "ready",
            SinkState::Failed(_) => "failed",
            SinkState::Closed(_) => "closed",
        };
        f.debug_struct("EventSink")
            .field("state", &state)
            .field("terminal", &self.terminal)
            .finish()
    }
}

impl<T> EventSink<T> {
    /// Wrap an ordinary sink. No preparation is required for direct SinkExt use.
    pub fn new<S>(sink: S) -> Self
    where
        S: Sink<Result<T>, Error = Fault> + Send + 'static,
    {
        Self {
            state: SinkState::Ready(Box::pin(sink)),
            terminal: false,
        }
    }

    /// Construct the real per-event sink synchronously from source metadata.
    /// Call `prepare` before using the standard sink operations. Preparation
    /// must not initiate I/O or retain a live source to read from the sink.
    /// It may inspect/encode a finite value, stop an already-encoded or
    /// bodyless source, or wrap a live source that remains owned by the caller.
    pub fn preparing<S, F>(prepare: F) -> Self
    where
        S: Sink<Result<T>, Error = Fault> + Send + 'static,
        F: FnOnce(&mut EventStream<T>) -> Result<S> + Send + 'static,
    {
        Self {
            state: SinkState::Preparing(Box::new(move |source| {
                prepare(source).map(|sink| Box::pin(sink) as BoxSink<T>)
            })),
            terminal: false,
        }
    }

    /// Prepare once, before the kernel marks the call sent. Repeated calls do
    /// nothing after success and retain the original failure after an error.
    pub fn prepare(&mut self, source: &mut EventStream<T>) -> Result<()> {
        if matches!(self.state, SinkState::Preparing(_)) {
            let missing = Fault::internal("event sink preparation did not complete");
            let SinkState::Preparing(prepare) =
                std::mem::replace(&mut self.state, SinkState::Failed(missing))
            else {
                unreachable!()
            };
            self.state = match prepare(source) {
                Ok(sink) => SinkState::Ready(sink),
                Err(fault) => SinkState::Failed(fault),
            };
        }
        self.sink().map(|_| ())
    }

    fn sink(&mut self) -> Result<&mut BoxSink<T>> {
        match &mut self.state {
            SinkState::Ready(sink) => Ok(sink),
            SinkState::Preparing(_) => Err(Fault::internal("event sink needs preparation")),
            SinkState::Failed(fault) => Err(fault.clone()),
            SinkState::Closed(_) => Err(Fault::internal("event sink is closed")),
        }
    }

    fn writable(&self) -> Result<()> {
        if self.terminal {
            Err(Fault::internal(
                "event sink is closed or received its terminal fault",
            ))
        } else {
            Ok(())
        }
    }
}

impl<T> Unpin for EventSink<T> {}

impl<T> Sink<Result<T>> for EventSink<T> {
    type Error = Fault;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let this = self.get_mut();
        this.writable()?;
        this.sink()?.as_mut().poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: Result<T>) -> Result<()> {
        let this = self.get_mut();
        this.writable()?;
        let terminal = item.is_err();
        this.sink()?.as_mut().start_send(item)?;
        this.terminal = terminal;
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let this = self.get_mut();
        if let SinkState::Closed(end) = &this.state {
            return Poll::Ready(end.clone());
        }
        this.sink()?.as_mut().poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let this = self.get_mut();
        this.terminal = true;
        if let SinkState::Closed(end) = &this.state {
            return Poll::Ready(end.clone());
        }
        let end = futures::ready!(this.sink()?.as_mut().poll_close(cx));
        this.state = SinkState::Closed(end.clone());
        Poll::Ready(end)
    }
}

/// Forward source faults as sink items, not TryStream failures. Flush even
/// before the first event so quiet duplex transfers can open. Bound each poll
/// so an always-ready source/sink cannot starve receiving or cancellation.
pub(crate) fn deliver<T: Send + Sync + 'static>(
    mut source: EventStream<T>,
    mut sink: EventSink<T>,
) -> BoxFuture<'static, Result<()>> {
    let mut ended = source.ended || matches!(source.shape, Shape::Empty);
    // Retain the source after Ready, until the owner drops this future. The
    // client delivery guard must mark failure before unread output is dropped.
    futures::future::poll_fn(move |cx| {
        for _ in 0..MAX_BATCH {
            if ended {
                return Pin::new(&mut sink).poll_close(cx);
            }
            match Pin::new(&mut sink).poll_ready(cx) {
                Poll::Ready(Ok(())) => {}
                other => return other,
            }
            match Pin::new(&mut source).poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    ended = item.is_err();
                    Pin::new(&mut sink).start_send(item)?;
                }
                Poll::Ready(None) => ended = true,
                Poll::Pending => {
                    if let Poll::Ready(Err(fault)) = Pin::new(&mut sink).poll_flush(cx) {
                        return Poll::Ready(Err(fault));
                    }
                    return Poll::Pending;
                }
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    })
    .boxed()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures::channel::oneshot;
    use futures::{TryStreamExt, stream};

    use super::*;

    #[test]
    fn snapshots_and_mapping_share_one_lazy_decode() {
        let decodes = Arc::new(AtomicUsize::new(0));
        let seen = decodes.clone();
        let mut events = EventStream::lazy(move || {
            seen.fetch_add(1, Ordering::SeqCst);
            Ok(17_u8)
        });
        let Some(single) = events.take_single() else {
            panic!("single expected")
        };
        let mapped = single
            .clone()
            .into_events()
            .map_values(|value| Ok(u16::from(value)), Clone::clone);
        assert!(mapped.is_lazy());
        assert_eq!(decodes.load(Ordering::SeqCst), 0);
        assert_eq!(single.value(), Ok(&17));
        assert_eq!(mapped.value(), Some(Ok(&17_u16)));
        assert_eq!(mapped.value(), Some(Ok(&17_u16)));
        assert_eq!(decodes.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn mapping_defers_and_memoizes_failures_without_changing_relay_provenance() {
        for lazy in [false, true] {
            let events = if lazy {
                EventStream::lazy(|| Ok(7_u8))
            } else {
                EventStream::single(7_u8)
            };
            let maps = Arc::new(AtomicUsize::new(0));
            let seen = maps.clone();
            let fault = Fault::bad_request("mapping failed");
            let failure = fault.clone();
            let mut mapped = events.map_values(
                move |_| -> Result<u16> {
                    seen.fetch_add(1, Ordering::SeqCst);
                    Err(failure.clone())
                },
                Clone::clone,
            );
            assert_eq!(mapped.is_lazy(), lazy);
            assert_eq!(maps.load(Ordering::SeqCst), 0);
            let Some(single) = mapped.take_single() else {
                panic!("single expected")
            };
            assert_eq!(single.known_error(), None);
            let replay = single.clone().into_events();
            assert_eq!(single.value(), Err(fault.clone()));
            assert_eq!(replay.value(), Some(Err(fault.clone())));
            assert_eq!(maps.load(Ordering::SeqCst), 1);
            assert_eq!(replay.is_lazy(), lazy);
            assert_eq!(single.known_error(), (!lazy).then_some(fault));
        }
    }

    #[test]
    fn lazy_panic_is_memoized() {
        let decodes = Arc::new(AtomicUsize::new(0));
        let seen = decodes.clone();
        let events = EventStream::<u8>::lazy(move || {
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
        let mut events = EventStream::stream(stream::poll_fn(move |_| {
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
        let mut events = EventStream::single(7_u8)
            .with_metadata("toy", 201_u16)
            .map_values(|value| Ok(u16::from(value)), Clone::clone);
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
        let mut events = EventStream::<u8>::refused(fault.clone());
        assert_eq!(events.refusal(), Some(&fault));
        assert_eq!(events.next().await, Some(Err(fault)));
        assert_eq!(events.next().await, None);
    }

    #[derive(Default)]
    struct Probe {
        items: Arc<std::sync::Mutex<Vec<Result<u8>>>>,
        flushes: Arc<AtomicUsize>,
        ready: Option<BoxFuture<'static, Result<()>>>,
        close: Option<BoxFuture<'static, Result<()>>>,
    }

    impl Sink<Result<u8>> for Probe {
        type Error = Fault;
        fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
            if let Some(ready) = &mut self.ready {
                futures::ready!(ready.as_mut().poll(cx))?;
                self.ready = None;
            }
            Poll::Ready(Ok(()))
        }
        fn start_send(self: Pin<&mut Self>, item: Result<u8>) -> Result<()> {
            self.items.lock().expect("items").push(item);
            Ok(())
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<()>> {
            self.flushes.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(Ok(()))
        }
        fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
            match &mut self.close {
                Some(close) => close.as_mut().poll(cx),
                None => Poll::Ready(Ok(())),
            }
        }
    }

    #[test]
    fn preparation_failure_happens_on_submission() {
        let fault = Fault::bad_request("cannot encode");
        let expected = fault.clone();
        let mut sink = EventSink::preparing(move |_: &mut EventStream<u8>| Err::<Probe, _>(fault));
        assert_eq!(
            sink.prepare(&mut EventStream::single(1)),
            Err(expected.clone())
        );
        assert_eq!(sink.prepare(&mut EventStream::single(2)), Err(expected));
    }

    #[tokio::test]
    async fn delivery_future_waits_for_completion_and_keeps_failure() {
        let (finish, finished) = oneshot::channel();
        let probe = Probe {
            close: Some(async { finished.await.map_err(|_| Fault::abandoned())? }.boxed()),
            ..Probe::default()
        };
        let items = probe.items.clone();
        let mut delivering = deliver(EventStream::single(1), EventSink::new(probe));
        assert!(delivering.as_mut().now_or_never().is_none());
        assert_eq!(*items.lock().expect("items"), [Ok(1)]);
        assert!(finish.send(Err(Fault::truncated())).is_ok());
        assert_eq!(delivering.await, Err(Fault::truncated()));
    }

    #[test]
    fn unused_writer_releases_resources_on_drop() {
        let resource = Arc::new(());
        let captured = resource.clone();
        let sink = EventSink::preparing(move |_: &mut EventStream<u8>| {
            drop(captured);
            Ok(Probe::default())
        });
        assert_eq!(Arc::strong_count(&resource), 2);
        drop(sink);
        assert_eq!(Arc::strong_count(&resource), 1);
    }

    #[tokio::test]
    async fn standard_sink_interoperability_and_terminal_source_fault() {
        use futures::SinkExt;
        let probe = Probe::default();
        let items = probe.items.clone();
        let mut sink = EventSink::new(probe);
        sink.send(Ok(3)).await.expect("send value");
        let fault = Fault::bad_request("source");
        sink.send(Err(fault.clone()))
            .await
            .expect("send fault item");
        assert!(sink.send(Ok(4)).await.is_err());
        sink.close().await.expect("close delivery");
        assert_eq!(*items.lock().expect("items"), [Ok(3), Err(fault)]);
        let mut ordinary = EventSink::new(futures::sink::unfold((), |(), _: Result<u8>| async {
            Ok::<_, Fault>(())
        }));
        ordinary.send(Ok(5)).await.expect("standard unfold sink");
        ordinary.close().await.expect("close");
        ordinary.close().await.expect("closed twice");
        ordinary.flush().await.expect("flush closed sink");
        assert!(ordinary.send(Ok(6)).await.is_err());
    }

    #[tokio::test]
    async fn pump_forwards_source_fault_but_returns_sink_fault() {
        let probe = Probe::default();
        let items = probe.items.clone();
        let source_fault = Fault::bad_request("source");
        deliver(
            EventStream::refused(source_fault.clone()),
            EventSink::new(probe),
        )
        .await
        .expect("fault delivered");
        assert_eq!(*items.lock().expect("items"), [Err(source_fault)]);
        let sink_fault = Fault::upstream("sink");
        let expected = sink_fault.clone();
        let sink = futures::sink::unfold((), move |(), _: Result<u8>| {
            futures::future::ready(Err::<(), _>(sink_fault.clone()))
        });
        assert_eq!(
            deliver(EventStream::single(1), EventSink::new(sink)).await,
            Err(expected)
        );
    }

    #[tokio::test]
    async fn backpressure_prevents_source_reads_and_drop_cancels_both_halves() {
        let (ready, readiness) = oneshot::channel();
        let probe = Probe {
            ready: Some(async { readiness.await.map_err(|_| Fault::abandoned()) }.boxed()),
            ..Probe::default()
        };
        let polls = Arc::new(AtomicUsize::new(0));
        let count = polls.clone();
        let resource = Arc::new(());
        let captured = resource.clone();
        let source = EventStream::stream(stream::poll_fn(move |_| {
            let _held = &captured;
            count.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(Some(Ok(1)))
        }));
        let mut pump = deliver(source, EventSink::new(probe));
        assert!(pump.as_mut().now_or_never().is_none());
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        drop(pump);
        assert_eq!(Arc::strong_count(&resource), 1);
        assert!(ready.send(()).is_err());
    }

    #[tokio::test]
    async fn pending_source_flushes_and_hot_source_yields() {
        let probe = Probe::default();
        let flushes = probe.flushes.clone();
        let mut quiet = deliver(
            EventStream::stream(stream::pending()),
            EventSink::new(probe),
        );
        assert!(quiet.as_mut().now_or_never().is_none());
        assert_eq!(flushes.load(Ordering::SeqCst), 1);
        let probe = Probe::default();
        let items = probe.items.clone();
        let mut hot = deliver(
            EventStream::stream(stream::repeat(Ok(1))),
            EventSink::new(probe),
        );
        assert!(hot.as_mut().now_or_never().is_none());
        assert_eq!(items.lock().expect("items").len(), 128);
    }

    #[tokio::test]
    async fn stopped_and_empty_sources_close_without_waiting_for_readiness() {
        let mut stopped = EventStream::stream(stream::poll_fn(|_| -> Poll<Option<Result<u8>>> {
            panic!("stopped source was read");
        }));
        stopped.stop();
        for source in [stopped, EventStream::empty()] {
            let sink = EventSink::new(Probe {
                ready: Some(futures::future::pending().boxed()),
                ..Probe::default()
            });
            assert_eq!(deliver(source, sink).now_or_never(), Some(Ok(())));
        }
    }

    #[tokio::test]
    async fn stopped_source_is_retained_until_delivery_closes_or_is_cancelled() {
        for cancel in [false, true] {
            let held = Arc::new(());
            let captured = held.clone();
            let mut source =
                EventStream::stream(stream::poll_fn(move |_| -> Poll<Option<Result<u8>>> {
                    let _keep_alive = &captured;
                    panic!("stopped source was read");
                }))
                .with_metadata("toy", 23_u16);
            source.stop();
            let source = source.map_values(Ok, Clone::clone);
            assert_eq!(source.metadata::<u16>("toy"), Some(&23));
            let (finish, finished) = oneshot::channel();
            let sink = EventSink::new(Probe {
                close: Some(async { finished.await.map_err(|_| Fault::abandoned())? }.boxed()),
                ..Probe::default()
            });
            let mut pump = deliver(source, sink);
            assert!(pump.as_mut().now_or_never().is_none());
            assert_eq!(
                Arc::strong_count(&held),
                2,
                "close still owns the stopped source"
            );
            if cancel {
                drop(pump);
                assert!(finish.is_canceled());
            } else {
                finish.send(Ok(())).expect("complete");
                pump.await.expect("delivered");
            }
            assert_eq!(Arc::strong_count(&held), 1);
        }
    }

    #[tokio::test]
    async fn preparation_can_relay_lazy_source_without_decoding() {
        let mut source = EventStream::lazy(|| -> Result<u8> { panic!("raw relay decoded") })
            .with_metadata("raw", "bytes");
        let mut sink = EventSink::preparing(|source: &mut EventStream<u8>| {
            assert!(source.is_lazy());
            assert_eq!(source.metadata::<&str>("raw"), Some(&"bytes"));
            source.stop();
            Ok(Probe::default())
        });
        sink.prepare(&mut source).expect("prepare raw");
        deliver(source, sink).await.expect("close raw");
    }
}
