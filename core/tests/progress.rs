//! Opening progress and bounded reads, including delivery cancellation.
#![allow(clippy::unwrap_used)]
use futures::channel::oneshot;
use futures::{FutureExt, Sink, StreamExt, stream};
use respan_gateway_core::{
    ClientContext, Connection, EventSink, EventStream, Extensions, Fault, Filter, Format, Gateway,
    Plugin, Response, Result, async_trait,
};
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::task::{Context, Poll};

fn drain<T: Send + 'static>() -> EventSink<T> {
    EventSink::new(futures::sink::unfold(
        (),
        |(), event: Result<T>| async move {
            event?;
            Ok::<_, Fault>(())
        },
    ))
}

struct CompletionSink<T> {
    close: futures::future::BoxFuture<'static, Result<()>>,
    marker: std::marker::PhantomData<fn(T)>,
}

impl<T> CompletionSink<T> {
    fn new(close: impl std::future::Future<Output = Result<()>> + Send + 'static) -> Self {
        Self {
            close: close.boxed(),
            marker: std::marker::PhantomData,
        }
    }
}

impl<T> Sink<Result<T>> for CompletionSink<T> {
    type Error = Fault;

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, event: Result<T>) -> Result<()> {
        // Completion-only fixtures discard each submitted event, including refusals.
        drop(event);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.get_mut().close.as_mut().poll(cx)
    }
}

#[derive(Clone)]
struct Input;
#[derive(Clone)]
struct Output(usize);
struct Protocol(Arc<AtomicUsize>, bool);
struct Io;
impl Format<Io, Input, Output> for Protocol {
    fn name(&self) -> &str {
        "messages"
    }
    fn matches(&self, _: &Io) -> bool {
        true
    }
    fn bind(&self, _: Io) -> Result<Connection<Input, Output>> {
        Ok((
            futures::future::ready(Ok::<_, Fault>(EventStream::single(Input))).boxed(),
            drain(),
        ))
    }
}
impl Format<Io, Output, Input> for Protocol {
    fn name(&self) -> &str {
        "messages"
    }
    fn bind(&self, _: Io) -> Result<Connection<Output, Input>> {
        let count = self.0.clone();
        let events = stream::iter(0..10_000).map(move |n| {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(Output(n))
        });
        Ok((
            futures::future::ready(Ok(EventStream::stream(
                events.chain(stream::iter(self.1.then(Fault::truncated).map(Err))),
            )))
            .boxed(),
            drain(),
        ))
    }
}
struct Inspect {
    read: Arc<AtomicUsize>,
    at_first_content: Arc<AtomicUsize>,
}
#[async_trait]
impl Plugin for Inspect {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>> {
        let mut response = cx.call("p").await?;
        response.buffer_until(|_: &Output| true).await?;
        self.at_first_content
            .store(self.read.load(Ordering::SeqCst), Ordering::SeqCst);
        Ok(Some(response))
    }
}
#[tokio::test]
async fn first_content_does_not_drain_every_ready_event() {
    let read = Arc::new(AtomicUsize::new(0));
    let at_first_content = Arc::new(AtomicUsize::new(0));
    let gateway = Gateway::new()
        .client::<Input, Output, _>(Protocol(read.clone(), false))
        .unwrap()
        .provider::<Input, Output, _>("p", Protocol(read.clone(), false), |_| Io)
        .unwrap()
        .plugin(
            "inspect",
            Inspect {
                read,
                at_first_content: at_first_content.clone(),
            },
        )
        .unwrap();
    gateway.handle(Io, Extensions::new()).await.unwrap();
    gateway.idle().await;
    assert!(
        at_first_content.load(Ordering::SeqCst) < 10_000,
        "buffer_until consumed all 10,000 ready events before returning the first content"
    );
}

struct Handshake(Arc<AtomicUsize>);
impl Format<Io, Input, Output> for Handshake {
    fn name(&self) -> &str {
        "handshake"
    }
    fn matches(&self, _: &Io) -> bool {
        true
    }
    fn bind(&self, _: Io) -> Result<Connection<Input, Output>> {
        let (opened, receive) = oneshot::channel();
        // Binding establishes state without asynchronous readiness. Opening
        // may use that state before the writer is prepared exactly once.
        let _ = opened.send(());
        let prepares = self.0.clone();
        Ok((
            async move {
                receive.await.map_err(|_| Fault::abandoned())?;
                Ok::<_, Fault>(EventStream::single(Input))
            }
            .boxed(),
            EventSink::preparing(move |_: &mut EventStream<Output>| {
                assert_eq!(prepares.fetch_add(1, Ordering::SeqCst), 0);
                Ok(futures::sink::unfold(
                    (),
                    |(), event: Result<Output>| async move {
                        event?;
                        Ok::<_, Fault>(())
                    },
                ))
            }),
        ))
    }
}
#[tokio::test]
async fn caller_opening_uses_state_established_during_bind() {
    let prepares = Arc::new(AtomicUsize::new(0));
    let gateway = Gateway::new().client(Handshake(prepares.clone())).unwrap();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        gateway.handle(Io, Extensions::new()),
    )
    .await;
    gateway.idle().await;
    assert_eq!(prepares.load(Ordering::SeqCst), 1);
    assert!(result.is_ok(), "caller opening stalled after binding");
}

struct RejectDelivery;
impl Format<Io, Input, Output> for RejectDelivery {
    fn name(&self) -> &str {
        "messages"
    }
    fn matches(&self, _: &Io) -> bool {
        true
    }
    fn bind(&self, _: Io) -> Result<Connection<Input, Output>> {
        Ok((
            futures::future::ready(Ok::<_, Fault>(EventStream::single(Input))).boxed(),
            EventSink::preparing(|events: &mut EventStream<Output>| {
                events.stop();
                Err::<CompletionSink<Output>, _>(Fault::internal("cannot prepare delivery"))
            }),
        ))
    }
}
#[tokio::test]
async fn failed_delivery_preparation_does_not_drain_provider() {
    let read = Arc::new(AtomicUsize::new(0));
    let gateway = Gateway::new()
        .client(RejectDelivery)
        .unwrap()
        .provider::<Input, Output, _>("p", Protocol(read.clone(), false), |_| Io)
        .unwrap()
        .target("p")
        .unwrap();
    assert!(gateway.handle(Io, Extensions::new()).await.is_err());
    gateway.idle().await;
    assert!(
        read.load(Ordering::SeqCst) < 10_000,
        "discarding the output during failed sink preparation drained all 10,000 provider events"
    );
}

#[derive(Clone, Default)]
struct Observe {
    outputs: Arc<std::sync::Mutex<Vec<usize>>>,
    filtered: Arc<AtomicUsize>,
    provider_ends: Arc<std::sync::Mutex<Vec<Result<()>>>>,
    observed_end: Arc<std::sync::Mutex<Option<Result<()>>>>,
}
#[async_trait]
impl Plugin for Observe {
    async fn on_provider(&self, px: &mut respan_gateway_core::ProviderContext) -> Result<()> {
        let filtered = self.filtered.clone();
        px.filter(move |output: &mut Output| {
            filtered.fetch_add(1, Ordering::SeqCst);
            output.0 += 1;
            Ok(())
        })
    }
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>> {
        let mut response = cx.call("p").await?;
        let end = response.until_end().await;
        *self.observed_end.lock().unwrap() = Some(end);
        let mut outputs = response.outputs::<Output>();
        while let Some(output) = outputs.next().await {
            if let Ok(output) = output {
                self.outputs.lock().unwrap().push(output.0);
            }
        }
        Ok(Some(Response::stream(stream::empty::<Result<Output>>())))
    }
    async fn on_provider_end(&self, _: &respan_gateway_core::ProviderContext, end: &Result<()>) {
        self.provider_ends.lock().unwrap().push(end.clone());
    }
}
#[tokio::test]
async fn batch_boundaries_preserve_order_filtering_and_the_final_outcome() {
    for fail in [false, true] {
        let read = Arc::new(AtomicUsize::new(0));
        let observe = Observe::default();
        let gateway = Gateway::new()
            .client::<Input, Output, _>(Protocol(read.clone(), false))
            .unwrap()
            .provider::<Input, Output, _>("p", Protocol(read, fail), |_| Io)
            .unwrap()
            .plugin("observe", observe.clone())
            .unwrap();
        gateway.handle(Io, Extensions::new()).await.unwrap();
        gateway.idle().await;
        assert_eq!(
            *observe.outputs.lock().unwrap(),
            (1..=10_000).collect::<Vec<_>>()
        );
        assert_eq!(observe.filtered.load(Ordering::SeqCst), 10_000);
        let expected = if fail {
            Err(Fault::truncated())
        } else {
            Ok(())
        };
        assert_eq!(
            *observe.observed_end.lock().unwrap(),
            Some(expected.clone())
        );
        assert_eq!(*observe.provider_ends.lock().unwrap(), [expected]);
    }
}

struct CancelHold(Arc<std::sync::Mutex<Vec<usize>>>);

#[async_trait]
impl Plugin for CancelHold {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>> {
        let mut response = cx.call("p").await?;
        let mut hold = response.until_end().boxed();
        assert!(hold.as_mut().now_or_never().is_none());
        drop(hold);
        let values = response.outputs::<Output>().collect::<Vec<_>>().await;
        *self.0.lock().unwrap() = values.into_iter().map(|v| v.unwrap().0).collect();
        Ok(Some(Response::stream(stream::empty::<Result<Output>>())))
    }
}

#[tokio::test]
async fn cancelling_borrowed_hold_replays_the_full_provider_batch() {
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let reads = Arc::new(AtomicUsize::new(0));
    let gw = Gateway::new()
        .client::<Input, Output, _>(Protocol(reads.clone(), false))
        .unwrap()
        .provider::<Input, Output, _>("p", Protocol(reads, false), |_| Io)
        .unwrap()
        .plugin("cancel-hold", CancelHold(seen.clone()))
        .unwrap();
    gw.handle(Io, Extensions::new()).await.unwrap();
    gw.idle().await;
    assert_eq!(*seen.lock().unwrap(), (0..10_000).collect::<Vec<_>>());
}

struct Suppress;
impl Filter<Output> for Suppress {
    fn edit(&mut self, _: Output, _: &mut Vec<Output>) -> Result<()> {
        Ok(())
    }
}

struct CollectHot {
    count: Arc<AtomicUsize>,
    suppress: bool,
}
#[async_trait]
impl Plugin for CollectHot {
    async fn on_provider(&self, px: &mut respan_gateway_core::ProviderContext) -> Result<()> {
        if self.suppress {
            px.filter(Suppress)?;
        }
        Ok(())
    }

    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>> {
        let response = cx.call("p").await?;
        let mut collecting = response.outputs::<Output>().collect::<Vec<_>>().boxed();
        assert!(
            collecting.as_mut().now_or_never().is_none(),
            "one poll drained every ready provider batch"
        );
        assert!(
            (1..10_000).contains(&self.count.load(Ordering::SeqCst)),
            "one poll must consume some, but not all, ready events"
        );
        drop(collecting);
        Ok(Some(Response::stream(stream::empty::<Result<Output>>())))
    }
}

#[tokio::test]
async fn collecting_hot_outputs_yields_even_when_filters_suppress_all_values() {
    for suppress in [false, true] {
        let count = Arc::new(AtomicUsize::new(0));
        let gateway = Gateway::new()
            .client::<Input, Output, _>(Protocol(count.clone(), false))
            .unwrap()
            .provider::<Input, Output, _>("p", Protocol(count.clone(), false), |_| Io)
            .unwrap()
            .plugin(
                "collect-hot",
                CollectHot {
                    count: count.clone(),
                    suppress,
                },
            )
            .unwrap();
        gateway.handle(Io, Extensions::new()).await.unwrap();
        gateway.idle().await;
        assert!(count.load(Ordering::SeqCst) < 10_000);
    }
}

#[tokio::test]
async fn a_hot_stream_yields_before_delivery_finishes_and_can_be_cancelled() {
    let read = Arc::new(AtomicUsize::new(0));
    let gateway = Gateway::new()
        .client::<Input, Output, _>(Protocol(read.clone(), false))
        .unwrap()
        .provider::<Input, Output, _>("p", Protocol(read.clone(), false), |_| Io)
        .unwrap()
        .target("p")
        .unwrap();
    let mut handled = gateway.handle(Io, Extensions::new()).boxed();
    assert!(
        handled.as_mut().now_or_never().is_none(),
        "one poll drained the entire provider"
    );
    assert!((1..10_000).contains(&read.load(Ordering::SeqCst)));
    drop(handled);
    gateway.idle().await;
    assert!(read.load(Ordering::SeqCst) < 10_000);
}

// Hand-built client bindings exercise ordering without a protocol adapter.
struct Custom;
type Sending = EventSink<Output>;
impl Format<Connection<Input, Output>, Input, Output> for Custom {
    fn name(&self) -> &str {
        "custom"
    }
    fn matches(&self, _: &Connection<Input, Output>) -> bool {
        true
    }
    fn bind(&self, io: Connection<Input, Output>) -> Result<Connection<Input, Output>> {
        Ok(io)
    }
}
#[tokio::test]
async fn preparation_failure_or_panic_after_opening_releases_the_writer() {
    for panics in [false, true] {
        let opening = futures::future::ready(Ok::<_, Fault>(EventStream::single(Input))).boxed();
        let held = Arc::new(());
        let captured = held.clone();
        let send: Sending = EventSink::preparing(move |_: &mut EventStream<Output>| {
            let _keep_alive = captured;
            assert!(!panics, "preparation panicked");
            Err::<CompletionSink<Output>, _>(Fault::config("cannot prepare"))
        });
        let gateway = Gateway::new().client(Custom).unwrap();
        let fault = gateway
            .handle((opening, send), Extensions::new())
            .await
            .unwrap_err();
        assert_eq!(fault.code(), if panics { "internal" } else { "config" });
        assert_eq!(Arc::strong_count(&held), 1);
        gateway.idle().await;
    }
}
#[tokio::test]
async fn cancelling_opening_drops_both_halves_and_releases_idle() {
    let (sender, receiver) = oneshot::channel::<()>();
    let opening = async move {
        receiver.await.map_err(|_| Fault::abandoned())?;
        Ok::<_, Fault>(EventStream::single(Input))
    }
    .boxed();
    let held = Arc::new(());
    let captured = held.clone();
    let send: Sending = EventSink::preparing(move |_: &mut EventStream<Output>| {
        drop(captured);
        Ok(CompletionSink::new(async { Ok(()) }))
    });
    let gateway = Gateway::new().client(Custom).unwrap();
    let mut handled = gateway.handle((opening, send), Extensions::new()).boxed();
    assert!(handled.as_mut().now_or_never().is_none());
    assert!(gateway.idle().now_or_never().is_none());
    drop(handled);
    assert!(sender.is_canceled());
    assert_eq!(Arc::strong_count(&held), 1);
    assert_eq!(gateway.idle().now_or_never(), Some(()));
}
struct Release(std::sync::Mutex<Option<oneshot::Sender<()>>>);
#[async_trait]
impl Plugin for Release {
    async fn on_client(&self, _: &mut ClientContext) -> Result<Option<Response>> {
        self.0.lock().unwrap().take().unwrap().send(()).unwrap();
        Ok(Some(Response::stream(stream::empty::<Result<Output>>())))
    }
}
#[tokio::test]
async fn receiving_can_open_before_delivery_completes() {
    let (send, receive) = oneshot::channel();
    let opening = futures::future::ready(Ok::<_, Fault>(EventStream::single(Input))).boxed();
    let deliver: Sending = EventSink::new(CompletionSink::new(async move {
        receive.await.map_err(|_| Fault::abandoned())
    }));
    let gateway = Gateway::new()
        .client(Custom)
        .unwrap()
        .plugin("release", Release(std::sync::Mutex::new(Some(send))))
        .unwrap();
    let handled = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        gateway.handle((opening, deliver), Extensions::new()),
    )
    .await;
    assert_eq!(handled.unwrap(), Ok(()));
    gateway.idle().await;
}

struct OpeningDone(Option<oneshot::Sender<()>>);
impl Drop for OpeningDone {
    fn drop(&mut self) {
        if let Some(done) = self.0.take() {
            let _ = done.send(());
        }
    }
}
#[tokio::test]
async fn a_completed_opening_is_released_before_delivery() {
    let (send, receive) = oneshot::channel();
    let held = OpeningDone(Some(send));
    let opening = futures::future::poll_fn(move |_| {
        let _keep_alive = &held;
        Poll::Ready(Ok::<_, Fault>(EventStream::single(Input)))
    })
    .boxed();
    let send: Sending = EventSink::new(CompletionSink::new(async move {
        receive.await.map_err(|_| Fault::abandoned())
    }));
    let gateway = Gateway::new().client(Custom).unwrap();
    let handled = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        gateway.handle((opening, send), Extensions::new()),
    )
    .await;
    assert_eq!(handled.unwrap(), Ok(()));
    gateway.idle().await;
}

struct FailAtBoundary(Arc<std::sync::Mutex<Vec<Result<()>>>>);
#[async_trait]
impl Plugin for FailAtBoundary {
    async fn on_provider(&self, px: &mut respan_gateway_core::ProviderContext) -> Result<()> {
        px.filter(|output: &mut Output| {
            if output.0 == 127 {
                Err(Fault::upstream("filter failed"))
            } else {
                Ok(())
            }
        })
    }
    async fn on_provider_end(&self, _: &respan_gateway_core::ProviderContext, end: &Result<()>) {
        self.0.lock().unwrap().push(end.clone());
    }
}
#[tokio::test]
async fn delivery_failure_keeps_precedence_over_a_full_batch_filter_failure() {
    let ends = Arc::new(std::sync::Mutex::new(Vec::new()));
    let read = Arc::new(AtomicUsize::new(0));
    let gateway = Gateway::new()
        .client(RejectDelivery)
        .unwrap()
        .provider::<Input, Output, _>("p", Protocol(read, false), |_| Io)
        .unwrap()
        .plugin("fail", FailAtBoundary(ends.clone()))
        .unwrap()
        .target("p")
        .unwrap();
    let expected = Err(Fault::internal("cannot prepare delivery"));
    assert_eq!(gateway.handle(Io, Extensions::new()).await, expected);
    gateway.idle().await;
    assert_eq!(*ends.lock().unwrap(), [expected]);
}

struct BufferBeforeDelivery;
#[async_trait]
impl Plugin for BufferBeforeDelivery {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>> {
        let mut response = cx.call("p").await?;
        assert_eq!(
            response.until_end().await,
            Err(Fault::upstream("filter failed"))
        );
        Ok(Some(response))
    }
}

#[tokio::test]
async fn a_buffered_filter_result_does_not_override_delivery_failure() {
    let ends = Arc::new(std::sync::Mutex::new(Vec::new()));
    let gateway = Gateway::new()
        .client(RejectDelivery)
        .unwrap()
        .provider::<Input, Output, _>("p", Protocol(Arc::new(AtomicUsize::new(0)), false), |_| Io)
        .unwrap()
        .plugin("fail", FailAtBoundary(ends.clone()))
        .unwrap()
        .plugin("buffer", BufferBeforeDelivery)
        .unwrap();
    let expected = Err(Fault::internal("cannot prepare delivery"));
    assert_eq!(gateway.handle(Io, Extensions::new()).await, expected);
    gateway.idle().await;
    assert_eq!(*ends.lock().unwrap(), [expected]);
}
