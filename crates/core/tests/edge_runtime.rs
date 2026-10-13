//! Cross-direction lifecycle guarantees of the symmetric edge contract.
#![allow(clippy::unwrap_used)]

mod faults;

use faults::{says, says_all};
use respan_gateway_core::futures::FutureExt;
use respan_gateway_core::futures::future::BoxFuture;
use std::sync::{Arc, Mutex};

use respan_gateway_core::futures::Sink;
use respan_gateway_core::futures::channel::oneshot;
use respan_gateway_core::{
    ClientContext, Connection, EventSink, EventStream, Extensions, Fault, Format, Gateway, Plugin,
    Provider, ProviderContext, Response, async_trait,
};
use std::pin::Pin;
use std::task::{Context, Poll};

#[derive(Clone)]
struct Input;
#[derive(Clone)]
struct Output;

enum Io {
    Client { fail_writing: bool },
    Provider(Arc<Dialer>),
    Ready,
}

struct Dialer {
    writing: Mutex<Option<BoxFuture<'static, Result<(), Fault>>>>,
    incoming: Mutex<Option<EventStream<Output>>>,
}

/// Inputs are accepted per item; only closing waits for actual delivery.
struct Completion(BoxFuture<'static, Result<(), Fault>>);
impl Sink<Result<Input, Fault>> for Completion {
    type Error = Fault;
    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Fault>> {
        Poll::Ready(Ok(()))
    }
    fn start_send(self: Pin<&mut Self>, item: Result<Input, Fault>) -> Result<(), Fault> {
        item.map(|_| ())
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Fault>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Fault>> {
        self.0.as_mut().poll(cx)
    }
}

struct Protocol;

impl Format<Io, Input, Output> for Protocol {
    fn name(&self) -> &str {
        "edge-lifecycle"
    }

    fn matches(&self, io: &Io) -> bool {
        matches!(io, Io::Client { .. })
    }

    fn bind(&self, io: Io) -> Result<Connection<Input, Output>, Fault> {
        let Io::Client { fail_writing } = io else {
            return Err(Fault::config("expected client I/O"));
        };
        let outgoing = EventSink::preparing(move |_: &mut EventStream<Output>| {
            if fail_writing {
                return Err(Fault::new("client delivery failed"));
            }
            Ok(futures::sink::unfold(
                (),
                |(), event: Result<Output, Fault>| futures::future::ready(event.map(|_| ())),
            ))
        });
        Ok((
            futures::future::ready(Ok(EventStream::single(Input))).boxed(),
            outgoing,
        ))
    }
}

impl Format<Io, Output, Input> for Protocol {
    fn name(&self) -> &str {
        "edge-lifecycle"
    }

    fn bind(&self, io: Io) -> Result<Connection<Output, Input>, Fault> {
        let Io::Provider(dialer) = io else {
            return Err(Fault::config("expected provider I/O"));
        };
        let delivery = dialer.writing.lock().unwrap().take().unwrap();
        let incoming = dialer.incoming.lock().unwrap().take().unwrap();
        let outgoing = EventSink::new(Completion(delivery));
        Ok((futures::future::ready(Ok(incoming)).boxed(), outgoing))
    }
}

#[derive(Clone, Default)]
struct Record {
    provider: Arc<Mutex<Vec<Result<(), Fault>>>>,
    client: Arc<Mutex<Vec<Result<(), Fault>>>>,
    hold: bool,
    sent: Arc<Mutex<Vec<bool>>>,
}

impl Record {
    /// The ends the provider saw, faults as messages.
    fn provider_ends(&self) -> Vec<Result<(), String>> {
        says_all(self.provider.lock().unwrap().iter().cloned())
    }

    /// The ends the client saw, faults as messages.
    fn client_ends(&self) -> Vec<Result<(), String>> {
        says_all(self.client.lock().unwrap().iter().cloned())
    }
}

#[async_trait]
impl Plugin for Record {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        if self.hold {
            let mut response = cx.call("provider").await?;
            response.until_end().await?;
            Ok(Some(response))
        } else {
            Ok(None)
        }
    }

    async fn on_provider_end(&self, px: &ProviderContext, end: &Result<(), Fault>) {
        self.provider.lock().unwrap().push(end.clone());
        self.sent.lock().unwrap().push(px.is_sent());
    }

    async fn on_client_end(&self, _: &ClientContext, end: &Result<(), Fault>) {
        self.client.lock().unwrap().push(end.clone());
    }
}

fn gateway(record: &Record, writing: BoxFuture<'static, Result<(), Fault>>) -> Gateway {
    gateway_with(record, writing, EventStream::single(Output))
}

fn gateway_with(
    record: &Record,
    writing: BoxFuture<'static, Result<(), Fault>>,
    incoming: EventStream<Output>,
) -> Gateway {
    Gateway::new()
        .client::<Input, _, _>(Protocol)
        .unwrap()
        .provider(Provider::new::<Input, _, _>("provider", Protocol, {
            let dialer = Arc::new(Dialer {
                writing: Mutex::new(Some(writing)),
                incoming: Mutex::new(Some(incoming)),
            });
            move |_| Io::Provider(dialer.clone())
        }))
        .unwrap()
        .plugin(record.clone())
        .target("provider")
        .unwrap()
}

#[tokio::test]
async fn finishing_provider_input_waits_for_its_outgoing_delivery() {
    let (finish, finished) = oneshot::channel();
    let writing = async move { finished.await.map_err(|_| Fault::abandoned())? }.boxed();
    let record = Record::default();
    let gw = gateway(&record, writing);
    let mut handled = gw
        .handle(
            Io::Client {
                fail_writing: false,
            },
            Extensions::new(),
        )
        .boxed();
    assert!(
        handled.as_mut().now_or_never().is_none(),
        "an ended incoming flow must not silently abandon an unfinished outgoing writer"
    );
    let fault = Fault::new("provider's last write failed");
    let flushed = fault.message().to_owned();
    finish.send(Err(fault)).unwrap();
    assert_eq!(says(handled.await), Err(flushed.clone()));
    gw.idle().await;
    assert_eq!(record.provider_ends(), [Err(flushed)]);
}

#[tokio::test]
async fn client_preparation_failure_does_not_settle_buffered_provider_success() {
    let record = Record {
        hold: true,
        ..Record::default()
    };
    let gw = gateway(&record, async { Ok(()) }.boxed());
    let fault = Fault::new("client delivery failed");
    let failed = Err(fault.message().to_owned());
    let handled = gw
        .handle(Io::Client { fail_writing: true }, Extensions::new())
        .await;
    assert_eq!(says(handled), failed);
    gw.idle().await;
    assert_eq!(record.client_ends(), std::slice::from_ref(&failed));
    assert_eq!(record.provider_ends(), [failed]);
}

#[tokio::test]
async fn provider_refusal_cancels_pending_upload_without_waiting_for_it() {
    let (finish, finished) = oneshot::channel::<Result<(), Fault>>();
    let writing = async move { finished.await.map_err(|_| Fault::abandoned())? }.boxed();
    let record = Record::default();
    let refusal = Fault::new("provider declined input");
    let gw = gateway_with(&record, writing, EventStream::refused(refusal.clone()));
    let handled = gw
        .handle(
            Io::Client {
                fail_writing: false,
            },
            Extensions::new(),
        )
        .await;
    assert_eq!(says(handled), Err(refusal.message().to_owned()));
    assert!(finish.is_canceled());
    gw.idle().await;
    assert_eq!(record.provider_ends(), [Err(refusal.message().to_owned())]);
}

struct RefusalHeld(Arc<Mutex<oneshot::Sender<Result<(), Fault>>>>);

#[async_trait]
impl Plugin for RefusalHeld {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let mut response = cx.call("provider").await?;
        assert!(
            self.0.lock().unwrap().is_canceled(),
            "a refused opening must release pending upload while response is held"
        );
        assert!(response.until_end().await.is_err());
        Ok(Some(response))
    }
}

#[tokio::test]
async fn refused_opening_releases_upload_before_held_response_is_replayed() {
    let (finish, finished) = oneshot::channel::<Result<(), Fault>>();
    let finish = Arc::new(Mutex::new(finish));
    let writing = async move { finished.await.map_err(|_| Fault::abandoned())? }.boxed();
    let refusal = Fault::new("provider declined input");
    let record = Record::default();
    let gw = gateway_with(&record, writing, EventStream::refused(refusal.clone()))
        .plugin(RefusalHeld(finish.clone()));
    let handled = gw
        .handle(
            Io::Client {
                fail_writing: false,
            },
            Extensions::new(),
        )
        .await;
    assert_eq!(says(handled), Err(refusal.message().to_owned()));
    gw.idle().await;
    assert_eq!(record.provider_ends(), [Err(refusal.message().to_owned())]);
}

#[tokio::test]
async fn cancelling_exchange_drops_both_pending_provider_directions() {
    let (finish, finished) = oneshot::channel::<Result<(), Fault>>();
    let writing = async move { finished.await.map_err(|_| Fault::abandoned())? }.boxed();
    let record = Record::default();
    let gw = gateway_with(
        &record,
        writing,
        EventStream::stream(futures::stream::pending()),
    );
    let mut handled = gw
        .handle(
            Io::Client {
                fail_writing: false,
            },
            Extensions::new(),
        )
        .boxed();
    assert!(handled.as_mut().now_or_never().is_none());
    assert!(!finish.is_canceled());
    drop(handled);
    assert!(finish.is_canceled());
    gw.idle().await;
    assert_eq!(record.provider_ends(), [Err("abandoned".to_owned())]);
    assert_eq!(record.client_ends(), [Err("abandoned".to_owned())]);
}

/// Make resource teardown observable after a sink has failed. With no plugins,
/// detached-hook bookkeeping can otherwise become idle before this drop ends.
struct BlockingSink {
    entered: Option<oneshot::Sender<()>>,
    release: std::sync::mpsc::Receiver<()>,
}

impl Drop for BlockingSink {
    fn drop(&mut self) {
        if let Some(entered) = self.entered.take() {
            let _ = entered.send(());
            self.release
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
        }
    }
}

impl Sink<Result<Output, Fault>> for BlockingSink {
    type Error = Fault;
    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Fault>> {
        Poll::Ready(Ok(()))
    }
    fn start_send(self: Pin<&mut Self>, _: Result<Output, Fault>) -> Result<(), Fault> {
        Err(faults::upstream("delivery failed"))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Fault>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Fault>> {
        Poll::Ready(Ok(()))
    }
}

struct BlockingProtocol;
impl Format<BlockingSink, Input, Output> for BlockingProtocol {
    fn name(&self) -> &str {
        "blocked-drop"
    }
    fn matches(&self, _: &BlockingSink) -> bool {
        true
    }
    fn bind(&self, sink: BlockingSink) -> Result<Connection<Input, Output>, Fault> {
        Ok((
            futures::future::ready(Ok(EventStream::single(Input))).boxed(),
            EventSink::new(sink),
        ))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_waits_for_delivery_resources_after_error_hooks_finish() {
    let gateway = Gateway::new().client(BlockingProtocol).unwrap();
    let (entered, dropping) = oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let handled = tokio::spawn(gateway.handle(
        BlockingSink {
            entered: Some(entered),
            release: released,
        },
        Extensions::new(),
    ));
    dropping.await.unwrap();
    let premature = gateway.idle().now_or_never().is_some();
    release.send(()).unwrap();
    assert!(
        !premature,
        "idle completed while delivery resources were being dropped"
    );
    assert_eq!(
        says(handled.await.unwrap()),
        Err("delivery failed".to_owned())
    );
    gateway.idle().await;
}

struct ReadyProtocol {
    delivery: Mutex<Option<oneshot::Receiver<Result<(), Fault>>>>,
    prepare: Result<(), Fault>,
    submitted: Arc<std::sync::atomic::AtomicBool>,
}

impl Format<Io, Output, Input> for ReadyProtocol {
    fn name(&self) -> &str {
        "ready-lifecycle"
    }

    fn bind(&self, _: Io) -> Result<Connection<Output, Input>, Fault> {
        let delivery = self.delivery.lock().unwrap().take().unwrap();
        let prepare = self.prepare.clone();
        let submitted = self.submitted.clone();
        Ok((
            futures::future::ready(Ok(EventStream::single(Output))).boxed(),
            EventSink::preparing(move |_: &mut EventStream<Input>| {
                prepare?;
                submitted.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(Completion(
                    async move { delivery.await.map_err(|_| Fault::abandoned())? }.boxed(),
                ))
            }),
        ))
    }
}

#[tokio::test]
async fn synchronous_preparation_controls_sent_status_while_delivery_progresses() {
    use std::sync::atomic::{AtomicBool, Ordering};
    for preparation in [Ok(()), Err(Fault::config("cannot prepare input"))] {
        let (finish, delivery) = oneshot::channel();
        let submitted = Arc::new(AtomicBool::new(false));
        let protocol = ReadyProtocol {
            delivery: Mutex::new(Some(delivery)),
            prepare: preparation.clone(),
            submitted: submitted.clone(),
        };
        let record = Record::default();
        let gw = Gateway::new()
            .client::<Input, _, _>(Protocol)
            .unwrap()
            .provider(Provider::new("provider", protocol, |_| Io::Ready))
            .unwrap()
            .plugin(record.clone())
            .target("provider")
            .unwrap();
        let mut handled = gw
            .handle(
                Io::Client {
                    fail_writing: false,
                },
                Extensions::new(),
            )
            .boxed();
        if preparation.is_ok() {
            assert!(handled.as_mut().now_or_never().is_none());
            assert!(submitted.load(Ordering::SeqCst));
            finish.send(Ok(())).unwrap();
        }
        assert_eq!(says(handled.await), says(preparation.clone()));
        gw.idle().await;
        assert_eq!(submitted.load(Ordering::SeqCst), preparation.is_ok());
        assert_eq!(*record.sent.lock().unwrap(), [preparation.is_ok()]);
    }
}
