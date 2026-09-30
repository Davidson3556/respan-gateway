//! Cross-direction lifecycle guarantees of the symmetric edge contract.
#![allow(clippy::unwrap_used)]

use respan_gateway_core::futures::future::BoxFuture;
use respan_gateway_core::futures::{FutureExt, Sink};
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use respan_gateway_core::futures::StreamExt;
use respan_gateway_core::futures::channel::oneshot;
use respan_gateway_core::{
    ClientContext, Connection, Events, Extensions, Fault, Format, Gateway, Plugin, ProviderContext,
    Response, async_trait,
};

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
    incoming: Mutex<Option<Events<Output>>>,
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
        let outgoing = respan_gateway_core::write(move |flow: Events<Output>| {
            if fail_writing {
                drop(flow);
                return Err(Fault::new("write_failed", "client delivery failed"));
            }
            Ok(async move {
                let mut events = flow;
                while let Some(event) = events.next().await {
                    event?;
                }
                Ok(())
            })
        });
        Ok((
            futures::future::ready(Ok(Events::single(Input))).boxed(),
            Box::pin(outgoing),
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
        let outgoing = respan_gateway_core::write(move |_: Events<Input>| Ok(delivery));
        Ok((
            futures::future::ready(Ok(incoming)).boxed(),
            Box::pin(outgoing),
        ))
    }
}

#[derive(Clone, Default)]
struct Record {
    provider: Arc<Mutex<Vec<Result<(), Fault>>>>,
    client: Arc<Mutex<Vec<Result<(), Fault>>>>,
    hold: bool,
    sent: Arc<Mutex<Vec<bool>>>,
}

#[async_trait]
impl Plugin for Record {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        if self.hold {
            let (response, _) = cx.call("provider").await?.until_end().await?;
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
    gateway_with(record, writing, Events::single(Output))
}

fn gateway_with(
    record: &Record,
    writing: BoxFuture<'static, Result<(), Fault>>,
    incoming: Events<Output>,
) -> Gateway {
    Gateway::new()
        .client::<Input, _, _>(Protocol)
        .unwrap()
        .provider::<Input, _, _>("provider", Protocol, {
            let dialer = Arc::new(Dialer {
                writing: Mutex::new(Some(writing)),
                incoming: Mutex::new(Some(incoming)),
            });
            move |_| Io::Provider(dialer.clone())
        })
        .unwrap()
        .plugin("record", record.clone())
        .unwrap()
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
    let fault = Fault::new("flush_failed", "provider's last write failed");
    finish.send(Err(fault.clone())).unwrap();
    assert_eq!(handled.await, Err(fault.clone()));
    gw.idle().await;
    assert_eq!(*record.provider.lock().unwrap(), [Err(fault)]);
}

#[tokio::test]
async fn client_preparation_failure_does_not_settle_buffered_provider_success() {
    let record = Record {
        hold: true,
        ..Record::default()
    };
    let gw = gateway(&record, async { Ok(()) }.boxed());
    let fault = Fault::new("write_failed", "client delivery failed");
    assert_eq!(
        gw.handle(Io::Client { fail_writing: true }, Extensions::new())
            .await,
        Err(fault.clone())
    );
    gw.idle().await;
    assert_eq!(*record.client.lock().unwrap(), [Err(fault.clone())]);
    assert_eq!(*record.provider.lock().unwrap(), [Err(fault)]);
}

#[tokio::test]
async fn provider_refusal_cancels_pending_upload_without_waiting_for_it() {
    let (finish, finished) = oneshot::channel::<Result<(), Fault>>();
    let writing = async move { finished.await.map_err(|_| Fault::abandoned())? }.boxed();
    let record = Record::default();
    let refusal = Fault::new("refused", "provider declined input");
    let gw = gateway_with(&record, writing, Events::refused(refusal.clone()));
    assert_eq!(
        gw.handle(
            Io::Client {
                fail_writing: false
            },
            Extensions::new()
        )
        .await,
        Err(refusal.clone())
    );
    assert!(finish.is_canceled());
    gw.idle().await;
    assert_eq!(*record.provider.lock().unwrap(), [Err(refusal)]);
}

#[tokio::test]
async fn cancelling_exchange_drops_both_pending_provider_directions() {
    let (finish, finished) = oneshot::channel::<Result<(), Fault>>();
    let writing = async move { finished.await.map_err(|_| Fault::abandoned())? }.boxed();
    let record = Record::default();
    let gw = gateway_with(&record, writing, Events::stream(futures::stream::pending()));
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
    assert_eq!(*record.provider.lock().unwrap(), [Err(Fault::abandoned())]);
    assert_eq!(*record.client.lock().unwrap(), [Err(Fault::abandoned())]);
}

struct ReadyProtocol {
    ready: Mutex<Option<oneshot::Receiver<Result<(), Fault>>>>,
    submitted: Arc<std::sync::atomic::AtomicBool>,
}

struct ReadySink {
    ready: oneshot::Receiver<Result<(), Fault>>,
    submitted: Arc<std::sync::atomic::AtomicBool>,
}

impl Sink<Events<Input>> for ReadySink {
    type Error = Fault;

    fn poll_ready(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Fault>> {
        use std::future::Future;
        let ready = std::task::ready!(Pin::new(&mut self.ready).poll(cx));
        std::task::Poll::Ready(ready.map_err(|_| Fault::abandoned())?)
    }

    fn start_send(self: Pin<&mut Self>, _: Events<Input>) -> Result<(), Fault> {
        self.submitted
            .store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Fault>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Fault>> {
        self.poll_flush(cx)
    }
}

#[allow(clippy::type_complexity)]
impl Format<Io, Output, Input> for ReadyProtocol {
    fn name(&self) -> &str {
        "ready-lifecycle"
    }

    fn bind(&self, _: Io) -> Result<Connection<Output, Input>, Fault> {
        let outgoing = ReadySink {
            ready: self.ready.lock().unwrap().take().unwrap(),
            submitted: self.submitted.clone(),
        };
        Ok((
            futures::future::ready(Ok(Events::single(Output))).boxed(),
            Box::pin(outgoing),
        ))
    }
}

#[tokio::test]
async fn sink_readiness_controls_submission_and_sent_status() {
    use std::sync::atomic::{AtomicBool, Ordering};
    for readiness in [Ok(()), Err(Fault::config("cannot prepare input"))] {
        let (ready, ready_receiver) = oneshot::channel();
        let submitted = Arc::new(AtomicBool::new(false));
        let protocol = ReadyProtocol {
            ready: Mutex::new(Some(ready_receiver)),
            submitted: submitted.clone(),
        };
        let record = Record::default();
        let gw = Gateway::new()
            .client::<Input, _, _>(Protocol)
            .unwrap()
            .provider::<Input, _, _>("provider", protocol, |_| Io::Ready)
            .unwrap()
            .plugin("record", record.clone())
            .unwrap()
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
        assert!(handled.as_mut().now_or_never().is_none());
        assert!(!submitted.load(Ordering::SeqCst));
        ready.send(readiness.clone()).unwrap();
        assert_eq!(handled.await, readiness);
        gw.idle().await;
        assert_eq!(submitted.load(Ordering::SeqCst), readiness.is_ok());
        assert_eq!(*record.sent.lock().unwrap(), [readiness.is_ok()]);
    }
}
