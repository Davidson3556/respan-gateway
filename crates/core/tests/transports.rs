//! One format over multiple transport types and fresh transports per attempt.
#![allow(clippy::unwrap_used, clippy::type_complexity)]

mod faults;

use parking_lot::Mutex;
use std::cell::Cell;
use std::sync::Arc;

use respan_gateway_core::futures::FutureExt;
use respan_gateway_core::futures::channel::oneshot;
use respan_gateway_core::{
    ClientContext, Connection, EventSink, EventStream, Extensions, Fault, Format, Gateway, Plugin,
    Provider, ProviderContext, Response, async_trait,
};

#[derive(Clone)]
struct Input(u8);
#[derive(Clone)]
struct Output(u8);
#[derive(Clone)]
struct FromHook;

#[derive(Default)]
struct Trace {
    factories: Mutex<Vec<(usize, bool)>>,
    sent: Mutex<Vec<(usize, u8)>>,
    received: Mutex<Vec<u8>>,
    ended: Mutex<Vec<bool>>,
}

enum Io {
    Client(Arc<Trace>),
    Provider {
        attempt: usize,
        fail: bool,
        trace: Arc<Trace>,
    },
}

// Cell deliberately makes this transport Send without Sync.
struct First(Io, Cell<u8>);
struct Second(Io);
struct Messages;

fn client(io: Io) -> Result<Connection<Input, Output>, Fault> {
    let Io::Client(trace) = io else {
        return Err(Fault::config("wrong direction"));
    };
    let incoming = EventStream::lazy(|| Ok(Input(7)));
    let outgoing = EventSink::preparing(move |_outputs: &mut EventStream<Output>| {
        Ok(futures::sink::unfold(
            trace,
            move |trace, output: Result<Output, Fault>| async move {
                trace.received.lock().push(output?.0);
                Ok::<_, Fault>(trace)
            },
        ))
    });
    Ok((futures::future::ready(Ok(incoming)).boxed(), outgoing))
}

fn provider(io: Io) -> Result<Connection<Output, Input>, Fault> {
    let Io::Provider {
        attempt,
        fail,
        trace,
    } = io
    else {
        return Err(Fault::config("wrong direction"));
    };
    let (answer, answered) = oneshot::channel();
    let incoming = async move { answered.await.map_err(|_| Fault::abandoned()) }.boxed();
    let outgoing = EventSink::preparing(move |inputs: &mut EventStream<Input>| {
        let value = inputs
            .value()
            .transpose()?
            .ok_or_else(|| faults::bad_request("one input"))?
            .0;
        trace.sent.lock().push((attempt, value));
        Ok(futures::sink::unfold(
            Some(answer),
            move |mut answer, input: Result<Input, Fault>| async move {
                let input = input?;
                if fail {
                    return Err(faults::upstream("retry this attempt"));
                }
                let outputs = EventStream::single(Output(input.0));
                answer
                    .take()
                    .ok_or_else(|| Fault::internal("extra input"))?
                    .send(outputs)
                    .map_err(|_| Fault::abandoned())?;
                Ok(answer)
            },
        ))
    });
    Ok((incoming, outgoing))
}

impl Format<First, Input, Output> for Messages {
    fn name(&self) -> &str {
        "messages"
    }

    fn matches(&self, io: &First) -> bool {
        io.1.get() == 0 && matches!(io.0, Io::Client(_))
    }

    fn bind(&self, io: First) -> Result<Connection<Input, Output>, Fault> {
        client(io.0)
    }
}

impl Format<First, Output, Input> for Messages {
    fn name(&self) -> &str {
        "messages"
    }

    fn bind(&self, io: First) -> Result<Connection<Output, Input>, Fault> {
        provider(io.0)
    }
}

impl Format<Second, Input, Output> for Messages {
    fn name(&self) -> &str {
        "messages"
    }

    fn matches(&self, io: &Second) -> bool {
        matches!(io.0, Io::Client(_))
    }

    fn bind(&self, io: Second) -> Result<Connection<Input, Output>, Fault> {
        client(io.0)
    }
}

impl Format<Second, Output, Input> for Messages {
    fn name(&self) -> &str {
        "messages"
    }

    fn bind(&self, io: Second) -> Result<Connection<Output, Input>, Fault> {
        provider(io.0)
    }
}

fn attempt(trace: &Arc<Trace>, extensions: Extensions, fail_first: bool) -> Io {
    let mut factories = trace.factories.lock();
    let attempt = factories.len() + 1;
    factories.push((attempt, extensions.get::<FromHook>().is_some()));
    Io::Provider {
        attempt,
        fail: fail_first && attempt == 1,
        trace: trace.clone(),
    }
}

struct Hooks {
    trace: Arc<Trace>,
    retry: bool,
    own: bool,
    edit: bool,
    reject: bool,
}

#[async_trait]
impl Plugin for Hooks {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        if self.edit {
            cx.filter(|input: &mut Input| {
                input.0 += 1;
                Ok(())
            })?;
        }
        if self.own {
            return cx.call("p").with_input(Input(9)).await.map(Some);
        }
        if self.retry {
            let call = cx.call("p");
            assert!(call.clone().await.is_err());
            return call.await.map(Some);
        }
        Ok(None)
    }
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        px.extensions_mut().insert(FromHook);
        if self.reject {
            return Err(Fault::config("hook refused before transport creation"));
        }
        Ok(())
    }
    async fn on_provider_end(&self, px: &ProviderContext, _: &Result<(), Fault>) {
        self.trace.ended.lock().push(px.is_sent());
    }
}

fn hooks(trace: &Arc<Trace>) -> Hooks {
    Hooks {
        trace: trace.clone(),
        retry: false,
        own: false,
        edit: false,
        reject: false,
    }
}

#[tokio::test]
async fn same_format_sends_plain_typed_input_across_different_transports() {
    let trace = Arc::new(Trace::default());
    let factories = trace.clone();
    let gateway = Gateway::new()
        .client::<Input, Output, First>(Messages)
        .unwrap()
        .client::<Input, Output, Second>(Messages)
        .unwrap()
        .provider(Provider::new::<Input, _, _>(
            "p",
            Messages,
            move |extensions| Second(attempt(&factories, extensions, false)),
        ))
        .unwrap()
        .plugin(hooks(&trace))
        .target("p")
        .unwrap();
    gateway
        .handle(
            First(Io::Client(trace.clone()), Cell::new(0)),
            Extensions::new(),
        )
        .await
        .unwrap();
    gateway.idle().await;
    assert_eq!(*trace.sent.lock(), [(1, 7)]);
    assert_eq!(*trace.received.lock(), [7]);
    assert_eq!(*trace.factories.lock(), [(1, true)]);

    // The first registered transport does not consume an incompatible
    // value while matching; the second implementation receives it intact.
    gateway
        .handle(Second(Io::Client(trace.clone())), Extensions::new())
        .await
        .unwrap();
    gateway.idle().await;
    assert_eq!(*trace.sent.lock(), [(1, 7), (2, 7)]);
    assert_eq!(*trace.received.lock(), [7, 7]);
}

#[tokio::test]
async fn same_transport_sends_filtered_typed_input() {
    let trace = Arc::new(Trace::default());
    let factories = trace.clone();
    let mut plugin = hooks(&trace);
    plugin.edit = true;
    let gateway = Gateway::new()
        .client::<Input, Output, First>(Messages)
        .unwrap()
        .provider(Provider::new::<Input, _, _>(
            "p",
            Messages,
            move |extensions| First(attempt(&factories, extensions, false), Cell::new(0)),
        ))
        .unwrap()
        .plugin(plugin)
        .target("p")
        .unwrap();
    gateway
        .handle(
            First(Io::Client(trace.clone()), Cell::new(0)),
            Extensions::new(),
        )
        .await
        .unwrap();
    gateway.idle().await;
    assert_eq!(*trace.sent.lock(), [(1, 8)]);
    assert_eq!(*trace.received.lock(), [8]);
}

#[tokio::test]
async fn own_input_reaches_a_provider_on_the_same_transport() {
    let trace = Arc::new(Trace::default());
    let factories = trace.clone();
    let mut plugin = hooks(&trace);
    plugin.own = true;
    let gateway = Gateway::new()
        .client::<Input, Output, First>(Messages)
        .unwrap()
        .provider(Provider::new::<Input, _, _>(
            "p",
            Messages,
            move |extensions| First(attempt(&factories, extensions, false), Cell::new(0)),
        ))
        .unwrap()
        .plugin(plugin)
        .target("p")
        .unwrap();
    gateway
        .handle(
            First(Io::Client(trace.clone()), Cell::new(0)),
            Extensions::new(),
        )
        .await
        .unwrap();
    gateway.idle().await;
    assert_eq!(*trace.sent.lock(), [(1, 9)]);
    assert_eq!(*trace.received.lock(), [9]);
}

#[tokio::test]
async fn retries_create_distinct_transports_after_each_attempts_hooks() {
    let trace = Arc::new(Trace::default());
    let factories = trace.clone();
    let mut plugin = hooks(&trace);
    plugin.retry = true;
    let gateway = Gateway::new()
        .client::<Input, Output, First>(Messages)
        .unwrap()
        .provider(Provider::new::<Input, _, _>(
            "p",
            Messages,
            move |extensions| First(attempt(&factories, extensions, true), Cell::new(0)),
        ))
        .unwrap()
        .plugin(plugin)
        .target("p")
        .unwrap();
    gateway
        .handle(
            First(Io::Client(trace.clone()), Cell::new(0)),
            Extensions::new(),
        )
        .await
        .unwrap();
    gateway.idle().await;
    assert_eq!(*trace.factories.lock(), [(1, true), (2, true)]);
    assert_eq!(*trace.sent.lock(), [(1, 7), (2, 7)]);
    assert_eq!(*trace.received.lock(), [7]);
}

#[tokio::test]
async fn rejected_provider_hook_does_not_create_a_transport() {
    let trace = Arc::new(Trace::default());
    let factories = trace.clone();
    let mut plugin = hooks(&trace);
    plugin.reject = true;
    let gateway = Gateway::new()
        .client::<Input, Output, First>(Messages)
        .unwrap()
        .provider(Provider::new::<Input, _, _>(
            "p",
            Messages,
            move |extensions| First(attempt(&factories, extensions, false), Cell::new(0)),
        ))
        .unwrap()
        .plugin(plugin)
        .target("p")
        .unwrap();
    assert!(
        gateway
            .handle(
                First(Io::Client(trace.clone()), Cell::new(0)),
                Extensions::new()
            )
            .await
            .is_err()
    );
    gateway.idle().await;
    assert!(trace.factories.lock().is_empty());
    assert_eq!(*trace.ended.lock(), [false]);
}

#[tokio::test]
async fn a_panicking_transport_factory_is_an_unsent_internal_failure() {
    let trace = Arc::new(Trace::default());
    let gateway = Gateway::new()
        .client::<Input, Output, First>(Messages)
        .unwrap()
        .provider(Provider::new::<Input, _, _>("p", Messages, |_| -> First {
            panic!("factory failed")
        }))
        .unwrap()
        .plugin(hooks(&trace))
        .target("p")
        .unwrap();
    let fault = gateway
        .handle(
            First(Io::Client(trace.clone()), Cell::new(0)),
            Extensions::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(faults::code(&fault), "internal");
    gateway.idle().await;
    assert_eq!(*trace.ended.lock(), [false]);
}

#[test]
fn registration_checks_the_kind_after_assigning_the_edge_role() {
    let gateway = Gateway::new()
        .client::<Input, Output, First>(Messages)
        .unwrap();
    // This selects the receive-Input/send-Output implementation for an
    // outgoing call: the gateway must interpret its kind in reverse.
    let fault = gateway
        .provider(Provider::new::<Output, Input, First>(
            "backwards",
            Messages,
            |_| panic!("registration must not construct I/O"),
        ))
        .unwrap_err();
    assert_eq!(faults::code(&fault), "config");
    assert_eq!(
        fault.message(),
        "two formats named messages are of different kinds"
    );
}
