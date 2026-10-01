//! Both factories use the same edge, including a live exchange that must
//! answer before its incoming direction closes.
#![allow(clippy::unwrap_used)]

use std::time::Duration;

use respan_gateway_core::futures::FutureExt;
use respan_gateway_core::futures::channel::mpsc;
use respan_gateway_core::futures::{SinkExt, StreamExt};
use respan_gateway_core::{Connection, EventSink, EventStream, Extensions, Fault, Format, Gateway};

#[derive(Clone, Debug)]
struct Input(u32);
#[derive(Clone, Debug, PartialEq)]
struct Output(u32);

struct Wire {
    incoming: EventStream<Input>,
    outgoing: mpsc::Sender<Output>,
}

enum Io {
    Client(Wire),
    Provider,
}

struct Echo;

impl Format<Io, Input, Output> for Echo {
    fn name(&self) -> &str {
        "echo"
    }

    fn matches(&self, io: &Io) -> bool {
        matches!(io, Io::Client(_))
    }

    fn bind(&self, io: Io) -> Result<Connection<Input, Output>, Fault> {
        let Io::Client(wire) = io else {
            return Err(Fault::config("expected client I/O"));
        };
        Ok((
            futures::future::ready(Ok(wire.incoming)).boxed(),
            EventSink::new(
                wire.outgoing
                    .sink_map_err(|_| Fault::abandoned())
                    .with(|event: Result<Output, Fault>| futures::future::ready(event)),
            ),
        ))
    }
}

impl Format<Io, Output, Input> for Echo {
    fn name(&self) -> &str {
        "echo"
    }

    fn bind(&self, _: Io) -> Result<Connection<Output, Input>, Fault> {
        // The channel is this in-process echo's actual wire, not a wrapper
        // around a callback that consumes the source stream.
        let (outgoing, incoming) = mpsc::channel(0);
        Ok((
            futures::future::ready(Ok(EventStream::stream(incoming))).boxed(),
            EventSink::new(outgoing.sink_map_err(|_| Fault::abandoned()).with(
                |event: Result<Input, Fault>| {
                    futures::future::ready(Ok(event.map(|Input(n)| Output(n))))
                },
            )),
        ))
    }
}

#[tokio::test]
async fn both_directions_progress_before_either_is_closed() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let gateway = Gateway::new()
            .client::<Input, _, _>(Echo)
            .unwrap()
            .provider::<Input, _, _>("echo", Echo, |_| Io::Provider)
            .unwrap()
            .target("echo")
            .unwrap();
        let (mut input, incoming) = mpsc::channel(0);
        let (outgoing, mut output) = mpsc::channel(0);
        let handled = tokio::spawn(gateway.handle(
            Io::Client(Wire {
                incoming: EventStream::stream(incoming.map(Ok)),
                outgoing,
            }),
            Extensions::new(),
        ));

        input.send(Input(1)).await.unwrap();
        assert_eq!(output.next().await, Some(Output(1)));
        assert!(
            !handled.is_finished(),
            "delivery still has a live incoming half"
        );
        input.send(Input(2)).await.unwrap();
        assert_eq!(output.next().await, Some(Output(2)));
        drop(input);
        assert_eq!(output.next().await, None);
        assert_eq!(handled.await.unwrap(), Ok(()));
        gateway.idle().await;
    })
    .await
    .expect("one edge waited for the other direction to close");
}
