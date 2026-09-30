//! Both factories use the same edge, including a live exchange that must
//! answer before its incoming direction closes.
#![allow(clippy::unwrap_used)]

use std::time::Duration;

use respan_gateway_core::futures::FutureExt;
use respan_gateway_core::futures::channel::{mpsc, oneshot};
use respan_gateway_core::futures::{SinkExt, StreamExt};
use respan_gateway_core::{Connection, Events, Extensions, Fault, Format, Gateway};

#[derive(Clone, Debug)]
struct Input(u32);
#[derive(Clone, Debug, PartialEq)]
struct Output(u32);

struct Wire {
    incoming: Events<Input>,
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
            Box::pin(respan_gateway_core::write(move |flow: Events<Output>| {
                let mut outgoing = wire.outgoing;
                Ok(async move {
                    let mut events = flow;
                    while let Some(event) = events.next().await {
                        outgoing
                            .send(event?)
                            .await
                            .map_err(|_| Fault::abandoned())?;
                    }
                    Ok(())
                })
            })),
        ))
    }
}

impl Format<Io, Output, Input> for Echo {
    fn name(&self) -> &str {
        "echo"
    }

    fn bind(&self, _: Io) -> Result<Connection<Output, Input>, Fault> {
        let (ready, opening) = oneshot::channel();
        Ok((
            async move { opening.await.map_err(|_| Fault::abandoned()) }.boxed(),
            Box::pin(respan_gateway_core::write(move |flow: Events<Input>| {
                Ok(async move {
                    let replies = flow.map(|event| event.map(|Input(n)| Output(n)));
                    ready
                        .send(Events::stream(replies))
                        .map_err(|_| Fault::abandoned())?;
                    Ok(())
                })
            })),
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
                incoming: Events::stream(incoming.map(Ok)),
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
