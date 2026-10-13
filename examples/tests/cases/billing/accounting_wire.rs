//! `Accounting` on a wire that is not HTTP: a test format that speaks the
//! edge contract itself, with no bytes and no heads. The ledger reads
//! neither, so it settles here by the same rules as over HTTP.

use respan_gateway_core::{ErrorCode, Provider};
use std::sync::{Arc, Mutex};

use crate::accounting::{Accounting, Quote, Tally};
use crate::ledger::{Amount, MemoryLedger};
use respan_gateway_core::futures::FutureExt;
use respan_gateway_core::futures::channel::oneshot;
use respan_gateway_core::futures::future::ready;
use respan_gateway_core::futures::{sink, stream};
use respan_gateway_core::{
    Connection, EventSink, EventStream, Extensions, Fault, Format, Gateway, ProviderContext,
};
use respan_gateway_http_faults::Status;

const RESERVE: Amount = 100;

/// One question.
#[derive(Debug, Clone, PartialEq)]
struct Question(String);

/// One reply, and what the provider says it cost, if it says.
#[derive(Debug, Clone, PartialEq)]
struct Reply {
    text: String,
    cost: Option<Amount>,
}

const PIPE: &str = "pipe";

/// A client's wire is its one question; its answer is read to its end and
/// dropped. A provider's reply is `text` or `text:cost`; one not ok is
/// refused at its head. A question `unwritable` cannot be written, so its
/// call fails before it is sent.
enum Io {
    Client(Question),
    Provider(Arc<Scripted>),
}

struct Pipe;

impl Format<Io, Question, Reply> for Pipe {
    fn name(&self) -> &str {
        PIPE
    }

    fn matches(&self, io: &Io) -> bool {
        matches!(io, Io::Client(_))
    }

    fn bind(&self, io: Io) -> Result<Connection<Question, Reply>, Fault> {
        let Io::Client(wire) = io else {
            return Err(Fault::config("expected client I/O"));
        };
        let writer = EventSink::new(sink::unfold(
            (),
            |(), item: Result<Reply, Fault>| async move {
                item?;
                Ok::<_, Fault>(())
            },
        ));
        Ok((ready(Ok(EventStream::single(wire))).boxed(), writer))
    }
}

impl Format<Io, Reply, Question> for Pipe {
    fn name(&self) -> &str {
        PIPE
    }

    fn bind(&self, io: Io) -> Result<Connection<Reply, Question>, Fault> {
        let Io::Provider(dialer) = io else {
            return Err(Fault::config("expected provider I/O"));
        };
        let next = dialer.0.lock().map(|mut s| s.pop()).ok().flatten();
        let (send, receive) = oneshot::channel();
        let incoming = async move { receive.await.map_err(|_| Fault::abandoned()) };
        let outgoing = EventSink::preparing(move |flow: &mut EventStream<Question>| {
            if flow
                .value()
                .transpose()?
                .is_some_and(|q| q.0 == "unwritable")
            {
                return Err(Fault::bad_request("unwritable"));
            }
            Ok(sink::unfold(
                Some((send, next)),
                |state, item: Result<Question, Fault>| async move {
                    item?;
                    if let Some((send, next)) = state {
                        let (ok, frames) =
                            next.unwrap_or_else(|| Err(Fault::upstream("no reply")))?;
                        let flow = if !ok {
                            EventStream::refused(
                                Fault::new("busy")
                                    .with(ErrorCode("busy".into()))
                                    .with(Status(
                                        respan_gateway_http::http::StatusCode::TOO_MANY_REQUESTS,
                                    )),
                            )
                        } else {
                            let replies = frames.into_iter().map(|raw| {
                                Ok(match raw.split_once(':') {
                                    Some((text, cost)) => Reply {
                                        text: text.to_owned(),
                                        cost: cost.parse().ok(),
                                    },
                                    None => Reply {
                                        text: raw.to_owned(),
                                        cost: None,
                                    },
                                })
                            });
                            EventStream::stream(stream::iter(replies))
                        };
                        let _ = send.send(flow);
                    }
                    Ok::<_, Fault>(None)
                },
            ))
        });
        Ok((incoming.boxed(), outgoing))
    }
}

/// Answers each call with the next of its scripted replies: whether the
/// provider took it and its replies, or a failure after the call was
/// handed over.
struct Scripted(Mutex<Vec<Script>>);

/// Whether the answer is ok, and its replies; or a failure.
type Script = Result<(bool, Vec<&'static str>), Fault>;

/// A reserve of 100; each reported cost goes into the tally.
fn quote(px: &mut ProviderContext, tally: Tally) -> Result<Quote, Fault> {
    px.filter(move |reply: &mut Reply| {
        if let Some(cost) = reply.cost {
            tally.add(cost);
        }
        Ok(())
    })?;
    Ok(Quote::new(["key"], RESERVE))
}

#[tokio::test]
async fn accounting_settles_on_a_wire_of_its_own() {
    // (case, question, what the transport does, charged)
    let cases = [
        ("reported", "hi", Ok((true, vec!["hello:4"])), 4),
        ("not reported", "hi", Ok((true, vec!["hello"])), RESERVE),
        ("provider refused", "hi", Ok((false, vec!["busy"])), 0),
        ("sent, no head", "hi", Err(Fault::upstream("lost")), RESERVE),
        (
            "never sent",
            "unwritable",
            Err(Fault::upstream("unused")),
            0,
        ),
    ];
    for (case, question, reply, charged) in cases {
        let ledger = MemoryLedger::new();
        ledger.open("key", None, 1000);
        let transport = Arc::new(Scripted(Mutex::new(vec![reply])));
        let accounting = Accounting::new(ledger.clone(), quote);
        let gw = Gateway::new().client::<Question, _, _>(Pipe);
        let gw = gw.and_then(|g| {
            g.provider(Provider::new::<Question, _, _>("p", Pipe, move |_| {
                Io::Provider(transport.clone())
            }))
        });
        let gw = gw.and_then(|g| g.target("p"));
        let gw = gw.map(|g| g.plugin(accounting));
        let gw = gw.unwrap_or_else(|f| panic!("{f}"));
        let handled = gw.handle(Io::Client(Question(question.into())), Extensions::new());
        drop(handled.await);
        gw.idle().await;
        assert_eq!(ledger.spent("key"), charged, "{case}");
        assert_eq!(ledger.available("key"), 1000 - charged, "{case}");
    }
}
