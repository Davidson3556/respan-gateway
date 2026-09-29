//! `Accounting` on a wire that is not HTTP: a test format that speaks the
//! edge contract itself, with no bytes and no heads. The ledger reads
//! neither, so it settles here by the same rules as over HTTP.

use std::sync::{Arc, Mutex};

use respan_gateway_core::futures::future::{BoxFuture, ready};
use respan_gateway_core::futures::{FutureExt, StreamExt, TryStreamExt, stream};
use respan_gateway_core::{
    Dialed, Ended, Events, Extensions, Fault, Format, Gateway, Half, Origin, ProviderContext,
};
use respan_gateway_http::Status;
use respan_gateway_ledger::{Accounting, Amount, MemoryLedger, Quote, Tally};

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
struct Pipe;

impl Format for Pipe {
    type Input = Question;
    type Output = Reply;
    type Wire = Question;
    type Dialer = Scripted;

    fn name(&self) -> &str {
        PIPE
    }

    fn accepts(&self, _wire: &Question) -> bool {
        true
    }

    fn accept(
        &self,
        wire: Question,
        reply: BoxFuture<'static, (Half<Reply>, Ended)>,
    ) -> BoxFuture<'static, (Option<Origin>, Result<Option<Events<Question>>, Fault>)> {
        tokio::spawn(async move {
            let ((_, outputs), ended) = reply.await;
            let end = match outputs {
                Ok(outputs) => outputs.try_for_each(|_| ready(Ok(()))).await,
                Err(fault) => Err(fault),
            };
            ended(end);
        });
        let origin = Arc::new(wire) as Origin;
        ready((Some(origin), Ok(None))).boxed()
    }

    fn opening(&self, origin: &Origin) -> Option<Result<Question, Fault>> {
        origin.downcast_ref::<Question>().cloned().map(Ok)
    }

    fn dial(
        &self,
        dialer: &Scripted,
        _extensions: Extensions,
        relay: Option<&Origin>,
        opening: Option<(&Question, bool)>,
        _later: Option<Events<(Question, bool)>>,
    ) -> Result<Dialed<Reply>, Fault> {
        let relayed = relay.and_then(|o| o.downcast_ref::<Question>());
        let question = opening.map(|(q, _)| q).or(relayed);
        if question.is_some_and(|q| q.0 == "unwritable") {
            return Err(Fault::bad_request("unwritable"));
        }
        let next = dialer.0.lock().map(|mut s| s.pop()).ok().flatten();
        Ok(async move {
            let (ok, frames) = next.unwrap_or_else(|| Err(Fault::upstream("no reply")))?;
            let origin = Some(Arc::new(ok) as Origin);
            if !ok {
                let busy = Fault::new("busy", "busy").with(Status(429));
                return Ok((origin, Err(busy)));
            }
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
            Ok((origin, Ok(stream::iter(replies).boxed())))
        }
        .boxed())
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
        let transport = Scripted(Mutex::new(vec![reply]));
        let accounting = Accounting::new(ledger.clone(), quote);
        let gw = Gateway::new().serve(Pipe);
        let gw = gw.and_then(|g| g.provider("p", Pipe, transport));
        let gw = gw.and_then(|g| g.target("p"));
        let gw = gw.and_then(|g| g.plugin("accounting", accounting));
        let gw = gw.unwrap_or_else(|f| panic!("{f}"));
        let outcome = gw
            .handle(Question(question.into()), Extensions::new())
            .await;
        drop(outcome.await);
        gw.idle().await;
        assert_eq!(ledger.spent("key"), charged, "{case}");
        assert_eq!(ledger.available("key"), 1000 - charged, "{case}");
    }
}
