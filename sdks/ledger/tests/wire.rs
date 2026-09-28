//! `Accounting` on a wire that is not HTTP: a test format and transport
//! with heads of their own. The ledger reads no head, so it settles here
//! by the same rules as over HTTP.

use std::sync::Mutex;

use respan_gateway_core::bytes::{Bytes, BytesMut};
use respan_gateway_core::futures::{StreamExt, stream};
use respan_gateway_core::{
    Decoder, Encoder, Exchange, Fault, Format, Gateway, ProviderContext, Transport, async_trait,
};
use respan_gateway_ledger::{Accounting, Amount, MemoryLedger, Quote, Tally};

const RESERVE: Amount = 100;

/// A request's head on the pipe wire.
#[derive(Debug, Clone)]
struct Ask;

/// A response's head on the pipe wire: whether the provider took the call.
#[derive(Debug)]
struct Answer {
    ok: bool,
}

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

/// Bodies are text; a reply is `text` or `text:cost`. A question `unwritable`
/// cannot be written, so its call fails before it is sent.
struct Pipe;

impl Format for Pipe {
    type Ask = Ask;
    type Answer = Answer;
    type Input = Question;
    type Output = Reply;

    fn name(&self) -> &str {
        PIPE
    }

    fn accepts(&self, _head: &Ask) -> bool {
        true
    }

    fn request_decoder(&self, _head: &Ask) -> Box<dyn Decoder<Question>> {
        respan_gateway_formats::message_decoder(|raw: Bytes| {
            Ok(Question(String::from_utf8_lossy(&raw).into_owned()))
        })
    }

    fn request_encoder(
        &self,
        _client: Option<&Ask>,
        body: &mut Bytes,
        _streams: bool,
    ) -> Box<dyn Encoder<Question, Ask>> {
        Box::new(Questions(&body[..] == b"unwritable"))
    }

    fn response_decoder(
        &self,
        answer: &Answer,
        _call: &Ask,
        _sent: &Bytes,
    ) -> Box<dyn Decoder<Reply>> {
        Box::new(Replies { ok: answer.ok })
    }

    fn response_encoder(
        &self,
        _answer: Option<&Answer>,
        _ask: &Ask,
        _body: &Bytes,
        refusal: Option<&Fault>,
    ) -> Box<dyn Encoder<Reply, Answer>> {
        Box::new(Texts(refusal.is_none()))
    }
}

/// Whether the call's relayed body cannot be written.
struct Questions(bool);

impl Encoder<Question, Ask> for Questions {
    fn head(&mut self, _first: Option<&Question>) -> Ask {
        Ask
    }

    fn open(&mut self, _out: &mut BytesMut) -> Result<(), Fault> {
        match self.0 {
            true => Err(Fault::bad_request("unwritable")),
            false => Ok(()),
        }
    }

    fn encode(&mut self, q: &Question, out: &mut BytesMut) -> Result<(), Fault> {
        out.extend_from_slice(q.0.as_bytes());
        Ok(())
    }

    fn end(&mut self, _end: &Result<(), Fault>, _out: &mut BytesMut) -> Result<(), Fault> {
        Ok(())
    }
}

/// One reply per frame; an answer that is not ok ends as the provider's
/// answer, with no reply.
struct Replies {
    ok: bool,
}

impl Decoder<Reply> for Replies {
    fn decode(&mut self, bytes: Bytes, out: &mut Vec<Reply>) -> Result<(), Fault> {
        if !self.ok {
            return Ok(());
        }
        let raw = String::from_utf8_lossy(&bytes);
        let reply = match raw.split_once(':') {
            Some((text, cost)) => Reply {
                text: text.to_owned(),
                cost: cost.parse().ok(),
            },
            None => Reply {
                text: raw.into_owned(),
                cost: None,
            },
        };
        out.push(reply);
        Ok(())
    }

    fn end(&mut self, _out: &mut Vec<Reply>) -> Result<(), Fault> {
        match self.ok {
            true => Ok(()),
            false => Err(Fault::new(429u16, "busy", "busy").with_answer(PIPE, ())),
        }
    }
}

struct Texts(bool);

impl Encoder<Reply, Answer> for Texts {
    fn head(&mut self, _first: Option<&Reply>) -> Answer {
        Answer { ok: self.0 }
    }

    fn encode(&mut self, reply: &Reply, out: &mut BytesMut) -> Result<(), Fault> {
        out.extend_from_slice(reply.text.as_bytes());
        Ok(())
    }

    fn end(&mut self, _end: &Result<(), Fault>, _out: &mut BytesMut) -> Result<(), Fault> {
        Ok(())
    }
}

/// Answers each call with the next of its scripted replies: an answer head
/// and body frames, or a failure after the call was handed over.
struct Scripted(Mutex<Vec<Script>>);

/// Whether the answer is ok, and its body's frames; or a failure.
type Script = Result<(bool, Vec<&'static str>), Fault>;

#[async_trait]
impl Transport for Scripted {
    type Ask = Ask;
    type Answer = Answer;

    async fn send(&self, _call: Exchange<Ask>) -> Result<Exchange<Answer>, Fault> {
        let next = self.0.lock().map(|mut s| s.pop()).ok().flatten();
        let (ok, frames) = next.unwrap_or_else(|| Err(Fault::upstream("no reply")))?;
        let frames = frames.into_iter().map(|f| Ok(Bytes::from(f)));
        Ok(Exchange::new(Answer { ok }, stream::iter(frames).boxed()))
    }
}

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
        let body = stream::iter([Ok(Bytes::from(question))]).boxed();
        let (resp, outcome) = gw
            .handle::<Ask, Answer>(Exchange::new(Ask, body), false)
            .await;
        if let Ok(resp) = resp {
            let _: Vec<_> = resp.body.collect().await;
        }
        drop(outcome.await);
        gw.idle().await;
        assert_eq!(ledger.spent("key"), charged, "{case}");
        assert_eq!(ledger.available("key"), 1000 - charged, "{case}");
    }
}
