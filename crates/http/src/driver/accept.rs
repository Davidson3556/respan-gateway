//! The client edge: a request's body read into its inputs, and the answer
//! written back, head and body, when this edge decides (rule 4).

use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt, stream};
use respan_gateway_core::{Ended, Events, Fault, Half, Origin, Reply};

use super::reader::Reader;
use super::writer::Writer;
use super::{Accepted, Came, PANICKED};
use crate::{Answer, Ask, Body, Codec, Frames, Unwritten, into_http_body, plain};

/// What the client edge hands its adapter: the answer's head and body, or
/// a failure no codec can write.
type Handed = Result<(Answer, Frames), Fault>;

/// The client edge of `codec` for `wire`: a one-shot request once its body
/// is read, its input in its origin and its inputs ended; a streaming one at
/// once, its data frames the inputs, which are never ended at once. The
/// writer goes to the wire's slot as the request is in. It never awaits
/// `reply`.
pub fn accept<C: Codec + Clone>(
    codec: &C,
    wire: Accepted,
    reply: BoxFuture<'static, Reply<C::Output>>,
) -> BoxFuture<'static, Half<C::Input>> {
    let codec = codec.clone();
    async move {
        let Accepted {
            head,
            frames,
            streams,
            slot,
        } = wire;
        let head = Arc::new(head);
        if streams {
            let writer = written(codec.clone(), reply, head.clone(), Bytes::new(), true);
            let _ = slot.send(writer.boxed());
            let came = Came {
                head: head.clone(),
                body: None,
            };
            let make = Box::new(move || codec.request_decoder(&head));
            let inputs = Reader::inputs(frames, make);
            return (Some(Arc::new(came) as Origin), Ok(inputs.boxed()));
        }
        // One that fails while read is refused.
        let (raw, broken) = match read_whole(frames).await {
            Ok(bytes) => (bytes, None),
            Err(fault) => (Bytes::new(), Some(fault)),
        };
        let writer = written(codec, reply, head.clone(), raw.clone(), false);
        let _ = slot.send(writer.boxed());
        let came = Arc::new(Came {
            head,
            body: Some(raw),
        }) as Origin;
        match broken {
            None => (Some(came), Ok(stream::empty().boxed())),
            Some(fault) => (Some(came), Err(fault)),
        }
    }
    .boxed()
}

/// The one input of a one-shot request, decoded from its origin; `None`
/// for a streaming one's, or an origin of another wire.
pub fn opening<C: Codec>(codec: &C, origin: &Origin) -> Option<Result<C::Input, Fault>> {
    let came = origin.downcast_ref::<Came>()?;
    let raw = came.body.as_ref()?;
    Some(decode(codec, &came.head, raw))
}

/// The one input of a one-shot request's body.
fn decode<C: Codec>(codec: &C, head: &Ask, raw: &Bytes) -> Result<C::Input, Fault> {
    let mut decoder = codec.request_decoder(head);
    let mut inputs = Vec::new();
    decoder.decode(raw.clone(), &mut inputs)?;
    decoder.end(&mut inputs)?;
    match (inputs.pop(), inputs.is_empty()) {
        (Some(input), true) => Ok(input),
        _ => Err(Fault::internal(
            "a whole request's body decoded to other than one input",
        )),
    }
}

/// A whole body's bytes, or the failure that broke it off.
async fn read_whole(mut body: Frames) -> Result<Bytes, Fault> {
    let mut all = BytesMut::new();
    while let Some(bytes) = body.next().await {
        all.extend_from_slice(&bytes?);
    }
    Ok(all.freeze())
}

/// What the adapter gets: `reply` written to the client whose request had
/// head `ask` and body `body`, streaming if `streaming`; a failure no codec
/// can write as plain text.
async fn written<C: Codec>(
    codec: C,
    reply: BoxFuture<'static, Reply<C::Output>>,
    ask: Arc<Ask>,
    body: Bytes,
    streaming: bool,
) -> http::Response<Body> {
    let (_extensions, (origin, outputs), ended) = reply.await;
    let to = Client {
        codec,
        ask,
        body,
        streaming,
    };
    let handed = match outputs {
        Err(fault) => to.refuse(fault, Some(ended), None).await,
        Ok(outputs) => to.deliver(origin, outputs, ended).await,
    };
    match handed {
        Ok((head, body)) => http::Response::from_parts(head, into_http_body(body)),
        Err(fault) => {
            let mut resp = plain(&fault);
            resp.extensions_mut().insert(Unwritten);
            resp
        }
    }
}

/// The client an answer is written to, in its codec.
struct Client<C> {
    codec: C,
    ask: Arc<Ask>,
    body: Bytes,
    streaming: bool,
}

/// Outputs as a writer takes them.
type Outputs<T> = Events<(T, Option<bool>)>;

impl<C: Codec> Client<C> {
    /// A refusal: an answer that is `fault` alone, which the client's codec
    /// writes. `hold` is an answer whose call it ends.
    async fn refuse(
        self,
        fault: Fault,
        ended: Option<Ended>,
        hold: Option<Outputs<C::Output>>,
    ) -> Handed {
        let Some(ended) = ended else {
            return Err(Fault::internal("an answer's end told twice"));
        };
        let build = || {
            let mut encoder =
                (self.codec).response_encoder(None, &self.ask, &self.body, Some(&fault));
            let head = encoder.head(None);
            (encoder, head)
        };
        // A codec that panics building its encoder cannot write a refusal.
        let Ok((encoder, head)) = std::panic::catch_unwind(AssertUnwindSafe(build)) else {
            return unwritable(Some(ended), Fault::internal(PANICKED));
        };
        let mut writer = Writer::answer(encoder, ended, hold, self.streaming);
        writer.refusal = Some(fault);
        if let Err(fault) = writer.first(false).await {
            return unwritable(writer.ended.take(), fault);
        }
        // With no bytes, it ends as its head goes out.
        if writer.out.is_empty() {
            writer.tell_end();
        }
        Ok((head, writer.frames()))
    }

    /// Send `outputs`, from the provider whose head is `origin` if relayed,
    /// to the client. A one-shot request's head goes out with its body's
    /// first byte, so an answer that fails before any is refused instead; a
    /// streaming request's at once, as its client waits for it.
    async fn deliver(
        self,
        origin: Option<Origin>,
        outputs: Events<C::Output>,
        ended: Ended,
    ) -> Handed {
        let outputs = outputs.map(|o| o.map(|o| (o, None))).boxed();
        // A head goes only to a codec of the name it came in.
        let answer = origin.as_ref().and_then(|o| o.downcast_ref::<Answer>());
        let build = || {
            let codec = &self.codec;
            let mut encoder = codec.response_encoder(answer, &self.ask, &self.body, None);
            let head = encoder.head(None);
            let mut opening = BytesMut::new();
            // A body done before it began reads nothing, and its head goes at once.
            let done = encoder.open(&mut opening)?;
            Ok((encoder, head, opening, done))
        };
        let (encoder, head, opening, done) = match std::panic::catch_unwind(AssertUnwindSafe(build))
        {
            Ok(Ok(built)) => built,
            // Its call ends with the failure, once the refusal is out.
            Ok(Err(fault)) => return self.refuse(fault, Some(ended), Some(outputs)).await,
            // A codec that panics building its encoder cannot write a refusal.
            Err(_) => {
                drop(outputs);
                return unwritable(Some(ended), Fault::internal(PANICKED));
            }
        };
        let streaming = self.streaming;
        let mut writer = Writer::answer(encoder, ended, Some(outputs), streaming);
        writer.out.push(opening, false);
        // The gateway took a provider's words out of a failure it did not
        // relay already.
        if !(streaming || done)
            && let Err(fault) = writer.first(true).await
        {
            let (ended, outputs) = (writer.ended.take(), writer.values.take());
            return self.refuse(fault, ended, outputs).await;
        }
        if done {
            // Its outputs go unread, their call ending as its end, if ready,
            // says.
            drop(writer.values.take());
            if let Some(ended) = writer.ended.take() {
                ended(Ok(()));
            }
            return Ok((head, stream::empty().boxed()));
        }
        // With no bytes, it ends as its head goes out.
        if writer.out.is_empty() {
            writer.tell_end();
        }
        Ok((head, writer.frames()))
    }
}

/// A refusal its codec could not write: the adapter writes it, as an
/// internal fault; the request ends with `fault`.
fn unwritable(ended: Option<Ended>, fault: Fault) -> Handed {
    let handed = Fault::internal(fault.message().to_owned()).with_source(fault.clone());
    if let Some(ended) = ended {
        ended(Err(fault));
    }
    Err(handed)
}
