//! The client edge: a request's body read into its inputs, and the answer
//! written back, head and body, when this edge decides (rule 3).

use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures::channel::oneshot;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt, stream};
use respan_gateway_core::{Events, Fault, write};

use super::reader::Reader;
use super::writer::Writer;
use super::{
    Completion, Connection, PANICKED, RequestMetadata, ResponseMetadata, Transport, TransportState,
};
use crate::{
    Body, Codec, Encoder, Frames, RequestHead, ResponseHead, Unwritten, answered, bodiless,
    fresh_response_head, into_http_body, plain,
};

/// What the client edge hands its adapter: the answer's head and body, or
/// a failure no codec can write.
type Handed = Result<(ResponseHead, Frames), Fault>;

/// Build the two directions of an accepted HTTP exchange. Reading the
/// incoming stream prepares protocol state needed by the response writer.
pub fn accept<C: Codec + Clone>(
    codec: &C,
    io: Transport,
) -> Result<Connection<C::Input, C::Output>, Fault> {
    let TransportState::Accepted { head, incoming } = io.state.as_ref() else {
        return Err(Fault::config(
            "an outbound transport cannot accept a client",
        ));
    };
    let (frames, streams, slot) = incoming
        .lock()
        .map_err(|_| Fault::config("accepted transport lock poisoned"))?
        .take()
        .ok_or_else(|| Fault::config("accepted transport already bound"))?;
    let head = head.clone();
    let (prepared, preparation) = oneshot::channel();
    let reader = codec.clone();
    let incoming = async move {
        if streams {
            let _ = prepared.send((head.clone(), Bytes::new()));
            let came = RequestMetadata {
                head: head.clone(),
                body: None,
            };
            let name = reader.name().to_owned();
            let make = Box::new(move || reader.request_decoder(&head));
            return Ok(Events::stream(Reader::inputs(frames, make)).with_metadata(&name, came));
        }
        let read = read_whole(frames).await;
        let raw = read.as_ref().cloned().unwrap_or_default();
        let _ = prepared.send((head.clone(), raw.clone()));
        read?;
        let came = RequestMetadata {
            head: head.clone(),
            body: Some(raw.clone()),
        };
        let name = reader.name().to_owned();
        Ok(Events::lazy(move || decode(&reader, &head, &raw)).with_metadata(&name, came))
    }
    .boxed();
    let codec = codec.clone();
    let outgoing = write(move |events| {
        let (tell, told) = oneshot::channel();
        let completion = told
            .map(|end| end.unwrap_or_else(|_| Err(Fault::abandoned())))
            .boxed()
            .shared();
        let delivered = completion.clone();
        Ok(async move {
            let (head, body) = preparation.await.map_err(|_| Fault::internal(PANICKED))?;
            let writing = async move {
                let response = written(codec, events, Completion(tell), head, body, streams).await;
                (response, completion.boxed())
            }
            .boxed();
            slot.send(writing).map_err(|_| Fault::abandoned())?;
            delivered.await
        })
    });
    Ok((incoming, Box::pin(outgoing)))
}

/// The one input of a one-shot request's body.
fn decode<C: Codec>(codec: &C, head: &RequestHead, raw: &Bytes) -> Result<C::Input, Fault> {
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

/// Encode a supplied events and report completion after its final frame.
async fn written<C: Codec>(
    codec: C,
    events: Events<C::Output>,
    completion: Completion,
    ask: Arc<RequestHead>,
    body: Bytes,
    streaming: bool,
) -> http::Response<Body> {
    let answer = events
        .metadata::<ResponseMetadata>(codec.name())
        .map(|state| state.0.clone());
    let to = Client {
        codec,
        ask,
        body,
        streaming,
    };
    let handed = match events.refusal().cloned() {
        Some(fault) => to.refuse(fault, Some(completion), None).await,
        None => to.deliver(answer, events.boxed(), completion).await,
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
    ask: Arc<RequestHead>,
    body: Bytes,
    streaming: bool,
}

impl<C: Codec> Client<C> {
    /// A refusal: an answer that is `fault` alone, which the client's codec
    /// writes. `hold` is an answer whose call it ends.
    async fn refuse(
        self,
        fault: Fault,
        ended: Option<Completion>,
        hold: Option<BoxStream<'static, Result<C::Output, Fault>>>,
    ) -> Handed {
        let Some(ended) = ended else {
            return Err(Fault::internal("an answer's end told twice"));
        };
        // The provider's answer as it came, else the codec's words under the
        // fault's status and `Retry-After`. A codec that panics writing its
        // refusal cannot write one.
        let mut head = fresh_response_head();
        let answer = answered(self.codec.name(), &fault, &mut head);
        let write = || answer.unwrap_or_else(|| self.codec.refusal(&fault, &self.body, &mut head));
        let Ok(body) = std::panic::catch_unwind(AssertUnwindSafe(write)) else {
            return unwritable(Some(ended), Fault::internal(PANICKED));
        };
        // No body goes where HTTP has none.
        let body = match bodiless(&self.ask.method, head.status) {
            true => Bytes::new(),
            false => body,
        };
        let mut encoder = Refusal { head, body };
        let head = Encoder::<C::Output, _>::head(&mut encoder, None);
        let mut writer = Writer::answer(Box::new(encoder), ended, hold, self.streaming);
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
        answer: Option<ResponseHead>,
        outputs: BoxStream<'static, Result<C::Output, Fault>>,
        ended: Completion,
    ) -> Handed {
        // A head goes only to a codec of the name it came in.
        let answer = answer.as_ref();
        let build = || {
            let codec = &self.codec;
            let mut encoder = codec.response_encoder(answer, &self.ask, &self.body);
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
                ended.finish(Ok(()));
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

/// A refusal's encoder: its head as written, and its body at the end,
/// however it ends.
struct Refusal {
    head: ResponseHead,
    body: Bytes,
}

impl<T> Encoder<T, ResponseHead> for Refusal {
    fn head(&mut self, _first: Option<&T>) -> ResponseHead {
        std::mem::replace(&mut self.head, fresh_response_head())
    }

    fn encode(&mut self, _value: &T, _out: &mut BytesMut) -> Result<(), Fault> {
        Ok(())
    }

    fn end(&mut self, _end: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault> {
        out.extend_from_slice(&std::mem::take(&mut self.body));
        Ok(())
    }
}

/// A refusal its codec could not write: the adapter writes it, as an
/// internal fault; the request ends with `fault`.
fn unwritable(ended: Option<Completion>, fault: Fault) -> Handed {
    let handed = Fault::internal(fault.message().to_owned()).with_source(fault.clone());
    if let Some(ended) = ended {
        ended.finish(Err(fault));
    }
    Err(handed)
}
