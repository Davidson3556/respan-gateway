//! The old client edge: a request's body read into its inputs, and the
//! response written back, head and body.

use std::any::Any;
use std::collections::VecDeque;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::{Bytes, BytesMut};
use futures::future::BoxFuture;
use futures::stream::{self, Stream};
use futures::{FutureExt, StreamExt};

use super::{Body, Came, Decoder, Encoder, Exchange, Format, Handed, Wire};
use crate::fault::PANICKED;
use crate::{Ended, Events, Fault, Half, Origin, Reply};

/// The client edge of `format` for `wire`: a whole request once its body is
/// read, its input in its origin; a streaming one at once, its data frames
/// the inputs. The writer goes to the wire's slot as the request is in.
pub(super) fn accept<F: Format>(
    format: Arc<F>,
    wire: Wire,
    reply: BoxFuture<'static, Reply<F::Output>>,
) -> BoxFuture<'static, Half<F::Input>> {
    async move {
        let Wire {
            head,
            body,
            streams,
            slot,
        } = wire;
        let head: Arc<dyn Any + Send + Sync> = Arc::from(head);
        if streams {
            let writer = written(format.clone(), reply, head.clone(), Bytes::new(), true);
            let _ = slot.send(writer.boxed());
            let came = Came {
                head: head.clone(),
                body: None,
            };
            let inputs = Flowing::new(frames(format, head, body));
            return (Some(Arc::new(came) as Origin), Ok(inputs.boxed()));
        }
        // One that fails while read is refused.
        let (raw, broken) = match read_whole(body).await {
            Ok(bytes) => (bytes, None),
            Err(fault) => (Bytes::new(), Some(fault)),
        };
        let writer = written(format, reply, head.clone(), raw.clone(), false);
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

/// The one input of a whole request, decoded from its origin; `None` for a
/// streaming one's.
pub(super) fn opening<F: Format>(format: &F, origin: &Origin) -> Option<Result<F::Input, Fault>> {
    let came = origin.downcast_ref::<Came>()?;
    let raw = came.body.as_ref()?;
    Some(decode(format, &*came.head, raw))
}

/// The one input of a whole request's body.
fn decode<F: Format>(format: &F, head: &dyn Any, raw: &Bytes) -> Result<F::Input, Fault> {
    let Some(head) = head.downcast_ref() else {
        return Err(Fault::internal("a head of another wire than its format's"));
    };
    let mut decoder = format.request_decoder(head);
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
async fn read_whole(mut body: Body) -> Result<Bytes, Fault> {
    let mut all = BytesMut::new();
    while let Some(bytes) = body.next().await {
        all.extend_from_slice(&bytes?);
    }
    Ok(all.freeze())
}

/// A streaming request's inputs, which are never ended at once: its head
/// goes out at once and its calls stream, even if no input comes; nor are
/// they read ahead, as its first poll says nothing.
struct Flowing<S> {
    started: bool,
    inner: S,
}

impl<S> Flowing<S> {
    fn new(inner: S) -> Self {
        Self {
            started: false,
            inner,
        }
    }
}

impl<S: Stream + Unpin> Stream for Flowing<S> {
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<S::Item>> {
        if !self.started {
            self.started = true;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        self.inner.poll_next_unpin(cx)
    }
}

/// A streaming request's data frames as inputs, each decoded as it passes.
fn frames<F: Format>(
    format: Arc<F>,
    head: Arc<dyn Any + Send + Sync>,
    body: Body,
) -> Events<F::Input> {
    let reading = Reading {
        format,
        head,
        body,
        decoder: None,
        ready: VecDeque::new(),
        ended: false,
    };
    stream::unfold(
        reading,
        |mut r| async move { r.next().await.map(|i| (i, r)) },
    )
    .boxed()
}

/// A body being read into inputs.
struct Reading<F: Format> {
    format: Arc<F>,
    head: Arc<dyn Any + Send + Sync>,
    body: Body,
    /// Built at the first frame.
    decoder: Option<Box<dyn Decoder<F::Input>>>,
    ready: VecDeque<Result<F::Input, Fault>>,
    ended: bool,
}

impl<F: Format> Reading<F> {
    async fn next(&mut self) -> Option<Result<F::Input, Fault>> {
        loop {
            if let Some(input) = self.ready.pop_front() {
                return Some(input);
            }
            if self.ended {
                return None;
            }
            // Every data frame is a message, even an empty one.
            let bytes = self.body.next().await.transpose();
            // A failure ends the inputs.
            if let Err(fault) = self.frames(bytes) {
                self.ended = true;
                self.ready.push_back(Err(fault));
            }
        }
    }

    /// The inputs of `bytes`, or of the body's end (`None`), made ready.
    /// What was decoded before an `Err` is ready before it, as a
    /// response's is.
    fn frames(&mut self, bytes: Result<Option<Bytes>, Fault>) -> Result<(), Fault> {
        let bytes = bytes?;
        let (format, head) = (&self.format, &self.head);
        let decoder = self
            .decoder
            .get_or_insert_with(|| match head.downcast_ref() {
                Some(head) => format.request_decoder(head),
                None => Box::new(OtherWire),
            });
        let mut inputs = Vec::new();
        let end = match bytes {
            Some(bytes) => decoder.decode(bytes, &mut inputs).err().map(Err),
            None => Some(decoder.end(&mut inputs)),
        };
        self.ready.extend(inputs.into_iter().map(Ok));
        match end {
            Some(end) => {
                self.ended = true;
                end
            }
            None => Ok(()),
        }
    }
}

/// A decoder for a head of another wire than its format's: it fails.
struct OtherWire;

impl<T> Decoder<T> for OtherWire {
    fn decode(&mut self, _: Bytes, _: &mut Vec<T>) -> Result<(), Fault> {
        Err(Fault::internal("a head of another wire than its format's"))
    }

    fn end(&mut self, _: &mut Vec<T>) -> Result<(), Fault> {
        Err(Fault::internal("a head of another wire than its format's"))
    }
}

/// What the adapter gets: `reply` written to the client whose request had
/// head `ask` and body `body`, streaming if `streaming`.
async fn written<F: Format>(
    format: Arc<F>,
    reply: BoxFuture<'static, Reply<F::Output>>,
    ask: Arc<dyn Any + Send + Sync>,
    body: Bytes,
    streaming: bool,
) -> Handed {
    let (_extensions, (origin, outputs), ended) = reply.await;
    let to = Client {
        format,
        ask,
        body,
        streaming,
    };
    match outputs {
        Err(fault) => to.refuse(fault, ended, None).await,
        Ok(outputs) => to.deliver(origin, outputs, ended).await,
    }
}

/// The client a response is written to, in its format.
struct Client<F> {
    format: Arc<F>,
    ask: Arc<dyn Any + Send + Sync>,
    body: Bytes,
    streaming: bool,
}

impl<F: Format> Client<F> {
    /// A refusal: a response that is `fault` alone, which the client's
    /// format writes. `hold` is an answer whose call it ends.
    async fn refuse(self, fault: Fault, ended: Ended, hold: Option<Events<F::Output>>) -> Handed {
        let build = || {
            let ask = self.ask.downcast_ref().ok_or_else(another_wire)?;
            let mut encoder = self
                .format
                .response_encoder(None, ask, &self.body, Some(&fault));
            let head = encoder.head(None);
            Ok((encoder, head))
        };
        // A format that panics building its encoder cannot write a refusal.
        let (encoder, head) = match std::panic::catch_unwind(AssertUnwindSafe(build)) {
            Ok(Ok(built)) => built,
            Ok(Err(fault)) => return unwritable(ended, fault),
            Err(_) => return unwritable(ended, Fault::internal(PANICKED)),
        };
        let mut pipe = Pipe::<F>::new(encoder, ended, hold, self.streaming);
        pipe.refusal = Some(fault);
        if let Err(fault) = pipe.first(false).await {
            return unwritable(pipe.ended.take().expect("ended once"), fault);
        }
        // With no bytes, it ends as its head goes out.
        if pipe.out.is_empty() {
            pipe.tell_end();
        }
        Ok(pipe.body(head))
    }

    /// Send `outputs`, from the provider whose head is `origin` if relayed,
    /// to the client. Its head goes out with its body's first byte, so a
    /// response that fails before any is refused instead, or at once for a
    /// streaming request.
    async fn deliver(
        self,
        origin: Option<Origin>,
        outputs: Events<F::Output>,
        ended: Ended,
    ) -> Handed {
        // A head goes only to a format of the name it came in.
        let relayed = origin.is_some();
        let answer = origin.as_ref().and_then(|o| o.downcast_ref::<F::Answer>());
        let build = || {
            let ask = self.ask.downcast_ref().ok_or_else(another_wire)?;
            let mut encoder = self.format.response_encoder(answer, ask, &self.body, None);
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
            Ok(Err(fault)) => return self.refuse(fault, ended, Some(outputs)).await,
            // A format that panics building its encoder cannot write a refusal.
            Err(_) => {
                drop(outputs);
                return unwritable(ended, Fault::internal(PANICKED));
            }
        };
        let streaming = self.streaming;
        let mut pipe = Pipe::<F>::new(encoder, ended, Some(outputs), streaming);
        pipe.out.push(opening);
        // A streaming request's client waits for the head before it sends.
        if !(streaming || done)
            && let Err(fault) = pipe.first(true).await
        {
            // A provider's words go only where its head would.
            let fault = match relayed {
                true => fault,
                false => fault.unrelayed(),
            };
            let ended = pipe.ended.take().expect("ended once");
            return self.refuse(fault, ended, pipe.outputs.take()).await;
        }
        if done {
            // Its outputs go unread, their call ending as its end, if ready,
            // says.
            drop(pipe.outputs.take());
            if let Some(ended) = pipe.ended.take() {
                ended(Ok(()));
            }
            return Ok(Exchange::new(Box::new(head), stream::empty().boxed()));
        }
        // With no bytes, it ends as its head goes out.
        if pipe.out.is_empty() {
            pipe.tell_end();
        }
        Ok(pipe.body(head))
    }
}

fn another_wire() -> Fault {
    Fault::internal("a request of another wire")
}

/// Turns the response sent to the client into its body.
struct Pipe<F: Format> {
    /// Told the end once its last bytes are handed out. Dropped first, so a
    /// client that leaves ends the request before its answer's call.
    ended: Option<Ended>,
    /// The answer's outputs; held, once ended or cut short, until `ended`
    /// is told, so their call ends as the request does.
    outputs: Option<Events<F::Output>>,
    /// A refusal's one failure, its fault alone.
    refusal: Option<Fault>,
    encoder: Box<dyn Encoder<F::Output, F::Answer>>,
    /// Bytes written and not yet handed out.
    out: Out,
    /// The end once written; told as its last bytes are handed out.
    end: Option<Result<(), Fault>>,
    /// A failure that could not be written, to abort the body with.
    abort: Option<Fault>,
}

impl<F: Format> Pipe<F> {
    fn new(
        encoder: Box<dyn Encoder<F::Output, F::Answer>>,
        ended: Ended,
        outputs: Option<Events<F::Output>>,
        streaming: bool,
    ) -> Self {
        Self {
            ended: Some(ended),
            outputs,
            refusal: None,
            encoder,
            out: Out {
                streaming,
                ..Out::default()
            },
            end: None,
            abort: None,
        }
    }

    /// The body, from here, under `head`.
    fn body(self, head: F::Answer) -> Exchange<super::Head> {
        let body = stream::unfold(self, |mut p| async move { p.next().await.map(|b| (b, p)) });
        Exchange::new(Box::new(head), body.boxed())
    }

    /// Write until the body's first bytes. `Err`, to refuse it, if it failed
    /// before any and is `refusable`, would be aborted, or panicked.
    async fn first(&mut self, refusable: bool) -> Result<(), Fault> {
        while self.out.is_empty() && self.end.is_none() {
            let Ok(bytes_before_end) = self.step().await else {
                return Err(Fault::internal(PANICKED));
            };
            // A failure before any bytes is refused.
            if refusable
                && !bytes_before_end
                && let Some(Err(f)) = &self.end
            {
                return Err(f.clone());
            }
            if self.out.is_empty()
                && let Some(f) = &self.abort
            {
                return Err(f.clone());
            }
        }
        Ok(())
    }

    async fn next(&mut self) -> Option<Result<Bytes, Fault>> {
        loop {
            if let Some(bytes) = self.out.pop() {
                if self.out.is_empty() {
                    self.tell_end();
                }
                return Some(Ok(bytes));
            }
            self.tell_end();
            if let Some(f) = self.abort.take() {
                return Some(Err(f));
            }
            // Its end told, the body is done.
            self.ended.as_ref()?;
            let _ = self.step().await;
        }
    }

    /// Tell the end, if it is written.
    fn tell_end(&mut self) {
        if let Some(end) = self.end.take()
            && let Some(ended) = self.ended.take()
        {
            ended(end);
        }
    }

    /// Write the next output, returning whether bytes came before an end.
    /// `Err` if the encoder panicked: the response ends internal and is
    /// aborted.
    async fn step(&mut self) -> Result<bool, ()> {
        match AssertUnwindSafe(self.write()).catch_unwind().await {
            Ok(bytes_before_end) => Ok(bytes_before_end),
            Err(_) => {
                let fault = Fault::internal(PANICKED);
                self.end = Some(Err(fault.clone()));
                self.abort = Some(fault);
                Err(())
            }
        }
    }

    /// Write the next output, or the end. Returns whether bytes came before
    /// the end.
    async fn write(&mut self) -> bool {
        let next = match (self.refusal.take(), &mut self.outputs) {
            (Some(fault), _) => Some(Err(fault)),
            (None, Some(outputs)) => outputs.next().await,
            (None, None) => Some(Err(Fault::abandoned())),
        };
        let end = match next {
            Some(Ok(output)) => self.encode(&output).err().map(Err),
            Some(Err(fault)) => Some(Err(fault)),
            None => Some(Ok(())),
        };
        let bytes_before_end = !self.out.is_empty();
        if let Some(end) = end {
            self.write_end(end);
        }
        bytes_before_end
    }

    /// Write `output`, its own frame to a streaming request.
    fn encode(&mut self, output: &F::Output) -> Result<(), Fault> {
        let mut one = BytesMut::new();
        self.encoder.encode(output, &mut one)?;
        self.out.push(one);
        Ok(())
    }

    /// Write `end`, else its encoder's fault, else abort the body.
    fn write_end(&mut self, mut end: Result<(), Fault>) {
        let mut one = BytesMut::new();
        if let Err(f) = self.encoder.end(&end, &mut one) {
            end = Err(f.clone());
            one.clear();
            if self.encoder.end(&end, &mut one).is_err() {
                one.clear();
                self.abort = Some(f);
            }
        }
        self.out.push(one);
        self.end = Some(end);
    }
}

/// Bytes not yet handed out, as data frames: one per output to a streaming
/// request, else one per batch.
#[derive(Default)]
struct Out {
    streaming: bool,
    frames: VecDeque<Bytes>,
    batch: BytesMut,
}

impl Out {
    fn push(&mut self, bytes: BytesMut) {
        match self.streaming {
            _ if bytes.is_empty() => {}
            true => self.frames.push_back(bytes.freeze()),
            false => self.batch.unsplit(bytes),
        }
    }

    fn is_empty(&self) -> bool {
        self.frames.is_empty() && self.batch.is_empty()
    }

    /// The next frame to hand out.
    fn pop(&mut self) -> Option<Bytes> {
        let batch = (!self.batch.is_empty()).then(|| self.batch.split().freeze());
        self.frames.pop_front().or(batch)
    }
}

/// A refusal its format could not write: the adapter writes it, as an
/// internal fault; the request ends with `fault`.
fn unwritable(ended: Ended, fault: Fault) -> Handed {
    let handed = Fault::internal(fault.message().to_owned()).with_source(fault.clone());
    ended(Err(fault));
    Err(handed)
}
