//! The old provider edge: a call written, head and body, handed to its
//! transport, and the response read back into outputs.

use std::collections::VecDeque;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll, ready};

use bytes::{Bytes, BytesMut};
use futures::stream::{self, Stream};
use futures::{FutureExt, StreamExt};

use super::{Body, Came, Decoder, Dialer, Encoder, Exchange, Format};
use crate::fault::PANICKED;
use crate::{Dialed, Events, Extensions, Fault, Origin};

/// Whose content an input is: whose fault it is if it cannot be written.
#[derive(Clone, Copy)]
enum Whose {
    /// The client's as decoded (`true`), whatever filters ran over it, or a
    /// plugin's (`false`).
    Of(bool),
    /// The format's own, such as an end.
    Format,
}

impl Whose {
    /// `fault`, of content that cannot be written, as whose it is.
    fn fault(self, fault: Fault) -> Fault {
        match self {
            Whose::Of(clients) => fault.of_input(clients),
            Whose::Format => fault,
        }
    }
}

/// Where a streamed call's request encoder panicked, if it did.
type Failure = Arc<OnceLock<Fault>>;

/// The provider edge of `format` over `transport`: the call written from
/// the client's head on a relay, its first input, then its later ones if
/// any follow; the answer classified at its head.
pub(super) fn dial<F: Format>(
    format: &Arc<F>,
    transport: &Dialer<F>,
    extensions: Extensions,
    relay: Option<&Origin>,
    opening: Option<(&F::Input, bool)>,
    later: Option<Events<(F::Input, bool)>>,
) -> Result<Dialed<F::Output>, Fault> {
    let came = relay.and_then(|o| o.downcast_ref::<Came>());
    let client = came.and_then(|c| c.head.downcast_ref::<F::Ask>());
    let (head, first, encoder, done) =
        first_frame(&**format, opening, came, client, later.is_some())?;
    // A body done as it opens has no later inputs.
    let later = later.filter(|_| !done);
    let streams = later.is_some();
    // The decoder reads the provider's head as it came, for the call as sent.
    let sent = head.clone();
    // An empty first frame is none, as an HTTP body of no bytes has none.
    let first_frame = stream::iter((!first.is_empty()).then(|| Ok(first.clone())));
    let failed = Failure::default();
    let body = match later {
        None => first_frame.boxed(),
        Some(later) => {
            let rest = later_frames::<F>(later, encoder, failed.clone());
            first_frame.chain(rest).boxed()
        }
    };
    let mut call = Exchange::new(head, body);
    call.extensions = extensions;
    call.relayed = relay.is_some();
    call.streams = streams;
    let (format, transport) = (format.clone(), transport.clone());
    Ok(async move {
        let resp = transport.send(call).await?;
        let Exchange { head, body, .. } = resp;
        let answer = Arc::new(head);
        let decoder = format.response_decoder(&answer, &sent, &first);
        let mut read = Decoded::<F> {
            body,
            decoder,
            failed,
            ready: VecDeque::new(),
            ended: false,
            broke: false,
            yielded: false,
        };
        let origin = Some(answer as Origin);
        Ok(match read.refused() {
            Some(fault) => (origin, Err(fault)),
            None => (origin, Ok(read.events())),
        })
    }
    .boxed())
}

/// The call's head as its format writes it, from the `client`'s head if it
/// relays the client's message; its first frame: the encoder's opening bytes,
/// then the client's bytes if relayed unread, else the input written from
/// its value, then the end unless it `streams`; the encoder; and whether the
/// body is done as it opens, when the first frame is the opening bytes
/// alone. Content that cannot be written is whose it is (`Whose`).
#[allow(clippy::type_complexity)]
fn first_frame<F: Format>(
    format: &F,
    opening: Option<(&F::Input, bool)>,
    came: Option<&Came>,
    client: Option<&F::Ask>,
    streams: bool,
) -> Result<(F::Ask, Bytes, Box<dyn Encoder<F::Input, F::Ask>>, bool), Fault> {
    // A plugin's or filtered input as its value; else as it came, its bytes
    // unread if relayed.
    let (mut body, first, whose) = match (opening, came.and_then(|c| c.body.as_ref())) {
        (Some((input, clients)), _) => (Bytes::new(), Some(input), Whose::Of(clients)),
        (None, Some(raw)) => (raw.clone(), None, Whose::Of(true)),
        (None, None) => (Bytes::new(), None, Whose::Format),
    };
    let mut encoder = format.request_encoder(client, &mut body, streams);
    let head = encoder.head(first);
    // What cannot be opened is the format's to say, whoever's input it is.
    let mut opening = BytesMut::new();
    if encoder.open(&mut opening)? {
        return Ok((head, opening.freeze(), encoder, true));
    }
    let mut more = BytesMut::new();
    let mut write = || {
        if let Some(first) = first {
            encoder.encode(first, &mut more)?;
        }
        match streams {
            true => Ok(()),
            false => encoder.end(&Ok(()), &mut more),
        }
    };
    write().map_err(|fault| whose.fault(fault))?;
    if !opening.is_empty() || !more.is_empty() {
        body = [&opening[..], &body[..], &more[..]].concat().into();
    }
    Ok((head, body, encoder, false))
}

/// A streamed call's frames after its first, as its body: each of `later`
/// written by `encoder`, then, if it has bytes, the end. A frame that cannot
/// be written is whose it is (`Whose`).
///
/// The body ends with the first failure, its own or the inputs' (the
/// client's body breaking, or the inputs cut off: `abandoned`), as an
/// `Err`, and only after the encoder was told that failure as its end, so
/// no clean end follows one. A panic in the encoder ends it `internal`, in
/// the body and, as the transport may read the body where nothing unwinds
/// to the call, in `failed`.
fn later_frames<F: Format>(
    later: Events<(F::Input, bool)>,
    encoder: Box<dyn Encoder<F::Input, F::Ask>>,
    failed: Failure,
) -> impl Stream<Item = Result<Bytes, Fault>> {
    let writing = Writing::<F> {
        later,
        encoder,
        failed,
        done: false,
        abort: None,
    };
    stream::unfold(
        writing,
        |mut w| async move { w.next().await.map(|b| (b, w)) },
    )
}

struct Writing<F: Format> {
    later: Events<(F::Input, bool)>,
    encoder: Box<dyn Encoder<F::Input, F::Ask>>,
    failed: Failure,
    /// Whether the inputs ended, or the body failed.
    done: bool,
    /// The failure to end the body with, once its last bytes are out.
    abort: Option<Fault>,
}

impl<F: Format> Writing<F> {
    async fn next(&mut self) -> Option<Result<Bytes, Fault>> {
        loop {
            if let Some(fault) = self.abort.take() {
                return Some(Err(fault));
            }
            if self.done {
                return None;
            }
            let input = self.later.next().await;
            let mut out = BytesMut::new();
            let encoder = &mut self.encoder;
            let step = std::panic::catch_unwind(AssertUnwindSafe(|| match &input {
                Some(Ok((input, clients))) => {
                    let written = encoder.encode(input, &mut out);
                    written.map_err(|f| f.of_input(*clients)).map(|()| true)
                }
                None => encoder.end(&Ok(()), &mut out).map(|()| false),
                Some(Err(fault)) => Err(fault.clone()),
            }));
            match step {
                Err(_) => return Some(Err(self.panicked())),
                Ok(Ok(more)) => {
                    self.done = !more;
                    // A frame is one, an empty one too; the end, if it has bytes.
                    if more || !out.is_empty() {
                        return Some(Ok(out.freeze()));
                    }
                }
                Ok(Err(fault)) => {
                    self.done = true;
                    // Its partial output is dropped; a failed end was told.
                    out.clear();
                    if input.is_some() {
                        let ended =
                            AssertUnwindSafe(|| self.encoder.end(&Err(fault.clone()), &mut out));
                        match std::panic::catch_unwind(ended) {
                            Ok(Ok(())) => {}
                            Ok(Err(_)) => out.clear(),
                            Err(_) => {
                                self.panicked();
                                out.clear();
                            }
                        }
                    }
                    self.abort = Some(fault);
                    if !out.is_empty() {
                        return Some(Ok(out.freeze()));
                    }
                }
            }
        }
    }

    /// The encoder panicked: the body and the call end `internal`.
    fn panicked(&mut self) -> Fault {
        self.done = true;
        let fault = Fault::internal(PANICKED);
        let _ = self.failed.set(fault.clone());
        fault
    }
}

/// A response's body being decoded into outputs.
struct Decoded<F: Format> {
    body: Body,
    decoder: Box<dyn Decoder<F::Output>>,
    /// Set if the call's request encoder panicked, which fails the response.
    failed: Failure,
    ready: VecDeque<Result<F::Output, Fault>>,
    ended: bool,
    /// Whether its failure is the body's, broken off, not the decoder's.
    broke: bool,
    /// Whether it said `Pending` before reading the next frame.
    yielded: bool,
}

impl<F: Format> Decoded<F> {
    /// The provider's refusal at its head (rule 3), as far as its answer
    /// is ready now, waiting for nothing: its decoder's failure before any
    /// output, the body unbroken, as a provider's error answer is.
    fn refused(&mut self) -> Option<Fault> {
        while self.ready.is_empty() && !self.ended {
            let Some(frame) = self.body.next().now_or_never() else {
                break;
            };
            self.read(frame);
        }
        match self.ready.front() {
            Some(Err(_)) if !self.broke => self.ready.pop_front().and_then(Result::err),
            _ => None,
        }
    }

    fn events(self) -> Events<F::Output> {
        self.boxed()
    }

    /// Decode `frame`, or the body's end (`None`), into `ready`: what it
    /// decoded, then the end if it came.
    fn read(&mut self, mut frame: Option<Result<Bytes, Fault>>) {
        // A request encoder that panicked, unseen by the transport, fails it.
        if let Some(fault) = self.failed.get() {
            frame = Some(Err(fault.clone()));
        }
        let mut outputs = Vec::new();
        let end = match frame {
            Some(Ok(bytes)) => self.decoder.decode(bytes, &mut outputs).err().map(Err),
            Some(Err(fault)) => {
                self.broke = true;
                Some(Err(fault))
            }
            None => Some(self.decoder.end(&mut outputs)),
        };
        self.ready.extend(outputs.into_iter().map(Ok));
        if let Some(end) = end {
            self.ended = true;
            if let Err(fault) = end {
                self.ready.push_back(Err(fault));
            }
        }
    }
}

/// Never pinned in place: its fields are moved, not projected.
impl<F: Format> Unpin for Decoded<F> {}

/// A frame's outputs, one by one, then the next frame's: it says `Pending`
/// before reading on, so what the gateway finds ready at once, and takes as
/// one batch, is one frame's.
impl<F: Format> Stream for Decoded<F> {
    type Item = Result<F::Output, Fault>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        loop {
            if let Some(output) = this.ready.pop_front() {
                return Poll::Ready(Some(output));
            }
            if this.ended {
                return Poll::Ready(None);
            }
            if !this.yielded {
                this.yielded = true;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            let frame = ready!(this.body.poll_next_unpin(cx));
            this.yielded = false;
            this.read(frame);
        }
    }
}
