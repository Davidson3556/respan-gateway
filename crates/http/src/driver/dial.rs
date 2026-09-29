//! The provider edge: a call written, head and body, handed to its
//! transport, and the answer classified at its head (rule 2), then read
//! into outputs.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures::{FutureExt, StreamExt, stream};
use http::StatusCode;
use http::header::UPGRADE;
use respan_gateway_core::{Dialed, Events, Extensions, Fault, Origin};

use super::Came;
use super::reader::{Failure, Reader};
use super::writer::Writer;
use crate::{Codec, Dialer, Encoder, Exchange, RequestHead, ResponseHead};

/// Whose content an input is: whose fault it is if it cannot be written.
#[derive(Clone, Copy)]
enum Whose {
    /// The client's as decoded (`true`), whatever filters ran over it, or a
    /// plugin's (`false`).
    Of(bool),
    /// The codec's own, such as an end.
    Codec,
}

impl Whose {
    /// `fault`, of content that cannot be written, as whose it is.
    fn fault(self, fault: Fault) -> Fault {
        match self {
            Whose::Of(clients) => fault.of_input(clients),
            Whose::Codec => fault,
        }
    }
}

/// The provider edge of `codec` through `dialer`, as
/// [`Format::dial`](respan_gateway_core::Format::dial):
///
/// - The call is written from the client's head on a relay (`relay`), its
///   first input from its value (`opening`), else, on a relay, the
///   client's bytes unread; then its later inputs, if any follow, streamed
///   ([`Exchange::streams`]), else whole. [`Exchange::relayed`] is set if
///   and only if the call carries the session's own inputs (it has an
///   origin to relay), as the plugins' filters left them, even edited or
///   replaced; a call with its own input (`Call::with_input`) has none. The
///   transport passes on the client's keys
///   ([`credentials`](crate::credentials)) only then.
/// - The answer is classified at its head (rule 2): a non-2xx answer, but
///   a 101 to a call that asked to switch protocols, is read whole before
///   this resolves, so its codec's failure before any output resolves
///   `Ok((origin, Err(fault)))`. It is read up to `ERROR_BODY_CAP` bytes
///   (1 MiB): past it the body ends truncated after the frames read, so a
///   body up to it is relayed byte for byte and a huge one cannot exhaust
///   memory. For another, what is ready of it when the
///   transport returns is read, waiting for nothing.
pub fn dial<C: Codec + Clone>(
    codec: &C,
    dialer: &Dialer,
    extensions: Extensions,
    relay: Option<&Origin>,
    opening: Option<(&C::Input, bool)>,
    later: Option<Events<(C::Input, bool)>>,
) -> Result<Dialed<C::Output>, Fault> {
    let came = relay.and_then(|o| o.downcast_ref::<Came>());
    let (head, first, encoder, done) = first_frame(codec, opening, came, later.is_some())?;
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
            let rest = Writer::call(encoder, later, failed.clone()).frames();
            first_frame.chain(rest).boxed()
        }
    };
    let mut call = Exchange::new(head, body);
    call.extensions = extensions;
    // Not `opening`'s source: a filter's edit of the session's own input
    // still relays (see `Exchange::relayed`); only a call of its own has no
    // origin.
    call.relayed = came.is_some();
    call.streams = streams;
    let (codec, transport) = (codec.clone(), dialer.0.clone());
    Ok(async move {
        let Exchange { head, body, .. } = transport.send(call).await?;
        let answer = Arc::new(head);
        let decoder = codec.response_decoder(&answer, &sent, &first);
        let mut read = Reader::outputs(body, decoder, failed);
        if is_whole(&answer, &sent) {
            read.buffer().await;
        }
        let origin = Some(answer as Origin);
        Ok(match read.refused() {
            Some(fault) => (origin, Err(fault)),
            None => (origin, Ok(read.boxed())),
        })
    }
    .boxed())
}

/// Whether `answer` to the call `sent` is read whole before it is
/// classified: a non-2xx, but a 101 to a call that asked to switch.
fn is_whole(answer: &ResponseHead, sent: &RequestHead) -> bool {
    let asked = sent.headers.contains_key(UPGRADE);
    let open = match answer.status {
        StatusCode::SWITCHING_PROTOCOLS => asked,
        status => status.is_success(),
    };
    !open
}

/// The call's head as its codec writes it, from the client's head if it
/// relays the client's message; its first frame: the encoder's opening
/// bytes, then the client's bytes if relayed unread, else the input written
/// from its value, then the end unless it `streams`; the encoder; and
/// whether the body is done as it opens, when the first frame is the
/// opening bytes alone. Content that cannot be written is whose it is
/// (`Whose`).
#[allow(clippy::type_complexity)]
fn first_frame<C: Codec>(
    codec: &C,
    opening: Option<(&C::Input, bool)>,
    came: Option<&Came>,
    streams: bool,
) -> Result<
    (
        RequestHead,
        Bytes,
        Box<dyn Encoder<C::Input, RequestHead>>,
        bool,
    ),
    Fault,
> {
    let client = came.map(|c| &*c.head);
    // A plugin's or filtered input as its value; else as it came, its bytes
    // unread if relayed.
    let (mut body, first, whose) = match (opening, came.and_then(|c| c.body.as_ref())) {
        (Some((input, clients)), _) => (Bytes::new(), Some(input), Whose::Of(clients)),
        (None, Some(raw)) => (raw.clone(), None, Whose::Of(true)),
        (None, None) => (Bytes::new(), None, Whose::Codec),
    };
    let mut encoder = codec.request_encoder(client, &mut body, streams);
    let head = encoder.head(first);
    // What cannot be opened is the codec's to say, whoever's input it is.
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
