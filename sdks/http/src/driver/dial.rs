//! The provider edge: a call written, head and body, handed to its
//! transport, and the answer classified at its head (rule 2), then read
//! into outputs.

use bytes::{Bytes, BytesMut};
use futures::channel::oneshot;
use futures::{FutureExt, StreamExt, stream};
use http::StatusCode;
use http::header::UPGRADE;
use respan_gateway_core::{Events, Fault, write};

use super::reader::{Failure, Reader};
use super::writer::Writer;
use super::{Connection, RequestMetadata, ResponseMetadata, Transport, TransportState};
use crate::{Codec, Encoder, Exchange, RequestHead, ResponseHead};

/// Build the two directions of a provider exchange. Writing the input
/// starts the transport operation; its response opens the incoming events.
pub fn dial<C: Codec + Clone>(
    codec: &C,
    mut io: Transport,
) -> Result<Connection<C::Output, C::Input>, Fault> {
    if !matches!(io.state.as_ref(), TransportState::Outbound(_)) {
        return Err(Fault::config(
            "an accepted transport cannot call a provider",
        ));
    }
    let extensions = std::mem::take(&mut io.extensions);
    let transport = io;
    let (answer, answered) = oneshot::channel();
    let incoming = async move { answered.await.map_err(|_| Fault::abandoned())? }.boxed();
    let codec = codec.clone();
    let outgoing = write(move |events: Events<C::Input>| {
        if let Some(fault) = events.refusal() {
            return Err(fault.clone());
        }
        let came = events.metadata::<RequestMetadata>(codec.name());
        let relayed = came.is_some();
        let streaming = !events.is_single();
        let input = if events.is_lazy() && came.is_some() {
            None
        } else {
            events.value().transpose()?
        };
        let (head, first, encoder, done) = first_frame(&codec, input, came, streaming)?;
        let streams = streaming && !done;
        let sent = head.clone();
        let first_frame = stream::iter((!first.is_empty()).then(|| Ok(first.clone())));
        let failed = Failure::default();
        let body = if streams {
            let rest = Writer::call(encoder, events.boxed(), failed.clone()).frames();
            first_frame.chain(rest).boxed()
        } else {
            first_frame.boxed()
        };
        let mut call = Exchange::new(head, body);
        call.extensions = extensions;
        call.relayed = relayed;
        call.streams = streams;
        Ok(async move {
            let response = async move {
                let Exchange {
                    head,
                    body,
                    write_completion,
                    ..
                } = transport.send(call).await?;
                let decoder = codec.response_decoder(&head, &sent, &first);
                let mut read = Reader::outputs(body, decoder, failed);
                if !is_open(&head, &sent) {
                    read.decide().await;
                }
                let response = match read.refused().await {
                    Some(fault) => Events::refused(fault),
                    None => Events::stream(read),
                };
                Ok::<_, Fault>((
                    response.with_metadata(codec.name(), ResponseMetadata(head)),
                    write_completion,
                ))
            }
            .await;
            match response {
                Ok((response, completion)) => {
                    let _ = answer.send(Ok(response));
                    match completion {
                        Some(completion) => completion.await,
                        None => Ok(()),
                    }
                }
                Err(fault) => {
                    let _ = answer.send(Err(fault.clone()));
                    Err(fault)
                }
            }
        })
    });
    Ok((incoming, Box::pin(outgoing)))
}

/// Whether `answer` to the call `sent` is open at its head: a 2xx, or a
/// 101 to a call that asked to switch. Any other is read until its codec
/// decides.
fn is_open(answer: &ResponseHead, sent: &RequestHead) -> bool {
    match answer.status {
        StatusCode::SWITCHING_PROTOCOLS => sent.headers.contains_key(UPGRADE),
        status => status.is_success(),
    }
}

/// The call's head as its codec writes it, from the client's head if it
/// relays the client's message; its first frame: the encoder's opening
/// bytes, then the client's bytes if relayed unread, else the input written
/// from its value, then the end unless it `streams`; the encoder; and
/// whether the body is done as it opens, when the first frame is the
/// opening bytes alone.
#[allow(clippy::type_complexity)]
fn first_frame<C: Codec>(
    codec: &C,
    input: Option<&C::Input>,
    came: Option<&RequestMetadata>,
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
    let mut body = match (input, came.and_then(|c| c.body.as_ref())) {
        (None, Some(raw)) => raw.clone(),
        _ => Bytes::new(),
    };
    let first = input;
    let mut encoder = codec.request_encoder(client, &mut body, streams);
    let head = encoder.head(first);
    let mut opening = BytesMut::new();
    if encoder.open(&mut opening)? {
        return Ok((head, opening.freeze(), encoder, true));
    }
    let mut more = BytesMut::new();
    if let Some(first) = first {
        encoder.encode(first, &mut more)?;
    }
    if !streams {
        encoder.end(&Ok(()), &mut more)?;
    }
    if !opening.is_empty() || !more.is_empty() {
        body = [&opening[..], &body[..], &more[..]].concat().into();
    }
    Ok((head, body, encoder, false))
}
