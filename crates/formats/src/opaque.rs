//! Traffic of no kind, relayed unread. Such a format is a kind of one,
//! `Sent<F>` in and `Got<F>` out, so it relays only to itself, byte for
//! byte: one value per body or data frame.
//!
//! ```ignore
//! impl Codec for Raw {
//!     type Input = Sent<Raw>;
//!     type Output = Got<Raw>;
//!     // name, accepts, and the four factories from this module
//! }
//! impl_format!(Raw);
//! ```

use std::marker::PhantomData;

use respan_gateway_core::Fault;
use respan_gateway_http::bytes::{Bytes, BytesMut};
use respan_gateway_http::http::{self, StatusCode};
use respan_gateway_http::{Codec, RequestHead, ResponseHead, bodiless, impl_format};

use respan_gateway_http::{Decoder, Encoder};

/// A request's message in format `F`, as the client sent it.
pub struct Sent<F>(pub Bytes, PhantomData<fn() -> F>);

/// A response's message in format `F`, as the provider sent it.
pub struct Got<F>(pub Bytes, PhantomData<fn() -> F>);

macro_rules! bytes_type {
    ($name:ident) => {
        impl<F> $name<F> {
            /// The message `bytes`.
            pub fn new(bytes: impl Into<Bytes>) -> Self {
                Self(bytes.into(), PhantomData)
            }
        }

        impl<F> Clone for $name<F> {
            fn clone(&self) -> Self {
                Self(self.0.clone(), PhantomData)
            }
        }

        /// Equal when the bytes are.
        impl<F> PartialEq for $name<F> {
            fn eq(&self, other: &Self) -> bool {
                self.0 == other.0
            }
        }

        impl<F> std::fmt::Debug for $name<F> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_tuple(stringify!($name)).field(&self.0).finish()
            }
        }

        impl<F: 'static> Encoder<$name<F>, ()> for Writes {
            fn head(&mut self, _first: Option<&$name<F>>) {}

            fn encode(&mut self, value: &$name<F>, out: &mut BytesMut) -> Result<(), Fault> {
                out.extend_from_slice(&value.0);
                Ok(())
            }

            fn end(&mut self, end: &Result<(), Fault>, _out: &mut BytesMut) -> Result<(), Fault> {
                end.clone()
            }
        }
    };
}

/// Writes each message as it came.
struct Writes;

bytes_type!(Sent);
bytes_type!(Got);

/// The request decoder: one message per body or data frame.
pub fn request_decoder<F: 'static>() -> Box<dyn Decoder<Sent<F>>> {
    crate::message_decoder(|bytes| Ok(Sent::new(bytes)))
}

/// The request encoder: each message as it came, under the client's head
/// ([`body_changed`](crate::body_changed) unless relayed unread).
pub fn request_encoder<F: 'static>(
    client: Option<&RequestHead>,
    streams: bool,
) -> Box<dyn Encoder<Sent<F>, RequestHead>> {
    let mut head = crate::call_head(client);
    if client.is_some() {
        // The path the client's request was matched by: its segments, as spelled.
        let path = crate::forwarded_path(head.uri.path());
        crate::set_path(&mut head, &path);
    }
    let head = move |first: Option<&Sent<F>>| {
        if first.is_some() || streams {
            crate::body_changed(&mut head);
        }
        head
    };
    crate::headed(head, Box::new(Writes))
}

/// The response decoder of `format` for a response with `head` to the call
/// with head `call`: a success's or a redirect's (a 304 too) bytes, or a 101's if the call asked to
/// switch protocols, one message per frame; none for a response with no
/// body; any other answer read whole ([`whole::answer`](crate::whole::answer)).
pub fn response_decoder<F: 'static>(
    format: &str,
    head: &ResponseHead,
    call: &RequestHead,
) -> Box<dyn Decoder<Got<F>>> {
    let switched = head.status == StatusCode::SWITCHING_PROTOCOLS && asked_to_switch(call);
    // A redirect or a 304 is an answer to relay as it came, not a refusal.
    let relayed = head.status.is_success() || head.status.is_redirection();
    match relayed || switched {
        _ if relayed && bodiless(&call.method, head.status) => crate::whole::nothing(),
        false => crate::whole::decoder(format, head, |_, _| Ok(())),
        true => crate::message_decoder(|bytes| Ok(Got::new(bytes))),
    }
}

/// Whether a call asked to switch protocols (RFC 9110 7.8).
fn asked_to_switch(call: &RequestHead) -> bool {
    call.headers.contains_key(http::header::UPGRADE)
}

/// The response encoder of a format: messages as they came; nothing for a
/// bodiless response. A refusal is [`Codec::refusal`]'s default, its
/// message as plain text.
///
/// [`Codec::refusal`]: respan_gateway_http::Codec::refusal
pub fn response_encoder<F: 'static>(
    answer: Option<&ResponseHead>,
    ask: &RequestHead,
) -> Box<dyn Encoder<Got<F>, ResponseHead>> {
    let head = crate::response_head(answer);
    let body: Box<dyn Encoder<Got<F>, ()>> = match bodiless(&ask.method, head.status) {
        true => crate::whole::no_body(),
        false => Box::new(Writes),
    };
    crate::with_head(head, body)
}

/// Every request no format before it accepts, relayed unread, byte for
/// byte, to a provider of this format: how a vendor's other endpoints
/// (token counting, files, a newer API) pass through a gateway that knows
/// only its chat one. Serve it last, and send its requests to a provider of
/// this format at the vendor's origin, as the client's path is relayed as it
/// is. Named `passthrough`, so a client's key goes only to a provider of
/// this format.
#[derive(Clone, Copy, Debug, Default)]
pub struct Passthrough;

impl Codec for Passthrough {
    type Input = Sent<Passthrough>;
    type Output = Got<Passthrough>;

    fn name(&self) -> &str {
        "passthrough"
    }

    fn accepts(&self, _head: &RequestHead) -> bool {
        true
    }

    fn request_decoder(&self, _head: &RequestHead) -> Box<dyn Decoder<Sent<Passthrough>>> {
        request_decoder()
    }

    fn request_encoder(
        &self,
        client: Option<&RequestHead>,
        _body: &mut Bytes,
        streams: bool,
    ) -> Box<dyn Encoder<Sent<Passthrough>, RequestHead>> {
        request_encoder(client, streams)
    }

    fn response_decoder(
        &self,
        head: &ResponseHead,
        call: &RequestHead,
        _body: &Bytes,
    ) -> Box<dyn Decoder<Got<Passthrough>>> {
        response_decoder(self.name(), head, call)
    }

    fn response_encoder(
        &self,
        answer: Option<&ResponseHead>,
        ask: &RequestHead,
        _body: &Bytes,
    ) -> Box<dyn Encoder<Got<Passthrough>, ResponseHead>> {
        response_encoder(answer, ask)
    }
}

impl_format!(Passthrough);

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone)]
    struct Raw;

    #[test]
    fn bytes_pass_as_they_came() {
        let head = respan_gateway_http::fresh_response_head();
        let call = respan_gateway_http::fresh_request_head();
        let mut decoder = response_decoder::<Raw>("raw", &head, &call);
        let mut got = Vec::new();
        assert!(decoder.decode(Bytes::from_static(b"a"), &mut got).is_ok());
        assert!(decoder.end(&mut got).is_ok());
        let mut encoder = request_encoder::<Raw>(None, false);
        let mut out = BytesMut::new();
        assert!(encoder.encode(&Sent::new("b"), &mut out).is_ok());
        assert_eq!((&got[0].0[..], &out[..]), (&b"a"[..], &b"b"[..]));
    }

    // A provider's 101 the call did not ask for fails it, now this
    // format's rule, not the kernel's.
    #[test]
    fn a_101_no_one_asked_for_fails_the_call() {
        let (mut head, ()) = http::Response::new(()).into_parts();
        head.status = StatusCode::SWITCHING_PROTOCOLS;
        let call = respan_gateway_http::fresh_request_head();
        let mut decoder = response_decoder::<Raw>("raw", &head, &call);
        let mut got = Vec::new();
        assert!(decoder.decode(Bytes::from_static(b"a"), &mut got).is_ok());
        let fault = decoder.end(&mut got).err();
        assert_eq!(fault.as_ref().map(Fault::code), Some("upstream"));
        let answer = fault.and_then(|f| f.answer::<http::Response<Bytes>>("raw").cloned());
        assert!(answer.is_none(), "never relayed");
    }
}
