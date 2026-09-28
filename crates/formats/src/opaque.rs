//! Traffic of no kind, relayed unread. Such a format is a kind of one,
//! `Sent<F>` in and `Got<F>` out, so it relays only to itself, byte for
//! byte: one value per body or data frame.
//!
//! ```ignore
//! impl Format for Raw {
//!     type Input = Sent<Raw>;
//!     type Output = Got<Raw>;
//!     // name, accepts, and the four factories from this module
//! }
//! ```

use std::marker::PhantomData;

use respan_gateway_core::bytes::{Bytes, BytesMut};
use respan_gateway_core::{Decoder, Encoder, Fault};
use respan_gateway_http::http::{self, StatusCode};
use respan_gateway_http::{Answer, Ask, bodiless};

use crate::default_content_type;

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
    };
}

bytes_type!(Sent);
bytes_type!(Got);

/// The request decoder: one message per body or data frame.
pub fn request_decoder<F: 'static>() -> Box<dyn Decoder<Sent<F>>> {
    crate::message_decoder(|bytes| Ok(Sent::new(bytes)))
}

/// Writes each message as it came.
struct Writes;

impl<F: 'static> Encoder<Sent<F>, ()> for Writes {
    fn head(&mut self, _first: Option<&Sent<F>>) {}

    fn encode(&mut self, sent: &Sent<F>, out: &mut BytesMut) -> Result<(), Fault> {
        out.extend_from_slice(&sent.0);
        Ok(())
    }

    fn end(&mut self, _end: &Result<(), Fault>, _out: &mut BytesMut) -> Result<(), Fault> {
        Ok(())
    }
}

/// The request encoder: each message as it came, the client's head kept
/// as the kernel handed it over; unless the call carries the client's bytes
/// unread, its `Content-Encoding` goes ([`body_changed`](crate::body_changed)).
pub fn request_encoder<F: 'static>(
    client: Option<&Ask>,
    streams: bool,
) -> Box<dyn Encoder<Sent<F>, Ask>> {
    let mut head = crate::call_head(client);
    let head = move |first: Option<&Sent<F>>| {
        if first.is_some() || streams {
            crate::body_changed(&mut head);
        }
        head
    };
    crate::headed(head, Box::new(Writes))
}

/// The response decoder of `format` for a response with `head` to the call
/// with head `call`: a success's bytes, or a 101's if the call asked to
/// switch protocols, one message per frame; none for a response with no
/// body; any other answer read whole ([`whole::answer`](crate::whole::answer)).
pub fn response_decoder<F: 'static>(
    format: &str,
    head: &Answer,
    call: &Ask,
) -> Box<dyn Decoder<Got<F>>> {
    let switched = head.status == StatusCode::SWITCHING_PROTOCOLS && asked_to_switch(call);
    match head.status.is_success() || switched {
        _ if head.status.is_success() && bodiless(&call.method, head.status) => {
            crate::whole::nothing()
        }
        false => crate::whole::decoder(format, head, |_, _| Ok(())),
        true => crate::message_decoder(|bytes| Ok(Got::new(bytes))),
    }
}

/// Whether a call asked to switch protocols (RFC 9110 7.8).
fn asked_to_switch(call: &Ask) -> bool {
    call.headers.contains_key(http::header::UPGRADE)
}

/// The response encoder of `format`: messages as they came; a `refusal`
/// as the provider's answer in `format` as it came, else its message as
/// plain text under its status, if an error's. A failure once bytes went
/// out is an `Err` that aborts the body. Nothing for a response with no
/// body.
pub fn response_encoder<F: 'static>(
    format: &str,
    answer: Option<&Answer>,
    ask: &Ask,
    refusal: Option<&Fault>,
) -> Box<dyn Encoder<Got<F>, Answer>> {
    let mut head = crate::answer_head(answer);
    let body = response_body(format, &mut head, ask, refusal);
    crate::with_head(head, body)
}

fn response_body<F: 'static>(
    format: &str,
    head: &mut Answer,
    ask: &Ask,
    refusal: Option<&Fault>,
) -> Box<dyn Encoder<Got<F>, ()>> {
    if let Some(fault) = refusal {
        let body = match respan_gateway_http::answered(format, fault, head) {
            Some(body) => body,
            None if !crate::whole::is_error(fault.status()) => return crate::whole::unwritable(),
            None => {
                default_content_type(head, "text/plain; charset=utf-8");
                Bytes::from(fault.message().to_owned())
            }
        };
        let bodiless = bodiless(&ask.method, head.status);
        return crate::whole::written(if bodiless { Bytes::new() } else { body });
    }
    if bodiless(&ask.method, head.status) {
        return crate::whole::no_body();
    }
    Box::new(Gets)
}

struct Gets;

impl<F: 'static> Encoder<Got<F>, ()> for Gets {
    fn head(&mut self, _first: Option<&Got<F>>) {}

    fn encode(&mut self, got: &Got<F>, out: &mut BytesMut) -> Result<(), Fault> {
        out.extend_from_slice(&got.0);
        Ok(())
    }

    fn end(&mut self, end: &Result<(), Fault>, _out: &mut BytesMut) -> Result<(), Fault> {
        end.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use respan_gateway_http::http::{HeaderValue, header::CONTENT_TYPE};

    struct Raw;

    // A refusal without an answer in this format is plain text under its
    // status; one with it goes as it came.
    #[test]
    fn a_refusal_is_its_answer_or_plain_text() {
        let ask = respan_gateway_http::fresh_ask();
        let fault = Fault::not_found("gone");
        let mut encoder = response_encoder::<Raw>("raw", None, &ask, Some(&fault));
        let head = encoder.head(None);
        let mut out = BytesMut::new();
        assert!(encoder.end(&Err(fault), &mut out).is_ok());
        let plain = HeaderValue::from_static("text/plain; charset=utf-8");
        assert_eq!(head.headers.get(CONTENT_TYPE), Some(&plain));
        assert_eq!((head.status.as_u16(), &out[..]), (404, &b"gone"[..]));
        let mut answer = http::Response::new(Bytes::from_static(b"{}"));
        *answer.status_mut() = StatusCode::TOO_MANY_REQUESTS;
        let fault = Fault::upstream("x").with_answer("raw", answer);
        let mut encoder = response_encoder::<Raw>("raw", None, &ask, Some(&fault));
        let head = encoder.head(None);
        let mut out = BytesMut::new();
        assert!(encoder.end(&Err(fault), &mut out).is_ok());
        assert_eq!((head.status.as_u16(), &out[..]), (429, &b"{}"[..]));
    }

    #[test]
    fn bytes_pass_as_they_came() {
        let head = respan_gateway_http::fresh_answer();
        let call = respan_gateway_http::fresh_ask();
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
        let call = respan_gateway_http::fresh_ask();
        let mut decoder = response_decoder::<Raw>("raw", &head, &call);
        let mut got = Vec::new();
        assert!(decoder.decode(Bytes::from_static(b"a"), &mut got).is_ok());
        let fault = decoder.end(&mut got).err();
        assert_eq!(fault.as_ref().map(Fault::code), Some("upstream"));
        assert!(fault.is_some_and(|f| !f.is_answer()), "never relayed");
    }
}
