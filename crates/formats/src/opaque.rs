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
use respan_gateway_core::http;
use respan_gateway_core::{Decoder, Encoder, Fault};

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

impl<F: 'static> Encoder<Sent<F>> for Writes {
    fn encode(&mut self, sent: &Sent<F>, out: &mut BytesMut) -> Result<(), Fault> {
        out.extend_from_slice(&sent.0);
        Ok(())
    }

    fn end(&mut self, _end: &Result<(), Fault>, _out: &mut BytesMut) -> Result<(), Fault> {
        Ok(())
    }
}

/// The request encoder: each message as it came.
pub fn request_encoder<F: 'static>() -> Box<dyn Encoder<Sent<F>>> {
    Box::new(Writes)
}

/// The response decoder of `format`: a success's or a 101's bytes, one
/// message per frame; a failure read whole ([`whole::decoder`](crate::whole::decoder)).
pub fn response_decoder<F: 'static>(
    format: &str,
    head: &http::response::Parts,
) -> Box<dyn Decoder<Got<F>>> {
    match passes(head) {
        false => crate::whole::decoder(format, head, |_, _| Ok(())),
        true => crate::message_decoder(|bytes| Ok(Got::new(bytes))),
    }
}

/// Whether a response's bytes are the protocol's own: a success or a 101.
fn passes(head: &http::response::Parts) -> bool {
    head.status.is_success() || head.status == http::StatusCode::SWITCHING_PROTOCOLS
}

/// The response encoder of `format`: messages and a provider's error body
/// in `format` as they came; another failure as plain text under a failed
/// head, else an `Err` that aborts the body.
pub fn response_encoder<F: 'static>(
    format: &str,
    head: &mut http::response::Parts,
) -> Box<dyn Encoder<Got<F>>> {
    let ok = passes(head);
    if !ok {
        default_content_type(head, "text/plain; charset=utf-8");
    }
    Box::new(Gets {
        format: format.to_owned(),
        ok,
    })
}

struct Gets {
    format: String,
    /// Whether the head is a success's, so a failure cannot be text.
    ok: bool,
}

impl<F: 'static> Encoder<Got<F>> for Gets {
    fn encode(&mut self, got: &Got<F>, out: &mut BytesMut) -> Result<(), Fault> {
        out.extend_from_slice(&got.0);
        Ok(())
    }

    fn end(&mut self, end: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault> {
        let Err(f) = end else { return Ok(()) };
        match (f.raw(&self.format), self.ok) {
            (Some(body), _) => out.extend_from_slice(body),
            (None, true) => return Err(f.clone()),
            (None, false) => out.extend_from_slice(f.message().as_bytes()),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use respan_gateway_core::http::{HeaderValue, header::CONTENT_TYPE};

    struct Raw;

    // A failed or redirect head without a content type gets plain text; a
    // success's is its bytes' to say.
    #[test]
    fn a_failure_or_redirect_without_a_content_type_gets_plain_text() {
        for (status, said) in [(302, Some("text/plain; charset=utf-8")), (200, None)] {
            let (mut head, ()) = http::Response::new(()).into_parts();
            head.status = http::StatusCode::from_u16(status).unwrap_or_default();
            let _ = response_encoder::<Raw>("raw", &mut head);
            let said = said.map(HeaderValue::from_static);
            assert_eq!(head.headers.get(CONTENT_TYPE), said.as_ref(), "{status}");
        }
    }

    #[test]
    fn bytes_pass_as_they_came() {
        let (head, ()) = http::Response::new(()).into_parts();
        let mut decoder = response_decoder::<Raw>("raw", &head);
        let mut got = Vec::new();
        assert!(decoder.decode(Bytes::from_static(b"a"), &mut got).is_ok());
        assert!(decoder.end(&mut got).is_ok());
        let mut encoder = request_encoder::<Raw>();
        let mut out = BytesMut::new();
        assert!(encoder.encode(&Sent::new("b"), &mut out).is_ok());
        assert_eq!((&got[0].0[..], &out[..]), (&b"a"[..], &b"b"[..]));
    }
}
