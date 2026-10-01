//! Traffic of no kind, relayed unread. Such a format is a kind of one,
//! `Sent<F>` in and `Got<F>` out, so it relays only to itself, byte for
//! byte: one value per body or data frame.
//!
use std::marker::PhantomData;

use crate::{Body, Encoder, Incoming, Outgoing, Reply, RequestHead, bodiless};
use futures::{StreamExt, stream::BoxStream};
use respan_gateway_http::bytes::{Bytes, BytesMut};
use respan_gateway_http::http::{self, StatusCode};
use respan_gateway_sdk::{Connection, EventSink, EventStream, Fault, Format};

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

        impl<F: 'static> Encoder<$name<F>> for Writes {
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

/// One message per finite body or explicitly live frame.
pub fn read_request<F: 'static>(
    request: http::Request<Body>,
) -> Result<EventStream<Sent<F>>, Fault> {
    crate::messages(request, |_, raw| Ok(Sent::new(raw)))
}

/// Relay eligible client metadata and its safe forwarded path.
pub fn write_request<F: 'static>(
    name: &str,
    source: &mut EventStream<Sent<F>>,
) -> Result<(http::Request<Body>, EventSink<Sent<F>>), Fault> {
    let relayed = crate::request_head(name, source).is_some();
    let (mut request, sink) = crate::request(name, source, Box::new(Writes))?;
    if relayed {
        let path = crate::formats::forwarded_path(request.uri().path());
        let (mut head, body) = request.into_parts();
        crate::formats::set_path(&mut head, &path);
        request = http::Request::from_parts(head, body);
    }
    Ok((request, sink))
}

/// Relay successes and ordinary redirects. A failed or unasked upgrade is a
/// refusal, and bodyless answers consume no semantic values.
pub fn read_response<F: 'static>(
    name: &str,
    response: http::Response<Body>,
    call: &RequestHead,
) -> BoxStream<'static, Result<Got<F>, Fault>> {
    let asked = call.headers.contains_key(http::header::UPGRADE);
    let status = response.status();
    let switched = status == StatusCode::SWITCHING_PROTOCOLS && asked;
    let relayed = status.is_success() || (status.is_redirection() && !asked);
    if relayed && bodiless(&call.method, status) {
        crate::formats::whole::nothing(response.into_body())
    } else if relayed || switched {
        response.into_body().map(|raw| raw.map(Got::new)).boxed()
    } else {
        crate::formats::whole::refusal(name, response)
    }
}

/// Write opaque response bytes, retaining an unread bodyless source.
pub fn write_response<F: 'static>(
    name: &str,
    reply: Reply,
    source: &mut EventStream<Got<F>>,
) -> EventSink<Got<F>> {
    let head = crate::response_head(name, source);
    if bodiless(reply.request().method(), head.status) {
        reply.empty(head, source)
    } else {
        reply.write(head, Box::new(Writes))
    }
}

/// Every request no format before it accepts, relayed unread, byte for
/// byte, to a provider of this format: how a vendor's other endpoints
/// (token counting, files, a newer API) pass through a gateway that knows
/// only its chat one. Serve it last, and send its requests to a provider of
/// this format at the vendor's origin, as the client's path is relayed as it
/// is. Named `passthrough`, so a client's key goes only to a provider of
/// this format.
///
/// It forwards what it never read, so it takes only a path every reader
/// reads one way: percent-decoded once, every byte is an ASCII letter or
/// digit or one of `/ - _ . ~ : @ + = ,` and `!`, and no segment is `.`,
/// `..` or ends in `.`. Any other path (non-ASCII, a space, a decoded `?`,
/// `#`, `%`, `;` or `\`, a control character, a quote) is no format's, a
/// 404: a format before it matched it by one reading, and a vendor, or a
/// front end before it, may read another, such as its chat endpoint, which
/// would carry a chat call past the chat format and its plugins.
///
/// This is an allowlist because each special character some reader treats
/// its own way was found one loop at a time: with a list of what is
/// allowed, a new one is refused by default. No vendor API this relays
/// needs more, as ids and model names are ASCII: an escaped `/` (a Bedrock
/// ARN's `%2F`), a `:` or an `@` (Vertex) and base64's `+` and `=` pass.
#[derive(Clone, Copy, Debug, Default)]
pub struct Passthrough;

impl Format<Incoming, Sent<Self>, Got<Self>> for Passthrough {
    fn name(&self) -> &str {
        "passthrough"
    }
    fn matches(&self, io: &Incoming) -> bool {
        !io.request()
            .method()
            .as_str()
            .bytes()
            .any(|b| b.is_ascii_lowercase())
            && crate::formats::reads_one_way(io.request().uri().path())
    }
    fn bind(&self, io: Incoming) -> Result<Connection<Sent<Self>, Got<Self>>, Fault> {
        Ok(io.bind(
            "passthrough",
            read_request,
            |reply, source| Ok(write_response("passthrough", reply, source)),
            crate::plain_refusal,
        ))
    }
}
impl Format<Outgoing, Got<Self>, Sent<Self>> for Passthrough {
    fn name(&self) -> &str {
        "passthrough"
    }
    fn bind(&self, io: Outgoing) -> Result<Connection<Got<Self>, Sent<Self>>, Fault> {
        Ok(io.bind(
            "passthrough",
            |source| write_request("passthrough", source),
            |response, request| read_response("passthrough", response, &request.into_parts().0),
            |head, call| {
                crate::success(head, call)
                    || (head.status.is_redirection()
                        && !call.headers.contains_key(http::header::UPGRADE))
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone)]
    struct Raw;

    #[tokio::test]
    async fn bytes_pass_as_they_came() {
        let head = respan_gateway_http::fresh_response_head();
        let call = respan_gateway_http::fresh_request_head();
        let got: Vec<_> = read_response::<Raw>(
            "raw",
            http::Response::from_parts(head, Body::full("a")),
            &call,
        )
        .map(|v| v.expect("expected value"))
        .collect()
        .await;
        let mut writer = Writes;
        let mut out = BytesMut::new();
        assert!(writer.encode(&Sent::<Raw>::new("b"), &mut out).is_ok());
        assert_eq!((&got[0].0[..], &out[..]), (&b"a"[..], &b"b"[..]));
    }

    // A provider's 101 the call did not ask for fails it, now this
    // format's rule, not the kernel's.
    #[tokio::test]
    async fn a_101_no_one_asked_for_fails_the_call() {
        let (mut head, ()) = http::Response::new(()).into_parts();
        head.status = StatusCode::SWITCHING_PROTOCOLS;
        let call = respan_gateway_http::fresh_request_head();
        let fault = read_response::<Raw>(
            "raw",
            http::Response::from_parts(head, Body::full("a")),
            &call,
        )
        .next()
        .await
        .expect("expected value")
        .err();
        assert_eq!(fault.as_ref().map(Fault::code), Some("upstream"));
        let answer = fault.and_then(|f| f.answer::<http::Response<Bytes>>("raw").cloned());
        assert!(answer.is_none(), "never relayed");
    }
}
