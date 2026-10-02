//! Traffic of no kind, relayed unread. Such a format is a kind of one,
//! `Sent<F>` in and `Got<F>` out, so it relays only to itself, byte for
//! byte: one value per body or data frame.
//!
use std::marker::PhantomData;

use crate::{Body, Encoder, Incoming, Outgoing, Reply, bodiless};
use futures::{StreamExt, stream::BoxStream};
use respan_gateway_http::bytes::{Bytes, BytesMut};
use respan_gateway_http::http::{self, StatusCode, request::Parts as RequestHead};
use respan_gateway_sdk::{Connection, EventSink, EventStream, Fault, Format};

/// A request's message in format `F`, as the client sent it.
pub struct Sent<F>(pub http::Request<Bytes>, PhantomData<fn() -> F>);

/// A response's message in format `F`, as the provider sent it.
pub struct Got<F>(pub http::Response<Bytes>, PhantomData<fn() -> F>);

macro_rules! bytes_type {
    ($name:ident, $http:ident, $($field:ident),+) => {
        impl<F> $name<F> {
            /// The message `bytes`.
            pub fn new(bytes: impl Into<Bytes>) -> Self {
                Self(http::$http::new(bytes.into()), PhantomData)
            }
            /// An owned head and body.
            pub fn from_http(message: http::$http<Bytes>) -> Self {
                Self(message, PhantomData)
            }
        }

        impl<F> Clone for $name<F> {
            fn clone(&self) -> Self {
                Self(self.0.clone(), PhantomData)
            }
        }

        impl<F> PartialEq for $name<F> {
            fn eq(&self, other: &Self) -> bool {
                $(self.0.$field() == other.0.$field() &&)+
                self.0.headers() == other.0.headers() && self.0.body() == other.0.body()
            }
        }

        impl<F> std::fmt::Debug for $name<F> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_tuple(stringify!($name)).field(&self.0).finish()
            }
        }

        impl<F: 'static> Encoder<$name<F>> for Writes {
            fn encode(&mut self, value: &$name<F>, out: &mut BytesMut) -> Result<(), Fault> {
                out.extend_from_slice(value.0.body());
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

bytes_type!(Sent, Request, method, uri, version);
bytes_type!(Got, Response, status, version);

/// One message per finite body or explicitly live frame.
pub fn read_request<F: 'static>(
    request: http::Request<Body>,
) -> Result<EventStream<Sent<F>>, Fault> {
    crate::messages(request, |head, raw| {
        Ok(Sent::from_http(http::Request::from_parts(
            head.clone(),
            raw,
        )))
    })
}

/// Encode the typed request head and body with its safe forwarded path.
pub fn write_request<F: 'static>(
    name: &str,
    source: &mut EventStream<Sent<F>>,
) -> Result<(http::Request<Body>, EventSink<Sent<F>>), Fault> {
    let head = source
        .value()
        .transpose()?
        .map(|sent| sent.0.clone().into_parts().0)
        .unwrap_or_else(|| http::Request::new(()).into_parts().0);
    let (request, sink) = crate::request(name, source, Box::new(Writes))?;
    let path = crate::formats::forwarded_path(head.uri.path());
    let mut head = head;
    crate::formats::set_path(&mut head, &path);
    Ok((http::Request::from_parts(head, request.into_body()), sink))
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
    if relayed || switched {
        let (head, body) = response.into_parts();
        let empty = bodiless(&call.method, status);
        let first = (empty || switched)
            .then(|| Got::from_http(http::Response::from_parts(head.clone(), Bytes::new())));
        let rest = futures::stream::unfold(
            (body, head, first.is_some(), empty),
            |(mut body, head, seen, empty)| async move {
                if empty {
                    return None;
                }
                match body.next().await {
                    Some(raw) => Some((
                        raw.map(|raw| {
                            Got::from_http(http::Response::from_parts(head.clone(), raw))
                        }),
                        (body, head, true, false),
                    )),
                    None if !seen => Some((
                        Ok(Got::from_http(http::Response::from_parts(
                            head.clone(),
                            Bytes::new(),
                        ))),
                        (body, head, true, true),
                    )),
                    None => None,
                }
            },
        );
        futures::stream::iter(first.map(Ok)).chain(rest).boxed()
    } else {
        crate::formats::whole::refusal(name, response)
    }
}

/// Write opaque response bytes, retaining an unread bodyless source.
pub fn write_response<F: 'static>(
    _name: &str,
    reply: Reply,
    source: &mut EventStream<Got<F>>,
) -> EventSink<Got<F>> {
    let _ = source;
    EventSink::new(OpaqueSink {
        reply: Some(reply),
        sink: None,
    })
}

struct OpaqueSink<F> {
    reply: Option<Reply>,
    sink: Option<EventSink<Got<F>>>,
}
impl<F> Unpin for OpaqueSink<F> {}
impl<F: 'static> futures::Sink<Result<Got<F>, Fault>> for OpaqueSink<F> {
    type Error = Fault;
    fn poll_ready(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Fault>> {
        match &mut self.sink {
            Some(sink) => std::pin::Pin::new(sink).poll_ready(cx),
            None => std::task::Poll::Ready(Ok(())),
        }
    }
    fn start_send(
        mut self: std::pin::Pin<&mut Self>,
        item: Result<Got<F>, Fault>,
    ) -> Result<(), Fault> {
        if let Some(reply) = self.reply.take() {
            let head = match &item {
                Ok(got) => reply.head_or(got.0.clone().into_parts().0),
                Err(_) => reply.head(),
            };
            self.sink = Some(reply.open(head, Bytes::new(), Box::new(Writes)));
        }
        std::pin::Pin::new(self.sink.as_mut().expect("opaque sink initialized")).start_send(item)
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Fault>> {
        match &mut self.sink {
            Some(sink) => std::pin::Pin::new(sink).poll_flush(cx),
            None => std::task::Poll::Ready(Ok(())),
        }
    }
    fn poll_close(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Fault>> {
        if let Some(reply) = self.reply.take() {
            let head = reply.head();
            self.sink = Some(reply.open(head, Bytes::new(), Box::new(Writes)));
        }
        std::pin::Pin::new(self.sink.as_mut().expect("opaque sink initialized")).poll_close(cx)
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
        let head = http::Response::new(()).into_parts().0;
        let call = http::Request::new(()).into_parts().0;
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
        assert_eq!((&got[0].0.body()[..], &out[..]), (&b"a"[..], &b"b"[..]));
    }

    // A provider's 101 the call did not ask for fails it, now this
    // format's rule, not the kernel's.
    #[tokio::test]
    async fn a_101_no_one_asked_for_fails_the_call() {
        let (mut head, ()) = http::Response::new(()).into_parts();
        head.status = StatusCode::SWITCHING_PROTOCOLS;
        let call = http::Request::new(()).into_parts().0;
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
        let answer = fault.and_then(|f| {
            f.get::<respan_gateway_http::ProviderError>()
                .map(|e| &e.response)
                .cloned()
        });
        assert!(answer.is_none(), "never relayed");
    }
}
