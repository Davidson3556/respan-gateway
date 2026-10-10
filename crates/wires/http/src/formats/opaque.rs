//! Traffic of no kind, relayed unread. Such a format is a kind of one,
//! [`Input<F>`] in and [`Output<F>`] out, so it relays only to itself, byte
//! for byte: one value per body or data frame.
//!
use std::marker::PhantomData;

use crate::{Body, ClientIo, Encoder, ProviderIo, Reply, bodiless};
use futures::StreamExt;
use respan_gateway_core::{Connection, EventSink, EventStream, Fault, Format};
use respan_gateway_http::bytes::{Bytes, BytesMut};
use respan_gateway_http::http::{self, StatusCode, request::Parts as RequestHead};

/// A request's message in format `F`, as the client sent it.
pub struct Input<F>(pub http::Request<Bytes>, PhantomData<fn() -> F>);

/// A response's message in format `F`, as the provider sent it.
pub struct Output<F>(pub http::Response<Bytes>, PhantomData<fn() -> F>);

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

bytes_type!(Input, Request, method, uri, version);
bytes_type!(Output, Response, status, version);

/// One message per finite body or explicitly live frame.
pub fn read_request<F: 'static>(request: http::Request<Body>) -> EventStream<Input<F>> {
    crate::messages(request, |head, raw| {
        Ok(Input::from_http(http::Request::from_parts(
            head.clone(),
            raw,
        )))
    })
}

/// Write each message's bytes as they came, under the head of the call's
/// one input with its safe forwarded path ([`forwarded_path`]); live input
/// keeps the given head.
///
/// [`forwarded_path`]: crate::formats::forwarded_path
pub fn write_request<F: 'static>(
    head: &mut RequestHead,
    input: Option<&Input<F>>,
) -> Result<Box<dyn Encoder<Input<F>>>, Fault> {
    if let Some(input) = input {
        *head = input.0.clone().into_parts().0;
        let path = crate::formats::forwarded_path(head.uri.path());
        crate::formats::set_path(head, &path)?;
    }
    Ok(Box::new(Writes))
}

/// The messages of a relayed answer: one per data frame, or one empty
/// message carrying the head of an answer with no body to the call `sent`
/// ([`bodiless`]) or no frames. A switched protocol's head comes first, as
/// its frames are the session's.
pub fn read_response<F: 'static>(
    response: http::Response<Body>,
    sent: http::Request<Bytes>,
) -> EventStream<Output<F>> {
    let (head, body) = response.into_parts();
    let empty = bodiless(sent.method(), head.status);
    let switched = head.status == StatusCode::SWITCHING_PROTOCOLS;
    let first = (empty || switched)
        .then(|| Output::from_http(http::Response::from_parts(head.clone(), Bytes::new())));
    let rest = futures::stream::unfold(
        (body, head, first.is_some(), empty),
        |(mut body, head, seen, empty)| async move {
            if empty {
                return None;
            }
            match body.next().await {
                Some(raw) => Some((
                    raw.map(|raw| Output::from_http(http::Response::from_parts(head.clone(), raw))),
                    (body, head, true, false),
                )),
                None if !seen => Some((
                    Ok(Output::from_http(http::Response::from_parts(
                        head.clone(),
                        Bytes::new(),
                    ))),
                    (body, head, true, true),
                )),
                None => None,
            }
        },
    );
    EventStream::stream(futures::stream::iter(first.map(Ok)).chain(rest))
}

/// Write opaque response bytes as they come, the head with the first.
pub fn write_response<F: 'static>(reply: Reply) -> Result<EventSink<Output<F>>, Fault> {
    Ok(EventSink::new(OpaqueSink {
        reply: Some(reply),
        sink: None,
    }))
}

struct OpaqueSink<F> {
    reply: Option<Reply>,
    sink: Option<EventSink<Output<F>>>,
}
impl<F> Unpin for OpaqueSink<F> {}
impl<F: 'static> futures::Sink<Result<Output<F>, Fault>> for OpaqueSink<F> {
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
        item: Result<Output<F>, Fault>,
    ) -> Result<(), Fault> {
        if let Some(reply) = self.reply.take() {
            let head = match &item {
                Ok(got) => reply.head_or(got.0.clone().into_parts().0),
                Err(_) => reply.head(),
            };
            self.sink = Some(reply.open(head, Bytes::new(), Box::new(Writes)));
            // A switched protocol's first message is its head alone
            // (`read_response`), not a frame of the session.
            if matches!(&item, Ok(got) if got.0.status() == StatusCode::SWITCHING_PROTOCOLS) {
                return Ok(());
            }
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

impl Format<ClientIo, Input<Self>, Output<Self>> for Passthrough {
    fn name(&self) -> &str {
        "passthrough"
    }
    fn matches(&self, io: &ClientIo) -> bool {
        !io.request()
            .method()
            .as_str()
            .bytes()
            .any(|b| b.is_ascii_lowercase())
            && crate::formats::reads_one_way(io.request().uri().path())
    }
    fn bind(&self, io: ClientIo) -> Result<Connection<Input<Self>, Output<Self>>, Fault> {
        Ok(io.bind(read_request, write_response, crate::plain))
    }
}
impl Format<ProviderIo, Output<Self>, Input<Self>> for Passthrough {
    fn name(&self) -> &str {
        "passthrough"
    }
    /// Relays successes and the redirects of a call that asked to switch no
    /// protocols, which are the client's to follow.
    fn bind(&self, io: ProviderIo) -> Result<Connection<Output<Self>, Input<Self>>, Fault> {
        let relayed = |answer: &http::response::Parts, call: &RequestHead| {
            let asked = call.headers.contains_key(http::header::UPGRADE);
            crate::success(answer, call) || (answer.status.is_redirection() && !asked)
        };
        Ok(io
            .with_success(relayed)
            .bind(write_request, read_response, |answer| {
                crate::provider_fault("passthrough", answer)
            }))
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
        let got: Vec<_> = read_response::<Raw>(
            http::Response::from_parts(head, Body::full("a")),
            http::Request::new(Bytes::new()),
        )
        .map(|v| v.expect("expected value"))
        .collect()
        .await;
        let mut writer = Writes;
        let mut out = BytesMut::new();
        assert!(writer.encode(&Input::<Raw>::new("b"), &mut out).is_ok());
        assert_eq!((&got[0].0.body()[..], &out[..]), (&b"a"[..], &b"b"[..]));
    }
}
