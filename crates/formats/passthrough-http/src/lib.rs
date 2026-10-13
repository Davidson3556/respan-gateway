//! The passthrough format: traffic of no kind over HTTP, relayed unread.
//! Such a format is a kind of one, [`Input<F>`] in and [`Output<F>`] out,
//! so it relays only to itself, byte for byte: one value per body or data
//! frame.
//!
//! # Paths
//!
//! The [`Passthrough`] format, served last, relays every request no format
//! before it recognises to a vendor, unread, except a path readers could read
//! two ways. It takes a path only if, after one percent-decode, every byte is
//! an ASCII letter or digit or one of `/ - _ . ~ : @ + = , !`, and no segment
//! is `.`, `..` or ends in `.`; anything else (non-ASCII, space, `?`, `#`,
//! `%`, `;`, `\`, control characters) is a 404. It is an allowlist, so a
//! character some reader treats specially is refused until someone shows a
//! vendor needs it.

mod paths;

use std::marker::PhantomData;

use respan_gateway_core::{Connection, EventSink, Fault, Format};
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::http::{
    self, StatusCode, request::Parts as RequestHead, response::Parts as ResponseHead,
};
use respan_gateway_http::{ClientIo, Decoder, Encoder, ProviderIo, Reply, rules::bodiless};

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
            fn encode(&mut self, value: &$name<F>, out: &mut Vec<Bytes>) -> Result<(), Fault> {
                out.push(value.0.body().clone());
                Ok(())
            }
        }
    };
}

/// Writes each message as it came.
struct Writes;

bytes_type!(Input, Request, method, uri, version);
bytes_type!(Output, Response, status, version);

/// One message per whole body or live frame, under the request's head.
pub fn read_request<F: 'static>(head: &RequestHead) -> Box<dyn Decoder<Input<F>>> {
    let head = head.clone();
    Box::new(move |raw: Bytes, out: &mut Vec<Input<F>>| {
        let message = http::Request::from_parts(head.clone(), raw);
        out.push(Input::from_http(message));
        Ok(())
    })
}

/// Write each message's bytes as they came, under the head of the call's
/// one input with its safe forwarded path ([`forwarded_path`]); live input
/// keeps the given head, as [`request_writer`] gives it.
///
/// [`forwarded_path`]: respan_gateway_http_syntax::forwarded_path
pub fn write_request<F: 'static>(
    head: &mut RequestHead,
    input: Option<&Input<F>>,
) -> Result<Box<dyn Encoder<Input<F>>>, Fault> {
    if let Some(input) = input {
        *head = input.0.clone().into_parts().0;
        let path = respan_gateway_http_syntax::forwarded_path(head.uri.path());
        respan_gateway_http_syntax::set_path(head, &path)?;
    }
    Ok(Box::new(Writes))
}

/// The encoder of a call's input, or why it cannot be written.
type Writing<F> = Result<Box<dyn Encoder<Input<F>>>, Fault>;

/// [`write_request`] for a call on `io`: live input goes under the head of
/// the client's request ([`Original`](respan_gateway_http::raw::Original)), with its
/// safe forwarded path, as a session's handshake relays.
pub fn request_writer<F: 'static>(
    io: &ProviderIo,
) -> impl FnOnce(&mut RequestHead, Option<&Input<F>>) -> Writing<F> + Send + 'static {
    let client = io
        .extensions()
        .get::<respan_gateway_http::raw::Original>()
        .and_then(|w| w.get())
        .map(|r| r.clone().into_parts().0);
    move |head, input| {
        if let (None, Some(client)) = (input, client) {
            *head = client;
            let path = respan_gateway_http_syntax::forwarded_path(head.uri.path());
            respan_gateway_http_syntax::set_path(head, &path)?;
        }
        write_request(head, input)
    }
}

/// The messages of a relayed answer: one per data frame, or one empty
/// message carrying the head of an answer with no body to the call `sent`
/// ([`bodiless`]) or no frames. A switched protocol's head comes first, as
/// its frames are the session's.
pub fn read_response<F: 'static>(
    head: &ResponseHead,
    sent: &http::Request<Bytes>,
) -> Option<Box<dyn Decoder<Output<F>>>> {
    Some(Box::new(Reads {
        head: head.clone(),
        empty: bodiless(sent.method(), head.status),
        seen: false,
        types: PhantomData,
    }))
}

/// Reads each frame of an answer as a message under its head.
struct Reads<F> {
    head: ResponseHead,
    /// Whether the answer has no body to read.
    empty: bool,
    /// Whether a message went out.
    seen: bool,
    types: PhantomData<fn() -> F>,
}

impl<F> Reads<F> {
    fn message(&mut self, raw: Bytes) -> Output<F> {
        self.seen = true;
        Output::from_http(http::Response::from_parts(self.head.clone(), raw))
    }
}

impl<F: 'static> Decoder<Output<F>> for Reads<F> {
    fn start(&mut self, out: &mut Vec<Output<F>>) -> Result<(), Fault> {
        if self.empty || self.head.status == StatusCode::SWITCHING_PROTOCOLS {
            out.push(self.message(Bytes::new()));
        }
        Ok(())
    }

    fn decode(&mut self, raw: Bytes, out: &mut Vec<Output<F>>) -> Result<(), Fault> {
        if !self.empty {
            out.push(self.message(raw));
        }
        Ok(())
    }

    fn end(&mut self, end: Result<(), Fault>, out: &mut Vec<Output<F>>) -> Result<(), Fault> {
        if end.is_ok() && !self.seen {
            out.push(self.message(Bytes::new()));
        }
        end
    }
}

/// Write opaque response bytes as they come, the head with the first.
pub fn write_response<F: 'static>(reply: Reply) -> Result<EventSink<Output<F>>, Fault> {
    Ok(EventSink::new(PassthroughSink {
        reply: Some(reply),
        sink: None,
    }))
}

struct PassthroughSink<F> {
    reply: Option<Reply>,
    sink: Option<EventSink<Output<F>>>,
}
impl<F> Unpin for PassthroughSink<F> {}
impl<F: 'static> futures::Sink<Result<Output<F>, Fault>> for PassthroughSink<F> {
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
            self.sink = Some(reply.open(head, Box::new(Writes)));
            // A switched protocol's first message is its head alone
            // (`read_response`), not a frame of the session.
            if matches!(&item, Ok(got) if got.0.status() == StatusCode::SWITCHING_PROTOCOLS) {
                return Ok(());
            }
        }
        std::pin::Pin::new(self.sink.as_mut().expect("passthrough sink initialized"))
            .start_send(item)
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
            self.sink = Some(reply.open(head, Box::new(Writes)));
        }
        std::pin::Pin::new(self.sink.as_mut().expect("passthrough sink initialized")).poll_close(cx)
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
            && paths::reads_one_way(io.request().uri().path())
    }
    fn bind(&self, io: ClientIo) -> Result<Connection<Input<Self>, Output<Self>>, Fault> {
        Ok(io.bind(read_request, write_response))
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
            respan_gateway_http::rules::success(answer, call)
                || (answer.status.is_redirection() && !asked)
        };
        let write = request_writer(&io);
        Ok(io.with_success(relayed).bind(write, read_response))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone)]
    struct Raw;

    #[test]
    fn bytes_pass_as_they_came() {
        let head = http::Response::new(()).into_parts().0;
        let sent = http::Request::new(Bytes::new());
        let mut reads = read_response::<Raw>(&head, &sent).expect("a decoder");
        let mut got = Vec::new();
        assert!(reads.start(&mut got).is_ok());
        assert!(reads.decode(Bytes::from_static(b"a"), &mut got).is_ok());
        let mut out = Vec::new();
        assert!(Writes.encode(&Input::<Raw>::new("b"), &mut out).is_ok());
        assert_eq!((&got[0].0.body()[..], &out[0][..]), (&b"a"[..], &b"b"[..]));
    }
}
