#![warn(missing_docs)]
//! HTTP at the gateway's edges: its formats and its rules.
//!
//! - **Formats.** A [`Codec`] is one protocol's bytes on HTTP;
//!   [`dialect!`] makes it a [`Format`](respan_gateway_core::Format), which
//!   HTTP's one driver runs on both edges: a client's [`Accepted`] request,
//!   and calls through a provider's [`Dialer`], made from any [`Transport`].
//!
//! Its rules live at the three places that own them:
//!
//! - **The adapter** ([`serve`], [`handle`]) strips the connection's
//!   headers from a request, lifts the client's credentials into a
//!   [`Credential`] extension, publishes an [`Asked`] view of the head,
//!   writes what no format can in plain text, and strips what a hop must
//!   not carry from the response ([`outgoing`]).
//! - **A transport** strips what a proxy must not pass on from a call
//!   ([`proxy_call`], [`strip_client_origin_headers`]), decides whose keys
//!   reach the provider ([`credentials`], [`CallHeaders`]), and strips a
//!   provider origin's own headers from its response
//!   ([`strip_origin_headers`]).
//! - **An HTTP format** writes a fault's status and `Retry-After`
//!   ([`error_head`], [`answered`]), reads a provider's
//!   ([`retry_after`]), and writes no body where HTTP has none
//!   ([`bodiless`]).

mod codec;
mod credential;
mod driver;
mod transport;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::future::{BoxFuture, ready};
use futures::{FutureExt, StreamExt, TryStreamExt};
use http::header::{
    CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, HeaderName, PROXY_AUTHENTICATE, PROXY_AUTHORIZATION,
    RETRY_AFTER, TE, TRAILER, TRANSFER_ENCODING, UPGRADE,
};
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use http_body::Frame;
use http_body_util::{BodyExt, Full, StreamBody};
use respan_gateway_core::futures;
use respan_gateway_core::{Extensions, Fault, Gateway, codes};

/// The crates HTTP formats and transports use, at these versions.
pub use {bytes, http, http_body, http_body_util};

pub use codec::{Codec, Decoder, Encoder};
pub use credential::Credential;
pub use driver::{Accepted, accept, dial, opening};
pub use transport::{
    CallHeaders, Dialer, Exchange, Frames, Transport, credentials, is_connection_header,
    proxy_call, strip_client_origin_headers, strip_origin_headers,
};

/// The head of an HTTP request.
pub type Ask = http::request::Parts;

/// The head of an HTTP response.
pub type Answer = http::response::Parts;

/// An HTTP body of bytes whose error is a [`Fault`].
pub type Body = http_body_util::combinators::UnsyncBoxBody<Bytes, Fault>;

/// A request head with nothing set: a `GET /`, for a format to fill.
pub fn fresh_ask() -> Ask {
    http::Request::new(()).into_parts().0
}

/// A response head with nothing set: a `200`, for a format to fill.
pub fn fresh_answer() -> Answer {
    http::Response::new(()).into_parts().0
}

/// The [`Frames`] of an HTTP `body`: its data frames, trailers dropped.
pub fn body_of<B>(body: B) -> Frames
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Fault>,
{
    let frames = http_body_util::BodyStream::new(body.map_err(Into::into));
    let data = frames.try_filter_map(|frame| futures::future::ready(Ok(frame.into_data().ok())));
    data.boxed()
}

/// An HTTP [`Body`] of `body`'s frames, one data frame per item.
pub fn into_http_body(body: Frames) -> Body {
    StreamBody::new(body.map_ok(Frame::data)).boxed_unsync()
}

/// A [`Body`] of `bytes`, in one frame.
pub fn full(bytes: impl Into<Bytes>) -> Body {
    Full::new(bytes.into()).map_err(Fault::from).boxed_unsync()
}

/// An HTTP request as an exchange, its HTTP extensions dropped but the
/// gateway's [`Extensions`] it carries, if any, which an embedder or test
/// hands plugins with `req.extensions_mut().insert(extensions)`.
pub fn ask<B>(req: http::Request<B>) -> Exchange<Ask>
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Fault>,
{
    let (mut head, body) = req.into_parts();
    let extensions = head.extensions.remove::<Extensions>().unwrap_or_default();
    head.extensions = http::Extensions::new();
    let mut req = Exchange::new(head, body_of(body));
    req.extensions = extensions;
    req
}

/// An HTTP response as an exchange, such as a transport returns.
pub fn answer<B>(resp: http::Response<B>) -> Exchange<Answer>
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Fault>,
{
    let (head, body) = resp.into_parts();
    Exchange::new(head, body_of(body))
}

/// A call as an HTTP request, its extensions apart, as a transport sends it.
pub fn call(call: Exchange<Ask>) -> (http::Request<Body>, Extensions) {
    let req = http::Request::from_parts(call.head, into_http_body(call.body));
    (req, call.extensions)
}

/// A message a plugin or format built wrong: a bug, a 500.
pub fn built_wrong(e: http::Error) -> Fault {
    Fault::internal("an HTTP request or response was built wrong").with_source(e)
}

/// The HTTP status a fault asks for, as a hint ([`Fault::with`]): a plugin's
/// `Fault::new("over_limit", msg).with(Status(429))`, a provider's answer's.
/// The kernel names codes only; this is where they get numbers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Status(pub u16);

/// The status of `fault` as HTTP's: its [`Status`] hint, else its code's
/// default (a 400 for [`codes::BAD_REQUEST`], a 404 for `NOT_FOUND`, a 502
/// for `UPSTREAM` and `TRUNCATED`, a 499 for `ABANDONED`, a 500 for `CONFIG`,
/// `INTERNAL` and a code that is none of these), a 500 for a number that is
/// no status.
pub fn status(fault: &Fault) -> StatusCode {
    let number = match fault.get::<Status>() {
        Some(Status(number)) => *number,
        None => match fault.code() {
            codes::BAD_REQUEST => 400,
            codes::NOT_FOUND => 404,
            codes::UPSTREAM | codes::TRUNCATED => 502,
            codes::ABANDONED => 499,
            _ => 500,
        },
    };
    StatusCode::from_u16(number).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
}

/// The client's request head, without its [`Credential`] or extensions: a
/// read-only view for plugins (`cx.extensions().get::<Asked>()`).
#[derive(Debug, Clone)]
pub struct Asked(pub Arc<http::request::Parts>);

impl std::ops::Deref for Asked {
    type Target = http::request::Parts;

    fn deref(&self) -> &http::request::Parts {
        &self.0
    }
}

/// [`handle`] `req`, streaming if it asks to switch protocols (`Upgrade`):
/// the adapter for an embedder or test that performs every switch.
pub async fn serve<B>(
    gateway: &Gateway,
    req: http::Request<B>,
) -> (http::Response<Body>, BoxFuture<'static, Result<(), Fault>>)
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Fault>,
{
    let streaming = req.headers().contains_key(UPGRADE);
    handle(gateway, req, streaming).await
}

/// Hand `req` to `gateway` as an HTTP adapter does, streaming if
/// `streaming`: as an [`Accepted`] request, to [`Gateway::handle`]. Before:
/// the hop's headers go but `Upgrade` (RFC 9110 7.6.1), credentials into a
/// [`Credential`], and an [`Asked`] view joins them. After: what no format
/// can write, or a 101 to a request not `streaming`, goes as text/plain
/// ([`plain`]); the response loses a hop's headers ([`outgoing`]); a
/// [`bodiless`] one goes with none, the gateway's body read apart. The
/// caller bounds a whole body and removes an `Upgrade` it will not perform.
/// Call it on a tokio runtime.
pub async fn handle<B>(
    gateway: &Gateway,
    req: http::Request<B>,
    streaming: bool,
) -> (http::Response<Body>, BoxFuture<'static, Result<(), Fault>>)
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Fault>,
{
    let (Arrived { head, body, to }, extensions) = arrive(req);
    let (wire, answer) = Accepted::new(head, body, streaming);
    let ended = gateway.handle(wire, extensions).await;
    let (resp, ended) = match answer.await {
        Some(resp) => (Ok(resp), ended),
        None => {
            let fault = ended.await.err();
            let fault = fault.unwrap_or_else(|| Fault::internal("no format answered"));
            (Err(fault.clone()), ready(Err(fault)).boxed())
        }
    };
    (depart(&to, streaming, resp), ended)
}

/// Marks the plain text of a refusal no codec could write, which goes as
/// it is: HTTP's rules for a body do not apply to it.
#[derive(Clone, Copy)]
pub(crate) struct Unwritten;

/// A request as the adapter hands it on, and what its answer needs of it.
pub(crate) struct Arrived {
    pub(crate) head: Ask,
    pub(crate) body: Frames,
    pub(crate) to: Asking,
}

/// What of a request its answer's rules need.
pub(crate) struct Asking {
    pub(crate) method: Method,
    pub(crate) path: String,
}

/// A request as it arrives at the adapter ([`handle`]): the hop's headers
/// go but `Upgrade`, credentials into a [`Credential`], and an [`Asked`]
/// view joins them.
pub(crate) fn arrive<B>(req: http::Request<B>) -> (Arrived, Extensions)
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Fault>,
{
    let Exchange {
        mut head,
        body,
        mut extensions,
        ..
    } = ask(req);
    let method = head.method.clone();
    let path = head.uri.path().to_owned();
    strip_hops(&mut head.headers, true);
    if let Some(credential) = Credential::lift(&mut head) {
        extensions.insert(credential);
    }
    extensions.insert(Asked(Arc::new(head.clone())));
    let arrived = Arrived {
        head,
        body,
        to: Asking { method, path },
    };
    (arrived, extensions)
}

/// The answer to request `to` as it departs the adapter ([`handle`]): what
/// no format can write, or a 101 to a request not `streaming`, as
/// text/plain; a hop's headers gone; a [`bodiless`] one with none, the
/// gateway's body read apart.
pub(crate) fn depart(
    to: &Asking,
    streaming: bool,
    resp: Result<http::Response<Body>, Fault>,
) -> http::Response<Body> {
    // A refusal no codec could write goes as the client edge wrote it.
    let resp = match resp {
        Ok(mut resp) => match resp.extensions_mut().remove::<Unwritten>() {
            Some(Unwritten) => return resp,
            None => Ok(resp),
        },
        Err(fault) => Err(fault),
    };
    match resp {
        Err(fault) if fault.code() == codes::NOT_FOUND => {
            let text = format!("no format served accepts {} {}", to.method, to.path);
            plain(&Fault::not_found(text))
        }
        Err(fault) => plain(&fault),
        Ok(resp) if resp.status() == StatusCode::SWITCHING_PROTOCOLS && !streaming => {
            // RFC 9110 15.2.2: a 101 only to a request that asked.
            let asked = "a 101 to a request that did not ask to switch protocols";
            plain(&Fault::internal(asked))
        }
        Ok(resp) => {
            let (mut head, mut body) = resp.into_parts();
            outgoing(&mut head);
            if bodiless(&to.method, head.status) {
                // A hop drops a bodiless answer's body once its head is out, so
                // that is when the client leaves. Read what is ready now, so a
                // refusal's end is told, then drop the rest: the session ends
                // with its client (rule 5), never when a provider's stream
                // does.
                while let Some(Some(_)) = body.frame().now_or_never() {}
                body = full(Bytes::new());
            }
            http::Response::from_parts(head, body)
        }
    }
}

/// A hop's own headers: the connection's (RFC 9110 7.6.1), the proxy's
/// (11.7), `Trailer`, as trailers are not delivered, and the length.
const HOP_BY_HOP: [HeaderName; 10] = [
    CONNECTION,
    CONTENT_LENGTH,
    HeaderName::from_static("keep-alive"),
    HeaderName::from_static("proxy-connection"),
    PROXY_AUTHENTICATE,
    PROXY_AUTHORIZATION,
    TE,
    TRAILER,
    TRANSFER_ENCODING,
    UPGRADE,
];

/// Remove the headers `Connection` names and a hop's own, but `Upgrade` if
/// `keep_upgrade`.
fn strip_hops(headers: &mut HeaderMap, keep_upgrade: bool) {
    strip_named(headers);
    for name in HOP_BY_HOP.iter().filter(|&n| !keep_upgrade || n != UPGRADE) {
        headers.remove(name);
    }
}

/// Remove each header whose name `is` says.
pub(crate) fn remove_if(headers: &mut HeaderMap, is: impl Fn(&HeaderName) -> bool) {
    let names: Vec<HeaderName> = headers.keys().filter(|n| is(n)).cloned().collect();
    for name in names {
        headers.remove(name);
    }
}

/// Remove the headers `Connection` names, but `Upgrade` (RFC 9110 7.6.1).
pub fn strip_named(headers: &mut HeaderMap) {
    let named: Vec<HeaderName> = headers
        .get_all(CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .filter_map(|name| HeaderName::from_bytes(name.trim().as_bytes()).ok())
        .filter(|name| name != UPGRADE)
        .collect();
    for name in named {
        headers.remove(name);
    }
}

/// A response head as sent to the client: without a hop's headers, but a
/// 101's `Upgrade` with its `Connection: upgrade` (RFC 9110 7.8), in the
/// client's version.
pub fn outgoing(head: &mut http::response::Parts) {
    let upgrade = head.status == StatusCode::SWITCHING_PROTOCOLS;
    strip_hops(&mut head.headers, upgrade);
    if upgrade {
        let option = HeaderValue::from_static("upgrade");
        head.headers.insert(CONNECTION, option);
    }
    head.version = http::Version::default();
}

/// `fault` as a text/plain response with its status and message, for what
/// no format can write.
pub fn plain(fault: &Fault) -> http::Response<Body> {
    let mut resp = http::Response::new(full(fault.message().to_owned()));
    *resp.status_mut() = status(fault);
    let text = HeaderValue::from_static("text/plain; charset=utf-8");
    resp.headers_mut().insert(CONTENT_TYPE, text);
    resp
}

/// Write `fault`'s status and retry, as `Retry-After` in whole seconds, into
/// `head`: a format's refusal.
pub fn error_head(head: &mut http::response::Parts, fault: &Fault) {
    head.status = status(fault);
    if let Some(wait) = fault.retry_after() {
        let secs = wait.as_secs() + u64::from(wait.subsec_nanos() > 0);
        head.headers.insert(RETRY_AFTER, HeaderValue::from(secs));
    }
}

/// A refusal in format `name`: if `name` read `fault`'s answer, an
/// `http::Response<Bytes>`, its head replaces `head` and its body comes
/// back; else [`error_head`] and `None`, for the format's own words.
pub fn answered(name: &str, fault: &Fault, head: &mut http::response::Parts) -> Option<Bytes> {
    match fault.answer::<http::Response<Bytes>>(name) {
        Some(answer) => {
            let (parts, body) = answer.clone().into_parts();
            *head = parts;
            Some(body)
        }
        None => {
            error_head(head, fault);
            None
        }
    }
}

/// A provider's `Retry-After` in seconds; an HTTP date counts as none.
pub fn retry_after(headers: &HeaderMap) -> Option<Duration> {
    let value = headers.get(RETRY_AFTER)?.to_str().ok()?;
    value.trim().parse().ok().map(Duration::from_secs)
}

/// Whether a response of `status` to `method` has no body (RFC 9110 9.3.2,
/// 15.3.5, 15.3.6, 15.4.5).
pub fn bodiless(method: &Method, status: StatusCode) -> bool {
    method == Method::HEAD || matches!(status.as_u16(), 204 | 205 | 304)
}

/// What [`dialect!`] names, not for use elsewhere.
#[doc(hidden)]
pub mod __private {
    pub use respan_gateway_core::futures::future::BoxFuture;
    pub use respan_gateway_core::{Dialed, Events, Extensions, Fault, Format, Half, Origin, Reply};
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&'static str, &'static str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.append(*name, HeaderValue::from_static(value));
        }
        map
    }

    // A client's `Connection` names only its own headers, never `Upgrade`.
    #[test]
    fn a_connection_header_names_only_its_own_headers() {
        let mut map = headers(&[
            ("connection", "x-hop, upgrade"),
            ("x-hop", "1"),
            ("upgrade", "websocket"),
            ("x-kept", "1"),
        ]);
        strip_named(&mut map);
        assert!(map.get("x-hop").is_none());
        assert_eq!(map["upgrade"], "websocket");
        assert_eq!(map["x-kept"], "1");
    }

    // A response loses its hop's headers, but a 101 keeps `Upgrade` and
    // gets `Connection: upgrade`.
    #[test]
    fn outgoing_strips_hop_by_hop_but_a_101s_upgrade() {
        for (status, upgrade) in [(200, false), (101, true)] {
            let (mut head, ()) = http::Response::new(()).into_parts();
            head.status = StatusCode::from_u16(status).unwrap_or_default();
            head.headers = headers(&[
                ("te", "trailers"),
                ("trailer", "x"),
                ("content-length", "3"),
                ("keep-alive", "timeout=5"),
                ("upgrade", "websocket"),
                ("x-kept", "1"),
            ]);
            head.version = http::Version::HTTP_10;
            outgoing(&mut head);
            for gone in ["te", "trailer", "content-length", "keep-alive"] {
                assert!(head.headers.get(gone).is_none(), "{status} {gone}");
            }
            assert_eq!(head.headers.contains_key("upgrade"), upgrade, "{status}");
            assert_eq!(head.headers.contains_key("connection"), upgrade, "{status}");
            assert_eq!(head.headers["x-kept"], "1");
            assert_eq!(head.version, http::Version::default());
        }
    }

    #[test]
    fn a_fault_writes_its_status_and_retry_after() {
        let (mut head, ()) = http::Response::new(()).into_parts();
        let fault = Fault::new("slow", "slow down")
            .with(Status(429))
            .with_retry_after(Duration::from_millis(1500));
        error_head(&mut head, &fault);
        assert_eq!(head.status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(head.headers[RETRY_AFTER], "2");
        assert_eq!(retry_after(&head.headers), Some(Duration::from_secs(2)));
    }

    #[test]
    fn a_code_has_a_default_status_and_a_hint_overrides_it() {
        let of = |f: Fault| status(&f).as_u16();
        for (fault, want) in [
            (Fault::bad_request("x"), 400),
            (Fault::not_found("x"), 404),
            (Fault::upstream("x"), 502),
            (Fault::truncated(), 502),
            (Fault::new(codes::ABANDONED, "x"), 499),
            (Fault::config("x"), 500),
            (Fault::internal("x"), 500),
            (Fault::new("plugin_own", "x"), 500),
        ] {
            let code = fault.code().to_owned();
            assert_eq!(of(fault.clone()), want, "{code}");
            assert_eq!(of(fault.with(Status(429))), 429, "{code}");
        }
    }

    #[test]
    fn a_number_that_is_no_status_is_a_500() {
        let fault = Fault::bad_request("x").with(Status(1000));
        assert_eq!(status(&fault), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn an_answer_replaces_the_head_only_in_its_format() {
        let mut answer = http::Response::new(Bytes::from_static(b"busy"));
        *answer.status_mut() = StatusCode::TOO_MANY_REQUESTS;
        let fault = Fault::new("busy", "busy")
            .with(Status(429))
            .with_answer("f", answer)
            .with_retry_after(Duration::from_secs(3));
        let (mut head, ()) = http::Response::new(()).into_parts();
        assert_eq!(
            answered("f", &fault, &mut head).as_deref(),
            Some(&b"busy"[..])
        );
        assert!(head.headers.get(RETRY_AFTER).is_none(), "as it came");
        let (mut head, ()) = http::Response::new(()).into_parts();
        assert_eq!(answered("g", &fault, &mut head), None);
        assert_eq!(
            (head.status, &head.headers[RETRY_AFTER]),
            (StatusCode::TOO_MANY_REQUESTS, &HeaderValue::from(3))
        );
    }

    #[test]
    fn head_and_204_205_304_have_no_body() {
        assert!(bodiless(&Method::HEAD, StatusCode::OK));
        for status in [204, 205, 304] {
            let status = StatusCode::from_u16(status).unwrap_or_default();
            assert!(bodiless(&Method::POST, status));
        }
        assert!(!bodiless(&Method::POST, StatusCode::OK));
    }

    #[test]
    fn plain_text_carries_the_status_and_message() {
        let resp = plain(&Fault::not_found("nothing here"));
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        assert_eq!(resp.headers()[CONTENT_TYPE], "text/plain; charset=utf-8");
    }
}
