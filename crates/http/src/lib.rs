#![warn(missing_docs)]
//! HTTP at the gateway's edges. The kernel carries `http` heads as an
//! envelope it never reads; the rules of HTTP live here, at the three
//! places that own them:
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

mod credential;
mod transport;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::FutureExt;
use futures::future::BoxFuture;
use http::header::{
    CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, HeaderName, PROXY_AUTHENTICATE, PROXY_AUTHORIZATION,
    RETRY_AFTER, TE, TRAILER, TRANSFER_ENCODING, UPGRADE,
};
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use http_body_util::{BodyExt, Full};
use respan_gateway_core::{Body, Fault, Gateway, Outcome};
use respan_gateway_core::{bytes, futures, http, http_body, http_body_util};

pub use credential::Credential;
pub use transport::{
    CallHeaders, credentials, is_connection_header, proxy_call, strip_client_origin_headers,
    strip_origin_headers,
};

/// The client's request head as the adapter handed it over, without its
/// credentials ([`Credential`]) or extensions: a read-only view for
/// plugins of any kind (`cx.extensions().get::<Asked>()`).
#[derive(Debug, Clone)]
pub struct Asked(pub Arc<http::request::Parts>);

impl std::ops::Deref for Asked {
    type Target = http::request::Parts;

    fn deref(&self) -> &http::request::Parts {
        &self.0
    }
}

/// [`handle`] `req`, streaming its inputs if it asks to switch protocols
/// (`Upgrade`, RFC 9110 7.8): the adapter for an embedder, or a test, that
/// performs every switch it is asked for.
pub async fn serve<B>(
    gateway: &Gateway,
    req: http::Request<B>,
) -> (http::Response<Body>, BoxFuture<'static, Outcome>)
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Fault>,
{
    let streaming = req.headers().contains_key(UPGRADE);
    handle(gateway, req, streaming).await
}

/// Hand `req` to `gateway` as an HTTP adapter does, its inputs streaming
/// if `streaming`. Before: the headers its `Connection` names and a hop's
/// own go, but `Upgrade` (RFC 9110 7.6.1); its credentials go into a [`Credential`]
/// extension; an [`Asked`] view of what is left joins them. After: a
/// failure no format can write goes as text/plain with its status, a 101
/// to a request not `streaming` as a text/plain 500, and the response
/// loses what a hop must not carry ([`outgoing`]). A response with no body
/// ([`bodiless`]) goes with none, the gateway's read to its end apart, so
/// the request ends as delivered. The adapter bounds a whole body and
/// removes an `Upgrade` it will not perform. Call it on a tokio runtime.
pub async fn handle<B>(
    gateway: &Gateway,
    req: http::Request<B>,
    streaming: bool,
) -> (http::Response<Body>, BoxFuture<'static, Outcome>)
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Fault>,
{
    let (mut head, body) = req.into_parts();
    let method = head.method.clone();
    strip_named(&mut head.headers);
    for name in HOP_BY_HOP.iter().filter(|&n| n != UPGRADE) {
        head.headers.remove(name);
    }
    if let Some(credential) = Credential::lift(&mut head) {
        head.extensions.insert(credential);
    }
    let mut view = head.clone();
    view.extensions = http::Extensions::new();
    head.extensions.insert(Asked(Arc::new(view)));
    let req = http::Request::from_parts(head, body);
    let (resp, outcome) = gateway.handle(req, streaming).await;
    let resp = match resp {
        Err(fault) => plain(&fault),
        Ok(resp) if resp.status() == StatusCode::SWITCHING_PROTOCOLS && !streaming => {
            // RFC 9110 15.2.2: a 101 only to a request that asked.
            let asked = "a 101 to a request that did not ask to switch protocols";
            plain(&Fault::internal(asked))
        }
        Ok(resp) => {
            let (mut head, mut body) = resp.into_parts();
            outgoing(&mut head);
            if bodiless(&method, head.status) {
                tokio::spawn(async move { while body.frame().await.is_some() {} });
                body = Full::new(Bytes::new()).map_err(Fault::from).boxed_unsync();
            }
            http::Response::from_parts(head, body)
        }
    };
    (resp, outcome.boxed())
}

/// Connection headers (RFC 9110 7.6.1), the proxy's own (11.7), `Trailer`,
/// as trailers are not delivered, and the length, which reframing changes.
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

/// Whether `name` is a hop's own header ([`HOP_BY_HOP`]).
fn is_hop_by_hop(name: &HeaderName) -> bool {
    HOP_BY_HOP.contains(name)
}

/// Remove the headers `Connection` names, but `Upgrade` (RFC 9110 7.6.1),
/// as an adapter does on arrival.
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

/// A response head as sent to the client: without the headers its
/// `Connection` names or a hop's own, but a 101's `Upgrade` with the
/// `Connection: upgrade` it needs (RFC 9110 7.8), in the client's version.
pub fn outgoing(head: &mut http::response::Parts) {
    strip_named(&mut head.headers);
    let upgrade = head.status == StatusCode::SWITCHING_PROTOCOLS;
    for name in HOP_BY_HOP.iter().filter(|&n| !upgrade || n != UPGRADE) {
        head.headers.remove(name);
    }
    if upgrade {
        let option = HeaderValue::from_static("upgrade");
        head.headers.insert(CONNECTION, option);
    }
    head.version = http::Version::default();
}

/// `fault` as a text/plain response with its status and message, for what
/// no format can write.
pub fn plain(fault: &Fault) -> http::Response<Body> {
    let body = Full::new(Bytes::from(fault.message().to_owned()));
    let mut resp = http::Response::new(body.map_err(Fault::from).boxed_unsync());
    *resp.status_mut() = fault.status();
    let text = HeaderValue::from_static("text/plain; charset=utf-8");
    resp.headers_mut().insert(CONTENT_TYPE, text);
    resp
}

/// Write `fault`'s status and its [`retry`](Fault::retry_after), as
/// `Retry-After` in whole seconds, into `head`: a format's refusal.
pub fn error_head(head: &mut http::response::Parts, fault: &Fault) {
    head.status = fault.status();
    if let Some(wait) = fault.retry_after() {
        let secs = wait.as_secs() + u64::from(wait.subsec_nanos() > 0);
        head.headers.insert(RETRY_AFTER, HeaderValue::from(secs));
    }
}

/// A refusal in format `name`: `fault`'s answer if `name` read it
/// ([`Fault::with_answer`]), as an `http::Response<Bytes>`, whose head
/// replaces `head` and whose body comes back; else `fault`'s status and
/// retry written into `head` ([`error_head`]) and `None`, for the format
/// to write the fault in its own words.
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

/// Whether a response of `status` to a request of `method` has no body
/// (RFC 9110 9.3.2, 15.3.5, 15.3.6, 15.4.5): a format neither reads nor
/// writes one.
pub fn bodiless(method: &Method, status: StatusCode) -> bool {
    method == Method::HEAD || matches!(status.as_u16(), 204 | 205 | 304)
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
        let fault = Fault::new(StatusCode::TOO_MANY_REQUESTS, "slow", "slow down")
            .with_retry_after(Duration::from_millis(1500));
        error_head(&mut head, &fault);
        assert_eq!(head.status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(head.headers[RETRY_AFTER], "2");
        assert_eq!(retry_after(&head.headers), Some(Duration::from_secs(2)));
    }

    #[test]
    fn an_answer_replaces_the_head_only_in_its_format() {
        let mut answer = http::Response::new(Bytes::from_static(b"busy"));
        *answer.status_mut() = StatusCode::TOO_MANY_REQUESTS;
        let fault = Fault::new(StatusCode::TOO_MANY_REQUESTS, "busy", "busy")
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
