//! HTTP's own rules at the edges: what a hop must not carry, what a body-less
//! answer is, and how a fault is written as a response. A fault's status and
//! error code are `respan-gateway-http-faults`'; how a path is matched is
//! [`path`](crate::path)'s.

use bytes::Bytes;
use http::header::{
    self, CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, HeaderName, PROXY_AUTHENTICATE,
    PROXY_AUTHORIZATION, RETRY_AFTER, TE, TRAILER, TRANSFER_ENCODING, UPGRADE,
};
use http::{
    HeaderMap, HeaderValue, Method, StatusCode, request::Parts as RequestHead,
    response::Parts as ResponseHead,
};
use respan_gateway_core::{Fault, RetryAfter};

use crate::Body;

use respan_gateway_http_faults::status;

/// The headers of one connection, never carried past it: a hop's own (RFC
/// 9110 7.6.1), the proxy's (11.7), and the body's framing
/// (`Content-Length`, `Transfer-Encoding`, `Trailer`), as the gateway
/// re-frames every body it passes on and delivers no trailers.
pub(crate) const CONNECTION_HEADERS: [HeaderName; 10] = [
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

/// Whether `name` is the connection's, never a transport's to send: one of
/// its own headers, `Expect` or `Host`.
pub fn is_connection_header(name: &HeaderName) -> bool {
    CONNECTION_HEADERS.contains(name) || name == header::EXPECT || name == header::HOST
}

/// The comma-separated tokens of every `name` header (RFC 9110 5.6.1).
pub fn header_tokens(headers: &HeaderMap, name: HeaderName) -> impl Iterator<Item = &str> {
    let values = headers.get_all(name).into_iter();
    values
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(str::trim)
}

/// HTTP's success, an [`ProviderIo`](crate::ProviderIo)'s unless it says another: 2xx, or a 101
/// to a call that asked to switch protocols (RFC 9110 15.2.2).
pub fn success(answer: &ResponseHead, call: &RequestHead) -> bool {
    match answer.status {
        StatusCode::SWITCHING_PROTOCOLS => call.headers.contains_key(UPGRADE),
        status => status.is_success(),
    }
}

/// A format's error writer ([`ClientIo::bind`](crate::ClientIo::bind)) for a protocol without an
/// error envelope: the fault's message as text/plain.
pub(crate) fn plain(
    fault: &Fault,
    _request: &http::Request<Bytes>,
    head: &mut ResponseHead,
) -> Bytes {
    head.headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    Bytes::from(fault.message().to_owned())
}

/// Remove the headers `Connection` names and a hop's own, but `Upgrade` if
/// `keep_upgrade`.
pub(crate) fn strip_hops(headers: &mut HeaderMap, keep_upgrade: bool) {
    strip_named(headers);
    for name in CONNECTION_HEADERS
        .iter()
        .filter(|&n| !keep_upgrade || n != UPGRADE)
    {
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
pub(crate) fn strip_named(headers: &mut HeaderMap) {
    let named: Vec<HeaderName> = header_tokens(headers, CONNECTION)
        .filter_map(|name| HeaderName::from_bytes(name.as_bytes()).ok())
        .filter(|name| name != UPGRADE)
        .collect();
    for name in named {
        headers.remove(name);
    }
}

/// A response head as sent to the client: without a hop's headers, but a
/// 101's `Upgrade` with its `Connection: upgrade` (RFC 9110 7.8), in the
/// client's version.
pub(crate) fn outgoing(head: &mut http::response::Parts) {
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
pub(crate) fn plain_response(fault: &Fault) -> http::Response<Body> {
    let mut resp = http::Response::new(Body::full(fault.message().to_owned()));
    *resp.status_mut() = status(fault);
    let text = HeaderValue::from_static("text/plain; charset=utf-8");
    resp.headers_mut().insert(CONTENT_TYPE, text);
    resp
}

/// Write `fault`'s status and retry, as `Retry-After` in whole seconds, into
/// `head`: a format's refusal. Fractional seconds round up, saturating at `u64::MAX`.
pub(crate) fn error_head(head: &mut http::response::Parts, fault: &Fault) {
    head.status = status(fault);
    if let Some(&RetryAfter(wait)) = fault.get() {
        let secs = wait
            .as_secs()
            .saturating_add(u64::from(wait.subsec_nanos() > 0));
        head.headers.insert(RETRY_AFTER, HeaderValue::from(secs));
    }
}

/// Whether a response of `status` to `method` has no body (RFC 9110 9.3.2,
/// 15.3.5, 15.3.6, 15.4.5).
pub fn bodiless(method: &Method, status: StatusCode) -> bool {
    method == Method::HEAD || matches!(status.as_u16(), 204 | 205 | 304)
}

#[cfg(test)]
mod tests {
    use super::*;
    use respan_gateway_http_faults::{Status, provider_fault};
    use std::time::Duration;

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
    fn a_fault_writes_its_status_and_retry_after_and_an_answer_reads_them() {
        let (mut head, ()) = http::Response::new(()).into_parts();
        let fault = Fault::new("slow down")
            .with(Status(StatusCode::TOO_MANY_REQUESTS))
            .with(RetryAfter(Duration::from_millis(1500)));
        error_head(&mut head, &fault);
        assert_eq!(head.status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(head.headers[RETRY_AFTER], "2");
        let read = provider_fault(http::Response::from_parts(head, Bytes::new()));
        assert_eq!(read.get(), Some(&RetryAfter(Duration::from_secs(2))));
        assert_eq!(read.get(), Some(&Status(StatusCode::TOO_MANY_REQUESTS)));
        assert_eq!(read.message(), "the provider returned 429, with no body");
    }

    #[test]
    fn retry_after_rounding_handles_maximum_duration() {
        for (wait, seconds) in [
            (Duration::ZERO, 0),
            (Duration::from_nanos(1), 1),
            (Duration::from_secs(1), 1),
            (Duration::from_millis(1500), 2),
            (Duration::new(u64::MAX - 1, 1), u64::MAX),
            (Duration::from_secs(u64::MAX), u64::MAX),
            (Duration::MAX, u64::MAX),
        ] {
            let fault = Fault::upstream("retry later").with(RetryAfter(wait));
            let mut head = http::Response::new(()).into_parts().0;
            error_head(&mut head, &fault);
            assert_eq!(head.headers[RETRY_AFTER], seconds.to_string(), "{wait:?}");
            let read = provider_fault(http::Response::from_parts(head, Bytes::new()));
            assert_eq!(read.get(), Some(&RetryAfter(Duration::from_secs(seconds))));
        }
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
        let resp = plain_response(&Fault::not_found("nothing here"));
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        assert_eq!(resp.headers()[CONTENT_TYPE], "text/plain; charset=utf-8");
    }
}
