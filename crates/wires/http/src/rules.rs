//! HTTP's own rules: statuses and error codes, what a hop must not carry,
//! and how a path or a header is read.

use std::time::Duration;

use bytes::Bytes;
use http::header::{
    CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, HeaderName, PROXY_AUTHENTICATE, PROXY_AUTHORIZATION,
    RETRY_AFTER, TE, TRAILER, TRANSFER_ENCODING, UPGRADE,
};
use http::{
    HeaderMap, HeaderValue, Method, StatusCode, request::Parts as RequestHead,
    response::Parts as ResponseHead,
};
use respan_gateway_core::{
    Abandoned, BadRequest, Config, ErrorCode, Fault, Internal, NotFound, RetryAfter, Truncated,
    Upstream,
};

use crate::{Body, ProviderError};

/// `s` with its `%XX` escapes decoded, a bad UTF-8 sequence replaced: how a
/// path or a parameter's name is compared, so a spelling cannot dodge a match.
pub fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = |b: u8| char::from(b).to_digit(16);
        let escaped = match bytes[i..] {
            [b'%', hi, lo, ..] => hex(hi).zip(hex(lo)).map(|(h, l)| (h * 16 + l) as u8),
            _ => None,
        };
        out.push(escaped.unwrap_or(bytes[i]));
        i += if escaped.is_some() { 3 } else { 1 };
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// HTTP's success, an [`ProviderIo`]'s unless it says another: 2xx, or a 101
/// to a call that asked to switch protocols (RFC 9110 15.2.2).
pub(crate) fn success(answer: &ResponseHead, call: &RequestHead) -> bool {
    match answer.status {
        StatusCode::SWITCHING_PROTOCOLS => call.headers.contains_key(UPGRADE),
        status => status.is_success(),
    }
}

/// A format's error writer ([`ClientIo::bind`]) for a protocol without an
/// error envelope: the fault's message as text/plain.
pub fn plain(fault: &Fault, _request: &http::Request<Bytes>, head: &mut ResponseHead) -> Bytes {
    head.headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    Bytes::from(fault.message().to_owned())
}

/// A format's error reader ([`ProviderIo::bind`]) for a protocol whose error
/// bodies it does not read: the fault of a provider's `answer` in `format`
/// that is no success. It is [`Upstream`], says the answer's status, with
/// that [`Status`] if an error's, else a 502, and its `Retry-After` in whole
/// seconds ([`RetryAfter`]; an HTTP date counts as none). Unless
/// informational (an unasked 101), it carries the answer as it came
/// ([`ProviderError`]), for a plugin to inspect or a relay to write.
pub fn provider_fault(format: &str, answer: http::Response<Bytes>) -> Fault {
    let status = answer.status();
    let to = match status.is_client_error() || status.is_server_error() {
        true => status,
        false => StatusCode::BAD_GATEWAY,
    };
    let fault = Upstream::fault(format!("the provider returned {}", status.as_u16()));
    let fault = fault.with(Status(to));
    if status.is_informational() {
        return fault;
    }
    let wait = answer
        .headers()
        .get(RETRY_AFTER)
        .and_then(|v| v.to_str().ok());
    let fault = match wait.and_then(|v| v.trim().parse().ok()) {
        Some(secs) => fault.with(RetryAfter(Duration::from_secs(secs))),
        None => fault,
    };
    fault.with(ProviderError::new(format, answer))
}

/// A message a plugin or format built wrong: a bug, a 500.
pub fn built_wrong(e: http::Error) -> Fault {
    Fault::internal("an HTTP request or response was built wrong").with_source(e)
}

/// The HTTP status a fault asks for, as a fact ([`Fault::with`]): a plugin's
/// `Fault::new(msg).with(Status(StatusCode::TOO_MANY_REQUESTS))`, a
/// provider's answer's. The kernel names no numbers; this is where they get
/// them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Status(pub StatusCode);

/// The status of `fault` as HTTP's: its [`Status`] fact, else its kind's
/// default (a 400 for [`BadRequest`], a 404 for [`NotFound`], a 502 for
/// [`Upstream`] and [`Truncated`], a 499 for [`Abandoned`], a 500 for
/// [`Config`], [`Internal`] and a fault of none of these).
pub fn status(fault: &Fault) -> StatusCode {
    if let Some(Status(status)) = fault.get() {
        return *status;
    }
    let number = match () {
        _ if fault.is::<NotFound>() => 404,
        _ if fault.is::<BadRequest>() => 400,
        _ if fault.is::<Upstream>() || fault.is::<Truncated>() => 502,
        _ if fault.is::<Abandoned>() => 499,
        _ => 500,
    };
    StatusCode::from_u16(number).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
}

/// The code a format writes in a JSON error body for `fault`: its
/// [`ErrorCode`] fact, else a name for its kind (`not_found`, `bad_request`,
/// `upstream`, `truncated`, `abandoned`, `config`, `internal`), else `error`.
pub fn error_code(fault: &Fault) -> &str {
    if let Some(ErrorCode(code)) = fault.get() {
        return code;
    }
    match () {
        _ if fault.is::<NotFound>() => "not_found",
        _ if fault.is::<BadRequest>() => "bad_request",
        _ if fault.is::<Upstream>() => "upstream",
        _ if fault.is::<Truncated>() => "truncated",
        _ if fault.is::<Abandoned>() => "abandoned",
        _ if fault.is::<Config>() => "config",
        _ if fault.is::<Internal>() => "internal",
        _ => "error",
    }
}

/// The statuses RFC 9110 names as transient: a later attempt may succeed
/// (408, 425, 429, 500, 502, 503, 504).
const TRANSIENT: [StatusCode; 7] = [
    StatusCode::REQUEST_TIMEOUT,
    StatusCode::TOO_EARLY,
    StatusCode::TOO_MANY_REQUESTS,
    StatusCode::INTERNAL_SERVER_ERROR,
    StatusCode::BAD_GATEWAY,
    StatusCode::SERVICE_UNAVAILABLE,
    StatusCode::GATEWAY_TIMEOUT,
];

/// HTTP retry eligibility for a failed `first_success` attempt. Before a
/// response opened, only an [`Upstream`] fault is retryable: the provider
/// could not be reached. After opening, a fault whose status is transient
/// is: its [`Status`] fact, or the 502 of an [`Upstream`] or [`Truncated`]
/// fault, which say the provider failed. A fault with neither, such as a
/// plugin's own, is not, nor is an [`Internal`], [`Config`] or
/// [`Abandoned`] one, whatever its status: a bug, a setting and a client
/// that left are the same on a second attempt.
/// `response_opened` means the call returned `Ok(Response)` before the
/// caller's response-selection criterion ran, not that bytes arrived.
pub fn retryable(fault: &Fault, response_opened: bool) -> bool {
    if !response_opened {
        return fault.is::<Upstream>();
    }
    if fault.is::<Internal>() || fault.is::<Config>() || fault.is::<Abandoned>() {
        return false;
    }
    match fault.get::<Status>() {
        Some(Status(status)) => TRANSIENT.contains(status),
        None => fault.is::<Upstream>() || fault.is::<Truncated>(),
    }
}

/// A hop's own headers: the connection's (RFC 9110 7.6.1), the proxy's
/// (11.7), `Trailer`, as trailers are not delivered, and the length.
pub(crate) const HOP_BY_HOP: [HeaderName; 10] = [
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
pub(crate) fn strip_hops(headers: &mut HeaderMap, keep_upgrade: bool) {
    strip_named(headers);
    for name in HOP_BY_HOP.iter().filter(|&n| !keep_upgrade || n != UPGRADE) {
        headers.remove(name);
    }
}

/// The comma-separated tokens of every `name` header (RFC 9110 5.6.1).
pub fn header_tokens(headers: &HeaderMap, name: HeaderName) -> impl Iterator<Item = &str> {
    let values = headers.get_all(name).into_iter();
    values
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(str::trim)
}

/// Whether `bytes` are a token (RFC 9110 5.6.2), as a subprotocol is.
pub fn is_token(bytes: &[u8]) -> bool {
    !bytes.is_empty()
        && (bytes.iter()).all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(b))
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
        let read = provider_fault("f", http::Response::from_parts(head, Bytes::new()));
        assert_eq!(read.get(), Some(&RetryAfter(Duration::from_secs(2))));
        assert_eq!(read.get(), Some(&Status(StatusCode::TOO_MANY_REQUESTS)));
        assert_eq!(read.message(), "the provider returned 429");
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
            let fault = Upstream::fault("retry later").with(RetryAfter(wait));
            let mut head = http::Response::new(()).into_parts().0;
            error_head(&mut head, &fault);
            assert_eq!(head.headers[RETRY_AFTER], seconds.to_string(), "{wait:?}");
            let read = provider_fault("f", http::Response::from_parts(head, Bytes::new()));
            assert_eq!(read.get(), Some(&RetryAfter(Duration::from_secs(seconds))));
        }
    }

    // An answer that is no error says a 502; an unasked 101 carries no
    // answer, as it was never relayed.
    #[test]
    fn a_refused_success_is_a_502_and_an_unasked_101_carries_no_answer() {
        for (status, carried) in [(200, true), (101, false)] {
            let mut answer = http::Response::new(Bytes::from_static(b"x"));
            *answer.status_mut() = StatusCode::from_u16(status).unwrap_or_default();
            let read = provider_fault("f", answer);
            assert_eq!(read.get(), Some(&Status(StatusCode::BAD_GATEWAY)));
            assert_eq!(read.get::<ProviderError>().is_some(), carried, "{status}");
        }
    }

    #[test]
    fn a_kind_has_a_default_status_and_a_status_fact_overrides_it() {
        let of = |f: Fault| status(&f).as_u16();
        for (fault, want) in [
            (BadRequest::fault("x"), 400),
            (Fault::not_found("x"), 404),
            (Upstream::fault("x"), 502),
            (Truncated::fault(), 502),
            (Fault::abandoned(), 499),
            (Fault::config("x"), 500),
            (Fault::internal("x"), 500),
            (Fault::new("x"), 500),
        ] {
            let code = error_code(&fault).to_owned();
            assert_eq!(of(fault.clone()), want, "{code}");
            assert_eq!(
                of(fault.with(Status(StatusCode::TOO_MANY_REQUESTS))),
                429,
                "{code}"
            );
        }
    }

    #[test]
    fn an_error_code_is_the_fact_else_a_name_for_the_kind() {
        for (fault, want) in [
            (BadRequest::fault("x"), "bad_request"),
            (Fault::not_found("x"), "not_found"),
            (Upstream::fault("x"), "upstream"),
            (Truncated::fault(), "truncated"),
            (Fault::abandoned(), "abandoned"),
            (Fault::config("x"), "config"),
            (Fault::internal("x"), "internal"),
            (Fault::new("x"), "error"),
            (
                Upstream::fault("x").with(ErrorCode("over_limit".into())),
                "over_limit",
            ),
        ] {
            assert_eq!(error_code(&fault), want);
        }
    }

    #[test]
    fn retryable_respects_opening_stage_kind_and_transient_status() {
        let hinted = |fault: Fault, number| {
            fault.with(Status(StatusCode::from_u16(number).unwrap_or_default()))
        };
        let custom = || Fault::new("x").with(ErrorCode("custom".into()));
        for (fault, before, after) in [
            (Upstream::fault("x"), true, true),
            (Truncated::fault(), false, true),
            (hinted(Upstream::fault("x"), 400), true, false),
            (hinted(Fault::config("x"), 503), false, false),
            (hinted(Fault::internal("x"), 429), false, false),
            (hinted(Fault::abandoned(), 502), false, false),
            (hinted(BadRequest::fault("x"), 503), false, true),
            (hinted(custom(), 400), false, false),
            // A plugin's own fault, with no status, is not retried.
            (custom(), false, false),
            (Fault::new("x"), false, false),
        ] {
            assert_eq!(retryable(&fault, false), before, "before: {fault:?}");
            assert_eq!(retryable(&fault, true), after, "after: {fault:?}");
        }
        for number in [408, 425, 429, 500, 502, 503, 504] {
            assert!(retryable(&hinted(custom(), number), true), "{number}");
        }
        for number in [
            200, 400, 401, 403, 404, 409, 422, 499, 501, 505, 528, 529, 530,
        ] {
            assert!(!retryable(&hinted(custom(), number), true), "{number}");
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
