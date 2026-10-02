#![warn(missing_docs)]
#![doc = include_str!("../README.md")]
//! HTTP at the gateway's edges: its formats and its rules.
//!
//! Formats implement the kernel's `Format` directly over [`Incoming`] and
//! [`Outgoing`]. Native HTTP messages meet standard event streams and sinks;
//! [`Encoder`] supplies synchronous stateful framing where needed.
//!
//! Its rules live at the three places that own them:
//!
//! - **The adapter** ([`serve`], [`handle`]) strips the connection's
//!   headers from a request, lifts the client's credentials into a
//!   [`Credential`] extension, publishes an [`Asked`] view of the head,
//!   writes what no format can in plain text, and strips what a hop must
//!   not carry from the response ([`outgoing`]).
//! - **An outbound transport** can bound its provider head and polled body
//!   frame gaps with [`watchdog`]. HTTP retry classification is explicitly
//!   selected by a routing caller through [`retryable`]; it is not built into
//!   routing's timing-only backoff.
//! - **A transport** strips what a proxy must not pass on from a call
//!   ([`proxy_call`], [`strip_client_origin_headers`]), decides whose keys
//!   reach the provider ([`credentials`], [`CallHeaders`]), and strips a
//!   provider origin's own headers from its response
//!   ([`strip_origin_headers`]).
//! - **An HTTP format** writes a fault's status and `Retry-After`
//!   ([`error_head`], [`answered`]), reads a provider's
//!   ([`retry_after`]), and writes no body where HTTP has none
//!   ([`bodiless`]).

extern crate self as respan_gateway_http;

/// Shared framing, faults and opaque relay helpers for format authors.
pub mod formats;
/// Explicit raw relay of a client's request, head and errors, as a plugin.
pub mod relay;

mod body;
mod codec;
mod credential;
mod driver;
#[cfg(feature = "reqwest")]
mod reqwest_backend;
mod transport;
mod watchdog;

use std::borrow::Cow;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures::FutureExt;
use futures::future::{BoxFuture, ready};
use http::header::{
    CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, HeaderName, PROXY_AUTHENTICATE, PROXY_AUTHORIZATION,
    RETRY_AFTER, SEC_WEBSOCKET_PROTOCOL, TE, TRAILER, TRANSFER_ENCODING, UPGRADE,
};
use http::{
    HeaderMap, HeaderValue, Method, StatusCode, request::Parts as RequestHead,
    response::Parts as ResponseHead,
};
use http_body_util::BodyExt;
use respan_gateway_sdk::futures;
use respan_gateway_sdk::{
    Abandoned, BadRequest, Config, ErrorCode, Extensions, Fault, Gateway, Internal, NotFound,
    RetryAfter, Truncated, Upstream,
};

#[cfg(feature = "reqwest")]
pub use reqwest;
/// The crates HTTP formats and transports use, at these versions.
pub use {bytes, http, http_body, http_body_util};

pub use body::Body;
pub use codec::{Encoder, encode_all, messages, request};
mod wire;
pub use credential::Credential;
pub use wire::{ClientHead, ProviderError, ProviderHead, RawReply, SendRaw, Wire};

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
pub use driver::{Incoming, Outgoing, Reply, drive};
#[cfg(feature = "reqwest")]
pub use reqwest_backend::HttpClient;
pub use transport::{
    CallHeaders, Completion, Frames, connect, credentials, is_connection_header, proxy_call,
    strip_client_origin_headers, strip_origin_headers,
};
pub use watchdog::watchdog;

/// Default HTTP success: 2xx, or a requested protocol switch.
pub fn success(answer: &ResponseHead, call: &RequestHead) -> bool {
    match answer.status {
        StatusCode::SWITCHING_PROTOCOLS => call.headers.contains_key(UPGRADE),
        status => status.is_success(),
    }
}

/// Plain protocol refusal text, for formats without a custom error envelope.
pub fn plain_refusal(
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

/// A message a plugin or format built wrong: a bug, a 500.
pub fn built_wrong(e: http::Error) -> Fault {
    Fault::internal("an HTTP request or response was built wrong").with_source(e)
}

/// The HTTP status a fault asks for, as a fact ([`Fault::with`]): a plugin's
/// `Fault::new(msg).with(Status(429))`, a provider's answer's.
/// The kernel names no numbers; this is where they get them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Status(pub u16);

/// The status of `fault` as HTTP's: its [`Status`] fact, else its kind's
/// default (a 400 for [`BadRequest`], a 404 for [`NotFound`], a 502 for
/// [`Upstream`] and [`Truncated`], a 499 for [`Abandoned`], a 500 for
/// [`Config`], [`Internal`] and a fault of none of these), a 500 for a
/// number that is no status.
pub fn status(fault: &Fault) -> StatusCode {
    let number = match fault.get::<Status>() {
        Some(Status(number)) => *number,
        None if fault.is::<NotFound>() => 404,
        None if fault.is::<BadRequest>() => 400,
        None if fault.is::<Upstream>() || fault.is::<Truncated>() => 502,
        None if fault.is::<Abandoned>() => 499,
        None => 500,
    };
    StatusCode::from_u16(number).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
}

/// The code a format writes in a JSON error body for `fault`: its
/// [`ErrorCode`] fact, else a name for its kind (`not_found`, `bad_request`,
/// `upstream`, `truncated`, `abandoned`, `config`, `internal`), else `error`.
pub fn error_code(fault: &Fault) -> Cow<'_, str> {
    if let Some(ErrorCode(code)) = fault.get() {
        return Cow::Borrowed(code);
    }
    let name = match () {
        _ if fault.is::<NotFound>() => "not_found",
        _ if fault.is::<BadRequest>() => "bad_request",
        _ if fault.is::<Upstream>() => "upstream",
        _ if fault.is::<Truncated>() => "truncated",
        _ if fault.is::<Abandoned>() => "abandoned",
        _ if fault.is::<Config>() => "config",
        _ if fault.is::<Internal>() => "internal",
        _ => "error",
    };
    Cow::Borrowed(name)
}

/// HTTP retry eligibility for a failed `first_success` attempt. Before a
/// response opened, only an [`Upstream`] fault is retryable, regardless of
/// any status fact. After opening, [`Internal`], [`Config`] and [`Abandoned`]
/// are never retryable, even with a retryable status fact; otherwise
/// statuses 408, 425, 429, 500, 502, 503, 504 and 529 are retryable (using
/// [`status`] for the fault's explicit fact or default).
/// `response_opened` means the call returned `Ok(Response)` before the
/// caller's response-selection criterion ran, not that bytes arrived.
pub fn retryable(fault: &Fault, response_opened: bool) -> bool {
    if !response_opened {
        return fault.is::<Upstream>();
    }
    if fault.is::<Internal>() || fault.is::<Config>() || fault.is::<Abandoned>() {
        return false;
    }
    [408, 425, 429, 500, 502, 503, 504, 529].contains(&status(fault).as_u16())
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
pub fn serve<B>(
    gateway: &Gateway,
    req: http::Request<B>,
) -> impl Future<Output = (http::Response<Body>, BoxFuture<'static, Result<(), Fault>>)> + Send + 'static
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Fault>,
{
    let streaming = req.headers().contains_key(UPGRADE);
    handle(gateway, req, streaming)
}

/// Hand `req` to `gateway` as an HTTP adapter does, streaming if
/// `streaming`: as an [`Incoming`] handle, to [`Gateway::handle`]. Before:
/// the hop's headers go but `Upgrade` (RFC 9110 7.6.1), credentials into a
/// [`Credential`], and an [`Asked`] view joins them. After: what no format
/// can write, or a 101 to a request not `streaming`, goes as text/plain
/// ([`plain`]); the response loses a hop's headers ([`outgoing`]); a
/// [`bodiless`] one goes with none, the gateway's body read apart. The
/// caller bounds a whole body and removes an `Upgrade` it will not perform.
///
/// Like [`Gateway::handle`], the request counts for [`Gateway::idle`] from
/// this call, not from the future's first poll, and the future borrows
/// nothing: a job handed over from an end hook calls this where it starts
/// and awaits the future in its own task. Await it on a tokio runtime.
pub fn handle<B>(
    gateway: &Gateway,
    req: http::Request<B>,
    streaming: bool,
) -> impl Future<Output = (http::Response<Body>, BoxFuture<'static, Result<(), Fault>>)> + Send + 'static
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Fault>,
{
    let (Arrived { head, body, to }, mut extensions) = arrive(req);
    let (wire, answer) = Incoming::accept(http::Request::from_parts(head, body), streaming);
    extensions.insert(wire.wire());
    extensions.insert(wire.client_head());
    let handled = gateway.handle(wire, extensions);
    async move {
        let (resp, ended) = match driver::drive(handled.boxed(), answer).await {
            Ok((resp, ended)) => (Ok(resp), ended),
            Err(fault) => (Err(fault.clone()), ready(Err(fault)).boxed()),
        };
        (depart(&to, streaming, resp), ended)
    }
}

/// Marks the plain text of a refusal no codec could write, which goes as
/// it is: HTTP's rules for a body do not apply to it.
#[derive(Clone, Copy)]
pub(crate) struct Unwritten;

/// A request as the adapter hands it on, and what its answer needs of it.
pub(crate) struct Arrived {
    pub(crate) head: RequestHead,
    pub(crate) body: Body,
    pub(crate) to: Asking,
}

/// What of a request its answer's rules need.
pub(crate) struct Asking {
    pub(crate) method: Method,
    pub(crate) path: String,
    /// The subprotocol to answer a 101 with if the provider chose none.
    pub(crate) protocol: Option<HeaderValue>,
}

/// A request as it arrives at the adapter ([`handle`]): the hop's headers
/// go but `Upgrade`, credentials into a [`Credential`], and an [`Asked`]
/// view joins them.
pub(crate) fn arrive<B>(req: http::Request<B>) -> (Arrived, Extensions)
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Fault>,
{
    let (mut head, body) = req.into_parts();
    let mut extensions = head.extensions.remove::<Extensions>().unwrap_or_default();
    head.extensions = http::Extensions::new();
    let body = Body::stream(body.map_err(Into::into).into_data_stream());
    let method = head.method.clone();
    let path = head.uri.path().to_owned();
    strip_hops(&mut head.headers, true);
    let offered = head.headers.contains_key(SEC_WEBSOCKET_PROTOCOL);
    let mut protocol = None;
    if let Some(credential) = Credential::lift(&mut head) {
        // A client that offered only keys still needs a subprotocol echoed
        // (browsers fail a handshake that echoes none), so the first of them.
        if offered && !head.headers.contains_key(SEC_WEBSOCKET_PROTOCOL) {
            let keys = credential.headers().get(SEC_WEBSOCKET_PROTOCOL);
            let first = keys.and_then(|v| v.to_str().ok()?.split(',').next());
            protocol = first.and_then(|e| HeaderValue::from_str(e.trim()).ok());
        }
        extensions.insert(credential);
    }
    extensions.insert(Asked(Arc::new(head.clone())));
    let arrived = Arrived {
        head,
        body,
        to: Asking {
            method,
            path,
            protocol,
        },
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
        Err(fault) if fault.is::<NotFound>() => {
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
            let length = head.headers.get(CONTENT_LENGTH).cloned();
            outgoing(&mut head);
            if head.status == StatusCode::SWITCHING_PROTOCOLS
                && !head.headers.contains_key(SEC_WEBSOCKET_PROTOCOL)
                && let Some(protocol) = &to.protocol
            {
                head.headers
                    .insert(SEC_WEBSOCKET_PROTOCOL, protocol.clone());
            }
            if bodiless(&to.method, head.status) {
                // A HEAD's or a 304's `Content-Length` is the provider's
                // for the body it would send (RFC 9110 8.6); a 204 has none.
                let kept = to.method == Method::HEAD || head.status == StatusCode::NOT_MODIFIED;
                if let Some(length) = length.filter(|_| kept) {
                    head.headers.insert(CONTENT_LENGTH, length);
                }
                // A hop drops a bodiless answer's body once its head is out, so
                // that is when the client leaves. Read what is ready now, so a
                // refusal's end is told, then drop the rest: the session ends
                // with its client (rule 4), never when a provider's stream
                // does.
                drain_ready(&mut body);
                body = Body::default();
            }
            http::Response::from_parts(head, body)
        }
    }
}

/// Read what `body` has ready now: until it ends, waits on something, or
/// the task's budget is spent. A bodiless answer's frames are content it
/// cannot send, read only to reach its end, so a ready body is read through
/// and its end told (rule 4: dropped outputs end their call as its end, if
/// ready, says, else `abandoned`). An output says `Pending` between the
/// frames it sends apart (rule 5) but wakes itself at once, as a yield
/// does, so a `Pending` that was woken during its poll is ready and one
/// that was not is waiting: the gateway's own body cannot tell a source
/// that yields between frames from one that has them all, so neither can
/// this. What a task may do in one poll is what tokio's cooperative budget
/// allows, so every poll spends from it, and a spent budget is not ready
/// either. A task under `tokio::task::unconstrained` never spends it, so
/// the read has a bound of its own that does not depend on the runtime,
/// `DRAIN_POLLS`: a bodiless answer's body has no frames to a few, so the
/// bound is far past any real one and costs the thread milliseconds at
/// most (a runtime's own budget is 128). A body ready
/// without end, or waking itself and never giving a frame, cannot hold the
/// thread. A bodiless answer of more frames than the budget or this bound
/// ends `abandoned`, as one still waiting does.
fn drain_ready(body: &mut Body) {
    /// The most polls one drain makes.
    const DRAIN_POLLS: usize = 1 << 16;
    struct Woken(AtomicBool);
    impl futures::task::ArcWake for Woken {
        fn wake_by_ref(this: &Arc<Self>) {
            this.0.store(true, Ordering::SeqCst);
        }
    }
    let woken = Arc::new(Woken(AtomicBool::new(false)));
    let waker = futures::task::waker(woken.clone());
    let mut cx = std::task::Context::from_waker(&waker);
    let mut left = DRAIN_POLLS;
    while left > 0 {
        let mut budget = std::pin::pin!(tokio::task::consume_budget());
        if budget.as_mut().poll(&mut cx).is_pending() {
            return;
        }
        left -= 1;
        woken.0.store(false, Ordering::SeqCst);
        match http_body::Body::poll_frame(std::pin::Pin::new(&mut *body), &mut cx) {
            std::task::Poll::Pending if woken.0.load(Ordering::SeqCst) => {}
            std::task::Poll::Ready(Some(Ok(_))) => {}
            _ => return,
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
    let mut resp = http::Response::new(Body::full(fault.message().to_owned()));
    *resp.status_mut() = status(fault);
    let text = HeaderValue::from_static("text/plain; charset=utf-8");
    resp.headers_mut().insert(CONTENT_TYPE, text);
    resp
}

/// Write `fault`'s status and retry, as `Retry-After` in whole seconds, into
/// `head`: a format's refusal. Fractional seconds round up, saturating at `u64::MAX`.
pub fn error_head(head: &mut http::response::Parts, fault: &Fault) {
    head.status = status(fault);
    if let Some(&RetryAfter(wait)) = fault.get() {
        let secs = wait
            .as_secs()
            .saturating_add(u64::from(wait.subsec_nanos() > 0));
        head.headers.insert(RETRY_AFTER, HeaderValue::from(secs));
    }
}

/// Use an explicitly supplied [`RawReply`], replacing `head` and returning
/// its body. Otherwise apply [`error_head`] and return `None` so the client
/// format writes its own error. Format names do not authorize forwarding.
pub fn answered(fault: &Fault, head: &mut http::response::Parts) -> Option<Bytes> {
    match fault.get::<RawReply>().map(|raw| &raw.0) {
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
        let fault = Fault::new("slow down")
            .with(Status(429))
            .with(RetryAfter(Duration::from_millis(1500)));
        error_head(&mut head, &fault);
        assert_eq!(head.status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(head.headers[RETRY_AFTER], "2");
        assert_eq!(retry_after(&head.headers), Some(Duration::from_secs(2)));
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
            let code = error_code(&fault).into_owned();
            assert_eq!(of(fault.clone()), want, "{code}");
            assert_eq!(of(fault.with(Status(429))), 429, "{code}");
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
    fn retryable_respects_opening_stage_code_precedence_and_http_status() {
        let hinted = |fault: Fault, number| fault.with(Status(number));
        let custom = || Fault::new("x").with(ErrorCode("custom".into()));
        for (fault, before, after) in [
            (Upstream::fault("x"), true, true),
            (hinted(Upstream::fault("x"), 400), true, false),
            (hinted(Fault::config("x"), 503), false, false),
            (hinted(Fault::internal("x"), 429), false, false),
            (hinted(Fault::abandoned(), 502), false, false),
            (hinted(BadRequest::fault("x"), 503), false, true),
            (hinted(custom(), 400), false, false),
            (custom(), false, true),
        ] {
            assert_eq!(retryable(&fault, false), before, "before: {fault:?}");
            assert_eq!(retryable(&fault, true), after, "after: {fault:?}");
        }
        for number in [408, 425, 429, 500, 502, 503, 504, 529] {
            assert!(retryable(&hinted(custom(), number), true), "{number}");
        }
        for number in [200, 400, 401, 403, 404, 409, 422, 499, 501, 505, 528, 530] {
            assert!(!retryable(&hinted(custom(), number), true), "{number}");
        }
    }

    #[test]
    fn a_number_that_is_no_status_is_a_500() {
        let fault = BadRequest::fault("x").with(Status(1000));
        assert_eq!(status(&fault), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn only_an_explicit_raw_reply_replaces_the_head() {
        let mut answer = http::Response::new(Bytes::from_static(b"busy"));
        *answer.status_mut() = StatusCode::TOO_MANY_REQUESTS;
        let fault = Fault::new("busy")
            .with(Status(429))
            .with(ProviderError::new("f", answer.clone()))
            .with(RetryAfter(Duration::from_secs(3)));
        let (mut head, ()) = http::Response::new(()).into_parts();
        assert_eq!(
            answered(&fault.clone().with(RawReply(answer)), &mut head).as_deref(),
            Some(&b"busy"[..])
        );
        assert!(head.headers.get(RETRY_AFTER).is_none(), "as it came");
        let (mut head, ()) = http::Response::new(()).into_parts();
        assert_eq!(answered(&fault, &mut head), None);
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
