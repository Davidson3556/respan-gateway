#![warn(missing_docs)]
//! Faults in HTTP's terms, with no I/O: the status and error code a
//! [`Fault`] asks for, whether a failed attempt is worth another, and a
//! provider's error answer read into a fault. A plugin that sets or reads a
//! status depends on this crate alone, not on the HTTP wire.

use std::time::Duration;

use bytes::Bytes;
use http::StatusCode;
use http::header::RETRY_AFTER;
use respan_gateway_core::{
    Abandoned, BadRequest, Config, ErrorCode, Fault, Internal, NotFound, RetryAfter, Truncated,
    Upstream,
};

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

/// A message a plugin or format built wrong: a bug, a 500.
pub fn built_wrong(e: http::Error) -> Fault {
    Fault::internal("an HTTP request or response was built wrong").with_source(e)
}

/// A provider's error answer as it came, for a plugin to inspect or a relay
/// to write; never written unasked.
#[derive(Clone, Debug)]
pub struct ProviderError(pub http::Response<Bytes>);

/// The fault of a provider's `answer` that is no success, read as by a
/// format whose error bodies it does not read
/// (`respan_gateway_http::ProviderIo::with_refusal`). It is [`Upstream`], says the
/// answer's status, with that [`Status`] if an error's, else a 502, and its
/// `Retry-After` in whole seconds ([`RetryAfter`]; an HTTP date counts as
/// none). Unless informational (an unasked 101), it carries the answer as
/// it came ([`ProviderError`]), for a plugin to inspect or a relay to write.
pub fn provider_fault(answer: http::Response<Bytes>) -> Fault {
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
    fault.with(ProviderError(answer))
}

#[cfg(test)]
mod tests {
    use super::*;

    // An answer that is no error says a 502; an unasked 101 carries no
    // answer, as it was never relayed.
    #[test]
    fn a_refused_success_is_a_502_and_an_unasked_101_carries_no_answer() {
        for (status, carried) in [(200, true), (101, false)] {
            let mut answer = http::Response::new(Bytes::from_static(b"x"));
            *answer.status_mut() = StatusCode::from_u16(status).unwrap_or_default();
            let read = provider_fault(answer);
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
}
