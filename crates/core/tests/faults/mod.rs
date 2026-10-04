//! Facts a test names its faults by, as an adapter or plugin would: the
//! kernel knows none of these.
#![allow(dead_code)]
use respan_gateway_core::*;

/// A provider failed.
#[derive(Clone, Copy, Debug)]
pub struct Upstream;

/// The client's input is not one the format can take.
#[derive(Clone, Copy, Debug)]
pub struct BadRequest;

/// A stream stopped before its end.
#[derive(Clone, Copy, Debug)]
pub struct Truncated;

/// A code of a plugin's own.
#[derive(Clone, Copy, Debug)]
pub struct Code(pub &'static str);

pub fn upstream(message: &str) -> Fault {
    Fault::new(message).with(Upstream)
}

pub fn bad_request(message: &str) -> Fault {
    Fault::new(message).with(BadRequest)
}

pub fn truncated() -> Fault {
    Fault::new("stream ended before completion").with(Truncated)
}

/// A name for the fact `fault` carries, "error" if none is known.
pub fn code(fault: &Fault) -> &'static str {
    if let Some(Code(code)) = fault.get() {
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

/// `r` with its fault as its message, which tests can compare.
pub fn says<T>(r: Result<T>) -> std::result::Result<T, String> {
    r.map_err(|f| f.message().to_owned())
}

/// `rs` with their faults as messages.
pub fn says_all<T>(rs: impl IntoIterator<Item = Result<T>>) -> Vec<std::result::Result<T, String>> {
    rs.into_iter().map(says).collect()
}
