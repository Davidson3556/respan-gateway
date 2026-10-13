//! How a test names its faults, as an adapter or plugin would.
#![allow(dead_code)]
use respan_gateway_core::*;

/// A code of a plugin's own.
#[derive(Clone, Copy, Debug)]
pub struct Code(pub &'static str);

pub fn upstream(message: &str) -> Fault {
    Fault::upstream(message)
}

pub fn bad_request(message: &str) -> Fault {
    Fault::bad_request(message)
}

pub fn truncated() -> Fault {
    Fault::truncated()
}

/// A name for `fault`: its plugin's [`Code`], else its own.
pub fn code(fault: &Fault) -> &str {
    match fault.get() {
        Some(Code(code)) => code,
        None => fault.code(),
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
