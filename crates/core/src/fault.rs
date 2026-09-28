use std::borrow::Cow;
use std::error::Error;
use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use http::StatusCode;
use serde_json::Value;

pub(crate) const PANICKED: &str = "a plugin or format panicked";

/// `f`, with a panic an internal fault.
pub(crate) fn guarded<T>(f: impl FnOnce() -> Result<T, Fault>) -> Result<T, Fault> {
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
    caught.unwrap_or_else(|_| Err(Fault::internal(PANICKED)))
}

/// The codes the kernel uses. A code is any short machine-readable string;
/// plugins may define their own.
pub mod codes {
    /// The client's request is not one the format or kind can take.
    pub const BAD_REQUEST: &str = "bad_request";
    /// Nothing takes the request: no format accepts it, or no plugin
    /// answers and there is no target.
    pub const NOT_FOUND: &str = "not_found";
    /// A provider failed: unreachable, broken off, not in its format, a head
    /// it may not send, or an error with no code of its own.
    pub const UPSTREAM: &str = "upstream";
    /// A provider's stream stopped before its end, for billing and retries
    /// to tell from a failed call.
    pub const TRUNCATED: &str = "truncated";
    /// A response or call ended before its end: the client left, a response
    /// was dropped, or its request ended first.
    pub const ABANDONED: &str = "abandoned";
    /// The gateway is set up in a way that cannot work.
    pub const CONFIG: &str = "config";
    /// A bug, such as a panic or a type of another kind than the client's.
    pub const INTERNAL: &str = "internal";
}

/// The one error type at the plugin boundary: a status, code, message and
/// optional details for the client, an optional source for logs, and
/// optionally the provider's error body ([`Fault::raw`]). Plugins implement
/// `From<TheirError> for Fault` so `?` works in hooks.
#[derive(Clone)]
pub struct Fault(Box<Inner>);

#[derive(Clone)]
struct Inner {
    status: StatusCode,
    code: Cow<'static, str>,
    message: String,
    details: Option<Value>,
    source: Option<Arc<dyn Error + Send + Sync>>,
    /// The provider's error body, and the name of the format it is in.
    raw: Option<(Arc<str>, Bytes)>,
}

impl Fault {
    /// A fault with this status, code and message; a refusal goes out with
    /// this status.
    pub fn new(
        status: StatusCode,
        code: impl Into<Cow<'static, str>>,
        message: impl Into<String>,
    ) -> Self {
        Self(Box::new(Inner {
            status,
            code: code.into(),
            message: message.into(),
            details: None,
            source: None,
            raw: None,
        }))
    }

    /// Extra fields for the client's error body, as a JSON object; formats
    /// ignore any other value and choose which fields they keep.
    pub fn with_details(mut self, details: Value) -> Self {
        self.0.details = Some(details);
        self
    }

    /// The underlying cause, for logs: an error or a message.
    pub fn with_source(mut self, source: impl Into<Box<dyn Error + Send + Sync>>) -> Self {
        self.0.source = Some(Arc::from(source.into()));
        self
    }

    /// The provider's error body in `format`, which an encoder of that
    /// format writes in place of the fault.
    pub fn with_raw(mut self, format: &str, raw: Bytes) -> Self {
        self.0.raw = Some((Arc::from(format), raw));
        self
    }

    /// The provider's error body, if one in `format` came with it
    /// ([`Fault::with_raw`]).
    pub fn raw(&self, format: &str) -> Option<&Bytes> {
        let raw = self.0.raw.as_ref().filter(|(of, _)| **of == *format);
        raw.map(|(_, raw)| raw)
    }

    /// The same fault with `other`'s error body, if any.
    pub(crate) fn with_raw_of(mut self, other: &Fault) -> Self {
        if let Some(raw) = &other.0.raw {
            self.0.raw = Some(raw.clone());
        }
        self
    }

    /// The same fault under `code`, with `status`.
    pub(crate) fn recoded(mut self, status: StatusCode, code: &'static str) -> Self {
        self.0.status = status;
        self.0.code = code.into();
        self
    }

    /// The status it was made with ([`Fault::new`]).
    pub fn status(&self) -> StatusCode {
        self.0.status
    }

    /// The machine-readable code, one of [`codes`] or the plugin's own.
    pub fn code(&self) -> &str {
        &self.0.code
    }

    /// The message for the client.
    pub fn message(&self) -> &str {
        &self.0.message
    }

    /// The extra fields for the client's error body, if any.
    pub fn details(&self) -> Option<&Value> {
        self.0.details.as_ref()
    }

    /// The client's mistake: a 400.
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, codes::BAD_REQUEST, message)
    }

    /// Nothing takes the request: a 404.
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, codes::NOT_FOUND, message)
    }

    /// A provider's failure: a 502.
    pub fn upstream(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_GATEWAY, codes::UPSTREAM, message)
    }

    /// A stream that ended unfinished: a 502 [`codes::TRUNCATED`].
    pub fn truncated() -> Self {
        Self::new(
            StatusCode::BAD_GATEWAY,
            codes::TRUNCATED,
            "stream ended before completion",
        )
    }

    pub(crate) fn abandoned() -> Self {
        // nginx's 499: the client left.
        let status = StatusCode::from_u16(499).expect("499 is a status code");
        Self::new(status, codes::ABANDONED, "abandoned")
    }

    /// A setup that cannot work: a 500.
    pub fn config(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, codes::CONFIG, message)
    }

    /// A bug: a 500.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, codes::INTERNAL, message)
    }

    /// Something asked for `T`, not of the client's kind: a bug.
    pub(crate) fn wrong_kind<T>() -> Self {
        Self::another_kind(format!("asked for {}", std::any::type_name::<T>()))
    }

    /// A type of another kind than the client's was used; `what` goes to logs.
    pub(crate) fn another_kind(what: String) -> Self {
        let message = "a type of another kind than this request's was asked for or made";
        Self::internal(message).with_source(what)
    }
}

impl fmt::Debug for Fault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Fault")
            .field("status", &self.0.status)
            .field("code", &self.0.code)
            .field("message", &self.0.message)
            .field("details", &self.0.details)
            .field("source", &self.0.source.as_ref().map(|s| s.to_string()))
            .field("raw", &self.0.raw)
            .finish()
    }
}

impl fmt::Display for Fault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.0.code, self.0.message)
    }
}

/// A body that cannot fail, such as a `String` or `Full`, never makes one.
impl From<std::convert::Infallible> for Fault {
    fn from(never: std::convert::Infallible) -> Self {
        match never {}
    }
}

/// A message a plugin or format built wrong: a bug, a 500.
impl From<http::Error> for Fault {
    fn from(e: http::Error) -> Self {
        Fault::internal("an HTTP request or response was built wrong").with_source(e)
    }
}

impl Error for Fault {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.0
            .source
            .as_deref()
            .map(|e| e as &(dyn Error + 'static))
    }
}

/// Equal when the client would read them alike: source and raw body ignored.
impl PartialEq for Fault {
    fn eq(&self, other: &Self) -> bool {
        let (a, b) = (&self.0, &other.0);
        a.status == b.status && a.code == b.code && a.message == b.message && a.details == b.details
    }
}

impl Eq for Fault {}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct PluginError {
        limit: u32,
    }

    impl fmt::Display for PluginError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "over the limit of {}", self.limit)
        }
    }

    impl Error for PluginError {}

    impl From<PluginError> for Fault {
        fn from(e: PluginError) -> Self {
            Fault::new(StatusCode::TOO_MANY_REQUESTS, "over_limit", e.to_string())
                .with_details(serde_json::json!({ "limit": e.limit }))
                .with_source(e)
        }
    }

    fn check() -> Result<(), Fault> {
        Err(PluginError { limit: 3 })?
    }

    #[test]
    fn plugin_errors_convert_with_question_mark() {
        let f = check().err();
        assert_eq!(f.as_ref().map(Fault::code), Some("over_limit"));
        assert_eq!(
            f.as_ref().and_then(Fault::details),
            Some(&serde_json::json!({ "limit": 3 }))
        );
        assert!(f.as_ref().and_then(|f| f.source()).is_some());
    }

    #[test]
    fn a_fault_is_one_pointer_wide() {
        assert_eq!(std::mem::size_of::<Fault>(), std::mem::size_of::<usize>());
    }
}
