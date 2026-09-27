use std::borrow::Cow;
use std::error::Error;
use std::fmt;
use std::sync::Arc;

use http::StatusCode;
use serde_json::Value;

/// Codes the kernel and its shorthand constructors use. Plugins define their
/// own freely; a code is any short machine-readable string.
pub mod codes {
    /// The client's request is not one the format or kind can take.
    pub const BAD_REQUEST: &str = "bad_request";
    /// Nothing answers the request.
    pub const NOT_FOUND: &str = "not_found";
    /// A provider failed: it could not be reached, broke off, sent what is
    /// not a response or a redirect that cannot be translated, or failed
    /// with no code of its own.
    pub const UPSTREAM: &str = "upstream_error";
    /// A provider's stream stopped before its end.
    pub const TRUNCATED: &str = "truncated";
    /// A response was given up before its end: the client left, or a
    /// call's response was dropped, or still open as its request closed.
    pub const ABANDONED: &str = "abandoned";
    /// The gateway is set up in a way that cannot work.
    pub const CONFIG: &str = "config_error";
    /// A plugin or format asked for, or made, a type of another kind than
    /// the client's format's.
    pub const WRONG_KIND: &str = "wrong_kind";
    /// A bug, such as a plugin or format that panicked.
    pub const INTERNAL: &str = "internal_error";
}

/// The one error type at the plugin boundary. Open: any status, any code,
/// optional details for the client and an optional source for logs.
///
/// Plugins keep their own error types (e.g. with `thiserror`) and implement
/// `From<TheirError> for Fault`, so `?` works in hooks.
#[derive(Clone)]
pub struct Fault(Box<Inner>);

#[derive(Clone)]
struct Inner {
    status: StatusCode,
    code: Cow<'static, str>,
    message: String,
    details: Option<Value>,
    source: Option<Arc<dyn Error + Send + Sync>>,
    said: bool,
}

impl Fault {
    /// A fault with this status, code and message for the client. A refusal
    /// with a status that is not 4xx or 5xx is sent as a 500.
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
            said: false,
        }))
    }

    /// Extra fields for the client's error body, as a JSON object; formats
    /// ignore any other value. The format writes the fields that state the
    /// fault, such as its message, and chooses which extra ones it keeps.
    pub fn with_details(mut self, details: Value) -> Self {
        self.0.details = Some(details);
        self
    }

    /// The underlying cause, for logs, never sent to the client: any error,
    /// a boxed one, or a message.
    pub fn with_source(mut self, source: impl Into<Box<dyn Error + Send + Sync>>) -> Self {
        self.0.source = Some(Arc::from(source.into()));
        self
    }

    /// Marks the fault as already said: its decoder passed the error body on
    /// as a Frame, so an encoder writing the provider's bytes is not handed
    /// the `Part::End` carrying it.
    pub fn said(mut self) -> Self {
        self.0.said = true;
        self
    }

    pub(crate) fn is_said(&self) -> bool {
        self.0.said
    }

    /// Not said after all: for a decoder that drops the Frame the fault was
    /// said with.
    pub fn unsaid(mut self) -> Self {
        self.0.said = false;
        self
    }

    /// The status it was made with; see [`Fault::new`] for how a refusal
    /// sends it.
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

    /// Nothing to answer with: a 404.
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, codes::NOT_FOUND, message)
    }

    /// A provider's failure: a 502.
    pub fn upstream(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_GATEWAY, codes::UPSTREAM, message)
    }

    pub(crate) fn truncated() -> Self {
        Self::new(
            StatusCode::BAD_GATEWAY,
            codes::TRUNCATED,
            "stream ended before completion",
        )
    }

    pub(crate) fn abandoned() -> Self {
        // 499 is the de facto status for a request given up before its end.
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

    /// Something asked for `T`, which is not the client's kind.
    pub(crate) fn wrong_kind<T>() -> Self {
        Self::another_kind(format!("asked for {}", std::any::type_name::<T>()))
    }

    /// A type of another kind than the client's was asked for or made: a
    /// misconfiguration, or traffic a plugin should skip. `what` goes to logs only.
    pub(crate) fn another_kind(what: String) -> Self {
        let message = "a type of another kind than this request's was asked for or made";
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            codes::WRONG_KIND,
            message,
        )
        .with_source(what)
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
            .field("said", &self.0.said)
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

/// A request or response a plugin or format built wrong, such as with a
/// bad header: a bug, a 500.
impl From<http::Error> for Fault {
    fn from(e: http::Error) -> Self {
        Fault::internal("an HTTP message was built wrong").with_source(e)
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

/// Faults are equal when the client would read them alike: the source and
/// whether it was said are not compared.
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
