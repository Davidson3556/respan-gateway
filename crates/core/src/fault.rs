use std::borrow::Cow;
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use http::StatusCode;
use serde_json::Value;

/// Codes the kernel and its shorthand constructors use. Plugins define their
/// own freely; a code is any short machine-readable string.
pub mod codes {
    pub const BAD_REQUEST: &str = "bad_request";
    pub const UNAUTHORIZED: &str = "unauthorized";
    pub const PAYMENT_REQUIRED: &str = "payment_required";
    pub const FORBIDDEN: &str = "forbidden";
    pub const NOT_FOUND: &str = "not_found";
    pub const RATE_LIMITED: &str = "rate_limited";
    pub const UPSTREAM: &str = "upstream_error";
    pub const TRUNCATED: &str = "truncated";
    pub const CONFIG: &str = "config_error";
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
    retry_after: Option<Duration>,
    details: Option<Value>,
    source: Option<Arc<dyn Error + Send + Sync>>,
}

impl Fault {
    pub fn new(
        status: StatusCode,
        code: impl Into<Cow<'static, str>>,
        message: impl Into<String>,
    ) -> Self {
        Self(Box::new(Inner {
            status,
            code: code.into(),
            message: message.into(),
            retry_after: None,
            details: None,
            source: None,
        }))
    }

    /// Extra fields the client's Format includes in the error body.
    pub fn with_details(mut self, details: Value) -> Self {
        self.0.details = Some(details);
        self
    }

    /// The underlying cause. For logs; never sent to the client.
    pub fn with_source(mut self, source: impl Error + Send + Sync + 'static) -> Self {
        self.0.source = Some(Arc::new(source));
        self
    }

    /// Sent as `Retry-After` when the Fault refuses a request.
    pub fn with_retry_after(mut self, d: Duration) -> Self {
        self.0.retry_after = Some(d);
        self
    }

    pub fn status(&self) -> StatusCode {
        self.0.status
    }

    pub fn code(&self) -> &str {
        &self.0.code
    }

    pub fn message(&self) -> &str {
        &self.0.message
    }

    pub fn retry_after(&self) -> Option<Duration> {
        self.0.retry_after
    }

    pub fn details(&self) -> Option<&Value> {
        self.0.details.as_ref()
    }

    pub fn is(&self, code: &str) -> bool {
        self.0.code == code
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, codes::BAD_REQUEST, message)
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, codes::UNAUTHORIZED, message)
    }

    pub fn payment_required(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::PAYMENT_REQUIRED,
            codes::PAYMENT_REQUIRED,
            message,
        )
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, codes::FORBIDDEN, message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, codes::NOT_FOUND, message)
    }

    pub fn too_many(retry_after: Duration) -> Self {
        Self::new(
            StatusCode::TOO_MANY_REQUESTS,
            codes::RATE_LIMITED,
            "rate limit exceeded",
        )
        .with_retry_after(retry_after)
    }

    pub fn upstream(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_GATEWAY, codes::UPSTREAM, message)
    }

    pub fn truncated() -> Self {
        Self::new(
            StatusCode::BAD_GATEWAY,
            codes::TRUNCATED,
            "stream ended before completion",
        )
    }

    pub fn config(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, codes::CONFIG, message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, codes::INTERNAL, message)
    }
}

impl fmt::Debug for Fault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Fault")
            .field("status", &self.0.status)
            .field("code", &self.0.code)
            .field("message", &self.0.message)
            .field("retry_after", &self.0.retry_after)
            .field("details", &self.0.details)
            .field("source", &self.0.source.as_ref().map(|s| s.to_string()))
            .finish()
    }
}

impl fmt::Display for Fault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.0.code, self.0.message)
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

impl PartialEq for Fault {
    fn eq(&self, other: &Self) -> bool {
        let (a, b) = (&self.0, &other.0);
        a.status == b.status
            && a.code == b.code
            && a.message == b.message
            && a.retry_after == b.retry_after
            && a.details == b.details
    }
}

impl Eq for Fault {}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, thiserror::Error)]
    enum BillingError {
        #[error("insufficient credits")]
        Insufficient { balance: f64 },
    }

    impl From<BillingError> for Fault {
        fn from(e: BillingError) -> Self {
            let BillingError::Insufficient { balance } = &e;
            Fault::new(
                StatusCode::PAYMENT_REQUIRED,
                "insufficient_credits",
                e.to_string(),
            )
            .with_details(serde_json::json!({ "balance": balance }))
        }
    }

    fn charge() -> Result<(), Fault> {
        Err(BillingError::Insufficient { balance: 0.5 })?
    }

    #[test]
    fn plugin_errors_convert_with_question_mark() {
        let f = charge().err();
        assert_eq!(
            f.as_ref().map(Fault::status),
            Some(StatusCode::PAYMENT_REQUIRED)
        );
        assert!(f.as_ref().is_some_and(|f| f.is("insufficient_credits")));
        assert_eq!(
            f.as_ref().and_then(Fault::details),
            Some(&serde_json::json!({ "balance": 0.5 }))
        );
    }

    #[test]
    fn a_fault_is_one_pointer_wide() {
        assert_eq!(std::mem::size_of::<Fault>(), std::mem::size_of::<usize>());
    }
}
