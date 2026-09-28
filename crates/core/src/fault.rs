use std::any::Any;
use std::borrow::Cow;
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

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
/// optional details for the client, when to retry, an optional source for
/// logs, and optionally the provider's own answer ([`Fault::answer`]).
/// Plugins implement `From<TheirError> for Fault` so `?` works in hooks.
///
/// The kernel never reads the status or the answer: the client's format
/// writes them ([`Format::response_encoder`](crate::Format::response_encoder)).
#[derive(Clone)]
pub struct Fault(Box<Inner>);

#[derive(Clone)]
struct Inner {
    status: StatusCode,
    code: Cow<'static, str>,
    message: String,
    details: Option<Value>,
    retry_after: Option<Duration>,
    source: Option<Arc<dyn Error + Send + Sync>>,
    /// A provider's own words for it, and the format that read them.
    answer: Option<(Arc<str>, Arc<dyn Any + Send + Sync>)>,
    /// Whether the provider answered with it in place of a response.
    answered: bool,
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
            retry_after: None,
            source: None,
            answer: None,
            answered: false,
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

    /// How long the client should wait before it tries again, which the
    /// client's format writes, such as a provider's `Retry-After`.
    pub fn with_retry_after(mut self, wait: Duration) -> Self {
        self.0.retry_after = Some(wait);
        self
    }

    /// How long to wait before trying again, if said
    /// ([`Fault::with_retry_after`]).
    pub fn retry_after(&self) -> Option<Duration> {
        self.0.retry_after
    }

    /// The provider answered with this failure in place of a response:
    /// `answer` is its answer as `format` read it, such as its head and
    /// body, which an encoder of that format writes as it came
    /// ([`Fault::is_answer`]).
    pub fn with_answer<A: Any + Send + Sync>(self, format: &str, answer: A) -> Self {
        self.answered(format, answer, true)
    }

    /// Like [`Fault::with_answer`], for a failure the provider reported
    /// within a response it had begun, such as an error event in a stream:
    /// its words go as they came to a client of `format`, but it is no
    /// answer ([`Fault::is_answer`]).
    pub fn with_words<A: Any + Send + Sync>(self, format: &str, words: A) -> Self {
        self.answered(format, words, false)
    }

    fn answered<A: Any + Send + Sync>(mut self, format: &str, value: A, whole: bool) -> Self {
        self.0.answer = Some((Arc::from(format), Arc::new(value)));
        self.0.answered = whole;
        self
    }

    /// The provider's answer or words as `A`, if they came in `format`
    /// ([`Fault::with_answer`], [`Fault::with_words`]).
    pub fn answer<A: Any>(&self, format: &str) -> Option<&A> {
        let (_, answer) = self.0.answer.as_ref().filter(|(of, _)| **of == *format)?;
        answer.downcast_ref()
    }

    /// Whether a provider answered with this failure in place of a
    /// response ([`Fault::with_answer`]), such as with an error status: it
    /// began no response, so a biller may charge nothing for it.
    pub fn is_answer(&self) -> bool {
        self.0.answered
    }

    /// The same fault with `other`'s answer and retry, if any.
    pub(crate) fn with_answer_of(mut self, other: &Fault) -> Self {
        if other.0.answer.is_some() {
            self.0.answer = other.0.answer.clone();
            self.0.answered = other.0.answered;
        }
        self.0.retry_after = self.0.retry_after.or(other.0.retry_after);
        self
    }

    /// The same fault without the provider's words, which go only to a
    /// client whose own message the call relayed, as a head does.
    pub(crate) fn unrelayed(mut self) -> Self {
        self.0.answer = None;
        self
    }

    /// The same fault under `code`, with `status`.
    pub(crate) fn recoded(mut self, status: StatusCode, code: &'static str) -> Self {
        self.0.status = status;
        self.0.code = code.into();
        self
    }

    /// The status it was made with ([`Fault::new`]), in HTTP's numbering,
    /// which a format of another protocol maps to its own.
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

    /// Its status is never written: no one waits for it.
    pub(crate) fn abandoned() -> Self {
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
            .field("retry_after", &self.0.retry_after)
            .field("source", &self.0.source.as_ref().map(|s| s.to_string()))
            .field("answer", &self.0.answer.as_ref().map(|(of, _)| &**of))
            .field("answered", &self.0.answered)
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

/// Equal when the client would read them alike: source and answer ignored.
impl PartialEq for Fault {
    fn eq(&self, other: &Self) -> bool {
        let (a, b) = (&self.0, &other.0);
        a.status == b.status
            && a.code == b.code
            && a.message == b.message
            && a.details == b.details
            && a.retry_after == b.retry_after
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
    fn an_answer_is_read_only_by_its_format_and_as_its_type() {
        let f = Fault::upstream("x").with_answer("f", 7_u8);
        assert_eq!(f.answer::<u8>("f"), Some(&7));
        assert_eq!(f.answer::<u8>("g"), None);
        assert_eq!(f.answer::<u16>("f"), None);
        assert!(f.is_answer());
        let words = Fault::upstream("x").with_words("f", 7_u8);
        assert!(!words.is_answer() && words.answer::<u8>("f").is_some());
    }

    #[test]
    fn a_fault_is_one_pointer_wide() {
        assert_eq!(std::mem::size_of::<Fault>(), std::mem::size_of::<usize>());
    }
}
