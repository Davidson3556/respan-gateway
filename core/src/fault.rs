use std::any::Any;
use std::borrow::Cow;
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use crate::{Extensions, Result};

pub(crate) const PANICKED: &str = "a plugin or format panicked";

/// `f`, with a panic an internal fault.
pub(crate) fn guarded<T>(f: impl FnOnce() -> Result<T>) -> Result<T> {
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
    caught.unwrap_or_else(|_| Err(Fault::internal(PANICKED)))
}

/// The kernel's codes; a plugin may use its own.
pub mod codes {
    /// The client's request is not one the format or kind can take.
    pub const BAD_REQUEST: &str = "bad_request";
    /// Nothing takes the request: no format accepts it, or no plugin
    /// replies and there is no target.
    pub const NOT_FOUND: &str = "not_found";
    /// A provider failed: unreachable, broken off, not in its format, or an
    /// error with no code of its own.
    pub const UPSTREAM: &str = "upstream";
    /// A provider's stream stopped before its end.
    pub const TRUNCATED: &str = "truncated";
    /// A response or call ended before its end: the client left, a response
    /// was dropped, or its request ended first.
    pub const ABANDONED: &str = "abandoned";
    /// The gateway is set up in a way that cannot work.
    pub const CONFIG: &str = "config";
    /// A bug, such as a panic or a type of another kind than the client's.
    pub const INTERNAL: &str = "internal";
}

/// The one error type: a code and message for the client, a retry delay, a source for logs, typed hints a transport reads (an HTTP status is
/// one, [`Fault::with`]), and a provider's answer, which the client's format
/// writes and the kernel never reads. Implement
/// `From<YourError> for Fault` so `?` works in hooks.
#[derive(Clone)]
pub struct Fault(Box<Inner>);

#[derive(Clone)]
struct Inner {
    code: Cow<'static, str>,
    message: String,
    retry_after: Option<Duration>,
    source: Option<Arc<dyn Error + Send + Sync>>,
    /// Hints for the edges, which the kernel never reads.
    hints: Extensions,
    /// A provider's own words for it, and the format that read them.
    answer: Option<(Arc<str>, Arc<dyn Any + Send + Sync>)>,
}

impl Fault {
    /// A fault of this code and message.
    pub fn new(code: impl Into<Cow<'static, str>>, message: impl Into<String>) -> Self {
        Self(Box::new(Inner {
            code: code.into(),
            message: message.into(),
            retry_after: None,
            source: None,
            hints: Extensions::new(),
            answer: None,
        }))
    }

    /// The underlying cause, for logs: an error or a message.
    pub fn with_source(mut self, source: impl Into<Box<dyn Error + Send + Sync>>) -> Self {
        self.0.source = Some(Arc::from(source.into()));
        self
    }

    /// How long the client should wait before it tries again.
    pub fn with_retry_after(mut self, wait: Duration) -> Self {
        self.0.retry_after = Some(wait);
        self
    }

    /// A hint for the edges, such as the status a transport writes; the
    /// kernel never reads one. A value of the same type replaces the last.
    pub fn with<T: Clone + Send + Sync + 'static>(mut self, value: T) -> Self {
        self.0.hints.insert(value);
        self
    }

    /// The hint of type `T`, if any ([`Fault::with`]).
    pub fn get<T: 'static>(&self) -> Option<&T> {
        self.0.hints.get()
    }

    /// How long to wait before trying again, if said.
    pub fn retry_after(&self) -> Option<Duration> {
        self.0.retry_after
    }

    /// The provider's own words for this failure, as `format` read them,
    /// which that format writes as they came.
    pub fn with_answer<A: Any + Send + Sync>(mut self, format: &str, answer: A) -> Self {
        self.0.answer = Some((Arc::from(format), Arc::new(answer)));
        self
    }

    /// The provider's words as `A`, if they came in `format`
    /// ([`Fault::with_answer`]).
    pub fn answer<A: Any>(&self, format: &str) -> Option<&A> {
        let (_, answer) = self.0.answer.as_ref().filter(|(of, _)| **of == *format)?;
        answer.downcast_ref()
    }

    /// The same fault with `other`'s answer and retry, if any.
    pub(crate) fn with_answer_of(mut self, other: &Fault) -> Self {
        if other.0.answer.is_some() {
            self.0.answer = other.0.answer.clone();
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

    /// The machine-readable code, one of [`codes`] or the plugin's own.
    pub fn code(&self) -> &str {
        &self.0.code
    }

    /// The message for the client.
    pub fn message(&self) -> &str {
        &self.0.message
    }

    /// The client's mistake.
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(codes::BAD_REQUEST, message)
    }

    /// Nothing takes the request.
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(codes::NOT_FOUND, message)
    }

    /// A provider's failure.
    pub fn upstream(message: impl Into<String>) -> Self {
        Self::new(codes::UPSTREAM, message)
    }

    /// A stream that ended unfinished: [`codes::TRUNCATED`].
    pub fn truncated() -> Self {
        Self::new(codes::TRUNCATED, "stream ended before completion")
    }

    /// No one waits for it: [`codes::ABANDONED`], such as a response whose
    /// client left.
    pub fn abandoned() -> Self {
        Self::new(codes::ABANDONED, "abandoned")
    }

    /// A setup that cannot work.
    pub fn config(message: impl Into<String>) -> Self {
        Self::new(codes::CONFIG, message)
    }

    /// A bug.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(codes::INTERNAL, message)
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
            .field("code", &self.0.code)
            .field("message", &self.0.message)
            .field("retry_after", &self.0.retry_after)
            .field("source", &self.0.source.as_ref().map(|s| s.to_string()))
            .field("answer", &self.0.answer.as_ref().map(|(of, _)| &**of))
            .finish()
    }
}

impl fmt::Display for Fault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.0.code, self.0.message)
    }
}

/// What cannot fail never makes one.
impl From<std::convert::Infallible> for Fault {
    fn from(never: std::convert::Infallible) -> Self {
        match never {}
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

/// Equal when the client would read them alike: source, hints and answer
/// ignored.
impl PartialEq for Fault {
    fn eq(&self, other: &Self) -> bool {
        let (a, b) = (&self.0, &other.0);
        a.code == b.code && a.message == b.message && a.retry_after == b.retry_after
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

    /// A hint an edge reads: the limit a client is told.
    #[derive(Clone, Debug, PartialEq)]
    struct Limit(u32);

    impl fmt::Display for PluginError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "over the limit of {}", self.limit)
        }
    }

    impl Error for PluginError {}

    impl From<PluginError> for Fault {
        fn from(e: PluginError) -> Self {
            Fault::new("over_limit", e.to_string())
                .with(Limit(e.limit))
                .with_source(e)
        }
    }

    fn check() -> Result<()> {
        Err(PluginError { limit: 3 })?
    }

    #[test]
    fn plugin_errors_convert_with_question_mark() {
        let f = check().err();
        assert_eq!(f.as_ref().map(Fault::code), Some("over_limit"));
        assert_eq!(f.as_ref().and_then(Fault::get::<Limit>), Some(&Limit(3)));
        assert!(f.as_ref().and_then(|f| f.source()).is_some());
    }

    #[test]
    fn an_answer_is_read_only_by_its_format_and_as_its_type() {
        let f = Fault::upstream("x").with_answer("f", 7_u8);
        assert_eq!(f.answer::<u8>("f"), Some(&7));
        assert_eq!(f.answer::<u8>("g"), None);
        assert_eq!(f.answer::<u16>("f"), None);
    }

    #[test]
    fn a_hint_is_read_by_its_type() {
        let f = Fault::new("x", "x").with(7_u8).with("s");
        assert_eq!((f.get::<u8>(), f.get::<&str>()), (Some(&7), Some(&"s")));
        assert_eq!(f.get::<u16>(), None);
        let g = f.clone().with(8_u8);
        assert_eq!((f.get::<u8>(), g.get::<u8>()), (Some(&7), Some(&8)));
    }
}
