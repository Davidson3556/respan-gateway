mod facts;

pub use facts::{BadRequest, Details, ErrorCode, RetryAfter, Truncated, Upstream};

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use crate::{Extensions, Result};

pub(crate) const PANICKED: &str = "a plugin or format panicked";

/// `f`, with a panic an internal fault.
pub(crate) fn guarded<T>(f: impl FnOnce() -> Result<T>) -> Result<T> {
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
    caught.unwrap_or_else(|_| Err(Fault::internal(PANICKED)))
}

/// The client left, a response was dropped, or its request ended first.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Abandoned;

/// A bug: a panic, or a type of another kind than the client's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Internal;

/// The gateway is set up in a way that cannot work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Config;

/// No format accepts the request, or nothing answers it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NotFound;

/// The one error type: a message for the client, a source for logs, and
/// typed facts the edges read ([`Fault::with`]), such as an HTTP status.
/// The kernel reads only its own markers: [`Abandoned`], [`Internal`],
/// [`Config`] and [`NotFound`]. Implement `From<YourError> for Fault` so `?`
/// works in hooks.
#[derive(Clone)]
pub struct Fault(Arc<Fields>);

#[derive(Clone)]
struct Fields {
    message: String,
    source: Option<Arc<dyn Error + Send + Sync>>,
    facts: Extensions,
}

impl Fault {
    /// A fault with this message, safe to show a client.
    pub fn new(message: impl Into<String>) -> Self {
        Self(Arc::new(Fields {
            message: message.into(),
            source: None,
            facts: Extensions::new(),
        }))
    }

    /// A fault with `e`'s display as its message and `e` as its source, so
    /// [`Fault::cause`] finds it.
    pub fn from_error(e: impl Error + Send + Sync + 'static) -> Self {
        Self::new(e.to_string()).with_source(e)
    }

    /// The underlying cause, for logs: an error or a message.
    pub fn with_source(mut self, source: impl Into<Box<dyn Error + Send + Sync>>) -> Self {
        Arc::make_mut(&mut self.0).source = Some(Arc::from(source.into()));
        self
    }

    /// A fact for the edges, such as the status a transport writes. A value
    /// of the same type replaces the last.
    pub fn with<T: Clone + Send + Sync + 'static>(mut self, fact: T) -> Self {
        Arc::make_mut(&mut self.0).facts.insert(fact);
        self
    }

    /// The fact of type `T`, if any.
    pub fn get<T: 'static>(&self) -> Option<&T> {
        self.0.facts.get()
    }

    /// Whether there is a fact of type `T`.
    pub fn is<T: 'static>(&self) -> bool {
        self.get::<T>().is_some()
    }

    /// The first error of type `E` in the source chain, if any
    /// ([`Fault::from_error`]).
    pub fn cause<E: Error + 'static>(&self) -> Option<&E> {
        let mut next = Error::source(self);
        while let Some(e) = next {
            if let Some(found) = e.downcast_ref() {
                return Some(found);
            }
            next = e.source();
        }
        None
    }

    /// The message for the client.
    pub fn message(&self) -> &str {
        &self.0.message
    }

    /// No one waits for it: [`Abandoned`], such as a response whose client
    /// left.
    pub fn abandoned() -> Self {
        Self::new("abandoned").with(Abandoned)
    }

    /// A bug: [`Internal`].
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(message).with(Internal)
    }

    /// A setup that cannot work: [`Config`].
    pub fn config(message: impl Into<String>) -> Self {
        Self::new(message).with(Config)
    }

    /// Nothing takes the request: [`NotFound`].
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(message).with(NotFound)
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
            .field("message", &self.0.message)
            .field("facts", &self.0.facts)
            .field("source", &self.0.source.as_ref().map(|s| s.to_string()))
            .finish()
    }
}

impl fmt::Display for Fault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0.message)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct PluginError {
        limit: u32,
    }

    /// A fact an edge reads: the limit a client is told.
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
            Fault::new(e.to_string())
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
        assert_eq!(f.as_ref().map(Fault::message), Some("over the limit of 3"));
        assert_eq!(f.as_ref().and_then(Fault::get::<Limit>), Some(&Limit(3)));
        assert!(f.as_ref().and_then(|f| f.source()).is_some());
    }

    #[test]
    fn a_fact_is_read_by_its_type() {
        let f = Fault::new("x").with(7_u8).with("s");
        assert_eq!((f.get::<u8>(), f.get::<&str>()), (Some(&7), Some(&"s")));
        assert_eq!(f.get::<u16>(), None);
        let g = f.clone().with(8_u8);
        assert_eq!((f.get::<u8>(), g.get::<u8>()), (Some(&7), Some(&8)));
    }

    #[test]
    fn a_source_error_is_found_by_its_type() {
        let f = Fault::from_error(PluginError { limit: 5 });
        assert_eq!(f.message(), "over the limit of 5");
        assert_eq!(f.cause::<PluginError>().map(|e| e.limit), Some(5));
        assert!(!f.is::<Limit>());
        let wrapped = Fault::new("outer").with_source(f);
        assert_eq!(wrapped.cause::<PluginError>().map(|e| e.limit), Some(5));
    }

    #[test]
    fn the_kernel_outcomes_carry_their_markers() {
        assert!(Fault::abandoned().is::<Abandoned>());
        assert_eq!(Fault::abandoned().message(), "abandoned");
        assert!(Fault::internal("x").is::<Internal>());
        assert!(Fault::config("x").is::<Config>());
        assert!(Fault::not_found("x").is::<NotFound>());
        assert!(!Fault::config("x").is::<Internal>());
    }
}
