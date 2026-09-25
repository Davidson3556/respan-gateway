use std::fmt;

use bytes::Bytes;
pub use http::Extensions;
use http::{HeaderMap, Method, Uri};

/// A unit of work handed to the gateway, by an HTTP adapter, an embedding
/// program, or a Format splitting a socket into turns.
#[derive(Debug, Default)]
pub struct Inbound {
    /// The route name from config.
    pub route: String,
    /// The message, in the route's format.
    pub body: Bytes,
    /// Whatever the caller has: [`HttpHead`], [`Credential`], or its own types.
    pub ext: Extensions,
}

impl Inbound {
    pub fn new(route: impl Into<String>, body: impl Into<Bytes>) -> Self {
        Self {
            route: route.into(),
            body: body.into(),
            ext: Extensions::new(),
        }
    }

    pub fn with<T: Clone + Send + Sync + 'static>(mut self, value: T) -> Self {
        self.ext.insert(value);
        self
    }
}

/// The HTTP request a client request arrived as. Present only when an HTTP
/// adapter made the request.
#[derive(Debug, Clone)]
pub struct HttpHead {
    pub method: Method,
    pub uri: Uri,
    pub headers: HeaderMap,
}

/// The caller's key, however the transport carried it.
#[derive(Clone, PartialEq, Eq)]
pub struct Credential(String);

impl Credential {
    pub fn new(secret: impl Into<String>) -> Self {
        Self(secret.into())
    }

    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Credential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Credential(***)")
    }
}

/// What a Format reads from a request without interpreting its content.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Envelope {
    pub model: Option<String>,
    pub stream: bool,
}
