use http::header::{HeaderName, HeaderValue};

/// A configured provider: where to send requests and how to authenticate.
#[derive(Debug, Clone)]
pub struct Target {
    pub name: String,
    /// The Format it speaks.
    pub format: String,
    pub base_url: String,
    /// Overrides the model the client asked for.
    pub model: Option<String>,
    pub headers: Vec<(HeaderName, HeaderValue)>,
}
