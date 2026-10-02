//! A pooled Reqwest backend for outbound HTTP provider calls.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use http::HeaderMap;
use http_body_util::BodyExt;
use respan_gateway_sdk::{Fault, Upstream};

use crate::{
    Body, Completion, Outgoing, credentials, is_connection_header, proxy_call,
    strip_client_origin_headers, strip_origin_headers,
};

/// A call the provider cannot be sent; the error is for logs.
fn invalid(e: impl std::error::Error + Send + Sync + 'static) -> Fault {
    Fault::config("the provider request is not valid").with_source(e)
}

/// Reaches one provider over HTTP(S) at a base URL, which each call's path
/// extends, sharing one connection pool unless given a client. It strips a
/// call ([`proxy_call`], [`strip_client_origin_headers`]), sends its keys
/// ([`credentials`]), and strips the response ([`strip_origin_headers`]).
#[derive(Clone)]
pub struct HttpClient {
    client: reqwest::Client,
    /// The base URL, without a trailing `/`.
    base: String,
    headers: HeaderMap,
}

impl std::fmt::Debug for HttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Header values may be keys.
        f.debug_struct("HttpClient")
            .field("base", &self.base)
            .field("headers", &self.headers.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

/// The client every transport shares: it returns redirects unfollowed and
/// gives up after 10 s connecting or 300 s of silence.
fn shared_client() -> Result<reqwest::Client, Fault> {
    static CLIENT: OnceLock<Result<reqwest::Client, String>> = OnceLock::new();
    let built = CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .read_timeout(Duration::from_secs(300))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| e.to_string())
    });
    built
        .clone()
        .map_err(|e| Fault::internal("the HTTP client could not be built").with_source(e))
}

impl HttpClient {
    /// A transport to `base_url`: `http` or `https`, a host, an optional path
    /// prefix such as `/v1`, and no query, fragment or credentials, else
    /// `Err` with `config`.
    pub fn new(base_url: impl Into<String>) -> Result<Self, Fault> {
        // The URL may contain a key, so the error does not echo it.
        let url = reqwest::Url::parse(&base_url.into()).ok().filter(|u| {
            matches!(u.scheme(), "http" | "https")
                && u.has_host()
                && u.port() != Some(0)
                && u.username().is_empty()
                && u.password().is_none()
                && u.query().is_none()
                && u.fragment().is_none()
        });
        let Some(url) = url else {
            return Err(Fault::config(
                "bad base URL: an http or https one with a host, and no query, \
                 fragment or credentials (those go in its headers)",
            ));
        };
        Ok(Self {
            client: shared_client()?,
            base: url.as_str().trim_end_matches('/').to_owned(),
            headers: HeaderMap::new(),
        })
    }

    /// Send `headers`, such as the API key, with every call, replacing any
    /// set before, in place of the client's ([`credentials`]). `Err` with
    /// `config` for a connection header.
    pub fn with_headers(mut self, headers: HeaderMap) -> Result<Self, Fault> {
        if let Some(name) = headers.keys().find(|k| is_connection_header(k)) {
            let name = name.as_str();
            return Err(Fault::config(format!(
                "header '{name}' is the connection's"
            )));
        }
        self.headers = headers;
        Ok(self)
    }

    /// Use `client` and its pool instead of the shared one. It should not
    /// follow redirects: they are the client's to follow.
    pub fn with_client(mut self, client: reqwest::Client) -> Self {
        self.client = client;
        self
    }

    /// Send an encoded call and return its response.
    pub async fn send(
        &self,
        mut call: http::Request<Body>,
    ) -> Result<(http::Response<Body>, Option<Completion>), Fault> {
        let (mut parts, body) = call.into_parts();
        proxy_call(&mut parts);
        strip_client_origin_headers(&mut parts.headers);
        call = http::Request::from_parts(parts, body);
        credentials(&mut call, &self.headers);
        let (mut parts, body) = call.into_parts();
        let path = parts.uri.path_and_query().map_or("/", |p| p.as_str());
        let url = format!("{}{path}", self.base);
        parts.uri = url.parse().map_err(invalid)?;
        // A whole call is one frame, or none if empty, and goes with its length.
        let body = match body.full_bytes() {
            Some(bytes) => reqwest::Body::from(bytes.clone()),
            None => reqwest::Body::wrap_stream(body),
        };
        let req =
            reqwest::Request::try_from(http::Request::from_parts(parts, body)).map_err(invalid)?;
        let resp = self
            .client
            .execute(req)
            .await
            // The error names the provider's URL, which is for logs only.
            .map_err(|e| match e.is_builder() {
                true => invalid(e),
                false => Upstream::fault("the provider could not be reached").with_source(e),
            })?;
        let broke = |e| Upstream::fault("the provider's body broke off").with_source(e);
        let mut resp = http::Response::from(resp)
            .map(|body| Body::stream(body.map_err(broke).into_data_stream()));
        strip_origin_headers(resp.headers_mut());
        Ok((resp, None))
    }
    /// A provider factory sharing this client's pool and configuration.
    pub fn connect(
        self,
    ) -> impl Fn(respan_gateway_sdk::Extensions) -> Outgoing + Clone + Send + Sync + 'static {
        let client = Arc::new(self);
        crate::connect(move |request| {
            let client = client.clone();
            async move { client.send(request).await }
        })
    }
}
