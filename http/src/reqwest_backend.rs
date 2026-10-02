//! A pooled Reqwest backend for outbound HTTP provider calls.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use http::HeaderMap;
use http_body_util::BodyExt;
use respan_gateway_sdk::{Fault, Upstream};

use crate::{Body, Completion, Outgoing, is_connection_header, prepare_call, strip_origin_headers};

/// A call the provider cannot be sent; the error is for logs.
fn invalid(e: impl std::error::Error + Send + Sync + 'static) -> Fault {
    Fault::config("the provider request is not valid").with_source(e)
}

/// How long connecting to a provider may take by default: TCP and TLS to a
/// reachable host take well under a second, and 10 s spans TCP's first
/// retransmits of a lost handshake (RFC 6298: after 1 s, then 2 s more, then
/// 4 s more), so a fallback still tries its next provider while the client
/// waits.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// The most bytes of one answer read at once by default: 32 MiB, the
/// largest request the vendors' JSON APIs take (Anthropic's Messages API),
/// so an answer may be as large as a request may be.
const LIMIT: usize = 32 << 20;

/// Reaches one provider over HTTP(S) at a base URL, which each call's path
/// extends, sharing one connection pool unless given a client. It prepares
/// a call ([`prepare_call`]) and strips the response
/// ([`strip_origin_headers`]). It sets no timeout on an answer: timing is
/// [`watchdog`](crate::watchdog)'s.
#[derive(Clone)]
pub struct HttpClient {
    client: reqwest::Client,
    /// The base URL, without a trailing `/`.
    base: String,
    headers: HeaderMap,
    limit: usize,
}

impl std::fmt::Debug for HttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Header values may be keys.
        f.debug_struct("HttpClient")
            .field("base", &self.base)
            .field("headers", &self.headers.keys().collect::<Vec<_>>())
            .field("limit", &self.limit)
            .finish_non_exhaustive()
    }
}

/// A client that returns redirects unfollowed, as they are the client's to
/// follow, and gives up connecting after `connect`.
fn client(connect: Duration) -> Result<reqwest::Client, Fault> {
    reqwest::Client::builder()
        .connect_timeout(connect)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|e| Fault::internal("the HTTP client could not be built").with_source(e))
}

/// The client every transport shares: [`client`] with [`CONNECT_TIMEOUT`].
fn shared_client() -> Result<reqwest::Client, Fault> {
    static CLIENT: OnceLock<Result<reqwest::Client, Fault>> = OnceLock::new();
    CLIENT.get_or_init(|| client(CONNECT_TIMEOUT)).clone()
}

impl HttpClient {
    /// A transport to `base_url`: `http` or `https` ([`base_url`](crate::base_url)),
    /// else
    /// `Err` with `config`. It connects within 10 s and reads up to 32 MiB
    /// of one answer at once, until told otherwise.
    pub fn new(base_url: impl Into<String>) -> Result<Self, Fault> {
        Ok(Self {
            client: shared_client()?,
            base: crate::base_url(&base_url.into(), &["http", "https"])?,
            headers: HeaderMap::new(),
            limit: LIMIT,
        })
    }

    /// Send `headers`, such as the API key, with every call, replacing any
    /// set before, in place of the client's ([`credentials`](crate::credentials)).
    /// `Err` with `config` for a connection header.
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

    /// Give up connecting after `timeout`, with a pool of its own. `Err`
    /// with `internal` if the client cannot be built.
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Result<Self, Fault> {
        self.client = client(timeout)?;
        Ok(self)
    }

    /// Read up to `bytes` of one answer at once ([`connect`](crate::connect)).
    pub fn with_limit(mut self, bytes: usize) -> Self {
        self.limit = bytes;
        self
    }

    /// Send an encoded call and return its response.
    pub async fn send(
        &self,
        mut call: http::Request<Body>,
    ) -> Result<(http::Response<Body>, Option<Completion>), Fault> {
        prepare_call(&mut call, &self.base, &self.headers)?;
        let (parts, body) = call.into_parts();
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
        let limit = self.limit;
        let client = Arc::new(self);
        crate::connect(
            move |request| {
                let client = client.clone();
                async move { client.send(request).await }
            },
            limit,
        )
    }
}
