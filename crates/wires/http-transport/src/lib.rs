#![warn(missing_docs)]
//! A pooled Reqwest backend for outbound HTTP provider calls: an
//! [`HttpTransport`] reaches one provider at a base URL. What a transport
//! does to a call and its answer is the HTTP wire's
//! (`respan_gateway_http::call`), shared with every transport.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use http::header::{AUTHORIZATION, COOKIE, LOCATION, PROXY_AUTHORIZATION};
use http::{HeaderMap, Method, StatusCode, Uri};
use http_body_util::BodyExt;
use respan_gateway_core::{Fault, Upstream};

use respan_gateway_http::{Body, Completion, ProviderIo};
use respan_gateway_http::{call::is_credential, call::prepare_call, call::strip_origin_headers};
use respan_gateway_http_syntax::is_connection_header;

/// The Reqwest this transport is built on, at its version, such as for
/// [`HttpTransport::with_client`].
pub use reqwest;

/// The most redirects a call follows.
const REDIRECTS: usize = 5;

/// Whether a redirect to a URI is followed.
type Allow = dyn Fn(&Uri) -> bool + Send + Sync;

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
/// [`watchdog`](respan_gateway_http::call::watchdog)'s.
#[derive(Clone)]
pub struct HttpTransport {
    client: reqwest::Client,
    /// The base URL, without a trailing `/`.
    base: String,
    headers: HeaderMap,
    limit: usize,
    follow: Option<Arc<Allow>>,
}

impl std::fmt::Debug for HttpTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Header values may be keys.
        f.debug_struct("HttpTransport")
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

impl HttpTransport {
    /// A transport to `base_url`: `http` or `https` ([`base_url`](respan_gateway_http::call::base_url)),
    /// else
    /// `Err` with `config`. It connects within 10 s and reads up to 32 MiB
    /// of one answer at once, until told otherwise.
    pub fn new(base_url: impl Into<String>) -> Result<Self, Fault> {
        Ok(Self {
            client: shared_client()?,
            base: respan_gateway_http::call::base_url(&base_url.into(), &["http", "https"])?,
            headers: HeaderMap::new(),
            limit: LIMIT,
            follow: None,
        })
    }

    /// Send `headers`, such as the API key, with every call, replacing any
    /// set before, in place of the client's ([`credentials`](respan_gateway_http::call::credentials)).
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
    /// follow redirects: they are the client's to follow, or
    /// [`follow_redirects`](Self::follow_redirects)'s, which keeps the call's
    /// credentials to its origin.
    pub fn with_client(mut self, client: reqwest::Client) -> Self {
        self.client = client;
        self
    }

    /// Follow a redirect of a `GET` or `HEAD` call to a location `allow`
    /// accepts, up to 5 in a row; any other answer is the client's to follow.
    /// A call that leaves its origin leaves its credentials behind: its
    /// `Authorization`, `Cookie` and every header or query parameter that
    /// names a key, token or secret.
    pub fn follow_redirects(
        mut self,
        allow: impl Fn(&Uri) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.follow = Some(Arc::new(allow));
        self
    }

    /// Give up connecting after `timeout`, with a pool of its own. `Err`
    /// with `internal` if the client cannot be built.
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Result<Self, Fault> {
        self.client = client(timeout)?;
        Ok(self)
    }

    /// Read up to `bytes` of one answer at once ([`connect`](respan_gateway_http::connect)).
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
        let (mut parts, body) = call.into_parts();
        let bodiless = !body.is_streaming() && body.full_bytes().is_none_or(|b| b.is_empty());
        let reads = matches!(parts.method, Method::GET | Method::HEAD);
        let follow = self.follow.as_ref().filter(|_| bodiless && reads);
        // A whole call is one frame, or none if empty, and goes with its length.
        let mut body = Some(body);
        let mut hops = 0;
        let resp = loop {
            let sent = match body.take() {
                Some(body) if body.is_streaming() => reqwest::Body::wrap_stream(body),
                Some(body) => body.full_bytes().cloned().unwrap_or_default().into(),
                None => reqwest::Body::from(Bytes::new()),
            };
            let mut head = http::Request::new(sent);
            *head.method_mut() = parts.method.clone();
            *head.uri_mut() = parts.uri.clone();
            *head.headers_mut() = parts.headers.clone();
            let req = reqwest::Request::try_from(head).map_err(invalid)?;
            let resp = self
                .client
                .execute(req)
                .await
                // The error names the provider's URL, which is for logs only.
                .map_err(|e| match e.is_builder() {
                    true => invalid(e),
                    false => Upstream::fault("the provider could not be reached").with_source(e),
                })?;
            let next = follow.and_then(|allow| redirect(&resp, &parts.uri, allow.as_ref()));
            match next {
                Some(next) if hops < REDIRECTS => {
                    if origin(&next) != origin(&parts.uri) {
                        leave_origin(&mut parts.headers);
                    }
                    parts.uri = next;
                    hops += 1;
                }
                _ => break resp,
            }
        };
        if resp.status() == http::StatusCode::SWITCHING_PROTOCOLS {
            return Err(Upstream::fault(
                "the provider switched protocols, which this transport does not",
            ));
        }
        let broke = |e| Upstream::fault("the provider's body broke off").with_source(e);
        let mut resp = http::Response::from(resp)
            .map(|body| Body::stream(body.map_err(broke).into_data_stream()));
        strip_origin_headers(resp.headers_mut());
        Ok((resp, None))
    }

    /// A provider factory sharing this client's pool and configuration.
    pub fn connect(
        self,
    ) -> impl Fn(respan_gateway_core::Extensions) -> ProviderIo + Clone + Send + Sync + 'static
    {
        let limit = self.limit;
        let client = Arc::new(self);
        respan_gateway_http::connect(
            move |request| {
                let client = client.clone();
                async move { client.send(request).await }
            },
            limit,
        )
    }
}

/// Where `resp`, an answer to a call to `from`, redirects it, if `allow`
/// accepts that.
fn redirect(resp: &reqwest::Response, from: &Uri, allow: &Allow) -> Option<Uri> {
    let redirects = matches!(
        resp.status(),
        StatusCode::MOVED_PERMANENTLY
            | StatusCode::FOUND
            | StatusCode::SEE_OTHER
            | StatusCode::TEMPORARY_REDIRECT
            | StatusCode::PERMANENT_REDIRECT
    );
    let location = resp.headers().get(LOCATION)?.to_str().ok()?;
    let to = reqwest::Url::parse(&from.to_string())
        .ok()?
        .join(location)
        .ok()?;
    let to: Uri = to.as_str().parse().ok()?;
    (redirects && allow(&to)).then_some(to)
}

/// A URI's scheme, host and port.
fn origin(uri: &Uri) -> (Option<&str>, Option<&str>, Option<u16>) {
    (uri.scheme_str(), uri.host(), uri.port_u16())
}

/// Remove from `headers` what only the origin they were meant for may see.
fn leave_origin(headers: &mut HeaderMap) {
    for name in [AUTHORIZATION, COOKIE, PROXY_AUTHORIZATION] {
        headers.remove(name);
    }
    let named: Vec<_> = headers
        .keys()
        .filter(|n| is_credential(n.as_str()))
        .cloned()
        .collect();
    for name in named {
        headers.remove(name);
    }
}
