#![warn(missing_docs)]
//! HTTP for the kernel: an axum router that hands requests to a [`Gateway`],
//! and a [`Transport`] that reaches a provider over HTTP.

use std::sync::OnceLock;
use std::time::Duration;

use axum::Router;
use axum::extract::{Request, State};
use axum::response::Response;
use http::header::{self, HeaderName};
use http::{HeaderMap, StatusCode};
use http_body_util::{BodyExt, Limited};
use respan_gateway_core::{Body, Fault, Gateway, Transport};
use respan_gateway_core::{async_trait, http, http_body_util};

pub use reqwest;

const MAX_BODY_BYTES: usize = 32 * 1024 * 1024;

/// A router sending every request to [`Gateway::handle`]. A body over
/// 32 MiB is refused 413 `too_large`, and one that breaks off 400
/// `bad_request`, both in the client's format.
pub fn router(gateway: Gateway) -> Router {
    Router::new().fallback(handle).with_state(gateway)
}

async fn handle(State(gateway): State<Gateway>, req: Request) -> Response {
    let req = req.map(|body| Limited::new(body, MAX_BODY_BYTES).map_err(body_fault));
    let (parts, body) = gateway.handle(req).await.into_parts();
    Response::from_parts(parts, axum::body::Body::new(body))
}

/// Why a client's body did not arrive whole.
fn body_fault(e: axum::BoxError) -> Fault {
    if e.is::<http_body_util::LengthLimitError>() {
        let status = StatusCode::PAYLOAD_TOO_LARGE;
        return Fault::new(status, "too_large", "request body too large");
    }
    Fault::bad_request("the request body broke off")
}

/// Reaches one provider over HTTP(S) at a base URL, which each request's
/// path extends, with headers of its own. Instances share one connection
/// pool unless given their own client.
#[derive(Clone)]
pub struct HttpTransport {
    client: reqwest::Client,
    /// The base URL, without a trailing `/`.
    base: String,
    headers: HeaderMap,
}

impl std::fmt::Debug for HttpTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Header values may be keys.
        f.debug_struct("HttpTransport")
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

impl HttpTransport {
    /// A transport to `base_url`: `http` or `https`, with a host, a valid
    /// port, an optional path prefix such as `/v1`, and no query, fragment
    /// or credentials. With no [`with_headers`](Self::with_headers),
    /// clients' credentials pass through, as to a local proxy. `Err` with
    /// `config_error` for any other URL, or `internal_error` if the HTTP
    /// client cannot be built.
    pub fn new(base_url: impl Into<String>) -> Result<Self, Fault> {
        // The URL may hold a key, so the error does not echo it.
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
    /// set before. With any at all, clients' credentials are removed
    /// ([`add_own_headers`]). `Err` with `config_error` for a connection
    /// header, such as `Host` or `Content-Length`.
    pub fn with_headers(mut self, headers: HeaderMap) -> Result<Self, Fault> {
        if let Some(name) = headers.keys().find(|k| CONNECTION_HEADERS.contains(k)) {
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
}

/// Headers the kernel strips from every call ([`Transport::send`]).
const CONNECTION_HEADERS: [HeaderName; 12] = [
    header::CONNECTION,
    header::CONTENT_LENGTH,
    header::EXPECT,
    header::HOST,
    HeaderName::from_static("keep-alive"),
    HeaderName::from_static("proxy-connection"),
    header::PROXY_AUTHENTICATE,
    header::PROXY_AUTHORIZATION,
    header::TE,
    header::TRAILER,
    header::TRANSFER_ENCODING,
    header::UPGRADE,
];

#[async_trait]
impl Transport for HttpTransport {
    async fn send(&self, req: http::Request<Body>) -> Result<http::Response<Body>, Fault> {
        let (mut parts, body) = req.into_parts();
        add_own_headers(&mut parts, &self.headers)?;
        let path = parts.uri.path_and_query().map_or("/", |p| p.as_str());
        let url = format!("{}{path}", self.base);
        parts.uri = url
            .parse()
            .map_err(|e| Fault::config("the provider request is not valid").with_source(e))?;
        let body = body.collect().await?.to_bytes();
        let req = reqwest::Request::try_from(http::Request::from_parts(parts, body))
            .map_err(|e| Fault::config("the provider request is not valid").with_source(e))?;
        let resp = self
            .client
            .execute(req)
            .await
            // The error names the provider's URL, which is for logs only.
            .map_err(|e| match e.is_builder() {
                true => Fault::config("the provider request is not valid").with_source(e),
                false => Fault::upstream("the provider could not be reached").with_source(e),
            })?;
        let broke = |e| Fault::upstream("the provider's body broke off").with_source(e);
        Ok(http::Response::from(resp).map(|body| body.map_err(broke).boxed_unsync()))
    }
}

/// Headers for one call, sent as the transport's own and replacing those
/// of the same name from [`HttpTransport::with_headers`], such as a
/// tenant's key set in `on_provider`
/// (`px.extensions_mut().insert(CallHeaders(headers))`).
#[derive(Clone, Default)]
pub struct CallHeaders(pub HeaderMap);

impl std::fmt::Debug for CallHeaders {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Header values may be keys.
        let names: Vec<_> = self.0.keys().collect();
        f.debug_tuple("CallHeaders").field(&names).finish()
    }
}

/// Write a transport's own `headers`, then the call's [`CallHeaders`]
/// (minus connection headers), onto a call's `head`, each replacing any of
/// the same name. If there are any, the client's credential headers and
/// query parameters are removed first; otherwise the client's pass
/// through. A credential is a name whose last `-` or `_` segment is
/// `authorization`, `auth`, `key`, `token`, `secret`, `password`,
/// `credential` or `credentials`, except `idempotency-key` and
/// `x-idempotency-key`.
///
/// For any transport keeping this rule, such as a mock. `Err` with
/// `config_error` if the query cannot be rewritten.
pub fn add_own_headers(head: &mut http::request::Parts, headers: &HeaderMap) -> Result<(), Fault> {
    let mut own = headers.clone();
    if let Some(CallHeaders(call)) = head.extensions.get::<CallHeaders>() {
        let sent = |n: &&HeaderName| !CONNECTION_HEADERS.contains(*n);
        for name in call.keys().filter(sent) {
            own.remove(name);
        }
        for (name, value) in call.iter().filter(|(n, _)| sent(n)) {
            own.append(name, value.clone());
        }
    }
    if own.is_empty() {
        return Ok(());
    }
    let names: Vec<HeaderName> = head.headers.keys().cloned().collect();
    for name in names.iter().filter(|n| is_credential(n.as_str())) {
        head.headers.remove(name);
    }
    if let Some(query) = head.uri.query() {
        let kept: Vec<&str> = query
            .split('&')
            .filter(|p| !is_credential(p.split('=').next().unwrap_or_default()))
            .collect();
        let path = head.uri.path();
        let uri = match kept.is_empty() {
            true => path.to_owned(),
            false => format!("{path}?{}", kept.join("&")),
        };
        head.uri = uri
            .parse()
            .map_err(|e| Fault::config("the provider request is not valid").with_source(e))?;
    }
    for name in own.keys() {
        head.headers.remove(name);
    }
    head.headers.extend(own);
    Ok(())
}

/// Whether a header or query parameter `name` names a credential
/// ([`add_own_headers`]).
fn is_credential(name: &str) -> bool {
    const CREDENTIALS: [&str; 8] = [
        "authorization",
        "auth",
        "key",
        "token",
        "secret",
        "password",
        "credential",
        "credentials",
    ];
    let name = name.to_ascii_lowercase();
    let last = name.rsplit(['-', '_']).next().unwrap_or_default();
    let idempotency = matches!(&*name, "idempotency-key" | "x-idempotency-key");
    !idempotency && CREDENTIALS.contains(&last)
}
