#![warn(missing_docs)]
//! HTTP for the kernel: an axum router that hands requests to a [`Gateway`]
//! through the HTTP adapter ([`respan_gateway_http::handle`]), and a
//! [`Transport`] that reaches a provider over HTTP.

use std::sync::OnceLock;
use std::time::Duration;

use axum::Router;
use axum::extract::{Request, State};
use axum::response::Response;
use http::header;
use http::{HeaderMap, HeaderValue, StatusCode};
use http_body_util::{BodyExt, Limited};
use respan_gateway_core::futures::TryStreamExt;
use respan_gateway_core::{Fault, Gateway, async_trait};
use respan_gateway_http::{Exchange, Transport};
use respan_gateway_http::{RequestHead, ResponseHead, Status, http, http_body_util};
use respan_gateway_http::{
    credentials, is_connection_header, proxy_call, strip_client_origin_headers,
    strip_origin_headers,
};

pub use reqwest;

const MAX_BODY_BYTES: usize = 32 * 1024 * 1024;

/// A router sending every request through the HTTP adapter
/// ([`respan_gateway_http::handle`]). It switches no protocols, so it
/// ignores `Upgrade` and reads every body whole: over 32 MiB is refused 413
/// `too_large`, one that breaks off 400 `bad_request`.
pub fn router(gateway: Gateway) -> Router {
    Router::new().fallback(handle).with_state(gateway)
}

async fn handle(State(gateway): State<Gateway>, mut req: Request) -> Response {
    ignore_upgrade(req.headers_mut());
    let req = req.map(|body| {
        Limited::new(body, MAX_BODY_BYTES)
            .map_err(body_fault)
            .boxed_unsync()
    });
    let (resp, _ended) = respan_gateway_http::handle(&gateway, req, false).await;
    let (parts, body) = resp.into_parts();
    Response::from_parts(parts, axum::body::Body::new(body))
}

/// Remove `Upgrade` and its `Connection` option (RFC 9110 7.8), keeping
/// the other options, which name headers the adapter strips.
fn ignore_upgrade(headers: &mut HeaderMap) {
    headers.remove(header::UPGRADE);
    let kept: Vec<HeaderValue> = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|value| {
            // A value that is not text is kept as it is.
            let Ok(text) = value.to_str() else {
                return Some(value.clone());
            };
            let options: Vec<&str> = text
                .split(',')
                .map(str::trim)
                .filter(|o| !o.is_empty() && !o.eq_ignore_ascii_case("upgrade"))
                .collect();
            HeaderValue::from_str(&options.join(", "))
                .ok()
                .filter(|v| !v.is_empty())
        })
        .collect();
    headers.remove(header::CONNECTION);
    for value in kept {
        headers.append(header::CONNECTION, value);
    }
}

/// Why a client's body did not arrive whole.
fn body_fault(e: axum::BoxError) -> Fault {
    match e.is::<http_body_util::LengthLimitError>() {
        true => Fault::new("too_large", "request body too large")
            .with(Status(StatusCode::PAYLOAD_TOO_LARGE.as_u16())),
        false => Fault::bad_request("the request body broke off"),
    }
}

/// A call the provider cannot be sent; the error is for logs.
fn invalid(e: impl std::error::Error + Send + Sync + 'static) -> Fault {
    Fault::config("the provider request is not valid").with_source(e)
}

/// Reaches one provider over HTTP(S) at a base URL, which each call's path
/// extends, sharing one connection pool unless given a client. It strips a
/// call ([`proxy_call`], [`strip_client_origin_headers`]), sends its keys
/// ([`credentials`]), and strips the response ([`strip_origin_headers`]).
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
}

#[async_trait]
impl Transport for HttpTransport {
    async fn send(&self, call: Exchange<RequestHead>) -> Result<Exchange<ResponseHead>, Fault> {
        let mut call = call;
        proxy_call(&mut call.head);
        strip_client_origin_headers(&mut call.head.headers);
        credentials(&mut call, &self.headers);
        let Exchange {
            head: mut parts,
            body,
            streams,
            ..
        } = call;
        let path = parts.uri.path_and_query().map_or("/", |p| p.as_str());
        let url = format!("{}{path}", self.base);
        parts.uri = url.parse().map_err(invalid)?;
        // A whole call is one frame, or none if empty, and goes with its length.
        let body = match streams {
            false => {
                let frames: Vec<_> = body.try_collect().await?;
                reqwest::Body::from(frames.concat())
            }
            true => reqwest::Body::wrap_stream(body),
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
                false => Fault::upstream("the provider could not be reached").with_source(e),
            })?;
        let broke = |e| Fault::upstream("the provider's body broke off").with_source(e);
        let resp = http::Response::from(resp).map(|body| body.map_err(broke));
        let mut resp = respan_gateway_http::from_response(resp);
        strip_origin_headers(&mut resp.head.headers);
        Ok(resp)
    }
}
