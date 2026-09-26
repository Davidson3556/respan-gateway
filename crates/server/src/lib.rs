#![warn(missing_docs)]
//! HTTP for the kernel: an axum router that hands requests to a [`Gateway`],
//! and a [`Transport`] that reaches providers over HTTP.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::{Request, State};
use axum::response::{IntoResponse, Response};
use http::StatusCode;
use http_body_util::{BodyExt, Limited};
use respan_gateway_core::{Body, Fault, Gateway, Transport};
use respan_gateway_core::{async_trait, http, http_body_util};

const MAX_BODY_BYTES: usize = 32 * 1024 * 1024;

/// Serves each `(path, route)`: a request of any method to exactly `path`,
/// whatever its query, runs `route`. Any other path is a plain-text 404. A
/// body over 32 MiB is refused 413 `too_large`, and one that breaks off 400
/// `bad_request`, both in the route's format.
pub fn router<P, R>(gateway: Gateway, routes: impl IntoIterator<Item = (P, R)>) -> Router
where
    P: Into<String>,
    R: Into<String>,
{
    let routes = routes.into_iter().map(|(p, r)| (p.into(), r.into()));
    let routes: Arc<HashMap<String, String>> = Arc::new(routes.collect());
    let handler = move |State(gateway): State<Gateway>, req: Request| {
        let path = req.uri().path().to_owned();
        let route = routes.get(&path).cloned();
        async move {
            match route {
                Some(route) => handle(gateway, route, req).await,
                None => (StatusCode::NOT_FOUND, format!("no route at '{path}'")).into_response(),
            }
        }
    };
    Router::new().fallback(handler).with_state(gateway)
}

async fn handle(gateway: Gateway, route: String, req: Request) -> Response {
    let req = req.map(|body| Limited::new(body, MAX_BODY_BYTES).map_err(body_fault));
    let (parts, body) = gateway.handle(&route, req).await.into_parts();
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

/// Reaches providers over HTTPS or HTTP.
#[derive(Debug, Clone)]
pub struct HttpTransport {
    client: reqwest::Client,
}

impl HttpTransport {
    /// A transport that passes redirects on rather than following them, and
    /// gives up after 10 s connecting or 300 s of silence. `Err` with
    /// `internal_error` if the HTTP client cannot be built.
    pub fn new() -> Result<Self, Fault> {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .read_timeout(Duration::from_secs(300))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| Fault::internal("the HTTP client could not be built").with_source(e))?;
        Ok(Self { client })
    }
}

#[async_trait]
impl Transport for HttpTransport {
    async fn send(&self, req: http::Request<Body>) -> Result<http::Response<Body>, Fault> {
        let (parts, body) = req.into_parts();
        let body = body.collect().await?.to_bytes();
        let req = reqwest::Request::try_from(http::Request::from_parts(parts, body))
            .map_err(|e| Fault::config("the provider request is not valid").with_source(e))?;
        let resp = self
            .client
            .execute(req)
            .await
            // Its message names the provider's URL, which is for logs only.
            .map_err(|e| match e.is_builder() {
                true => Fault::config("the provider request is not valid").with_source(e),
                false => Fault::upstream("the provider could not be reached").with_source(e),
            })?;
        let broke = |e| Fault::upstream("the provider's body broke off").with_source(e);
        Ok(http::Response::from(resp).map(|body| body.map_err(broke).boxed_unsync()))
    }

    /// An `http` or `https` URL with a host and a nonzero port.
    fn reaches(&self, base_url: &str) -> bool {
        reqwest::Url::parse(base_url).is_ok_and(|u| {
            matches!(u.scheme(), "http" | "https") && u.has_host() && u.port() != Some(0)
        })
    }
}
