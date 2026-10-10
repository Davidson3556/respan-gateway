//! HTTP requests and body collection for integration tests.

use futures::StreamExt;
use respan_gateway_core::{Fault, futures};
use respan_gateway_http::Body;
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::http::{self, StatusCode};
use respan_gateway_http::http_body_util::BodyExt;

/// The most bytes of one message a test's adapter and transports hold.
pub const LIMIT: usize = 32 << 20;

/// Hand `req` to `gateway` as an HTTP server does: whole, up to [`LIMIT`].
pub fn serve<B>(
    gateway: &respan_gateway_core::Gateway,
    req: http::Request<B>,
) -> impl Future<
    Output = (
        http::Response<Body>,
        futures::future::BoxFuture<'static, Result<(), Fault>>,
    ),
> + Send
+ 'static
where
    B: respan_gateway_http::http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Fault>,
{
    respan_gateway_http::handle(gateway, req, respan_gateway_http::Framing::Whole, LIMIT)
}

/// Hand a session's `req` to `gateway`: live, a message per frame of its
/// body, up to [`LIMIT`] each.
pub fn serve_live<B>(
    gateway: &respan_gateway_core::Gateway,
    req: http::Request<B>,
) -> impl Future<
    Output = (
        http::Response<Body>,
        futures::future::BoxFuture<'static, Result<(), Fault>>,
    ),
> + Send
+ 'static
where
    B: respan_gateway_http::http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Fault>,
{
    respan_gateway_http::handle(gateway, req, respan_gateway_http::Framing::Live, LIMIT)
}

pub fn post(path: &str, body: impl Into<Bytes>) -> http::Request<Body> {
    let body = Body::full(body);
    let mut req = http::Request::new(body);
    *req.method_mut() = http::Method::POST;
    *req.uri_mut() = path.parse().unwrap_or_else(|e| panic!("path {path}: {e}"));
    req
}

pub fn streamed(
    path: &str,
) -> (
    futures::channel::mpsc::UnboundedSender<Bytes>,
    http::Request<Body>,
) {
    let (send, frames) = futures::channel::mpsc::unbounded::<Bytes>();
    let body = Body::stream(frames.map(Ok));
    let mut req = http::Request::new(body);
    *req.uri_mut() = path.parse().unwrap_or_else(|e| panic!("path {path}: {e}"));
    (send, req)
}

pub async fn read_whole(
    resp: http::Response<Body>,
) -> (StatusCode, http::HeaderMap, Result<Bytes, Fault>) {
    let (parts, body) = resp.into_parts();
    let body = BodyExt::collect(body).await.map(|b| b.to_bytes());
    (parts.status, parts.headers, body)
}
