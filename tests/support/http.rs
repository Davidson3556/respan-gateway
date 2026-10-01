//! HTTP requests and body collection for integration tests.

use futures::StreamExt;
use respan_gateway_http::Body;
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::http::{self, StatusCode};
use respan_gateway_http::http_body_util::BodyExt;
use respan_gateway_sdk::{Fault, futures};

pub use respan_gateway_http::serve;

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
