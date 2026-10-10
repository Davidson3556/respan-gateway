#![warn(missing_docs)]
//! Axum HTTP hosting for the gateway. WebSocket hosting, [`router_with_ws`],
//! is behind the optional `ws` feature.

use std::sync::Arc;

use axum::Router;
use axum::extract::Request;
use axum::response::Response;
use http_body_util::BodyExt;
use respan_gateway_core::{BadRequest, Gateway};
use respan_gateway_http::Framing;
use respan_gateway_http::http::request::Parts as RequestHead;

#[cfg(feature = "ws")]
mod ws;
#[cfg(feature = "ws")]
pub use ws::router_with_ws;

/// The most bytes of a request [`router`] reads: 32 MiB, the largest request
/// the vendors' JSON APIs take (Anthropic's Messages API), so no request a
/// provider would take is refused here.
const MAX_BODY_BYTES: usize = 32 << 20;

/// A router sending every request through the HTTP adapter
/// ([`respan_gateway_http::handle`]), each body whole ([`Framing::Whole`]):
/// it switches no protocols, so `Upgrade` goes, and over 32 MiB is refused
/// 413 `too_large`, one that breaks off 400 `bad_request`.
pub fn router(gateway: Gateway) -> Router {
    router_with_limit(gateway, |_| MAX_BODY_BYTES)
}

/// An HTTP router with a per-request body limit in bytes, the adapter's
/// ([`respan_gateway_http::handle`]). The callback sees the HTTP head before
/// the body is read, so paths or authenticated adapter extensions can
/// select a limit. Oversized bodies receive a 413 fault.
/// Decoding and protocol recognition still belong to the gateway's formats.
pub fn router_with_limit(
    gateway: Gateway,
    limit: impl Fn(&RequestHead) -> usize + Send + Sync + 'static,
) -> Router {
    let limit = Arc::new(limit);
    Router::new().fallback(move |req: Request| {
        let (gateway, limit) = (gateway.clone(), limit.clone());
        async move { handle(gateway, req, &*limit).await }
    })
}

async fn handle(
    gateway: Gateway,
    req: Request,
    limit: &(dyn Fn(&RequestHead) -> usize + Send + Sync),
) -> Response {
    let (head, body) = req.into_parts();
    let max = limit(&head);
    let broke = |_| BadRequest::fault("the request body broke off");
    let req = Request::from_parts(head, body.map_err(broke).boxed_unsync());
    let (resp, _ended) = respan_gateway_http::handle(&gateway, req, Framing::Whole, max).await;
    let (parts, body) = resp.into_parts();
    Response::from_parts(parts, axum::body::Body::new(body))
}
