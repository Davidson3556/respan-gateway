//! Axum hosting for a gateway, behind the `axum` feature: each request goes
//! through the HTTP adapter ([`handle`](crate::handle)), and formats decide
//! what it is. A WebSocket upgrade is hosted by `respan_gateway_ws::axum`.

use std::sync::Arc;

use ::axum::Router;
use ::axum::extract::Request;
use ::axum::response::Response;
use http::request::Parts as RequestHead;
use http_body_util::BodyExt;
use respan_gateway_core::{BadRequest, Gateway};

use crate::Framing;

/// The most bytes of a request [`router`] reads: 32 MiB, the largest request
/// the vendors' JSON APIs take (Anthropic's Messages API), so no request a
/// provider would take is refused here.
const MAX_BODY_BYTES: usize = 32 << 20;

/// A router sending every request through the HTTP adapter
/// ([`handle`](crate::handle)), each body whole ([`Framing::Whole`]):
/// it switches no protocols, so `Upgrade` goes, and over 32 MiB is refused
/// 413 `too_large`, one that breaks off 400 `bad_request`.
pub fn router(gateway: Gateway) -> Router {
    router_with_limit(gateway, |_| MAX_BODY_BYTES)
}

/// An HTTP router with a per-request body limit in bytes, the adapter's
/// ([`handle`](crate::handle)). The callback sees the HTTP head before
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

/// One request as [`router_with_limit`] serves it: its body read whole up
/// to `limit`'s bytes for its head, then through the HTTP adapter
/// ([`handle`](crate::handle)). For a router that serves some requests
/// itself, such as `respan_gateway_ws::axum::router`.
pub async fn handle(
    gateway: Gateway,
    req: Request,
    limit: &(dyn Fn(&RequestHead) -> usize + Send + Sync),
) -> Response {
    let (head, body) = req.into_parts();
    let max = limit(&head);
    let broke = |_| BadRequest::fault("the request body broke off");
    let req = Request::from_parts(head, body.map_err(broke).boxed_unsync());
    let (resp, _ended) = crate::handle(&gateway, req, Framing::Whole, max).await;
    let (parts, body) = resp.into_parts();
    Response::from_parts(parts, ::axum::body::Body::new(body))
}
