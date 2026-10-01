#![warn(missing_docs)]
//! Axum HTTP hosting for the gateway. WebSocket hosting is available under
//! the optional `ws` module (enable the `ws` feature).

use std::sync::Arc;

use axum::Router;
use axum::extract::Request;
use axum::response::Response;
use http::header;
use http::{HeaderMap, HeaderValue, StatusCode};
use http_body_util::{BodyExt, Limited};
use respan_gateway_http::{Status, http::request::Parts as RequestHead};
use respan_gateway_sdk::{Fault, Gateway};

#[cfg(feature = "ws")]
pub mod ws;

const MAX_BODY_BYTES: usize = 32 * 1024 * 1024;

/// A router sending every request through the HTTP adapter
/// ([`respan_gateway_http::handle`]). It switches no protocols, so it
/// ignores `Upgrade` and reads every body whole: over 32 MiB is refused 413
/// `too_large`, one that breaks off 400 `bad_request`.
pub fn router(gateway: Gateway) -> Router {
    router_with_limit(gateway, |_| MAX_BODY_BYTES)
}

/// An HTTP router with a per-request body limit in bytes. The callback sees
/// the HTTP head before the body is read, so paths or authenticated adapter
/// extensions can select a limit. Oversized bodies receive a 413 fault.
/// Decoding and protocol recognition still belong to the gateway's formats.
pub fn router_with_limit(
    gateway: Gateway,
    limit: impl Fn(&RequestHead) -> usize + Send + Sync + 'static,
) -> Router {
    let limit = Arc::new(limit);
    Router::new().fallback(move |req: Request| {
        let gateway = gateway.clone();
        let limit = limit.clone();
        async move {
            let (head, body) = req.into_parts();
            let max = limit(&head);
            handle(gateway, Request::from_parts(head, body), max).await
        }
    })
}

async fn handle(gateway: Gateway, mut req: Request, max: usize) -> Response {
    ignore_upgrade(req.headers_mut());
    let req = req.map(|body| Limited::new(body, max).map_err(body_fault).boxed_unsync());
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
