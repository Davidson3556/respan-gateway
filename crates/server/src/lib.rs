//! HTTP adapter for the kernel. The kernel has no server inside it; this crate
//! is one way to put it on the network. It maps method and path to a route,
//! and attaches [`HttpHead`] and [`Credential`] to the request.

use std::future::Future;

use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::response::{IntoResponse, Response};
use http::header::AUTHORIZATION;
use http::{HeaderMap, StatusCode};
use respan_gateway_core::{Credential, Gateway, HttpHead, Inbound, plain_error};
use tokio::net::TcpListener;

/// Default cap on a buffered request body.
pub const MAX_BODY_BYTES: usize = 32 * 1024 * 1024;

#[derive(Clone)]
struct AppState {
    gateway: Gateway,
    max_body: usize,
}

/// Every request goes to the gateway's HTTP routes.
pub fn router(gateway: Gateway, max_body: usize) -> Router {
    Router::new()
        .fallback(handle)
        .with_state(AppState { gateway, max_body })
}

/// Serve until `shutdown` resolves, then stop accepting and let open responses
/// finish.
pub async fn serve(
    gateway: Gateway,
    listener: TcpListener,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    axum::serve(listener, router(gateway, MAX_BODY_BYTES))
        .with_graceful_shutdown(shutdown)
        .await
}

async fn handle(State(state): State<AppState>, req: Request) -> Response {
    let (parts, body) = req.into_parts();
    let Some(route) = state
        .gateway
        .http_route(&parts.method, parts.uri.path())
        .map(str::to_owned)
    else {
        return into_axum(plain_error(StatusCode::NOT_FOUND, "no route"));
    };
    let bytes = match axum::body::to_bytes(body, state.max_body).await {
        Ok(b) => b,
        Err(_) => {
            return (StatusCode::PAYLOAD_TOO_LARGE, "request body too large").into_response();
        }
    };
    let mut inbound = Inbound::new(route, bytes);
    if let Some(c) = credential(&parts.headers) {
        inbound.ext.insert(c);
    }
    inbound.ext.insert(HttpHead {
        method: parts.method,
        uri: parts.uri,
        headers: parts.headers,
    });
    into_axum(state.gateway.handle(inbound).await)
}

/// `Authorization: Bearer <key>`, or `x-api-key: <key>`.
fn credential(headers: &HeaderMap) -> Option<Credential> {
    let bearer = headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    let api_key = headers.get("x-api-key").and_then(|v| v.to_str().ok());
    bearer.or(api_key).map(|k| Credential::new(k.trim()))
}

fn into_axum(resp: http::Response<respan_gateway_core::Body>) -> Response {
    let (parts, body) = resp.into_parts();
    Response::from_parts(parts, Body::from_stream(body))
}
