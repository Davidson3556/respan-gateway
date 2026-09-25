//! HTTP adapter for the kernel. The kernel has no server inside it; this crate
//! is one way to put it on the network.

use std::future::Future;

use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::response::{IntoResponse, Response};
use http::StatusCode;
use respan_gateway_core::Gateway;
use tokio::net::TcpListener;

/// Default cap on a buffered request body.
pub const MAX_BODY_BYTES: usize = 32 * 1024 * 1024;

#[derive(Clone)]
struct AppState {
    gateway: Gateway,
    max_body: usize,
}

/// Every request goes to the gateway; routing happens there.
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
    let bytes = match axum::body::to_bytes(body, state.max_body).await {
        Ok(b) => b,
        Err(_) => return (StatusCode::PAYLOAD_TOO_LARGE, "request body too large").into_response(),
    };
    let resp = state
        .gateway
        .handle(http::Request::from_parts(parts, bytes))
        .await;
    let (parts, body) = resp.into_parts();
    Response::from_parts(parts, Body::from_stream(body))
}
