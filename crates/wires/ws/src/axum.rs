//! Axum hosting for a gateway's sessions, behind the `axum` feature:
//! [`router`].

use std::future::Future;
use std::sync::Arc;

use ::axum::Router;
use ::axum::extract::Request;
use ::axum::response::Response;
use hyper_util::rt::TokioIo;
use respan_gateway_core::Gateway;
use respan_gateway_core::futures::FutureExt;
use respan_gateway_http::axum::handle;
use respan_gateway_http::http::header;
use respan_gateway_http::http::request::Parts as RequestHead;

use crate::Limits;

/// A router that serves each WebSocket upgrade (`Upgrade: websocket`) as a
/// session, taking `limits` ([`serve`](crate::serve) says how a session
/// goes and ends), and every other request as
/// [`router_with_limit`](respan_gateway_http::axum::router_with_limit) does with `limit`. When
/// `stop` completes, every open session is closed 1001 (going away), so
/// that a server's graceful shutdown drains them too:
///
/// ```no_run
/// # use respan_gateway_core::futures::FutureExt;
/// # async fn run(gateway: respan_gateway_core::Gateway) -> std::io::Result<()> {
/// let (_stop, stopped) = tokio::sync::oneshot::channel::<()>();
/// let stopped = stopped.map(drop).shared();
/// let (limit, limits) = (|_: &_| 32 << 20, Default::default());
/// let app = respan_gateway_ws::axum::router(gateway.clone(), limit, limits, stopped.clone());
/// let listener = tokio::net::TcpListener::bind("127.0.0.1:8080").await?;
/// axum::serve(listener, app).with_graceful_shutdown(stopped).await?;
/// gateway.idle().await;
/// # Ok(())
/// # }
/// ```
///
/// Only HTTP/1.1 upgrades.
pub fn router(
    gateway: Gateway,
    limit: impl Fn(&RequestHead) -> usize + Send + Sync + 'static,
    limits: Limits,
    stop: impl Future<Output = ()> + Send + 'static,
) -> Router {
    let stop = stop.boxed().shared();
    let limit = Arc::new(limit);
    Router::new().fallback(move |mut req: Request| {
        let (gateway, stop, limit) = (gateway.clone(), stop.clone(), limit.clone());
        async move {
            let upgrade = req.headers().get(header::UPGRADE);
            if !upgrade.is_some_and(|v| v.as_bytes().eq_ignore_ascii_case(b"websocket")) {
                return handle(gateway, req, &*limit).await;
            }
            let upgraded = hyper::upgrade::on(&mut req).map(|io| io.ok().map(TokioIo::new));
            let head = req.into_parts().0;
            let resp = crate::serve(&gateway, head, upgraded, limits, stop).await;
            let (head, body) = resp.into_parts();
            Response::from_parts(head, ::axum::body::Body::new(body))
        }
    })
}
