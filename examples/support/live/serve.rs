//! Stop accepting clients and close every upgraded session before returning.
use respan_gateway_sdk::Gateway;
use respan_gateway_sdk::futures::FutureExt;
use std::future::Future;

pub async fn run(gateway: Gateway, address: &str) -> Result<(), std::io::Error> {
    let listener = tokio::net::TcpListener::bind(address).await?;
    serve_until(gateway, listener, async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await
}

/// The production drain path, with the listener and shutdown supplied by the caller.
pub async fn serve_until(
    gateway: Gateway,
    listener: tokio::net::TcpListener,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), std::io::Error> {
    let shutdown = shutdown.boxed().shared();
    let limits = respan_gateway_ws::Limits::default();
    let app = respan_gateway_axum::router_with_ws(
        gateway.clone(),
        |_| respan_gateway_examples::LIMIT,
        limits,
        shutdown.clone(),
    );
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await?;
    gateway.idle().await;
    Ok(())
}
