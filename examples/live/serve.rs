//! Stop accepting clients and close every upgraded session before returning.
use axum::{
    Router,
    extract::{Request, State, ws::WebSocketUpgrade},
};
use respan_gateway_sdk::Gateway;
use tokio::sync::watch;

pub async fn run(gateway: Gateway, address: &str) -> Result<(), std::io::Error> {
    let (stop, stopped) = watch::channel(false);
    let app = Router::new()
        .fallback(
            move |State(gw): State<Gateway>, ws: WebSocketUpgrade, req: Request| {
                let mut stopped = stopped.clone();
                async move {
                    respan_gateway_axum::ws::serve_ws_until(&gw, ws, req, async move {
                        while !*stopped.borrow_and_update() {
                            if stopped.changed().await.is_err() {
                                break;
                            }
                        }
                        "draining"
                    })
                    .await
                }
            },
        )
        .with_state(gateway.clone());
    let listener = tokio::net::TcpListener::bind(address).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = tokio::signal::ctrl_c().await;
            let _ = stop.send(true);
        })
        .await?;
    gateway.idle().await;
    Ok(())
}
