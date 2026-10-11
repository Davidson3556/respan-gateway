//! Serving, and stopping gracefully.

use std::future::{Future, IntoFuture};

use respan_gateway_core::Gateway;
use respan_gateway_core::futures::future::join_all;
use tokio::net::TcpListener;
use tokio::time::Instant;

use super::{App, SHUTDOWN};
use crate::logging::Level;

impl App {
    /// Serves on `listener` until `shutdown` resolves, then shuts down
    /// gracefully: open sessions close 1001, and open responses and then
    /// end hooks share one deadline, the config's `shutdown_grace` (30 s).
    /// Responses open at it are cut and lose their end hooks.
    pub async fn serve(
        &self,
        listener: TcpListener,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> std::io::Result<()> {
        let addr = listener.local_addr()?;
        let process = &self.shared.process;
        let _ = process.listen.set(addr);
        self.shared.current().local_is_on(addr);
        let stopping = process.stopping.clone();
        let shared = self.shared.clone();
        let grace = move || shared.current().config.shutdown_grace.unwrap_or(SHUTDOWN);
        let (deadline_sender, mut deadline) = tokio::sync::oneshot::channel();
        let serve = axum::serve(listener, self.router.clone())
            .with_graceful_shutdown({
                let grace = grace.clone();
                async move {
                    shutdown.await;
                    stopping.send_replace(true);
                    let _ = deadline_sender.send(Instant::now() + grace());
                }
            })
            .into_future();
        let log = self.shared.current().config.log;
        let mut serve = std::pin::pin!(serve);
        let mut at = None;
        let served = tokio::select! {
            served = &mut serve => served,
            Ok(deadline) = &mut deadline => {
                at = Some(deadline);
                match tokio::time::timeout_at(deadline, &mut serve).await {
                    Ok(served) => served,
                    Err(_) => {
                        log.say(Level::Warn, "shutdown: responses still open at the deadline are cut");
                        Ok(())
                    }
                }
            }
        };
        let at = at
            .or_else(|| deadline.try_recv().ok())
            .unwrap_or_else(|| Instant::now() + grace());
        let gateways = self.shared.gateways();
        let idle = join_all(gateways.iter().map(Gateway::idle));
        if tokio::time::timeout_at(at, idle).await.is_err() {
            log.say(
                Level::Warn,
                "shutdown: end hooks still running at the deadline were dropped",
            );
        }
        served
    }
}

/// Resolves on Ctrl-C, or on SIGTERM where there is one.
pub async fn shutdown_signal() {
    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(_) => std::future::pending().await,
        }
    };
    // Ctrl-Break, what a process manager sends a console program's group.
    #[cfg(windows)]
    let terminate = async {
        match tokio::signal::windows::ctrl_break() {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(_) => std::future::pending().await,
        }
    };
    #[cfg(not(any(unix, windows)))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        () = terminate => {}
    }
}
