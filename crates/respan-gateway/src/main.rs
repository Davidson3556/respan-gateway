use std::future::IntoFuture;
use std::process::ExitCode;
use std::time::Duration;

use respan_gateway_core::Fault;
use respan_gateway_core::http::HeaderMap;
use respan_gateway_formats::{AnthropicMessages, OpenAiChat};
use respan_gateway_server::{HttpTransport, router};

mod config;

use config::KnownFormat;

const USAGE: &str = "usage: respan-gateway [-c|--config <path>] [check] [-V|--version]";
const DEFAULT_LISTEN: std::net::SocketAddr =
    std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 8080);
const VERSION: &str = env!("CARGO_PKG_VERSION");
/// How long open responses and end hooks get, together, after a signal.
const SHUTDOWN: Duration = Duration::from_secs(30);

/// Every format this binary speaks. A custom gateway builds its own
/// `Gateway` in its own `main.rs`.
fn formats() -> [KnownFormat; 2] {
    [
        KnownFormat::of::<OpenAiChat>(),
        KnownFormat::of::<AnthropicMessages>(),
    ]
}

/// A provider's transport: HTTP to `base_url`, sending `auth`.
fn http(base_url: &str, auth: HeaderMap) -> Result<HttpTransport, Fault> {
    HttpTransport::new(base_url)?.with_headers(auth)
}

#[tokio::main]
async fn main() -> ExitCode {
    let mut config_path = "gateway.yaml".to_owned();
    let mut check_only = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" | "-c" => match args.next() {
                Some(p) => config_path = p,
                None => return fail(USAGE),
            },
            "check" => check_only = true,
            "--version" | "-V" => {
                println!("respan-gateway {VERSION}");
                return ExitCode::SUCCESS;
            }
            _ => return fail(USAGE),
        }
    }

    let loaded = std::fs::read_to_string(&config_path)
        .map_err(|e| e.to_string())
        .and_then(|text| config::Config::parse(&text).map_err(|e| e.message().to_owned()));
    let config = match loaded {
        Ok(c) => c,
        Err(e) => return fail(&format!("{config_path}: {e}")),
    };
    let built = config.build(http, &formats());
    let gateway = match built {
        Ok(built) => built,
        Err(e) => return fail(&format!("{config_path}: {}", e.message())),
    };
    if check_only {
        println!("{config_path}: ok");
        return ExitCode::SUCCESS;
    }

    let addr = config.listen.unwrap_or(DEFAULT_LISTEN);
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => return fail(&format!("listen on {addr}: {e}")),
    };
    let bound = listener.local_addr().unwrap_or(addr);
    println!("respan-gateway {VERSION} listening on {bound}");
    // On a signal, open responses and then end hooks share one deadline.
    // Responses open at it are cut and lose their end hooks.
    let (stopping, mut stopped) = tokio::sync::oneshot::channel();
    let serve = axum::serve(listener, router(gateway.clone()))
        .with_graceful_shutdown(async {
            shutdown().await;
            let _ = stopping.send(tokio::time::Instant::now() + SHUTDOWN);
        })
        .into_future();
    let mut serve = std::pin::pin!(serve);
    let mut deadline = None;
    let served = tokio::select! {
        served = &mut serve => served,
        Ok(at) = &mut stopped => {
            deadline = Some(at);
            match tokio::time::timeout_at(at, &mut serve).await {
                Ok(served) => served,
                Err(_) => {
                    eprintln!("shutdown: responses still open at the deadline are cut");
                    Ok(())
                }
            }
        }
    };
    let deadline = deadline
        .or_else(|| stopped.try_recv().ok())
        .unwrap_or_else(|| tokio::time::Instant::now() + SHUTDOWN);
    if tokio::time::timeout_at(deadline, gateway.idle())
        .await
        .is_err()
    {
        eprintln!("shutdown: end hooks still running at the deadline were dropped");
    }
    match served {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => fail(&e.to_string()),
    }
}

/// Resolves on Ctrl-C, or on SIGTERM where there is one.
async fn shutdown() {
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
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        () = terminate => {}
    }
}

fn fail(message: &str) -> ExitCode {
    eprintln!("{message}");
    ExitCode::FAILURE
}
