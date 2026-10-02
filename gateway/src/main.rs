use std::future::IntoFuture;
use std::process::ExitCode;
use std::time::Duration;

use respan_gateway_axum::router;
use respan_gateway_http::formats::opaque::{Got, Passthrough, Sent};
use respan_gateway_http::http::HeaderMap;
use respan_gateway_http::{HttpClient, Outgoing};
use respan_gateway_providers::{AnthropicMessages, OpenAiChat};
use respan_gateway_sdk::conversation::{Conversation, Reply};
use respan_gateway_sdk::{Extensions, Fault};

mod config;

use config::KnownFormat;

const USAGE: &str = "usage: respan-gateway [-c|--config <path>] [check] [-V|--version]
       respan-gateway proxy (--anthropic | --openai) [--url <base_url>] [--listen <addr>]

proxy needs no config file: it relays to the vendor with each client's own
credentials, chat and every other endpoint of the vendor, byte for byte.";
const DEFAULT_LISTEN: std::net::SocketAddr =
    std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 8080);
const VERSION: &str = env!("CARGO_PKG_VERSION");
/// How long open responses and end hooks get, together, after a signal.
const SHUTDOWN: Duration = Duration::from_secs(30);

/// Every format this binary speaks. A custom gateway builds its own
/// `Gateway` in its own `main.rs`.
fn formats() -> [KnownFormat; 3] {
    [
        KnownFormat::of::<OpenAiChat, Conversation, Reply>(),
        KnownFormat::of::<AnthropicMessages, Conversation, Reply>(),
        KnownFormat::of::<Passthrough, Sent<Passthrough>, Got<Passthrough>>(),
    ]
}

/// The config of `proxy`: one vendor, no keys of its own, its chat format
/// and every other request relayed raw to the origin of `url`.
fn proxy_config(anthropic: bool, url: Option<&str>) -> config::Config {
    let (format, default) = match anthropic {
        true => ("anthropic-messages", "https://api.anthropic.com"),
        false => ("openai-chat", "https://api.openai.com/v1"),
    };
    let url = url.unwrap_or(default).trim_end_matches('/');
    // Every call is a raw relay of the client's own path, so both providers
    // sit at the vendor's origin.
    let origin = url.strip_suffix("/v1").unwrap_or(url);
    config::Config {
        listen: None,
        providers: [("vendor", format, origin), ("any", "passthrough", origin)]
            .into_iter()
            .map(|(name, format, base)| {
                (
                    name.to_owned(),
                    config::ProviderConfig {
                        format: format.to_owned(),
                        base_url: base.to_owned(),
                        auth: None,
                    },
                )
            })
            .collect(),
        serve: vec![format.to_owned(), "passthrough".to_owned()],
        plugins: Vec::new(),
        target: Some("vendor".to_owned()),
    }
}

/// A provider's transport: HTTP to `base_url`, sending `auth`.
fn http(
    base_url: &str,
    auth: HeaderMap,
) -> Result<impl Fn(Extensions) -> Outgoing + Send + Sync + use<>, Fault> {
    Ok(HttpClient::new(base_url)?.with_headers(auth)?.connect())
}

#[tokio::main]
async fn main() -> ExitCode {
    let mut config_path = "gateway.yaml".to_owned();
    let mut explicit_config = false;
    let mut check_only = false;
    // `proxy`: which vendor (true for Anthropic), its URL, where to listen.
    let (mut proxy, mut vendor, mut url, mut listen) = (false, None, None, None);
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" | "-c" => match args.next() {
                Some(p) => {
                    config_path = p;
                    explicit_config = true;
                }
                None => return fail(USAGE),
            },
            "check" => check_only = true,
            "proxy" => proxy = true,
            "--anthropic" | "--openai" if proxy && vendor.is_none() => {
                vendor = Some(arg == "--anthropic");
            }
            "--url" | "--listen" if proxy => {
                let Some(value) = args.next() else {
                    return fail(USAGE);
                };
                if arg == "--url" {
                    url = Some(value);
                } else {
                    listen = Some(value);
                }
            }
            "--version" | "-V" => {
                println!("respan-gateway {VERSION}");
                return ExitCode::SUCCESS;
            }
            _ => return fail(USAGE),
        }
    }

    if proxy && explicit_config {
        return fail("proxy cannot be used with --config; proxy builds its own configuration");
    }

    let loaded = match (proxy, vendor) {
        (true, Some(anthropic)) => {
            config_path = "proxy".to_owned();
            let mut config = proxy_config(anthropic, url.as_deref());
            listen
                .as_deref()
                .map(str::parse)
                .transpose()
                .map(|addr| {
                    config.listen = addr;
                    config
                })
                .map_err(|e| format!("--listen: {e}"))
        }
        (true, None) => return fail(USAGE),
        (false, _) => std::fs::read_to_string(&config_path)
            .map_err(|e| e.to_string())
            .and_then(|text| config::Config::parse(&text).map_err(|e| e.message().to_owned())),
    };
    let config = match loaded {
        Ok(c) => c,
        Err(e) => return fail(&format!("{config_path}: {e}")),
    };
    let built = config.build(http, &formats());
    let gateway = match built {
        Ok(built) => built,
        Err(e) => return fail(&format!("{config_path}: {}", e.message())),
    };
    let gateway = if proxy {
        gateway.plugin(respan_gateway_http::relay::Relay::RAW)
    } else {
        gateway
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
