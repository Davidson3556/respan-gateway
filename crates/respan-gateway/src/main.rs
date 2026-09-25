use std::process::ExitCode;

use respan_gateway_core::{Builder, Config, Fault};
use respan_gateway_formats::OpenAiChat;

const USAGE: &str = "usage: respan-gateway [--config <path>] [check]";
const DEFAULT_LISTEN: &str = "0.0.0.0:8080";

/// Every format and plugin this binary can use. A custom gateway is a copy of
/// this function with more registrations.
fn builder() -> Builder {
    Builder::new().format(OpenAiChat)
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
                println!("respan-gateway {}", respan_gateway::VERSION);
                return ExitCode::SUCCESS;
            }
            _ => return fail(USAGE),
        }
    }

    let config = match load(&config_path) {
        Ok(c) => c,
        Err(e) => return fail(&format!("{config_path}: {e}")),
    };
    let gateway = match builder().build(&config) {
        Ok(g) => g,
        Err(e) => return fail(&format!("{config_path}: {}", e.message())),
    };
    if check_only {
        println!("{config_path}: ok");
        return ExitCode::SUCCESS;
    }

    let addr = config.listen.as_deref().unwrap_or(DEFAULT_LISTEN);
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => return fail(&format!("listen on {addr}: {e}")),
    };
    println!(
        "respan-gateway {} listening on {addr}",
        respan_gateway::VERSION
    );
    let shutdown = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    let served = respan_gateway_server::serve(gateway.clone(), listener, shutdown).await;
    gateway.idle().await;
    match served {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => fail(&e.to_string()),
    }
}

fn load(path: &str) -> Result<Config, Fault> {
    let text = std::fs::read_to_string(path).map_err(|e| Fault::config(e.to_string()))?;
    serde_norway::from_str(&text).map_err(|e| Fault::config(e.to_string()))
}

fn fail(message: &str) -> ExitCode {
    eprintln!("{message}");
    ExitCode::FAILURE
}
