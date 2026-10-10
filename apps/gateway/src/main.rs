use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{ArgGroup, Parser, Subcommand};
use respan_gateway::{App, Config, ProxyVendor};

/// An LLM gateway: one endpoint for OpenAI, Anthropic, Gemini and more,
/// translating between their APIs.
///
/// With no config file it needs only a provider key in the environment:
///
///   export OPENAI_API_KEY=sk-...   # and/or ANTHROPIC_API_KEY, GEMINI_API_KEY, ...
///   respan-gateway
///
/// It reads ./gateway.yaml if there is one; `respan-gateway init` writes a
/// starter.
#[derive(Debug, Parser)]
#[command(name = "respan-gateway", version, verbatim_doc_comment, after_help = EXAMPLES)]
struct Cli {
    /// The config file [default: ./gateway.yaml if present, else providers
    /// from the environment]
    #[arg(short, long, global = true, value_name = "FILE")]
    config: Option<PathBuf>,

    /// Where to listen, such as 127.0.0.1:8080, in place of the config's
    #[arg(long, global = true, value_name = "ADDR")]
    listen: Option<String>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Serve the gateway (the default)
    Serve,
    /// Check the configuration and exit
    Check,
    /// Write a commented starter config from the keys in the environment
    Init {
        /// Where to write it
        #[arg(short, long, value_name = "FILE", default_value = respan_gateway::DEFAULT_FILE)]
        output: PathBuf,
        /// Replace the file if it exists
        #[arg(long)]
        force: bool,
    },
    /// Relay one vendor's whole API, with each client's own credentials
    ///
    /// No config file and no keys of its own: chat and every other endpoint
    /// of the vendor, byte for byte. For Claude Code
    /// (ANTHROPIC_BASE_URL=http://127.0.0.1:8080) or Codex.
    #[command(group(ArgGroup::new("vendor").required(true).args(["anthropic", "openai"])))]
    Proxy {
        /// Relay to Anthropic
        #[arg(long)]
        anthropic: bool,
        /// Relay to OpenAI
        #[arg(long)]
        openai: bool,
        /// The vendor's base URL, in place of its public one
        #[arg(long, value_name = "URL")]
        url: Option<String>,
        /// `check`: check the proxy's configuration and exit
        #[arg(hide = true, value_parser = ["check"])]
        action: Option<String>,
    },
}

const EXAMPLES: &str = "Examples:
  respan-gateway                    Serve ./gateway.yaml, or the providers in the environment
  respan-gateway init               Write a starter ./gateway.yaml
  respan-gateway check -c my.yaml   Check a config
  respan-gateway proxy --anthropic  Sit in front of Claude Code";

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let (config, check_only) = match cli.command.unwrap_or(Command::Serve) {
        Command::Init { output, force } => return init(&output, force),
        Command::Proxy {
            anthropic,
            url,
            action,
            ..
        } => {
            if cli.config.is_some() {
                return fail(
                    "proxy cannot be used with --config; proxy builds its own configuration",
                );
            }
            let vendor = match anthropic {
                true => ProxyVendor::Anthropic,
                false => ProxyVendor::OpenAi,
            };
            (Ok(Config::proxy(vendor, url.as_deref())), action.is_some())
        }
        Command::Serve => (Config::discover(cli.config.as_deref()), false),
        Command::Check => (Config::discover(cli.config.as_deref()), true),
    };
    let mut config = match config {
        Ok(config) => config,
        Err(e) => return fail(e.message()),
    };
    if let Some(listen) = &cli.listen {
        match listen.parse::<SocketAddr>() {
            Ok(addr) => config.listen = Some(addr),
            Err(e) => return fail(&format!("--listen: {e}")),
        }
    }
    let source = config.source().to_string();
    let app = match App::new(config) {
        Ok(app) => app,
        Err(e) => return fail(&format!("{source}: {e}")),
    };
    if check_only {
        println!("{source}: ok");
        return ExitCode::SUCCESS;
    }

    let addr = app.listen();
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => return fail(&format!("listen on {addr}: {e}")),
    };
    let bound = listener.local_addr().unwrap_or(addr);
    println!("{}", app.banner(bound));
    if let Some(warning) = app.warning(bound) {
        eprintln!("{warning}");
    }
    match app.serve(listener, respan_gateway::shutdown_signal()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => fail(&e.to_string()),
    }
}

fn init(output: &std::path::Path, force: bool) -> ExitCode {
    if output.exists() && !force {
        let path = output.display();
        return fail(&format!("{path} exists; add --force to replace it"));
    }
    let config = respan_gateway::starter_config();
    if let Err(e) = std::fs::write(output, config) {
        return fail(&format!("write {}: {e}", output.display()));
    }
    println!(
        "wrote {}; check it with `respan-gateway check`, run it with `respan-gateway`",
        output.display()
    );
    ExitCode::SUCCESS
}

fn fail(message: &str) -> ExitCode {
    eprintln!("{message}");
    ExitCode::FAILURE
}
