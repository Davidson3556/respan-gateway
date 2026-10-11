//! The `respan-gateway` command line.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

use crate::init::Chosen;
use crate::logging::{Level, LogFormat};
use crate::{App, Config, ProxyVendor, VERSION};
use clap::{ArgGroup, Parser, Subcommand};
use serde_json::{Map, json};

/// An LLM gateway: one endpoint for OpenAI, Anthropic, Gemini and more,
/// translating between their APIs.
///
/// With no config file it needs only a provider key in the environment:
///
///   export OPENAI_API_KEY=sk-...   # and/or ANTHROPIC_API_KEY, GEMINI_API_KEY, ...
///   respan-gateway
///
/// It reads ./gateway.yaml if there is one; `respan-gateway init` writes a
/// starter. A .env file in the current directory is read first, so keys and
/// the settings below may live there.
#[derive(Debug, Parser)]
#[command(name = "respan-gateway", version = VERSION, verbatim_doc_comment, after_help = EXAMPLES)]
struct Cli {
    /// The config file [default: ./gateway.yaml if present, else providers
    /// from the environment]
    #[arg(short, long, global = true, value_name = "FILE", env = "RESPAN_CONFIG")]
    config: Option<PathBuf>,

    /// Where to listen, such as 127.0.0.1:8080, localhost:8080 or 8080, in
    /// place of the config's (PORT alone sets the port)
    #[arg(long, global = true, value_name = "ADDR", env = "RESPAN_LISTEN")]
    listen: Option<String>,

    /// How requests are logged to stderr, in place of the config's
    #[arg(long, global = true, value_name = "FORMAT", env = "RESPAN_LOG")]
    log: Option<LogFormat>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Serve the gateway (the default)
    Serve,
    /// Check the configuration, print what it serves and exit
    Check {
        /// Also make one call to each provider with its key (listing its
        /// models) and say whether it is reachable, refuses the key or is
        /// unreachable; the exit status is 1 unless all are reachable
        #[arg(long)]
        online: bool,
    },
    /// List the models of every provider, as `/v1/models` does, and exit
    Models,
    /// Write a commented starter config from the keys in the environment
    Init {
        /// Where to write it
        #[arg(short, long, value_name = "FILE", default_value = crate::DEFAULT_FILE)]
        output: PathBuf,
        /// Replace the file if it exists
        #[arg(long)]
        force: bool,
        /// Enable a vendor too, such as `groq`, its key read from its
        /// variable (`env:GROQ_API_KEY`); `VENDOR=env:NAME` reads another,
        /// and `VENDOR=KEY` writes the key itself into the file
        #[arg(short, long, value_name = "VENDOR[=KEY]")]
        provider: Vec<String>,
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
        /// Check the proxy's configuration, print it and exit
        #[arg(long)]
        check: bool,
    },
}

const EXAMPLES: &str = "Examples:
  respan-gateway                    Serve ./gateway.yaml, or the providers in the environment
  respan-gateway init               Write a starter ./gateway.yaml
  respan-gateway init -p groq       Write a starter that also enables groq, key env:GROQ_API_KEY
  respan-gateway check -c my.yaml   Check a config and print what it serves
  respan-gateway check --online     ... and call each provider with its key
  respan-gateway models             List the models the providers offer
  respan-gateway proxy --anthropic  Sit in front of Claude Code

Environment (also read from ./.env):
  OPENAI_API_KEY, ANTHROPIC_API_KEY, GEMINI_API_KEY, GROQ_API_KEY, ...
                     Provider keys; with no config file each is a provider
  RESPAN_CONFIG      The config file, as --config
  RESPAN_LISTEN      Where to listen, as --listen
  PORT               The port to listen on (on 127.0.0.1), if nothing else says
  RESPAN_LOG         text, json or off, as --log
  RESPAN_<VENDOR>_BASE_URL
                     Moves a vendor's base URL when there is no config file";

/// Runs the `respan-gateway` command line on `args` (the program name
/// first) and returns its exit status.
pub async fn run<I, T>(args: I) -> u8
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    run_with(args, None).await
}

/// Runs the command line as the binary does: reads `./.env` if there is
/// one, then runs on a runtime of its own.
pub fn execute<I, T>(args: I) -> u8
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let dotenv = match load_dotenv() {
        Ok(loaded) => loaded,
        Err(e) => return fail(&e),
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build();
    match runtime {
        Ok(runtime) => runtime.block_on(run_with(args, dotenv)),
        Err(e) => fail(&format!("start the runtime: {e}")),
    }
}

/// Reads `./.env` into the environment, never over a variable already set;
/// the file if there was one.
fn load_dotenv() -> Result<Option<PathBuf>, String> {
    let path = Path::new(".env");
    if !path.is_file() {
        return Ok(None);
    }
    dotenvy::from_path(path).map_err(|e| format!(".env: {e}"))?;
    Ok(Some(path.to_owned()))
}

async fn run_with<I, T>(args: I, dotenv: Option<PathBuf>) -> u8
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let cli = match Cli::try_parse_from(args) {
        Ok(cli) => cli,
        Err(e) => {
            let _ = e.print();
            return u8::try_from(e.exit_code()).unwrap_or(FAILURE);
        }
    };
    let command = cli.command.unwrap_or(Command::Serve);
    let models = matches!(command, Command::Models);
    let online = matches!(command, Command::Check { online: true });
    let (config, check) = match command {
        Command::Init {
            output,
            force,
            provider,
        } => return init(&output, force, &provider),
        Command::Proxy {
            anthropic,
            url,
            check,
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
            (Ok(Config::proxy(vendor, url.as_deref())), check)
        }
        Command::Serve => (Config::discover(cli.config.as_deref()), false),
        Command::Check { .. } | Command::Models => (Config::discover(cli.config.as_deref()), true),
    };
    let mut config = match config {
        Ok(config) => config,
        Err(e) => return fail(&e.to_string()),
    };
    let listen = cli.listen.or_else(|| std::env::var("PORT").ok());
    if let Some(listen) = listen.filter(|l| !l.is_empty()) {
        match crate::config::parse_listen(&listen) {
            Ok(addr) => config.listen = Some(addr),
            Err(e) => return fail(&format!("listen {listen}: {e}")),
        }
    }
    if let Some(log) = cli.log {
        config.log = log;
    }
    let log = config.log;
    let source = config.source().to_string();
    let app = match App::new(config) {
        Ok(app) => app,
        Err(e) => return fail(&format!("{source}: {e}")),
    };
    if models {
        return list_models(&app).await;
    }
    if check {
        println!("{}", app.report());
        println!("{source}: ok");
        if online {
            let (report, all_reachable) = app.probe().await;
            println!("\n{report}");
            return if all_reachable { SUCCESS } else { FAILURE };
        }
        return SUCCESS;
    }
    if let Some(path) = &dotenv {
        log.say(Level::Info, &format!("loaded {}", path.display()));
    }

    let addr = app.listen();
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => return fail(&bind_error(addr, &e)),
    };
    let bound = listener.local_addr().unwrap_or(addr);
    match log {
        LogFormat::Json => {
            let fields = json!({ "addr": bound.to_string(), "version": VERSION, "config": source });
            let fields = fields.as_object().cloned().unwrap_or_else(Map::new);
            eprintln!("{}", log.line(Level::Info, "listening", fields));
        }
        _ => println!("{}", app.banner(bound)),
    }
    if let Some(warning) = app.warning(bound) {
        log.say(Level::Warn, &warning);
    }
    match app.serve(listener, crate::shutdown_signal()).await {
        Ok(()) => SUCCESS,
        Err(e) => fail(&e.to_string()),
    }
}

/// Why `addr` cannot be listened on, and what to do.
fn bind_error(addr: std::net::SocketAddr, e: &std::io::Error) -> String {
    use std::io::ErrorKind::{AddrInUse, AddrNotAvailable, PermissionDenied};
    let hint = match e.kind() {
        AddrInUse => format!(
            "something else is listening there; stop it, or use another port \
             (`--listen {}` or `--listen 127.0.0.1:0` for any free one)",
            addr.port().saturating_add(1)
        ),
        PermissionDenied => {
            "ports below 1024 need privileges; use a higher one, such as 8080".to_owned()
        }
        AddrNotAvailable => "that address is not one of this machine's".to_owned(),
        _ => return format!("listen on {addr}: {e}"),
    };
    format!("listen on {addr}: {e}\n  {hint}")
}

async fn list_models(app: &App) -> u8 {
    let (models, failed) = app.models().await;
    let text = |m: &serde_json::Value, key: &str| m[key].as_str().unwrap_or_default().to_owned();
    let mut rows: Vec<[String; 3]> = models
        .iter()
        .map(|m| [text(m, "owned_by"), text(m, "kind"), text(m, "id")])
        .collect();
    rows.sort();
    let width = |i: usize| rows.iter().map(|r| r[i].len()).max().unwrap_or(0).max(8);
    let (provider, kind) = (width(0), width(1));
    println!("{:<provider$}  {:<kind$}  MODEL", "PROVIDER", "KIND");
    for [owner, kind_of, id] in &rows {
        println!("{owner:<provider$}  {kind_of:<kind$}  {id}");
    }
    for why in &failed {
        eprintln!("warning: could not list {why}");
    }
    match rows.is_empty() && !failed.is_empty() {
        true => FAILURE,
        false => SUCCESS,
    }
}

fn init(output: &Path, force: bool, providers: &[String]) -> u8 {
    let chosen: Result<Vec<Chosen>, String> = providers.iter().map(|p| Chosen::parse(p)).collect();
    let chosen = match chosen {
        Ok(chosen) => chosen,
        Err(e) => return fail(&e),
    };
    if output.exists() && !force {
        let path = output.display();
        return fail(&format!("{path} exists; add --force to replace it"));
    }
    let private = chosen.iter().any(Chosen::is_literal);
    if let Err(e) = write_config(output, &crate::init::starter(&chosen), private) {
        return fail(&format!("write {}: {e}", output.display()));
    }
    let path = output.display();
    for c in &chosen {
        let id = c.vendor.id;
        match (c.is_literal(), c.variable()) {
            (true, _) => eprintln!(
                "warning: {path} holds {id}'s key in plain text; keep it private, or use \
                 `-p {id}` to read it from {} instead",
                c.vendor.key_env.first().copied().unwrap_or("a variable")
            ),
            (_, Some(var)) if std::env::var(var).is_ok_and(|v| !v.is_empty()) => {}
            (_, Some(var)) => {
                println!("{id}: set {var} (export {var}=..., or put it in .env) before you serve.")
            }
            _ => {}
        }
    }
    let mut found = crate::init::detected_vendors();
    if !chosen.is_empty() {
        for c in &chosen {
            if !found.contains(&c.vendor.id) {
                found.push(c.vendor.id);
            }
        }
        println!(
            "wrote {path} with {} enabled.\n\
             Next: `respan-gateway check --online`, then `respan-gateway`.",
            found.join(", ")
        );
    } else if found.is_empty() {
        println!(
            "wrote {path} with openai enabled; no provider key is in the environment.\n\
             Next: set OPENAI_API_KEY (export it, or put it in .env), then `respan-gateway check`."
        );
    } else {
        println!(
            "wrote {path} with {} enabled, from your environment.\n\
             Next: `respan-gateway check`, then `respan-gateway`.",
            found.join(", ")
        );
    }
    SUCCESS
}

/// Writes `text` to `path`, readable by its owner alone if `private`: it
/// holds a key.
fn write_config(path: &Path, text: &str, private: bool) -> std::io::Result<()> {
    #[cfg(unix)]
    if private {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)?;
        return file.write_all(text.as_bytes());
    }
    let _ = private;
    std::fs::write(path, text)
}

const SUCCESS: u8 = 0;
const FAILURE: u8 = 1;

fn fail(message: &str) -> u8 {
    eprintln!("error: {message}");
    FAILURE
}
