//! The harness of the end-to-end tests: start the shipped binary on a free
//! port with a temp config, and read what it answers. The providers it
//! reaches are `mockllm`'s.

#![allow(dead_code, clippy::unwrap_used)]

use parking_lot::Mutex;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::StreamExt;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};

/// How long anything in these tests may take before it counts as a hang.
pub const BOUND: Duration = Duration::from_secs(10);

/// The shipped binary, running on a free port until dropped.
pub struct Gateway {
    /// `http://127.0.0.1:<port>`
    pub base: String,
    pub child: Child,
    /// Its stdout lines so far.
    pub stdout: Arc<Mutex<Vec<String>>>,
}

impl Gateway {
    /// Starts the binary with `config` (TOML without `listen`), listening on
    /// a port the OS picks, and waits, bounded, until it says where.
    pub async fn start(config: &str) -> Self {
        let path = write_config(&format!("listen = \"127.0.0.1:0\"\n{config}"));
        Self::run(&["--config", path.to_str().unwrap()]).await
    }

    /// Starts the binary with `args`, which give no config file and say
    /// where to listen (`--listen 127.0.0.1:0`).
    pub async fn run(args: &[&str]) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_respan-gateway"));
        command.args(args);
        Self::spawn(command).await
    }

    /// Starts the binary with `args` in `dir`, with `env` as its whole
    /// environment, so no provider key of the test's own leaks in.
    pub async fn run_with(args: &[&str], dir: &std::path::Path, env: &[(&str, &str)]) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_respan-gateway"));
        command.args(args).current_dir(dir).env_clear();
        command.envs(env.iter().copied());
        Self::spawn(command).await
    }

    async fn spawn(mut command: Command) -> Self {
        let mut child = command
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .unwrap_or_else(|e| panic!("spawn the binary: {e}"));
        let stdout = child.stdout.take().unwrap();
        let mut lines = BufReader::new(stdout).lines();
        let line = tokio::time::timeout(BOUND, lines.next_line())
            .await
            .unwrap_or_else(|_| panic!("the binary did not start within {BOUND:?}"))
            .unwrap_or_else(|e| panic!("read its stdout: {e}"))
            .unwrap_or_else(|| panic!("the binary exited before listening"));
        let addr = line
            .rsplit_once("listening on ")
            .map(|(_, a)| a.trim().to_owned())
            .unwrap_or_else(|| panic!("unexpected first line: {line}"));
        // Keep draining stdout so the child never blocks on a full pipe.
        let stdout = Arc::new(Mutex::new(vec![line]));
        let printed = stdout.clone();
        tokio::spawn(async move {
            while let Ok(Some(line)) = lines.next_line().await {
                printed.lock().push(line);
            }
        });
        Self {
            base: format!("http://{addr}"),
            child,
            stdout,
        }
    }

    /// What it printed on stdout, once some line contains `text`, bounded.
    pub async fn stdout_until(&self, text: &str) -> String {
        let printed = || self.stdout.lock().join("\n");
        let deadline = tokio::time::Instant::now() + BOUND;
        while !printed().contains(text) {
            let late = tokio::time::Instant::now() > deadline;
            assert!(!late, "no {text:?} in {}", printed());
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        printed()
    }

    pub fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }
}

/// Writes `text` to a fresh file under Cargo's temp dir for tests.
/// `config` with `plugins.routing.listings = false`, unless it says.
pub fn quiet(config: &str) -> String {
    let Ok(mut table) = config.parse::<toml::Table>() else {
        return config.to_owned(); // A test of a bad config.
    };
    let plugins = table
        .entry("plugins")
        .or_insert_with(|| toml::Table::new().into());
    let Some(routing) = plugins
        .as_table_mut()
        .map(|p| {
            p.entry("routing")
                .or_insert_with(|| toml::Table::new().into())
        })
        .and_then(|r| r.as_table_mut())
    else {
        return config.to_owned();
    };
    routing.entry("listings").or_insert(false.into());
    table.to_string()
}

/// Writes `text` as a config file. It reads no provider's model list
/// unless it sets `plugins.routing.listings`, so a provider sees only the
/// calls a test makes.
pub fn write_config(text: &str) -> std::path::PathBuf {
    let text = &quiet(text);
    static N: AtomicUsize = AtomicUsize::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"));
    let path = dir.join(format!("e2e-{}-{n}.toml", std::process::id()));
    std::fs::write(&path, text).unwrap_or_else(|e| panic!("write config: {e}"));
    path
}

/// A client with the same bound on every request.
pub fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(BOUND)
        .build()
        .unwrap_or_else(|e| panic!("client: {e}"))
}

/// An address nothing listens on: a port bound but never listened on, and
/// kept bound for the rest of the process so no other test can take it.
/// A connection to it is refused.
pub async fn closed_port() -> String {
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = socket.local_addr().unwrap();
    std::mem::forget(socket);
    format!("http://{addr}")
}

/// Reads `resp`'s body chunk by chunk, bounded, keeping what arrived
/// before any error: `(bytes, Err(..) if the body broke off)`.
pub async fn read_all(resp: reqwest::Response) -> (String, Result<(), String>) {
    let mut out = Vec::new();
    let mut stream = resp.bytes_stream();
    let read = async {
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(b) => out.extend_from_slice(&b),
                Err(e) => return Err(format!("{e:?}")),
            }
        }
        Ok(())
    };
    let ended = tokio::time::timeout(BOUND, read)
        .await
        .unwrap_or_else(|_| panic!("the body did not end within {BOUND:?}"));
    (String::from_utf8(out).unwrap(), ended)
}

pub fn json(text: &str) -> serde_json::Value {
    serde_json::from_str(text).unwrap_or_else(|e| panic!("not JSON ({e}): {text}"))
}

/// Gemini's formats at `base`, a fake of its API, as a provider's
/// `formats` table: its own API at `/v1beta`, its OpenAI-compatible one
/// at `/v1beta/openai`, and Live at the origin, as Google has them.
pub fn gemini_formats(base: &str) -> String {
    let (own, openai) = (format!("{base}/v1beta"), format!("{base}/v1beta/openai"));
    format!(
        "{{ gemini-generate = \"{own}\", openai-chat = \"{openai}\", gemini-live = \"{base}\", \
         openai-images = \"{openai}\", gemini-veo = \"{own}\", openai-embeddings = \"{openai}\" }}"
    )
}
