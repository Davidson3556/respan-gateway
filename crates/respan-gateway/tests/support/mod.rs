//! The harness of the end-to-end tests: start the shipped binary on a free
//! port with a temp config, and read what it answers. The providers it
//! reaches are `mockllm`'s.

#![allow(dead_code, clippy::unwrap_used)]

use std::process::Stdio;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::StreamExt;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpListener;
use tokio::process::{Child, Command};

/// How long anything in these tests may take before it counts as a hang.
pub const BOUND: Duration = Duration::from_secs(10);

/// The shipped binary, running on a free port until dropped.
pub struct Gateway {
    /// `http://127.0.0.1:<port>`
    pub base: String,
    pub child: Child,
}

impl Gateway {
    /// Starts the binary with `config` (YAML without `listen`), listening on
    /// a port the OS picks, and waits, bounded, until it says where.
    pub async fn start(config: &str) -> Self {
        let path = write_config(&format!("listen: 127.0.0.1:0\n{config}"));
        Self::run(&["--config", path.to_str().unwrap()]).await
    }

    /// Starts the binary with `args`, which give no config file and say
    /// where to listen (`--listen 127.0.0.1:0`).
    pub async fn run(args: &[&str]) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_respan-gateway"))
            .args(args)
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
        tokio::spawn(async move { while let Ok(Some(_)) = lines.next_line().await {} });
        Self {
            base: format!("http://{addr}"),
            child,
        }
    }

    pub fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }
}

/// Writes `yaml` to a fresh file under Cargo's temp dir for tests.
pub fn write_config(yaml: &str) -> std::path::PathBuf {
    static N: AtomicUsize = AtomicUsize::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"));
    let path = dir.join(format!("e2e-{}-{n}.yaml", std::process::id()));
    std::fs::write(&path, yaml).unwrap_or_else(|e| panic!("write config: {e}"));
    path
}

/// A client with the same bound on every request.
pub fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(BOUND)
        .build()
        .unwrap_or_else(|e| panic!("client: {e}"))
}

/// An address nothing listens on: a port the OS gave, then closed.
pub async fn closed_port() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
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
