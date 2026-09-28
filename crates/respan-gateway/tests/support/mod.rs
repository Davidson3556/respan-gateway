//! Shared helpers for the end-to-end tests: start the shipped binary on a
//! free port with a temp config, and a small axum provider that streams.
//!
//! wiremock serves whole bodies; [`Streamer`] serves chunks over time, a
//! body cut mid-way and a slow first byte, which wiremock cannot.

#![allow(dead_code, clippy::unwrap_used)]

use std::process::Stdio;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::Request;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
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
        let mut child = Command::new(env!("CARGO_BIN_EXE_respan-gateway"))
            .arg("--config")
            .arg(&path)
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

/// One step of a scripted streaming body.
#[derive(Clone, Debug)]
pub enum Step {
    /// Send these bytes.
    Send(&'static str),
    /// Wait this long.
    Wait(Duration),
    /// Break the body off: the connection closes without its end. Put a
    /// `Wait` before it: hyper drops bytes not yet flushed when the body
    /// fails in the same poll, head included.
    Cut,
}

/// A request a [`Streamer`] received.
#[derive(Clone, Debug)]
pub struct Seen {
    pub path: String,
    pub headers: HeaderMap,
    pub body: Bytes,
}

/// A provider that answers every request with one scripted event stream,
/// and records what it was sent.
#[derive(Clone)]
pub struct Streamer {
    pub base: String,
    seen: Arc<Mutex<Vec<Seen>>>,
}

impl Streamer {
    /// Serves `steps` as a 200 `text/event-stream` to every request.
    pub async fn start(steps: Vec<Step>) -> Self {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let record = seen.clone();
        let app = Router::new().fallback(move |req: Request| {
            let steps = steps.clone();
            let record = record.clone();
            async move {
                let (parts, body) = req.into_parts();
                let body = axum::body::to_bytes(body, usize::MAX).await.unwrap();
                record.lock().unwrap().push(Seen {
                    path: parts.uri.path().to_owned(),
                    headers: parts.headers,
                    body,
                });
                stream_response(steps).await
            }
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await });
        Self {
            base: format!("http://{addr}"),
            seen,
        }
    }

    pub fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }
}

/// Waits out leading `Wait`s before the head, so a slow first byte is
/// slow to the head too, then streams the rest.
async fn stream_response(steps: Vec<Step>) -> Response {
    let mut steps = steps.into_iter().peekable();
    while let Some(Step::Wait(d)) = steps.peek() {
        tokio::time::sleep(*d).await;
        steps.next();
    }
    let body = futures::stream::iter(steps)
        .then(|step| async move {
            match step {
                Step::Send(s) => Some(Ok::<_, std::io::Error>(Bytes::from_static(s.as_bytes()))),
                Step::Wait(d) => {
                    tokio::time::sleep(d).await;
                    None
                }
                Step::Cut => Some(Err(std::io::Error::other("cut"))),
            }
        })
        .filter_map(|x| async move { x });
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/event-stream")
        .body(Body::from_stream(body))
        .unwrap()
}

/// All of `steps`' bytes, as a client should receive them.
pub fn sent(steps: &[Step]) -> String {
    steps
        .iter()
        .filter_map(|s| match s {
            Step::Send(s) => Some(*s),
            _ => None,
        })
        .collect()
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
