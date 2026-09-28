//! One MCP server's child process: its stdin, and where its messages go.

use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use respan_gateway_core::bytes::Bytes;
use respan_gateway_core::futures::{StreamExt, stream};
use respan_gateway_core::http::header::CONTENT_TYPE;
use respan_gateway_core::http::{self, HeaderValue, StatusCode};
use respan_gateway_core::http_body::Frame;
use respan_gateway_core::http_body_util::{BodyExt, StreamBody};
use respan_gateway_core::serde_json::{self, Value};
use respan_gateway_core::{Body, Fault};
use respan_gateway_formats::sse;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{ChildStdin, Command};
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::sync::oneshot;
use tokio::time::{Instant, sleep_until};

use crate::stdio::reply;
use crate::{lock, requests};

#[derive(Debug)]
pub(crate) struct Session {
    stdin: tokio::sync::Mutex<ChildStdin>,
    routes: Arc<Mutex<Routes>>,
    /// Dropped with the session, which ends its child.
    _alive: oneshot::Sender<()>,
}

/// Where the child's messages go: a response to the `POST` that asked; a
/// request or notification to the oldest open `POST`, else the `GET`
/// stream, else nowhere.
#[derive(Debug, Default)]
struct Routes {
    posts: Vec<(Vec<Value>, UnboundedSender<Value>)>,
    listener: Option<UnboundedSender<Value>>,
    /// The last request, or the last `POST` answered.
    touched: Option<Instant>,
}

impl Session {
    /// Runs `command` until it exits, the session is dropped, or it is idle
    /// for `idle`; then kills and reaps it, and calls `ended`.
    pub(crate) fn start(
        command: &mut Command,
        idle: Duration,
        ended: impl FnOnce() + Send + 'static,
    ) -> Result<Self, Fault> {
        let command = command.stdin(Stdio::piped()).stdout(Stdio::piped());
        let mut child = command
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| Fault::upstream("the MCP server could not be started").with_source(e))?;
        let (Some(stdin), Some(stdout)) = (child.stdin.take(), child.stdout.take()) else {
            return Err(Fault::upstream("the MCP server has no stdio"));
        };
        let (routes, (alive, mut gone)) =
            (Arc::new(Mutex::new(Routes::default())), oneshot::channel());
        let reader = routes.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            let mut due = Instant::now() + idle;
            loop {
                tokio::select! {
                    line = lines.next_line() => match line {
                        // Not JSON, such as a stray log line: not a message.
                        Ok(Some(line)) => if let Ok(v) = serde_json::from_str(&line) {
                            route(&mut lock(&reader), v);
                        },
                        _ => break,
                    },
                    _ = &mut gone => break,
                    () = sleep_until(due) => {
                        let mut routes = lock(&reader);
                        routes.posts.retain(|(_, tx)| !tx.is_closed());
                        let (since, now) = (routes.touched.unwrap_or(due), Instant::now());
                        match routes.posts.is_empty() && since + idle <= now {
                            true => break,
                            false => due = now.max(since) + idle,
                        }
                    }
                }
            }
            // Its open streams end, unanswered, as the session is dropped.
            let _ = child.start_kill();
            let _ = child.wait().await;
            ended();
        });
        let stdin = tokio::sync::Mutex::new(stdin);
        let _alive = alive;
        Ok(Self {
            stdin,
            routes,
            _alive,
        })
    }

    /// Notes a request, which keeps it from idling.
    pub(crate) fn touch(&self) {
        lock(&self.routes).touched = Some(Instant::now());
    }

    /// The server's own stream of messages, or `None` if it is open.
    pub(crate) fn listen(&self) -> Option<http::Response<Body>> {
        let mut routes = lock(&self.routes);
        if routes.listener.as_ref().is_some_and(|l| !l.is_closed()) {
            return None;
        }
        let (tx, rx) = mpsc::unbounded_channel();
        routes.listener = Some(tx);
        Some(events(rx))
    }

    /// Writes `messages` to the child: an event stream of the answers, or
    /// 202 if nothing asks.
    pub(crate) async fn post(&self, messages: Vec<Value>) -> Result<http::Response<Body>, Fault> {
        let ids: Vec<Value> = requests(&messages).cloned().collect();
        let (asks, (tx, rx)) = (!ids.is_empty(), mpsc::unbounded_channel());
        // Registered before writing, so no answer is missed.
        if asks {
            lock(&self.routes).posts.push((ids, tx));
        }
        let mut stdin = self.stdin.lock().await;
        for m in &messages {
            let written = stdin.write_all(format!("{m}\n").as_bytes()).await;
            written.map_err(|e| Fault::upstream("the MCP server stopped").with_source(e))?;
        }
        let _ = stdin.flush().await;
        if !asks {
            return Ok(reply(StatusCode::ACCEPTED, None));
        }
        Ok(events(rx))
    }
}

fn route(routes: &mut Routes, v: Value) {
    routes.posts.retain(|(_, tx)| !tx.is_closed());
    let Some(id) = v.get("id").filter(|_| v.get("method").is_none()) else {
        let first = routes.posts.first().map(|(_, tx)| tx);
        if let Some(tx) = first.or(routes.listener.as_ref()) {
            let _ = tx.send(v);
        }
        return;
    };
    if let Some(i) = routes.posts.iter().position(|(ids, _)| ids.contains(id)) {
        let (ids, tx) = &mut routes.posts[i];
        ids.retain(|x| x != id);
        let _ = tx.send(v);
        if ids.is_empty() {
            routes.posts.remove(i);
            routes.touched = Some(Instant::now());
        }
    }
}

/// An event stream of what `rx` receives, until it closes.
fn events(rx: mpsc::UnboundedReceiver<Value>) -> http::Response<Body> {
    let events = stream::unfold(rx, |mut rx| async move {
        let event = Bytes::from(sse::event(None, rx.recv().await?));
        Some((Ok::<_, Fault>(Frame::data(event)), rx))
    });
    let mut resp = http::Response::new(StreamBody::new(events.boxed()).boxed_unsync());
    let sse = HeaderValue::from_static("text/event-stream");
    resp.headers_mut().insert(CONTENT_TYPE, sse);
    resp
}
