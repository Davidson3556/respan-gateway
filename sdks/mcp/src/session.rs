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
use tokio::process::Command;
use tokio::sync::mpsc::{self, Sender};
use tokio::sync::oneshot;
use tokio::time::{Instant, sleep_until, timeout_at};

use crate::stdio::reply;
use crate::{TIMED_OUT, lock, requests};

#[derive(Debug)]
pub(crate) struct Session {
    /// Whole lines for the child's stdin, written by one task, so a `POST`
    /// dropped mid-write cannot leave half a line.
    stdin: Sender<Bytes>,
    routes: Arc<Mutex<Routes>>,
    /// How long a `POST` waits for its answers.
    timeout: Duration,
    /// Dropped with the session, which ends its child.
    _alive: oneshot::Sender<()>,
}

/// Messages a stream may fall behind by; past it, the stream ends.
const BACKLOG: usize = 64;

/// Where the child's messages go: a response to the `POST` that asked, or
/// nowhere if that `POST` closed; a request or notification to the oldest
/// open `POST`, else the `GET` stream. A response to `null`, such as a
/// parse error, ends the oldest `POST`, whose requests the server won't
/// answer either.
#[derive(Debug, Default)]
struct Routes {
    posts: Vec<(Vec<Value>, Sender<Value>)>,
    listener: Option<Sender<Value>>,
    /// The last request, or the last `POST` answered.
    touched: Option<Instant>,
}

impl Session {
    /// Runs `command` until it exits, the session is dropped, or it is idle
    /// for `idle`; then kills and reaps it, and calls `ended`. A `POST`
    /// waits `timeout` for its answers.
    pub(crate) fn start(
        command: &mut Command,
        idle: Duration,
        timeout: Duration,
        ended: impl FnOnce() + Send + 'static,
    ) -> Result<Self, Fault> {
        let command = command.stdin(Stdio::piped()).stdout(Stdio::piped());
        let mut child = command
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| Fault::upstream("the MCP server could not be started").with_source(e))?;
        let (Some(mut stdin), Some(stdout)) = (child.stdin.take(), child.stdout.take()) else {
            return Err(Fault::upstream("the MCP server has no stdio"));
        };
        let (lines, mut written) = mpsc::channel::<Bytes>(BACKLOG);
        tokio::spawn(async move {
            while let Some(line) = written.recv().await {
                if stdin.write_all(&line).await.is_err() || stdin.flush().await.is_err() {
                    break;
                }
            }
        });
        let routes = Arc::new(Mutex::new(Routes::default()));
        let (alive, mut gone) = oneshot::channel();
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
                        let since = routes.touched.unwrap_or(due);
                        let now = Instant::now();
                        if routes.posts.is_empty() && since + idle <= now {
                            break;
                        }
                        due = now.max(since) + idle;
                    }
                }
            }
            // Its open streams end, unanswered, as the session is dropped.
            let _ = child.start_kill();
            let _ = child.wait().await;
            ended();
        });
        Ok(Self {
            stdin: lines,
            routes,
            timeout,
            _alive: alive,
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
        let (tx, rx) = mpsc::channel(BACKLOG);
        routes.listener = Some(tx);
        Some(events(rx, None))
    }

    /// Writes `messages` to the child: an event stream of the answers, or
    /// 202 if nothing asks.
    pub(crate) async fn post(&self, messages: Vec<Value>) -> Result<http::Response<Body>, Fault> {
        let ids: Vec<Value> = requests(&messages).cloned().collect();
        let asks = !ids.is_empty();
        let (tx, rx) = mpsc::channel(BACKLOG);
        // Registered before writing, so no answer is missed.
        if asks {
            lock(&self.routes).posts.push((ids, tx));
        }
        let lines: String = messages.iter().map(|m| format!("{m}\n")).collect();
        let sent = self.stdin.send(lines.into()).await;
        sent.map_err(|_| Fault::upstream("the MCP server stopped"))?;
        if !asks {
            return Ok(reply(StatusCode::ACCEPTED, None));
        }
        Ok(events(rx, Some(Instant::now() + self.timeout)))
    }
}

fn route(routes: &mut Routes, v: Value) {
    routes.posts.retain(|(_, tx)| !tx.is_closed());
    let answer = v.get("method").is_none();
    let id = v.get("id").filter(|id| answer && !id.is_null()).cloned();
    let i = match &id {
        Some(id) => match routes.posts.iter().position(|(ids, _)| ids.contains(id)) {
            Some(i) => i,
            // Its `POST` closed, as when the client gave up: no one waits.
            None => return,
        },
        // A request, a notification, or an answer to `null`: the oldest POST.
        None if !routes.posts.is_empty() => 0,
        None if answer => return,
        // The server's own stream.
        None => {
            if let Some(tx) = &routes.listener
                && tx.try_send(v).is_err()
            {
                routes.listener = None;
            }
            return;
        }
    };
    let (ids, tx) = &mut routes.posts[i];
    match &id {
        Some(id) => ids.retain(|x| x != id),
        None if answer => ids.clear(),
        None => {}
    }
    let fell_behind = tx.try_send(v).is_err();
    if ids.is_empty() || fell_behind {
        routes.posts.remove(i);
        routes.touched = Some(Instant::now());
    }
}

/// An event stream of what `rx` receives, until it closes, or failing at
/// `until` if any.
fn events(rx: mpsc::Receiver<Value>, until: Option<Instant>) -> http::Response<Body> {
    let events = stream::unfold(Some(rx), move |rx| async move {
        let mut rx = rx?;
        let next = match until {
            Some(until) => timeout_at(until, rx.recv()).await,
            None => Ok(rx.recv().await),
        };
        let Ok(next) = next else {
            let late = "the MCP server did not answer in time";
            let fault = Fault::new(StatusCode::GATEWAY_TIMEOUT, TIMED_OUT, late);
            return Some((Err(fault), None));
        };
        let event = Bytes::from(sse::event(None, next?));
        Some((Ok(Frame::data(event)), Some(rx)))
    });
    let mut resp = http::Response::new(StreamBody::new(events.boxed()).boxed_unsync());
    let sse = HeaderValue::from_static("text/event-stream");
    resp.headers_mut().insert(CONTENT_TYPE, sse);
    resp
}
