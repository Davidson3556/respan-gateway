//! One MCP server's child process: its stdin, and where its messages go.

use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::messages::Message;
use respan_gateway_core::futures::stream;
use respan_gateway_core::{BadRequest, Fault, Upstream};
use respan_gateway_http::Body;
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::formats::sse;
use respan_gateway_http::http::header::CONTENT_TYPE;
use respan_gateway_http::http::{self, HeaderValue, StatusCode};
use serde_json::{self, Value, json};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::mpsc::{self, Sender};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep_until, timeout, timeout_at};

use crate::lock;
use crate::stdio::{empty, late};

/// How long a child has to exit once its stdin is closed, and again once
/// it is sent SIGTERM: 2 seconds, as the official MCP SDKs wait, long
/// enough for a server to flush and close what it holds.
const GRACE: Duration = Duration::from_secs(2);

/// The limits of a session, as its transport was configured.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Limits {
    /// How long a session with nothing open and no request lives.
    pub(crate) idle: Duration,
    /// How long a `POST` waits for its answers.
    pub(crate) timeout: Duration,
    /// Messages a stream, or the child's stdin, may fall behind by.
    pub(crate) backlog: usize,
    /// The most bytes of one line the child writes.
    pub(crate) max_line: usize,
}

#[derive(Debug)]
pub(crate) struct Session {
    /// Whole lines for the child's stdin, written by one task, so a `POST`
    /// dropped mid-write cannot leave half a line.
    stdin: Sender<Bytes>,
    routes: Arc<Mutex<Routes>>,
    limits: Limits,
    /// Dropped with the session, which ends its child.
    _alive: oneshot::Sender<()>,
}

/// Where the child's messages go (MCP, "Streamable HTTP": a server sends
/// each message on one stream only):
///
/// - a response to the `POST` that asked, or nowhere if it closed; a
///   response to `null`, such as a parse error, ends the oldest `POST`,
///   whose requests the server won't answer either;
/// - a `notifications/progress` to the `POST` whose request carried its
///   `progressToken`, a `notifications/cancelled` to the `POST` that carried
///   the server's request it names;
/// - anything else (a request or notification of the server's own, or one
///   whose `POST` closed) to the `GET` stream if one is open, else to the
///   oldest open `POST`, else nowhere.
///
/// A request the client cancels (`notifications/cancelled`) is no longer
/// waited for: its answer, if the server sends one, goes nowhere (MCP,
/// "Cancellation": the sender "SHOULD ignore any response"), and its
/// `POST` ends once nothing else of it is waited for.
#[derive(Debug)]
struct Routes {
    posts: Vec<Post>,
    listener: Option<Sender<Message>>,
    /// The start, the last request, or the last `POST` answered.
    touched: Instant,
    /// The number of the next `POST`.
    next: u64,
}

/// An open `POST`, waiting for answers.
#[derive(Debug)]
struct Post {
    number: u64,
    /// The ids of its requests the server has not answered.
    ids: Vec<Value>,
    /// The `progressToken`s its requests carry.
    tokens: Vec<Value>,
    /// The ids of the server's requests sent on it.
    asked: Vec<Value>,
    tx: Sender<Message>,
}

impl Routes {
    /// Whether the `GET` stream is open.
    fn listening(&self) -> bool {
        self.listener.as_ref().is_some_and(|l| !l.is_closed())
    }
}

impl Session {
    /// Runs `command`, in a process group of its own, until it exits, the
    /// session is dropped, or it is idle for `limits.idle`; then stops it
    /// ([`stop`]) and calls `ended`.
    pub(crate) fn start(
        command: &mut Command,
        limits: Limits,
        ended: impl FnOnce() + Send + 'static,
    ) -> Result<Self, Fault> {
        if limits.backlog == 0 {
            return Err(Fault::config(
                "an MCP stdio backlog must be greater than zero",
            ));
        }
        let command = command.stdin(Stdio::piped()).stdout(Stdio::piped());
        #[cfg(unix)]
        command.process_group(0);
        let mut child = command
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| Upstream::fault("the MCP server could not be started").with_source(e))?;
        let (Some(mut stdin), Some(stdout)) = (child.stdin.take(), child.stdout.take()) else {
            return Err(Upstream::fault("the MCP server has no stdio"));
        };
        let group = child.id().and_then(|id| i32::try_from(id).ok());
        let (lines, mut written) = mpsc::channel::<Bytes>(limits.backlog);
        let writer = tokio::spawn(async move {
            while let Some(line) = written.recv().await {
                if stdin.write_all(&line).await.is_err() || stdin.flush().await.is_err() {
                    break;
                }
            }
        });
        // Started counts as touched, so a server that never answers
        // `initialize` still idles out and frees its slot.
        let routes = Arc::new(Mutex::new(Routes {
            posts: Vec::new(),
            listener: None,
            touched: Instant::now(),
            next: 0,
        }));
        let (alive, mut gone) = oneshot::channel();
        let reader = routes.clone();
        tokio::spawn(async move {
            let mut lines = Lines::new(BufReader::new(stdout), limits.max_line);
            let mut due = Instant::now().checked_add(limits.idle);
            let mut exited = false;
            loop {
                tokio::select! {
                    line = lines.next() => match line {
                        // Not JSON-RPC, such as a stray log line or one
                        // over the limit: not a message.
                        Ok(Some(line)) => if let Ok(message) = serde_json::from_slice(&line) {
                            route(&mut lock(&reader), message);
                        },
                        _ => break,
                    },
                    // What it wrote is still read: its group, which may
                    // hold its stdout open, is killed, so the output ends.
                    _ = child.wait(), if !exited => {
                        exited = true;
                        signal(&mut child, group, true);
                    },
                    _ = &mut gone => break,
                    () = until(due) => {
                        let mut routes = lock(&reader);
                        routes.posts.retain(|p| !p.tx.is_closed());
                        let now = Instant::now();
                        let open = !routes.posts.is_empty() || routes.listening();
                        let idle_at = routes.touched.checked_add(limits.idle);
                        if !open && idle_at.is_some_and(|at| at <= now) {
                            break;
                        }
                        due = now.max(routes.touched).checked_add(limits.idle);
                    }
                }
            }
            // Its open streams end, unanswered, as the session is dropped.
            stop(child, group, writer).await;
            ended();
        });
        Ok(Self {
            stdin: lines,
            routes,
            limits,
            _alive: alive,
        })
    }

    /// Notes a request, which keeps it from idling.
    pub(crate) fn touch(&self) {
        lock(&self.routes).touched = Instant::now();
    }

    /// The server's own stream of messages, or `None` if it is open.
    pub(crate) fn listen(&self) -> Option<http::Response<Body>> {
        let mut routes = lock(&self.routes);
        if routes.listening() {
            return None;
        }
        let (tx, rx) = mpsc::channel(self.limits.backlog);
        routes.listener = Some(tx);
        Some(events(rx, None, || {}))
    }

    /// Writes `messages` to the child: an event stream of the answers, or
    /// 202 if nothing asks. Past the timeout, a `POST` fails, and the child
    /// is sent a `notifications/cancelled` for each request it left
    /// unanswered (MCP, "Lifecycle", "Timeouts": "the sender SHOULD issue a
    /// cancellation notification for that request"), but `initialize`,
    /// which a client must not cancel (MCP, "Cancellation").
    pub(crate) async fn post(&self, messages: Vec<Message>) -> Result<http::Response<Body>, Fault> {
        let ids: Vec<Value> = messages.iter().filter_map(Message::request_id).collect();
        let lines: String = messages
            .iter()
            .map(|m| format!("{}\n", Value::from(m.clone())))
            .collect();
        let cancellable: Vec<Value> = messages
            .iter()
            .filter(|m| m.method() != Some("initialize"))
            .filter_map(Message::request_id)
            .collect();
        let tokens = messages
            .iter()
            .filter_map(|m| m.params()?.pointer("/_meta/progressToken"))
            .filter(|t| !t.is_null())
            .cloned()
            .collect();
        let cancels = messages
            .iter()
            .filter(|m| m.method() == Some("notifications/cancelled"));
        let dropped: Vec<&Value> = cancels
            .filter_map(|m| m.params()?.get("requestId"))
            .collect();
        let (tx, rx) = mpsc::channel(self.limits.backlog);
        // Registered before writing, so no answer is missed; none if
        // nothing asks.
        let number = {
            let mut routes = lock(&self.routes);
            for post in &mut routes.posts {
                post.ids.retain(|id| !dropped.contains(&id));
            }
            routes
                .posts
                .retain(|p| !p.tx.is_closed() && !p.ids.is_empty());
            if ids
                .iter()
                .any(|id| routes.posts.iter().any(|post| post.ids.contains(id)))
            {
                return Err(BadRequest::fault("an MCP request ID is already pending"));
            }
            (!ids.is_empty()).then(|| {
                let number = routes.next;
                routes.next += 1;
                let asked = Vec::new();
                let post = Post {
                    number,
                    ids,
                    tokens,
                    asked,
                    tx,
                };
                routes.posts.push(post);
                number
            })
        };
        // The deadline covers the write too: a child that stops reading
        // stdin fills the queue, and a POST waiting for room must still end.
        let until = Instant::now().checked_add(self.limits.timeout);
        within(until, self.stdin.send(lines.into()))
            .await?
            .map_err(|_| stopped())?;
        let Some(number) = number else {
            return Ok(empty(StatusCode::ACCEPTED));
        };
        // Weak, so the stream does not keep its session's routes, and so
        // its own sender, open once the session is gone.
        let (routes, stdin) = (Arc::downgrade(&self.routes), self.stdin.downgrade());
        let cancel = move || {
            let (Some(routes), Some(stdin)) = (routes.upgrade(), stdin.upgrade()) else {
                return;
            };
            let mut all = lock(&routes);
            let Some(i) = all.posts.iter().position(|p| p.number == number) else {
                return;
            };
            let left = all.posts.remove(i).ids;
            let lines: String = left
                .iter()
                .filter(|id| cancellable.contains(id))
                .map(|id| format!("{}\n", cancelled(id)))
                .collect();
            if !lines.is_empty() {
                // In order after the requests, once the child takes them.
                tokio::spawn(async move { stdin.send(lines.into()).await });
            }
        };
        Ok(events(rx, until, cancel))
    }
}

/// The notification that cancels the request `id`.
fn cancelled(id: &Value) -> Value {
    let reason = "the gateway's timeout passed";
    let params = json!({ "requestId": id, "reason": reason });
    json!({ "jsonrpc": "2.0", "method": "notifications/cancelled", "params": params })
}

fn route(routes: &mut Routes, message: Message) {
    routes.posts.retain(|p| !p.tx.is_closed());
    let Some(method) = message.method() else {
        let id = message.response_id();
        let asked = match &id {
            Some(id) => routes.posts.iter().position(|p| p.ids.contains(id)),
            None => (!routes.posts.is_empty()).then_some(0),
        };
        if let Some(i) = asked {
            let ids = &mut routes.posts[i].ids;
            match &id {
                Some(id) => ids.retain(|x| x != id),
                None => ids.clear(),
            }
            send(routes, i, message);
        }
        return;
    };
    let params = message.params().unwrap_or(&Value::Null);
    let owner = match method {
        "notifications/progress" => {
            let token = &params["progressToken"];
            routes.posts.iter().position(|p| p.tokens.contains(token))
        }
        "notifications/cancelled" => {
            let request = &params["requestId"];
            routes.posts.iter().position(|p| p.asked.contains(request))
        }
        _ => None,
    };
    match owner {
        Some(i) => send(routes, i, message),
        None if routes.listening() => {
            if let Some(tx) = &routes.listener
                && tx.try_send(message).is_err()
            {
                routes.listener = None;
            }
        }
        None if !routes.posts.is_empty() => {
            routes.posts[0].asked.extend(message.request_id());
            send(routes, 0, message);
        }
        None => {}
    }
}

/// Sends `message` to the `POST` at `i`, which ends once every request of
/// its is answered, or once it falls behind.
fn send(routes: &mut Routes, i: usize, message: Message) {
    let post = &routes.posts[i];
    let fell_behind = post.tx.try_send(message).is_err();
    if post.ids.is_empty() || fell_behind {
        routes.posts.remove(i);
        routes.touched = Instant::now();
    }
}

/// Stops a session's `child` as MCP asks of a client ("Lifecycle",
/// "Shutdown"): closes its stdin, by ending the `writer` that holds it,
/// then sends its process `group` SIGTERM if it has not exited within
/// [`GRACE`], and SIGKILL, which also ends anything the child started
/// and left behind; then reaps it.
async fn stop(mut child: Child, group: Option<i32>, writer: JoinHandle<()>) {
    writer.abort();
    if timeout(GRACE, child.wait()).await.is_err() {
        signal(&mut child, group, false);
        let _ = timeout(GRACE, child.wait()).await;
    }
    signal(&mut child, group, true);
    let _ = child.wait().await;
}

/// Sends the child's process `group` SIGKILL if `kill`, else SIGTERM.
/// Without unix process groups and signals, kills only the child, if
/// `kill`.
#[cfg_attr(not(unix), allow(unused_variables))]
fn signal(child: &mut Child, group: Option<i32>, kill: bool) {
    #[cfg(unix)]
    if let Some(group) = group {
        let signal = if kill { libc::SIGKILL } else { libc::SIGTERM };
        // SAFETY: kill(2) takes no pointer and touches none of our memory.
        #[allow(unsafe_code)]
        unsafe {
            libc::kill(-group, signal);
        }
    }
    if kill {
        let _ = child.start_kill();
    }
}

/// The fault of a child that stopped taking messages.
fn stopped() -> Fault {
    Upstream::fault("the MCP server stopped")
}

/// Waits until `due`, or forever for `None`: a time past what an
/// `Instant` holds.
async fn until(due: Option<Instant>) {
    match due {
        Some(due) => sleep_until(due).await,
        None => std::future::pending().await,
    }
}

/// `work`, unless `until` passes first (never for `None`): then the fault
/// of a `POST` not answered in time.
async fn within<T>(until: Option<Instant>, work: impl Future<Output = T>) -> Result<T, Fault> {
    match until {
        Some(until) => timeout_at(until, work).await.map_err(|_| late()),
        None => Ok(work.await),
    }
}

/// An event stream of what `rx` receives, until it closes, or failing at
/// `until` if any, after calling `on_late`.
fn events(
    rx: mpsc::Receiver<Message>,
    until: Option<Instant>,
    on_late: impl FnOnce() + Send + 'static,
) -> http::Response<Body> {
    let events = stream::unfold(Some((rx, on_late)), move |state| async move {
        let (mut rx, on_late) = state?;
        match within(until, rx.recv()).await {
            Ok(next) => {
                let event = Bytes::from(sse::event(None, Value::from(next?)));
                Some((Ok(event), Some((rx, on_late))))
            }
            Err(late) => {
                on_late();
                Some((Err(late), None))
            }
        }
    });
    let mut resp = http::Response::new(Body::stream(events));
    let sse = HeaderValue::from_static("text/event-stream");
    resp.headers_mut().insert(CONTENT_TYPE, sse);
    resp
}

/// A child's stdout as lines (MCP, "stdio": messages are delimited by
/// newlines) of at most `max` bytes each. A longer one is read through
/// without being held, and given empty, which is no message, as is a line
/// of no JSON (not UTF-8, say).
struct Lines<R> {
    reader: R,
    /// What was read of the next line.
    line: Vec<u8>,
    /// Whether the next line is over `max`.
    over: bool,
    max: usize,
}

impl<R: AsyncBufRead + Unpin> Lines<R> {
    fn new(reader: R, max: usize) -> Self {
        Self {
            reader,
            line: Vec::new(),
            over: false,
            max,
        }
    }

    /// The next line, without its end; `None` once the output ends.
    /// Cancel-safe: what was read of a line stays for the next call.
    async fn next(&mut self) -> std::io::Result<Option<Vec<u8>>> {
        loop {
            let buf = self.reader.fill_buf().await?;
            if buf.is_empty() {
                // The last line may lack its end.
                let had = !self.line.is_empty() || std::mem::take(&mut self.over);
                return Ok(had.then(|| std::mem::take(&mut self.line)));
            }
            let end = buf.iter().position(|&b| b == b'\n');
            let part = &buf[..end.unwrap_or(buf.len())];
            if self.line.len() + part.len() > self.max {
                self.over = true;
                self.line = Vec::new();
            }
            if !self.over {
                self.line.extend_from_slice(part);
            }
            let used = end.map_or(buf.len(), |end| end + 1);
            self.reader.consume(used);
            if end.is_some() {
                self.over = false;
                return Ok(Some(std::mem::take(&mut self.line)));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_line_over_the_limit_is_read_through_and_given_empty() {
        let out: &[u8] = b"12345\n123456789\n\xff\x00\n12";
        let mut lines = Lines::new(out, 5);
        let mut got = Vec::new();
        while let Ok(Some(line)) = lines.next().await {
            got.push(line);
        }
        let want: [&[u8]; 4] = [b"12345", b"", b"\xff\x00", b"12"];
        assert_eq!(got, want);
    }
}
