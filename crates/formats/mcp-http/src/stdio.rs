//! An MCP server over stdio with a native HTTP send callback: one child process per
//! session, one JSON-RPC message per line. It answers the requests
//! [`StreamableHttp`](crate::StreamableHttp) sends, as an HTTP MCP server
//! would: messages under the success statuses, the session in its header,
//! and every failure as a [`Fault`], which the client's format writes.

use std::collections::HashMap;
use std::ffi::OsString;
use std::hash::{BuildHasher, RandomState};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use respan_gateway_core::futures::TryStreamExt;
use respan_gateway_core::{BadRequest, ErrorCode, Extensions, Fault, Upstream};
use respan_gateway_http::Status;
use respan_gateway_http::http::header::AUTHORIZATION;
use respan_gateway_http::http::{self, HeaderValue, Method, StatusCode};
use respan_gateway_http::http_body_util::BodyExt;
use respan_gateway_http::{Body, Completion, Credential};
use respan_gateway_mcp::Message;
use tokio::process::Command;

use crate::session::{Limits, Session};
use crate::{PARSE_ERROR, SESSION, lock, read};
use respan_gateway_mcp::RpcCode;

/// Sessions by id and a keyed hash of their owner; `None` while starting.
type Sessions = Mutex<HashMap<(String, u64), Option<Arc<Session>>>>;

/// Runs `program` for each new session (an `initialize` without a session
/// id), and sends each line it writes to the request waiting for it.
///
/// A session belongs to the owner that started it: the call's [`Owner`]
/// extension if any, else the client's `Authorization`, as the adapter
/// lifted it ([`Credential`]); callers with neither are one owner. A
/// session ends when its child exits, on `DELETE`, when idle, or when the
/// answer to its `initialize` ends before its first message, as when its
/// client left or it timed out, since its id then reached no one. Its child
/// is then stopped as MCP asks of a client: stdin closed, then SIGTERM,
/// then SIGKILL to its process group, each a moment later (on Windows it
/// starts in a process group of its own, and is ended with the processes it
/// started by `taskkill /T /F`). Its id is 128
/// bits from the operating system's secure random source (MCP: a session
/// id "SHOULD be globally unique and cryptographically secure").
///
/// The child is given only the gateway's `PATH` (and, on Windows, the few
/// variables a program cannot start without), so a program is found as
/// from a shell, and what [`env`](Self::env) adds: an MCP server is handed
/// only the secrets meant for it, not the gateway's own. It runs in the
/// gateway's directory unless [`current_dir`](Self::current_dir) says
/// otherwise, and writes its stderr to the gateway's, where MCP lets a
/// server log ("stdio") and where the operator reads the gateway's own.
///
/// Its own failures are faults the client's format writes, each with a
/// JSON-RPC code ([`RpcCode`]) of its own in the range JSON-RPC 2.0 (5.1)
/// reserves for a server's own errors, -32000 to -32099, but -32000 and
/// -32002, which MCP and its SDKs use for a closed connection and a
/// missing resource; -32001 is theirs for a request timeout too:
///
/// | When | Status | `error_code` | JSON-RPC |
/// |---|---|---|---|
/// | A request other than `initialize` names no session (MCP: "SHOULD respond … with HTTP 400 Bad Request") | 400 | `no_session` | -32003 |
/// | Its session is not there, or another owner's, as if not there (MCP: a terminated session "MUST respond … with HTTP 404 Not Found") | 404 | `no_session` | -32003 |
/// | A second `GET` while the session's stream is open | 409 | `busy` | -32004 |
/// | A new session past [`max_sessions`](Self::max_sessions) or [`max_total`](Self::max_total) | 503 | `too_many_sessions` | -32005 |
/// | A `POST`'s requests are not all answered within [`timeout`](Self::timeout) | 504 | `timed_out` | -32001 |
///
/// Authenticate callers before this transport. Owners taken from an
/// unauthenticated `Authorization` header are unbounded, since anyone can
/// send a new one; only the total limit ([`max_total`](Self::max_total))
/// then caps the children.
#[derive(Debug)]
pub struct StdioTransport {
    program: PathBuf,
    args: Vec<String>,
    env: Vec<(OsString, OsString)>,
    dir: Option<PathBuf>,
    max_sessions: usize,
    max_total: usize,
    limits: Limits,
    sessions: Arc<Sessions>,
    /// Secret keys, so an owner's hash cannot be chosen to match another's.
    keys: RandomState,
}

/// Who a call's session belongs to, set as an extension by a plugin that
/// knows the caller, such as one that authenticates it
/// (`cx.extensions_mut().insert(Owner(tenant))`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Owner(pub String);

impl StdioTransport {
    /// Run `program` with `args` per session, with the default limits.
    pub fn new(program: impl Into<PathBuf>, args: impl IntoIterator<Item: Into<String>>) -> Self {
        Self {
            program: program.into(),
            args: args.into_iter().map(Into::into).collect(),
            env: Vec::new(),
            dir: None,
            max_sessions: 8,
            max_total: 64,
            limits: Limits {
                idle: Duration::from_secs(600),
                timeout: Duration::from_secs(300),
                backlog: 64,
                max_line: 32 << 20,
            },
            sessions: Arc::default(),
            keys: RandomState::new(),
        }
    }

    /// Sets `key` to `value` in the child's environment.
    pub fn env(mut self, key: impl Into<OsString>, value: impl Into<OsString>) -> Self {
        self.env.push((key.into(), value.into()));
        self
    }

    /// Runs the child in `dir`.
    pub fn current_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.dir = Some(dir.into());
        self
    }

    /// At most `n` sessions per owner at once. By default 8: an owner runs
    /// a few clients at once (an editor, an agent, a terminal), each with a
    /// session per server, and no one owner may take the whole
    /// [`max_total`](Self::max_total).
    pub fn max_sessions(mut self, n: usize) -> Self {
        self.max_sessions = n;
        self
    }

    /// At most `n` sessions, so child processes, at once in all. By default
    /// 64: eight owners at their default [`max_sessions`](Self::max_sessions),
    /// a number of processes any host holds.
    pub fn max_total(mut self, n: usize) -> Self {
        self.max_total = n;
        self
    }

    /// Ends a session after `idle` with no `POST` waiting, no `GET` stream
    /// open and no request. By default 10 minutes: a person pauses between
    /// turns for longer than a minute or two, and a client whose session
    /// ended starts a new one on its 404 (MCP: it "MUST start a new
    /// session").
    pub fn idle(mut self, idle: Duration) -> Self {
        self.limits.idle = idle;
        self
    }

    /// Ends a `POST` whose requests are not all answered within `timeout`.
    /// By default 5 minutes: a tool may run for minutes (a build, a crawl),
    /// and MCP leaves the timeout to the sender ("Lifecycle", "Timeouts").
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.limits.timeout = timeout;
        self
    }

    /// Ends a stream that falls `n` messages behind its client, and holds
    /// up to `n` writes for a child's stdin. By default 64: a client that
    /// reads at all keeps up with a server's progress, and one this far
    /// behind is not reading.
    pub fn backlog(mut self, n: usize) -> Self {
        self.limits.backlog = n;
        self
    }

    /// Reads at most `bytes` of one line, so of one message, the child
    /// writes; a longer line is no message. By default 32 MiB, the most
    /// an [`HttpTransport`](respan_gateway_http::HttpTransport) reads of one
    /// answer, so a server over stdio may say as much at once as one over
    /// HTTP.
    pub fn max_line(mut self, bytes: usize) -> Self {
        self.limits.max_line = bytes;
        self
    }

    /// Create fresh outgoing I/O for each call, sharing this session
    /// registry. Its answers are events of one line each, so read up to
    /// [`max_line`](Self::max_line).
    pub fn connect(
        self,
    ) -> impl Fn(Extensions) -> respan_gateway_http::ProviderIo + Clone + Send + Sync {
        let limit = self.limits.max_line;
        let backend = Arc::new(self);
        respan_gateway_http::connect(
            move |request| {
                let backend = backend.clone();
                async move { backend.send(request).await }
            },
            limit,
        )
    }

    /// Send an encoded call and return its response.
    pub async fn send(
        &self,
        call: http::Request<Body>,
    ) -> Result<(http::Response<Body>, Option<Completion>), Fault> {
        let (mut head, body) = call.into_parts();
        let extensions = head.extensions.remove::<Extensions>().unwrap_or_default();
        let owner = match extensions.get::<Owner>() {
            Some(Owner(owner)) => self.keys.hash_one((true, Some(owner.as_bytes()))),
            None => {
                let credential = extensions.get::<Credential>();
                let authorization = credential.and_then(|c| c.get(AUTHORIZATION.as_str()));
                self.keys
                    .hash_one((false, authorization.map(HeaderValue::as_bytes)))
            }
        };
        let body = body.collect().await?.to_bytes();
        let Some(id) = head.headers.get(SESSION) else {
            return match head.method {
                Method::POST => Ok((self.start(owner, &body).await?, None)),
                _ => Err(no_session_named()),
            };
        };
        let key = (id.to_str().unwrap_or_default().to_owned(), owner);
        let Some(session) = lock(&self.sessions).get(&key).cloned().flatten() else {
            return Err(no_session());
        };
        session.touch();
        let answer = match head.method {
            Method::POST => session.post(posted(&body)?).await?,
            Method::GET => session.listen().ok_or_else(|| {
                let open = "the session's stream is already open";
                own(Fault::new(open), "busy", -32004, StatusCode::CONFLICT)
            })?,
            Method::DELETE => {
                lock(&self.sessions).remove(&key);
                empty(StatusCode::NO_CONTENT)
            }
            _ => return Err(BadRequest::fault("not an MCP method")),
        };
        Ok((answer, None))
    }

    /// Starts a session for `owner` with the `initialize` in `body`.
    async fn start(&self, owner: u64, body: &[u8]) -> Result<http::Response<Body>, Fault> {
        let messages = posted(body)?;
        if !messages.iter().any(|m| m.method() == Some("initialize")) {
            return Err(no_session_named());
        }
        let key = (session_id()?, owner);
        {
            // Its place is held while the child starts, outside the lock.
            let mut sessions = lock(&self.sessions);
            let owned = sessions.keys().filter(|(_, o)| *o == owner).count();
            if owned >= self.max_sessions || sessions.len() >= self.max_total {
                let full = Fault::new("too many sessions");
                let status = StatusCode::SERVICE_UNAVAILABLE;
                return Err(own(full, "too_many_sessions", -32005, status));
            }
            sessions.insert(key.clone(), None);
        }
        let (all, forgotten) = (Arc::downgrade(&self.sessions), key.clone());
        let forget = move || {
            if let Some(all) = all.upgrade() {
                lock(&all).remove(&forgotten);
            }
        };
        let mut command = Command::new(&self.program);
        command
            .args(&self.args)
            .env_clear()
            .stderr(Stdio::inherit());
        if let Some(path) = std::env::var_os("PATH") {
            command.env("PATH", path);
        }
        // Windows programs, and the `.cmd` shims such as `npx`'s, need
        // these to start at all.
        #[cfg(windows)]
        for name in ["SystemRoot", "ComSpec", "PATHEXT", "TEMP", "TMP"] {
            if let Some(value) = std::env::var_os(name) {
                command.env(name, value);
            }
        }
        command.envs(self.env.iter().cloned());
        if let Some(dir) = &self.dir {
            command.current_dir(dir);
        }
        let started = Session::start(&mut command, self.limits, forget.clone());
        let session = {
            let mut sessions = lock(&self.sessions);
            match (started, sessions.get_mut(&key)) {
                (Ok(session), Some(slot)) => slot.insert(Arc::new(session)).clone(),
                (Err(fault), _) => {
                    sessions.remove(&key);
                    return Err(fault);
                }
                // It ended already.
                (Ok(_), None) => return Err(Upstream::fault("the MCP server stopped")),
            }
        };
        // Forgotten unless a message of its answer reaches the client,
        // which learns the id in the answer's head.
        let mut unclaimed = OnDrop(Some(forget));
        let resp = session.post(messages).await?;
        let id = HeaderValue::from_str(&key.0).map_err(|e| Fault::internal("an id").with_source(e));
        let mut resp =
            resp.map(|body| Body::stream(body.inspect_ok(move |_| drop(unclaimed.0.take()))));
        resp.headers_mut().insert(SESSION, id?);
        Ok(resp)
    }
}

/// Calls its function when dropped, unless it was taken out first.
struct OnDrop<F: FnOnce()>(Option<F>);

impl<F: FnOnce()> Drop for OnDrop<F> {
    fn drop(&mut self) {
        if let Some(f) = self.0.take() {
            f();
        }
    }
}

/// A new session's id: 128 random bits as 32 hex digits, which are visible
/// ASCII, as MCP requires of a session id.
fn session_id() -> Result<String, Fault> {
    let mut bits = [0u8; 16];
    getrandom::fill(&mut bits)
        .map_err(|e| Fault::internal("no secure random source").with_source(e))?;
    Ok(bits.iter().map(|b| format!("{b:02x}")).collect())
}

/// The messages a `POST` carries; `Err` with a parse error if none.
fn posted(body: &[u8]) -> Result<Vec<Message>, Fault> {
    let read = read(body).map(|(messages, _)| messages);
    read.ok_or_else(|| BadRequest::fault("not JSON-RPC").with(PARSE_ERROR))
}

/// A success of `status` with no body, such as a 202.
pub(crate) fn empty(status: StatusCode) -> http::Response<Body> {
    let mut resp = http::Response::new(Body::full(""));
    *resp.status_mut() = status;
    resp
}

/// `fault`, one of the transport's own: its `code`, JSON-RPC code and
/// `status`.
fn own(fault: Fault, code: &'static str, rpc: i64, status: StatusCode) -> Fault {
    let fault = fault.with(ErrorCode(code.into())).with(RpcCode(rpc));
    fault.with(Status(status))
}

/// A request that names no session, but `initialize`: 400.
fn no_session_named() -> Fault {
    let fault = BadRequest::fault("the request names no session");
    own(fault, "no_session", -32003, StatusCode::BAD_REQUEST)
}

/// A request for a session that is not there, or not the caller's: 404.
fn no_session() -> Fault {
    let fault = Fault::new("no such session");
    own(fault, "no_session", -32003, StatusCode::NOT_FOUND)
}

/// The fault of a `POST` the child did not take or answer in time.
pub(crate) fn late() -> Fault {
    let late = Fault::new("the MCP server did not answer in time");
    own(late, "timed_out", -32001, StatusCode::GATEWAY_TIMEOUT)
}
