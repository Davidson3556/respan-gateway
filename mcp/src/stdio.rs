//! An MCP server over stdio with a native HTTP send callback: one child process per
//! session, one JSON-RPC message per line. It answers the requests
//! [`StreamableHttp`](crate::StreamableHttp) sends, as an HTTP MCP server
//! would: messages under the success statuses, the session in its header,
//! and every failure as a [`Fault`], which the client's format writes.

use std::collections::HashMap;
use std::hash::{BuildHasher, RandomState};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use respan_gateway_http::Status;
use respan_gateway_http::http::header::AUTHORIZATION;
use respan_gateway_http::http::{self, HeaderValue, Method, StatusCode};
use respan_gateway_http::http_body_util::BodyExt;
use respan_gateway_http::{Body, Completion, Credential};
use respan_gateway_sdk::{BadRequest, ErrorCode, Extensions, Fault, Upstream};
use tokio::process::Command;

use crate::session::Session;
use crate::{SESSION, lock, read};

/// Stands for "never": `Duration::MAX` would overflow a deadline.
const CENTURY: Duration = Duration::from_secs(100 * 365 * 24 * 3600);
/// Sessions by id and a keyed hash of their owner; `None` while starting.
type Sessions = Mutex<HashMap<(String, u64), Option<Arc<Session>>>>;
type Reply = http::Response<Body>;

/// The code of a failure for a session that is not there, or not the
/// caller's.
pub const NO_SESSION: &str = "no_session";
/// The code of a second `GET` while a session's stream is open.
pub const BUSY: &str = "busy";
/// The code of a new session past a limit.
pub const TOO_MANY_SESSIONS: &str = "too_many_sessions";

/// Runs `program` for each new session (an `initialize` without a session
/// id), and sends each line it writes to the request waiting for it.
///
/// A session belongs to the owner that started it: the call's [`Owner`]
/// extension if any, else the client's `Authorization`, as the adapter
/// lifted it ([`Credential`]). A request from another owner fails 404
/// [`NO_SESSION`], as if the session did not exist. A second `GET` while
/// the session's stream is open fails 409 [`BUSY`]. A session ends when its
/// child exits, on `DELETE`, or when idle; its child is then killed and
/// reaped. A `POST` whose requests are not all answered in time ends with
/// a [`TIMED_OUT`](crate::TIMED_OUT) error for each.
///
/// Authenticate callers before this transport. Owners taken from an
/// unauthenticated `Authorization` header are unbounded, since anyone can
/// send a new one; only the total limit ([`max_total`](Self::max_total))
/// then caps the children.
#[derive(Debug)]
pub struct StdioTransport {
    program: PathBuf,
    args: Vec<String>,
    max_sessions: usize,
    max_total: usize,
    idle: Duration,
    timeout: Duration,
    sessions: Arc<Sessions>,
    minted: AtomicU64,
    /// Secret keys, so a session id cannot be guessed.
    keys: RandomState,
}

/// Who a call's session belongs to, set as an extension by a plugin that
/// knows the caller, such as one that authenticates it
/// (`cx.extensions_mut().insert(Owner(tenant))`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Owner(pub String);

impl StdioTransport {
    /// Run `program` with `args` per session.
    pub fn new(program: impl Into<PathBuf>, args: impl IntoIterator<Item: Into<String>>) -> Self {
        Self {
            program: program.into(),
            args: args.into_iter().map(Into::into).collect(),
            max_sessions: 8,
            max_total: 64,
            idle: Duration::from_secs(600),
            timeout: Duration::from_secs(300),
            sessions: Arc::default(),
            minted: AtomicU64::new(0),
            keys: RandomState::new(),
        }
    }

    /// At most `n` sessions per owner at once (default 8); a new one past
    /// it fails 503 [`TOO_MANY_SESSIONS`].
    pub fn max_sessions(mut self, n: usize) -> Self {
        self.max_sessions = n;
        self
    }

    /// At most `n` sessions at once in all (default 64); a new one past it
    /// fails 503 [`TOO_MANY_SESSIONS`].
    pub fn max_total(mut self, n: usize) -> Self {
        self.max_total = n;
        self
    }

    /// Ends a session with no `POST` waiting and no request for `idle`
    /// (default 10 minutes).
    pub fn idle(mut self, idle: Duration) -> Self {
        self.idle = idle.min(CENTURY);
        self
    }

    /// Ends a `POST` whose requests are not all answered within `timeout`
    /// (default 5 minutes).
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout.min(CENTURY);
        self
    }

    /// Starts a session for `owner` with the `initialize` in `body`.
    async fn start(&self, owner: u64, body: &[u8]) -> Result<Reply, Fault> {
        let messages = read(body).map(|(m, _)| m).unwrap_or_default();
        if !messages.iter().any(|m| m["method"] == "initialize") {
            return Err(no_session());
        }
        let n = self.minted.fetch_add(1, Ordering::Relaxed);
        let hash = |i: u8| self.keys.hash_one((n, i));
        let key = (format!("{:016x}{:016x}", hash(0), hash(1)), owner);
        {
            // Its place is held while the child starts, outside the lock.
            let mut sessions = lock(&self.sessions);
            let owned = sessions.keys().filter(|(_, o)| *o == owner).count();
            if owned >= self.max_sessions || sessions.len() >= self.max_total {
                let full = "too many sessions";
                let fault = Fault::new(full).with(ErrorCode(TOO_MANY_SESSIONS.into()));
                return Err(fault.with(Status(StatusCode::SERVICE_UNAVAILABLE)));
            }
            sessions.insert(key.clone(), None);
        }
        let all = Arc::downgrade(&self.sessions);
        let forget = key.clone();
        let ended = move || {
            if let Some(all) = all.upgrade() {
                lock(&all).remove(&forget);
            }
        };
        let mut command = Command::new(&self.program);
        let started = Session::start(command.args(&self.args), self.idle, self.timeout, ended);
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
        let mut resp = session.post(messages).await?;
        let id = HeaderValue::from_str(&key.0).map_err(|e| Fault::internal("an id").with_source(e));
        resp.headers_mut().insert(SESSION, id?);
        Ok(resp)
    }
}

/// A success of `status` with no body, such as a 202.
pub(crate) fn reply(status: StatusCode) -> Reply {
    let body = Body::full("");
    let mut resp = http::Response::new(body);
    *resp.status_mut() = status;
    resp
}

/// A session that is not there, or not the caller's: 404.
fn no_session() -> Fault {
    Fault::new("no such session")
        .with(ErrorCode(NO_SESSION.into()))
        .with(Status(StatusCode::NOT_FOUND))
}

impl StdioTransport {
    /// Create fresh outgoing I/O for each call, sharing this session registry.
    pub fn connect(
        self,
    ) -> impl Fn(Extensions) -> respan_gateway_http::Outgoing + Clone + Send + Sync {
        let backend = Arc::new(self);
        // Its answers are made of the child's lines, read whole before
        // they are: a binding's limit would bound nothing more.
        respan_gateway_http::connect(
            move |request| {
                let backend = backend.clone();
                async move { backend.send(request).await }
            },
            usize::MAX,
        )
    }

    /// Send an encoded call and return its response.
    pub async fn send(
        &self,
        call: http::Request<Body>,
    ) -> Result<(http::Response<Body>, Option<Completion>), Fault> {
        Ok((self.send_http(call).await?, None))
    }
}

impl StdioTransport {
    async fn send_http(&self, call: http::Request<Body>) -> Result<Reply, Fault> {
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
                Method::POST => self.start(owner, &body).await,
                _ => Err(BadRequest::fault("no session")),
            };
        };
        let key = (id.to_str().unwrap_or_default().to_owned(), owner);
        let Some(session) = lock(&self.sessions).get(&key).cloned().flatten() else {
            return Err(no_session());
        };
        session.touch();
        match head.method {
            Method::POST => match read(&body) {
                Some((messages, _)) => session.post(messages).await,
                None => Err(BadRequest::fault("not JSON-RPC")),
            },
            Method::GET => session.listen().ok_or_else(|| {
                let open = "the stream is already open";
                Fault::new(open)
                    .with(ErrorCode(BUSY.into()))
                    .with(Status(StatusCode::CONFLICT))
            }),
            Method::DELETE => {
                lock(&self.sessions).remove(&key);
                Ok(reply(StatusCode::NO_CONTENT))
            }
            _ => Err(BadRequest::fault("not an MCP method")),
        }
    }
}
