//! An MCP server over stdio as a [`Transport`]: one child process per
//! session, one JSON-RPC message per line. It answers the requests
//! [`StreamableHttp`](crate::StreamableHttp) sends, as an HTTP MCP server
//! would.

use std::collections::HashMap;
use std::hash::{BuildHasher, RandomState};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use respan_gateway_core::http::header::{AUTHORIZATION, CONTENT_TYPE};
use respan_gateway_core::http::{self, HeaderValue, Method, StatusCode};
use respan_gateway_core::http_body_util::{BodyExt, Full};
use respan_gateway_core::serde_json::Value;
use respan_gateway_core::{Body, Fault, Transport, async_trait};
use tokio::process::Command;

use crate::session::Session;
use crate::{SESSION, error, lock, read};

/// Sessions by id and a keyed hash of the `Authorization` that started them.
type Sessions = Mutex<HashMap<(String, u64), Arc<Session>>>;
type Reply = http::Response<Body>;

const NO_SESSION: &str = "no such session";

/// Runs `program` for each new session (an `initialize` without a session
/// id), and sends each line it writes to the request waiting for it.
///
/// A session belongs to the `Authorization` header that started it: a
/// request with another one gets 404, as if the session did not exist. A
/// second `GET` while the session's stream is open gets 409. A session
/// ends when its child exits, on `DELETE`, or when idle; its child is then
/// killed and reaped.
#[derive(Debug)]
pub struct StdioTransport {
    program: PathBuf,
    args: Vec<String>,
    max_sessions: usize,
    idle: Duration,
    sessions: Arc<Sessions>,
    minted: AtomicU64,
    /// Secret keys, so a session id cannot be guessed.
    keys: RandomState,
}

impl StdioTransport {
    /// Run `program` with `args` per session.
    pub fn new(program: impl Into<PathBuf>, args: impl IntoIterator<Item: Into<String>>) -> Self {
        Self {
            program: program.into(),
            args: args.into_iter().map(Into::into).collect(),
            max_sessions: 64,
            idle: Duration::from_secs(600),
            sessions: Arc::default(),
            minted: AtomicU64::new(0),
            keys: RandomState::new(),
        }
    }

    /// At most `n` sessions at once (default 64); a new one past it gets 503.
    pub fn max_sessions(mut self, n: usize) -> Self {
        self.max_sessions = n;
        self
    }

    /// Ends a session with no `POST` waiting and no request for `idle`
    /// (default 10 minutes).
    pub fn idle(mut self, idle: Duration) -> Self {
        self.idle = idle;
        self
    }

    /// Starts a session for `owner` with the `initialize` in `body`.
    async fn start(&self, owner: u64, body: &[u8]) -> Result<Reply, Fault> {
        let messages = read(body).map(|(m, _)| m).unwrap_or_default();
        if !messages.iter().any(|m| m["method"] == "initialize") {
            return Ok(reply(StatusCode::NOT_FOUND, Some(NO_SESSION)));
        }
        let n = self.minted.fetch_add(1, Ordering::Relaxed);
        let id = [0, 1].map(|i| format!("{:016x}", self.keys.hash_one((n, i))));
        let key = (id.concat(), owner);
        let session = {
            let mut sessions = lock(&self.sessions);
            if sessions.len() >= self.max_sessions {
                return Ok(reply(
                    StatusCode::SERVICE_UNAVAILABLE,
                    Some("too many sessions"),
                ));
            }
            let (all, forget) = (Arc::downgrade(&self.sessions), key.clone());
            let ended = move || drop(all.upgrade().map(|all| lock(&all).remove(&forget)));
            let mut command = Command::new(&self.program);
            let session = Session::start(command.args(&self.args), self.idle, ended)?;
            let session = Arc::new(session);
            sessions.insert(key.clone(), session.clone());
            session
        };
        let mut resp = session.post(messages).await?;
        let id = HeaderValue::from_str(&key.0).map_err(|e| Fault::internal("an id").with_source(e));
        resp.headers_mut().insert(SESSION, id?);
        Ok(resp)
    }
}

/// A response of `status`, with a JSON-RPC error of `message` if any.
pub(crate) fn reply(status: StatusCode, message: Option<&str>) -> Reply {
    let error = message.map(|m| error(Value::Null, -32600, m).to_string());
    let body = Full::from(error.clone().unwrap_or_default());
    let mut resp = http::Response::new(body.map_err(Fault::from).boxed_unsync());
    *resp.status_mut() = status;
    if error.is_some() {
        let json = HeaderValue::from_static("application/json");
        resp.headers_mut().insert(CONTENT_TYPE, json);
    }
    resp
}

#[async_trait]
impl Transport for StdioTransport {
    async fn send(&self, call: http::Request<Body>) -> Result<Reply, Fault> {
        let (head, body) = call.into_parts();
        let credential = head.headers.get(AUTHORIZATION).map(HeaderValue::as_bytes);
        let owner = self.keys.hash_one(credential);
        let body = body.collect().await?.to_bytes();
        let Some(id) = head.headers.get(SESSION) else {
            return match head.method {
                Method::POST => self.start(owner, &body).await,
                _ => Ok(reply(StatusCode::BAD_REQUEST, Some("no session"))),
            };
        };
        let key = (id.to_str().unwrap_or_default().to_owned(), owner);
        let Some(session) = lock(&self.sessions).get(&key).cloned() else {
            return Ok(reply(StatusCode::NOT_FOUND, Some(NO_SESSION)));
        };
        session.touch();
        match head.method {
            Method::POST => match read(&body) {
                Some((messages, _)) => session.post(messages).await,
                None => Ok(reply(StatusCode::BAD_REQUEST, Some("not JSON-RPC"))),
            },
            Method::GET => Ok(session.listen().unwrap_or_else(|| {
                reply(StatusCode::CONFLICT, Some("the stream is already open"))
            })),
            Method::DELETE => {
                lock(&self.sessions).remove(&key);
                Ok(reply(StatusCode::NO_CONTENT, None))
            }
            _ => Ok(reply(StatusCode::METHOD_NOT_ALLOWED, None)),
        }
    }
}
