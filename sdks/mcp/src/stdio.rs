//! An MCP server over stdio as a [`Transport`]: one child process per
//! session, speaking JSON-RPC one message per line. It answers the
//! streamable-HTTP requests [`StreamableHttp`](crate::StreamableHttp)
//! prepares, so the kernel sees an ordinary HTTP MCP server.

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use respan_gateway_core::bytes::Bytes;
use respan_gateway_core::futures::{StreamExt, stream};
use respan_gateway_core::http::header::CONTENT_TYPE;
use respan_gateway_core::http::{self, Method, StatusCode};
use respan_gateway_core::http_body::Frame;
use respan_gateway_core::http_body_util::{BodyExt, StreamBody};
use respan_gateway_core::serde_json::{self, Value};
use respan_gateway_core::{Body, Doc, Fault, Transport, async_trait};
use respan_gateway_formats::sse;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::mpsc;

use crate::format::SESSION;
use crate::kind::{Message, rpc};

/// Starts `program` for each new session (an `initialize` without a
/// session id) and routes its lines back to the requests waiting for them.
#[derive(Debug, Clone)]
pub struct StdioTransport {
    program: PathBuf,
    args: Vec<String>,
    sessions: Arc<Mutex<HashMap<String, Arc<Session>>>>,
    minted: Arc<AtomicU64>,
}

#[derive(Debug)]
struct Session {
    stdin: tokio::sync::Mutex<ChildStdin>,
    child: Mutex<Option<Child>>,
    routes: Arc<Mutex<Routes>>,
}

/// Where the child's messages go: a response to the `POST` that asked; a
/// request or notification to the oldest open `POST`, else the `GET`
/// stream, else nowhere.
#[derive(Debug, Default)]
struct Routes {
    posts: Vec<Post>,
    listener: Option<mpsc::UnboundedSender<Value>>,
}

#[derive(Debug)]
struct Post {
    /// The requests still unanswered; the stream ends when none is left.
    ids: Vec<Value>,
    tx: mpsc::UnboundedSender<Value>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl StdioTransport {
    /// Run `program` with `args` per session.
    pub fn new(program: impl Into<PathBuf>, args: impl IntoIterator<Item = String>) -> Self {
        Self {
            program: program.into(),
            args: args.into_iter().collect(),
            sessions: Arc::default(),
            minted: Arc::default(),
        }
    }

    /// How many sessions have a running child.
    pub fn sessions(&self) -> usize {
        lock(&self.sessions).len()
    }

    fn spawn(&self) -> Result<(String, Arc<Session>), Fault> {
        let mut child = Command::new(&self.program)
            .args(&self.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| Fault::upstream("the MCP server could not be started").with_source(e))?;
        let (Some(stdin), Some(stdout)) = (child.stdin.take(), child.stdout.take()) else {
            return Err(Fault::upstream("the MCP server has no stdio"));
        };
        let routes = Arc::new(Mutex::new(Routes::default()));
        let reader = routes.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let Ok(v) = serde_json::from_str::<Value>(&line) else {
                    continue;
                };
                route(&mut lock(&reader), v);
            }
            // The child is gone: every open stream ends, unanswered.
            *lock(&reader) = Routes::default();
        });
        let id = format!("stdio-{}", self.minted.fetch_add(1, Ordering::Relaxed) + 1);
        let session = Arc::new(Session {
            stdin: tokio::sync::Mutex::new(stdin),
            child: Mutex::new(Some(child)),
            routes,
        });
        lock(&self.sessions).insert(id.clone(), session.clone());
        Ok((id, session))
    }
}

fn route(routes: &mut Routes, v: Value) {
    let m = Message::new(v.clone());
    if m.is_response() {
        let id = m.id();
        if let Some(i) = routes
            .posts
            .iter()
            .position(|p| p.ids.iter().any(|x| Some(x) == id))
        {
            let post = &mut routes.posts[i];
            let _ = post.tx.send(v);
            post.ids.retain(|x| Some(x) != id);
            if post.ids.is_empty() {
                routes.posts.remove(i);
            }
        }
    } else if let Some(post) = routes.posts.first() {
        let _ = post.tx.send(v);
    } else if let Some(listener) = &routes.listener {
        let _ = listener.send(v);
    }
}

/// A response of `status` with a JSON body.
fn json(status: StatusCode, v: &Value) -> http::Response<Body> {
    let mut resp = http::Response::new(Body::from(Doc::from(v.clone())));
    *resp.status_mut() = status;
    let ct = http::HeaderValue::from_static("application/json");
    resp.headers_mut().insert(CONTENT_TYPE, ct);
    resp
}

fn empty(status: StatusCode) -> http::Response<Body> {
    let mut resp = http::Response::new(Body::from(Doc::default()));
    *resp.status_mut() = status;
    resp
}

/// An event stream of what `rx` receives, until it closes.
fn events(rx: mpsc::UnboundedReceiver<Value>) -> http::Response<Body> {
    let events = stream::unfold(rx, |mut rx| async move {
        let v = rx.recv().await?;
        let frame = Frame::data(Bytes::from(sse::event(None, v)));
        Some((Ok::<_, Fault>(frame), rx))
    });
    let mut resp = http::Response::new(StreamBody::new(events.boxed()).boxed_unsync());
    let ct = http::HeaderValue::from_static("text/event-stream");
    resp.headers_mut().insert(CONTENT_TYPE, ct);
    resp
}

fn error(status: StatusCode, code: i64, text: &str) -> http::Response<Body> {
    json(status, Message::error(Value::Null, code, text, None).json())
}

#[async_trait]
impl Transport for StdioTransport {
    async fn send(&self, req: http::Request<Body>) -> Result<http::Response<Body>, Fault> {
        let (head, body) = req.into_parts();
        let id = head.headers.get(SESSION).and_then(|v| v.to_str().ok());
        let known = id.and_then(|id| lock(&self.sessions).get(id).cloned());
        match head.method {
            Method::GET => {
                let Some(session) = known else {
                    return Ok(error(
                        StatusCode::NOT_FOUND,
                        rpc::INVALID_REQUEST,
                        "no session",
                    ));
                };
                let (tx, rx) = mpsc::unbounded_channel();
                lock(&session.routes).listener = Some(tx);
                Ok(events(rx))
            }
            Method::DELETE => {
                let Some(id) = id.filter(|_| known.is_some()) else {
                    return Ok(empty(StatusCode::NOT_FOUND));
                };
                if let Some(session) = lock(&self.sessions).remove(id) {
                    drop(lock(&session.child).take());
                }
                Ok(empty(StatusCode::OK))
            }
            Method::POST => {
                // The body now streams, so it is collected and parsed again
                // (FINDINGS R2).
                let body = body.collect().await?.to_bytes();
                let Some(v) = Doc::from(body).json().cloned() else {
                    return Ok(error(StatusCode::BAD_REQUEST, rpc::PARSE_ERROR, "not JSON"));
                };
                let messages = match v {
                    Value::Array(all) => all,
                    one => vec![one],
                };
                let messages: Vec<Message> = messages.into_iter().map(Message::new).collect();
                let init = messages.iter().any(|m| m.method() == Some("initialize"));
                let (session, minted) = match (known, id, init) {
                    (Some(s), _, _) => (s, None),
                    (None, None, true) => {
                        let (id, s) = self.spawn()?;
                        (s, Some(id))
                    }
                    (None, Some(_), _) => {
                        return Ok(error(
                            StatusCode::NOT_FOUND,
                            rpc::INVALID_REQUEST,
                            "no such session",
                        ));
                    }
                    (None, None, false) => {
                        return Ok(error(
                            StatusCode::BAD_REQUEST,
                            rpc::INVALID_REQUEST,
                            "no session",
                        ));
                    }
                };
                let ids: Vec<Value> = messages
                    .iter()
                    .filter(|m| m.is_request())
                    .filter_map(|m| m.id().cloned())
                    .collect();
                // Registered before writing, so no response is missed.
                let rx = (!ids.is_empty()).then(|| {
                    let (tx, rx) = mpsc::unbounded_channel();
                    lock(&session.routes).posts.push(Post { ids, tx });
                    rx
                });
                let mut stdin = session.stdin.lock().await;
                for m in &messages {
                    let line = format!("{}\n", m.json());
                    stdin
                        .write_all(line.as_bytes())
                        .await
                        .map_err(|e| Fault::upstream("the MCP server stopped").with_source(e))?;
                }
                let _ = stdin.flush().await;
                drop(stdin);
                let mut resp = match rx {
                    Some(rx) => events(rx),
                    None => empty(StatusCode::ACCEPTED),
                };
                if let Some(id) = minted {
                    let id = http::HeaderValue::try_from(id)
                        .map_err(|e| Fault::internal("bad session id").with_source(e))?;
                    resp.headers_mut().insert(SESSION, id);
                }
                Ok(resp)
            }
            _ => Ok(empty(StatusCode::METHOD_NOT_ALLOWED)),
        }
    }
}
