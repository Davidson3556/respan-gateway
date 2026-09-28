//! Helpers the MCP tests share.

#![allow(dead_code)]

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use respan_gateway_core::bytes::Bytes;
use respan_gateway_core::futures::stream;
use respan_gateway_core::http::header::CONTENT_TYPE;
use respan_gateway_core::http::{self, HeaderMap, Method, StatusCode};
use respan_gateway_core::http_body::Frame;
use respan_gateway_core::http_body_util::{BodyExt, StreamBody};
use respan_gateway_core::serde_json::{self, Value};
use respan_gateway_core::{Body, Doc, Fault, Gateway, Outcome, Plugin, Transport, async_trait};
use respan_gateway_testkit::mock;
use tokio::sync::mpsc;

pub const UPSTREAM: &str = "http://mcp.test";

/// A client request to the gateway, as an MCP client sends it.
pub fn mcp(method: Method, body: &str, headers: &[(&str, &str)]) -> http::Request<Body> {
    let mut req = http::Request::builder()
        .method(method.clone())
        .uri("/mcp")
        .header("accept", "application/json, text/event-stream");
    if method == Method::POST {
        req = req.header(CONTENT_TYPE, "application/json");
    }
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    req.body(whole(body.to_owned()))
        .unwrap_or_else(|e| panic!("{e}"))
}

/// A whole body of `bytes`.
pub fn whole(bytes: impl Into<Bytes>) -> Body {
    Body::from(Doc::from(bytes.into()))
}

pub fn post(body: &str) -> http::Request<Body> {
    mcp(Method::POST, body, &[])
}

/// A JSON-RPC request.
pub fn rpc(id: i64, method: &str, params: Value) -> String {
    serde_json::json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params })
        .to_string()
}

pub fn call_tool(id: i64, name: &str) -> String {
    rpc(
        id,
        "tools/call",
        serde_json::json!({ "name": name, "arguments": { "text": "hi" } }),
    )
}

/// A gateway with `providers`, `format` served, `plugins` in order and
/// `target`.
pub fn gateway(
    providers: Result<Gateway, Fault>,
    format: impl respan_gateway_core::Format,
    plugins: Vec<Arc<dyn Plugin>>,
    target: &str,
) -> Gateway {
    let gw = providers
        .and_then(|g| g.serve(format))
        .unwrap_or_else(|e| panic!("{e}"));
    let gw = plugins
        .into_iter()
        .enumerate()
        .try_fold(gw, |gw, (i, p)| gw.plugin(format!("p{i}"), p))
        .unwrap_or_else(|e| panic!("{e}"));
    gw.target(target).unwrap_or_else(|e| panic!("{e}"))
}

/// Status, headers, body text and the request's outcome.
pub async fn send(
    gw: &Gateway,
    req: http::Request<Body>,
) -> (StatusCode, HeaderMap, String, Option<Outcome>) {
    let (resp, ended) = gw.handle(req).await;
    let (head, body) = resp.into_parts();
    let body = body.collect().await.map(|b| b.to_bytes());
    let body = body.unwrap_or_else(|f| panic!("aborted: {f}"));
    let outcome = Some(ended.await);
    // `idle` waits for every request, an open GET stream's too.
    if !OPEN_STREAMS.with(|o| o.get()) {
        gw.idle().await;
    }
    (
        head.status,
        head.headers,
        String::from_utf8_lossy(&body).into_owned(),
        outcome,
    )
}

/// The JSON messages of a JSON body or of every event of a stream.
pub fn messages(body: &str) -> Vec<Value> {
    if let Ok(v) = serde_json::from_str::<Value>(body) {
        return match v {
            Value::Array(all) => all,
            one => vec![one],
        };
    }
    body.split("\n\n")
        .filter_map(|event| {
            let data: Vec<&str> = event
                .lines()
                .filter_map(|l| l.strip_prefix("data:"))
                .map(str::trim)
                .collect();
            serde_json::from_str(&data.join("\n")).ok()
        })
        .collect()
}

/// A 200 event stream of these JSON messages.
pub fn sse(messages: &[Value]) -> mock::Reply {
    let chunks: Vec<String> = messages.iter().map(|m| format!("data: {m}\n\n")).collect();
    let chunks: Vec<&str> = chunks.iter().map(String::as_str).collect();
    mock::sse(&chunks)
}

/// A reply of `status` with no body and no content type.
pub fn bare(status: StatusCode) -> mock::Reply {
    let mut reply = http::Response::new(Vec::new());
    *reply.status_mut() = status;
    reply
}

/// The chunks of a fed body.
type Feed = mpsc::UnboundedReceiver<Result<Bytes, Fault>>;

/// A transport whose one response's body the test feeds, chunk by chunk,
/// and may leave open forever.
#[derive(Clone)]
pub struct Fed {
    head: Arc<Mutex<Option<Feed>>>,
    content_type: &'static str,
    pub sent: Arc<Mutex<Vec<http::request::Parts>>>,
}

impl std::fmt::Debug for Fed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Fed").finish_non_exhaustive()
    }
}

impl Fed {
    pub fn new(content_type: &'static str) -> (Self, mpsc::UnboundedSender<Result<Bytes, Fault>>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let fed = Self {
            head: Arc::new(Mutex::new(Some(rx))),
            content_type,
            sent: Arc::default(),
        };
        (fed, tx)
    }
}

#[async_trait]
impl Transport for Fed {
    async fn send(&self, req: http::Request<Body>) -> Result<http::Response<Body>, Fault> {
        let (head, _) = req.into_parts();
        self.sent
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(head);
        let rx = self
            .head
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        let rx = rx.ok_or_else(|| Fault::upstream("fed once"))?;
        let chunks = stream::unfold(rx, |mut rx| async move {
            let chunk = rx.recv().await?;
            Some((chunk.map(Frame::data), rx))
        });
        let mut resp = http::Response::new(StreamBody::new(chunks).boxed_unsync());
        let ct = http::HeaderValue::from_static(self.content_type);
        resp.headers_mut().insert(CONTENT_TYPE, ct);
        Ok(resp)
    }
}

thread_local! {
    /// Set by a test that keeps a stream open while it sends.
    pub static OPEN_STREAMS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// How long a test waits for something that should come at once.
pub const PROMPTLY: Duration = Duration::from_secs(2);
