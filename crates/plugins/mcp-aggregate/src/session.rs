//! The aggregate's state: its sessions with clients, and each of theirs
//! with a server, and what ends them when a reload retires the build.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use respan_gateway_core::futures::stream;
use respan_gateway_core::{
    ClientContext, Extensions, Fault, Gateway, Plugin, Response, async_trait,
};
use respan_gateway_http::Body;
use respan_gateway_http::http::{self, Method};
use respan_gateway_mcp::Output;
use serde_json::Value;

use super::{ROOT, Run};
use respan_gateway_plugin_tool_loop::ToolFilter;

/// How long ending the sessions may take.
const CLOSING: Duration = Duration::from_secs(10);

pub(super) struct Inner {
    /// The servers, in the order lists show them.
    pub(super) servers: Vec<String>,
    pub(super) filter: ToolFilter,
    /// The client sessions, each kept until idle for [`IDLE`](super::IDLE),
    /// at most [`SESSIONS`](super::SESSIONS), the least recently used
    /// dropped first.
    pub(super) sessions: moka::sync::Cache<String, Arc<Session>>,
    /// The next JSON-RPC id of the gateway's own requests.
    pub(super) next_id: AtomicU64,
}

/// One client's session.
pub(super) struct Session {
    /// The session with each server: none until opened, and again once the
    /// server lost it.
    pub(super) upstream: HashMap<String, tokio::sync::Mutex<Option<Opened>>>,
}

/// A session with a server.
#[derive(Debug, Clone)]
pub(super) struct Opened {
    pub(super) id: Option<String>,
    pub(super) protocol: String,
    pub(super) capabilities: Value,
}

impl Session {
    /// The session with `server`, if it is open.
    pub(super) async fn opened(&self, server: &str) -> Option<Opened> {
        self.upstream.get(server)?.lock().await.clone()
    }

    /// Drops the session with `server` if it is still `opened`.
    pub(super) async fn forget(&self, server: &str, opened: &Opened) {
        if let Some(slot) = self.upstream.get(server) {
            let mut slot = slot.lock().await;
            if slot.as_ref().map(|o| &o.id) == Some(&opened.id) {
                *slot = None;
            }
        }
    }
}

/// Marks the request that ends a retired aggregate's sessions.
#[derive(Debug, Clone, Copy)]
struct Close;

/// Ends the sessions the aggregate kept, with clients and with servers.
#[derive(Clone)]
pub struct Closer(pub(super) Arc<Inner>);

impl std::fmt::Debug for Closer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Closer").finish_non_exhaustive()
    }
}

impl Closer {
    /// The plugin that does it for the request [`close`](Self::close) makes.
    /// Register it first, so nothing else sees that request.
    pub fn plugin(&self) -> impl Plugin + use<> {
        Closing(self.0.clone())
    }

    /// Ends every session, through `gateway`: the one the aggregate was built
    /// into, once its requests are done. A session that does not end is the
    /// server's to time out.
    pub async fn close(&self, gateway: &Gateway) {
        let mut request = http::Request::new(Body::full(Vec::<u8>::new()));
        *request.method_mut() = Method::DELETE;
        *request.uri_mut() = http::Uri::from_static(ROOT);
        let mut marks = Extensions::new();
        marks.insert(Close);
        request.extensions_mut().insert(marks);
        let sent = respan_gateway_http::handle(gateway, request, 1 << 20);
        let work = async {
            let (response, done) = sent.await;
            let body = respan_gateway_http::http_body_util::BodyExt::collect(response.into_body());
            let _ = tokio::join!(body, done);
        };
        let _ = tokio::time::timeout(CLOSING, work).await;
    }
}

struct Closing(Arc<Inner>);

#[async_trait]
impl Plugin for Closing {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        if cx.extensions().get::<Close>().is_none() {
            return Ok(None);
        }
        let calls = self.0.servers.iter().map(|s| (s.clone(), cx.call(s)));
        let run = Run {
            inner: self.0.clone(),
            calls: Arc::new(calls.collect()),
            scope: None,
        };
        run.end_all().await;
        Ok(Some(Response::stream(
            stream::empty::<Result<Output, Fault>>(),
        )))
    }
}
