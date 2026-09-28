#![warn(missing_docs)]
//! Test kit for plugin authors: a scripted provider, recorders of outcomes
//! on both edges, and request and response helpers.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, PoisonError};

use bytes::Bytes;
use futures::channel::mpsc::UnboundedSender;
use futures::{StreamExt, stream};
use http::StatusCode;
use http::header::CONTENT_TYPE;
use http_body::Frame;
use http_body_util::{BodyExt, Full, StreamBody};
use respan_gateway_core::{
    Body, ClientContext, Fault, Outcome, Plugin, ProviderContext, Transport, Usage,
};
use respan_gateway_core::{async_trait, bytes, futures, http, http_body, http_body_util};

/// Scripted provider responses: the body arrives chunk by chunk, and an
/// `Err` chunk breaks it off.
pub mod mock {
    use super::*;

    /// A scripted response.
    pub type Reply = http::Response<Vec<Result<Bytes, Fault>>>;

    /// A 200 event stream of these chunks.
    pub fn sse(chunks: &[&str]) -> Reply {
        let chunks = chunks
            .iter()
            .map(|c| Ok(Bytes::from(c.to_string())))
            .collect();
        typed(StatusCode::OK, "text/event-stream", chunks)
    }

    /// A JSON body with this status, in one chunk.
    pub fn json(status: StatusCode, body: &str) -> Reply {
        typed(
            status,
            "application/json",
            vec![Ok(Bytes::from(body.to_owned()))],
        )
    }

    fn typed(
        status: StatusCode,
        content_type: &'static str,
        chunks: Vec<Result<Bytes, Fault>>,
    ) -> Reply {
        let mut reply = http::Response::new(chunks);
        *reply.status_mut() = status;
        reply
            .headers_mut()
            .insert(CONTENT_TYPE, http::HeaderValue::from_static(content_type));
        reply
    }
}

/// Responses queued for one base URL.
type Script = (String, VecDeque<mock::Reply>);

/// Scripted providers, one [`Transport`] per base URL from [`at`](Self::at):
/// each queued reply answers one request, and one with none queued gets a
/// 502 `upstream`. Every request is recorded with its full URL.
#[derive(Debug, Clone, Default)]
pub struct MockTransport {
    scripts: Arc<Mutex<Vec<Script>>>,
    sent: Arc<Mutex<Vec<http::Request<Bytes>>>>,
}

impl MockTransport {
    /// Scripts with nothing queued.
    pub fn new() -> Self {
        Self::default()
    }

    /// The transport of the provider at `base_url`.
    pub fn at(&self, base_url: &str) -> MockAt {
        MockAt {
            mock: self.clone(),
            base: base_url.trim_end_matches('/').to_owned(),
            headers: http::HeaderMap::new(),
        }
    }

    /// Queues a response for requests to the provider at `base_url`.
    pub fn reply(&self, base_url: &str, response: mock::Reply) -> &Self {
        let base_url = base_url.trim_end_matches('/');
        let mut scripts = self.scripts.lock().unwrap_or_else(PoisonError::into_inner);
        match scripts.iter_mut().find(|(base, _)| base == base_url) {
            Some((_, queue)) => queue.push_back(response),
            None => scripts.push((base_url.to_owned(), VecDeque::from([response]))),
        }
        drop(scripts);
        self
    }

    /// Every request sent so far, in order, each with its full URL and the
    /// headers its transport added.
    pub fn sent(&self) -> Vec<http::Request<Bytes>> {
        self.sent
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// One provider's transport, from [`MockTransport::at`]: it handles headers
/// as `HttpTransport` does
/// ([`add_own_headers`](respan_gateway_server::add_own_headers),
/// [`strip_origin_headers`](respan_gateway_server::strip_origin_headers)),
/// and reads the call's body whole before it replies.
#[derive(Debug, Clone)]
pub struct MockAt {
    mock: MockTransport,
    base: String,
    headers: http::HeaderMap,
}

impl MockAt {
    /// Sends `headers` with every request, replacing same-named ones; with
    /// any, clients' credentials are not passed on.
    pub fn with_headers(mut self, headers: http::HeaderMap) -> Self {
        self.headers = headers;
        self
    }
}

#[async_trait]
impl Transport for MockAt {
    async fn send(&self, call: http::Request<Body>) -> Result<http::Response<Body>, Fault> {
        // Keep alike with `HttpTransport::send` in the server crate.
        let (mut head, body) = call.into_parts();
        respan_gateway_server::add_own_headers(&mut head, &self.headers)?;
        let path = head.uri.path_and_query().map_or("/", |p| p.as_str());
        let url = format!("{}{path}", self.base);
        head.uri = url
            .parse()
            .map_err(|e| Fault::config(format!("bad mock URL {url}")).with_source(e))?;
        let body = body.collect().await?.to_bytes();
        let mock = &self.mock;
        let sent = http::Request::from_parts(head, body);
        mock.sent
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(sent);
        let script = {
            let mut scripts = mock.scripts.lock().unwrap_or_else(PoisonError::into_inner);
            scripts
                .iter_mut()
                .find(|(base, _)| *base == self.base)
                .and_then(|(_, q)| q.pop_front())
        };
        let script =
            script.ok_or_else(|| Fault::upstream(format!("no mock response for {url}")))?;
        let (mut head, chunks) = script.into_parts();
        respan_gateway_server::strip_origin_headers(&mut head.headers);
        let body = stream::iter(chunks).map(|c| c.map(Frame::data));
        Ok(http::Response::from_parts(
            head,
            StreamBody::new(body).boxed_unsync(),
        ))
    }
}

/// A `POST` of `body` to `path`, for
/// [`Gateway::handle`](respan_gateway_core::Gateway::handle).
pub fn post(path: &str, body: impl Into<Bytes>) -> http::Request<Body> {
    let body = Full::new(body.into()).map_err(Fault::from).boxed_unsync();
    let mut req = http::Request::new(body);
    *req.method_mut() = http::Method::POST;
    *req.uri_mut() = path.parse().unwrap_or_else(|e| panic!("path {path}: {e}"));
    req
}

/// A `GET` of `path` whose body is fed through the returned sender, one data
/// frame per send, ending when the sender is dropped.
pub fn streamed(path: &str) -> (UnboundedSender<Bytes>, http::Request<Body>) {
    let (send, frames) = futures::channel::mpsc::unbounded::<Bytes>();
    let body = StreamBody::new(frames.map(|f| Ok::<_, Fault>(Frame::data(f))));
    let mut req = http::Request::new(body.boxed_unsync());
    *req.uri_mut() = path.parse().unwrap_or_else(|e| panic!("path {path}: {e}"));
    (send, req)
}

/// Reads a whole response: status, headers, and body or the fault that
/// broke it off.
pub async fn read_whole(
    resp: http::Response<Body>,
) -> (StatusCode, http::HeaderMap, Result<Bytes, Fault>) {
    let (parts, body) = resp.into_parts();
    let body = body.collect().await.map(|b| b.to_bytes());
    (parts.status, parts.headers, body)
}

/// A plugin that records each request's client-edge outcome; register it
/// first to see every request whose plugins ran.
#[derive(Debug, Clone, Default)]
pub struct Outcomes(Arc<Mutex<Vec<Outcome>>>);

impl Outcomes {
    /// Every outcome recorded so far, in the order their end hooks ran.
    pub fn all(&self) -> Vec<Outcome> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// The one outcome recorded; panics if there is not exactly one.
    pub fn only(&self) -> Outcome {
        let mut all = self.all();
        assert_eq!(all.len(), 1, "expected exactly one outcome: {all:?}");
        all.remove(0)
    }
}

#[async_trait]
impl Plugin for Outcomes {
    async fn on_client_end(&self, _cx: &ClientContext, outcome: &Outcome) {
        let mut all = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        all.push(outcome.clone());
    }
}

/// A plugin that records each call's provider-edge outcome, where a call's
/// usage counts; register it first to see every call.
#[derive(Debug, Clone, Default)]
pub struct Calls(Arc<Mutex<Vec<Outcome>>>);

impl Calls {
    /// Every outcome recorded so far, in the order their end hooks ran.
    pub fn all(&self) -> Vec<Outcome> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// The usage of every call, added up.
    pub fn usage(&self) -> Usage {
        let mut usage = Usage::default();
        for outcome in self.all() {
            usage += outcome.usage;
        }
        usage
    }
}

#[async_trait]
impl Plugin for Calls {
    async fn on_provider_end(&self, _px: &ProviderContext, outcome: &Outcome) {
        let mut all = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        all.push(outcome.clone());
    }
}
