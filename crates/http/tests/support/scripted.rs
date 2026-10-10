//! Ordered wire responses for HTTP header, framing, and fault tests. Unlike a
//! provider fake, these deliberately return malformed or truncated bodies.
//! Replies are consumed in send order, not looked up by URL.
#![allow(dead_code)]

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, PoisonError};

use respan_gateway_core::Fault;
use respan_gateway_core::Upstream;
use respan_gateway_core::futures::stream;
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::http::header::CONTENT_TYPE;
use respan_gateway_http::http::{self, HeaderMap, HeaderValue, StatusCode};
use respan_gateway_http::http_body_util::BodyExt;
use respan_gateway_http::{Body, Completion, credentials, strip_origin_headers};

pub type Reply = http::Response<Vec<Result<Bytes, Fault>>>;
type WireResult = Result<(http::Response<Body>, Option<Completion>), Fault>;

pub fn sse(chunks: &[&str]) -> Reply {
    let chunks = chunks
        .iter()
        .map(|s| Ok(Bytes::copy_from_slice(s.as_bytes())))
        .collect();
    typed(StatusCode::OK, "text/event-stream", chunks)
}

pub fn json(status: StatusCode, body: &str) -> Reply {
    typed(
        status,
        "application/json",
        vec![Ok(Bytes::copy_from_slice(body.as_bytes()))],
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
        .insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    reply
}

#[derive(Debug, Clone, Default)]
pub struct Scripted {
    queued: Arc<Mutex<VecDeque<(String, Reply)>>>,
    sent: Arc<Mutex<Vec<http::Request<Bytes>>>>,
}

impl Scripted {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn reply(&self, base: &str, response: Reply) -> &Self {
        self.queued
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push_back((base.trim_end_matches('/').to_owned(), response));
        self
    }

    pub fn at(&self, base: &str) -> ScriptedAt {
        ScriptedAt {
            fixture: self.clone(),
            base: base.trim_end_matches('/').to_owned(),
            headers: HeaderMap::new(),
        }
    }

    pub fn sent(&self) -> Vec<http::Request<Bytes>> {
        self.sent
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

#[derive(Debug, Clone)]
pub struct ScriptedAt {
    fixture: Scripted,
    base: String,
    headers: HeaderMap,
}

impl ScriptedAt {
    pub fn with_headers(mut self, headers: HeaderMap) -> Self {
        self.headers = headers;
        self
    }

    pub async fn send(
        &self,
        request: http::Request<Body>,
    ) -> Result<(http::Response<Body>, Option<Completion>), Fault> {
        let mut request = request;
        credentials(&mut request, &self.headers);
        let (mut head, body) = request.into_parts();
        let path = head.uri.path_and_query().map_or("/", |p| p.as_str());
        let url = format!("{}{path}", self.base);
        head.uri = url
            .parse()
            .map_err(|e| Fault::config("bad scripted URI").with_source(e))?;
        let body = BodyExt::collect(body).await?.to_bytes();
        head.extensions = http::Extensions::new();
        self.fixture
            .sent
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(http::Request::from_parts(head, body));
        let (expected, reply) = self
            .fixture
            .queued
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop_front()
            .ok_or_else(|| Upstream::fault(format!("no mock response for {url}")))?;
        assert_eq!(
            expected, self.base,
            "provider calls reached the wrong endpoint"
        );
        let (mut head, chunks) = reply.into_parts();
        strip_origin_headers(&mut head.headers);
        Ok((
            http::Response::from_parts(head, Body::stream(stream::iter(chunks))),
            None,
        ))
    }
}

impl ScriptedAt {
    pub fn into_sender(
        self,
    ) -> impl Fn(
        http::Request<Body>,
    ) -> respan_gateway_core::futures::future::BoxFuture<'static, WireResult>
    + Clone {
        use respan_gateway_core::futures::FutureExt;
        move |request| {
            let value = self.clone();
            async move { value.send(request).await }.boxed()
        }
    }
}
