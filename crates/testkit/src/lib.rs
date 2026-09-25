//! Test kit for plugin authors: a scripted provider and response helpers.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, stream};
use http::header::CONTENT_TYPE;
use respan_gateway_core::{Body, Fault, Transport};

/// One scripted provider response.
#[derive(Debug, Clone)]
pub struct MockResponse {
    pub status: u16,
    pub content_type: &'static str,
    pub chunks: Vec<Bytes>,
    /// Fail the body with a transport error after this many chunks.
    pub fail_after: Option<usize>,
}

impl MockResponse {
    pub fn sse(chunks: &[&str]) -> Self {
        Self {
            status: 200,
            content_type: "text/event-stream",
            chunks: chunks.iter().map(|c| Bytes::from(c.to_string())).collect(),
            fail_after: None,
        }
    }

    pub fn json(status: u16, body: &str) -> Self {
        Self {
            status,
            content_type: "application/json",
            chunks: vec![Bytes::from(body.to_owned())],
            fail_after: None,
        }
    }
}

/// A request the gateway sent to a provider.
#[derive(Debug, Clone)]
pub struct Sent {
    pub url: String,
    pub headers: http::HeaderMap,
    pub body: Bytes,
}

/// A [`Transport`] that answers from scripts, keyed by base URL.
#[derive(Debug, Clone, Default)]
pub struct MockTransport {
    scripts: Arc<Mutex<HashMap<String, VecDeque<MockResponse>>>>,
    sent: Arc<Mutex<Vec<Sent>>>,
}

impl MockTransport {
    pub fn new() -> Self {
        Self::default()
    }

    /// Queue a response for requests whose URL starts with `base_url`.
    pub fn reply(&self, base_url: &str, response: MockResponse) -> &Self {
        self.scripts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(base_url.to_owned())
            .or_default()
            .push_back(response);
        self
    }

    pub fn sent(&self) -> Vec<Sent> {
        self.sent
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

#[async_trait]
impl Transport for MockTransport {
    async fn send(&self, req: http::Request<Bytes>) -> Result<http::Response<Body>, Fault> {
        let url = req.uri().to_string();
        self.sent
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Sent {
                url: url.clone(),
                headers: req.headers().clone(),
                body: req.body().clone(),
            });
        let script = {
            let mut scripts = self.scripts.lock().unwrap_or_else(PoisonError::into_inner);
            scripts
                .iter_mut()
                .find(|(base, _)| url.starts_with(base.as_str()))
                .and_then(|(_, q)| q.pop_front())
        };
        let script =
            script.ok_or_else(|| Fault::upstream(format!("no mock response for {url}")))?;
        let fail_after = script.fail_after;
        let items: Vec<Result<Bytes, Fault>> = script
            .chunks
            .into_iter()
            .enumerate()
            .map(|(i, c)| match fail_after {
                Some(n) if i >= n => Err(Fault::upstream("connection reset")),
                _ => Ok(c),
            })
            .collect();
        let body: Body = stream::iter(items).boxed();
        http::Response::builder()
            .status(script.status)
            .header(CONTENT_TYPE, script.content_type)
            .body(body)
            .map_err(|e| Fault::internal(e.to_string()))
    }
}

/// Read a whole response. Returns the status, headers and body bytes.
pub async fn collect(resp: http::Response<Body>) -> (u16, http::HeaderMap, Bytes) {
    let (parts, mut body) = resp.into_parts();
    let mut out = Vec::new();
    while let Some(chunk) = body.next().await {
        match chunk {
            Ok(b) => out.extend_from_slice(&b),
            Err(_) => break,
        }
    }
    (parts.status.as_u16(), parts.headers, Bytes::from(out))
}
