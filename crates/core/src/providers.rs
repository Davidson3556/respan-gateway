use std::sync::{Arc, Mutex};

use http::header::{CONTENT_TYPE, HeaderName, HeaderValue};
use http::{Method, StatusCode};

use crate::edits::{Edit, apply_once, lock};
use crate::gateway::{Inner, Route};
use crate::{
    AttemptEnd, AttemptRecord, Body, ClientRequest, Fault, Format, Part, ProviderRequest,
    ResponseHead, Usage,
};

/// A configured provider.
#[derive(Debug, Clone)]
pub struct Target {
    pub name: String,
    pub format: String,
    pub base_url: String,
    /// Overrides the model the client asked for.
    pub model: Option<String>,
    pub headers: Vec<(HeaderName, HeaderValue)>,
}

pub(crate) type Attempts = Arc<Mutex<Vec<AttemptRecord>>>;

/// The capability to make provider requests. Only a route's source gets it.
pub struct Providers<'a> {
    pub(crate) inner: &'a Inner,
    pub(crate) route: &'a Route,
    pub(crate) client: &'a ClientRequest,
    pub(crate) attempts: Attempts,
}

impl std::fmt::Debug for Providers<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Providers")
            .field("route", &self.route.name)
            .finish()
    }
}

impl Providers<'_> {
    /// Send the client request to one provider. A provider error status is
    /// an `Ok` response; check [`ProviderResponse::status`].
    pub async fn call(&self, provider: &str) -> Result<ProviderResponse, Fault> {
        let target = self
            .inner
            .targets
            .get(provider)
            .ok_or_else(|| Fault::config(format!("unknown provider '{provider}'")))?;
        let format = self.inner.format(&target.format)?;
        if format.name() != self.route.format.name() {
            return Err(Fault::config(format!(
                "provider '{provider}' speaks {}, route speaks {}: translation is not supported yet",
                format.name(),
                self.route.format.name()
            )));
        }

        let index = {
            let mut a = lock(&self.attempts);
            a.push(AttemptRecord {
                provider: target.name.clone(),
                status: None,
                end: AttemptEnd::Pending,
                usage: Usage::default(),
            });
            a.len() - 1
        };
        let fail = |f: Fault| {
            lock(&self.attempts)[index].end = AttemptEnd::Failed;
            f
        };

        let pr = ProviderRequest::new(self.client, target, index);
        for plugin in &self.route.chain {
            plugin.on_provider_request(&pr).await.map_err(fail)?;
        }
        let (body, to_provider, from_provider) = pr.into_parts();
        let mut part = Part::Doc(body);
        apply_once(&mut part, to_provider.take()).map_err(fail)?;
        let Part::Doc(mut body) = part else {
            return Err(fail(Fault::internal("request edit replaced the body")));
        };

        let stream = self.client.is_stream();
        format
            .prepare_request(&mut body, target.model.as_deref(), stream)
            .map_err(fail)?;
        let model = target.model.as_deref().or(self.client.model());
        let url = format!(
            "{}{}",
            target.base_url.trim_end_matches('/'),
            format.provider_path(self.client.head(), model, stream)
        );
        let mut req = http::Request::builder()
            .method(Method::POST)
            .uri(url)
            .header(CONTENT_TYPE, "application/json");
        for (k, v) in &target.headers {
            req = req.header(k, v);
        }
        let req = req
            .body(body.to_bytes())
            .map_err(|e| fail(Fault::config(e.to_string())))?;

        let resp = self.inner.transport.send(req).await.map_err(fail)?;
        let (parts, body) = resp.into_parts();
        lock(&self.attempts)[index].status = Some(parts.status.as_u16());
        Ok(ProviderResponse {
            provider: target.name.clone(),
            format,
            head: ResponseHead {
                status: parts.status,
                headers: parts.headers,
            },
            body: Some(body),
            from_provider: from_provider.take(),
            index,
            attempts: self.attempts.clone(),
        })
    }
}

/// A provider's response, not yet sent anywhere. Dropping it cancels it;
/// converting it into a [`ClientResponse`] commits it.
pub struct ProviderResponse {
    provider: String,
    format: Arc<dyn Format>,
    head: ResponseHead,
    body: Option<Body>,
    from_provider: Vec<Edit>,
    index: usize,
    attempts: Attempts,
}

impl std::fmt::Debug for ProviderResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProviderResponse")
            .field("provider", &self.provider)
            .field("status", &self.head.status)
            .finish()
    }
}

impl ProviderResponse {
    pub fn provider(&self) -> &str {
        &self.provider
    }

    pub fn status(&self) -> StatusCode {
        self.head.status
    }

    pub fn head(&self) -> &ResponseHead {
        &self.head
    }
}

impl Drop for ProviderResponse {
    fn drop(&mut self) {
        if self.body.is_some() {
            let failed = !self.head.status.is_success();
            lock(&self.attempts)[self.index].end = if failed {
                AttemptEnd::Failed
            } else {
                AttemptEnd::Cancelled
            };
        }
    }
}

pub(crate) struct Committed {
    pub format: Arc<dyn Format>,
    pub head: ResponseHead,
    pub body: Body,
    pub from_provider: Vec<Edit>,
    pub index: usize,
}

/// What the client gets back. Built from a [`ProviderResponse`] (which commits
/// it) or from Parts the plugin made itself.
#[derive(Debug)]
pub struct ClientResponse(pub(crate) Reply);

#[derive(Debug)]
pub(crate) enum Reply {
    Provider(ProviderResponse),
    Synthetic {
        head: ResponseHead,
        parts: Vec<Part>,
    },
}

impl ClientResponse {
    /// A response the plugin made itself: a cache hit, a mock, a merge.
    pub fn synthetic(head: ResponseHead, parts: Vec<Part>) -> Self {
        Self(Reply::Synthetic { head, parts })
    }
}

impl From<ProviderResponse> for ClientResponse {
    fn from(r: ProviderResponse) -> Self {
        Self(Reply::Provider(r))
    }
}

impl ProviderResponse {
    pub(crate) fn commit(mut self) -> Committed {
        let body = self
            .body
            .take()
            .unwrap_or_else(|| Box::pin(futures::stream::empty()));
        Committed {
            format: self.format.clone(),
            head: self.head.clone(),
            body,
            from_provider: std::mem::take(&mut self.from_provider),
            index: self.index,
        }
    }
}
