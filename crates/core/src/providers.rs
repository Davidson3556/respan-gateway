use std::sync::Arc;

use http::header::{CONTENT_TYPE, HeaderName, HeaderValue};
use http::{Method, StatusCode};

use crate::edits::{Edit, apply_once};
use crate::gateway::{Inner, Route};
use crate::request::{ProviderCloseFn, ProviderRequestParts};
use crate::{
    Body, ClientRequest, Fault, Format, Part, ProviderEnd, ProviderOutcome, ProviderRequest,
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

fn close_all(fns: Vec<ProviderCloseFn>, outcome: &ProviderOutcome) {
    for f in fns {
        f(outcome);
    }
}

/// One provider request for `client`, running the `on_provider_request` hooks
/// of chain entries `0..=upto`.
pub(crate) async fn call(
    inner: &Inner,
    route: &Route,
    client: &ClientRequest,
    upto: usize,
    index: usize,
    provider: &str,
) -> Result<ProviderResponse, Fault> {
    {
        let target = inner
            .targets
            .get(provider)
            .ok_or_else(|| Fault::config(format!("unknown provider '{provider}'")))?;
        let format = inner.format(&target.format)?;
        if format.name() != route.format.name() {
            return Err(Fault::config(format!(
                "provider '{provider}' speaks {}, route speaks {}: translation is not supported yet",
                format.name(),
                route.format.name()
            )));
        }

        let pr = ProviderRequest::new(client, target, index);
        let mut hook_result = Ok(());
        for plugin in route.chain.iter().take(upto + 1) {
            hook_result = plugin.on_provider_request(&pr).await;
            if hook_result.is_err() {
                break;
            }
        }
        let ProviderRequestParts {
            body,
            to_provider,
            from_provider,
            on_close,
        } = pr.into_parts();
        let fail = |on_close: Vec<ProviderCloseFn>, f: Fault| {
            close_all(
                on_close,
                &ProviderOutcome {
                    provider: target.name.clone(),
                    status: None,
                    end: ProviderEnd::Failed,
                    usage: Usage::default(),
                },
            );
            f
        };
        if let Err(f) = hook_result {
            return Err(fail(on_close, f));
        }

        let prepared = (|| {
            let mut part = Part::Doc(body);
            apply_once(&mut part, to_provider.take())?;
            let Part::Doc(mut body) = part else {
                return Err(Fault::internal("request edit replaced the body"));
            };
            let mut envelope = client.envelope().clone();
            if target.model.is_some() {
                envelope.model = target.model.clone();
            }
            format.prepare_request(&mut body, target.model.as_deref(), envelope.stream)?;
            let url = format!(
                "{}{}",
                target.base_url.trim_end_matches('/'),
                format.provider_path(&envelope)
            );
            let mut req = http::Request::builder()
                .method(Method::POST)
                .uri(url)
                .header(CONTENT_TYPE, "application/json");
            for (k, v) in &target.headers {
                req = req.header(k, v);
            }
            req.body(body.to_bytes())
                .map_err(|e| Fault::config(e.to_string()))
        })();
        let req = match prepared {
            Ok(r) => r,
            Err(f) => return Err(fail(on_close, f)),
        };

        let resp = match inner.transport.send(req).await {
            Ok(r) => r,
            Err(f) => return Err(fail(on_close, f)),
        };
        let (parts, body) = resp.into_parts();
        Ok(ProviderResponse {
            provider: target.name.clone(),
            format,
            head: ResponseHead {
                status: parts.status,
                headers: parts.headers,
            },
            body: Some(body),
            from_provider: from_provider.take(),
            on_close,
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
    on_close: Vec<ProviderCloseFn>,
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

    pub(crate) fn commit(mut self) -> Committed {
        let body = self
            .body
            .take()
            .unwrap_or_else(|| Box::pin(futures::stream::empty()));
        Committed {
            provider: std::mem::take(&mut self.provider),
            format: self.format.clone(),
            head: self.head.clone(),
            body,
            from_provider: std::mem::take(&mut self.from_provider),
            on_close: std::mem::take(&mut self.on_close),
        }
    }
}

impl Drop for ProviderResponse {
    fn drop(&mut self) {
        if self.body.is_none() {
            return;
        }
        let end = if self.head.status.is_success() {
            ProviderEnd::Cancelled
        } else {
            ProviderEnd::Failed
        };
        close_all(
            std::mem::take(&mut self.on_close),
            &ProviderOutcome {
                provider: self.provider.clone(),
                status: Some(self.head.status.as_u16()),
                end,
                usage: Usage::default(),
            },
        );
    }
}

pub(crate) struct Committed {
    pub provider: String,
    pub format: Arc<dyn Format>,
    pub head: ResponseHead,
    pub body: Body,
    pub from_provider: Vec<Edit>,
    pub on_close: Vec<ProviderCloseFn>,
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
