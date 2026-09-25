use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;

use crate::Fault;

pub type Body = BoxStream<'static, Result<Bytes, Fault>>;

/// How the kernel reaches providers. Swappable so tests and embedders can
/// supply their own.
#[async_trait]
pub trait Transport: Send + Sync + 'static {
    async fn send(&self, req: http::Request<Bytes>) -> Result<http::Response<Body>, Fault>;
}

#[cfg(feature = "reqwest")]
pub use http_transport::HttpTransport;

#[cfg(feature = "reqwest")]
mod http_transport {
    use futures::{StreamExt, TryStreamExt};

    use super::*;

    #[derive(Debug, Clone, Default)]
    pub struct HttpTransport {
        client: reqwest::Client,
    }

    #[async_trait]
    impl Transport for HttpTransport {
        async fn send(&self, req: http::Request<Bytes>) -> Result<http::Response<Body>, Fault> {
            let (parts, body) = req.into_parts();
            let resp = self
                .client
                .request(parts.method, parts.uri.to_string())
                .headers(parts.headers)
                .body(body)
                .send()
                .await
                .map_err(|e| Fault::upstream(e.to_string()))?;
            let mut builder = http::Response::builder().status(resp.status());
            if let Some(h) = builder.headers_mut() {
                *h = resp.headers().clone();
            }
            let stream = resp
                .bytes_stream()
                .map_err(|e| Fault::upstream(e.to_string()))
                .boxed();
            builder
                .body(stream)
                .map_err(|e| Fault::internal(e.to_string()))
        }
    }
}
