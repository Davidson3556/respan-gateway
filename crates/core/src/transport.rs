use async_trait::async_trait;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};

use crate::Fault;

/// A message body: a boxed [`http_body::Body`] of bytes whose error is the
/// [`Fault`] that broke it. Build one with `http_body_util`, or pass any
/// body whose error converts into a `Fault`.
pub type Body = http_body_util::combinators::UnsyncBoxBody<Bytes, Fault>;

/// A body of `bytes`.
pub(crate) fn full(bytes: Bytes) -> Body {
    Full::new(bytes).map_err(Fault::from).boxed_unsync()
}

/// How the kernel reaches providers: the same `http` request and response
/// the gateway takes from its clients.
#[async_trait]
pub trait Transport: Send + Sync + 'static {
    /// Send `req` and return the provider's response, whatever its status.
    /// `Err` only if the request could not be sent, such as to an
    /// unreachable provider. It may return once the response head arrives,
    /// with both bodies still streaming. Redirects are returned, not
    /// followed: a relayed one reaches the client.
    async fn send(&self, req: http::Request<Body>) -> Result<http::Response<Body>, Fault>;

    /// Whether requests to `base_url` can be sent, such as by scheme and
    /// port, beyond the kernel's own check. Asked when the gateway is built,
    /// so an unreachable URL fails the build. Any, by default.
    fn reaches(&self, base_url: &str) -> bool {
        let _ = base_url;
        true
    }
}

/// A shared transport; it forwards every method.
#[async_trait]
impl<T: Transport + ?Sized> Transport for std::sync::Arc<T> {
    async fn send(&self, req: http::Request<Body>) -> Result<http::Response<Body>, Fault> {
        (**self).send(req).await
    }

    fn reaches(&self, base_url: &str) -> bool {
        (**self).reaches(base_url)
    }
}
