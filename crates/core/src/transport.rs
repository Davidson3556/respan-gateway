use async_trait::async_trait;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};

use crate::Fault;

/// A message body: a boxed [`http_body::Body`] of bytes whose error is a
/// [`Fault`]. Make one with [`body`].
pub type Body = http_body_util::combinators::UnsyncBoxBody<Bytes, Fault>;

/// A whole [`Body`] of `bytes`.
pub fn body(bytes: impl Into<Bytes>) -> Body {
    Full::new(bytes.into()).map_err(Fault::from).boxed_unsync()
}

/// How the kernel reaches one [`Provider`]: a URL and credentials, a
/// process, a socket, or in-process code. It may hold state its calls
/// share, such as a connection pool.
///
/// [`Provider`]: crate::Provider
#[async_trait]
pub trait Transport: Send + Sync + 'static {
    /// Send `req` and return the provider's response, whatever its status.
    /// `req` is as [`Format::prepare`](crate::Format::prepare) made it, its
    /// URI the path and query relative to this transport, without
    /// connection headers, `Host` or `Expect`. It may carry the client's
    /// credentials: the transport decides whose keys reach the provider.
    /// Its extensions are the call's
    /// ([`ProviderContext::extensions`](crate::ProviderContext::extensions)),
    /// overlaid by any `prepare` set. It may return once the head arrives,
    /// both bodies still streaming. Return redirects rather than follow
    /// them.
    ///
    /// `Err` only if the request could not be sent: [`Fault::upstream`] if
    /// the provider is unreachable, [`Fault::config`] if this transport
    /// cannot express the request.
    async fn send(&self, req: http::Request<Body>) -> Result<http::Response<Body>, Fault>;
}

/// A shared transport; it forwards every method.
#[async_trait]
impl<T: Transport + ?Sized> Transport for std::sync::Arc<T> {
    async fn send(&self, req: http::Request<Body>) -> Result<http::Response<Body>, Fault> {
        (**self).send(req).await
    }
}
