use async_trait::async_trait;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};

use crate::Fault;

/// A message body: a boxed [`http_body::Body`] of bytes whose error is the
/// [`Fault`] that broke it. Make one of bytes with [`body`], or pass any
/// body whose error converts into a `Fault`.
pub type Body = http_body_util::combinators::UnsyncBoxBody<Bytes, Fault>;

/// A whole [`Body`] of `bytes`.
pub fn body(bytes: impl Into<Bytes>) -> Body {
    Full::new(bytes.into()).map_err(Fault::from).boxed_unsync()
}

/// How the kernel reaches one provider, and where: a URL and credentials,
/// a process, a socket, or code in the same process. Each [`Provider`]
/// has its own; it may hold state its calls share, such as a connection
/// pool.
///
/// [`Provider`]: crate::Provider
#[async_trait]
pub trait Transport: Send + Sync + 'static {
    /// Send `req` and return the provider's response, whatever its status.
    /// `req` is as the provider's format prepared it
    /// ([`Format::prepare`](crate::Format::prepare)): its URI is the path
    /// and query, relative to this transport, and the kernel has removed
    /// hop-by-hop headers, those the client's `Connection` names, `Host`
    /// and `Expect`. It may carry the client's
    /// credentials, as a format passes the client's headers on within its
    /// protocol: whose keys reach the provider is the transport's to
    /// decide. Its extensions are the call's, as its `on_provider` hooks
    /// left them
    /// ([`ProviderContext::extensions`](crate::ProviderContext::extensions)),
    /// under any `prepare` set, so it can read a per-call value such as a
    /// tenant's key. It may return once the response head arrives, with both
    /// bodies still streaming. Redirects are returned, not followed: a
    /// relayed one reaches the client.
    ///
    /// `Err` only if the request could not be sent: [`Fault::upstream`] if
    /// the provider could not be reached, [`Fault::config`] if this
    /// transport cannot express the request. The call returns it, and the
    /// target's call refuses the request with it in the client's format.
    async fn send(&self, req: http::Request<Body>) -> Result<http::Response<Body>, Fault>;
}

/// A shared transport; it forwards every method.
#[async_trait]
impl<T: Transport + ?Sized> Transport for std::sync::Arc<T> {
    async fn send(&self, req: http::Request<Body>) -> Result<http::Response<Body>, Fault> {
        (**self).send(req).await
    }
}
