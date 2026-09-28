use async_trait::async_trait;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};

use crate::Fault;

/// A body of a response or a call: a boxed [`http_body::Body`] of bytes
/// whose error is a [`Fault`].
pub type Body = http_body_util::combinators::UnsyncBoxBody<Bytes, Fault>;

/// A [`Body`] of `bytes`, in one frame.
pub(crate) fn one_frame(bytes: impl Into<Bytes>) -> Body {
    Full::new(bytes.into()).map_err(Fault::from).boxed_unsync()
}

/// How the gateway reaches one provider
/// ([`Gateway::provider`](crate::Gateway::provider)): a URL and
/// credentials, a process, a socket, or in-process code, with any state its
/// calls share, such as a connection pool.
#[async_trait]
pub trait Transport: Send + Sync + 'static {
    /// Send `call` and return the provider's response, whatever its status,
    /// once its head arrives; return redirects rather than follow them.
    /// `call` is as its format
    /// ([`Format::request_encoder`](crate::Format::request_encoder)) and
    /// [`ProviderContext::edit_call`](crate::ProviderContext::edit_call) left
    /// it: its URI relative to this transport, without the client's
    /// connection headers, `Host` or `Expect`, with `Upgrade` and
    /// `Connection: upgrade` if it switches protocols. Its body carries the
    /// inputs, one per data frame: a whole call's at once, its size exact;
    /// an upgrading request's as they arrive, a WebSocket transport picking
    /// each message's type from the subprotocol. It may carry the client's
    /// credentials: the transport decides whose keys reach the provider. Its
    /// extensions are the call's
    /// ([`ProviderContext::extensions`](crate::ProviderContext::extensions)).
    ///
    /// `Err` if the call could not be sent: [`Fault::upstream`] if the
    /// provider is unreachable, [`Fault::config`] if this transport cannot
    /// express it. A body left unread gives an upgrading request's inputs
    /// back, so a retry gets them.
    async fn send(&self, call: http::Request<Body>) -> Result<http::Response<Body>, Fault>;
}

/// A shared transport; it forwards every method.
#[async_trait]
impl<T: Transport + ?Sized> Transport for std::sync::Arc<T> {
    async fn send(&self, call: http::Request<Body>) -> Result<http::Response<Body>, Fault> {
        (**self).send(call).await
    }
}
