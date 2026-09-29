use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;

use crate::Fault;

/// A body of a request, call or response: its bytes, one item per data
/// frame, ending with an `Err` if it broke off.
pub type Body = BoxStream<'static, Result<Bytes, Fault>>;

/// One direction of an exchange on a wire: a head, which the wire's formats
/// and transports share and the kernel never reads, a body, and
/// [`Extensions`]. What [`Gateway::handle`](crate::Gateway::handle) takes
/// and gives, and a [`Transport`] sends and gets back.
#[non_exhaustive]
pub struct Exchange<H> {
    /// The wire's head, such as an HTTP request's method, URI and headers.
    pub head: H,
    /// The bytes after the head.
    pub body: Body,
    /// Values for plugins and the transport, such as a credential the
    /// adapter lifted from the head; none on a response.
    pub extensions: Extensions,
}

impl<H> Exchange<H> {
    /// An exchange of `head` and `body`, with no extensions.
    pub fn new(head: H, body: Body) -> Self {
        Self {
            head,
            body,
            extensions: Extensions::new(),
        }
    }

    /// The same exchange with `extensions`.
    pub fn with_extensions(mut self, extensions: Extensions) -> Self {
        self.extensions = extensions;
        self
    }
}

/// A head of any wire, erased.
pub(crate) type Head = Box<dyn Any + Send + Sync>;

impl<H: Any + Send + Sync> Exchange<H> {
    /// The same, its head erased.
    pub(crate) fn erase(self) -> Exchange<Head> {
        Exchange {
            head: Box::new(self.head),
            body: self.body,
            extensions: self.extensions,
        }
    }
}

impl Exchange<Head> {
    /// The same with its head as `H`; `Err` with `what` for another type.
    pub(crate) fn downcast<H: Any>(self, what: &'static str) -> Result<Exchange<H>, Fault> {
        Ok(Exchange {
            head: *self.head.downcast().map_err(|_| Fault::internal(what))?,
            body: self.body,
            extensions: self.extensions,
        })
    }
}

impl<H: std::fmt::Debug> std::fmt::Debug for Exchange<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Exchange")
            .field("head", &self.head)
            .field("extensions", &self.extensions)
            .finish_non_exhaustive()
    }
}

/// A value an [`Extensions`] can hold.
trait Value: Any + Send + Sync {
    fn clone_box(&self) -> Box<dyn Value>;
}

impl<T: Clone + Send + Sync + 'static> Value for T {
    fn clone_box(&self) -> Box<dyn Value> {
        Box::new(self.clone())
    }
}

/// Values of any `Clone` type, at most one per type, that the adapter,
/// plugins and transports hand each other with a request or call. The
/// kernel reads none but [`Relayed`](crate::Relayed).
#[derive(Default)]
pub struct Extensions(HashMap<TypeId, Box<dyn Value>>);

impl Clone for Extensions {
    fn clone(&self) -> Self {
        Self(
            self.0
                .iter()
                .map(|(k, v)| (*k, Value::clone_box(&**v)))
                .collect(),
        )
    }
}

impl std::fmt::Debug for Extensions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Extensions")
            .field("len", &self.0.len())
            .finish()
    }
}

/// `value`, of type `T`, unboxed.
fn unbox<T: 'static>(value: Box<dyn Value>) -> Option<T> {
    (value as Box<dyn Any>).downcast().ok().map(|v| *v)
}

impl Extensions {
    /// None.
    pub fn new() -> Self {
        Self::default()
    }

    /// Put `value` in, returning the one of its type it replaces.
    pub fn insert<T: Clone + Send + Sync + 'static>(&mut self, value: T) -> Option<T> {
        unbox(self.0.insert(TypeId::of::<T>(), Box::new(value))?)
    }

    /// The value of type `T`, if any.
    pub fn get<T: 'static>(&self) -> Option<&T> {
        (&**self.0.get(&TypeId::of::<T>())? as &dyn Any).downcast_ref()
    }

    /// The value of type `T`, if any, to change.
    pub fn get_mut<T: 'static>(&mut self) -> Option<&mut T> {
        (&mut **self.0.get_mut(&TypeId::of::<T>())? as &mut dyn Any).downcast_mut()
    }

    /// Take out the value of type `T`, if any.
    pub fn remove<T: 'static>(&mut self) -> Option<T> {
        unbox(self.0.remove(&TypeId::of::<T>())?)
    }

    /// Put in every value of `other`, replacing those of its types.
    pub fn extend(&mut self, other: Extensions) {
        self.0.extend(other.0);
    }

    /// Whether it holds none.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// How many it holds.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Take out every value.
    pub fn clear(&mut self) {
        self.0.clear();
    }
}

/// How the gateway reaches one provider
/// ([`Gateway::provider`](crate::Gateway::provider)): a URL and keys, a
/// process, a socket or in-process code, with any state its calls share. Its
/// heads are its format's.
#[async_trait]
pub trait Transport: Send + Sync + 'static {
    /// The head of a call.
    type Ask: Send + Sync + 'static;

    /// The head of a provider's response.
    type Answer: Send + Sync + 'static;

    /// Send `call` and return the provider's response, whatever it says,
    /// once its head arrives; return redirects rather than follow them.
    /// `call` is as its format
    /// ([`Format::request_encoder`](crate::Format::request_encoder)) and
    /// [`ProviderContext::edit_call`](crate::ProviderContext::edit_call) left
    /// it, its address relative to this transport. A relay's starts from the
    /// client's head, so strip what the protocol forbids a proxy to pass
    /// on. Its body carries the inputs, one per data frame: a whole call's
    /// at once, its size exact; a streaming request's as they arrive. The
    /// transport decides whose keys reach the provider. Its extensions are
    /// the call's, with [`Relayed`](crate::Relayed) on a relay alone.
    ///
    /// `Err` if the call could not be sent: [`Fault::upstream`] if the
    /// provider is unreachable, [`Fault::config`] if this transport cannot
    /// express it, or a failure of its own. A body left unread gives a
    /// streaming request's inputs back, so a retry gets them.
    async fn send(&self, call: Exchange<Self::Ask>) -> Result<Exchange<Self::Answer>, Fault>;
}

/// A shared transport; it forwards every method.
#[async_trait]
impl<T: Transport + ?Sized> Transport for Arc<T> {
    type Ask = T::Ask;
    type Answer = T::Answer;

    async fn send(&self, call: Exchange<Self::Ask>) -> Result<Exchange<Self::Answer>, Fault> {
        (**self).send(call).await
    }
}

/// A transport with its heads erased, as the gateway keeps it.
pub(crate) type AnyTransport = dyn Transport<Ask = Head, Answer = Head>;

/// A typed transport, erased.
pub(crate) struct Erased<T>(pub(crate) T);

#[async_trait]
impl<T: Transport> Transport for Erased<T> {
    type Ask = Head;
    type Answer = Head;

    async fn send(&self, call: Exchange<Head>) -> Result<Exchange<Head>, Fault> {
        let call = call.downcast("a call of another wire than its transport's")?;
        Ok(self.0.send(call).await?.erase())
    }
}
