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
    /// Whether this exchange relays the client's own message. The kernel
    /// sets it on a call, and only when the call sends the client's inputs
    /// (not an input of its own) to a provider whose format has the client
    /// format's name; a transport passes on what belongs to the client,
    /// such as its credential, only then. No plugin can set it, and the
    /// kernel ignores it on a request. False on a response.
    pub relayed: bool,
    /// Whether later frames follow the head: the body's data frames are
    /// messages as they arrive, and the head goes on at once. On a request
    /// into [`Gateway::handle`](crate::Gateway::handle), the adapter sets it
    /// (a request that switches protocols, say); else the body is read in
    /// full, so the caller bounds it. On a call, the kernel sets it when the
    /// call sends the client's inputs of a streaming request, and no other:
    /// a transport sends the body as it arrives, else whole.
    pub streams: bool,
}

impl<H> Exchange<H> {
    /// An exchange of `head` and `body`, with no extensions, not relayed,
    /// not streaming.
    pub fn new(head: H, body: Body) -> Self {
        Self {
            head,
            body,
            extensions: Extensions::new(),
            relayed: false,
            streams: false,
        }
    }
}

/// A head of any wire, erased.
pub type Head = Box<dyn Any + Send + Sync>;

impl<H: Any + Send + Sync> Exchange<H> {
    /// The same, its head erased: what [`Gateway::handle`](crate::Gateway::handle)
    /// takes. A head already erased stays as it is, not boxed twice.
    pub fn erase(self) -> Exchange<Head> {
        let head: Head = Box::new(self.head);
        let head = match head.downcast::<Head>() {
            Ok(erased) => *erased,
            Err(head) => head,
        };
        Exchange {
            head,
            body: self.body,
            extensions: self.extensions,
            relayed: self.relayed,
            streams: self.streams,
        }
    }
}

impl Exchange<Head> {
    /// The same with its head as `H`, as an adapter types its wire's response;
    /// `Err` [`INTERNAL`](crate::codes::INTERNAL) with `what` for another type.
    pub fn downcast<H: Any>(self, what: &'static str) -> Result<Exchange<H>, Fault> {
        Ok(Exchange {
            head: *self.head.downcast().map_err(|_| Fault::internal(what))?,
            body: self.body,
            extensions: self.extensions,
            relayed: self.relayed,
            streams: self.streams,
        })
    }
}

impl<H: std::fmt::Debug> std::fmt::Debug for Exchange<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Exchange")
            .field("head", &self.head)
            .field("extensions", &self.extensions)
            .field("relayed", &self.relayed)
            .field("streams", &self.streams)
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
/// kernel reads none.
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
    /// at once, its size exact; a call that `streams` has them as they
    /// arrive, so send its body as it comes; a body that breaks off or is cut
    /// ends with an `Err`, never as a complete one, so abort the message
    /// then. The transport decides whose
    /// keys reach the provider. Its extensions are the call's; the kernel
    /// sets `call.relayed` on a relay alone.
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

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::*;

    #[test]
    fn erasing_twice_keeps_one_box() {
        let once = Exchange::new(7u32, futures::stream::empty().boxed()).erase();
        let twice = once.erase();
        let head = twice.downcast::<u32>("not a u32");
        assert_eq!(head.map(|e| e.head).ok(), Some(7));
    }
}
