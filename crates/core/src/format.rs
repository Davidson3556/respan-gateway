use std::any::{Any, TypeId};
use std::marker::PhantomData;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};

use crate::{Doc, Fault, Part};

/// The client's request as formats see it: as it arrived, in the client's
/// format (which may not be the viewing format), with the model plugins
/// left it asking for.
#[derive(Debug, Clone, Copy)]
pub struct RequestView<'a> {
    own: bool,
    head: &'a http::request::Parts,
    body: &'a Doc,
    model: Option<&'a str>,
}

impl<'a> RequestView<'a> {
    pub(crate) fn new(
        own: bool,
        head: &'a http::request::Parts,
        body: &'a Doc,
        model: Option<&'a str>,
    ) -> Self {
        Self {
            own,
            head,
            body,
            model,
        }
    }

    /// The client's request line and headers, in its format, even for a call
    /// with a request of its own. Values attached by type are in its
    /// `extensions`, which any format may read.
    pub fn head(&self) -> &'a http::request::Parts {
        self.head
    }

    /// Whether the viewing format is the client's, whose headers are then
    /// its own.
    pub fn is_own(&self) -> bool {
        self.own
    }

    /// The client's body, if the viewing format is the client's
    /// ([`is_own`](Self::is_own)), even for a call with a request of its own,
    /// whose body `prepare` is given instead.
    pub fn body(&self) -> Option<&'a Doc> {
        self.own.then_some(self.body)
    }

    /// The model to send: the one a plugin set, else the client's; `None`,
    /// and none written, if neither named one.
    pub fn model(&self) -> Option<&'a str> {
        self.model
    }
}

/// A wire protocol. Stateless and shared; each response gets its own
/// [`Decoder`] and [`Encoder`].
pub trait Format: Send + Sync + 'static {
    /// The protocol's identity. Variants of one protocol, such as a
    /// provider's own endpoint for it, share the name, so their traffic is
    /// relayed, not translated. Formats of one name must share a kind, or
    /// the gateway does not build.
    fn name(&self) -> &str;

    /// The model a client request asks for, wherever this format keeps it:
    /// in the body, the path, or a header. `None` if it names none.
    fn model(&self, _head: &http::request::Parts, _body: &Doc) -> Option<String> {
        None
    }

    /// The provider request for `body` in this format: method, path
    /// (relative to the provider's base URL), content type, the client's
    /// headers it passes on, and `client.model()`. `client` may be in
    /// another format. The kernel then removes hop-by-hop headers
    /// (`Proxy-Authorization`, `Content-Length` and `TE` among them, keeping
    /// `te: trailers`), those `Connection` names, `Host` and `Expect`, then
    /// adds the `provider` headers, replacing same-named ones. A format
    /// passes on the client's credentials only to a provider that sets none.
    fn prepare(
        &self,
        body: Doc,
        client: RequestView<'_>,
        provider: &http::HeaderMap,
    ) -> Result<http::Request<Doc>, Fault>;

    /// Bytes to Parts, for a response with `head` to `client`'s request,
    /// which may be in another format. The decoder reads what the response
    /// is from `head`; `client` only says what the client sees, such as
    /// whether it asked for something this format adds.
    fn decoder(&self, head: &http::response::Parts, client: RequestView<'_>) -> Box<dyn Decoder>;

    /// Parts to bytes, for the response to `client`: bytes already in this
    /// format (`relayed`) or Parts made anew.
    ///
    /// Relayed: `head` is the response's. Items are written as their bytes,
    /// or rebuilt from them once edited; Frames as they are. Events a filter
    /// adds come as items without bytes.
    ///
    /// Made anew: `head` is a plugin's, or the status and `Retry-After` of a
    /// provider's response that was translated or answered a plugin's own
    /// request. A provider's is never a redirect: the kernel refuses that as
    /// a 502, as its `Location` would not reach the client. The encoder sets
    /// the content type if there is none, as the client's request asks. A
    /// format whose requests cannot ask to stream writes a translated
    /// response whole. Items are written from their events; one this format
    /// cannot write is an `Err`, so nothing is dropped.
    ///
    /// `Usage` always comes; relayed, its bytes already say it. The end
    /// always comes; write a failure in this format's framing, except,
    /// relayed, a [`Fault::relayed`] one, which its Frame already says.
    fn encoder(
        &self,
        head: &mut http::response::Parts,
        client: RequestView<'_>,
        relayed: bool,
    ) -> Box<dyn Encoder>;

    /// The format's kind, if any. `None`: it only relays its own traffic,
    /// and plugins cannot read its requests. Read once, when a route or
    /// provider is built.
    fn kind(&self) -> Option<Kind> {
        None
    }
}

/// A shared format; it forwards every method.
impl<F: Format + ?Sized> Format for Arc<F> {
    fn name(&self) -> &str {
        (**self).name()
    }

    fn model(&self, head: &http::request::Parts, body: &Doc) -> Option<String> {
        (**self).model(head, body)
    }

    fn prepare(
        &self,
        body: Doc,
        client: RequestView<'_>,
        provider: &http::HeaderMap,
    ) -> Result<http::Request<Doc>, Fault> {
        (**self).prepare(body, client, provider)
    }

    fn decoder(&self, head: &http::response::Parts, client: RequestView<'_>) -> Box<dyn Decoder> {
        (**self).decoder(head, client)
    }

    fn encoder(
        &self,
        head: &mut http::response::Parts,
        client: RequestView<'_>,
        relayed: bool,
    ) -> Box<dyn Encoder> {
        (**self).encoder(head, client, relayed)
    }

    fn kind(&self) -> Option<Kind> {
        (**self).kind()
    }
}

/// A kind's request or event type: one the kernel can copy and compare, to
/// tell what a plugin changed, and send across threads. Implemented for
/// every such type.
pub trait KindType: Any + Send + Sync + Clone + PartialEq {}

impl<T: Any + Send + Sync + Clone + PartialEq> KindType for T {}

/// A format's membership in a kind: the kind's request and event types,
/// which identify it, and this format's `decode` and `encode` for the
/// request type. Formats with the same types are of one kind; a client in
/// one reaches a provider in another through them. Anyone may define a
/// kind outside the kernel.
#[derive(Clone)]
pub struct Kind {
    pub(crate) request: TypeId,
    pub(crate) event: TypeId,
    /// The request and event types' names, for debugging.
    names: (&'static str, &'static str),
    pub(crate) codec: Arc<dyn ErasedCodec>,
}

impl std::fmt::Debug for Kind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (request, event) = self.names;
        f.debug_tuple("Kind").field(&request).field(&event).finish()
    }
}

impl Kind {
    /// The kind with request type `R`, whose responses decode into events
    /// of type `E`. Both should be the kind's own: a type shared with
    /// another kind, such as `serde_json::Value`, lets a plugin for one run
    /// on the other's routes. A kind whose requests carry nothing typed
    /// still has its own type for them, such as a unit struct.
    ///
    /// The kernel copies and compares requests, and filters compare events,
    /// to tell what a plugin changed. `R` does not hold the model: the
    /// kernel does ([`Context::model`](crate::Context::model)), and
    /// [`Format::prepare`] writes it.
    ///
    /// `decode` reads a client request of this format as `R`. It refuses
    /// only a body that is not this format; what `R` has no place for is
    /// noted in it, not refused, as the request may go to a provider of
    /// this format.
    ///
    /// `encode` writes an `R` as a body of this format. When the client's
    /// body is in this format too, it gets that body and the request decoded
    /// from it, and patches the body with what changed, keeping what `R`
    /// cannot hold. Otherwise it refuses, rather than drops, what this
    /// format cannot say. It runs only for a request a plugin changed or
    /// brought, or one going to another format; an unchanged one goes as
    /// it came.
    pub fn new<R, E>(
        decode: impl Fn(&http::request::Parts, &Doc) -> Result<R, Fault> + Send + Sync + 'static,
        encode: impl Fn(&R, Option<(&Doc, &R)>) -> Result<Doc, Fault> + Send + Sync + 'static,
    ) -> Self
    where
        R: KindType,
        E: KindType,
    {
        Self {
            request: TypeId::of::<R>(),
            event: TypeId::of::<E>(),
            names: (std::any::type_name::<R>(), std::any::type_name::<E>()),
            codec: Arc::new(Codec {
                decode,
                encode,
                request: PhantomData,
            }),
        }
    }

    /// Whether two formats are of one kind: the same request and event
    /// types, though each keeps its own codec.
    pub(crate) fn same(&self, other: &Kind) -> bool {
        (self.request, self.event) == (other.request, other.event)
    }
}

/// A kind's codec, erased: what the kernel calls.
pub(crate) trait ErasedCodec: Send + Sync {
    fn decode(
        &self,
        head: &http::request::Parts,
        body: &Doc,
    ) -> Result<Box<dyn Any + Send + Sync>, Fault>;
    fn encode(
        &self,
        request: &(dyn Any + Send + Sync),
        original: Option<(&Doc, &(dyn Any + Send + Sync))>,
    ) -> Result<Doc, Fault>;
    fn same(&self, a: &(dyn Any + Send + Sync), b: &(dyn Any + Send + Sync)) -> bool;
    /// A copy, if `request` is this kind's type.
    fn copy(&self, request: &(dyn Any + Send + Sync)) -> Option<Box<dyn Any + Send + Sync>>;
}

/// A format's `decode` and `encode` for request type `R`.
struct Codec<R, D, E> {
    decode: D,
    encode: E,
    request: PhantomData<fn() -> R>,
}

impl<R, D, E> ErasedCodec for Codec<R, D, E>
where
    R: KindType,
    D: Fn(&http::request::Parts, &Doc) -> Result<R, Fault> + Send + Sync,
    E: Fn(&R, Option<(&Doc, &R)>) -> Result<Doc, Fault> + Send + Sync,
{
    fn copy(&self, request: &(dyn Any + Send + Sync)) -> Option<Box<dyn Any + Send + Sync>> {
        let request = request.downcast_ref::<R>()?;
        Some(Box::new(request.clone()))
    }

    fn same(&self, a: &(dyn Any + Send + Sync), b: &(dyn Any + Send + Sync)) -> bool {
        matches!((a.downcast_ref::<R>(), b.downcast_ref::<R>()), (Some(a), Some(b)) if a == b)
    }

    fn decode(
        &self,
        head: &http::request::Parts,
        body: &Doc,
    ) -> Result<Box<dyn Any + Send + Sync>, Fault> {
        Ok(Box::new((self.decode)(head, body)?))
    }

    fn encode(
        &self,
        request: &(dyn Any + Send + Sync),
        original: Option<(&Doc, &(dyn Any + Send + Sync))>,
    ) -> Result<Doc, Fault> {
        let original = match original {
            Some((body, before)) => Some((body, typed::<R>(before)?)),
            None => None,
        };
        (self.encode)(typed::<R>(request)?, original)
    }
}

fn typed<R: Any>(request: &(dyn Any + Send + Sync)) -> Result<&R, Fault> {
    request
        .downcast_ref()
        .ok_or_else(|| Fault::internal("a request of another kind"))
}

/// Bytes to Parts, for one response. Every byte belongs to a Part, an
/// item's bytes or a Frame, except what the format leaves out on purpose.
/// A failed response, by error status or by an error the format reads as
/// failing a success, ends `Err`. Parts after the first `Part::End` are
/// ignored; with none, the response ends truncated. Trailers are not
/// delivered.
pub trait Decoder: Send + 'static {
    /// Decode what is whole; hold back an incomplete message for later.
    fn decode(&mut self, bytes: Bytes, out: &mut Vec<Part>);

    /// The body ended. Bytes of an incomplete message are not content:
    /// without a `Part::End` the response ends truncated.
    fn finish(&mut self, _out: &mut Vec<Part>) {}
}

/// Parts to bytes, for one response.
pub trait Encoder: Send + 'static {
    /// Write `part` to `out`. `Err` if it cannot be written: the response
    /// fails with that
    /// fault, handed back as a failed `Part::End`. If that cannot be written
    /// either, such as once a successful unary body began, the body is
    /// aborted.
    fn encode(&mut self, part: &Part, out: &mut BytesMut) -> Result<(), Fault>;
}
