use std::any::{Any, TypeId};
use std::sync::Arc;

use bytes::{Bytes, BytesMut};

use crate::{Doc, Fault, Part};

/// The client's request as a format sees it, which may be in another
/// format: the head as plugins left it, the body as it arrived, and the
/// model to send. On a call, the extensions and model are the call's.
#[derive(Debug, Clone, Copy)]
pub struct ClientView<'a> {
    own: bool,
    head: &'a http::request::Parts,
    body: &'a Doc,
    model: Option<&'a str>,
}

impl<'a> ClientView<'a> {
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

    /// The client's request line and headers as plugins left them
    /// ([`Context::head_mut`](crate::Context::head_mut)). Any format may
    /// read its `extensions`.
    pub fn head(&self) -> &'a http::request::Parts {
        self.head
    }

    /// Whether the viewing format is the client's.
    pub fn is_own(&self) -> bool {
        self.own
    }

    /// The client's body, if the viewing format is the client's
    /// ([`is_own`](Self::is_own)). A call with its own input still sees the
    /// client's here; `prepare` gets the call's.
    pub fn body(&self) -> Option<&'a Doc> {
        self.own.then_some(self.body)
    }

    /// The model to send: the call's ([`Call::model`](crate::Call::model)
    /// or [`ProviderContext::set_model`](crate::ProviderContext::set_model)),
    /// else [`Context::model`](crate::Context::model); `None` if none is
    /// named, and then none is written.
    pub fn model(&self) -> Option<&'a str> {
        self.model
    }
}

/// A wire protocol. Stateless and shared; each response gets its own
/// [`Decoder`] and [`Encoder`].
///
/// Its kind is its pair of types: `Input`, what a caller of the protocol
/// sends, and `Output`, what it gets back, on both edges. A request is one
/// input; a streamed response is many outputs. Formats of one kind
/// translate through `decode` and `encode`. Kinds live outside the kernel.
///
/// The two types must differ, since the type names the direction;
/// otherwise [`Gateway::new`](crate::Gateway::new) and
/// [`Gateway::serve`](crate::Gateway::serve) fail. Both should belong to
/// the kind alone: a shared type such as `serde_json::Value` lets a plugin
/// for one kind read another's traffic. A format of no kind declares `()`
/// for both: it only relays, plugins cannot read it, and its `decode` and
/// `encode` are never called.
pub trait Format: Send + Sync + 'static {
    /// What a caller sends, compared to tell what a plugin changed. It
    /// leaves out the model, which the kernel holds
    /// ([`Context::model`](crate::Context::model)) and
    /// [`Format::prepare`] writes.
    type Input: Event;

    /// What a caller gets back: the events responses decode into.
    type Output: Event;

    /// The protocol's identity. Formats of one name relay to each other
    /// rather than translate, and must share a kind.
    fn name(&self) -> &str;

    /// Whether a client request with `head` is of this format, from the
    /// head alone. A request goes to the first format given to
    /// [`Gateway::serve`](crate::Gateway::serve) that accepts it. Formats
    /// used only by providers are never asked.
    fn accepts(&self, head: &http::request::Parts) -> bool;

    /// The model a client request names, wherever this format keeps it;
    /// `None` if none. `head` is as it arrived.
    fn model(&self, _head: &http::request::Parts, _body: &Doc) -> Option<String> {
        None
    }

    /// A client request of this format as its input. `Err` with
    /// [`codes::BAD_REQUEST`](crate::codes::BAD_REQUEST) only for a body
    /// not of this format; note in the input what it cannot hold rather
    /// than refuse it, as a provider of this format may take it. `head` is
    /// as it arrived, so reading the input never changes what is sent.
    fn decode(&self, head: &http::request::Parts, body: &Doc) -> Result<Self::Input, Fault>;

    /// An input as a body of this format. If the client's body is of this
    /// format, `original` is that body and its decoded input: patch the
    /// body with what changed, keeping what `Input` cannot hold. Called
    /// only for a changed or brought input, or across formats. Never drop
    /// what this format cannot say: `Err` with
    /// [`codes::BAD_REQUEST`](crate::codes::BAD_REQUEST) if the client sent
    /// it, [`codes::CONFIG`](crate::codes::CONFIG) if a plugin did.
    fn encode(
        &self,
        input: &Self::Input,
        original: Option<(&Doc, &Self::Input)>,
    ) -> Result<Doc, Fault>;

    /// The call's HTTP request for `body`: method, path and query (relative
    /// to the transport), content type, passed-on client headers, and
    /// `client.model()`. Within one format ([`ClientView::is_own`]) pass on
    /// the client's headers, credentials included, since the transport
    /// decides whose keys reach the provider; across formats, pass none.
    ///
    /// The kernel then drops `Content-Encoding` if the body's bytes differ
    /// from the client's, and removes hop-by-hop headers (keeping
    /// `te: trailers`), `Host` and `Expect`. `client` already lacks the
    /// headers the client's `Connection` names. `Err` with
    /// [`codes::BAD_REQUEST`](crate::codes::BAD_REQUEST) for a body it
    /// cannot write the model into.
    fn prepare(&self, body: Doc, client: ClientView<'_>) -> Result<http::Request<Doc>, Fault>;

    /// Bytes to Parts, for a response with `head` to `client`'s request.
    /// The response is read from `head`; `client` only says what the client
    /// asked for.
    fn decoder(&self, head: &http::response::Parts, client: ClientView<'_>) -> Box<dyn Decoder>;

    /// Parts to bytes, for the response to `client`, the client's own
    /// request ([`is_own`](ClientView::is_own) is `true`, and the model is
    /// the request's). `relayed` if the Parts came in this format.
    ///
    /// Relayed: `head` is the response's. Write an item as its bytes, or
    /// rebuild it from them once edited, and Frames as they are. Events a
    /// filter adds come as items without bytes.
    ///
    /// Otherwise: `head` is a plugin's, or a provider's status and
    /// `Retry-After`; never a redirect, which the kernel refuses as a 502.
    /// Set the content type if there is none. Write items from their
    /// events, whole if the client could not ask to stream; one this format
    /// cannot write is an `Err`, so nothing is dropped.
    ///
    /// `Usage` always comes; relayed, its bytes already carry it. Write a
    /// failure in this format's framing. The end always comes, except a
    /// relayed [`Fault::said`] one, which its Frame already wrote.
    fn encoder(
        &self,
        head: &mut http::response::Parts,
        client: ClientView<'_>,
        relayed: bool,
    ) -> Box<dyn Encoder>;
}

/// A shared format; it forwards every method.
impl<F: Format> Format for Arc<F> {
    type Input = F::Input;
    type Output = F::Output;

    fn name(&self) -> &str {
        (**self).name()
    }

    fn accepts(&self, head: &http::request::Parts) -> bool {
        (**self).accepts(head)
    }

    fn model(&self, head: &http::request::Parts, body: &Doc) -> Option<String> {
        (**self).model(head, body)
    }

    fn decode(&self, head: &http::request::Parts, body: &Doc) -> Result<Self::Input, Fault> {
        (**self).decode(head, body)
    }

    fn encode(
        &self,
        input: &Self::Input,
        original: Option<(&Doc, &Self::Input)>,
    ) -> Result<Doc, Fault> {
        (**self).encode(input, original)
    }

    fn prepare(&self, body: Doc, client: ClientView<'_>) -> Result<http::Request<Doc>, Fault> {
        (**self).prepare(body, client)
    }

    fn decoder(&self, head: &http::response::Parts, client: ClientView<'_>) -> Box<dyn Decoder> {
        (**self).decoder(head, client)
    }

    fn encoder(
        &self,
        head: &mut http::response::Parts,
        client: ClientView<'_>,
        relayed: bool,
    ) -> Box<dyn Encoder> {
        (**self).encoder(head, client, relayed)
    }
}

/// The bound on a format's input and output: one the kernel can copy and
/// compare, to tell what a plugin changed, and send across threads.
/// Implemented for every such type.
pub trait Event: Any + Send + Sync + Clone + PartialEq {}

impl<T: Any + Send + Sync + Clone + PartialEq> Event for T {}

/// An input, erased and shared: copied only when changed while shared.
pub(crate) type AnyInput = Arc<dyn Any + Send + Sync>;

/// A [`Format`] with its input and output erased, as the gateway and
/// providers hold it.
pub(crate) trait AnyFormat: Send + Sync + 'static {
    fn name(&self) -> &str;
    fn accepts(&self, head: &http::request::Parts) -> bool;
    fn model(&self, head: &http::request::Parts, body: &Doc) -> Option<String>;
    fn prepare(&self, body: Doc, client: ClientView<'_>) -> Result<http::Request<Doc>, Fault>;
    fn decoder(&self, head: &http::response::Parts, client: ClientView<'_>) -> Box<dyn Decoder>;
    fn encoder(
        &self,
        head: &mut http::response::Parts,
        client: ClientView<'_>,
        relayed: bool,
    ) -> Box<dyn Encoder>;
    /// Its input and output types; `None` for no kind.
    fn kind(&self) -> Option<(TypeId, TypeId)>;
    fn decode(&self, head: &http::request::Parts, body: &Doc) -> Result<AnyInput, Fault>;
    fn encode(&self, input: &AnyInput, original: Option<(&Doc, &AnyInput)>) -> Result<Doc, Fault>;
    /// Whether two inputs are equal.
    fn same(&self, a: &AnyInput, b: &AnyInput) -> bool;
    /// A copy of an input.
    fn copy(&self, input: &AnyInput) -> AnyInput;
}

impl<F: Format> AnyFormat for F {
    fn name(&self) -> &str {
        Format::name(self)
    }

    fn accepts(&self, head: &http::request::Parts) -> bool {
        Format::accepts(self, head)
    }

    fn model(&self, head: &http::request::Parts, body: &Doc) -> Option<String> {
        Format::model(self, head, body)
    }

    fn prepare(&self, body: Doc, client: ClientView<'_>) -> Result<http::Request<Doc>, Fault> {
        Format::prepare(self, body, client)
    }

    fn decoder(&self, head: &http::response::Parts, client: ClientView<'_>) -> Box<dyn Decoder> {
        Format::decoder(self, head, client)
    }

    fn encoder(
        &self,
        head: &mut http::response::Parts,
        client: ClientView<'_>,
        relayed: bool,
    ) -> Box<dyn Encoder> {
        Format::encoder(self, head, client, relayed)
    }

    fn kind(&self) -> Option<(TypeId, TypeId)> {
        let kind = (TypeId::of::<F::Input>(), TypeId::of::<F::Output>());
        let none = (TypeId::of::<()>(), TypeId::of::<()>());
        (kind != none).then_some(kind)
    }

    fn decode(&self, head: &http::request::Parts, body: &Doc) -> Result<AnyInput, Fault> {
        Ok(Arc::new(Format::decode(self, head, body)?))
    }

    fn encode(&self, input: &AnyInput, original: Option<(&Doc, &AnyInput)>) -> Result<Doc, Fault> {
        let original = original.map(|(body, before)| (body, typed::<F::Input>(before)));
        Format::encode(self, typed(input), original)
    }

    fn same(&self, a: &AnyInput, b: &AnyInput) -> bool {
        matches!((a.downcast_ref::<F::Input>(), b.downcast_ref::<F::Input>()), (Some(a), Some(b)) if a == b)
    }

    fn copy(&self, input: &AnyInput) -> AnyInput {
        Arc::new(typed::<F::Input>(input).clone())
    }
}

/// An input as its format's type, checked when the kernel took it.
fn typed<T: Any>(input: &AnyInput) -> &T {
    input
        .downcast_ref()
        .expect("inputs are checked to be of their format's kind")
}

/// Bytes to Parts, for one response. Every byte belongs to an item or a
/// Frame, except what the format drops on purpose. A failed response ends
/// `Err`; a success that is not of this format ends `Err` with
/// [`codes::UPSTREAM`](crate::codes::UPSTREAM). Parts after the first
/// `Part::End` are ignored; with none, the response ends truncated.
/// Trailers are not delivered.
pub trait Decoder: Send + 'static {
    /// Decode what is whole; hold back an incomplete message for later.
    fn decode(&mut self, bytes: Bytes, out: &mut Vec<Part>);

    /// The body ended. Without a `Part::End` the response ends truncated.
    fn finish(&mut self, _out: &mut Vec<Part>) {}
}

/// Parts to bytes, for one response.
pub trait Encoder: Send + 'static {
    /// Write `part` to `out`. On `Err` its partial output is dropped and
    /// the fault comes back as a failed `Part::End`; if that fails too, the
    /// body is aborted.
    fn encode(&mut self, part: &Part, out: &mut BytesMut) -> Result<(), Fault>;
}
