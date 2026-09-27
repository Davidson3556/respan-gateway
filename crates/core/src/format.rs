use std::any::{Any, TypeId};
use std::sync::Arc;

use bytes::{Bytes, BytesMut};

use crate::{Doc, Fault, Part};

/// The client's request as formats see it, in the client's format (which
/// may not be the viewing format), with the model to send. Its head is the
/// head as plugins left it ([`Context::head_mut`](crate::Context::head_mut)),
/// on a call with that call's extensions; only its body is as it arrived.
/// On a call, the model is that call's.
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

    /// The client's request line and headers as plugins left them, in its
    /// format, even for a call with an input of its own. Values attached by
    /// type are in its `extensions`, as the call's hooks left them, which
    /// any format may read.
    pub fn head(&self) -> &'a http::request::Parts {
        self.head
    }

    /// Whether the viewing format is the client's, whose headers are then
    /// its own.
    pub fn is_own(&self) -> bool {
        self.own
    }

    /// The client's body, if the viewing format is the client's
    /// ([`is_own`](Self::is_own)), even for a call with an input of its own,
    /// whose body `prepare` is given instead.
    pub fn body(&self) -> Option<&'a Doc> {
        self.own.then_some(self.body)
    }

    /// The model to send: the call's ([`Call::model`](crate::Call::model)
    /// or [`ProviderContext::set_model`](crate::ProviderContext::set_model)),
    /// else [`Context::model`](crate::Context::model), the one a plugin set
    /// or the client's; `None`, and none written, if none names one.
    pub fn model(&self) -> Option<&'a str> {
        self.model
    }
}

/// A wire protocol. Stateless and shared; each response gets its own
/// [`Decoder`] and [`Encoder`].
///
/// Its kind is its pair of types: `Input`, what the protocol's caller
/// sends, and `Output`, what it gets back. Both are named from the
/// protocol's view, so they hold on both edges. A plain HTTP request is
/// one input, its whole body; a stream's response is many outputs. Formats
/// with the same pair are of one kind: a client in one reaches a provider
/// in another through `decode` and `encode`. Anyone may define a kind
/// outside the kernel.
///
/// The two types must differ, as the type names the direction: a format
/// with one type for both fails the build ([`Gateway::new`](crate::Gateway::new),
/// [`Gateway::serve`](crate::Gateway::serve)).
/// Both should be the kind's own: a type shared with another kind, such as
/// `serde_json::Value`, lets a plugin for one read the other's traffic. A
/// format of no kind, which only relays its own traffic and whose requests
/// plugins cannot read, declares `()` for both; the kernel never calls its
/// `decode` or `encode`.
pub trait Format: Send + Sync + 'static {
    /// What the protocol's caller sends, which the kernel copies and
    /// compares to tell what a plugin changed. It does not hold the model:
    /// the kernel does ([`Context::model`](crate::Context::model)), and
    /// [`Format::prepare`] writes it.
    type Input: Event;

    /// What the protocol's caller gets back: the events responses decode
    /// into, which filters compare.
    type Output: Event;

    /// The protocol's identity. Variants of one protocol, such as a
    /// provider's own endpoint for it, share the name, so their traffic is
    /// relayed, not translated. Formats of one name must share a kind, or
    /// the gateway does not build.
    fn name(&self) -> &str;

    /// Whether a client request with `head` is of this format, told from
    /// the head alone, such as its method and path: the gateway serves a
    /// request in the first format given to
    /// [`Gateway::serve`](crate::Gateway::serve) that accepts it. The body
    /// is never read here. A format given only to providers is never asked,
    /// and may answer `false`.
    fn accepts(&self, head: &http::request::Parts) -> bool;

    /// The model a client request asks for, wherever this format keeps it:
    /// in the body, the path, or a header. `None` if it names none. `head`
    /// is the head as it arrived, before any plugin's edit.
    fn model(&self, _head: &http::request::Parts, _body: &Doc) -> Option<String> {
        None
    }

    /// A client request of this format as its input. It refuses, with
    /// [`codes::BAD_REQUEST`](crate::codes::BAD_REQUEST), only a body that
    /// is not this format; what `Input` has no place for is noted in it,
    /// not refused, as the request may go to a provider of this format.
    /// `head` is the head as it arrived, so reading the input never changes
    /// what is sent; a plugin's edit to the head reaches calls only through
    /// [`Format::prepare`].
    fn decode(&self, head: &http::request::Parts, body: &Doc) -> Result<Self::Input, Fault>;

    /// An input as a body of this format. When the client's body is in this
    /// format too, `original` is that body and the input decoded from it,
    /// and the body is patched with what changed, keeping what `Input`
    /// cannot hold. Otherwise it refuses, rather than drops, what this
    /// format cannot say. It runs only for an input a plugin changed or
    /// brought, or one going to another format; an unchanged one goes as
    /// it came. `Err` with [`codes::BAD_REQUEST`](crate::codes::BAD_REQUEST)
    /// for what the client sent that this format cannot say, and with
    /// [`codes::CONFIG`](crate::codes::CONFIG) for a plugin's change it
    /// cannot make.
    fn encode(
        &self,
        input: &Self::Input,
        original: Option<(&Doc, &Self::Input)>,
    ) -> Result<Doc, Fault>;

    /// The call's HTTP request for `body` in this format: method, path and
    /// query (relative to the provider's transport), content type, the
    /// client's headers it passes on, and `client.model()`. `client` may be
    /// in another format. Within one format ([`ClientView::is_own`]) the
    /// client's end-to-end headers pass on, credentials included: the
    /// transport knows whose keys go to the provider; across formats, being
    /// another protocol's, none.
    ///
    /// The kernel then drops `Content-Encoding` if the prepared body's bytes
    /// are not the client's as they arrived, such as when patched, encoded
    /// anew or given the model, and removes hop-by-hop headers
    /// (`Proxy-Authorization`, `Content-Length` and `TE` among them,
    /// keeping `te: trailers`), `Host` and `Expect`; the transport adds its
    /// own. Headers the client's `Connection` names are not in `client`.
    /// `Err` with [`codes::BAD_REQUEST`](crate::codes::BAD_REQUEST) for a
    /// body it cannot write the model into.
    fn prepare(&self, body: Doc, client: ClientView<'_>) -> Result<http::Request<Doc>, Fault>;

    /// Bytes to Parts, for a response with `head` to `client`'s request,
    /// which may be in another format. The decoder reads what the response
    /// is from `head`; `client` only says what the client sees, such as
    /// whether it asked for something this format adds.
    fn decoder(&self, head: &http::response::Parts, client: ClientView<'_>) -> Box<dyn Decoder>;

    /// Parts to bytes, for the response to `client`: bytes already in this
    /// format (`relayed`) or Parts made anew. `client` is the client's own
    /// request, so its [`is_own`](ClientView::is_own) is `true`, and its
    /// [`model`](ClientView::model) is the request's, not that of the call
    /// that answered.
    ///
    /// Relayed: `head` is the response's. Items are written as their bytes,
    /// or rebuilt from them once edited; Frames as they are. Events a filter
    /// adds come as items without bytes.
    ///
    /// Made anew: `head` is a plugin's, or the status and `Retry-After` of a
    /// provider's response that was translated or answered a call's own
    /// input. A provider's is never a redirect (301, 302, 303, 307 or 308
    /// with a `Location`): the kernel refuses that as a 502, as its
    /// `Location` would not reach the client. Any other status passes, a
    /// 304 or 300 too, written as a failure if its format read one. The
    /// encoder
    /// sets the content type if there is none, as the client's request
    /// asks. A format whose requests cannot ask to stream writes a
    /// translated response whole. Items are written from their events; one
    /// this format cannot write is an `Err`, so nothing is dropped.
    ///
    /// `Usage` always comes; relayed, its bytes already say it. A failure is
    /// written in this format's framing. The end always comes, except,
    /// relayed, a [`Fault::said`] one: its Frame already said it, so the
    /// encoder is not handed it.
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
    /// Its kind: its input and output types; `None` for `()`, no kind.
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

/// An input as its format's type, which the kernel checked when it took it.
fn typed<T: Any>(input: &AnyInput) -> &T {
    input
        .downcast_ref()
        .expect("inputs are checked to be of their format's kind")
}

/// Bytes to Parts, for one response. Every byte belongs to a Part, an
/// item's bytes or a Frame, except what the format leaves out on purpose.
/// A failed response, by error status or by an error the format reads as
/// failing a success, ends `Err`; a success that is not a response of its
/// format ends `Err` with [`codes::UPSTREAM`](crate::codes::UPSTREAM).
/// Parts after the first `Part::End` are ignored; with none, the response
/// ends truncated. Trailers are not delivered.
pub trait Decoder: Send + 'static {
    /// Decode what is whole; hold back an incomplete message for later.
    fn decode(&mut self, bytes: Bytes, out: &mut Vec<Part>);

    /// The body ended. Bytes of an incomplete message are not content:
    /// without a `Part::End` the response ends truncated.
    fn finish(&mut self, _out: &mut Vec<Part>) {}
}

/// Parts to bytes, for one response.
pub trait Encoder: Send + 'static {
    /// Write `part` to `out`. `Err` if it cannot be written: what it wrote
    /// of it is dropped, and the response fails with that fault, handed
    /// back as a failed `Part::End`. If that cannot be written either, such
    /// as once a successful unary body began, the body is aborted.
    fn encode(&mut self, part: &Part, out: &mut BytesMut) -> Result<(), Fault>;
}
