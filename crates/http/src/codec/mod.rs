//! HTTP's codecs and its one driver, on the kernel's edge contract
//! ([`Dialect`](respan_gateway_core::Dialect), a temporary name, decision 0020, step 4).
//!
//! A [`Codec`] is one protocol's bytes on HTTP: today's
//! [`Format`](respan_gateway_core::Format) with its heads fixed to HTTP's
//! ([`Ask`], [`Answer`]). A [`Transport`] reaches one provider. The driver
//! turns any codec into a dialect ([`dialect!`](crate::dialect)): its client
//! edge ([`accept`], [`opening`]) serves an [`Accepted`] request, its
//! provider edge ([`dial`]) calls through a [`Dialer`]. It is the byte code
//! the kernel had, moved, one symmetric pair: a reader turns frames into
//! events through a [`Decoder`], a writer events into frames through an
//! [`Encoder`], on both edges. The byte half of the kernel's guarantees
//! lives here:
//!
//! - **Head timing (rule 4).** A one-shot request's answer writes its head
//!   with its first bytes, a failure before any written as a refusal; a
//!   streaming request's (an upgrade) when its reply resolves.
//! - **The verdict at the head (rule 3).** A non-2xx answer, but a 101 to a
//!   call that asked to switch, is read whole before [`dial`] resolves, so
//!   a refusal is known as one however slowly its body comes.
//! - **Batches (rule 6).** A provider's outputs say `Pending` between the
//!   frames it sent apart.
//! - **Streaming inputs (rule 1).** A streaming request's inputs are never
//!   ended at once.
//! - **A body ends at its first failure (B1)**, an encoder's panic on a
//!   call's later frame fails the call (B2), and [`Encoder::open`]'s bytes
//!   come first, done at once for a response with no body.
//! - **Relay.** A call written from the client's origin starts from the
//!   client's head and bytes, unread, and is [`Exchange::relayed`]; a
//!   provider's answer on a relay is written from its head and bytes.
//!
//! Nothing uses it yet: the flip (step 5) makes it the gateway's HTTP edge.

mod accept;
mod dial;
mod reader;
mod writer;

use std::sync::Arc;

use bytes::Bytes;
use futures::FutureExt;
use futures::channel::oneshot;
use futures::future::{BoxFuture, ready};
use futures::stream::BoxStream;
use http::HeaderMap;
use http::header::UPGRADE;
use respan_gateway_core::{Extensions, Fault, Gateway, async_trait};

pub use accept::{accept, opening};
pub use dial::dial;
pub use respan_gateway_core::{Decoder, Encoder};

use crate::{Answer, Ask, Body};

/// The message a panic in a codec or plugin ends with.
const PANICKED: &str = "a plugin or format panicked";

/// A body, its bytes one item per data frame, ending with an `Err` if it
/// broke off.
pub type Frames = BoxStream<'static, Result<Bytes, Fault>>;

/// One protocol's codec on HTTP, stateless and shared: each body, either
/// way, gets its own [`Decoder`] or [`Encoder`], which keeps that stream's
/// state. The factories never fail: a failure travels in the stream. Its
/// methods are [`Format`](respan_gateway_core::Format)'s, with its heads
/// HTTP's; [`dialect!`](crate::dialect) makes it a dialect.
///
/// A body is a stream of messages, each decoded into one value of the kind,
/// even one the kind cannot read, which keeps its bytes: a relay loses
/// nothing only if the codec writes a value as the bytes it came in while
/// it says the same. A request's inputs stream, one per data frame, if its
/// adapter says it streams ([`Accepted::new`]); else its body is its one
/// input, even empty.
///
/// Its kind is its pair of value types, through which codecs of one kind
/// translate. They must differ, as the type names the direction.
pub trait Codec: Send + Sync + 'static {
    /// What a request carries, one or a stream.
    type Input: Clone + PartialEq + Send + Sync + 'static;

    /// What a response carries, one or a stream.
    type Output: Clone + PartialEq + Send + Sync + 'static;

    /// The protocol's identity. Codecs of one name relay rather than
    /// translate, and share a kind.
    fn name(&self) -> &str;

    /// Whether a request with `head` is in this protocol; the first served
    /// that accepts takes it.
    fn accepts(&self, head: &Ask) -> bool;

    /// Decodes the client's request with `head`: its body when first read,
    /// or a streaming request's frames as they pass. `Err` with
    /// [`codes::BAD_REQUEST`](respan_gateway_core::codes::BAD_REQUEST) for
    /// bytes not in this protocol.
    fn request_decoder(&self, head: &Ask) -> Box<dyn Decoder<Self::Input>>;

    /// Encodes a call. `client` is the client's head if the call relays the
    /// client's message; `body` its first bytes, the client's when relayed
    /// unread, else empty, to edit. The encoder writes the head
    /// ([`Encoder::head`]), then any inputs written from their values.
    /// `streams`: a streaming request's inputs follow, one per frame.
    fn request_encoder(
        &self,
        client: Option<&Ask>,
        body: &mut Bytes,
        streams: bool,
    ) -> Box<dyn Encoder<Self::Input, Ask>>;

    /// Decodes a response with head `answer` to the call with head `call`
    /// and first bytes `sent`. A provider's error answer ends `Err`, with
    /// its answer ([`Fault::with_answer`]) to write as it came.
    fn response_decoder(
        &self,
        answer: &Answer,
        call: &Ask,
        sent: &Bytes,
    ) -> Box<dyn Decoder<Self::Output>>;

    /// Encodes a response to the client's request with head `ask` and body
    /// `body` (empty if streaming). `answer` is the provider's head on a
    /// relay, to start the head from. A `refusal` is the response's one
    /// failure, before any byte.
    fn response_encoder(
        &self,
        answer: Option<&Answer>,
        ask: &Ask,
        body: &Bytes,
        refusal: Option<&Fault>,
    ) -> Box<dyn Encoder<Self::Output, Answer>>;

    /// Whether `output` is content, what
    /// [`Response::until_content`](respan_gateway_core::Response::until_content)
    /// stops at; a keep-alive is not. By default, true.
    fn is_content(&self, _output: &Self::Output) -> bool {
        true
    }
}

/// One direction of an HTTP exchange: a head, a body and [`Extensions`].
/// What a [`Transport`] sends and gets back.
#[non_exhaustive]
pub struct Exchange<H> {
    /// The head: a request's method, URI and headers, or a response's
    /// status and headers.
    pub head: H,
    /// The bytes after the head.
    pub body: Frames,
    /// Values for the transport, such as the client's credential; none on
    /// a response.
    pub extensions: Extensions,
    /// Whether this call was written from the client's own message: its
    /// head and bytes, on a relay to a provider of the client's codec's
    /// name. The driver sets it then alone; a transport passes on what
    /// belongs to the client, such as its credential ([`credentials`]),
    /// only then. False on a response.
    pub relayed: bool,
    /// Whether the body's frames follow the head as they come: a streaming
    /// request's inputs. A transport sends such a body as it comes, else
    /// whole.
    pub streams: bool,
}

impl<H> Exchange<H> {
    /// An exchange of `head` and `body`, with no extensions, not relayed,
    /// not streaming.
    pub fn new(head: H, body: Frames) -> Self {
        Self {
            head,
            body,
            extensions: Extensions::new(),
            relayed: false,
            streams: false,
        }
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

/// How the gateway reaches one provider over HTTP: a URL and keys, a
/// process, a socket or in-process code, with any state its calls share.
#[async_trait]
pub trait Transport: Send + Sync + 'static {
    /// Send `call` and return the provider's response, whatever it says,
    /// once its head arrives; return redirects rather than follow them.
    /// A relay's head starts from the client's, so strip what a proxy must
    /// not pass on ([`proxy_call`](crate::proxy_call)). Send a body that
    /// [`streams`](Exchange::streams) as it comes, else whole; one that ends
    /// with an `Err` aborts the message. Decide whose keys reach the
    /// provider ([`credentials`]).
    ///
    /// `Err` if the call could not be sent: [`Fault::upstream`] if the
    /// provider is unreachable, [`Fault::config`] if this transport cannot
    /// express it, or a failure of its own.
    async fn send(&self, call: Exchange<Ask>) -> Result<Exchange<Answer>, Fault>;
}

/// A shared transport; it forwards every method.
#[async_trait]
impl<T: Transport + ?Sized> Transport for Arc<T> {
    async fn send(&self, call: Exchange<Ask>) -> Result<Exchange<Answer>, Fault> {
        (**self).send(call).await
    }
}

/// [`crate::credentials`] for a call of this module's [`Exchange`]: its
/// [`CallHeaders`](crate::CallHeaders) over the transport's own `headers`;
/// with neither, the client's [`Credential`](crate::Credential), only on a
/// call that is [`relayed`](Exchange::relayed).
pub fn credentials(call: &mut Exchange<Ask>, headers: &HeaderMap) {
    crate::transport::keys(&mut call.head, &call.extensions, call.relayed, headers);
}

/// How a codec's calls reach one provider: any [`Transport`], shared. A
/// dialect's [`Dialer`](respan_gateway_core::Dialect::Dialer).
#[derive(Clone)]
pub struct Dialer(Arc<dyn Transport>);

impl<T: Transport> From<T> for Dialer {
    fn from(transport: T) -> Self {
        Dialer(Arc::new(transport))
    }
}

impl std::fmt::Debug for Dialer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Dialer").finish_non_exhaustive()
    }
}

/// What the client edge hands its adapter, through the wire's slot.
type Writing = BoxFuture<'static, http::Response<Body>>;

/// A client's HTTP request as its adapter accepted it: a dialect's
/// [`Wire`](respan_gateway_core::Dialect::Wire), which
/// [`Gateway::open`] takes. It carries the adapter's reply slot.
pub struct Accepted {
    head: Ask,
    frames: Frames,
    streams: bool,
    slot: oneshot::Sender<Writing>,
}

impl Accepted {
    /// A request of `head` and `body`, and what to answer it with once the
    /// gateway opened it ([`Gateway::open`]). If `streams`, its data frames
    /// are inputs as they come and the answer's head goes out once the
    /// reply resolves, as a client that switches protocols waits for it;
    /// else its body is read whole, so the caller bounds it.
    ///
    /// The answer is the response as its codec wrote it, before the
    /// adapter's rules for a hop, or plain text for a refusal no codec can
    /// write. `None` if no dialect took the request: [`Gateway::open`]'s end
    /// says why. Drive `open` to its end while awaiting it.
    pub fn new(
        head: Ask,
        body: Frames,
        streams: bool,
    ) -> (Accepted, BoxFuture<'static, Option<http::Response<Body>>>) {
        let (slot, writing) = oneshot::channel::<Writing>();
        let wire = Accepted {
            head,
            frames: body,
            streams,
            slot,
        };
        let answer = async move {
            let writing = writing.await.ok()?;
            Some(writing.await)
        };
        (wire, answer.boxed())
    }

    /// The request's head.
    pub fn head(&self) -> &Ask {
        &self.head
    }
}

impl std::fmt::Debug for Accepted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Accepted")
            .field("head", &self.head)
            .field("streams", &self.streams)
            .finish_non_exhaustive()
    }
}

/// What came with a client's head, its origin: the head, which a call that
/// relays starts from, and a one-shot request's body, its bytes unread.
struct Came {
    head: Arc<Ask>,
    body: Option<Bytes>,
}

/// [`crate::serve`] through the driver: [`handle`] `req`, streaming if it
/// asks to switch protocols (`Upgrade`).
pub async fn serve<B>(
    gateway: &Gateway,
    req: http::Request<B>,
) -> (http::Response<Body>, BoxFuture<'static, Result<(), Fault>>)
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Fault>,
{
    let streaming = req.headers().contains_key(UPGRADE);
    handle(gateway, req, streaming).await
}

/// [`crate::handle`] through the driver: `req` as an [`Accepted`] wire,
/// handed to [`Gateway::open`], under the same rules for a hop, before and
/// after, to a gateway that serves dialects
/// ([`Gateway::serve_dialect`]).
pub async fn handle<B>(
    gateway: &Gateway,
    req: http::Request<B>,
    streaming: bool,
) -> (http::Response<Body>, BoxFuture<'static, Result<(), Fault>>)
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Fault>,
{
    let (crate::Arrived { head, body, to }, extensions) = crate::arrive(req);
    let (wire, answer) = Accepted::new(head, body, streaming);
    let ended = gateway.open(wire, extensions).await;
    let (resp, ended) = match answer.await {
        Some(resp) => (Ok(resp), ended),
        None => {
            let fault = ended.await.err();
            let fault = fault.unwrap_or_else(|| Fault::internal("no format answered"));
            (Err(fault.clone()), ready(Err(fault)).boxed())
        }
    };
    (crate::depart(&to, streaming, resp), ended)
}

/// Implements the kernel's [`Dialect`](respan_gateway_core::Dialect) for a
/// [`Codec`](crate::codec::Codec) through HTTP's driver: its wire is an
/// [`Accepted`](crate::codec::Accepted) request, its dialer a
/// [`Dialer`](crate::codec::Dialer). The codec must be `Clone`, as its
/// edges outlive the call that made them.
///
/// ```
/// # use respan_gateway_http::codec::{Codec, Decoder, Encoder};
/// # use respan_gateway_http::{Answer, Ask};
/// # use respan_gateway_core::{Fault, bytes::Bytes};
/// #[derive(Clone)]
/// struct Echo;
/// impl Codec for Echo {
///     type Input = String;
///     type Output = Vec<u8>;
///     fn name(&self) -> &str { "echo" }
///     fn accepts(&self, _: &Ask) -> bool { true }
///     # fn request_decoder(&self, _: &Ask) -> Box<dyn Decoder<String>> { unimplemented!() }
///     # fn request_encoder(&self, _: Option<&Ask>, _: &mut Bytes, _: bool)
///     #     -> Box<dyn Encoder<String, Ask>> { unimplemented!() }
///     # fn response_decoder(&self, _: &Answer, _: &Ask, _: &Bytes)
///     #     -> Box<dyn Decoder<Vec<u8>>> { unimplemented!() }
///     # fn response_encoder(&self, _: Option<&Answer>, _: &Ask, _: &Bytes, _: Option<&Fault>)
///     #     -> Box<dyn Encoder<Vec<u8>, Answer>> { unimplemented!() }
///     // ...
/// }
/// respan_gateway_http::dialect!(Echo);
/// let gateway = respan_gateway_core::Gateway::new().serve_dialect(Echo);
/// assert!(gateway.is_ok());
/// ```
#[macro_export]
macro_rules! dialect {
    ($t:ty) => {
        impl $crate::codec::__private::Dialect for $t {
            type Input = <$t as $crate::codec::Codec>::Input;
            type Output = <$t as $crate::codec::Codec>::Output;
            type Wire = $crate::codec::Accepted;
            type Dialer = $crate::codec::Dialer;

            fn name(&self) -> &str {
                $crate::codec::Codec::name(self)
            }

            fn accepts(&self, wire: &$crate::codec::Accepted) -> bool {
                $crate::codec::Codec::accepts(self, wire.head())
            }

            fn is_content(&self, output: &Self::Output) -> bool {
                $crate::codec::Codec::is_content(self, output)
            }

            fn accept(
                &self,
                wire: $crate::codec::Accepted,
                reply: $crate::codec::__private::BoxFuture<
                    'static,
                    $crate::codec::__private::Reply<Self::Output>,
                >,
            ) -> $crate::codec::__private::BoxFuture<
                'static,
                $crate::codec::__private::Half<Self::Input>,
            > {
                $crate::codec::accept(self, wire, reply)
            }

            fn opening(
                &self,
                origin: &$crate::codec::__private::Origin,
            ) -> ::core::option::Option<
                ::core::result::Result<Self::Input, $crate::codec::__private::Fault>,
            > {
                $crate::codec::opening(self, origin)
            }

            fn dial(
                &self,
                dialer: &$crate::codec::Dialer,
                extensions: $crate::codec::__private::Extensions,
                relay: ::core::option::Option<&$crate::codec::__private::Origin>,
                opening: ::core::option::Option<(&Self::Input, bool)>,
                later: ::core::option::Option<
                    $crate::codec::__private::Events<(Self::Input, bool)>,
                >,
            ) -> ::core::result::Result<
                $crate::codec::__private::Dialed<Self::Output>,
                $crate::codec::__private::Fault,
            > {
                $crate::codec::dial(self, dialer, extensions, relay, opening, later)
            }
        }
    };
}

/// What [`dialect!`](crate::dialect) names, not for use elsewhere.
#[doc(hidden)]
pub mod __private {
    pub use respan_gateway_core::futures::future::BoxFuture;
    pub use respan_gateway_core::{
        Dialect, Dialed, Events, Extensions, Fault, Half, Origin, Reply,
    };
}
