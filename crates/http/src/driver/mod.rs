//! HTTP's one driver, which makes any [`Codec`] a
//! [`Format`](respan_gateway_core::Format) ([`dialect!`](crate::dialect)):
//! its client edge ([`accept`], [`opening`]) serves an [`Accepted`]
//! request, its provider edge ([`dial`]) calls through a [`Dialer`]. It is
//! one symmetric pair: a reader turns frames into events through a
//! [`Decoder`](crate::Decoder), a writer events into frames through an
//! [`Encoder`], on both edges. The byte half of the gateway's guarantees
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
//! - **Relay.** A call of the session's own inputs to a format of the same
//!   name starts from the client's head and, if no filter edited them, bytes
//!   unread, and is [`Exchange::relayed`](crate::Exchange::relayed), even if
//!   a filter edited its inputs; a call with its own input is not. A
//!   provider's answer on a relay is written from its head and bytes.

mod accept;
mod dial;
mod reader;
mod writer;

use std::sync::Arc;

use bytes::Bytes;
use futures::FutureExt;
use futures::channel::oneshot;
use futures::future::BoxFuture;
#[cfg(doc)]
use respan_gateway_core::Gateway;

pub use accept::{accept, opening};
pub use dial::dial;

use crate::{Ask, Body, Frames};
#[cfg(doc)]
use crate::{Codec, Dialer, Encoder};

/// The message a panic in a codec or plugin ends with.
const PANICKED: &str = "a plugin or format panicked";

/// What the client edge hands its adapter, through the wire's slot.
type Writing = BoxFuture<'static, http::Response<Body>>;

/// A client's HTTP request as its adapter accepted it: an HTTP format's
/// [`Wire`](respan_gateway_core::Format::Wire), which [`Gateway::handle`]
/// takes. It carries the adapter's reply slot.
pub struct Accepted {
    head: Ask,
    frames: Frames,
    streams: bool,
    slot: oneshot::Sender<Writing>,
}

impl Accepted {
    /// A request of `head` and `body`, and what to answer it with once the
    /// gateway took it ([`Gateway::handle`]). If `streams`, its data frames
    /// are inputs as they come and the answer's head goes out once the
    /// reply resolves, as a client that switches protocols waits for it;
    /// else its body is read whole, so the caller bounds it.
    ///
    /// The answer is the response as its codec wrote it, before the
    /// adapter's rules for a hop, or plain text for a refusal no codec can
    /// write. `None` if no format took the request: [`Gateway::handle`]'s
    /// end says why. Drive `handle` to its end while awaiting it.
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

/// Implements the kernel's [`Format`](respan_gateway_core::Format) for a
/// [`Codec`](crate::Codec) through HTTP's driver: its wire is an
/// [`Accepted`](crate::Accepted) request, its dialer a
/// [`Dialer`](crate::Dialer). The codec must be `Clone`, as its edges
/// outlive the call that made them.
///
/// ```
/// # use respan_gateway_http::{Answer, Ask, Codec, Decoder, Encoder};
/// # use respan_gateway_core::Fault;
/// # use bytes::Bytes;
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
/// let gateway = respan_gateway_core::Gateway::new().serve(Echo);
/// assert!(gateway.is_ok());
/// ```
#[macro_export]
macro_rules! dialect {
    ($t:ty) => {
        impl $crate::__private::Format for $t {
            type Input = <$t as $crate::Codec>::Input;
            type Output = <$t as $crate::Codec>::Output;
            type Wire = $crate::Accepted;
            type Dialer = $crate::Dialer;

            fn name(&self) -> &str {
                $crate::Codec::name(self)
            }

            fn accepts(&self, wire: &$crate::Accepted) -> bool {
                $crate::Codec::accepts(self, wire.head())
            }

            fn is_content(&self, output: &Self::Output) -> bool {
                $crate::Codec::is_content(self, output)
            }

            fn accept(
                &self,
                wire: $crate::Accepted,
                reply: $crate::__private::BoxFuture<
                    'static,
                    $crate::__private::Reply<Self::Output>,
                >,
            ) -> $crate::__private::BoxFuture<
                'static,
                $crate::__private::Half<Self::Input>,
            > {
                $crate::accept(self, wire, reply)
            }

            fn opening(
                &self,
                origin: &$crate::__private::Origin,
            ) -> ::core::option::Option<
                ::core::result::Result<Self::Input, $crate::__private::Fault>,
            > {
                $crate::opening(self, origin)
            }

            fn dial(
                &self,
                dialer: &$crate::Dialer,
                extensions: $crate::__private::Extensions,
                relay: ::core::option::Option<&$crate::__private::Origin>,
                opening: ::core::option::Option<(&Self::Input, bool)>,
                later: ::core::option::Option<
                    $crate::__private::Events<(Self::Input, bool)>,
                >,
            ) -> ::core::result::Result<
                $crate::__private::Dialed<Self::Output>,
                $crate::__private::Fault,
            > {
                $crate::dial(self, dialer, extensions, relay, opening, later)
            }
        }
    };
}
