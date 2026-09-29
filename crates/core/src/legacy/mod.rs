//! The old edge, served on the new contract (decision 0020, step 3): every
//! [`Format`] and its [`Transport`] are a [`Dialect`] and its dialer, driven
//! by the byte code the kernel had, moved here: reading a body into inputs,
//! writing a call and a response (head timing, [`Encoder::open`], framing),
//! and the byte half of the guarantees (a cut body never ends clean, an
//! encoder's panic in a body read elsewhere). [`Gateway::handle`] hands the
//! request to the kernel as a [`Wire`]. It goes with the flip (step 5).

mod accept;
mod dial;
mod format;
mod transport;

use std::any::Any;
use std::sync::Arc;

use bytes::Bytes;
use futures::FutureExt;
use futures::channel::oneshot;
use futures::future::{BoxFuture, ready};

pub use format::{Decoder, Encoder, Format};
pub use transport::{Body, Exchange, Head, Transport};

use crate::{Dialect, Dialed, Events, Extensions, Fault, Gateway, Half, Origin, Reply};

/// What `Gateway::handle` hands the adapter: the response, or a failure
/// no format can write.
pub(crate) type Handed = Result<Exchange<Head>, Fault>;

/// A provider's transport, of its format's heads: an old format's dialer.
pub(crate) type Dialer<F> =
    Arc<dyn Transport<Ask = <F as Format>::Ask, Answer = <F as Format>::Answer>>;

/// A client's request as [`Gateway::handle`] took it, and the slot the
/// client edge hands its writer to: what the adapter gets back.
pub(crate) struct Wire {
    head: Head,
    body: Body,
    streams: bool,
    slot: oneshot::Sender<BoxFuture<'static, Handed>>,
}

/// What came with a client's head, its origin: the head, which a call that
/// relays starts from, and a whole request's body, its bytes unread.
pub(crate) struct Came {
    head: Arc<dyn Any + Send + Sync>,
    body: Option<Bytes>,
}

/// An old format, as a dialect.
pub(crate) struct Legacy<F>(Arc<F>);

impl<F: Format> Legacy<F> {
    pub(crate) fn new(format: F) -> Self {
        Self(Arc::new(format))
    }
}

impl<F: Format> Dialect for Legacy<F> {
    type Input = F::Input;
    type Output = F::Output;
    type Wire = Wire;
    type Dialer = Dialer<F>;

    fn name(&self) -> &str {
        self.0.name()
    }

    fn accepts(&self, wire: &Wire) -> bool {
        (wire.head.downcast_ref()).is_some_and(|head| self.0.accepts(head))
    }

    fn is_content(&self, output: &F::Output) -> bool {
        self.0.is_content(output)
    }

    fn accept(
        &self,
        wire: Wire,
        reply: BoxFuture<'static, Reply<F::Output>>,
    ) -> BoxFuture<'static, Half<F::Input>> {
        accept::accept(self.0.clone(), wire, reply)
    }

    fn opening(&self, origin: &Origin) -> Option<Result<F::Input, Fault>> {
        accept::opening(&*self.0, origin)
    }

    fn dial(
        &self,
        dialer: &Dialer<F>,
        extensions: Extensions,
        relay: Option<&Origin>,
        opening: Option<(&F::Input, bool)>,
        later: Option<Events<(F::Input, bool)>>,
    ) -> Result<Dialed<F::Output>, Fault> {
        dial::dial(&self.0, dialer, extensions, relay, opening, later)
    }
}

/// [`Gateway::handle`]: `req` as a wire, opened, and what its client edge
/// wrote, or the end that says why no format took it.
pub(crate) async fn handle(
    gateway: &Gateway,
    req: Exchange<Head>,
) -> (Handed, BoxFuture<'static, Result<(), Fault>>) {
    let Exchange {
        head,
        body,
        extensions,
        streams,
        ..
    } = req;
    let (slot, writer) = oneshot::channel();
    let wire = Wire {
        head,
        body,
        streams,
        slot,
    };
    let ended = gateway.open_any(Box::new(wire), extensions).await;
    match writer.await {
        Ok(writer) => (writer.await, ended),
        Err(_) => {
            let fault = ended.await.err();
            let fault = fault.unwrap_or_else(|| Fault::internal("no format answered"));
            (Err(fault.clone()), ready(Err(fault)).boxed())
        }
    }
}
