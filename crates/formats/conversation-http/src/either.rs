//! One provider format of the conversation kind, or another for the calls
//! whose input the first refuses ([`Either`]): chosen per call from the
//! input the call actually sends, whoever sent it (a client, a tool loop
//! that added tools, a route's fallback), so no router or plugin needs to
//! know.

use std::sync::{Arc, Mutex, PoisonError};

use respan_gateway_conversation::{Input, Output};
use respan_gateway_core::futures::FutureExt;
use respan_gateway_core::futures::channel::oneshot;
use respan_gateway_core::{Connection, EventSink, Fault, Format, Provider};
use respan_gateway_http::{Connect, KeyIn, ProviderIo, Reach, Upstream};

/// Format `A`, or `B` for a call whose input `refused` says `A` refuses.
/// It speaks as `A`, by `A`'s name, and is reached as `A` is; `B` writes
/// its own path and reads its own answer, beside `A`'s base URL. A call of
/// live (streaming) inputs is always `A`'s: they are not known before it is
/// sent.
///
/// ```ignore
/// Either::new(OpenAiChat, OpenAiResponses, chat_refuses)
/// ```
#[derive(Debug, Clone, Copy)]
pub struct Either<A, B> {
    a: A,
    b: B,
    refused: fn(&Input) -> bool,
}

impl<A, B> Either<A, B> {
    /// `a`, or `b` where `refused` says `a` refuses the input.
    pub const fn new(a: A, b: B, refused: fn(&Input) -> bool) -> Self {
        Self { a, b, refused }
    }
}

/// `A`'s.
impl<A, B> Upstream for Either<A, B>
where
    A: Upstream + Format<ProviderIo, Output, Input> + Clone,
    B: Format<ProviderIo, Output, Input> + Clone,
{
    fn name(&self) -> &str {
        Upstream::name(&self.a)
    }

    fn key(&self) -> KeyIn {
        self.a.key()
    }

    fn reach(&self) -> Reach {
        self.a.reach()
    }

    fn at_origin(&self) -> bool {
        self.a.at_origin()
    }

    fn provider(&self, name: String, connect: Connect) -> Provider {
        Provider::new(name, self.clone(), move |x| connect(x))
    }
}

impl<A, B> Format<ProviderIo, Output, Input> for Either<A, B>
where
    A: Format<ProviderIo, Output, Input> + Clone,
    B: Format<ProviderIo, Output, Input> + Clone,
{
    fn name(&self) -> &str {
        self.a.name()
    }

    fn bind(&self, io: ProviderIo) -> Result<Connection<Output, Input>, Fault> {
        // The choice waits for the call's input, which the sink is prepared
        // with before anything is sent; the opening waits for the choice.
        let (opened, opening) = oneshot::channel();
        let io = Arc::new(Mutex::new(Some(io)));
        let (a, b, refused) = (self.a.clone(), self.b.clone(), self.refused);
        let sink = EventSink::preparing(move |source| {
            let io = io.lock().unwrap_or_else(PoisonError::into_inner).take();
            let io = io.ok_or_else(|| Fault::internal("a call's transport is bound once"))?;
            let to_b = source.value().and_then(Result::ok).is_some_and(refused);
            let (open, mut sink) = if to_b { b.bind(io)? } else { a.bind(io)? };
            sink.prepare(source)?;
            let _ = opened.send(open);
            Ok(sink)
        });
        let opening = async move {
            let open = opening
                .await
                .map_err(|_| Fault::internal("the call was not prepared"))?;
            open.await
        };
        Ok((opening.boxed(), sink))
    }
}
