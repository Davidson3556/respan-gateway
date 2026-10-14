//! One provider format, or another for the calls whose input it refuses:
//! chosen per call from the input the call actually sends, whoever sent it
//! (a client, a tool loop that added tools, a route's fallback), so no
//! router or plugin needs to know.

use std::sync::{Arc, Mutex, PoisonError};

use respan_gateway_core::futures::FutureExt;
use respan_gateway_core::futures::channel::oneshot;
use respan_gateway_core::{Connection, EventSink, Fault, Format};
use respan_gateway_http::ProviderIo;

/// Format `A`, or `B` for a call whose input `refused` says `A` refuses.
/// It speaks as `A`, by `A`'s name; `B` writes its own path and reads its
/// own answer, of the same kind. A call of live (streaming) inputs is
/// always `A`'s: they are not known before it is sent.
///
/// ```ignore
/// &Speaks::http(Either::new(OpenAiChat, OpenAiResponses, chat_refuses))
/// ```
#[derive(Debug, Clone)]
pub struct Either<A, B, I> {
    a: A,
    b: B,
    refused: fn(&I) -> bool,
}

impl<A, B, I> Either<A, B, I> {
    /// `a`, or `b` where `refused` says `a` refuses the input.
    pub const fn new(a: A, b: B, refused: fn(&I) -> bool) -> Self {
        Self { a, b, refused }
    }
}

impl<A, B, I, O> Format<ProviderIo, O, I> for Either<A, B, I>
where
    A: Format<ProviderIo, O, I> + Clone,
    B: Format<ProviderIo, O, I> + Clone,
    I: Send + 'static,
    O: Send + 'static,
{
    fn name(&self) -> &str {
        self.a.name()
    }

    fn bind(&self, io: ProviderIo) -> Result<Connection<O, I>, Fault> {
        // The choice waits for the call's input, which the sink is prepared
        // with before anything is sent; the opening waits for the choice.
        let (opened, opening) = oneshot::channel();
        let io = Arc::new(Mutex::new(Some(io)));
        let (a, b, refused) = (self.a_bind(), self.b_bind(), self.refused);
        let sink = EventSink::preparing(move |source| {
            let io = io.lock().unwrap_or_else(PoisonError::into_inner).take();
            let io = io.ok_or_else(|| Fault::internal("a call's transport is bound once"))?;
            let to_b = source.value().and_then(Result::ok).is_some_and(refused);
            let (open, mut sink) = if to_b { b(io)? } else { a(io)? };
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

type Bind<O, I> = Box<dyn FnOnce(ProviderIo) -> Result<Connection<O, I>, Fault> + Send>;

impl<A, B, I> Either<A, B, I> {
    /// A binding of `a`, for the call to make once it is chosen.
    fn a_bind<O>(&self) -> Bind<O, I>
    where
        A: Format<ProviderIo, O, I> + Clone,
    {
        let a = self.a.clone();
        Box::new(move |io| a.bind(io))
    }

    fn b_bind<O>(&self) -> Bind<O, I>
    where
        B: Format<ProviderIo, O, I> + Clone,
    {
        let b = self.b.clone();
        Box::new(move |io| b.bind(io))
    }
}
