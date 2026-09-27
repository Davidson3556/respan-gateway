use futures::StreamExt;
use futures::stream::{AbortHandle, Abortable, BoxStream, Stream};
use http_body_util::{BodyExt, BodyStream, StreamBody};
use std::any::TypeId;

use http::StatusCode;

use crate::context::Sent;
use crate::part::head;
use crate::provider::{CallEdge, SharedCall, lock};
use crate::{Body, Decoder, Event, Fault, Item, Part, Usage};

/// A response to send to the client: one a provider is streaming, or one a
/// plugin made. Dropping it unsent cancels it.
#[must_use = "a response does nothing unless sent or collected"]
pub struct Response {
    pub(crate) head: http::response::Parts,
    pub(crate) body: Source,
    /// Usage a plugin added, reported before the body's own.
    pub(crate) usage: Usage,
    /// The output type its items are, if known.
    pub(crate) event: Option<TypeId>,
}

/// Bytes a plugin has in the client's format, such as a cached response with
/// content its kind's events cannot hold, and the head to read them by. The
/// client's format decodes it like a provider's; it is sent as is unless an
/// edit changes it. It can be sent, not read with [`Response::collect`].
impl<B> From<http::Response<B>> for Response
where
    B: http_body::Body<Data = bytes::Bytes> + Send + 'static,
    B::Error: Into<Fault>,
{
    fn from(response: http::Response<B>) -> Self {
        let (head, body) = response.into_parts();
        Self {
            head,
            body: Source::Raw(body.map_err(Into::into).boxed_unsync()),
            usage: Usage::default(),
            event: None,
        }
    }
}

impl std::fmt::Debug for Response {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Response")
            .field("status", &self.head.status)
            .finish_non_exhaustive()
    }
}

impl Response {
    /// A response made from events of the client's kind, such as a cached
    /// answer: an `http::Response` whose body streams them
    /// (`stream::iter(events.map(Ok))` for a whole one), ending `Ok` or
    /// failed by the first `Err`. The head is the plugin's; the client's
    /// format sets the content type if it has none. Events of another type
    /// than the client format's output type fail the response with
    /// [`codes::WRONG_KIND`](crate::codes::WRONG_KIND) when it is sent.
    pub fn stream<T, S>(response: http::Response<S>) -> Self
    where
        T: Event,
        S: Stream<Item = Result<T, Fault>> + Send + 'static,
    {
        let (head, events) = response.into_parts();
        let parts = events.map(|event| match event {
            Ok(event) => Item::new([event]).into(),
            Err(fault) => Part::End(Err(fault)),
        });
        let done = futures::stream::once(async { Part::End(Ok(())) });
        Self {
            head,
            body: Source::Parts(parts.chain(done).boxed()),
            usage: Usage::default(),
            event: Some(TypeId::of::<T>()),
        }
    }

    /// With `usage` added, reported to the client's format and the `Outcome`
    /// before any the body reports. Only for usage no call produced, such
    /// as a cached answer's. Such a response is sent, not read:
    /// [`Response::collect`] refuses it, so the usage is never lost.
    pub fn with_usage(mut self, usage: Usage) -> Self {
        self.usage += &usage;
        self
    }

    /// Read it whole instead of sending it, such as a judge's verdict: its
    /// events of `T`, the kind's output type. `Err` with its fault if
    /// it ends `Err`, with [`codes::WRONG_KIND`](crate::codes::WRONG_KIND)
    /// if it is of another kind, with
    /// [`codes::CONFIG`](crate::codes::CONFIG) if it is a plugin's own
    /// bytes or was given usage ([`Response::with_usage`]), and with
    /// [`codes::INTERNAL`](crate::codes::INTERNAL) if it answers a call of
    /// a request whose plugins have answered, such as when read by another
    /// request. Its usage is the provider edge's, not the client edge's
    /// `Outcome.usage`; its call ends as it does.
    pub async fn collect<T: Event>(mut self) -> Result<Vec<T>, Fault> {
        if matches!(self.body, Source::Raw(_)) {
            return Err(plugin_bytes());
        }
        if !self.usage.is_empty() {
            return Err(Fault::config(
                "a response given usage can be sent, not read",
            ));
        }
        // Of another kind, or none, it holds no `T`s, even with no items.
        if self.event != Some(TypeId::of::<T>()) {
            return Err(Fault::wrong_kind::<T>());
        }
        if self.body.answered() {
            return Err(Fault::internal(
                "a response to another request's call was read",
            ));
        }
        let read = self.read().await;
        self.body
            .end(read.as_ref().map(|_| ()).map_err(Clone::clone));
        read
    }

    /// Its events of `T`, to its end.
    async fn read<T: Event>(&mut self) -> Result<Vec<T>, Fault> {
        let (mut events, mut parts) = (Vec::new(), Vec::new());
        loop {
            self.body.next(&mut parts).await;
            for part in parts.drain(..) {
                match part {
                    Part::Item(item) => events.extend_from_slice(item.events::<T>()?),
                    Part::End(Ok(())) => return Ok(events),
                    Part::End(Err(fault)) => return Err(fault),
                    _ => {}
                }
            }
        }
    }

    /// A refusal: the kernel's own response, from Parts.
    pub(crate) fn made(
        status: StatusCode,
        parts: impl Stream<Item = Part> + Send + 'static,
    ) -> Self {
        Self {
            head: head(status),
            body: Source::Parts(parts.boxed()),
            usage: Usage::default(),
            event: None,
        }
    }

    /// A provider's response to a call, `relayed` if in the client's format
    /// rather than translated, with the call's provider edge if it has one.
    pub(crate) fn provider(
        (head, body, decoder): Sent,
        relayed: bool,
        event: Option<TypeId>,
        call: Option<CallEdge>,
    ) -> Self {
        // A call's body is cut off at its end, such as when its request
        // closes, so a reader still waiting wakes to read it abandoned.
        let (body, call) = match call {
            Some(call) => {
                let (abort, cut) = AbortHandle::new_pair();
                let body = Abortable::new(BodyStream::new(body), cut);
                (
                    StreamBody::new(body).boxed_unsync(),
                    Some(call.share(abort)),
                )
            }
            None => (body, None),
        };
        Self {
            head,
            body: Source::Bytes {
                body,
                decoder,
                relayed,
                call,
            },
            usage: Usage::default(),
            event,
        }
    }

    /// Its status and headers, such as a provider's error status.
    pub fn head(&self) -> &http::response::Parts {
        &self.head
    }
}

pub(crate) type PartStream = BoxStream<'static, Part>;

/// A response's body: a plugin's bytes, decoded only by the client's format
/// when sent; bytes and their decoder, `relayed` (in the client's format)
/// or translated, and the provider edge of the call they answer; or Parts a
/// plugin made.
pub(crate) enum Source {
    Raw(Body),
    Bytes {
        body: Body,
        decoder: Box<dyn Decoder>,
        relayed: bool,
        call: Option<SharedCall>,
    },
    Parts(PartStream),
}

impl Source {
    /// Whether its Parts are the client format's own bytes: items that
    /// carry them and Frames, rather than items made anew.
    /// Asked once a plugin's bytes have their decoder, as relayed ones.
    pub(crate) fn relayed(&self) -> bool {
        matches!(self, Source::Bytes { relayed: true, .. })
    }

    /// Push the next Parts to `parts`. Eventually pushes a `Part::End`; the
    /// caller must not read past it.
    pub(crate) async fn next(&mut self, parts: &mut Vec<Part>) {
        let (body, decoder, call) = match self {
            Source::Bytes {
                body,
                decoder,
                call,
                ..
            } => (body, decoder, call),
            // Never read: the client's format decodes them, and `collect` refuses them.
            Source::Raw(_) => return parts.push(Part::End(Err(plugin_bytes()))),
            Source::Parts(made) => {
                let truncated = || Part::End(Err(Fault::truncated()));
                return parts.push(made.next().await.unwrap_or_else(truncated));
            }
        };
        // Decoded apart, so the call sees only its own Parts.
        let mut decoded = Vec::new();
        match body.frame().await {
            // Trailers carry nothing a format decodes yet.
            Some(Ok(frame)) => {
                if let Ok(bytes) = frame.into_data() {
                    decoder.decode(bytes, &mut decoded);
                }
            }
            // Only a decoder's own end is said already.
            Some(Err(f)) => decoded.push(Part::End(Err(f.unsaid()))),
            None => {
                decoder.finish(&mut decoded);
                // Never read after the decoder's own `Part::End`.
                decoded.push(Part::End(Err(Fault::truncated())));
            }
        }
        if let Some(call) = call {
            lock(call).parts(&mut decoded);
        }
        parts.append(&mut decoded);
    }

    /// Its reader is done with it, ending `end`: the call it answers ends
    /// so.
    pub(crate) fn end(&mut self, end: Result<(), Fault>) {
        if let Source::Bytes { call: Some(c), .. } = self {
            lock(c).finish(end);
        }
    }

    /// Whether it answers a call of a request whose plugins have answered.
    fn answered(&self) -> bool {
        matches!(self, Source::Bytes { call: Some(c), .. } if lock(c).answered())
    }
}

/// A plugin's own bytes, which the client's format decodes only when they are sent.
fn plugin_bytes() -> Fault {
    Fault::config("a plugin's own bytes can be sent, not read")
}
