use futures::StreamExt;
use futures::stream::{AbortHandle, Abortable, BoxStream, Stream};
use http_body_util::{BodyExt, BodyStream, StreamBody};
use std::any::TypeId;

use http::StatusCode;

use crate::context::Sent;
use crate::part::head;
use crate::provider::{CallEdge, SharedCall, lock};
use crate::{Body, Decoder, Event, Fault, Item, Part, Usage};

/// A response for the client, streaming from a provider or made by a
/// plugin. Dropping it unsent cancels it.
#[must_use = "a response does nothing unless sent or collected"]
pub struct Response {
    pub(crate) head: http::response::Parts,
    pub(crate) body: Source,
    /// Usage a plugin added, reported before the body's own.
    pub(crate) usage: Usage,
    /// Its items' output type, if known.
    pub(crate) event: Option<TypeId>,
}

/// A plugin's response in the client's format, such as a cached one with
/// content its kind cannot hold. The client's format decodes it like a
/// provider's and sends it as is unless edited. It cannot be read with
/// [`Response::collect`].
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
    /// A response made of events of the client's kind, streamed by the
    /// body of `response` (`stream::iter(events.map(Ok))` for a whole one);
    /// the first `Err` fails it. The client's format sets the content type
    /// if the head has none. Events of another type fail it with
    /// [`codes::WRONG_KIND`](crate::codes::WRONG_KIND) when sent.
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

    /// Add `usage` no call produced, such as a cached answer's, reported
    /// before the body's. [`Response::collect`] refuses such a response, so
    /// the usage is never lost.
    pub fn with_usage(mut self, usage: Usage) -> Self {
        self.usage += &usage;
        self
    }

    /// Read it whole rather than send it, such as a judge's verdict: its
    /// events of `T`, the kind's output type. `Err` with its own fault if
    /// it fails, with [`codes::WRONG_KIND`](crate::codes::WRONG_KIND) for
    /// another kind, with [`codes::CONFIG`](crate::codes::CONFIG) for a
    /// plugin's bytes or a response given usage, and with
    /// [`codes::INTERNAL`](crate::codes::INTERNAL) once its request's
    /// plugins have answered. Its call ends as it does; its usage counts on
    /// the provider edge only.
    pub async fn collect<T: Event>(mut self) -> Result<Vec<T>, Fault> {
        if matches!(self.body, Source::Raw(_)) {
            return Err(plugin_bytes());
        }
        if !self.usage.is_empty() {
            return Err(Fault::config(
                "a response given usage can be sent, not read",
            ));
        }
        // Of another kind, or none, it holds no `T`s even with no items.
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

    /// Its events of `T`, to the end.
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

    /// A kernel-made response from Parts, such as a refusal.
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

    /// A provider's response to a call, `relayed` if in the client's
    /// format, with the call's provider edge if it has one.
    pub(crate) fn provider(
        (head, body, decoder): Sent,
        relayed: bool,
        event: Option<TypeId>,
        call: Option<CallEdge>,
    ) -> Self {
        // A call's body is cut off at its end, such as when its request
        // closes, so a waiting reader wakes and reads it abandoned.
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

/// A response's body: a plugin's raw bytes, decoded only when sent; a
/// provider's bytes with their decoder and call; or Parts.
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
    /// Whether its Parts carry the client format's own bytes. Asked once a
    /// plugin's raw bytes have their decoder, as relayed ones.
    pub(crate) fn relayed(&self) -> bool {
        matches!(self, Source::Bytes { relayed: true, .. })
    }

    /// Push the next Parts to `parts`, eventually a `Part::End`; the caller
    /// must not read past it.
    pub(crate) async fn next(&mut self, parts: &mut Vec<Part>) {
        let (body, decoder, call) = match self {
            Source::Bytes {
                body,
                decoder,
                call,
                ..
            } => (body, decoder, call),
            // Unreachable: `commit` gives them a decoder, `collect` refuses.
            Source::Raw(_) => return parts.push(Part::End(Err(plugin_bytes()))),
            Source::Parts(made) => {
                let truncated = || Part::End(Err(Fault::truncated()));
                return parts.push(made.next().await.unwrap_or_else(truncated));
            }
        };
        // Decoded apart, so the call sees only these Parts.
        let mut decoded = Vec::new();
        match body.frame().await {
            // Trailers are not decoded.
            Some(Ok(frame)) => {
                if let Ok(bytes) = frame.into_data() {
                    decoder.decode(bytes, &mut decoded);
                }
            }
            // Only a decoder's own end is said already.
            Some(Err(f)) => decoded.push(Part::End(Err(f.unsaid()))),
            None => {
                decoder.finish(&mut decoded);
                // Ignored after the decoder's own `Part::End`.
                decoded.push(Part::End(Err(Fault::truncated())));
            }
        }
        if let Some(call) = call {
            lock(call).parts(&mut decoded);
        }
        parts.append(&mut decoded);
    }

    /// Its reader is done, ending `end`; the call it answers ends so.
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

/// The fault for reading a plugin's raw bytes.
fn plugin_bytes() -> Fault {
    Fault::config("a plugin's own bytes can be sent, not read")
}
