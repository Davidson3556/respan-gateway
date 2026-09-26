use futures::StreamExt;
use futures::stream::{BoxStream, Stream};
use http_body_util::BodyExt;
use std::any::TypeId;

use http::StatusCode;

use crate::part::head;
use crate::{Body, Decoder, Fault, Item, KindType, Part, Usage};

/// A response to send to the client: one a provider is streaming, or one a
/// plugin made. Dropping it unsent cancels it.
#[must_use = "a response does nothing unless sent or collected"]
pub struct Response {
    pub(crate) head: http::response::Parts,
    pub(crate) body: Source,
    /// Usage a plugin added, reported before the body's own.
    pub(crate) usage: Usage,
    /// The event type its items are, if known.
    event: Option<TypeId>,
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
    /// than the route's event type fail the response with
    /// [`codes::WRONG_KIND`](crate::codes::WRONG_KIND) when it is sent.
    pub fn stream<T, S>(response: http::Response<S>) -> Self
    where
        T: KindType,
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
    /// as a cached answer's.
    pub fn with_usage(mut self, usage: Usage) -> Self {
        self.usage += &usage;
        self
    }

    /// Read it whole instead of sending it, such as a judge's verdict: its
    /// events of `T`, the kind's event type. `Err` with its fault if it ends
    /// `Err`, with [`codes::WRONG_KIND`](crate::codes::WRONG_KIND) if it is
    /// of another kind, and with [`codes::CONFIG`](crate::codes::CONFIG) if
    /// it is a plugin's own bytes. Its usage is not counted in
    /// `Outcome.usage`.
    pub async fn collect<T: KindType>(mut self) -> Result<Vec<T>, Fault> {
        if matches!(self.body, Source::Raw(_)) {
            return Err(plugin_bytes());
        }
        // Of another kind, or none, it holds no `T`s, even with no items.
        if self.event != Some(TypeId::of::<T>()) {
            return Err(Fault::wrong_kind::<T>());
        }
        let mut events = Vec::new();
        loop {
            for part in self.body.next().await {
                match part {
                    Part::Item(item) => events.extend_from_slice(item.events::<T>()?),
                    Part::End(Ok(())) => return Ok(events),
                    Part::End(Err(fault)) => return Err(fault),
                    _ => {}
                }
            }
        }
    }

    /// The kernel's own response from Parts, such as a refusal.
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

    /// A provider's response, `own` if in the client's format rather than
    /// translated.
    pub(crate) fn provider(
        head: http::response::Parts,
        body: Body,
        decoder: Box<dyn Decoder>,
        own: bool,
        event: Option<TypeId>,
    ) -> Self {
        Self {
            head,
            body: Source::Bytes(body, decoder, own),
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

/// A response's body: a plugin's bytes, decoded only by the route that
/// sends them; bytes and their decoder, own (in the client's format) or
/// translated; or Parts a plugin made.
pub(crate) enum Source {
    Raw(Body),
    Bytes(Body, Box<dyn Decoder>, bool),
    Parts(PartStream),
}

impl Source {
    /// Whether its Parts are the client format's own bytes: items that
    /// carry them and Frames, rather than items made anew.
    pub(crate) fn own(&self) -> bool {
        match self {
            // Never asked of: a route gives a plugin's bytes a decoder first.
            Source::Raw(_) => true,
            Source::Bytes(_, _, own) => *own,
            Source::Parts(_) => false,
        }
    }

    /// The next Parts. Eventually yields a `Part::End`; the caller must not
    /// read past it.
    pub(crate) async fn next(&mut self) -> Vec<Part> {
        let (body, decoder) = match self {
            Source::Bytes(body, decoder, _) => (body, decoder),
            // Never read: a route decodes them, and `collect` refuses them.
            Source::Raw(_) => {
                let undecoded = plugin_bytes();
                return vec![Part::End(Err(undecoded))];
            }
            Source::Parts(parts) => {
                let truncated = || Part::End(Err(Fault::truncated()));
                return vec![parts.next().await.unwrap_or_else(truncated)];
            }
        };
        let mut parts = Vec::new();
        match body.frame().await {
            // Trailers carry nothing a format decodes yet.
            Some(Ok(frame)) => {
                if let Ok(bytes) = frame.into_data() {
                    decoder.decode(bytes, &mut parts);
                }
            }
            // Only a decoder's own end is said already.
            Some(Err(f)) => parts.push(Part::End(Err(f.unsaid()))),
            None => {
                decoder.finish(&mut parts);
                // Never read after the decoder's own `Part::End`.
                parts.push(Part::End(Err(Fault::truncated())));
            }
        }
        parts
    }
}

/// A plugin's own bytes, which a route decodes only when it sends them.
fn plugin_bytes() -> Fault {
    Fault::config("a plugin's own bytes can be sent, not read")
}
