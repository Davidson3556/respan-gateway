//! The client's request and the provider's answer as they came: the request
//! for formats and plugins to read ([`Original`]), and a provider's error answer
//! to write ([`RawReply`]) on a call [`Relayed`] to a provider of the
//! client's own format.

use bytes::Bytes;
use http::{Request, Response, response::Parts};
use respan_gateway_core::Fault;
use std::any::Any;
use std::sync::{Arc, Mutex, OnceLock};

/// The client's request as it came, without its
/// [`Credential`](crate::Credential): its head, with the bytes of a whole
/// body, or none for live input, whose body is its messages. An
/// [`ClientIo`](crate::ClientIo) fills it before plugin hooks run, a live one
/// at once; a whole body that broke off or passed the limit leaves the head
/// alone, as the request is refused. Plugins copy the `Credential`
/// deliberately when constructing a relay.
#[derive(Clone)]
pub struct Original {
    request: Arc<OnceLock<Request<Bytes>>>,
    memo: Arc<Memo>,
}
impl std::fmt::Debug for Original {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Header values and query strings may be keys.
        let mut debug = f.debug_struct("Original");
        match self.request.get() {
            Some(request) => {
                let names: Vec<_> = request.headers().keys().collect();
                debug
                    .field("method", request.method())
                    .field("path", &request.uri().path())
                    .field("headers", &names)
                    .field("body_len", &request.body().len())
            }
            None => debug.field("request", &"unread"),
        };
        debug.finish()
    }
}
impl Original {
    /// The request as it came.
    pub fn new(request: Request<Bytes>) -> Self {
        Self {
            request: Arc::new(OnceLock::from(request)),
            memo: Arc::default(),
        }
    }
    /// A wire its request fills later, once read.
    pub(crate) fn unread() -> Self {
        Self {
            request: Arc::default(),
            memo: Arc::default(),
        }
    }
    /// Fill it, once.
    pub(crate) fn fill(&self, request: Request<Bytes>) {
        let _ = self.request.set(request);
    }
    /// The original head and collected bytes (empty for live input).
    pub fn get(&self) -> Option<&Request<Bytes>> {
        self.request.get()
    }
    /// A value `make` derives from the request, computed once per type `T`
    /// for every holder of this wire, such as what a format must read of
    /// the body to frame its response: the first `T` made is the one every
    /// later ask of `T` gets. `None` while the request is unread.
    pub fn memo<T: Send + Sync + 'static>(
        &self,
        make: impl FnOnce(&Request<Bytes>) -> T,
    ) -> Option<&T> {
        let request = self.request.get()?;
        Some(self.memo.get_or_init(|| make(request)))
    }
}

/// Values derived from a request, one per type: a chain of slots, each
/// filled once and never emptied, so a value lives as long as its wire.
#[derive(Default)]
struct Memo {
    value: OnceLock<Box<dyn Any + Send + Sync>>,
    next: OnceLock<Box<Memo>>,
}

impl Memo {
    /// The `T` kept, else `make`'s, kept in the first empty slot.
    fn get_or_init<T: Send + Sync + 'static>(&self, make: impl FnOnce() -> T) -> &T {
        let mut make = Some(make);
        let mut at = self;
        loop {
            let value = at.value.get_or_init(|| match make.take() {
                Some(make) => Box::new(make()),
                // `make` runs at most once, in the slot that keeps it.
                None => Box::new(()),
            });
            if let Some(value) = value.downcast_ref() {
                return value;
            }
            at = at.next.get_or_init(Box::default);
        }
    }
}

/// The client's response head, if one was chosen for it: a relayed answer's,
/// which the provider binding gives the client binding through the
/// exchange's extensions.
#[derive(Clone, Debug, Default)]
pub(crate) struct ClientHead(Arc<Mutex<Option<Parts>>>);

impl ClientHead {
    /// The head chosen, if any.
    pub(crate) fn get(&self) -> Option<Parts> {
        crate::edge::locked(&self.0).clone()
    }

    /// Choose `head`.
    pub(crate) fn set(&self, head: Parts) {
        *crate::edge::locked(&self.0) = Some(head);
    }
}

use respan_gateway_http_faults::ProviderError;

/// Explicit permission to write this error response to the client.
#[derive(Clone, Debug)]
pub struct RawReply(pub Response<Bytes>);

/// A call that relays its client to a provider of the client's own format,
/// as `respan-gateway-plugin-relay`'s `Relay` marks one. On such a call the
/// wire carries the client's own [`Credential`](crate::Credential) to a
/// transport with no key of its own (headers, query parameters and
/// subprotocol keys alike), gives the client the head of an answer that
/// succeeds, and gives every fault of the answer, refusal or failure, the
/// provider's [`ProviderError`] as a [`RawReply`] to write as it came, under
/// the fault's status if the answer said a success. A WebSocket transport
/// gives the client the provider's close code too.
#[derive(Clone, Copy, Debug, Default)]
pub struct Relayed;

impl Relayed {
    /// `fault` with its provider's error answer to write as it came.
    pub(crate) fn apply(&self, fault: Fault) -> Fault {
        let Some(ProviderError(mut response)) = fault.get::<ProviderError>().cloned() else {
            return fault;
        };
        if response.status().is_success() {
            *response.status_mut() = respan_gateway_http_faults::status(&fault);
        }
        fault.with(RawReply(response))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderValue, Uri};

    #[test]
    fn each_type_is_memoized_apart_and_once() {
        let wire = Original::new(Request::new(Bytes::from_static(b"body")));
        assert_eq!(wire.memo(|r| r.body().len()), Some(&4));
        assert_eq!(wire.memo(|_| "text"), Some(&"text"));
        assert_eq!(wire.memo(|_| 0usize), Some(&4), "kept, not made again");
        assert_eq!(wire.memo(|_| "again"), Some(&"text"));
        assert!(Original::unread().memo(|_| 1u8).is_none());
    }

    #[test]
    fn debug_shows_header_names_only() {
        let mut request = Request::new(Bytes::from_static(b"body"));
        request.headers_mut().insert(
            "authorization",
            HeaderValue::from_static("Bearer sk-secret"),
        );
        *request.uri_mut() = Uri::from_static("/v1/x?key=sk-secret");
        let shown = format!("{:?}", Original::new(request));
        assert!(shown.contains("authorization"), "{shown}");
        assert!(!shown.contains("sk-secret"), "{shown}");
    }
}
