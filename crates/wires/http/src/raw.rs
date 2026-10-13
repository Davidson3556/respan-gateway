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
    memo: Arc<OnceLock<Box<dyn Any + Send + Sync>>>,
    live: bool,
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
        debug.field("live", &self.live).finish()
    }
}
impl Original {
    /// Supply an owned request; `live` means its body is a handshake prefix.
    pub fn new(request: Request<Bytes>, live: bool) -> Self {
        Self {
            request: Arc::new(OnceLock::from(request)),
            memo: Arc::default(),
            live,
        }
    }
    /// A wire its request fills later, once read.
    pub(crate) fn unread(live: bool) -> Self {
        Self {
            request: Arc::default(),
            memo: Arc::default(),
            live,
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
    /// A value `make` derives from the request, computed once for every
    /// holder of this wire, such as what a format must read of the body to
    /// frame its response. The first `T` asked for is the one kept: none for
    /// another type, or while the request is unread.
    pub fn memo<T: Send + Sync + 'static>(
        &self,
        make: impl FnOnce(&Request<Bytes>) -> T,
    ) -> Option<&T> {
        let request = self.request.get()?;
        let kept = self.memo.get_or_init(|| Box::new(make(request)));
        kept.downcast_ref()
    }
    /// Whether subsequent input consists of live events.
    pub fn is_live(&self) -> bool {
        self.live
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
        self.0.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Choose `head`.
    pub(crate) fn set(&self, head: Parts) {
        *self.0.lock().unwrap_or_else(|e| e.into_inner()) = Some(head);
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
    fn debug_shows_header_names_only() {
        let mut request = Request::new(Bytes::from_static(b"body"));
        request.headers_mut().insert(
            "authorization",
            HeaderValue::from_static("Bearer sk-secret"),
        );
        *request.uri_mut() = Uri::from_static("/v1/x?key=sk-secret");
        let shown = format!("{:?}", Original::new(request, false));
        assert!(shown.contains("authorization"), "{shown}");
        assert!(!shown.contains("sk-secret"), "{shown}");
    }
}
