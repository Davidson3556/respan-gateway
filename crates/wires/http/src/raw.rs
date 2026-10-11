//! The client's request and the provider's answer as they came, shared with
//! plugins through extensions, and the [`Relay`] plugin that forwards them.

use crate::Credential;
use bytes::Bytes;
use http::{Request, Response, response::Parts};
use respan_gateway_core::{ClientContext, Fault, Plugin, async_trait};
use std::sync::{Arc, Mutex, OnceLock};

/// The client's request as it came, without its
/// [`Credential`](crate::Credential): its head, with the bytes of a whole
/// body, or none for live input, whose body is its messages. An
/// [`ClientIo`](crate::ClientIo) fills it before plugin hooks run, a live one
/// at once; a whole body that broke off or passed the limit leaves the head
/// alone, as the request is refused. Plugins copy the `Credential`
/// deliberately when constructing a relay.
#[derive(Clone)]
pub struct Wire {
    request: Arc<OnceLock<Request<Bytes>>>,
    live: bool,
}
impl std::fmt::Debug for Wire {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Header values and query strings may be keys.
        let mut debug = f.debug_struct("Wire");
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
impl Wire {
    /// Supply an owned request; `live` means its body is a handshake prefix.
    pub fn new(request: Request<Bytes>, live: bool) -> Self {
        Self {
            request: Arc::new(OnceLock::from(request)),
            live,
        }
    }
    /// A wire its request fills later, once read.
    pub(crate) fn unread(live: bool) -> Self {
        Self {
            request: Arc::default(),
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
    /// Whether subsequent input consists of live events.
    pub fn is_live(&self) -> bool {
        self.live
    }
}
/// Send this request as given. For a whole body, its bytes go out instead of
/// the call's input, so no edit to that input, by any plugin's filter, is
/// applied: to send edits, build the body from the edited input yourself.
/// For live input, its head goes out and later typed events are encoded as
/// usual, edits included. Its path is joined to the transport's base URL as
/// any call's is, so a provider that takes raw relays sits at the vendor's
/// origin.
#[derive(Clone, Debug)]
pub struct SendRaw(pub Wire);

macro_rules! head_slot {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Debug, Default)]
        pub struct $name(Arc<Mutex<Option<Parts>>>);
        impl $name {
            /// Read the currently published head.
            pub fn get(&self) -> Option<Parts> {
                self.0.lock().unwrap_or_else(|e| e.into_inner()).clone()
            }
            /// Publish or clear the head.
            pub fn set(&self, head: Option<Parts>) {
                *self.0.lock().unwrap_or_else(|e| e.into_inner()) = head;
            }
        }
    };
}
head_slot!(
    ProviderHead,
    "A per-call slot filled when the provider's head arrives."
);
head_slot!(
    ClientHead,
    "An exchange slot a plugin sets to choose the client's response head."
);

/// Original provider error data for plugin inspection; never written implicitly.
#[derive(Clone, Debug)]
pub struct ProviderError {
    /// Format that decoded the failure.
    pub format: String,
    /// Original HTTP head and error bytes.
    pub response: Response<Bytes>,
}
impl ProviderError {
    /// Preserve an HTTP error answer for a plugin.
    pub fn new(format: impl Into<String>, response: Response<Bytes>) -> Self {
        Self {
            format: format.into(),
            response,
        }
    }
}
/// Explicit permission to write this error response to the client.
#[derive(Clone, Debug)]
pub struct RawReply(pub Response<Bytes>);

/// A call's explicit permission to write its provider's error answer to the
/// client as it came ([`RawReply`]), if that provider spoke the `client`'s
/// format: the outgoing binding gives every fault of the call's answer,
/// refusal or failure, the provider's [`ProviderError`] as a `RawReply`. An
/// answer that failed under a success status goes with the fault's status.
#[derive(Clone, Debug)]
pub struct RawErrors {
    /// The client's format name.
    pub client: String,
}
impl RawErrors {
    /// `fault` with its provider's error answer to write as it came, if that
    /// provider spoke the client's format.
    pub(crate) fn apply(&self, fault: Fault) -> Fault {
        let Some(mut error) = fault.get::<ProviderError>().cloned() else {
            return fault;
        };
        if error.format != self.client {
            return fault;
        }
        if error.response.status().is_success() {
            *error.response.status_mut() = crate::status(&fault);
        }
        fault.with(RawReply(error.response))
    }
}

/// Opt into raw request, credentials, response head and error body forwarding.
/// Its [`RawErrors`], granted only on a call to a provider of the client's
/// own format, are the permission a transport reads too: on a WebSocket one
/// (`respan_gateway_ws::WsTransport`), the provider's close code goes to the
/// client as a `CloseCode`.
///
/// It sends the target call itself: it is the routing for the requests it
/// handles, answering each with its call to the target, so register no
/// other router after it.
#[derive(Debug)]
pub struct Relay {
    raw: bool,
}
impl Relay {
    /// Forward the client's original request, its key, and the provider's
    /// head and error body. A whole body goes out as the client sent it
    /// ([`SendRaw`]), unedited: an edit a plugin made to the conversation is
    /// not sent, and keeping a raw relay and such plugins apart is the
    /// composer's responsibility. The request keeps the client's path and
    /// headers as they came, so its provider sits at the vendor's origin.
    pub const RAW: Self = Self { raw: true };
    /// Encode typed input, forwarding only response heads and raw errors.
    pub const HEADS: Self = Self { raw: false };
}
#[async_trait]
impl Plugin for Relay {
    async fn on_provider(
        &self,
        px: &mut respan_gateway_core::ProviderContext,
    ) -> Result<(), Fault> {
        // A raw request, the client's key, the provider's head and its raw
        // errors (bodies and close codes alike) go only to and from a
        // provider of the client's own format.
        if px.format() != px.client().format() {
            if px.extensions().get::<SendRaw>().is_some() {
                return Err(Fault::config(format!(
                    "a raw relay from a {} client cannot go to provider '{}', which speaks {}",
                    px.client().format(),
                    px.provider(),
                    px.format()
                )));
            }
            px.extensions_mut().remove::<ProviderHead>();
            return Ok(());
        }
        let client = px.client().format().to_owned();
        px.extensions_mut().insert(RawErrors { client });
        Ok(())
    }
    async fn on_client(
        &self,
        cx: &mut ClientContext,
    ) -> Result<Option<respan_gateway_core::Response>, Fault> {
        let Some(target) = cx.target().map(str::to_owned) else {
            return Ok(None);
        };
        let mut call = cx.call(target);
        if self.raw {
            let wire = cx
                .extensions()
                .get::<Wire>()
                .ok_or_else(|| Fault::internal("missing Wire"))?;
            let (mut head, bytes) = wire
                .get()
                .ok_or_else(|| Fault::internal("missing body"))?
                .clone()
                .into_parts();
            if let Some(credential) = cx.extensions().get::<Credential>() {
                credential.restore(&mut head);
            }
            let raw = SendRaw(Wire::new(
                http::Request::from_parts(head, bytes),
                wire.is_live(),
            ));
            call = call.with_extension(raw);
        }
        let slot = ProviderHead::default();
        let mut response = call.with_extension(slot.clone()).await?;
        if let Some(client) = cx.extensions().get::<ClientHead>() {
            client.set(slot.get());
        }
        if slot.get().is_some_and(|h| {
            !h.status.is_success() && h.status != http::StatusCode::SWITCHING_PROTOCOLS
        }) {
            response.until_end().await?;
        }
        Ok(Some(response))
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
        let shown = format!("{:?}", SendRaw(Wire::new(request, false)));
        assert!(shown.contains("authorization"), "{shown}");
        assert!(!shown.contains("sk-secret"), "{shown}");
    }
}
