//! Explicit HTTP data shared with plugins through extensions.
use bytes::Bytes;
use http::{Request, Response, response::Parts};
use respan_gateway_core::Fault;
use std::sync::{Arc, Mutex, OnceLock};

/// The client's request as it came, without its
/// [`Credential`](crate::Credential): its head, with the bytes of a whole
/// body, or none for live input, whose body is its messages. An
/// [`Incoming`](crate::Incoming) fills it before plugin hooks run, a live one
/// at once; a whole body that broke off or passed the limit leaves the head
/// alone, as the request is refused. Plugins copy the `Credential`
/// deliberately when constructing a relay.
#[derive(Clone, Debug)]
pub struct Wire {
    request: Arc<OnceLock<Request<Bytes>>>,
    live: bool,
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
