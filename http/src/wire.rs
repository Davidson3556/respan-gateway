//! Explicit HTTP data shared with plugins through extensions.
use bytes::Bytes;
use http::{Request, Response, response::Parts};
use std::sync::{Arc, Mutex, OnceLock};

/// Raw HTTP request data. The adapter publishes the original request without
/// credentials; plugins copy `Credential` deliberately when constructing a relay.
/// Whole bodies are available before hooks run; live requests contain their head.
#[derive(Clone, Debug, Default)]
pub struct Wire {
    pub(crate) request: Arc<OnceLock<Request<Bytes>>>,
    pub(crate) live: bool,
}
impl Wire {
    /// Supply an owned request; `live` means its body is a handshake prefix.
    pub fn new(request: Request<Bytes>, live: bool) -> Self {
        Self {
            request: Arc::new(OnceLock::from(request)),
            live,
        }
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
/// Send this request explicitly, bypassing encoding for a whole body.
/// For live input, use this head and encode subsequent typed events normally.
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
