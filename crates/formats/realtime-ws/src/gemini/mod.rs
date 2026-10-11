//! Gemini Live over WebSocket.

use respan_gateway_core::{Connection, Fault, Format};
use respan_gateway_http::{ClientIo, ProviderIo};
use respan_gateway_realtime::{Input, Output};
use respan_gateway_ws::WsTransport;

use crate::session::{self, Spec};

const SPEC: Spec = Spec {
    name: "gemini-live",
    path: "/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent",
    protocol: None,
    binary_text: true,
};

/// Gemini Live: a `GET` upgrade to a path ending in the Live API's endpoint,
/// and a call to it after the origin Google's SDK takes
/// (`wss://generativelanguage.googleapis.com`). Its server sends each JSON
/// event in a binary frame, which its [`transport`](Self::transport) takes
/// as text.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GeminiLive;

impl GeminiLive {
    /// The format's name, `gemini-live`, which tags its native events.
    pub const NAME: &'static str = SPEC.name;
    /// The path it is served at the end of, and sent to.
    pub const PATH: &'static str = SPEC.path;

    /// A transport to its provider at `base_url` ([`WsTransport::new`]),
    /// taking binary frames as text ([`WsTransport::with_binary_text`]).
    pub fn transport(&self, base_url: impl Into<String>) -> Result<WsTransport, Fault> {
        SPEC.transport(base_url)
    }
}

impl Format<ClientIo, Input, Output> for GeminiLive {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn matches(&self, io: &ClientIo) -> bool {
        SPEC.matches(io)
    }

    fn bind(&self, io: ClientIo) -> Result<Connection<Input, Output>, Fault> {
        let (read, write) = session::native_client(Self::NAME);
        Ok(SPEC.client(io, read, write))
    }
}

impl Format<ProviderIo, Output, Input> for GeminiLive {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn bind(&self, io: ProviderIo) -> Result<Connection<Output, Input>, Fault> {
        let (write, read) = session::native_provider(Self::NAME);
        Ok(SPEC.provider(io, write, read))
    }
}
