//! OpenAI Realtime over WebSocket.

use respan_gateway_core::{Connection, Fault, Format};
use respan_gateway_http::{ClientIo, ProviderIo};
use respan_gateway_realtime::{Input, Output};
use respan_gateway_ws::WsTransport;

use crate::session::{self, Spec};

const SPEC: Spec = Spec {
    name: "openai-realtime",
    path: "/realtime",
    protocol: Some("realtime"),
    binary_text: false,
};

/// OpenAI Realtime: a `GET` upgrade to a path ending in `/realtime`, and a
/// call to `/realtime` after the base URL its SDKs take
/// (`wss://api.openai.com/v1`). It speaks the subprotocol `realtime`, which
/// its browser clients offer beside their key: a call offers it beside the
/// client's own offers, and a client that offered it gets it back.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OpenAiRealtime;

impl OpenAiRealtime {
    /// The format's name, `openai-realtime`, which tags its native events.
    pub const NAME: &'static str = SPEC.name;
    /// The path it is served at the end of, and sent to.
    pub const PATH: &'static str = SPEC.path;

    /// A transport to its provider at `base_url` ([`WsTransport::new`]).
    pub fn transport(&self, base_url: impl Into<String>) -> Result<WsTransport, Fault> {
        SPEC.transport(base_url)
    }
}

impl Format<ClientIo, Input, Output> for OpenAiRealtime {
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

impl Format<ProviderIo, Output, Input> for OpenAiRealtime {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn bind(&self, io: ProviderIo) -> Result<Connection<Output, Input>, Fault> {
        let (write, read) = session::native_provider(Self::NAME);
        Ok(SPEC.provider(io, write, read))
    }
}
