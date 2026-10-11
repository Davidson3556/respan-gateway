//! Gemini Live over WebSocket.

mod input;
mod output;
mod setup;
#[cfg(test)]
mod tests;

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use respan_gateway_core::{Connection, Fault, Format};
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::{ClientIo, ProviderIo, formats};
use respan_gateway_realtime::{Input, Output, Session};
use respan_gateway_ws::WsTransport;

use crate::session::Spec;

const NAME: &str = SPEC.name;

/// The setup a session's provider took, which its encoder wrote and its
/// decoder says took effect.
type Applied = Arc<Mutex<Option<Session>>>;

fn lock(applied: &Applied) -> MutexGuard<'_, Option<Session>> {
    applied.lock().unwrap_or_else(PoisonError::into_inner)
}

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
///
/// Its messages are the kind's events where the kind has a form for them:
/// the `setup` (its model without `models/`), audio, text, turns, tools'
/// calls and answers, transcripts, interruptions and usage. A client's
/// `setup` comes first and once, and each of its messages holds one field,
/// as the Live API takes them; else the session closes 1007. What the kind
/// has no form for, such as a session's resumption or a warning that it
/// ends, stays native, so a relay to Gemini keeps what every message says.
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
        let mut client = input::Client::default();
        let read = move |raw: Bytes, out: &mut Vec<Input>| client.read(raw, out);
        let mut client = output::Client::default();
        let write = formats::each_message(move |event: &Output, out: &mut Vec<Bytes>| {
            client.write(event, out)
        });
        Ok(SPEC.client(io, Box::new(read), write))
    }
}

impl Format<ProviderIo, Output, Input> for GeminiLive {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn bind(&self, io: ProviderIo) -> Result<Connection<Output, Input>, Fault> {
        let applied = Applied::default();
        let mut provider = input::Provider::new(applied.clone());
        let write = formats::each_message(move |event: &Input, out: &mut Vec<Bytes>| {
            provider.write(event, out)
        });
        let mut provider = output::Provider::new(applied);
        let read = move |raw: Bytes, out: &mut Vec<Output>| provider.read(raw, out);
        Ok(SPEC.provider(io, write, Box::new(read)))
    }
}
