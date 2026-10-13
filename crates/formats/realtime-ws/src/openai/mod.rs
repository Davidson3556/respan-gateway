//! OpenAI Realtime over WebSocket.

mod input;
mod output;
mod session;
#[cfg(test)]
mod tests;

use respan_gateway_core::{BadRequest, Connection, Fault, Format};
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::{ClientIo, ProviderIo, raw::Original};
use respan_gateway_realtime::{Input, Output};
use respan_gateway_ws::WsTransport;

use crate::session::{Spec, model_at};
use crate::{SessionModel, requested_model};

const NAME: &str = SPEC.name;

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
///
/// Its GA events are the kind's where the kind has a form for them: a
/// session's configuration, audio, messages, tools' calls and answers, and
/// a response's start, content, usage and end. The rest, such as its items
/// and their content parts, stays native, so a relay to OpenAI keeps every
/// event. A session's events begin with the session its handshake asked
/// for, naming the model at `?model=`; OpenAI takes its model at connect,
/// so a session that says nothing more is no frame.
///
/// A call dials the model routing chose ([`SessionModel`](crate::SessionModel)),
/// else the one at the client's `?model=`, and its own query alone; with
/// neither it is refused, `bad_request`.
///
/// A response of another format has none of OpenAI's items: its client is
/// told of them as OpenAI does, with ids made up for them.
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

    /// `input` as its writer takes it: the fields of other formats' natives
    /// that OpenAI can do without are dropped, and one that could change
    /// the answer is refused. A plugin that adds OpenAI's own fields to an
    /// input from another format's client admits it first.
    pub fn admit(&self, input: &Input) -> Result<Input, Fault> {
        input::admit(input)
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
        let model = model_at(io.request().uri());
        let mut client = input::Client::new(model);
        let read = move |raw: Bytes, out: &mut Vec<Input>| client.read(raw, out);
        let mut client = output::Client::default();
        let write = Box::new(move |event: &Output, out: &mut Vec<Bytes>| client.write(event, out));
        Ok(SPEC.client(io, Box::new(read), write))
    }
}

impl Format<ProviderIo, Output, Input> for OpenAiRealtime {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn bind(&self, io: ProviderIo) -> Result<Connection<Output, Input>, Fault> {
        // The model routing chose, else the one the client's handshake asks for.
        let chosen = io.extensions().get::<SessionModel>().map(|m| m.0.clone());
        let asked = || requested_model(io.extensions().get::<Original>()?);
        let model = chosen.or_else(asked).ok_or_else(|| {
            BadRequest::fault(
                "OpenAI Realtime takes its model at connect: name it with ?model= on the session's URL",
            )
        })?;
        let query = Some(format!("model={model}"));
        let mut provider = input::Provider::new(model);
        let write = Box::new(move |event: &Input, out: &mut Vec<Bytes>| provider.write(event, out));
        let mut provider = output::Provider::default();
        let read = move |raw: Bytes, out: &mut Vec<Output>| provider.read(raw, out);
        Ok(SPEC.provider(io, query, write, Box::new(read), None))
    }
}
