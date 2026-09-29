//! A WebSocket session as JSON events: the kind ([`ClientEvent`] in,
//! [`ProviderEvent`] out) and its format on the HTTP wire ([`JsonEvents`]).

use std::sync::Arc;

use respan_gateway_core::Fault;
use respan_gateway_formats::relayed::Relayed;
use respan_gateway_formats::{self as formats, whole};
use respan_gateway_http::bytes::{Bytes, BytesMut};
use respan_gateway_http::http::header::{CONNECTION, UPGRADE};
use respan_gateway_http::http::{HeaderValue, Method, StatusCode};
use respan_gateway_http::{Codec, Decoder, Encoder, impl_format};
use respan_gateway_http::{RequestHead, ResponseHead};
use serde_json::{self, Value};

/// Whether `head` asks to upgrade to a WebSocket.
fn upgrades(head: &RequestHead) -> bool {
    let asks = head.headers.get_all(UPGRADE).iter();
    asks.filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .any(|v| v.trim().eq_ignore_ascii_case("websocket"))
}

macro_rules! event {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Debug, Clone, PartialEq)]
        pub struct $name {
            /// The event. Edit it in place: an unedited event is written as
            /// the bytes it came in, an edited one as this JSON.
            pub json: Value,
            origin: Relayed<$name>,
        }

        impl $name {
            /// An event of your own, which came from nowhere.
            pub fn new(json: Value) -> Self {
                Self { json, origin: Relayed::default() }
            }

            /// Decoded from `raw` by the format called `name`.
            fn decoded(name: &Arc<str>, raw: Bytes, json: Value) -> Self {
                let said = Self::new(json.clone());
                Self { json, origin: Relayed::came(name, raw, said) }
            }
        }

        impl Encoder<$name, ()> for Writes {
            fn head(&mut self, _first: Option<&$name>) {}

            fn open(&mut self, _out: &mut BytesMut) -> Result<bool, Fault> {
                match self.streams {
                    true => Ok(false),
                    false => Err(Fault::config(
                        "JSON events flow in a WebSocket session, not in a call with one input",
                    )),
                }
            }

            fn encode(&mut self, event: &$name, out: &mut BytesMut) -> Result<(), Fault> {
                match event.origin.bytes(&self.name, event) {
                    Some(bytes) => out.extend_from_slice(bytes),
                    None => {
                        let text = serde_json::to_vec(&event.json);
                        out.extend_from_slice(&text.map_err(|e| {
                            Fault::internal("an event could not be written").with_source(e)
                        })?);
                    }
                }
                Ok(())
            }

            fn end(&mut self, end: &Result<(), Fault>, _out: &mut BytesMut) -> Result<(), Fault> {
                end.clone()
            }
        }
    };
}

event!(
    /// The kind's input: one JSON event a client sent, to the provider.
    ClientEvent
);
event!(
    /// The kind's output: one JSON event the provider sent, to the client.
    ProviderEvent
);

/// Writes each event as it came, or as its JSON once edited. `streams`:
/// whether the call carries a session.
struct Writes {
    name: Arc<str>,
    streams: bool,
}

/// A WebSocket session of JSON text frames, one event per frame, as a
/// format of a kind of its own. It relays to a format of the same name,
/// byte for byte while an event is unedited, and reads no event's fields:
/// what an event means is for the plugins and the tests of the protocol
/// behind it.
///
/// It accepts a `GET` with `Upgrade: websocket` to a path ending in `path`;
/// a call it writes itself is such a request to `path`. A frame that is not
/// JSON fails: the client's, `bad_request`; the provider's, `upstream`.
///
/// ```
/// # use respan_gateway_ws::JsonEvents;
/// // Served at `/v1/realtime`, and sent there too, relative to the transport.
/// let realtime = JsonEvents::new("realtime", "/v1/realtime");
/// ```
#[derive(Debug, Clone)]
pub struct JsonEvents {
    name: Arc<str>,
    path: String,
}

impl JsonEvents {
    /// A format called `name` (a relay needs the same name at both edges),
    /// served at a path ending in `path` and sent to it.
    pub fn new(name: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            name: Arc::from(name.into()),
            path: path.into(),
        }
    }
}

impl Codec for JsonEvents {
    type Input = ClientEvent;
    type Output = ProviderEvent;

    fn name(&self) -> &str {
        &self.name
    }

    fn accepts(&self, head: &RequestHead) -> bool {
        head.method == Method::GET && upgrades(head) && head.uri.path().ends_with(&self.path)
    }

    fn request_decoder(&self, _head: &RequestHead) -> Box<dyn Decoder<ClientEvent>> {
        let name = self.name.clone();
        formats::message_decoder(move |raw: Bytes| {
            let json = serde_json::from_slice(&raw)
                .map_err(|_| Fault::bad_request("a client's frame is not a JSON event"))?;
            Ok(ClientEvent::decoded(&name, raw, json))
        })
    }

    fn request_encoder(
        &self,
        client: Option<&RequestHead>,
        _body: &mut Bytes,
        streams: bool,
    ) -> Box<dyn Encoder<ClientEvent, RequestHead>> {
        let mut head = formats::call_head(client);
        if client.is_none() {
            head.method = Method::GET;
            formats::set_path(&mut head, &self.path);
            head.headers
                .insert(UPGRADE, HeaderValue::from_static("websocket"));
            head.headers
                .insert(CONNECTION, HeaderValue::from_static("upgrade"));
        }
        if streams {
            formats::body_changed(&mut head);
        }
        let body = Writes {
            name: self.name.clone(),
            streams,
        };
        formats::with_head(head, Box::new(body))
    }

    /// A 101 to a call that asked, as events; any other answer fails the
    /// call, as its format reads it.
    fn response_decoder(
        &self,
        answer: &ResponseHead,
        call: &RequestHead,
        _sent: &Bytes,
    ) -> Box<dyn Decoder<ProviderEvent>> {
        let switched = answer.status == StatusCode::SWITCHING_PROTOCOLS && upgrades(call);
        if !switched {
            return whole::decoder(&self.name, answer, |_, _| Ok(()));
        }
        let name = self.name.clone();
        formats::message_decoder(move |raw: Bytes| {
            let json = serde_json::from_slice(&raw)
                .map_err(|_| Fault::upstream("a provider's frame is not a JSON event"))?;
            Ok(ProviderEvent::decoded(&name, raw, json))
        })
    }

    /// The provider's 101, or a fresh one; a refusal is the provider's
    /// answer as it came, else its message as plain text.
    fn response_encoder(
        &self,
        answer: Option<&ResponseHead>,
        ask: &RequestHead,
        _body: &Bytes,
    ) -> Box<dyn Encoder<ProviderEvent, ResponseHead>> {
        let mut head = formats::response_head(answer);
        if upgrades(ask) && answer.is_none() {
            head.status = StatusCode::SWITCHING_PROTOCOLS;
            head.headers
                .insert(UPGRADE, HeaderValue::from_static("websocket"));
        }
        let body = Writes {
            name: self.name.clone(),
            streams: true,
        };
        formats::with_head(head, Box::new(body))
    }
}

impl_format!(JsonEvents);
