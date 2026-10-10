//! A WebSocket session as JSON events: the kind ([`Input`] in,
//! [`Output`] out) and its format on the HTTP wire ([`JsonEvents`]).

use std::borrow::Cow;

use respan_gateway_core::futures::StreamExt;
use respan_gateway_core::{BadRequest, Connection, EventStream, Fault, Format, Upstream};
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::formats;
use respan_gateway_http::http::header::{CONTENT_TYPE, SEC_WEBSOCKET_PROTOCOL, UPGRADE};
use respan_gateway_http::http::response::Parts as ResponseHead;
use respan_gateway_http::http::{HeaderValue, Method, StatusCode};
use respan_gateway_http::http::{Request, Response};
use respan_gateway_http::{self as h, Body, Incoming, Outgoing};
use respan_gateway_realtime::{Input, Output};

use crate::WsTransport;

/// An event's `json` as its frame. An event of another format is the
/// gateway's misconfiguration, a plugin's that moved it untranslated, not
/// the client's: `config`.
fn written(format: &str, event: &respan_gateway_core::NativeValue) -> Result<Bytes, Fault> {
    if event.format.as_ref() != format {
        return Err(Fault::config(
            "a realtime event requires explicit translation to another format",
        ));
    }
    let json = &*event.value;
    serde_json::to_vec(json)
        .map(Bytes::from)
        .map_err(|e| Fault::internal("an event could not be written").with_source(e))
}

/// A refused handshake's answer: text/plain, with the fault's error code
/// ([`error_code`](respan_gateway_http::error_code)), never its message.
fn refusal(fault: &Fault, _request: &Request<Bytes>, head: &mut ResponseHead) -> Bytes {
    let text = HeaderValue::from_static("text/plain; charset=utf-8");
    head.headers.insert(CONTENT_TYPE, text);
    Bytes::from(h::error_code(fault).to_owned())
}

/// A WebSocket session of JSON text frames, one event per frame, as a
/// format of a kind of its own. It relays to a format of the same name,
/// writing each event from its parsed JSON, and reads no event's fields:
/// what an event means is for the plugins and the tests of the protocol
/// behind it.
///
/// It accepts a `GET` with `Upgrade: websocket` to a path ending in `path`;
/// a call it writes itself is such a request to `path`, relative to the
/// transport's base URL. A frame that is not JSON fails: the client's,
/// `bad_request`; the provider's, `upstream`. A handshake it refuses is
/// answered text/plain with the fault's error code, never its message.
///
/// It speaks a subprotocol only if told one ([`protocol`](Self::protocol)).
/// A vendor's own is a constant, named for it, whose provider's base URL
/// is the one the vendor's SDK takes, with a `ws` or `wss` scheme; it
/// reads no event either. Its provider is reached by
/// [`transport`](Self::transport), set as the vendor's frames need.
///
/// ```
/// # use respan_gateway_ws::JsonEvents;
/// // Served at a path ending in `/events`, and sent to `/events`.
/// let events = JsonEvents::new("events", "/events");
/// ```
#[derive(Debug, Clone)]
pub struct JsonEvents {
    name: Cow<'static, str>,
    path: Cow<'static, str>,
    protocol: Option<HeaderValue>,
    binary_text: bool,
}

impl JsonEvents {
    /// OpenAI Realtime, `openai-realtime`: served at a path ending in
    /// `/realtime` and sent there, as OpenAI's SDKs add it to their base URL
    /// (`wss://api.openai.com/v1`). It speaks the subprotocol `realtime`,
    /// which OpenAI's browser clients offer (beside their key, as
    /// `openai-insecure-api-key.<key>`), as a browser that offered one
    /// refuses a `101` that names none.
    pub const OPENAI_REALTIME: Self = Self {
        name: Cow::Borrowed("openai-realtime"),
        path: Cow::Borrowed("/realtime"),
        protocol: Some(HeaderValue::from_static("realtime")),
        binary_text: false,
    };
    /// OpenAI Responses over WebSocket, `responses-ws`: served at a path
    /// ending in `/responses` and sent there, as for HTTP, relative to
    /// OpenAI's base URL (`wss://api.openai.com/v1`). Only an upgrade is
    /// this format's: a `POST` there is no session.
    pub const RESPONSES_WS: Self = Self {
        name: Cow::Borrowed("responses-ws"),
        path: Cow::Borrowed("/responses"),
        protocol: None,
        binary_text: false,
    };
    /// Gemini Live, `gemini-live`: served at a path ending in, and sent to,
    /// `/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent`,
    /// the Live API's endpoint, relative to the origin Google's SDK takes
    /// (`wss://generativelanguage.googleapis.com`). Its server sends each
    /// JSON event in a binary frame (Google's SDK reads frames as bytes),
    /// so its [`transport`](Self::transport) takes binary text.
    pub const GEMINI_LIVE: Self = Self {
        name: Cow::Borrowed("gemini-live"),
        path: Cow::Borrowed(
            "/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent",
        ),
        protocol: None,
        binary_text: true,
    };

    /// A format called `name` (a relay needs the same name at both edges),
    /// served at a path ending in `path` and sent to it.
    pub fn new(name: impl Into<Cow<'static, str>>, path: impl Into<Cow<'static, str>>) -> Self {
        Self {
            name: name.into(),
            path: path.into(),
            protocol: None,
            binary_text: false,
        }
    }

    /// Speak the subprotocol `protocol` (RFC 6455 1.9): a call it writes
    /// offers it to the provider, and a client that offered it gets it back
    /// in its `101`; a client that did not gets none. `Err` with `config`
    /// if `protocol` is no token (RFC 9110 5.6.2).
    pub fn protocol(mut self, protocol: impl Into<String>) -> Result<Self, Fault> {
        let protocol = protocol.into();
        let value = HeaderValue::from_str(&protocol);
        let value = value.ok().filter(|_| crate::token(protocol.as_bytes()));
        let value = value.ok_or_else(|| Fault::config("a subprotocol must be a token"))?;
        self.protocol = Some(value);
        Ok(self)
    }

    /// A transport to this format's provider at `base_url`
    /// ([`WsTransport::new`]), taking binary text if its vendor sends
    /// binary frames ([`WsTransport::with_binary_text`]).
    pub fn transport(&self, base_url: impl Into<String>) -> Result<WsTransport, Fault> {
        Ok(WsTransport::new(base_url)?.with_binary_text(self.binary_text))
    }
}

impl Format<Incoming, Input, Output> for JsonEvents {
    fn name(&self) -> &str {
        &self.name
    }

    fn matches(&self, io: &Incoming) -> bool {
        let head = io.request();
        head.method() == Method::GET
            && crate::tokens(head.headers(), UPGRADE).any(|v| v.eq_ignore_ascii_case("websocket"))
            && formats::path_ends_with(head.uri(), &self.path)
    }

    fn bind(&self, io: Incoming) -> Result<Connection<Input, Output>, Fault> {
        let protocol = self.protocol.clone();
        let input_format = self.name.clone();
        let output_format = self.name.clone();
        Ok(io.bind(
            move |request| {
                h::messages(request, move |_, raw| {
                    let json = serde_json::from_slice(&raw)
                        .map_err(|_| BadRequest::fault("a client's frame is not a JSON event"))?;
                    Ok(Input::new(input_format.clone(), json))
                })
            },
            move |reply| {
                let mut head = reply.head();
                if head.status == StatusCode::OK {
                    head.status = StatusCode::SWITCHING_PROTOCOLS;
                    head.headers
                        .insert(UPGRADE, HeaderValue::from_static("websocket"));
                }
                if let Some(protocol) = protocol {
                    head.headers.remove(SEC_WEBSOCKET_PROTOCOL);
                    let mut offers =
                        crate::tokens(reply.request().headers(), SEC_WEBSOCKET_PROTOCOL);
                    if offers.any(|v| v.as_bytes() == protocol.as_bytes()) {
                        head.headers.insert(SEC_WEBSOCKET_PROTOCOL, protocol);
                    }
                }
                let write =
                    formats::each(move |event: &Output| written(&output_format, &event.event));
                Ok(reply.open(head, Bytes::new(), write))
            },
            refusal,
        ))
    }
}

impl Format<Outgoing, Output, Input> for JsonEvents {
    fn name(&self) -> &str {
        &self.name
    }

    fn bind(&self, io: Outgoing) -> Result<Connection<Output, Input>, Fault> {
        let name = self.name.clone();
        let refused = self.name.clone();
        let input_format = self.name.clone();
        let path = self.path.clone();
        let protocol = self.protocol.clone();
        // Only the handshake's 101 is a success: a 2xx is no session.
        let upgraded =
            |answer: &ResponseHead, _: &_| answer.status == StatusCode::SWITCHING_PROTOCOLS;
        Ok(io.with_success(upgraded).bind(
            move |head, input: Option<&Input>| {
                if input.is_some() {
                    return Err(Fault::config(
                        "JSON events flow in a WebSocket session, not in a call with one input",
                    ));
                }
                head.method = Method::GET;
                formats::set_path(head, &path)?;
                head.headers
                    .insert(UPGRADE, HeaderValue::from_static("websocket"));
                if let Some(protocol) = protocol {
                    head.headers.insert(SEC_WEBSOCKET_PROTOCOL, protocol);
                }
                Ok(formats::each(move |event: &Input| {
                    written(&input_format, &event.event)
                }))
            },
            move |answer, _call| response_events(&name, answer),
            move |answer| h::provider_fault(&refused, answer),
        ))
    }
}

/// The provider's events, one per frame of its upgraded session.
fn response_events(name: &str, answer: Response<Body>) -> EventStream<Output> {
    let name = name.to_owned();
    EventStream::stream(answer.into_body().map(move |raw| {
        let json = serde_json::from_slice(&raw?)
            .map_err(|_| Upstream::fault("a provider's frame is not a JSON event"))?;
        Ok(Output::new(name.clone(), json))
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_events_require_the_target_format_and_edits_do_not_mutate_clones() {
        let original = Input::new("one", serde_json::json!({"text":"original"}));
        let mut edited = original.clone();
        edited.json_mut()["text"] = "edited".into();
        assert_eq!(original.json()["text"], "original");
        assert_eq!(
            written("one", &edited.event).expect("same format"),
            Bytes::from_static(br#"{"text":"edited"}"#)
        );
        assert!(
            written("two", &edited.event)
                .expect_err("foreign event")
                .is::<respan_gateway_core::Config>()
        );
    }

    #[test]
    fn a_subprotocol_must_be_a_token() {
        for bad in ["", "a, b", "a b", "realtime\n"] {
            let refused = JsonEvents::new("e", "/e").protocol(bad);
            let refused = refused.expect_err("no token");
            assert!(refused.is::<respan_gateway_core::Config>(), "{bad:?}");
        }
        assert!(JsonEvents::new("e", "/e").protocol("v1.realtime").is_ok());
    }
}
