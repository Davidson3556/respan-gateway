#![warn(missing_docs)]
//! The realtime kind over WebSocket: a session of JSON events, the kind's
//! `Input` in and `Output` out, as a format on the HTTP wire
//! ([`JsonEvents`]). Each spec of such a session is a value of it, with its
//! path, its subprotocol and its frames, such as
//! [`JsonEvents::OPENAI_REALTIME`].

use std::borrow::Cow;

use respan_gateway_core::futures::StreamExt;
use respan_gateway_core::{BadRequest, Connection, EventStream, Fault, Format, Upstream};
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::formats;
use respan_gateway_http::http::header::{CONTENT_TYPE, SEC_WEBSOCKET_PROTOCOL, UPGRADE};
use respan_gateway_http::http::response::Parts as ResponseHead;
use respan_gateway_http::http::{HeaderMap, HeaderValue, Method, StatusCode};
use respan_gateway_http::http::{Request, Response};
use respan_gateway_http::{self as h, Body, ClientIo, ProviderIo};
use respan_gateway_ws::WsTransport;

use respan_gateway_realtime::{Input, Output};

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
/// It speaks a subprotocol only if its spec has one, or it is told one
/// ([`protocol`](Self::protocol)). Its provider is reached by
/// [`transport`](Self::transport), set as the spec's frames need.
///
/// ```
/// # use respan_gateway_realtime_ws::JsonEvents;
/// // Served at a path ending in `/events`, and sent to `/events`.
/// let events = JsonEvents::new("events", "/events");
/// ```
#[derive(Debug, Clone)]
pub struct JsonEvents {
    name: Cow<'static, str>,
    path: Cow<'static, str>,
    protocol: Option<Cow<'static, str>>,
    binary_text: bool,
}

impl JsonEvents {
    /// OpenAI Realtime, `openai-realtime`: at `/realtime`, after the base
    /// URL its SDKs take (`wss://api.openai.com/v1`), in the subprotocol
    /// `realtime`, which its browser clients offer beside their key.
    pub const OPENAI_REALTIME: Self =
        Self::spec("openai-realtime", "/realtime", Some("realtime"), false);

    /// OpenAI's Responses over WebSocket, `responses-ws`: at `/responses`,
    /// as for HTTP. Only an upgrade is this format's: a `POST` there is no
    /// session.
    pub const RESPONSES_WS: Self = Self::spec("responses-ws", "/responses", None, false);

    /// Gemini Live, `gemini-live`: at the Live API's endpoint, after the
    /// origin Google's SDK takes (`wss://generativelanguage.googleapis.com`).
    /// Its server sends each JSON event in a binary frame.
    pub const GEMINI_LIVE: Self = Self::spec(
        "gemini-live",
        "/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent",
        None,
        true,
    );

    /// A spec's session, `name`, at `path`, in `protocol` if it has one; its
    /// provider sends JSON in binary frames if `binary_text`.
    const fn spec(
        name: &'static str,
        path: &'static str,
        protocol: Option<&'static str>,
        binary_text: bool,
    ) -> Self {
        let protocol = match protocol {
            Some(protocol) => Some(Cow::Borrowed(protocol)),
            None => None,
        };
        Self {
            name: Cow::Borrowed(name),
            path: Cow::Borrowed(path),
            protocol,
            binary_text,
        }
    }

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
    /// offers it to the provider beside the client's own offers, and a client
    /// that offered it gets it back
    /// in its `101`; a client that did not gets none. `Err` with `config`
    /// if `protocol` is no token (RFC 9110 5.6.2).
    pub fn protocol(mut self, protocol: impl Into<String>) -> Result<Self, Fault> {
        let protocol = protocol.into();
        if !respan_gateway_http::is_token(protocol.as_bytes()) {
            return Err(Fault::config("a subprotocol must be a token"));
        }
        self.protocol = Some(protocol.into());
        Ok(self)
    }

    /// Its name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The path it is served at the end of, and sent to.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// The subprotocol it speaks, if any.
    pub fn subprotocol(&self) -> Option<&str> {
        self.protocol.as_deref()
    }

    /// Whether its provider sends JSON in binary frames, which its
    /// transport takes as text.
    pub fn binary_text(&self) -> bool {
        self.binary_text
    }

    /// Its subprotocol, as a header's value.
    fn protocol_header(&self) -> Result<Option<HeaderValue>, Fault> {
        let protocol = self.protocol.as_deref().map(HeaderValue::from_str);
        let protocol = protocol.transpose();
        protocol.map_err(|_| Fault::config("a subprotocol must be a token"))
    }

    /// A transport to this format's provider at `base_url`
    /// ([`WsTransport::new`]), taking binary text if its spec sends binary
    /// frames ([`WsTransport::with_binary_text`]).
    pub fn transport(&self, base_url: impl Into<String>) -> Result<WsTransport, Fault> {
        Ok(WsTransport::new(base_url)?.with_binary_text(self.binary_text))
    }
}

impl Format<ClientIo, Input, Output> for JsonEvents {
    fn name(&self) -> &str {
        &self.name
    }

    fn matches(&self, io: &ClientIo) -> bool {
        let head = io.request();
        head.method() == Method::GET
            && respan_gateway_http::header_tokens(head.headers(), UPGRADE)
                .any(|v| v.eq_ignore_ascii_case("websocket"))
            && formats::path_ends_with(head.uri(), &self.path)
    }

    fn bind(&self, io: ClientIo) -> Result<Connection<Input, Output>, Fault> {
        let protocol = self.protocol_header()?;
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
                    let mut offers = respan_gateway_http::header_tokens(
                        reply.request().headers(),
                        SEC_WEBSOCKET_PROTOCOL,
                    );
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

impl Format<ProviderIo, Output, Input> for JsonEvents {
    fn name(&self) -> &str {
        &self.name
    }

    fn bind(&self, io: ProviderIo) -> Result<Connection<Output, Input>, Fault> {
        let name = self.name.clone();
        let refused = self.name.clone();
        let input_format = self.name.clone();
        let path = self.path.clone();
        let protocol = self.protocol_header()?;
        let wire = io.extensions().get::<h::Wire>().cloned();
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
                    let client = wire.as_ref().and_then(h::Wire::get);
                    offer_beside_the_clients(
                        &mut head.headers,
                        client.map(Request::headers),
                        &protocol,
                    )?;
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

/// Offer `protocol` in `headers` beside the subprotocols the client
/// offered, which are kept, if the client did not offer it.
fn offer_beside_the_clients(
    headers: &mut HeaderMap,
    client: Option<&HeaderMap>,
    protocol: &HeaderValue,
) -> Result<(), Fault> {
    let offered = client
        .into_iter()
        .flat_map(|client| respan_gateway_http::header_tokens(client, SEC_WEBSOCKET_PROTOCOL));
    let mut offers: Vec<&str> = offered.filter(|offer| !offer.is_empty()).collect();
    let named = protocol.to_str().unwrap_or_default();
    if !offers.contains(&named) {
        offers.push(named);
    }
    let all = HeaderValue::from_str(&offers.join(", "))
        .map_err(|_| Fault::config("a subprotocol must be a token"))?;
    headers.insert(SEC_WEBSOCKET_PROTOCOL, all);
    Ok(())
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
