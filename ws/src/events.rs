//! A WebSocket session as JSON events: the kind ([`ClientEvent`] in,
//! [`ProviderEvent`] out) and its format on the HTTP wire ([`JsonEvents`]).

use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::formats::{self, whole};
use respan_gateway_http::http::header::{CONTENT_TYPE, SEC_WEBSOCKET_PROTOCOL, UPGRADE};
use respan_gateway_http::http::response::Parts as ResponseHead;
use respan_gateway_http::http::{HeaderMap, HeaderValue, Method, StatusCode};
use respan_gateway_http::http::{Request, Response};
use respan_gateway_http::{self as h, Body, Incoming, Outgoing};
use respan_gateway_sdk::futures::{StreamExt, stream::BoxStream};
use respan_gateway_sdk::{BadRequest, Connection, Fault, Format, Upstream};
use serde_json::{self, Value};

/// Whether `headers` ask to upgrade to a WebSocket.
fn upgrades(headers: &HeaderMap) -> bool {
    let asks = headers.get_all(UPGRADE).iter();
    asks.filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .any(|v| v.trim().eq_ignore_ascii_case("websocket"))
}

/// Whether `headers` offer the subprotocol `protocol`.
fn offers(headers: &HeaderMap, protocol: &HeaderValue) -> bool {
    let offered = headers.get_all(SEC_WEBSOCKET_PROTOCOL).iter();
    offered
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .any(|v| v.trim().as_bytes() == protocol.as_bytes())
}

macro_rules! event {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Debug, Clone, PartialEq)]
        pub struct $name {
            /// The event, written as this JSON.
            pub json: Value,
        }

        impl $name {
            /// An event of `json`.
            pub fn new(json: Value) -> Self {
                Self { json }
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

/// An event's `json` as its frame.
fn written(json: &Value) -> Result<Bytes, Fault> {
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
/// a call it writes itself is such a request to `path`. A frame that is not
/// JSON fails: the client's, `bad_request`; the provider's, `upstream`. A
/// handshake it refuses is answered text/plain with the fault's error code,
/// never its message.
///
/// It speaks a subprotocol only if told one ([`protocol`](Self::protocol)).
///
/// ```
/// # use respan_gateway_ws::JsonEvents;
/// // Served at `/v1/realtime`, and sent there too, relative to the transport.
/// let realtime = JsonEvents::new("realtime", "/v1/realtime");
/// ```
#[derive(Debug, Clone)]
pub struct JsonEvents {
    name: String,
    path: String,
    protocol: Option<HeaderValue>,
}

impl JsonEvents {
    /// A format called `name` (a relay needs the same name at both edges),
    /// served at a path ending in `path` and sent to it.
    pub fn new(name: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            path: path.into(),
            protocol: None,
        }
    }

    /// Speak the subprotocol `protocol` (RFC 6455 1.9): a call it writes
    /// offers it to the provider, and a client that offered it gets it back
    /// in its `101`; a client that did not gets none. `Err` with `config`
    /// if `protocol` is no token (RFC 9110 5.6.2).
    pub fn protocol(mut self, protocol: impl Into<String>) -> Result<Self, Fault> {
        let protocol = protocol.into();
        let token = !protocol.is_empty()
            && protocol
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b));
        let value = HeaderValue::from_str(&protocol).ok().filter(|_| token);
        let value = value.ok_or_else(|| Fault::config("a subprotocol must be a token"))?;
        self.protocol = Some(value);
        Ok(self)
    }
}

impl Format<Incoming, ClientEvent, ProviderEvent> for JsonEvents {
    fn name(&self) -> &str {
        &self.name
    }

    fn matches(&self, io: &Incoming) -> bool {
        let head = io.request();
        head.method() == Method::GET
            && upgrades(head.headers())
            && formats::path_ends_with(head.uri(), &self.path)
    }

    fn bind(&self, io: Incoming) -> Result<Connection<ClientEvent, ProviderEvent>, Fault> {
        let protocol = self.protocol.clone();
        Ok(io.bind(
            |request| {
                Ok(h::messages(request, |_, raw| {
                    let json = serde_json::from_slice(&raw)
                        .map_err(|_| BadRequest::fault("a client's frame is not a JSON event"))?;
                    Ok(ClientEvent::new(json))
                }))
            },
            move |reply, _source| {
                let mut head = reply.head();
                if head.status == StatusCode::OK {
                    head.status = StatusCode::SWITCHING_PROTOCOLS;
                    head.headers
                        .insert(UPGRADE, HeaderValue::from_static("websocket"));
                }
                if let Some(protocol) = protocol {
                    head.headers.remove(SEC_WEBSOCKET_PROTOCOL);
                    if offers(reply.request().headers(), &protocol) {
                        head.headers.insert(SEC_WEBSOCKET_PROTOCOL, protocol);
                    }
                }
                let write = formats::each(|event: &ProviderEvent| written(&event.json));
                Ok(reply.open(head, Bytes::new(), write))
            },
            refusal,
        ))
    }
}

impl Format<Outgoing, ProviderEvent, ClientEvent> for JsonEvents {
    fn name(&self) -> &str {
        &self.name
    }

    fn bind(&self, io: Outgoing) -> Result<Connection<ProviderEvent, ClientEvent>, Fault> {
        let name = self.name.clone();
        let path = self.path.clone();
        let protocol = self.protocol.clone();
        Ok(io.bind(
            move |source| {
                if source.is_single() {
                    return Err(Fault::config(
                        "JSON events flow in a WebSocket session, not in a call with one input",
                    ));
                }
                let mut head = Request::new(()).into_parts().0;
                head.method = Method::GET;
                formats::set_path(&mut head, &path)?;
                head.headers
                    .insert(UPGRADE, HeaderValue::from_static("websocket"));
                if let Some(protocol) = protocol {
                    head.headers.insert(SEC_WEBSOCKET_PROTOCOL, protocol);
                }
                let write = formats::each(|event: &ClientEvent| written(&event.json));
                let (request, sink) = h::request(source, write)?;
                Ok((Request::from_parts(head, request.into_body()), sink))
            },
            move |answer, _call| response_events(&name, answer),
            |answer, _call| answer.status == StatusCode::SWITCHING_PROTOCOLS,
        ))
    }
}

/// The provider's events, or its refusal: an answer that is no `101`. The
/// call always asks to upgrade, as it is written so ([`JsonEvents`]) or is
/// the client's, which [`Format::matches`] took for one.
fn response_events(
    name: &str,
    answer: Response<Body>,
) -> BoxStream<'static, Result<ProviderEvent, Fault>> {
    if answer.status() != StatusCode::SWITCHING_PROTOCOLS {
        return whole::refusal(name, answer);
    }
    answer
        .into_body()
        .map(move |raw| {
            let raw = raw?;
            let json = serde_json::from_slice(&raw)
                .map_err(|_| Upstream::fault("a provider's frame is not a JSON event"))?;
            Ok(ProviderEvent::new(json))
        })
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn non_101_success_is_a_refusal_with_original_answer() {
        for status in [StatusCode::OK, StatusCode::NO_CONTENT] {
            let bytes = if status == StatusCode::OK {
                Bytes::from_static(b"not upgraded")
            } else {
                Bytes::new()
            };
            let answer = Response::builder()
                .status(status)
                .header("x-provider", "original")
                .body(Body::full(bytes.clone()))
                .expect("valid HTTP head");
            let mut events = response_events("strict-json-ws", answer);
            let fault = events
                .next()
                .await
                .expect("refusal")
                .expect_err("a WebSocket call did not upgrade");
            let original = fault
                .get::<respan_gateway_http::ProviderError>()
                .map(|e| &e.response)
                .expect("original provider answer");
            assert_eq!(
                (
                    original.status(),
                    &original.headers()["x-provider"],
                    original.body()
                ),
                (status, &HeaderValue::from_static("original"), &bytes)
            );
            assert!(events.next().await.is_none());
        }
    }

    #[test]
    fn a_subprotocol_must_be_a_token() {
        for bad in ["", "a, b", "a b", "realtime\n"] {
            let refused = JsonEvents::new("e", "/e").protocol(bad);
            let refused = refused.expect_err("no token");
            assert!(refused.is::<respan_gateway_sdk::Config>(), "{bad:?}");
        }
        assert!(JsonEvents::new("e", "/e").protocol("v1.realtime").is_ok());
    }
}
