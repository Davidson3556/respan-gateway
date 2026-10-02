//! A WebSocket session as JSON events: the kind ([`ClientEvent`] in,
//! [`ProviderEvent`] out) and its format on the HTTP wire ([`JsonEvents`]).

use std::sync::Arc;

use respan_gateway_http::bytes::{Bytes, BytesMut};
use respan_gateway_http::formats::{self as formats, whole};
use respan_gateway_http::http::header::{CONNECTION, UPGRADE};
use respan_gateway_http::http::{HeaderValue, Method, StatusCode};
use respan_gateway_http::http::{Request, Response};
use respan_gateway_http::{
    self as h, Body, Encoder, Incoming, Outgoing, http::request::Parts as RequestHead,
};
use respan_gateway_sdk::futures::{StreamExt, stream::BoxStream};
use respan_gateway_sdk::relayed::Relayed;
use respan_gateway_sdk::{Connection, Fault, Format};
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

        impl Encoder<$name> for Writes {
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

/// Writes each event as it came, or as its JSON once edited.
struct Writes {
    name: Arc<str>,
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

impl Format<Incoming, ClientEvent, ProviderEvent> for JsonEvents {
    fn name(&self) -> &str {
        &self.name
    }

    fn matches(&self, io: &Incoming) -> bool {
        let head = io.request();
        let mut path = respan_gateway_http::http::Request::new(()).into_parts().0;
        path.uri = head.uri().clone();
        head.method() == Method::GET
            && head
                .headers()
                .get_all(UPGRADE)
                .iter()
                .filter_map(|v| v.to_str().ok())
                .flat_map(|v| v.split(','))
                .any(|v| v.trim().eq_ignore_ascii_case("websocket"))
            && formats::path_ends_with(&path, &self.path)
    }

    fn bind(&self, io: Incoming) -> Result<Connection<ClientEvent, ProviderEvent>, Fault> {
        let name = self.name.clone();
        let reader_name = name.clone();
        let writer_name = name.clone();
        Ok(io.bind(
            move |request| {
                h::messages(request, move |_, raw| {
                    let json = serde_json::from_slice(&raw)
                        .map_err(|_| Fault::bad_request("a client's frame is not a JSON event"))?;
                    Ok(ClientEvent::decoded(&reader_name, raw, json))
                })
            },
            move |reply, _source| {
                let mut head = reply.head();
                let ask = reply.request();
                let asks = ask
                    .headers()
                    .get_all(UPGRADE)
                    .iter()
                    .filter_map(|v| v.to_str().ok())
                    .flat_map(|v| v.split(','))
                    .any(|v| v.trim().eq_ignore_ascii_case("websocket"));
                if asks && head.status == StatusCode::OK {
                    head.status = StatusCode::SWITCHING_PROTOCOLS;
                    head.headers
                        .insert(UPGRADE, HeaderValue::from_static("websocket"));
                }
                Ok(reply.open(head, Bytes::new(), Box::new(Writes { name: writer_name })))
            },
            h::plain_refusal,
        ))
    }
}

impl Format<Outgoing, ProviderEvent, ClientEvent> for JsonEvents {
    fn name(&self) -> &str {
        &self.name
    }

    fn bind(&self, io: Outgoing) -> Result<Connection<ProviderEvent, ClientEvent>, Fault> {
        let name = self.name.clone();
        let writer_name = name.clone();
        let reader_name = name.clone();
        let path = self.path.clone();
        Ok(io.bind(
            move |source| {
                if source.is_single() {
                    return Err(Fault::config(
                        "JSON events flow in a WebSocket session, not in a call with one input",
                    ));
                }
                let mut head = Request::new(()).into_parts().0;
                head.method = Method::GET;
                formats::set_path(&mut head, &path);
                head.headers
                    .insert(UPGRADE, HeaderValue::from_static("websocket"));
                head.headers
                    .insert(CONNECTION, HeaderValue::from_static("upgrade"));
                formats::body_changed(&mut head);
                let (request, sink) = h::request(
                    &writer_name,
                    source,
                    Box::new(Writes {
                        name: writer_name.clone(),
                    }),
                )?;
                Ok((Request::from_parts(head, request.into_body()), sink))
            },
            move |answer, sent| response_events(reader_name, answer, sent),
            |answer, call| answer.status == StatusCode::SWITCHING_PROTOCOLS && upgrades(call),
        ))
    }
}

fn response_events(
    name: Arc<str>,
    answer: Response<Body>,
    sent: Request<Bytes>,
) -> BoxStream<'static, Result<ProviderEvent, Fault>> {
    let (call, _) = sent.into_parts();
    if answer.status() != StatusCode::SWITCHING_PROTOCOLS || !upgrades(&call) {
        return whole::refusal(&name, answer);
    }
    answer
        .into_body()
        .map(move |raw| {
            let raw = raw?;
            let json = serde_json::from_slice(&raw)
                .map_err(|_| Fault::upstream("a provider's frame is not a JSON event"))?;
            Ok(ProviderEvent::decoded(&name, raw, json))
        })
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn non_101_success_is_a_refusal_with_original_answer() {
        let name: Arc<str> = Arc::from("strict-json-ws");
        for status in [StatusCode::OK, StatusCode::NO_CONTENT] {
            let sent = Request::builder()
                .header(UPGRADE, "websocket")
                .body(Bytes::new())
                .expect("valid HTTP head");
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
            let mut events = response_events(name.clone(), answer, sent);
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
}
