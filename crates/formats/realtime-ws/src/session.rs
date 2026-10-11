//! What every format's session shares: its handshake at both edges, and its
//! frames read and written by the format's own codec.

use respan_gateway_core::futures::channel::mpsc;
use respan_gateway_core::futures::stream::{self, PollNext};
use respan_gateway_core::futures::{StreamExt, future};
use respan_gateway_core::{BadRequest, Connection, EventStream, Fault, NativeValue, Upstream};
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::formats;
use respan_gateway_http::http::header::{CONTENT_TYPE, SEC_WEBSOCKET_PROTOCOL, UPGRADE};
use respan_gateway_http::http::response::Parts as ResponseHead;
use respan_gateway_http::http::{HeaderMap, HeaderValue, Method, Request, StatusCode, Uri};
use respan_gateway_http::{self as h, ClientIo, Encoder, ProviderIo};
use respan_gateway_realtime::{Input, Output};
use respan_gateway_ws::WsTransport;
use serde_json::Value;

/// Reads one frame as zero or more events.
pub(crate) type Read<T> = Box<dyn FnMut(Bytes, &mut Vec<T>) -> Result<(), Fault> + Send>;

/// Where a provider's writer says what answers an input rather than a
/// frame, such as an update it need not send: its events reach the client
/// before the frames' next, until the frames end.
pub(crate) type Say = mpsc::UnboundedSender<Output>;

/// What a [`Say`] says, read beside the frames.
pub(crate) type Said = mpsc::UnboundedReceiver<Output>;

/// A channel for what a provider's writer says itself.
pub(crate) fn say() -> (Say, Said) {
    mpsc::unbounded()
}

/// What a spec says of its session.
#[derive(Debug)]
pub(crate) struct Spec {
    /// The format's name.
    pub(crate) name: &'static str,
    /// The path it is served at the end of, and sent to.
    pub(crate) path: &'static str,
    /// The subprotocol it speaks, if any.
    pub(crate) protocol: Option<&'static str>,
    /// Whether its provider sends JSON in binary frames.
    pub(crate) binary_text: bool,
}

impl Spec {
    /// Whether a client's request is an upgrade to this session.
    pub(crate) fn matches(&self, io: &ClientIo) -> bool {
        let head = io.request();
        head.method() == Method::GET
            && h::header_tokens(head.headers(), UPGRADE)
                .any(|v| v.eq_ignore_ascii_case("websocket"))
            && formats::path_ends_with(head.uri(), self.path)
    }

    /// A transport to its provider at `base_url`, taking binary text if the
    /// spec sends binary frames.
    pub(crate) fn transport(&self, base_url: impl Into<String>) -> Result<WsTransport, Fault> {
        Ok(WsTransport::new(base_url)?.with_binary_text(self.binary_text))
    }

    /// A client's session: its frames read by `read`, the provider's events
    /// written by `write`. Its `101` gives back the subprotocol if the
    /// client offered it.
    pub(crate) fn client(
        &self,
        io: ClientIo,
        read: Read<Input>,
        write: Box<dyn Encoder<Output>>,
    ) -> Connection<Input, Output> {
        let protocol = self.protocol.map(HeaderValue::from_static);
        io.bind(
            move |request| frames(request.into_body(), read),
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
                        h::header_tokens(reply.request().headers(), SEC_WEBSOCKET_PROTOCOL);
                    if offers.any(|v| v.as_bytes() == protocol.as_bytes()) {
                        head.headers.insert(SEC_WEBSOCKET_PROTOCOL, protocol);
                    }
                }
                Ok(reply.open(head, Bytes::new(), write))
            },
            refusal,
        )
    }

    /// A session with the provider: an upgrade to the spec's path and
    /// `query`, offering its subprotocol beside the client's own offers;
    /// the client's events written by `write`, the provider's frames read
    /// by `read`, and what `write` says itself, if anything, among them.
    pub(crate) fn provider(
        &self,
        io: ProviderIo,
        query: Option<String>,
        write: Box<dyn Encoder<Input>>,
        read: Read<Output>,
        said: Option<Said>,
    ) -> Connection<Output, Input> {
        let (name, path) = (self.name, self.path);
        let protocol = self.protocol;
        let wire = io.extensions().get::<h::Wire>().cloned();
        // Only the handshake's 101 is a success: a 2xx is no session.
        let upgraded =
            |answer: &ResponseHead, _: &_| answer.status == StatusCode::SWITCHING_PROTOCOLS;
        io.with_success(upgraded).bind(
            move |head, input: Option<&Input>| {
                if input.is_some() {
                    return Err(Fault::config(
                        "a session's events flow in a WebSocket session, not in a call with one input",
                    ));
                }
                head.method = Method::GET;
                let at = match &query {
                    Some(query) => format!("{path}?{query}"),
                    None => path.to_owned(),
                };
                head.uri = at.parse().map_err(|e| {
                    Fault::internal("a session's URL could not be written").with_source(e)
                })?;
                head.headers
                    .insert(UPGRADE, HeaderValue::from_static("websocket"));
                if let Some(protocol) = protocol {
                    let client = wire.as_ref().and_then(h::Wire::get);
                    offer_beside_the_clients(&mut head.headers, client.map(Request::headers), protocol)?;
                }
                Ok(write)
            },
            move |answer, _call| {
                let frames = frames(answer.into_body(), read);
                match said {
                    Some(said) => among(said, frames),
                    None => frames,
                }
            },
            move |answer| h::provider_fault(name, answer),
        )
    }
}

/// The model at `?model=` of a client's handshake, if it names one.
pub(crate) fn model_at(uri: &Uri) -> Option<String> {
    let query = uri.query()?;
    let model = query.split('&').find_map(|p| p.strip_prefix("model="))?;
    (!model.is_empty()).then(|| model.to_owned())
}

/// The model at `?model=` of the handshake of the client a call is for.
pub(crate) fn clients_model(io: &ProviderIo) -> Option<String> {
    model_at(io.extensions().get::<h::Wire>()?.get()?.uri())
}

/// The events of a session's frames, each read by `read`.
fn frames<T: Send + 'static>(body: h::Body, read: Read<T>) -> EventStream<T> {
    formats::parse(body, read, |read, raw, out| read(raw, out), |_, end, _| end)
}

/// The events of `frames`, with what was `said` first whenever there is
/// some, until the frames end.
fn among(said: Said, frames: EventStream<Output>) -> EventStream<Output> {
    let said = said.map(|event| Some(Ok(event)));
    let frames = frames.map(Some).chain(stream::once(future::ready(None)));
    let first = |_: &mut ()| PollNext::Left;
    let events = stream::select_with_strategy(said, frames, first)
        .take_while(|event| future::ready(event.is_some()))
        .filter_map(future::ready);
    EventStream::stream(events)
}

/// A refused handshake's answer: text/plain, with the fault's error code
/// ([`error_code`](h::error_code)), never its message.
fn refusal(fault: &Fault, _request: &Request<Bytes>, head: &mut ResponseHead) -> Bytes {
    let text = HeaderValue::from_static("text/plain; charset=utf-8");
    head.headers.insert(CONTENT_TYPE, text);
    Bytes::from(h::error_code(fault).to_owned())
}

/// Offer `protocol` in `headers` beside the subprotocols the client
/// offered, which are kept, if the client did not offer it.
fn offer_beside_the_clients(
    headers: &mut HeaderMap,
    client: Option<&HeaderMap>,
    protocol: &str,
) -> Result<(), Fault> {
    let offered = client
        .into_iter()
        .flat_map(|client| h::header_tokens(client, SEC_WEBSOCKET_PROTOCOL));
    let mut offers: Vec<&str> = offered.filter(|offer| !offer.is_empty()).collect();
    if !offers.contains(&protocol) {
        offers.push(protocol);
    }
    let all = HeaderValue::from_str(&offers.join(", "))
        .map_err(|_| Fault::config("a subprotocol must be a token"))?;
    headers.insert(SEC_WEBSOCKET_PROTOCOL, all);
    Ok(())
}

/// A client's session of `name`'s native events alone: each frame one
/// `Input::Native`, and only `Output::Native` of `name` written. A frame that
/// is not JSON is the client's fault, `bad_request`.
pub(crate) fn native_client(name: &'static str) -> (Read<Input>, Box<dyn Encoder<Output>>) {
    let read = move |raw: Bytes, out: &mut Vec<Input>| {
        let json: Value = serde_json::from_slice(&raw)
            .map_err(|_| BadRequest::fault("a client's frame is not a JSON event"))?;
        out.push(Input::native(name, json));
        Ok(())
    };
    let write = formats::each(move |event: &Output| match event {
        Output::Native(native) => written(name, Some(native)),
        _ => written(name, None),
    });
    (Box::new(read), write)
}

/// A provider's session of `name`'s native events alone: only
/// `Input::Native` of `name` written, and each frame one `Output::Native`. A
/// frame that is not JSON is the provider's fault, `upstream`.
pub(crate) fn native_provider(name: &'static str) -> (Box<dyn Encoder<Input>>, Read<Output>) {
    let write = formats::each(move |event: &Input| match event {
        Input::Native(native) => written(name, Some(native)),
        _ => written(name, None),
    });
    let read = move |raw: Bytes, out: &mut Vec<Output>| {
        let json: Value = serde_json::from_slice(&raw)
            .map_err(|_| Upstream::fault("a provider's frame is not a JSON event"))?;
        out.push(Output::native(name, json));
        Ok(())
    };
    (write, Box::new(read))
}

/// A native event's JSON as its frame. An event of another format, or one
/// the kind has a form for, which this format does not translate, is
/// the gateway's misconfiguration or a plugin's, not the client's: `config`.
fn written(format: &str, event: Option<&NativeValue>) -> Result<Bytes, Fault> {
    let Some(event) = event.filter(|event| event.format.as_ref() == format) else {
        return Err(Fault::config(
            "a realtime event requires explicit translation to another format",
        ));
    };
    serde_json::to_vec(&*event.value)
        .map(Bytes::from)
        .map_err(|e| Fault::internal("an event could not be written").with_source(e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_native_events_of_the_format_are_written() {
        let event = NativeValue::new("one", serde_json::json!({"text":"edited"}));
        assert_eq!(
            written("one", Some(&event)).expect("same format"),
            Bytes::from_static(br#"{"text":"edited"}"#)
        );
        for refused in [written("two", Some(&event)), written("one", None)] {
            let refused = refused.expect_err("foreign event");
            assert!(refused.is::<respan_gateway_core::Config>());
        }
    }
}
