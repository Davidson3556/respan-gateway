//! The client edge: an axum adapter that accepts a WebSocket upgrade and
//! hands the session to the gateway.
//!
//! # How a session ends
//!
//! What the client and the provider are told, by [`serve_ws_until`] and
//! [`WsTransport`](respan_gateway_ws::WsTransport), of each way a session
//! ends:
//!
//! | The end | Provider (input) | Client (output) |
//! |---|---|---|
//! | Client sends Close | input ends clean; provider is sent Close 1000, and its end, however it comes within `close_wait`, is clean | nothing more, as its connection is closing |
//! | Client drops, no Close | input ends `Err(abandoned)`; provider is sent Close 1001 | nothing, as it is gone; request and calls end `abandoned` |
//! | Client sends a binary frame | input ends `Err(abandoned)`; provider is sent Close 1001 | Close 1003 `binary`; request ends `abandoned` |
//! | Provider sends Close 1000 (or no code) | dropped | Close 1000; ends clean |
//! | Provider sends Close with another code | dropped | an `upstream` fault with that [`ProviderClose`]: Close 1011 `upstream`, or that code if the call was relayed ([`Relay`](respan_gateway_http::relay::Relay)) |
//! | Provider drops, no Close | dropped | Close 1011 `truncated` |
//! | Provider sends a binary frame or a message over `max_message` | dropped | Close 1011 `upstream` |
//! | Provider is unreachable, or refuses the upgrade | (no session) | the client's format answers the handshake: 502, or under the provider's status |
//! | `until` completes | provider is sent Close 1001 | Close 1001 with `until`'s reason |
//! | A filter or plugin refuses an input | input ends `Err(fault)`; provider is sent Close 1011, or the fault's [`CloseCode`] | Close 1011, or the fault's [`CloseCode`], with its error code |
//! | A fault ends the response | provider is sent Close 1001 | Close 1011, or the fault's [`CloseCode`], with its error code |
//!
//! A fault's message is never sent to the client, only its error code
//! ([`error_code`]): as a Close's reason, and as the body of a handshake
//! this adapter answers itself. A `CloseCode` no Close may carry is sent
//! as 1011 ([`CloseCode::sendable`]). A provider's close reason text is not
//! relayed, nor is a client's close code or reason.
//!
//! [`ProviderClose`]: respan_gateway_ws::ProviderClose

use std::future::Future;
use std::pin::{Pin, pin};

use axum::Router;
use axum::body::Body as AxumBody;
use axum::extract::ws::{CloseFrame, Message, Utf8Bytes, WebSocket, WebSocketUpgrade};
use axum::extract::{Request, State};
use axum::response::{IntoResponse, Response};
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::http::header::{self, HeaderName, HeaderValue};
use respan_gateway_http::http::{HeaderMap, StatusCode};
use respan_gateway_http::http_body_util::BodyExt;
use respan_gateway_http::{Credential, Frames, error_code, error_head};
use respan_gateway_http::{http_body, http_body_util};
use respan_gateway_sdk::futures::channel::{mpsc, oneshot};
use respan_gateway_sdk::futures::stream::{SplitSink, SplitStream};
use respan_gateway_sdk::futures::{SinkExt, StreamExt, TryStreamExt};
#[cfg(test)]
use respan_gateway_sdk::{ErrorCode, Upstream};
use respan_gateway_sdk::{Fault, Gateway};

use respan_gateway_ws::{CloseCode, Limits};

/// A router that serves every request as a WebSocket session
/// ([`serve_ws`]). A request that is no upgrade is refused by axum. To mix
/// sessions with plain HTTP, route the sessions to [`serve_ws`] on top of
/// another router, whose fallback serves the rest:
///
/// ```ignore
/// respan_gateway_axum::router(gateway.clone()).route(
///     "/v1/realtime",
///     get(|State(gw): State<Gateway>, ws, req| async move { serve_ws(&gw, ws, req).await }),
/// )
/// ```
pub fn router(gateway: Gateway) -> Router {
    async fn session(
        State(gateway): State<Gateway>,
        ws: WebSocketUpgrade,
        req: Request,
    ) -> Response {
        serve_ws(&gateway, ws, req).await
    }
    Router::new().fallback(session).with_state(gateway)
}

/// [`serve_ws_until`] with [`Limits::default`] and no end of its own.
pub async fn serve_ws(gateway: &Gateway, ws: WebSocketUpgrade, req: Request) -> Response {
    serve_ws_until(gateway, ws, req, Limits::default(), std::future::pending()).await
}

/// Hand a client's WebSocket upgrade to `gateway`, and pump its frames both
/// ways: the HTTP adapter's [`handle`](respan_gateway_http::handle), live,
/// with the client's text frames as the request's body, one message per
/// frame of up to `limits.max_message`, and the response's body as the
/// frames sent back. The session ends as the [module](self)'s table says.
///
/// The gateway answers before the client is upgraded, so a request it
/// refuses (no format accepts it, a plugin refuses it, the provider cannot
/// be reached or refuses the upgrade) is an ordinary HTTP answer to the
/// handshake, whole and sized: 404, 401, 502... With a `101` the client is
/// upgraded with the `101`'s headers, and its subprotocol if the client
/// offered it, unless that is a key ([`Credential`]): a key is never sent
/// back.
///
/// The client's Close ends the input clean; the gateway's response is then
/// read to its end (within `limits.close_wait`) and dropped unsent, as the
/// connection is closing. A Close to the client is given
/// `limits.close_send` to leave.
///
/// The client's frames are read no faster than the provider takes them,
/// one frame ahead: while a frame waits to be taken, the next is read, so
/// its connection dropping right behind the waiting frame is seen at once.
/// Behind two or more frames not yet taken, it is seen only once all but
/// one of them are: with a provider that is not reading, the session lasts
/// until it does, or until `until` completes.
///
/// When `until` completes the session ends: the client is sent Close 1001
/// (going away) with the future's output as the reason (up to 123 bytes;
/// say why briefly, as `"draining"` or `"session_too_long"`), and the
/// response is dropped, ending its calls and request. It is how sessions of
/// hours are drained at shutdown and bounded in length, idle ones included,
/// and the adapter knows nothing of either: `until` is yours, made per
/// session, such as
///
/// ```ignore
/// serve_ws_until(&gateway, ws, req, Limits::default(), async move {
///     tokio::select! {
///         _ = stop.wait_for(|stopped| *stopped) => "draining",
///         _ = tokio::time::sleep(max_length) => "session_too_long",
///     }
/// })
/// ```
///
/// A session that has ended by itself, or by the client's Close, ignores it.
/// Only HTTP/1.1 upgrades, which is what `WebSocketUpgrade` accepts of a
/// `GET` with `Upgrade: websocket`.
pub async fn serve_ws_until(
    gateway: &Gateway,
    ws: WebSocketUpgrade,
    req: Request,
    limits: Limits,
    until: impl Future<Output = &'static str> + Send + 'static,
) -> Response {
    let offered = protocols(req.headers());
    let (input, frames) = mpsc::channel::<Result<Bytes, Fault>>(0);
    let req = req.map(|_| {
        let frames = frames.map_ok(http_body::Frame::data);
        http_body_util::StreamBody::new(frames).boxed_unsync()
    });
    let (resp, _ended) = respan_gateway_http::handle(gateway, req, true, limits.max_message).await;
    let (head, body) = resp.into_parts();
    if head.status != StatusCode::SWITCHING_PROTOCOLS {
        return refusal(head, body).await;
    }
    let mut ws = ws
        .max_message_size(limits.max_message)
        .max_frame_size(limits.max_message);
    let chosen = head.headers.get(header::SEC_WEBSOCKET_PROTOCOL);
    if let Some(chosen) = chosen.and_then(|v| v.to_str().ok())
        && offered.iter().any(|offer| offer == chosen)
    {
        ws = ws.protocols([chosen.to_owned()]);
    }
    let mut upgraded = ws
        .on_upgrade(move |socket| {
            pump(
                socket,
                input,
                body.into_data_stream().boxed(),
                Box::pin(until),
                limits,
            )
        })
        .into_response();
    // The provider's other headers go with the upgrade; the handshake's are axum's.
    let own = |name: &HeaderName| {
        name == header::CONNECTION
            || name == header::UPGRADE
            || name.as_str().starts_with("sec-websocket-")
    };
    for (name, value) in head.headers.iter().filter(|(n, _)| !own(n)) {
        upgraded.headers_mut().append(name, value.clone());
    }
    upgraded
}

/// The subprotocols a client offered that are no key ([`Credential`]).
fn protocols(headers: &HeaderMap) -> Vec<String> {
    let mut head = respan_gateway_http::http::Request::new(()).into_parts().0;
    for value in headers.get_all(header::SEC_WEBSOCKET_PROTOCOL) {
        head.headers
            .append(header::SEC_WEBSOCKET_PROTOCOL, value.clone());
    }
    Credential::lift(&mut head);
    let offered = head.headers.get_all(header::SEC_WEBSOCKET_PROTOCOL).iter();
    offered
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(|v| v.trim().to_owned())
        .collect()
}

/// An answer that is no upgrade, whole and sized. A failed handshake is a
/// complete answer for clients (tungstenite) that do not decode a chunked
/// body, which a streamed one would be. A body that fails is written as
/// text under the fault's status, with its error code.
async fn refusal(
    head: respan_gateway_http::http::response::Parts,
    body: respan_gateway_http::Body,
) -> Response {
    match BodyExt::collect(body).await {
        Ok(all) => Response::from_parts(head, AxumBody::from(all.to_bytes())),
        Err(fault) => {
            let mut head = Response::new(()).into_parts().0;
            error_head(&mut head, &fault);
            let text = HeaderValue::from_static("text/plain; charset=utf-8");
            head.headers.insert(header::CONTENT_TYPE, text);
            Response::from_parts(head, AxumBody::from(error_code(&fault).to_owned()))
        }
    }
}

/// How the client's side ended.
enum Heard {
    /// The client sent Close: the input ended clean.
    Closed,
    /// The connection dropped with no Close.
    Dropped,
    /// A binary frame.
    Binary,
    /// The gateway stopped reading the input (its request ended).
    Ignored,
}

/// When the session must end, with why.
type Until = Pin<Box<dyn Future<Output = &'static str> + Send>>;

async fn pump(
    socket: WebSocket,
    input: mpsc::Sender<Result<Bytes, Fault>>,
    output: Frames,
    until: Until,
    limits: Limits,
) {
    let (sink, stream) = socket.split();
    let (tell, told) = oneshot::channel();
    let mut heard = pin!(hear(stream, input, tell));
    let mut wrote = pin!(write(sink, output, told, until, limits.close_send));
    tokio::select! {
        () = &mut wrote => {}
        heard = &mut heard => match heard {
            // The connection is gone: drop the response.
            Heard::Dropped => {}
            // The client closed: the response is read to its end, unsent.
            Heard::Closed | Heard::Binary => {
                let _ = tokio::time::timeout(limits.close_wait, wrote).await;
            }
            Heard::Ignored => wrote.await,
        },
    }
}

/// A message of the client's: a text frame, one to skip, or how its side
/// ended.
enum Step {
    Text(Bytes),
    Skip,
    Ended(Heard),
}

fn step(message: Option<Result<Message, axum::Error>>) -> Step {
    match message {
        Some(Ok(Message::Text(text))) => Step::Text(Bytes::copy_from_slice(text.as_bytes())),
        Some(Ok(Message::Close(_))) => Step::Ended(Heard::Closed),
        Some(Ok(Message::Binary(_))) => Step::Ended(Heard::Binary),
        Some(Ok(_)) => Step::Skip,
        Some(Err(_)) | None => Step::Ended(Heard::Dropped),
    }
}

/// The client's text frames into `input`, then how the client's side ended.
///
/// What the client did, a Close or a binary frame, is told to the writer
/// (`tell`) before the input ends with it, as the writer must say why the
/// response fails.
///
/// While a frame waits to be sent (the gateway reads `input` as slowly as
/// its wraps do), the socket is still read, so a client that leaves is seen
/// at once, not after the frames before it: one more text frame is held as
/// look-ahead, and a Close or binary frame is acted on once the frames
/// heard are sent, as it came after them.
async fn hear(
    mut stream: SplitStream<WebSocket>,
    mut input: mpsc::Sender<Result<Bytes, Fault>>,
    tell: oneshot::Sender<Heard>,
) -> Heard {
    // Ends the input while a send holds `input`.
    let ender = input.clone();
    let finish = |heard: Heard, tell: oneshot::Sender<Heard>| match heard {
        Heard::Closed => {
            let _ = tell.send(Heard::Closed);
            Heard::Closed
        }
        Heard::Binary => {
            let _ = tell.send(Heard::Binary);
            // The client's own end, which no one waits for: 1003 says why.
            end_input(&ender);
            Heard::Binary
        }
        other => {
            end_input(&ender);
            other
        }
    };
    let (mut held, mut ended) = (None, None);
    loop {
        let frame = match held.take() {
            Some(frame) => frame,
            None => match ended.take() {
                Some(ended) => return finish(ended, tell),
                None => match step(stream.next().await) {
                    Step::Text(frame) => frame,
                    Step::Skip => continue,
                    Step::Ended(ended) => return finish(ended, tell),
                },
            },
        };
        let mut send = pin!(input.send(Ok(frame)));
        loop {
            tokio::select! {
                sent = &mut send => match sent {
                    Ok(()) => break,
                    Err(_) => return Heard::Ignored,
                },
                message = stream.next(), if held.is_none() && ended.is_none() => {
                    match step(message) {
                        Step::Text(frame) => held = Some(frame),
                        Step::Skip => {}
                        Step::Ended(Heard::Dropped) => return finish(Heard::Dropped, tell),
                        Step::Ended(other) => ended = Some(other),
                    }
                }
            }
        }
    }
}

/// End `input` with `abandoned`, at once: a fresh sender always has a slot,
/// so this never waits for the gateway to read past what is still pending
/// (an input wrap's wait), which would keep the session alive, and its
/// provider fed, after the client left.
fn end_input(input: &mpsc::Sender<Result<Bytes, Fault>>) {
    let _ = input.clone().try_send(Err(Fault::abandoned()));
}

/// The response's frames as text messages, then a Close: 1000 for a clean
/// end, 1001 when `until` completes, else the fault's. After the client's
/// Close (`told`), nothing is sent, as the connection is closing, and the
/// response is read to its end.
async fn write(
    mut sink: SplitSink<WebSocket, Message>,
    mut output: Frames,
    mut told: oneshot::Receiver<Heard>,
    mut until: Until,
    close_send: std::time::Duration,
) {
    let mut closed = false;
    let frame = loop {
        let next = tokio::select! {
            // What the client did comes first: it is why the response fails.
            biased;
            Ok(told) = &mut told => {
                match told {
                    Heard::Binary => return close(&mut sink, 1003, "binary", close_send).await,
                    _ => closed = true,
                }
                continue;
            }
            reason = &mut until => break (1001, clipped(reason.to_owned())),
            next = output.next() => next,
        };
        match next {
            Some(Ok(bytes)) if !closed => {
                let Ok(text) = Utf8Bytes::try_from(bytes) else {
                    break (1011, "internal".to_owned());
                };
                // A client that stopped reading blocks the send: `until` still ends it.
                tokio::select! {
                    sent = sink.send(Message::Text(text)) => {
                        if sent.is_err() {
                            return;
                        }
                    }
                    reason = &mut until => break (1001, clipped(reason.to_owned())),
                }
            }
            Some(Ok(_)) => {}
            Some(Err(fault)) => break failure(&fault),
            None => break (1000, String::new()),
        }
    };
    if !closed {
        close(&mut sink, frame.0, &frame.1, close_send).await;
    }
}

/// The close code and reason for a fault: its code as the reason, never its
/// message.
fn failure(fault: &Fault) -> (u16, String) {
    let code = fault.get::<CloseCode>().map_or(1011, |c| c.sendable());
    (code, clipped(error_code(fault).to_owned()))
}

/// `reason` cut to the 123 bytes a Close's reason takes.
fn clipped(mut reason: String) -> String {
    while reason.len() > 123 {
        reason.pop();
    }
    reason
}

/// Send Close `code` with `reason` within `wait`.
async fn close(
    sink: &mut SplitSink<WebSocket, Message>,
    code: u16,
    reason: &str,
    wait: std::time::Duration,
) {
    let frame = CloseFrame {
        code,
        reason: Utf8Bytes::from(reason.to_owned()),
    };
    let _ = tokio::time::timeout(wait, async {
        let _ = sink.send(Message::Close(Some(frame))).await;
        let _ = sink.close().await;
    })
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fault_closes_with_1011_or_its_code_and_says_only_its_code() {
        let plain = Upstream::fault("secret provider detail");
        assert_eq!(failure(&plain), (1011, "upstream".to_owned()));
        let asked = Fault::new("x")
            .with(ErrorCode("over_limit".into()))
            .with(CloseCode(1008));
        assert_eq!(failure(&asked), (1008, "over_limit".to_owned()));
        // A code no Close may carry is 1011.
        for bad in [1005, 1006, 1015, 999, 5000] {
            assert_eq!(
                failure(&plain.clone().with(CloseCode(bad))).0,
                1011,
                "{bad}"
            );
        }
        assert_eq!(failure(&plain.with(CloseCode(4001))).0, 4001);
    }
}
