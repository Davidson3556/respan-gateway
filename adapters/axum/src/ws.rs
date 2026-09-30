//! The client edge: an axum adapter that accepts a WebSocket upgrade and
//! hands the session to the gateway.

use std::future::Future;
use std::pin::{Pin, pin};
use std::time::Duration;

use axum::Router;
use axum::body::Body as AxumBody;
use axum::extract::ws::{CloseFrame, Message, Utf8Bytes, WebSocket, WebSocketUpgrade};
use axum::extract::{Request, State};
use axum::response::{IntoResponse, Response};
use respan_gateway_core::futures::channel::{mpsc, oneshot};
use respan_gateway_core::futures::stream::{SplitSink, SplitStream};
use respan_gateway_core::futures::{SinkExt, StreamExt, TryStreamExt};
use respan_gateway_core::{Fault, Gateway};
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::http::StatusCode;
use respan_gateway_http::http::header::{self, HeaderName};
use respan_gateway_http::http_body_util::BodyExt;
use respan_gateway_http::{body_of, http_body, http_body_util};

use respan_gateway_http::Frames;
use respan_gateway_ws::{CLOSE_WAIT, CloseCode, MAX_MESSAGE};

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

/// Hand a client's WebSocket upgrade to `gateway`, and pump its frames both
/// ways: the HTTP adapter's [`handle`](respan_gateway_http::handle), asked
/// to stream, with the client's text frames as the request's body, one frame
/// per item, and the response's body as the frames sent back.
///
/// The gateway answers before the client is upgraded, so a request it
/// refuses (no format accepts it, a plugin refuses it, the provider cannot
/// be reached or refuses the upgrade) is an ordinary HTTP answer to the
/// handshake: 404, 401, 502... With a `101` the client is upgraded, and:
///
/// - the client's Close ends the input clean; the gateway's response is then
///   read to its end (within [`CLOSE_WAIT`]) and dropped unsent, as the
///   connection is closed;
/// - the client's connection dropping with no Close ends the input with an
///   `Err`, and drops the response: its calls and request end `abandoned`;
/// - a binary frame from the client ends the input with an `Err` and
///   closes the connection with 1003;
/// - the response ending clean closes with 1000; failing, with 1011 (or the
///   fault's [`CloseCode`]) and the fault's code as the reason, never its
///   message.
///
/// The client's frames are read no faster than the provider takes them,
/// one frame ahead: while a frame waits to be taken, the next is read, so
/// its connection dropping right behind the waiting frame is seen at once.
/// Behind two or more frames not yet taken, it is seen only once all but
/// one of them are: with a provider that is not reading, the session lasts
/// until it does, or until [`serve_ws_until`]'s future completes.
///
/// Messages up to [`MAX_MESSAGE`]. Only HTTP/1.1 upgrades, which is what
/// `WebSocketUpgrade` accepts of a `GET` with `Upgrade: websocket`.
pub async fn serve_ws(gateway: &Gateway, ws: WebSocketUpgrade, req: Request) -> Response {
    serve_ws_until(gateway, ws, req, std::future::pending()).await
}

/// [`serve_ws`], and when `until` completes the session ends: the client is
/// sent Close 1001 (going away) with the future's output as the reason (up to
/// 123 bytes; say why briefly, as `"draining"` or `"session_too_long"`), and
/// the response is dropped, ending its calls and request. It is
/// how sessions of hours are drained at shutdown and bounded in length, idle
/// ones included, and the adapter knows nothing of either: `until` is yours,
/// made per session, such as
///
/// ```ignore
/// serve_ws_until(&gateway, ws, req, async move {
///     tokio::select! {
///         _ = stop.wait_for(|stopped| *stopped) => "draining",
///         _ = tokio::time::sleep(max_length) => "session_too_long",
///     }
/// })
/// ```
///
/// A session that has ended by itself, or by the client's Close, ignores it.
pub async fn serve_ws_until(
    gateway: &Gateway,
    ws: WebSocketUpgrade,
    req: Request,
    until: impl Future<Output = &'static str> + Send + 'static,
) -> Response {
    let (input, frames) = mpsc::channel::<Result<Bytes, Fault>>(0);
    let req = req.map(|_| {
        let frames = frames.map_ok(http_body::Frame::data);
        http_body_util::StreamBody::new(frames).boxed_unsync()
    });
    let (resp, _ended) = respan_gateway_http::handle(gateway, req, true).await;
    let (head, body) = resp.into_parts();
    if head.status != StatusCode::SWITCHING_PROTOCOLS {
        return refusal(head, body).await;
    }
    let mut ws = ws.max_message_size(MAX_MESSAGE).max_frame_size(MAX_MESSAGE);
    if let Some(chosen) = head.headers.get(header::SEC_WEBSOCKET_PROTOCOL) {
        ws = ws.protocols([chosen.to_str().unwrap_or_default().to_owned()]);
    }
    let mut upgraded = ws
        .on_upgrade(move |socket| pump(socket, input, body_of(body), Box::pin(until)))
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

/// An answer that is no upgrade, whole and sized. A failed handshake is a
/// complete answer for clients (tungstenite) that do not decode a chunked
/// body, which a streamed one would be. A body that fails is written as
/// text under the fault's status.
async fn refusal(
    head: respan_gateway_http::http::response::Parts,
    body: respan_gateway_http::Body,
) -> Response {
    match body.collect().await {
        Ok(all) => Response::from_parts(head, AxumBody::from(all.to_bytes())),
        Err(fault) => {
            let (mut head, body) = respan_gateway_http::plain(&fault).into_parts();
            respan_gateway_http::error_head(&mut head, &fault);
            Response::from_parts(head, AxumBody::new(body))
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

/// What the writer is told of the client's side, if it did not drop.
enum Told {
    Closed,
    Binary,
}

/// When the session must end, with why.
type Until = Pin<Box<dyn Future<Output = &'static str> + Send>>;

async fn pump(
    socket: WebSocket,
    input: mpsc::Sender<Result<Bytes, Fault>>,
    output: Frames,
    until: Until,
) {
    let (sink, stream) = socket.split();
    let (tell, told) = oneshot::channel();
    let mut heard = pin!(hear(stream, input, tell));
    let mut wrote = pin!(write(sink, output, told, until));
    tokio::select! {
        () = &mut wrote => {}
        heard = &mut heard => match heard {
            // The connection is gone: drop the response.
            Heard::Dropped => {}
            // The client closed: the response is read to its end, unsent.
            Heard::Closed | Heard::Binary => {
                let _ = tokio::time::timeout(CLOSE_WAIT, wrote).await;
            }
            Heard::Ignored => wrote.await,
        },
    }
}

/// How the client's side ended, before it is told.
enum Ended {
    Closed,
    Binary,
    /// The connection dropped with no Close.
    Gone,
}

/// A message of the client's: a text frame, one to skip, or its end.
enum Step {
    Text(Bytes),
    Skip,
    Ended(Ended),
}

fn step(message: Option<Result<Message, axum::Error>>) -> Step {
    match message {
        Some(Ok(Message::Text(text))) => Step::Text(Bytes::copy_from_slice(text.as_bytes())),
        Some(Ok(Message::Close(_))) => Step::Ended(Ended::Closed),
        Some(Ok(Message::Binary(_))) => Step::Ended(Ended::Binary),
        Some(Ok(_)) => Step::Skip,
        Some(Err(_)) | None => Step::Ended(Ended::Gone),
    }
}

/// The client's text frames into `input`, then how the client's side ended.
///
/// What the client did is told to the writer (`tell`) before the input
/// ends with it, as the writer must say why the response fails.
///
/// While a frame waits to be sent (the gateway reads `input` as slowly as
/// its wraps do), the socket is still read, so a client that leaves is seen
/// at once, not after the frames before it: one more text frame is held as
/// look-ahead, and a Close or binary frame is acted on once the frames
/// heard are sent, as it came after them.
async fn hear(
    mut stream: SplitStream<WebSocket>,
    mut input: mpsc::Sender<Result<Bytes, Fault>>,
    tell: oneshot::Sender<Told>,
) -> Heard {
    // Ends the input while a send holds `input`.
    let ender = input.clone();
    let finish = |ended: Ended, tell: oneshot::Sender<Told>| match ended {
        Ended::Closed => {
            let _ = tell.send(Told::Closed);
            Heard::Closed
        }
        Ended::Binary => {
            let _ = tell.send(Told::Binary);
            // The client's own end, which no one waits for: 1003 says why.
            end_input(&ender);
            Heard::Binary
        }
        Ended::Gone => {
            end_input(&ender);
            Heard::Dropped
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
                        Step::Ended(Ended::Gone) => return finish(Ended::Gone, tell),
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
/// end, 1001 when `until` completes, else the fault's. After the client's Close (`told`), nothing is
/// sent, as the connection is closing, and the response is read to its end.
async fn write(
    mut sink: SplitSink<WebSocket, Message>,
    mut output: Frames,
    told: oneshot::Receiver<Told>,
    mut until: Until,
) {
    let mut gone = false;
    let mut told = told;
    let frame = loop {
        let next = tokio::select! {
            // What the client did comes first: it is why the response fails.
            biased;
            Ok(told) = &mut told => {
                match told {
                    Told::Closed => gone = true,
                    Told::Binary => return close(&mut sink, 1003, "binary").await,
                }
                continue;
            }
            reason = &mut until => break (1001, clipped(reason.to_owned())),
            next = output.next() => next,
        };
        match next {
            Some(Ok(bytes)) if !gone => {
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
    if !gone {
        close(&mut sink, frame.0, &frame.1).await;
    }
}

/// The close code and reason for a fault: its code as the reason, never its
/// message.
fn failure(fault: &Fault) -> (u16, String) {
    let code = fault.get::<CloseCode>().map_or(1011, |c| c.sendable());
    (code, clipped(fault.code().to_owned()))
}

/// `reason` cut to the 123 bytes a Close's reason takes.
fn clipped(mut reason: String) -> String {
    while reason.len() > 123 {
        reason.pop();
    }
    reason
}

/// How long a Close is given to leave: it is under 130 bytes, so a client
/// that has not taken it in this time has stopped reading, and waiting on it
/// would keep the session alive past its end.
const CLOSE_SEND: Duration = Duration::from_secs(1);

async fn close(sink: &mut SplitSink<WebSocket, Message>, code: u16, reason: &str) {
    let frame = CloseFrame {
        code,
        reason: Utf8Bytes::from(reason.to_owned()),
    };
    let _ = tokio::time::timeout(CLOSE_SEND, async {
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
        let plain = Fault::upstream("secret provider detail");
        assert_eq!(failure(&plain), (1011, "upstream".to_owned()));
        let asked = Fault::new("over_limit", "x").with(CloseCode(1008));
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
