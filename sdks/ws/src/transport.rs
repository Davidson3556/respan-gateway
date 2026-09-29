//! [`WsTransport`]: a provider reached over WebSocket.

use std::pin::pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use respan_gateway_core::futures::channel::oneshot;
use respan_gateway_core::futures::stream::{SplitSink, SplitStream};
use respan_gateway_core::futures::{SinkExt, StreamExt, stream};
use respan_gateway_core::{Fault, async_trait, codes};
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::http::header::{self, HeaderName};
use respan_gateway_http::http::{self, HeaderMap};
use respan_gateway_http::{RequestHead, ResponseHead, full};
use respan_gateway_http::{
    credentials, is_connection_header, proxy_call, strip_client_origin_headers,
    strip_origin_headers,
};
use tokio::sync::mpsc::{self, Sender};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode as Code;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, WebSocketConfig};
use tokio_tungstenite::tungstenite::{Error, Message};
use tokio_tungstenite::{
    Connector, MaybeTlsStream, WebSocketStream, connect_async_tls_with_config,
};

use crate::{CLOSE_WAIT, CloseCode, MAX_MESSAGE};
use respan_gateway_http::{Exchange, Frames, Transport};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

type Socket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// Headers of the handshake the WebSocket library writes itself, and the
/// extension and key headers of the client's own connection: none of them
/// is passed on.
fn is_handshake(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "sec-websocket-key"
            | "sec-websocket-version"
            | "sec-websocket-extensions"
            | "sec-websocket-accept"
    ) || is_connection_header(name)
}

/// Reaches one provider over WebSocket at a base URL (`ws` or `wss`), which
/// each call's path and query extend, as [`HttpTransport`] does for HTTP.
///
/// A call's head is stripped and given keys as [`HttpTransport`] does
/// ([`proxy_call`], [`strip_client_origin_headers`], [`credentials`]), so a
/// client's key passes only on a relay; the handshake's own headers are the
/// WebSocket library's. The answer is the provider's `101` (its origin
/// headers, `Sec-WebSocket-Accept` and extensions removed), or the
/// provider's refusal of the upgrade, a non-`101` answer with its body,
/// which the format reads as any error answer. A provider that cannot be
/// reached fails the call `upstream`.
///
/// The body of the call is the session's frames to send, each one text
/// message; the answer's body is the provider's, each one text message.
///
/// - The call's body ends clean: Close 1000 is sent, and the provider's
///   end, however it comes within [`CLOSE_WAIT`], ends the answer clean.
/// - It ends `Err` (the client broke or left): the connection is dropped
///   with no Close, and the answer ends `Err(abandoned)`.
/// - The provider sends Close with no code or 1000: the answer ends clean.
///   With another code: `Err(upstream)` with that [`CloseCode`].
/// - The provider's connection breaks with no Close: `Err(truncated)`.
/// - A binary frame, from the provider or in the call, is a fault
///   (`upstream`, `config`): text only.
///
/// Dropping the answer's body drops the connection. The transport reads
/// the provider no faster than the answer is read (one frame in flight),
/// and sends no faster than the provider takes.
///
/// [`HttpTransport`]: https://docs.rs/respan-gateway-server
#[derive(Clone)]
pub struct WsTransport {
    /// The base URL, without a trailing `/`.
    base: String,
    headers: HeaderMap,
    max_message: usize,
}

impl std::fmt::Debug for WsTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Header values may be keys.
        f.debug_struct("WsTransport")
            .field("base", &self.base)
            .field("headers", &self.headers.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl WsTransport {
    /// A transport to `base_url`: `ws` or `wss`, a host, an optional path
    /// prefix, and no query, fragment or credentials, else `Err` with
    /// `config`.
    pub fn new(base_url: impl Into<String>) -> Result<Self, Fault> {
        // The URL may contain a key, so the error does not echo it.
        let raw: String = base_url.into();
        let url = raw.parse::<http::Uri>().ok().filter(|u| {
            let authority = u.authority().map(|a| a.as_str()).unwrap_or_default();
            matches!(u.scheme_str(), Some("ws" | "wss"))
                && !authority.is_empty()
                && !authority.contains('@')
                && u.query().is_none()
                && !raw.contains('#')
        });
        let Some(url) = url else {
            return Err(Fault::config(
                "bad base URL: a ws or wss one with a host, and no query, \
                 fragment or credentials (those go in its headers)",
            ));
        };
        Ok(Self {
            base: url.to_string().trim_end_matches('/').to_owned(),
            headers: HeaderMap::new(),
            max_message: MAX_MESSAGE,
        })
    }

    /// Send `headers`, such as the API key, with every call, replacing any
    /// set before, in place of the client's ([`credentials`]). `Err` with
    /// `config` for a connection header.
    pub fn with_headers(mut self, headers: HeaderMap) -> Result<Self, Fault> {
        if let Some(name) = headers.keys().find(|k| is_handshake(k)) {
            let name = name.as_str();
            return Err(Fault::config(format!(
                "header '{name}' is the connection's"
            )));
        }
        self.headers = headers;
        Ok(self)
    }

    /// Accept messages up to `bytes` (default [`MAX_MESSAGE`]); a larger one
    /// breaks the connection, so the answer ends `Err(upstream)`.
    pub fn max_message(mut self, bytes: usize) -> Self {
        self.max_message = bytes;
        self
    }
}

/// The TLS settings every `wss` connection shares, built once: the system's
/// roots, as `reqwest`'s `rustls` build has, and the `aws-lc-rs` provider that
/// build has, named here since the crates in a build may enable others and
/// rustls then cannot choose one.
fn tls() -> Result<Arc<rustls::ClientConfig>, Fault> {
    static TLS: OnceLock<Result<Arc<rustls::ClientConfig>, String>> = OnceLock::new();
    let built = TLS.get_or_init(|| {
        let mut roots = rustls::RootCertStore::empty();
        roots.add_parsable_certificates(rustls_native_certs::load_native_certs().certs);
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let config = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| e.to_string())?;
        Ok(Arc::new(
            config.with_root_certificates(roots).with_no_client_auth(),
        ))
    });
    built
        .clone()
        .map_err(|e| Fault::internal("the TLS settings could not be built").with_source(e))
}

fn invalid(e: impl std::error::Error + Send + Sync + 'static) -> Fault {
    Fault::config("the provider request is not valid").with_source(e)
}

#[async_trait]
impl Transport for WsTransport {
    async fn send(&self, call: Exchange<RequestHead>) -> Result<Exchange<ResponseHead>, Fault> {
        let mut call = call;
        proxy_call(&mut call.head);
        strip_client_origin_headers(&mut call.head.headers);
        credentials(&mut call, &self.headers);
        let Exchange { head, body, .. } = call;
        let path = head.uri.path_and_query().map_or("/", |p| p.as_str());
        let mut req = format!("{}{path}", self.base)
            .into_client_request()
            .map_err(invalid)?;
        for (name, value) in head.headers.iter().filter(|(n, _)| !is_handshake(n)) {
            req.headers_mut().append(name, value.clone());
        }
        let config = WebSocketConfig::default()
            .max_message_size(Some(self.max_message))
            .max_frame_size(Some(self.max_message));
        let tls = match self.base.starts_with("wss") {
            true => Some(Connector::Rustls(tls()?)),
            false => None,
        };
        let connect = connect_async_tls_with_config(req, Some(config), false, tls);
        let connected = tokio::time::timeout(CONNECT_TIMEOUT, connect).await;
        let (socket, resp) = match connected {
            Ok(Ok(connected)) => connected,
            // The provider refused the upgrade: its answer, as it came.
            Ok(Err(Error::Http(resp))) => {
                let (mut parts, body) = (*resp).into_parts();
                // The library hands back the body as it read it, still framed.
                let body = body.unwrap_or_default();
                let chunked = parts.headers.remove(header::TRANSFER_ENCODING).is_some();
                let body = if chunked { unchunked(&body) } else { body };
                parts.headers.remove(header::CONTENT_LENGTH);
                let body = full(body);
                let mut answer =
                    respan_gateway_http::answer(http::Response::from_parts(parts, body));
                strip_origin_headers(&mut answer.head.headers);
                return Ok(answer);
            }
            Ok(Err(e)) => {
                return Err(match e {
                    Error::Url(_) | Error::HttpFormat(_) => invalid(e),
                    // The error names the provider's URL, for logs only.
                    e => Fault::upstream("the provider could not be reached").with_source(e),
                });
            }
            Err(_) => return Err(Fault::upstream("the provider took too long to accept")),
        };
        let (mut head, _) = resp.into_parts();
        strip_origin_headers(&mut head.headers);
        head.headers.remove(header::SEC_WEBSOCKET_ACCEPT);
        head.headers.remove(header::SEC_WEBSOCKET_EXTENSIONS);
        // One frame in flight: a reader that stops reading stops the connection's.
        let (frames, frames_in) = mpsc::channel(1);
        tokio::spawn(session(socket, body, frames));
        let frames_in = stream::unfold(frames_in, |mut rx| async {
            rx.recv().await.map(|f| (f, rx))
        });
        let mut answer = Exchange::new(head, frames_in.boxed());
        answer.streams = true;
        Ok(answer)
    }
}

/// How the call's frames ended.
enum Sent {
    /// The call's body ended clean, and Close was sent.
    Closed,
    /// The body ended `Err`: the client is gone.
    Gone,
    /// The connection broke while sending; the reader says how.
    Lost,
    /// A frame that cannot be sent.
    Failed(Fault),
}

/// Pump `body`'s frames to the provider and the provider's to `frames`,
/// until one side ends and the other has had its say.
async fn session(socket: Socket, body: Frames, frames: Sender<Result<Bytes, Fault>>) {
    let (sink, stream) = socket.split();
    let closing = Arc::new(AtomicBool::new(false));
    // Only the reader holds the answer's sender, so the answer ends when it
    // says so; a fault of the sending side goes to it to say.
    let (abort, aborted) = oneshot::channel();
    let mut read = pin!(read_frames(stream, frames, closing.clone(), aborted));
    let sent = send_frames(sink, body, closing);
    tokio::select! {
        // The provider is done or the answer is dropped, so the frames
        // still to send are dropped too, with the call's body.
        () = &mut read => {}
        sent = sent => match sent {
            // Close is sent; the provider's part of it may take a while.
            Sent::Closed => {
                let _ = tokio::time::timeout(CLOSE_WAIT, read).await;
            }
            Sent::Gone => {
                let _ = abort.send(Fault::new(codes::ABANDONED, "the client is gone"));
                read.await;
            }
            Sent::Failed(fault) => {
                let _ = abort.send(fault);
                read.await;
            }
            Sent::Lost => read.await,
        },
    }
}

/// The call's frames as text messages, then Close 1000 if the body ended
/// clean.
async fn send_frames(
    mut sink: SplitSink<Socket, Message>,
    mut body: Frames,
    closing: Arc<AtomicBool>,
) -> Sent {
    while let Some(frame) = body.next().await {
        let Ok(bytes) = frame else {
            return Sent::Gone;
        };
        let Ok(text) = String::from_utf8(bytes.into()) else {
            let text = "a frame of a call is not text: only text frames are carried";
            return Sent::Failed(Fault::config(text));
        };
        if sink.send(Message::Text(text.into())).await.is_err() {
            return Sent::Lost;
        }
    }
    closing.store(true, Ordering::SeqCst);
    let frame = CloseFrame {
        code: Code::Normal,
        reason: "".into(),
    };
    let _ = sink.send(Message::Close(Some(frame))).await;
    Sent::Closed
}

/// The provider's text messages into `frames`, then how it ended: `frames`
/// ends clean, or with the fault.
async fn read_frames(
    mut stream: SplitStream<Socket>,
    frames: Sender<Result<Bytes, Fault>>,
    closing: Arc<AtomicBool>,
    mut aborted: oneshot::Receiver<Fault>,
) {
    let fault = loop {
        let next = tokio::select! {
            next = stream.next() => next,
            // The sending side failed: its fault ends the answer.
            Ok(fault) = &mut aborted => break fault,
            // The answer was dropped, quiet connection or not.
            () = frames.closed() => return,
        };
        match next {
            Some(Ok(Message::Text(text))) => {
                let frame = Bytes::copy_from_slice(text.as_bytes());
                if frames.send(Ok(frame)).await.is_err() {
                    // The answer was dropped.
                    return;
                }
            }
            Some(Ok(Message::Binary(_))) => {
                let text = "the provider sent a binary frame: only text frames are carried";
                break Fault::upstream(text);
            }
            Some(Ok(Message::Close(frame))) => {
                let code = frame.map(|f| u16::from(f.code));
                let clean =
                    closing.load(Ordering::SeqCst) || matches!(code, None | Some(1000 | 1005));
                if let (false, Some(code)) = (clean, code) {
                    let fault = Fault::upstream("the provider closed the session");
                    let _ = frames.send(Err(fault.with(CloseCode(code)))).await;
                }
                // The answer ends; the library sends its reply to the Close
                // as the stream is read on.
                drop(frames);
                let _ = tokio::time::timeout(Duration::from_secs(1), stream.next()).await;
                return;
            }
            Some(Ok(_)) => {}
            // No Close came. After ours, that is how a provider may end.
            Some(Err(_)) | None if closing.load(Ordering::SeqCst) => return,
            Some(Err(Error::Capacity(_))) => {
                break Fault::upstream("the provider sent a message that is too large");
            }
            Some(Err(_)) | None => break Fault::truncated(),
        }
    };
    let _ = frames.send(Err(fault)).await;
}

/// The body of `raw`, a `Transfer-Encoding: chunked` body as far as it came;
/// what is not framed as chunks is kept as it is.
fn unchunked(raw: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    let mut rest = raw;
    loop {
        let Some(line_end) = rest.windows(2).position(|w| w == b"\r\n") else {
            return raw.to_vec();
        };
        let size = std::str::from_utf8(&rest[..line_end])
            .ok()
            .and_then(|line| usize::from_str_radix(line.split(';').next()?.trim(), 16).ok());
        let Some(size) = size else {
            return raw.to_vec();
        };
        let data = &rest[line_end + 2..];
        if size == 0 {
            return body;
        }
        // A chunk cut short gives what came of it.
        body.extend_from_slice(&data[..size.min(data.len())]);
        match data.get(size + 2..) {
            Some(more) => rest = more,
            None => return body,
        }
    }
}
