//! [`WsTransport`]: a provider reached over WebSocket.

use std::pin::pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::http::header::{self, HeaderName};
use respan_gateway_http::http::{self, HeaderMap, StatusCode};
use respan_gateway_http::{Body, Completion, full};
use respan_gateway_http::{
    credentials, is_connection_header, proxy_call, strip_client_origin_headers,
    strip_origin_headers,
};
use respan_gateway_sdk::Fault;
use respan_gateway_sdk::futures::channel::oneshot;
use respan_gateway_sdk::futures::stream::{SplitSink, SplitStream};
use respan_gateway_sdk::futures::{FutureExt, SinkExt, StreamExt, stream};
use tokio::sync::mpsc::{self, Sender};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode as Code;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Role, WebSocketConfig};
use tokio_tungstenite::tungstenite::{Error, Message};

use crate::{CLOSE_WAIT, CloseCode, MAX_MESSAGE};
use respan_gateway_http::Frames;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

type Socket = WebSocketStream<reqwest::Upgraded>;

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
/// each call's path and query extend, as [`HttpClient`] does for HTTP.
///
/// A call's head is stripped and given keys as [`HttpClient`] does
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
/// - It ends `Err` (the client broke or left, or a filter or plugin refused
///   an input): the connection is dropped with no Close, and the answer ends
///   with that fault, so what the client is told is the fault's own (an
///   edge that reports its client's leaving says `abandoned`).
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
/// [`HttpClient`]: https://docs.rs/respan-gateway-http/latest/respan_gateway_http/struct.HttpClient.html
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

/// HTTP owns refusal framing; tungstenite receives only validated upgrades.
/// Keep system roots and an explicit TLS provider even in builds enabling both providers.
fn client() -> Result<reqwest::Client, Fault> {
    static CLIENT: OnceLock<Result<reqwest::Client, Fault>> = OnceLock::new();
    CLIENT
        .get_or_init(|| {
            let mut roots = rustls::RootCertStore::empty();
            roots.add_parsable_certificates(rustls_native_certs::load_native_certs().certs);
            let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::aws_lc_rs::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .map_err(|e| Fault::internal("the TLS settings could not be built").with_source(e))?
            .with_root_certificates(roots)
            .with_no_client_auth();
            reqwest::Client::builder()
                .http1_only()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .retry(reqwest::retry::never())
                .no_gzip()
                .no_brotli()
                .no_deflate()
                .no_zstd()
                .tls_backend_preconfigured(tls)
                .build()
                .map_err(|e| {
                    Fault::internal("the WebSocket HTTP client could not be built").with_source(e)
                })
        })
        .clone()
}

/// Verify the negotiated protocol before exposing the upgraded stream.
fn validate_upgrade(sent: &HeaderMap, received: &HeaderMap) -> Result<(), Fault> {
    let equals = |name, value: &str| {
        received
            .get(name)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.eq_ignore_ascii_case(value))
    };
    let connection = received
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .any(|v| v.trim().eq_ignore_ascii_case("upgrade"));
    let accept = sent
        .get(header::SEC_WEBSOCKET_KEY)
        .map(|key| derive_accept_key(key.as_bytes()));
    let key_matches = accept.as_ref().is_some_and(|key| {
        received
            .get(header::SEC_WEBSOCKET_ACCEPT)
            .is_some_and(|v| v == key)
    });
    let protocol = match (
        sent.get(header::SEC_WEBSOCKET_PROTOCOL),
        received.get(header::SEC_WEBSOCKET_PROTOCOL),
    ) {
        (None, None) => true,
        (Some(offered), Some(chosen)) => offered.to_str().ok().is_some_and(|offers| {
            offers
                .split(',')
                .any(|p| p.trim().as_bytes() == chosen.as_bytes())
        }),
        _ => false,
    };
    if equals(header::UPGRADE, "websocket")
        && connection
        && key_matches
        && protocol
        && !received.contains_key(header::SEC_WEBSOCKET_EXTENSIONS)
    {
        Ok(())
    } else {
        Err(Fault::upstream(
            "the provider returned an invalid WebSocket handshake",
        ))
    }
}

fn invalid(e: impl std::error::Error + Send + Sync + 'static) -> Fault {
    Fault::config("the provider request is not valid").with_source(e)
}

impl WsTransport {
    /// Create fresh outgoing I/O for each call using this backend.
    pub fn connect(
        self,
    ) -> impl Fn(respan_gateway_sdk::Extensions) -> respan_gateway_http::Outgoing + Clone + Send + Sync
    {
        let backend = Arc::new(self);
        respan_gateway_http::connect(move |request| {
            let backend = backend.clone();
            async move { backend.send(request).await }
        })
    }

    /// Send an encoded call and return its response.
    pub async fn send(
        &self,
        mut call: http::Request<Body>,
    ) -> Result<(http::Response<Body>, Option<Completion>), Fault> {
        let (mut head, body) = call.into_parts();
        proxy_call(&mut head);
        strip_client_origin_headers(&mut head.headers);
        call = http::Request::from_parts(head, body);
        credentials(&mut call, &self.headers);
        let (head, body) = call.into_parts();
        let body = respan_gateway_http::body_of(body);
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
        let connect = async {
            let expected = req.headers().clone();
            let url = req.uri().to_string().replacen("ws", "http", 1);
            let resp = client()?
                .get(url)
                .headers(expected.clone())
                .send()
                .await
                .map_err(|e| Fault::upstream("the provider could not be reached").with_source(e))?;
            let mut head = respan_gateway_http::fresh_response_head();
            head.status = resp.status();
            head.version = resp.version();
            head.headers = resp.headers().clone();
            if head.status != StatusCode::SWITCHING_PROTOCOLS {
                let body = resp
                    .bytes()
                    .await
                    .map_err(|e| Fault::truncated().with_source(e))?;
                head.headers.remove(header::TRANSFER_ENCODING);
                head.headers.remove(header::CONTENT_LENGTH);
                strip_origin_headers(&mut head.headers);
                return Ok((http::Response::from_parts(head, full(body)), None));
            }
            validate_upgrade(&expected, &head.headers)?;
            let upgraded = resp.upgrade().await.map_err(|e| {
                Fault::upstream("the provider did not upgrade the connection").with_source(e)
            })?;
            let socket =
                WebSocketStream::from_raw_socket(upgraded, Role::Client, Some(config)).await;
            strip_origin_headers(&mut head.headers);
            head.headers.remove(header::SEC_WEBSOCKET_ACCEPT);
            head.headers.remove(header::SEC_WEBSOCKET_EXTENSIONS);
            // One frame in flight: a reader that stops reading stops the connection's.
            let (frames, frames_in) = mpsc::channel(1);
            let writing = tokio::spawn(session(socket, body, frames));
            let frames_in = stream::unfold(frames_in, |mut rx| async {
                rx.recv().await.map(|f| (f, rx))
            });
            let answer = http::Response::from_parts(head, Body::stream(frames_in));
            let completion: Completion = async move {
                writing.await.map_err(|error| {
                    if error.is_cancelled() {
                        Fault::abandoned()
                    } else {
                        Fault::internal("the WebSocket session panicked")
                    }
                })?
            }
            .boxed();
            Ok((answer, Some(completion)))
        };
        tokio::time::timeout(CONNECT_TIMEOUT, connect)
            .await
            .map_err(|_| Fault::upstream("the provider took too long to accept"))?
    }
}

/// How the call's frames ended.
enum Sent {
    /// The call's body ended clean, and Close was sent.
    Closed,
    /// The body ended `Err`: the session is over, and the answer ends with
    /// its fault.
    Gone(Fault),
    /// The connection broke while sending; the reader says how.
    Lost,
    /// A frame that cannot be sent.
    Failed(Fault),
}

/// Pump `body`'s frames to the provider and the provider's to `frames`,
/// until one side ends and the other has had its say. Its result completes
/// the HTTP transport's outgoing transfer; provider-initiated clean closure
/// stops the upload normally. Read failures remain on the incoming flow.
async fn session(
    socket: Socket,
    body: Frames,
    frames: Sender<Result<Bytes, Fault>>,
) -> Result<(), Fault> {
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
        () = &mut read => Ok(()),
        sent = sent => match sent {
            // Close is sent; the provider's part of it may take a while.
            Sent::Closed => {
                let _ = tokio::time::timeout(CLOSE_WAIT, read).await;
                Ok(())
            }
            Sent::Gone(fault) | Sent::Failed(fault) => {
                let _ = abort.send(fault.clone());
                read.await;
                Err(fault)
            }
            Sent::Lost => {
                // The reader classifies the socket failure on the incoming
                // flow, preserving its precise close code or truncation.
                read.await;
                Ok(())
            },
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
        let bytes = match frame {
            Ok(bytes) => bytes,
            Err(fault) => return Sent::Gone(fault),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upgrades_require_the_key_and_only_negotiated_protocols() {
        let mut sent = HeaderMap::new();
        sent.insert(
            header::SEC_WEBSOCKET_KEY,
            "dGhlIHNhbXBsZSBub25jZQ==".parse().expect("header"),
        );
        let mut received = HeaderMap::new();
        received.insert(header::UPGRADE, "websocket".parse().expect("header"));
        received.insert(
            header::CONNECTION,
            "keep-alive, Upgrade".parse().expect("header"),
        );
        received.insert(
            header::SEC_WEBSOCKET_ACCEPT,
            "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=".parse().expect("header"),
        );
        assert!(validate_upgrade(&sent, &received).is_ok());
        for name in [
            header::UPGRADE,
            header::CONNECTION,
            header::SEC_WEBSOCKET_ACCEPT,
        ] {
            let mut invalid = received.clone();
            invalid.remove(&name);
            assert!(validate_upgrade(&sent, &invalid).is_err());
            invalid.insert(name, "wrong".parse().expect("header"));
            assert!(validate_upgrade(&sent, &invalid).is_err());
        }
        for name in [
            header::SEC_WEBSOCKET_PROTOCOL,
            header::SEC_WEBSOCKET_EXTENSIONS,
        ] {
            let mut unsolicited = received.clone();
            unsolicited.insert(name, "unsolicited".parse().expect("header"));
            assert!(validate_upgrade(&sent, &unsolicited).is_err());
        }
        sent.insert(
            header::SEC_WEBSOCKET_PROTOCOL,
            "chat, realtime".parse().expect("header"),
        );
        assert!(validate_upgrade(&sent, &received).is_err());
        received.insert(
            header::SEC_WEBSOCKET_PROTOCOL,
            "realtime".parse().expect("header"),
        );
        assert!(validate_upgrade(&sent, &received).is_ok());
        received.insert(
            header::SEC_WEBSOCKET_PROTOCOL,
            "other".parse().expect("header"),
        );
        assert!(validate_upgrade(&sent, &received).is_err());
    }

    #[tokio::test]
    async fn non_upgrade_200_and_204_are_original_refusals_without_waiting_for_upload() {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        use std::pin::Pin;
        use std::sync::Mutex;
        use std::task::{Context, Poll};

        use respan_gateway_http::http_body::Frame;
        use respan_gateway_http::http_body_util::{BodyExt, StreamBody};
        use respan_gateway_sdk::futures::Stream;
        use respan_gateway_sdk::{Gateway, Plugin, ProviderContext, async_trait};

        use crate::JsonEvents;

        struct NeverUpload(Arc<AtomicBool>);
        impl Stream for NeverUpload {
            type Item = Result<Frame<Bytes>, Fault>;
            fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
                Poll::Pending
            }
        }
        impl Drop for NeverUpload {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        type EndRecord = (bool, Result<(), Fault>);
        #[derive(Clone, Default)]
        struct Ended(Arc<Mutex<Vec<EndRecord>>>);
        #[async_trait]
        impl Plugin for Ended {
            async fn on_provider_end(&self, px: &ProviderContext, end: &Result<(), Fault>) {
                self.0
                    .lock()
                    .expect("record end")
                    .push((px.is_refused(), end.clone()));
            }
        }

        for (status, body) in [
            (StatusCode::OK, "not upgraded"),
            (StatusCode::NO_CONTENT, ""),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind local handshake server");
            let address = listener.local_addr().expect("local address");
            let server = std::thread::spawn(move || {
                let (mut socket, _) = listener.accept().expect("receive handshake");
                socket
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .expect("read timeout");
                let mut headers = Vec::new();
                let mut one = [0u8; 1024];
                while !headers.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                    let n = socket.read(&mut one).expect("read handshake");
                    assert!(
                        n > 0 && headers.len() + n < 16 * 1024,
                        "incomplete handshake"
                    );
                    headers.extend_from_slice(&one[..n]);
                }
                assert!(String::from_utf8_lossy(&headers).contains("GET /events HTTP/1.1"));
                let reply = format!(
                    "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nX-Provider: original\r\nConnection: close\r\n\r\n{}",
                    status.as_u16(),
                    status.canonical_reason().unwrap_or(""),
                    body.len(),
                    body,
                );
                socket
                    .write_all(reply.as_bytes())
                    .expect("write non-upgrade answer");
            });
            let ended = Ended::default();
            let format = JsonEvents::new("json-ws", "/events");
            let transport = WsTransport::new(format!("ws://{address}")).expect("transport");
            let gateway = Gateway::new()
                .provider::<crate::ClientEvent, _, _>("ws", format.clone(), transport.connect())
                .and_then(|g| g.client::<crate::ClientEvent, _, _>(format))
                .and_then(|g| g.plugin("ended", ended.clone()))
                .and_then(|g| g.target("ws"))
                .expect("gateway");
            let dropped = Arc::new(AtomicBool::new(false));
            let body_stream = StreamBody::new(NeverUpload(dropped.clone()));
            let request = http::Request::builder()
                .uri("/events")
                .header(header::UPGRADE, "websocket")
                .header(header::CONNECTION, "upgrade")
                .body(body_stream)
                .expect("request");
            let (response, completion) = tokio::time::timeout(
                Duration::from_secs(3),
                respan_gateway_http::handle(&gateway, request, true),
            )
            .await
            .expect("refusal must not wait for an upload frame");
            assert_eq!(response.status(), status);
            assert_eq!(response.headers()["x-provider"], "original");
            let bytes = tokio::time::timeout(
                Duration::from_secs(3),
                BodyExt::collect(response.into_body()),
            )
            .await
            .expect("complete refusal body")
            .expect("read refusal body")
            .to_bytes();
            assert_eq!(bytes, body.as_bytes());
            let _ = tokio::time::timeout(Duration::from_secs(3), completion)
                .await
                .expect("finish refused call");
            gateway.idle().await;
            let records = ended.0.lock().expect("recorded provider end");
            assert_eq!(records.len(), 1);
            assert!(records[0].0, "provider refused before opening");
            let fault = records[0].1.as_ref().expect_err("refused call");
            let original = fault
                .answer::<http::Response<Bytes>>("json-ws")
                .expect("fault retains original answer");
            assert_eq!(
                (
                    original.status(),
                    &original.headers()["x-provider"],
                    original.body()
                ),
                (
                    status,
                    &http::HeaderValue::from_static("original"),
                    &Bytes::copy_from_slice(body.as_bytes())
                )
            );
            assert!(
                dropped.load(Ordering::SeqCst),
                "unread upload was cancelled"
            );
            server.join().expect("handshake server");
        }
    }
}
