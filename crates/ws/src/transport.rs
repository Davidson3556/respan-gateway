//! [`WsTransport`]: a provider reached over WebSocket.

use std::pin::pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::http::header::{self, HeaderName};
use respan_gateway_http::http::{self, HeaderMap, StatusCode};
use respan_gateway_http::{Body, Completion, Frames, RawErrors};
use respan_gateway_http::{is_connection_header, prepare_call, strip_origin_headers};
use respan_gateway_sdk::futures::channel::oneshot;
use respan_gateway_sdk::futures::stream::{SplitSink, SplitStream};
use respan_gateway_sdk::futures::{FutureExt, SinkExt, Stream, StreamExt, stream};
use respan_gateway_sdk::{Abandoned, Extensions, Fault, Truncated, Upstream};
use tokio::sync::mpsc::{self, Sender};
use tokio::time::Instant;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Role, WebSocketConfig};
use tokio_tungstenite::tungstenite::{Error, Message};

use crate::{CloseCode, Limits, ProviderClose};

/// How long a provider has to answer the upgrade by default: connecting
/// takes well under a second to a reachable host, and 10 s spans TCP's
/// first retransmits of a lost handshake (RFC 6298: after 1 s, then 2 s
/// more, then 4 s more), as `HttpClient`'s connect timeout does.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

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
/// A call is prepared as [`HttpClient`] prepares one ([`prepare_call`]):
/// stripped, and given keys, so a client's key passes only where a plugin
/// put it back; the handshake's own headers are the WebSocket library's.
/// The answer is the provider's `101` (its origin headers,
/// `Sec-WebSocket-Accept` and extensions removed), or the provider's
/// refusal of the upgrade, a non-`101` answer with its body, which the
/// format reads as any error answer. A provider that cannot be reached, or
/// does not answer the upgrade, its refusal's body included, within the
/// handshake timeout ([`with_handshake_timeout`](Self::with_handshake_timeout)),
/// fails the call `upstream`.
///
/// The body of the call is the session's frames to send, each one text
/// message; the answer's body is the provider's, each one text message.
///
/// - The call's body ends clean: Close 1000 is sent, and the provider's
///   end, however it comes within [`Limits::close_wait`], ends the answer
///   clean.
/// - It ends `Err` (the client broke or left, or a filter or plugin refused
///   an input): the provider is sent Close (1001 for a client that left
///   ([`Abandoned`]), else the fault's [`CloseCode`] or 1011) within
///   [`Limits::close_send`], the connection is dropped, and the answer ends
///   with that fault.
/// - The answer's body is dropped: the provider is sent Close 1001 within
///   [`Limits::close_send`], and the connection is dropped.
/// - The provider sends Close with no code or 1000: the answer ends clean.
///   With another code: an `Upstream` fault with that [`ProviderClose`],
///   and, on a call with [`RawErrors`] (the
///   [`Relay`](respan_gateway_http::relay::Relay)'s permission to pass the
///   provider's errors on), that [`CloseCode`] too.
/// - The provider's connection breaks with no Close: `Err(truncated)`.
/// - A binary frame in the call is a `config` fault. A provider's binary
///   frame is an `upstream` fault unless [`Self::with_binary_text`] enables
///   UTF-8 normalization; invalid UTF-8 remains a fault.
///
/// The transport reads the provider no faster than the answer is read (one
/// frame in flight), and sends no faster than the provider takes.
///
/// [`HttpClient`]: respan_gateway_http::HttpClient
#[derive(Clone)]
pub struct WsTransport {
    client: reqwest::Client,
    /// The base URL, without a trailing `/`.
    base: String,
    headers: HeaderMap,
    limits: Limits,
    binary_text: bool,
    handshake_timeout: Duration,
}

impl std::fmt::Debug for WsTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Header values may be keys.
        f.debug_struct("WsTransport")
            .field("base", &self.base)
            .field("headers", &self.headers.keys().collect::<Vec<_>>())
            .field("limits", &self.limits)
            .field("binary_text", &self.binary_text)
            .field("handshake_timeout", &self.handshake_timeout)
            .finish_non_exhaustive()
    }
}

impl WsTransport {
    /// A transport to `base_url`: `ws` or `wss`
    /// ([`base_url`](respan_gateway_http::base_url)), else `Err` with
    /// `config`. It takes [`Limits::default`], gives the provider 10 s to
    /// answer the upgrade, and shares one client, until told otherwise.
    pub fn new(base_url: impl Into<String>) -> Result<Self, Fault> {
        Ok(Self {
            client: shared_client()?,
            base: respan_gateway_http::base_url(&base_url.into(), &["ws", "wss"])?,
            headers: HeaderMap::new(),
            limits: Limits::default(),
            binary_text: false,
            handshake_timeout: HANDSHAKE_TIMEOUT,
        })
    }

    /// Send `headers`, such as the API key, with every call, replacing any
    /// set before, in place of the client's
    /// ([`credentials`](respan_gateway_http::credentials)). `Err` with
    /// `config` for a header of the connection or the handshake.
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

    /// Use `client` instead of the shared one: it must speak HTTP/1.1, as
    /// only HTTP/1.1 upgrades, and should not follow redirects, which are
    /// the client's to follow.
    pub fn with_client(mut self, client: reqwest::Client) -> Self {
        self.client = client;
        self
    }

    /// Take `limits` ([`Limits`]): a message, or a refusal's body, over
    /// `max_message` breaks the answer `Err(upstream)`.
    pub fn with_limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }

    /// Accept the provider's binary messages as UTF-8 text. Some JSON
    /// protocols, including Gemini Live, use binary frames for their JSON.
    /// Disabled by default; invalid UTF-8 still fails `upstream`. Outbound
    /// messages and the normalized response body remain text.
    pub fn with_binary_text(mut self, enabled: bool) -> Self {
        self.binary_text = enabled;
        self
    }

    /// Give the provider `timeout` to answer the upgrade: to be reached, to
    /// answer the handshake, and to send a refusal's body whole.
    pub fn with_handshake_timeout(mut self, timeout: Duration) -> Self {
        self.handshake_timeout = timeout;
        self
    }
}

/// The client every transport shares: HTTP/1.1, as only HTTP/1.1 upgrades;
/// no redirects, as they are the client's to follow; and the system's root
/// certificates with an explicit TLS provider, `aws-lc-rs`, because the
/// workspace's dependencies enable both `ring` and `aws-lc-rs`, which makes
/// rustls's own choice panic. Proxies are the environment's, as for
/// `HttpClient`.
fn shared_client() -> Result<reqwest::Client, Fault> {
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
                .redirect(reqwest::redirect::Policy::none())
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
            .get_all(header::SEC_WEBSOCKET_ACCEPT)
            .iter()
            .count()
            == 1
            && received
                .get(header::SEC_WEBSOCKET_ACCEPT)
                .is_some_and(|v| v == key)
    });
    let mut chosen = received.get_all(header::SEC_WEBSOCKET_PROTOCOL).iter();
    let protocol = match (chosen.next(), chosen.next()) {
        // RFC 6455 4.2.2: a server may decline every offered subprotocol.
        (None, None) => true,
        (Some(chosen), None)
            if !chosen.is_empty()
                && chosen
                    .as_bytes()
                    .iter()
                    .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(b)) =>
        {
            sent.get_all(header::SEC_WEBSOCKET_PROTOCOL)
                .iter()
                .filter_map(|v| v.to_str().ok())
                .flat_map(|offers| offers.split(','))
                .any(|p| p.trim().as_bytes() == chosen.as_bytes())
        }
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
        Err(Upstream::fault(
            "the provider returned an invalid WebSocket handshake",
        ))
    }
}

fn invalid(e: impl std::error::Error + Send + Sync + 'static) -> Fault {
    Fault::config("the provider request is not valid").with_source(e)
}

/// The fault of a provider that did not answer the upgrade in time.
fn too_slow() -> Fault {
    Upstream::fault("the provider took too long to accept")
}

/// `body`, failing [`too_slow`] once `deadline` passes.
fn until(deadline: Instant, body: Frames) -> impl Stream<Item = Result<Bytes, Fault>> + Send {
    stream::unfold(Some(body), move |body| async move {
        let mut body = body?;
        match tokio::time::timeout_at(deadline, body.next()).await {
            Ok(item) => Some((item?, Some(body))),
            Err(_) => Some((Err(too_slow()), None)),
        }
    })
}

impl WsTransport {
    /// Create fresh outgoing I/O for each call using this backend.
    pub fn connect(
        self,
    ) -> impl Fn(respan_gateway_sdk::Extensions) -> respan_gateway_http::Outgoing + Clone + Send + Sync
    {
        // One message, or a refusal's body, is what a reader holds of the
        // provider's answer.
        let limit = self.limits.max_message;
        let backend = Arc::new(self);
        respan_gateway_http::connect(
            move |request| {
                let backend = backend.clone();
                async move { backend.send(request).await }
            },
            limit,
        )
    }

    /// Send an encoded call and return its response.
    pub async fn send(
        &self,
        mut call: http::Request<Body>,
    ) -> Result<(http::Response<Body>, Option<Completion>), Fault> {
        let relayed = call
            .extensions()
            .get::<Extensions>()
            .is_some_and(|e| e.get::<RawErrors>().is_some());
        prepare_call(&mut call, &self.base, &self.headers)?;
        let (head, body) = call.into_parts();
        let body = body.boxed();
        let mut req = head
            .uri
            .to_string()
            .into_client_request()
            .map_err(invalid)?;
        for (name, value) in head.headers.iter().filter(|(n, _)| !is_handshake(n)) {
            req.headers_mut().append(name, value.clone());
        }
        let limits = self.limits;
        let config = WebSocketConfig::default()
            .max_message_size(Some(limits.max_message))
            .max_frame_size(Some(limits.max_message));
        let deadline = Instant::now() + self.handshake_timeout;
        let connect = async {
            let expected = req.headers().clone();
            let url = req.uri().to_string().replacen("ws", "http", 1);
            let resp = self
                .client
                .get(url)
                .headers(expected.clone())
                .send()
                .await
                .map_err(|e| Upstream::fault("the provider could not be reached").with_source(e))?;
            let mut head = http::Response::new(()).into_parts().0;
            head.status = resp.status();
            head.version = resp.version();
            head.headers = resp.headers().clone();
            strip_origin_headers(&mut head.headers);
            if head.status != StatusCode::SWITCHING_PROTOCOLS {
                // The body is read whole by the format, as framed here.
                head.headers.remove(header::TRANSFER_ENCODING);
                head.headers.remove(header::CONTENT_LENGTH);
                let broke = |e| Truncated::fault().with_source(e);
                let body = resp.bytes_stream().map(move |chunk| chunk.map_err(broke));
                let body = body.boxed();
                let body = Body::stream(until(deadline, body));
                return Ok((http::Response::from_parts(head, body), None));
            }
            validate_upgrade(&expected, &head.headers)?;
            let upgraded = resp.upgrade().await.map_err(|e| {
                Upstream::fault("the provider did not upgrade the connection").with_source(e)
            })?;
            let socket =
                WebSocketStream::from_raw_socket(upgraded, Role::Client, Some(config)).await;
            head.headers.remove(header::SEC_WEBSOCKET_ACCEPT);
            head.headers.remove(header::SEC_WEBSOCKET_EXTENSIONS);
            // One frame in flight: a reader that stops reading stops the connection's.
            let (frames, frames_in) = mpsc::channel(1);
            let writing = tokio::spawn(session(
                socket,
                body,
                frames,
                limits,
                relayed,
                self.binary_text,
            ));
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
        tokio::time::timeout_at(deadline, connect)
            .await
            .map_err(|_| too_slow())?
    }
}

/// How the call's frames ended.
enum Sent {
    /// The call's body ended clean, and Close was sent.
    Closed,
    /// The body ended `Err`, or held a frame that cannot be sent: the
    /// session is over, and the answer ends with the fault.
    Failed(Fault),
    /// The connection broke while sending; the reader says how.
    Lost,
}

/// How the provider's frames ended.
enum Read {
    /// The provider's side ended, and the answer with it.
    Ended,
    /// The answer was dropped: no one reads the provider.
    Dropped,
}

/// Pump `body`'s frames to the provider and the provider's to `frames`,
/// until one side ends and the other has had its say. Its result completes
/// the HTTP transport's outgoing transfer; provider-initiated clean closure
/// stops the upload normally. Read failures remain on the incoming flow.
async fn session(
    socket: Socket,
    body: Frames,
    frames: Sender<Result<Bytes, Fault>>,
    limits: Limits,
    relayed: bool,
    binary_text: bool,
) -> Result<(), Fault> {
    let (mut sink, stream) = socket.split();
    let closing = AtomicBool::new(false);
    // Only the reader holds the answer's sender, so the answer ends when it
    // says so; a fault of the sending side goes to it to say.
    let (abort, aborted) = oneshot::channel();
    let reader = read_frames(
        stream,
        frames,
        &closing,
        aborted,
        limits,
        relayed,
        binary_text,
    );
    let mut read = pin!(reader);
    let sent = tokio::select! {
        // An input's failure, which ends the call's answer too, says why.
        biased;
        sent = send_frames(&mut sink, body, &closing, limits.close_send) => Ok(sent),
        read = &mut read => Err(read),
    };
    match sent {
        Err(Read::Ended) => Ok(()),
        // No one reads the session any more: the gateway goes away.
        Err(Read::Dropped) => {
            close(&mut sink, 1001, limits.close_send).await;
            Ok(())
        }
        // Close is sent; the provider's part of it may take a while.
        Ok(Sent::Closed) => {
            let _ = tokio::time::timeout(limits.close_wait, read).await;
            Ok(())
        }
        Ok(Sent::Failed(fault)) => {
            // The reader ends the answer with the fault before the
            // provider's answer to this Close could end it clean.
            let _ = abort.send(fault.clone());
            let code = match fault.is::<Abandoned>() {
                true => 1001,
                false => fault.get::<CloseCode>().map_or(1011, |c| c.sendable()),
            };
            close(&mut sink, code, limits.close_send).await;
            read.await;
            Err(fault)
        }
        Ok(Sent::Lost) => {
            // The reader classifies the socket failure on the incoming
            // flow, preserving its precise close code or truncation.
            read.await;
            Ok(())
        }
    }
}

/// Send Close `code` within `wait`, as a peer that has not taken it by then
/// has stopped reading.
async fn close(sink: &mut SplitSink<Socket, Message>, code: u16, wait: Duration) {
    let frame = CloseFrame {
        code: code.into(),
        reason: "".into(),
    };
    let _ = tokio::time::timeout(wait, sink.send(Message::Close(Some(frame)))).await;
}

/// The call's frames as text messages, then Close 1000 if the body ended
/// clean.
async fn send_frames(
    sink: &mut SplitSink<Socket, Message>,
    mut body: Frames,
    closing: &AtomicBool,
    close_send: Duration,
) -> Sent {
    while let Some(frame) = body.next().await {
        let bytes = match frame {
            Ok(bytes) => bytes,
            Err(fault) => return Sent::Failed(fault),
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
    close(sink, 1000, close_send).await;
    Sent::Closed
}

/// The provider's text messages into `frames`, then how it ended: `frames`
/// ends clean, or with the fault. A provider's Close with a code other than
/// 1000 is a [`ProviderClose`], and a [`CloseCode`] too if `relayed`.
async fn read_frames(
    mut stream: SplitStream<Socket>,
    frames: Sender<Result<Bytes, Fault>>,
    closing: &AtomicBool,
    mut aborted: oneshot::Receiver<Fault>,
    limits: Limits,
    relayed: bool,
    binary_text: bool,
) -> Read {
    let fault = loop {
        let next = tokio::select! {
            biased;
            // The sending side failed: its fault ends the answer.
            Ok(fault) = &mut aborted => break fault,
            // The answer was dropped, quiet connection or not.
            () = frames.closed() => return Read::Dropped,
            next = stream.next() => next,
        };
        match next {
            Some(Ok(Message::Text(text))) => {
                let frame = Bytes::copy_from_slice(text.as_bytes());
                if frames.send(Ok(frame)).await.is_err() {
                    return Read::Dropped;
                }
            }
            Some(Ok(Message::Binary(bytes))) if binary_text => {
                if std::str::from_utf8(&bytes).is_err() {
                    break Upstream::fault(
                        "the provider sent a binary message that is not UTF-8 text",
                    );
                }
                if frames.send(Ok(bytes)).await.is_err() {
                    return Read::Dropped;
                }
            }
            Some(Ok(Message::Binary(_))) => {
                let text = "the provider sent a binary frame: only text frames are carried";
                break Upstream::fault(text);
            }
            Some(Ok(Message::Close(frame))) => {
                let code = frame.map(|f| u16::from(f.code));
                let clean = closing.load(Ordering::SeqCst) || matches!(code, None | Some(1000));
                if let (false, Some(code)) = (clean, code) {
                    let mut fault = Upstream::fault("the provider closed the session")
                        .with(ProviderClose(code));
                    if relayed {
                        fault = fault.with(CloseCode(code));
                    }
                    let _ = frames.send(Err(fault)).await;
                }
                // The answer ends; the library sends its reply to the Close
                // as the stream is read on.
                drop(frames);
                let _ = tokio::time::timeout(limits.close_send, stream.next()).await;
                return Read::Ended;
            }
            Some(Ok(_)) => {}
            // No Close came. After ours, that is how a provider may end.
            Some(Err(_)) | None if closing.load(Ordering::SeqCst) => return Read::Ended,
            Some(Err(Error::Capacity(_))) => {
                break Upstream::fault("the provider sent a message that is too large");
            }
            Some(Err(_)) | None => break Truncated::fault(),
        }
    };
    let _ = frames.send(Err(fault)).await;
    Read::Ended
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener};
    use std::sync::Mutex;

    use respan_gateway_http::ProviderError;
    use respan_gateway_http::http_body_util::BodyExt;
    use respan_gateway_sdk::{Gateway, Plugin, ProviderContext, async_trait};

    use crate::JsonEvents;
    use respan_gateway_realtime::Input;

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
        let mut duplicate = received.clone();
        duplicate.append(
            header::SEC_WEBSOCKET_ACCEPT,
            "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=".parse().expect("header"),
        );
        assert!(validate_upgrade(&sent, &duplicate).is_err());
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
        assert!(validate_upgrade(&sent, &received).is_ok());
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
        sent.append(
            header::SEC_WEBSOCKET_PROTOCOL,
            "other".parse().expect("header"),
        );
        assert!(validate_upgrade(&sent, &received).is_ok());
        received.append(
            header::SEC_WEBSOCKET_PROTOCOL,
            "realtime".parse().expect("header"),
        );
        assert!(validate_upgrade(&sent, &received).is_err());
        for invalid in ["bad protocol", "bad/protocol", "", " "] {
            sent.insert(
                header::SEC_WEBSOCKET_PROTOCOL,
                invalid.parse().expect("header"),
            );
            received.insert(
                header::SEC_WEBSOCKET_PROTOCOL,
                invalid.parse().expect("header"),
            );
            assert!(validate_upgrade(&sent, &received).is_err(), "{invalid:?}");
        }
    }

    #[tokio::test]
    async fn fragmented_unicode_with_interleaved_ping_and_no_selected_subprotocol() {
        use tokio_tungstenite::tungstenite::protocol::frame::{
            Frame,
            coding::{Data, OpCode},
        };
        let text = "雪😀";
        for max_message in [text.len(), text.len() - 1] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("listen");
            let base = format!("ws://{}", listener.local_addr().expect("address"));
            let provider = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.expect("accept");
                // No selected subprotocol, despite the client's offer.
                let mut socket = tokio_tungstenite::accept_async(socket)
                    .await
                    .expect("handshake");
                let bytes = text.as_bytes();
                socket
                    .send(Message::Frame(Frame::message(
                        bytes[..2].to_vec(),
                        OpCode::Data(Data::Text),
                        false,
                    )))
                    .await
                    .expect("first fragment");
                socket
                    .send(Message::Ping(Bytes::from_static(b"ping")))
                    .await
                    .expect("ping");
                socket
                    .send(Message::Frame(Frame::message(
                        bytes[2..].to_vec(),
                        OpCode::Data(Data::Continue),
                        true,
                    )))
                    .await
                    .expect("continuation");
                socket.send(Message::Close(None)).await.expect("close");
                while let Some(Ok(_)) = socket.next().await {}
            });
            let transport = WsTransport::new(base)
                .expect("transport")
                .with_limits(Limits {
                    max_message,
                    ..Limits::default()
                });
            let request = http::Request::get("/events")
                .header(header::SEC_WEBSOCKET_PROTOCOL, "optional")
                .body(Body::stream(stream::pending()))
                .expect("request");
            let (answer, completion) = transport.send(request).await.expect("valid upgrade");
            assert_eq!(answer.status(), StatusCode::SWITCHING_PROTOCOLS);
            assert!(
                !answer
                    .headers()
                    .contains_key(header::SEC_WEBSOCKET_PROTOCOL)
            );
            let mut body = answer.into_body();
            let first = tokio::time::timeout(Duration::from_secs(3), body.next())
                .await
                .expect("message deadline")
                .expect("message");
            if max_message == text.len() {
                assert_eq!(first.expect("complete Unicode message"), text);
                assert!(body.next().await.is_none());
            } else {
                assert!(first.is_err_and(|f| f.is::<Upstream>()));
            }
            drop(body);
            tokio::time::timeout(Duration::from_secs(3), completion.expect("completion"))
                .await
                .expect("session deadline")
                .expect("upload ended");
            tokio::time::timeout(Duration::from_secs(3), provider)
                .await
                .expect("server deadline")
                .expect("server task");
        }
    }

    #[tokio::test]
    async fn binary_text_is_opt_in_and_rejects_invalid_utf8() {
        for (enabled, bytes, accepted) in [
            (false, "雪😀".as_bytes(), false),
            (true, "雪😀".as_bytes(), true),
            (true, &[0xff, 0xfe][..], false),
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("listen");
            let base = format!("ws://{}", listener.local_addr().expect("address"));
            let provider = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.expect("accept");
                let mut socket = tokio_tungstenite::accept_async(socket)
                    .await
                    .expect("handshake");
                socket
                    .send(Message::Binary(Bytes::copy_from_slice(bytes)))
                    .await
                    .expect("binary");
                socket.send(Message::Close(None)).await.expect("close");
                while let Some(Ok(_)) = socket.next().await {}
            });
            let transport = WsTransport::new(base)
                .expect("transport")
                .with_binary_text(enabled);
            let request = http::Request::get("/events")
                .body(Body::stream(stream::pending()))
                .expect("request");
            let (answer, completion) = transport.send(request).await.expect("upgrade");
            let mut body = answer.into_body();
            let frame = tokio::time::timeout(Duration::from_secs(3), body.next())
                .await
                .expect("message deadline")
                .expect("message");
            if accepted {
                assert_eq!(frame.expect("UTF-8 text"), bytes);
                assert!(body.next().await.is_none());
            } else {
                assert!(frame.is_err_and(|f| f.is::<Upstream>()));
            }
            drop(body);
            tokio::time::timeout(Duration::from_secs(3), completion.expect("completion"))
                .await
                .expect("session deadline")
                .expect("upload ended");
            tokio::time::timeout(Duration::from_secs(3), provider)
                .await
                .expect("server deadline")
                .expect("server task");
        }
    }

    /// A server that reads one handshake, to `GET /events`, and writes
    /// `head` then `body`, then holds the connection until the test ends.
    fn answering(head: String, body: Vec<u8>) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind local handshake server");
        let address = listener.local_addr().expect("local address");
        std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().expect("receive handshake");
            socket
                .set_read_timeout(Some(Duration::from_secs(3)))
                .expect("read timeout");
            let mut headers = Vec::new();
            let mut chunk = [0u8; 1024];
            while !headers.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                let n = socket.read(&mut chunk).expect("read handshake");
                assert!(
                    n > 0 && headers.len() + n < 16 * 1024,
                    "incomplete handshake"
                );
                headers.extend_from_slice(&chunk[..n]);
            }
            assert!(String::from_utf8_lossy(&headers).contains("GET /events HTTP/1.1"));
            let _ = socket.write_all(head.as_bytes());
            let _ = socket.write_all(&body);
            // Hold the connection open: its end is the test's.
            let _ = socket.read(&mut chunk);
        });
        address
    }

    type EndRecord = (bool, Result<(), Fault>);

    /// How each provider call ended, and whether it was refused.
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

    /// A session through `transport` whose client never sends a frame, with
    /// the client's answer, read whole, how its one call ended, and whether
    /// its upload was dropped.
    async fn session_through(
        transport: WsTransport,
    ) -> (http::Response<Bytes>, EndRecord, Arc<AtomicBool>) {
        use respan_gateway_http::http_body::Frame;
        use respan_gateway_http::http_body_util::StreamBody;

        /// An upload that never sends, and says when it was dropped.
        struct NeverUpload(Arc<AtomicBool>);
        impl Stream for NeverUpload {
            type Item = Result<Frame<Bytes>, Fault>;
            fn poll_next(
                self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Option<Self::Item>> {
                std::task::Poll::Pending
            }
        }
        impl Drop for NeverUpload {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let ended = Ended::default();
        let format = JsonEvents::new("json-ws", "/events");
        let limit = transport.limits.max_message;
        let gateway = Gateway::new()
            .provider::<Input, _, _>("ws", format.clone(), transport.connect())
            .and_then(|g| g.client::<Input, _, _>(format))
            .map(|g| g.plugin(ended.clone()))
            .and_then(|g| g.target("ws"))
            .expect("gateway");
        let dropped = Arc::new(AtomicBool::new(false));
        let request = http::Request::builder()
            .uri("/events")
            .header(header::UPGRADE, "websocket")
            .header(header::CONNECTION, "upgrade")
            .body(StreamBody::new(NeverUpload(dropped.clone())))
            .expect("request");
        let (response, completion) = tokio::time::timeout(
            Duration::from_secs(3),
            respan_gateway_http::handle(&gateway, request, true, limit),
        )
        .await
        .expect("refusal must not wait for an upload frame");
        let (head, body) = response.into_parts();
        let bytes = tokio::time::timeout(Duration::from_secs(3), BodyExt::collect(body))
            .await
            .expect("complete refusal body")
            .expect("read refusal body")
            .to_bytes();
        let _ = tokio::time::timeout(Duration::from_secs(3), completion)
            .await
            .expect("finish refused call");
        gateway.idle().await;
        let mut records = ended.0.lock().expect("recorded provider end").clone();
        assert_eq!(records.len(), 1);
        let response = http::Response::from_parts(head, bytes);
        (response, records.remove(0), dropped)
    }

    #[tokio::test]
    async fn non_upgrade_200_and_204_are_original_refusals_without_waiting_for_upload() {
        for (status, body) in [
            (StatusCode::OK, "not upgraded"),
            (StatusCode::NO_CONTENT, ""),
        ] {
            let head = format!(
                "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nX-Provider: original\r\nConnection: close\r\n\r\n",
                status.as_u16(),
                status.canonical_reason().unwrap_or(""),
                body.len(),
            );
            let address = answering(head, body.as_bytes().to_vec());
            let transport = WsTransport::new(format!("ws://{address}")).expect("transport");
            let (response, (refused, end), dropped) = session_through(transport).await;
            assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
            assert!(!response.headers().contains_key("x-provider"));
            // The client's format answers with the error code alone.
            assert_eq!(response.body(), "upstream");
            assert!(refused, "provider refused before opening");
            let fault = end.expect_err("refused call");
            let original = fault
                .get::<ProviderError>()
                .map(|e| &e.response)
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
        }
    }

    #[tokio::test]
    async fn an_incomplete_refusal_body_fails_at_the_handshake_deadline() {
        let head = "HTTP/1.1 403 Forbidden\r\nContent-Length: 1000000\r\n\r\n".to_owned();
        let address = answering(head, b"partial".to_vec());
        let deadline = Duration::from_millis(300);
        let transport = WsTransport::new(format!("ws://{address}"))
            .expect("transport")
            .with_handshake_timeout(deadline);
        let request = http::Request::get("/events")
            .body(Body::full(Bytes::new()))
            .expect("request");
        let start = Instant::now();
        let (answer, completion) = transport.send(request).await.expect("the refusal's head");
        assert!(completion.is_none());
        assert_eq!(answer.status(), StatusCode::FORBIDDEN);
        let read = tokio::time::timeout(deadline * 10, BodyExt::collect(answer.into_body()))
            .await
            .expect("an incomplete refusal stops at the handshake's deadline");
        let fault = read.expect_err("the body is incomplete");
        assert!(fault.is::<Upstream>(), "{fault}");
        assert!(start.elapsed() >= deadline);
    }

    #[tokio::test]
    async fn a_refusal_body_is_read_up_to_the_message_limit() {
        let limits = Limits {
            max_message: 1024,
            ..Limits::default()
        };
        for size in [1024, 1025] {
            let head = format!("HTTP/1.1 403 Forbidden\r\nContent-Length: {size}\r\n\r\n");
            let address = answering(head, vec![b'x'; size]);
            let transport = WsTransport::new(format!("ws://{address}"))
                .expect("transport")
                .with_limits(limits);
            let (_, (refused, end), _) = session_through(transport).await;
            assert!(refused);
            let fault = end.expect_err("refused call");
            match fault.get::<ProviderError>() {
                Some(original) => {
                    assert_eq!(size, 1024);
                    assert_eq!(original.response.status(), StatusCode::FORBIDDEN);
                    assert_eq!(original.response.body().len(), 1024);
                }
                None => {
                    assert_eq!(size, 1025);
                    assert!(fault.is::<Upstream>());
                    assert!(fault.message().contains("over the limit"), "{fault}");
                }
            }
        }
    }
}
