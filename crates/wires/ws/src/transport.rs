//! [`WsTransport`]: a provider reached over WebSocket.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use respan_gateway_core::futures::{FutureExt, Stream, StreamExt, stream};
use respan_gateway_core::{Extensions, Fault, Truncated, Upstream};
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::http::header::{self, HeaderName};
use respan_gateway_http::http::{self, HeaderMap, StatusCode};
use respan_gateway_http::{Body, Completion, Frames, RawErrors};
use respan_gateway_http::{is_connection_header, prepare_call, strip_origin_headers};
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
use tokio_tungstenite::tungstenite::protocol::Role;

use crate::Limits;
use crate::session::{Edge, inbound, pump};
use respan_gateway_http::{header_tokens, is_token};

/// How long a provider has to answer the upgrade by default: connecting
/// takes well under a second to a reachable host, and 10 s spans TCP's
/// first retransmits of a lost handshake (RFC 6298: after 1 s, then 2 s
/// more, then 4 s more), as `HttpTransport`'s connect timeout does.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

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
/// A call is prepared as [`HttpTransport`] prepares one ([`prepare_call`]):
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
/// It pumps the session as the client edge does ([`serve`](crate::serve)
/// says how each end goes):
///
/// - The call's body ends clean: Close 1000 is sent, and the provider's
///   end, however it comes within [`Limits::close_wait`], ends the answer
///   clean.
/// - It ends `Err` (the client left, or a filter or plugin refused an
///   input): the provider is sent Close (1001 for a client that left, else
///   the fault's [`CloseCode`](crate::CloseCode) or 1011), and the answer
///   ends with that fault. A frame of it that is no text is a `config`
///   fault. The answer's body dropped: Close 1001.
/// - The provider sends Close with no code or 1000: the answer ends clean.
///   With another code: an `Upstream` fault with that
///   [`ProviderClose`](crate::ProviderClose), and, on a call with
///   [`RawErrors`] (the [`Relay`](respan_gateway_http::Relay)'s
///   permission to pass the provider's errors on), that `CloseCode` too.
/// - The provider's connection breaks with no Close, or it is silent
///   through a ping: `Err(truncated)`.
/// - A provider's binary frame, unless [`Self::with_binary_text`] enables
///   UTF-8 normalization (invalid UTF-8 remains a fault), or a message over
///   `max_message`: `upstream`, and the provider is sent Close 1003 or 1009.
///
/// [`HttpTransport`]: respan_gateway_http::HttpTransport
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

    /// What this transport takes of a session ([`with_limits`](Self::with_limits)).
    pub fn limits(&self) -> Limits {
        self.limits
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
/// `HttpTransport`.
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
    let upgrade = received.get(header::UPGRADE);
    let upgrade = upgrade.is_some_and(|v| v.as_bytes().eq_ignore_ascii_case(b"websocket"));
    let connection =
        header_tokens(received, header::CONNECTION).any(|v| v.eq_ignore_ascii_case("upgrade"));
    let accept = sent
        .get(header::SEC_WEBSOCKET_KEY)
        .map(|key| derive_accept_key(key.as_bytes()));
    let mut accepted = received.get_all(header::SEC_WEBSOCKET_ACCEPT).iter();
    let key_matches = match (accepted.next(), accepted.next(), accept) {
        (Some(got), None, Some(accept)) => got == &accept,
        _ => false,
    };
    let mut chosen = received.get_all(header::SEC_WEBSOCKET_PROTOCOL).iter();
    let protocol = match (chosen.next(), chosen.next()) {
        // RFC 6455 4.2.2: a server may decline every offered subprotocol.
        (None, None) => true,
        (Some(chosen), None) if is_token(chosen.as_bytes()) => {
            header_tokens(sent, header::SEC_WEBSOCKET_PROTOCOL)
                .any(|p| p.as_bytes() == chosen.as_bytes())
        }
        _ => false,
    };
    let extended = received.contains_key(header::SEC_WEBSOCKET_EXTENSIONS);
    match upgrade && connection && key_matches && protocol && !extended {
        true => Ok(()),
        false => Err(Upstream::fault(
            "the provider returned an invalid WebSocket handshake",
        )),
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
    ) -> impl Fn(respan_gateway_core::Extensions) -> respan_gateway_http::ProviderIo + Clone + Send + Sync
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
            head.headers.remove(header::SEC_WEBSOCKET_ACCEPT);
            head.headers.remove(header::SEC_WEBSOCKET_EXTENSIONS);
            let (frames, end, answer) = inbound(Truncated::fault);
            let edge = Edge::Provider {
                relayed,
                binary_text: self.binary_text,
            };
            let stop = std::future::pending();
            let session = pump(
                upgraded,
                Role::Client,
                body,
                (frames, end),
                edge,
                limits,
                stop,
            );
            let writing = tokio::spawn(session);
            let answer = http::Response::from_parts(head, Body::stream(answer));
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
}
