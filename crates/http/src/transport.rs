//! How the gateway reaches a provider over HTTP ([`Transport`], [`Dialer`]),
//! and what a transport does to a call and its response.

use std::sync::Arc;

use bytes::Bytes;
use futures::stream::BoxStream;
use http::header::{self, CONNECTION, HeaderName, UPGRADE};
use http::{HeaderMap, HeaderValue};
use respan_gateway_core::{Extensions, Fault, async_trait};

use crate::credential::{Credential, is_credential, take_protocol_keys, take_query};
use crate::{HOP_BY_HOP, RequestHead, ResponseHead, remove_if, strip_named};

/// A body, its bytes one item per data frame, ending with an `Err` if it
/// broke off.
pub type Frames = BoxStream<'static, Result<Bytes, Fault>>;

/// One direction of an HTTP exchange: a head, a body and [`Extensions`].
/// What a [`Transport`] sends and gets back.
#[non_exhaustive]
pub struct Exchange<H> {
    /// The head: a request's method, URI and headers, or a response's
    /// status and headers.
    pub head: H,
    /// The bytes after the head.
    pub body: Frames,
    /// Values for the transport, such as the client's credential; none on
    /// a response.
    pub extensions: Extensions,
    /// Whether this call carries the session's own inputs to a format of
    /// the same name as the client's, as the plugins' filters left them: a
    /// filter that edits or replaces an input (a PII redactor, say) keeps
    /// the relay, so a subscription passthrough still works. A call with
    /// its own input ([`Call::with_input`](respan_gateway_core::Call::with_input))
    /// is not one, so a plugin that wants its prompt billed to itself uses
    /// it. The driver sets this then alone; a transport passes on what
    /// belongs to the client, such as its credential ([`credentials`]),
    /// only then. False on a response.
    pub relayed: bool,
    /// Whether the body's frames follow the head as they come: a streaming
    /// request's inputs. A transport sends such a body as it comes, else
    /// whole.
    pub streams: bool,
}

impl<H> Exchange<H> {
    /// An exchange of `head` and `body`, with no extensions, not relayed,
    /// not streaming.
    pub fn new(head: H, body: Frames) -> Self {
        Self {
            head,
            body,
            extensions: Extensions::new(),
            relayed: false,
            streams: false,
        }
    }
}

impl<H: std::fmt::Debug> std::fmt::Debug for Exchange<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Exchange")
            .field("head", &self.head)
            .field("extensions", &self.extensions)
            .field("relayed", &self.relayed)
            .field("streams", &self.streams)
            .finish_non_exhaustive()
    }
}

/// How the gateway reaches one provider over HTTP: a URL and keys, a
/// process, a socket or in-process code, with any state its calls share.
#[async_trait]
pub trait Transport: Send + Sync + 'static {
    /// Send `call` and return the provider's response, whatever it says,
    /// once its head arrives; return redirects rather than follow them.
    /// A relay's head starts from the client's, so strip what a proxy must
    /// not pass on ([`proxy_call`](crate::proxy_call)). Send a body that
    /// [`streams`](Exchange::streams) as it comes, else whole; one that ends
    /// with an `Err` aborts the message. Decide whose keys reach the
    /// provider ([`credentials`]).
    ///
    /// `Err` if the call could not be sent: [`Fault::upstream`] if the
    /// provider is unreachable, [`Fault::config`] if this transport cannot
    /// express it, or a failure of its own.
    async fn send(&self, call: Exchange<RequestHead>) -> Result<Exchange<ResponseHead>, Fault>;
}

/// A shared transport; it forwards every method.
#[async_trait]
impl<T: Transport + ?Sized> Transport for Arc<T> {
    async fn send(&self, call: Exchange<RequestHead>) -> Result<Exchange<ResponseHead>, Fault> {
        (**self).send(call).await
    }
}

/// How a codec's calls reach one provider: any [`Transport`], shared. A
/// format's [`Dialer`](respan_gateway_core::Format::Dialer).
#[derive(Clone)]
pub struct Dialer(pub(crate) Arc<dyn Transport>);

impl<T: Transport> From<T> for Dialer {
    fn from(transport: T) -> Self {
        Dialer(Arc::new(transport))
    }
}

impl std::fmt::Debug for Dialer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Dialer").finish_non_exhaustive()
    }
}

/// Strip from a call, whose head may be the client's, what a proxy does not
/// pass on: a hop's headers (RFC 9110 7.6.1) but `Upgrade`, which gets its
/// `Connection: upgrade` (7.8); `Host`; `Expect`; conditions and ranges
/// (13, 14); method overrides; and `Accept-Encoding`, as the gateway
/// decodes what it reads.
pub fn proxy_call(head: &mut http::request::Parts) {
    strip_named(&mut head.headers);
    remove_if(&mut head.headers, |n| !passes(n));
    if head.headers.contains_key(UPGRADE) {
        let upgrade = HeaderValue::from_static("upgrade");
        head.headers.insert(CONNECTION, upgrade);
    }
}

/// Whether a proxy passes on header `name` ([`proxy_call`]).
fn passes(name: &HeaderName) -> bool {
    let hop = HOP_BY_HOP.contains(name) && name != UPGRADE;
    let name = name.as_str();
    let denied = matches!(
        name,
        "host"
            | "expect"
            | "range"
            | "x-http-method-override"
            | "x-method-override"
            | "x-http-method"
            | "x-original-url"
            | "x-rewrite-url"
            | "accept-encoding"
    );
    !hop && !denied && !name.starts_with("if-")
}

/// Whether `name` is the connection's, never a transport's to send: a hop's
/// own, `Expect` or `Host`.
pub fn is_connection_header(name: &HeaderName) -> bool {
    HOP_BY_HOP.contains(name) || name == header::EXPECT || name == header::HOST
}

/// Remove from a call the headers of the gateway's origin: `Cookie`,
/// `Forwarded`, `Via`, `X-Forwarded-*` and `X-Real-IP`. Plugins still see
/// them ([`Asked`](crate::Asked)); a call's [`CallHeaders`] may set them.
pub fn strip_client_origin_headers(headers: &mut HeaderMap) {
    remove_if(headers, |name| {
        let name = name.as_str();
        matches!(name, "cookie" | "forwarded" | "via" | "x-real-ip")
            || name.starts_with("x-forwarded-")
    });
}

/// Remove from a response the headers bound to the provider's origin:
/// cookies, alternative services and HSTS.
pub fn strip_origin_headers(headers: &mut HeaderMap) {
    for name in [
        header::SET_COOKIE,
        header::ALT_SVC,
        header::STRICT_TRANSPORT_SECURITY,
    ] {
        headers.remove(name);
    }
}

/// Headers for one call, replacing the transport's own of their names, such
/// as a tenant's key set in `on_provider`.
#[derive(Clone, Default)]
pub struct CallHeaders(pub HeaderMap);

impl std::fmt::Debug for CallHeaders {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Header values may be keys.
        let names: Vec<_> = self.0.keys().collect();
        f.debug_tuple("CallHeaders").field(&names).finish()
    }
}

/// Decide whose keys `call` carries: its [`CallHeaders`] over the
/// transport's own `headers`, after removing the call's credential headers
/// and parameters; with neither, the client's [`Credential`], only on a
/// call that is `relayed`, as a key is for its own protocol's provider.
pub fn credentials(call: &mut Exchange<crate::RequestHead>, headers: &HeaderMap) {
    keys(&mut call.head, &call.extensions, call.relayed, headers);
}

/// [`credentials`] for a call of `head` and `extensions`, `relayed` or not.
pub(crate) fn keys(
    head: &mut crate::RequestHead,
    extensions: &Extensions,
    relayed: bool,
    headers: &HeaderMap,
) {
    let mut own = headers.clone();
    if let Some(CallHeaders(call)) = extensions.get::<CallHeaders>() {
        let sent = |n: &&HeaderName| !is_connection_header(n);
        for name in call.keys().filter(sent) {
            own.remove(name);
        }
        for (name, value) in call.iter().filter(|(n, _)| sent(n)) {
            own.append(name, value.clone());
        }
    }
    if own.is_empty() {
        let client = extensions.get::<Credential>().filter(|_| relayed);
        match client.cloned() {
            Some(client) => client.restore(head),
            // No key follows a subprotocol's marker (`token`), so it goes:
            // a provider refuses a marker with no key.
            None => {
                take_protocol_keys(&mut head.headers, true);
            }
        }
        return;
    }
    remove_if(&mut head.headers, |n| is_credential(n.as_str()));
    take_protocol_keys(&mut head.headers, true);
    take_query(&mut head.uri, is_credential);
    for name in own.keys() {
        head.headers.remove(name);
    }
    head.headers.extend(own);
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::*;
    use crate::RequestHead;

    fn head(headers: &[(&'static str, &'static str)]) -> http::request::Parts {
        let mut req = http::Request::new(());
        for (name, value) in headers {
            let value = HeaderValue::from_static(value);
            req.headers_mut().append(*name, value);
        }
        req.into_parts().0
    }

    #[test]
    fn a_proxy_passes_end_to_end_headers_and_upgrade() {
        let mut parts = head(&[
            ("connection", "x-hop"),
            ("x-hop", "1"),
            ("host", "gw"),
            ("expect", "100-continue"),
            ("if-none-match", "\"a\""),
            ("range", "bytes=0-1"),
            ("accept-encoding", "gzip"),
            ("x-http-method-override", "GET"),
            ("x-http-method", "GET"),
            ("x-original-url", "/v1/chat/completions"),
            ("x-rewrite-url", "/v1/chat/completions"),
            ("te", "trailers"),
            ("upgrade", "websocket"),
            ("openai-beta", "v9"),
        ]);
        proxy_call(&mut parts);
        let names: Vec<&str> = parts.headers.keys().map(HeaderName::as_str).collect();
        assert_eq!(names, ["upgrade", "openai-beta", "connection"]);
        assert_eq!(parts.headers[CONNECTION], "upgrade");
    }

    /// A call with `head` and the client's `credential`, if any.
    fn call(
        head: RequestHead,
        credential: Option<HeaderMap>,
        relayed: bool,
    ) -> Exchange<RequestHead> {
        let mut call = Exchange::new(head, futures::stream::empty().boxed());
        if let Some(client) = credential {
            call.extensions.insert(Credential::of(client));
        }
        call.relayed = relayed;
        call
    }

    #[test]
    fn with_no_keys_of_its_own_the_clients_pass_through() {
        let mut client = HeaderMap::new();
        client.insert("x-api-key", HeaderValue::from_static("k"));
        let mut relay = call(head(&[]), Some(client.clone()), true);
        credentials(&mut relay, &HeaderMap::new());
        assert_eq!(relay.head.headers["x-api-key"], "k");
        let mut own = HeaderMap::new();
        own.insert("authorization", HeaderValue::from_static("Bearer own"));
        let mut relay = call(head(&[]), Some(client), true);
        credentials(&mut relay, &own);
        assert!(relay.head.headers.get("x-api-key").is_none());
        assert_eq!(relay.head.headers["authorization"], "Bearer own");
    }

    #[test]
    fn the_clients_credentials_pass_only_through_a_relay() {
        // A translated call, or any not marked a relay, carries none of
        // the client's keys, even to a provider with none of its own.
        let mut client = HeaderMap::new();
        client.insert("authorization", HeaderValue::from_static("Bearer sk-c"));
        client.insert("x-api-key", HeaderValue::from_static("k"));
        let mut unrelayed = call(head(&[]), Some(client.clone()), false);
        credentials(&mut unrelayed, &HeaderMap::new());
        assert!(unrelayed.head.headers.is_empty());

        // A relay whose Credential a plugin removed carries none either.
        let mut removed = call(head(&[]), None, true);
        credentials(&mut removed, &HeaderMap::new());
        assert!(removed.head.headers.is_empty());

        let mut relay = call(head(&[]), Some(client), true);
        credentials(&mut relay, &HeaderMap::new());
        assert_eq!(relay.head.headers["authorization"], "Bearer sk-c");
        assert_eq!(relay.head.headers["x-api-key"], "k");
    }

    #[test]
    fn a_subprotocol_marker_goes_where_its_key_does() {
        let marked = || head(&[("sec-websocket-protocol", "realtime, token")]);
        // Own keys replace the client's: no key is left for the marker.
        let mut own = HeaderMap::new();
        own.insert("authorization", HeaderValue::from_static("Bearer own"));
        let mut keyed = call(marked(), None, true);
        credentials(&mut keyed, &own);
        assert_eq!(keyed.head.headers["sec-websocket-protocol"], "realtime");
        // No key at all, as on a call not a relay.
        let mut unrelayed = call(marked(), None, false);
        credentials(&mut unrelayed, &HeaderMap::new());
        assert_eq!(unrelayed.head.headers["sec-websocket-protocol"], "realtime");
        // The client's own key comes back after its marker.
        let mut client = HeaderMap::new();
        client.insert("sec-websocket-protocol", HeaderValue::from_static("dg"));
        let mut relay = call(marked(), Some(client), true);
        credentials(&mut relay, &HeaderMap::new());
        assert_eq!(
            relay.head.headers["sec-websocket-protocol"],
            "realtime, token, dg"
        );
    }
}
