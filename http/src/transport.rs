//! How the gateway reaches a provider over HTTP ([`connect`]),
//! and what a transport does to a call and its response.

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use http::header::{self, CONNECTION, HeaderName, UPGRADE};
use http::{HeaderMap, HeaderValue, request::Parts as RequestHead};
use respan_gateway_sdk::{Extensions, Fault};

use crate::credential::{is_credential, take_protocol_keys, take_query};
use crate::{HOP_BY_HOP, remove_if, strip_named};

/// A body, its bytes one item per data frame, ending with an `Err` if it
/// broke off.
pub type Frames = BoxStream<'static, Result<Bytes, Fault>>;

/// Actual completion of an outgoing transfer, independent of response headers.
pub type Completion = BoxFuture<'static, Result<(), Fault>>;

/// Make a cloneable provider factory from an ordinary async sending function.
/// `Some(completion)` remains authoritative even after response EOF; `None`
/// means only the outgoing handoff is complete, not later socket writes.
/// `limit` is the most bytes a reader holds of one answer at once
/// ([`Body::limit`](crate::Body::limit)): a whole body, a refusal's, or one
/// event; past it, the answer fails `upstream`.
pub fn connect<F, Fut>(
    send: F,
    limit: usize,
) -> impl Fn(Extensions) -> crate::Outgoing + Clone + Send + Sync + 'static
where
    F: Fn(http::Request<crate::Body>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(http::Response<crate::Body>, Option<Completion>), Fault>>
        + Send
        + 'static,
{
    use futures::FutureExt;
    let send: std::sync::Arc<crate::driver::Sending> =
        std::sync::Arc::new(move |request| send(request).boxed());
    move |extensions| crate::Outgoing {
        send: send.clone(),
        extensions,
        limit,
    }
}

/// `url` as a transport's base URL, without a trailing `/`: one of
/// `schemes`, a host (a name, an IPv4 address, or an IPv6 one in brackets)
/// with an optional port other than 0, an optional path prefix such as
/// `/v1`, and no query, fragment or credentials, else `Err` with `config`.
/// The error does not echo the URL, which may hold a key.
pub fn base_url(url: &str, schemes: &[&str]) -> Result<String, Fault> {
    let host = |host: &str| match host.strip_prefix('[') {
        Some(v6) => v6
            .strip_suffix(']')
            .is_some_and(|v6| v6.parse::<std::net::Ipv6Addr>().is_ok()),
        None if host.bytes().all(|b| b.is_ascii_digit() || b == b'.') => {
            host.parse::<std::net::Ipv4Addr>().is_ok()
        }
        None => host
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.'),
    };
    let parsed = url.parse::<http::Uri>().ok().filter(|u| {
        let authority = u.authority().map_or("", |a| a.as_str());
        // A port follows the host, past an IPv6 address's brackets.
        let after_host = authority
            .rsplit_once(']')
            .map_or(authority, |(_, rest)| rest);
        let port = after_host
            .split_once(':')
            .map(|(_, port)| port.parse::<u16>());
        u.scheme_str().is_some_and(|s| schemes.contains(&s))
            && u.host().is_some_and(|h| !h.is_empty() && host(h))
            && !authority.contains('@')
            && port.is_none_or(|p| p.is_ok_and(|p| p != 0))
            && u.query().is_none()
            && !url.contains('#')
    });
    match parsed {
        Some(url) => Ok(url.to_string().trim_end_matches('/').to_owned()),
        None => Err(Fault::config(format!(
            "bad base URL: {} with a host, and no query, fragment or credentials \
             (those go in its headers)",
            schemes.join(" or "),
        ))),
    }
}

/// Make `call` the request a transport sends to `base` ([`base_url`]): what
/// a proxy does not pass on stripped ([`proxy_call`]) with the gateway
/// origin's headers ([`strip_client_origin_headers`]), its keys decided
/// ([`credentials`]) over the transport's `headers`, and its path and
/// query joined to `base`. `Err` with `config` if they make no URL.
pub fn prepare_call(
    call: &mut http::Request<crate::Body>,
    base: &str,
    headers: &HeaderMap,
) -> Result<(), Fault> {
    let (mut head, body) = std::mem::take(call).into_parts();
    proxy_call(&mut head);
    strip_client_origin_headers(&mut head.headers);
    let extensions = head.extensions.get::<Extensions>().cloned();
    keys(&mut head, &extensions.unwrap_or_default(), headers);
    let path = head.uri.path_and_query().map_or("/", |p| p.as_str());
    let uri = format!("{base}{path}").parse::<http::Uri>();
    *call = http::Request::from_parts(head, body);
    *call.uri_mut() =
        uri.map_err(|e| Fault::config("the provider request is not valid").with_source(e))?;
    Ok(())
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
/// them ([`Wire`](crate::Wire)); a call's [`CallHeaders`] may set them.
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
/// transport's own `headers`, replacing credentials already in the head.
/// With neither, preserve only credentials explicitly supplied in the head.
/// The exchange's `Credential` extension is never copied automatically.
pub fn credentials(call: &mut http::Request<crate::Body>, headers: &HeaderMap) {
    let extensions = call
        .extensions()
        .get::<Extensions>()
        .cloned()
        .unwrap_or_default();
    let (mut head, body) = std::mem::take(call).into_parts();
    keys(&mut head, &extensions, headers);
    *call = http::Request::from_parts(head, body);
}

/// [`credentials`] for a call of `head` and `extensions`.
pub(crate) fn keys(head: &mut RequestHead, extensions: &Extensions, headers: &HeaderMap) {
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
        // The only credentials already present are deliberately supplied by
        // the caller (for example in SendRaw); exchange Credential is ignored.
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

    use super::*;
    use crate::Credential;

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

    /// A call with `head` and the client's `credential`, if any, put back
    /// on its head if `restored`, as a plugin that relays it does.
    fn call(
        head: RequestHead,
        credential: Option<HeaderMap>,
        restored: bool,
    ) -> http::Request<crate::Body> {
        let mut call = http::Request::from_parts(head, crate::Body::default());
        let mut extensions = Extensions::new();
        if let Some(client) = credential {
            extensions.insert(Credential::of(client));
        }
        call.extensions_mut().insert(extensions);
        if restored {
            let (mut head, body) = call.into_parts();
            if let Some(credential) = head
                .extensions
                .get::<Extensions>()
                .and_then(|e| e.get::<Credential>())
                .cloned()
            {
                credential.restore(&mut head);
            }
            call = http::Request::from_parts(head, body);
        }
        call
    }

    #[test]
    fn with_no_keys_of_its_own_the_heads_pass_through() {
        let mut client = HeaderMap::new();
        client.insert("x-api-key", HeaderValue::from_static("k"));
        let mut restored = call(head(&[]), Some(client.clone()), true);
        credentials(&mut restored, &HeaderMap::new());
        assert_eq!(restored.headers()["x-api-key"], "k");
        let mut own = HeaderMap::new();
        own.insert("authorization", HeaderValue::from_static("Bearer own"));
        let mut restored = call(head(&[]), Some(client), true);
        credentials(&mut restored, &own);
        assert!(restored.headers().get("x-api-key").is_none());
        assert_eq!(restored.headers()["authorization"], "Bearer own");
    }

    #[test]
    fn the_clients_credentials_pass_only_where_a_plugin_restored_them() {
        // A call whose head has none of the client's keys carries none,
        // even to a provider with none of its own: the Credential
        // extension is never copied.
        let mut client = HeaderMap::new();
        client.insert("authorization", HeaderValue::from_static("Bearer sk-c"));
        client.insert("x-api-key", HeaderValue::from_static("k"));
        let mut kept = call(head(&[]), Some(client.clone()), false);
        credentials(&mut kept, &HeaderMap::new());
        assert!(kept.headers().is_empty());

        // A Credential a plugin removed is restored by none either.
        let mut removed = call(head(&[]), None, true);
        credentials(&mut removed, &HeaderMap::new());
        assert!(removed.headers().is_empty());

        let mut restored = call(head(&[]), Some(client), true);
        credentials(&mut restored, &HeaderMap::new());
        assert_eq!(restored.headers()["authorization"], "Bearer sk-c");
        assert_eq!(restored.headers()["x-api-key"], "k");
    }

    #[test]
    fn a_subprotocol_marker_goes_where_its_key_does() {
        let marked = || head(&[("sec-websocket-protocol", "realtime, token")]);
        // Own keys replace the client's: no key is left for the marker.
        let mut own = HeaderMap::new();
        own.insert("authorization", HeaderValue::from_static("Bearer own"));
        let mut keyed = call(marked(), None, true);
        credentials(&mut keyed, &own);
        assert_eq!(keyed.headers()["sec-websocket-protocol"], "realtime");
        // No key at all, as on a call whose head has none.
        let mut kept = call(marked(), None, false);
        credentials(&mut kept, &HeaderMap::new());
        assert_eq!(kept.headers()["sec-websocket-protocol"], "realtime, token");
        // The client's own key comes back after its marker.
        let mut client = HeaderMap::new();
        client.insert("sec-websocket-protocol", HeaderValue::from_static("dg"));
        let mut restored = call(marked(), Some(client), true);
        credentials(&mut restored, &HeaderMap::new());
        assert_eq!(
            restored.headers()["sec-websocket-protocol"],
            "realtime, token, dg"
        );
    }
}
