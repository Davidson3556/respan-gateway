//! What an HTTP transport does to a call and its response.

use http::header::{self, CONNECTION, HeaderName, UPGRADE};
use http::{HeaderMap, HeaderValue};
use respan_gateway_core::http;

use crate::credential::{Credential, is_credential, take_query};
use crate::{is_hop_by_hop, strip_named};

/// Strip from a call what a proxy does not pass on, as its head may be the
/// client's ([`Format::request_encoder`](respan_gateway_core::Format::request_encoder)):
/// the headers its `Connection` names and a hop's own (RFC 9110 7.6.1),
/// `TE` included, but `Upgrade`, which gets the `Connection: upgrade` it
/// needs (7.8); `Host`; `Expect`; conditions and ranges (13, 14), which
/// ask of the client's copy; method overrides, as the format says the
/// method; and `Accept-Encoding`, as the gateway decodes what it reads.
pub fn proxy_call(head: &mut http::request::Parts) {
    strip_named(&mut head.headers);
    let names: Vec<HeaderName> = head.headers.keys().cloned().collect();
    for name in names.iter().filter(|n| !passes(n)) {
        head.headers.remove(name);
    }
    if head.headers.contains_key(UPGRADE) {
        let upgrade = HeaderValue::from_static("upgrade");
        head.headers.insert(CONNECTION, upgrade);
    }
}

/// Whether a proxy passes on header `name` ([`proxy_call`]).
fn passes(name: &HeaderName) -> bool {
    let hop = is_hop_by_hop(name) && name != UPGRADE;
    let name = name.as_str();
    let denied = matches!(
        name,
        "host"
            | "expect"
            | "range"
            | "x-http-method-override"
            | "x-method-override"
            | "accept-encoding"
    );
    !hop && !denied && !name.starts_with("if-")
}

/// Whether `name` is a header of the connection, never a transport's own
/// to send: a hop's own, `Expect` or `Host`.
pub fn is_connection_header(name: &HeaderName) -> bool {
    is_hop_by_hop(name) || name == header::EXPECT || name == header::HOST
}

/// Remove the headers of the client's hop to the gateway from a call, as
/// they belong to the gateway's origin, not the provider's: `Cookie` and
/// the forwarding headers (`Forwarded`, RFC 7239; `Via`; `X-Forwarded-*`;
/// `X-Real-IP`). Plugins still see them ([`Asked`](crate::Asked)); a call's
/// [`CallHeaders`], written after, may set them again.
pub fn strip_client_origin_headers(headers: &mut HeaderMap) {
    let names: Vec<HeaderName> = headers
        .keys()
        .filter(|name| {
            let name = name.as_str();
            matches!(name, "cookie" | "forwarded" | "via" | "x-real-ip")
                || name.starts_with("x-forwarded-")
        })
        .cloned()
        .collect();
    for name in names {
        headers.remove(name);
    }
}

/// Remove a provider origin's own headers from its response: cookies,
/// alternative services and HSTS bind to the origin, which the gateway is not.
pub fn strip_origin_headers(headers: &mut HeaderMap) {
    for name in [
        header::SET_COOKIE,
        header::ALT_SVC,
        header::STRICT_TRANSPORT_SECURITY,
    ] {
        headers.remove(name);
    }
}

/// Headers for one call, sent as the transport's own and replacing those
/// of the same name a transport has, such as a tenant's key set in
/// `on_provider` (`px.extensions_mut().insert(CallHeaders(headers))`).
#[derive(Clone, Default)]
pub struct CallHeaders(pub HeaderMap);

impl std::fmt::Debug for CallHeaders {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Header values may be keys.
        let names: Vec<_> = self.0.keys().collect();
        f.debug_tuple("CallHeaders").field(&names).finish()
    }
}

/// Decide whose keys a call carries: the call's [`CallHeaders`] (minus the
/// connection's headers), else the transport's own `headers`, each
/// replacing any of the same name, with any credential header or query
/// parameter the call has removed first ([`Credential`] says which); else,
/// with neither, the client's [`Credential`] from the call's extensions,
/// passed through.
pub fn credentials(head: &mut http::request::Parts, headers: &HeaderMap) {
    let mut own = headers.clone();
    if let Some(CallHeaders(call)) = head.extensions.get::<CallHeaders>() {
        let sent = |n: &&HeaderName| !is_connection_header(n);
        for name in call.keys().filter(sent) {
            own.remove(name);
        }
        for (name, value) in call.iter().filter(|(n, _)| sent(n)) {
            own.append(name, value.clone());
        }
    }
    if own.is_empty() {
        if let Some(client) = head.extensions.get::<Credential>().cloned() {
            client.restore(head);
        }
        return;
    }
    let names: Vec<HeaderName> = head.headers.keys().cloned().collect();
    for name in names.iter().filter(|n| is_credential(n.as_str())) {
        head.headers.remove(name);
    }
    take_query(&mut head.uri, is_credential);
    for name in own.keys() {
        head.headers.remove(name);
    }
    head.headers.extend(own);
}

#[cfg(test)]
mod tests {
    use super::*;

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
            ("te", "trailers"),
            ("upgrade", "websocket"),
            ("openai-beta", "v9"),
        ]);
        proxy_call(&mut parts);
        let names: Vec<&str> = parts.headers.keys().map(HeaderName::as_str).collect();
        assert_eq!(names, ["upgrade", "openai-beta", "connection"]);
        assert_eq!(parts.headers[CONNECTION], "upgrade");
    }

    #[test]
    fn with_no_keys_of_its_own_the_clients_pass_through() {
        let mut parts = head(&[]);
        let mut client = HeaderMap::new();
        client.insert("x-api-key", HeaderValue::from_static("k"));
        parts.extensions.insert(Credential::of(client));
        credentials(&mut parts, &HeaderMap::new());
        assert_eq!(parts.headers["x-api-key"], "k");
        let mut own = HeaderMap::new();
        own.insert("authorization", HeaderValue::from_static("Bearer own"));
        let mut parts = head(&[]);
        let mut client = HeaderMap::new();
        client.insert("x-api-key", HeaderValue::from_static("k"));
        parts.extensions.insert(Credential::of(client));
        credentials(&mut parts, &own);
        assert!(parts.headers.get("x-api-key").is_none());
        assert_eq!(parts.headers["authorization"], "Bearer own");
    }
}
