//! What an HTTP transport does to a call and its response.

use http::header::{self, CONNECTION, HeaderName, UPGRADE};
use http::{HeaderMap, HeaderValue};
use respan_gateway_core::{Exchange, Extensions};

use crate::credential::{Credential, is_credential, take_query};
use crate::{HOP_BY_HOP, remove_if, strip_named};

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
pub fn credentials(call: &mut Exchange<crate::Ask>, headers: &HeaderMap) {
    keys(&mut call.head, &call.extensions, call.relayed, headers);
}

/// [`credentials`] for a call of `head` and `extensions`, `relayed` or not.
pub(crate) fn keys(
    head: &mut crate::Ask,
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
        if let Some(client) = client.cloned() {
            client.restore(head);
        }
        return;
    }
    remove_if(&mut head.headers, |n| is_credential(n.as_str()));
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
    use crate::Ask;

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

    /// A call with `head` and the client's `credential`, if any.
    fn call(head: Ask, credential: Option<HeaderMap>, relayed: bool) -> Exchange<Ask> {
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
}
