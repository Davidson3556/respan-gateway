//! The client's credentials, lifted out of its head by the adapter.

use crate::http;
use http::header::{AUTHORIZATION, SEC_WEBSOCKET_PROTOCOL};
use http::{HeaderMap, HeaderName, HeaderValue};

/// The credential headers and query parameters a client sent, which an
/// [`ClientIo`](crate::ClientIo) lifts out of its head into the request's
/// extensions, so nothing passes them on by accident. A name is
/// one if its last `-` or `_` segment is `authorization`, `auth`, `key`,
/// `apikey`, `token`, `secret`, `password`, `credential`, `credentials`,
/// `sig` or `signature`, but not an
/// idempotency key; names are read percent-decoded. A key offered as a
/// WebSocket subprotocol (`Sec-WebSocket-Protocol: realtime,
/// openai-insecure-api-key.<key>`) is one too: an entry `name.value` whose
/// `name` is such a name is lifted out of that header, under its name, and
/// the other entries stay; so is the entry after a bare such name
/// (`token, <key>`), whose name stays.
///
/// No transport copies it ([`credentials`](crate::call::credentials)): a plugin
/// that relays the client's key gives it to the call
/// ([`CallHeaders`](crate::call::CallHeaders)) or restores it on a head it
/// writes ([`restore`](Self::restore)); a plugin that authenticates the
/// client with the gateway's own key removes it.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct Credential {
    headers: HeaderMap,
    /// Query parameters as they came, `name=value`.
    query: Vec<String>,
}

/// Names only: values are keys.
impl std::fmt::Debug for Credential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&str> = self.headers.keys().map(HeaderName::as_str).collect();
        let query: Vec<&str> = self.query.iter().map(|p| name_of(p)).collect();
        f.debug_struct("Credential")
            .field("headers", &names)
            .field("query", &query)
            .finish()
    }
}

impl Credential {
    /// Take the credentials out of `head`; `None` if it has none.
    pub fn lift(head: &mut http::request::Parts) -> Option<Credential> {
        let mut credential = Credential::default();
        let names: Vec<HeaderName> = head.headers.keys().cloned().collect();
        for name in names.into_iter().filter(|n| is_credential(n.as_str())) {
            for value in head.headers.get_all(&name) {
                credential.headers.append(name.clone(), value.clone());
            }
            head.headers.remove(&name);
        }
        if let Some(keys) = take_protocol_keys(&mut head.headers, false) {
            credential.headers.append(SEC_WEBSOCKET_PROTOCOL, keys);
        }
        credential.query = take_query(&mut head.uri, is_credential);
        (!credential.is_empty()).then_some(credential)
    }

    /// Credentials of these `headers` alone, such as a test's.
    pub fn of(headers: HeaderMap) -> Credential {
        Credential {
            headers,
            query: Vec::new(),
        }
    }

    /// Whether it holds nothing.
    pub(crate) fn is_empty(&self) -> bool {
        self.headers.is_empty() && self.query.is_empty()
    }

    /// The credential headers.
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// The value of the credential header `name`, if one came.
    pub fn get(&self, name: &str) -> Option<&HeaderValue> {
        self.headers.get(name)
    }

    /// The token of `Authorization: Bearer <token>`, if one came.
    pub fn bearer(&self) -> Option<&str> {
        let value = self.headers.get(AUTHORIZATION)?.to_str().ok()?;
        let (scheme, token) = value.split_once(' ')?;
        scheme.eq_ignore_ascii_case("bearer").then(|| token.trim())
    }

    /// The credential query parameters, as they came (`name=value`).
    pub fn query(&self) -> impl Iterator<Item = &str> {
        self.query.iter().map(String::as_str)
    }

    /// Take out the keys that `is_key` accepts from the places a browser
    /// can use for one, where it can set no header: the entries of the
    /// WebSocket subprotocol and the query parameters. Whether any went.
    /// A subprotocol entry `name.value` offers `value` as well as itself.
    /// The credential headers are not looked at: they are the caller's.
    pub fn take_keys(&mut self, is_key: impl Fn(&str) -> bool) -> bool {
        let before = self.query.len();
        self.query.retain(|param| {
            let value = param.split_once('=').map_or("", |(_, v)| v);
            !is_key(&crate::path::decoded(value))
        });
        let mut taken = self.query.len() != before;
        let entries = protocols(&self.headers);
        let kept: Vec<&str> = entries
            .iter()
            .map(String::as_str)
            .filter(|entry| {
                let named = entry
                    .split_once('.')
                    .filter(|(name, _)| is_credential(name));
                !(is_key(entry) || named.is_some_and(|(_, value)| is_key(value)))
            })
            .collect();
        if kept.len() != entries.len() {
            taken = true;
            set_protocols(&mut self.headers, &kept);
        }
        taken
    }

    /// Put its headers and parameters on `head` where it has none of a name.
    pub fn restore(&self, head: &mut http::request::Parts) {
        for name in self.headers.keys() {
            let ours = self.headers.get_all(name);
            if name == SEC_WEBSOCKET_PROTOCOL {
                // Its other entries stay: the keys join them.
                let all = head.headers.get_all(name).iter().chain(ours);
                let all: Vec<&str> = all.filter_map(|v| v.to_str().ok()).collect();
                if let Ok(all) = HeaderValue::from_str(&all.join(", ")) {
                    head.headers.insert(name.clone(), all);
                }
            } else if !head.headers.contains_key(name) {
                for value in ours {
                    head.headers.append(name.clone(), value.clone());
                }
            }
        }
        let query = head.uri.query().unwrap_or_default();
        let had: Vec<&str> = query.split('&').map(name_of).collect();
        let more: Vec<&str> = self
            .query()
            .filter(|p| !had.contains(&name_of(p)))
            .collect();
        if more.is_empty() {
            return;
        }
        let path = head.uri.path();
        let all = match query.is_empty() {
            true => more.join("&"),
            false => format!("{query}&{}", more.join("&")),
        };
        // A path and parameters that parsed parse.
        if let Ok(uri) = format!("{path}?{all}").parse() {
            head.uri = uri;
        }
    }
}

/// The name of a query parameter `name=value`.
fn name_of(param: &str) -> &str {
    param.split('=').next().unwrap_or_default()
}

/// Take the parameters whose name `is` says from `uri`'s query.
pub(crate) fn take_query(uri: &mut http::Uri, is: impl Fn(&str) -> bool) -> Vec<String> {
    let Some(query) = uri.query() else {
        return Vec::new();
    };
    let (taken, kept): (Vec<&str>, Vec<&str>) = query.split('&').partition(|p| is(name_of(p)));
    if taken.is_empty() {
        return Vec::new();
    }
    let taken = taken.into_iter().map(str::to_owned).collect();
    let path = uri.path();
    let rest = match kept.is_empty() {
        true => path.to_owned(),
        false => format!("{path}?{}", kept.join("&")),
    };
    // A path and parameters that parsed parse.
    if let Ok(rest) = rest.parse() {
        *uri = rest;
    }
    taken
}

/// The bare `Sec-WebSocket-Protocol` entries after which a browser puts a key,
/// where it can set no header: Deepgram's `token, <key>` for an API key and
/// `bearer, <jwt>` for a temporary token. An explicit list, as a name that
/// only ends in a credential word (`v1.x-auth`) marks nothing: the entry
/// after it is a protocol.
const PROTOCOL_MARKERS: [&str; 2] = ["token", "bearer"];

/// Take the entries of `Sec-WebSocket-Protocol` that carry a key out of
/// `headers`, joined, if any; empty entries go (RFC 6455 4.1 has none). An
/// entry carries one as `name.value` with a credential `name`
/// (`openai-insecure-api-key.<key>`), or as the entry after a bare
/// [`PROTOCOL_MARKERS`] name. That marker stays if `markers` is false: it is
/// no secret, it names the protocol, and it is what a server answers with.
/// It goes if `markers` is true, for a caller that puts other keys where
/// these were: a marker with no key after it makes a provider refuse.
pub(crate) fn take_protocol_keys(headers: &mut HeaderMap, markers: bool) -> Option<HeaderValue> {
    let all = protocols(headers);
    let (mut keys, mut kept) = (Vec::new(), Vec::new());
    let mut named = false;
    let mut found = false;
    for entry in all {
        let dotted = entry
            .split_once('.')
            .is_some_and(|(name, _)| is_credential(name));
        let lower = entry.to_ascii_lowercase();
        let marker = !dotted && !named && PROTOCOL_MARKERS.contains(&lower.as_str());
        if dotted || named {
            keys.push(entry.clone());
        } else if !(marker && markers) {
            kept.push(entry.clone());
        }
        found |= marker;
        named = marker;
    }
    if keys.is_empty() && !(found && markers) {
        return None;
    }
    set_protocols(headers, &kept);
    HeaderValue::from_str(&keys.join(", "))
        .ok()
        .filter(|k| !k.is_empty())
}

/// The entries of `headers`' `Sec-WebSocket-Protocol`, but empty ones (RFC
/// 6455 4.1 has none).
fn protocols(headers: &HeaderMap) -> Vec<String> {
    let entries = crate::rules::header_tokens(headers, SEC_WEBSOCKET_PROTOCOL);
    entries
        .filter(|e| !e.is_empty())
        .map(str::to_owned)
        .collect()
}

/// `headers`' `Sec-WebSocket-Protocol` made `entries`, or gone for none.
fn set_protocols(headers: &mut HeaderMap, entries: &[impl AsRef<str>]) {
    let entries: Vec<&str> = entries.iter().map(AsRef::as_ref).collect();
    match HeaderValue::from_str(&entries.join(", ")) {
        Ok(kept) if !kept.is_empty() => headers.insert(SEC_WEBSOCKET_PROTOCOL, kept),
        _ => headers.remove(SEC_WEBSOCKET_PROTOCOL),
    };
}

/// Whether a header or query parameter `name` names a credential. The
/// WebSocket handshake's own `Sec-WebSocket-*` headers (its `Key` nonce, its
/// `Accept`) are the connection's, not a secret.
pub fn is_credential(name: &str) -> bool {
    // `sig` and `signature` sign a URL: an Azure SAS's `sig`, a presigned
    // S3 URL's `X-Amz-Signature`.
    const CREDENTIALS: [&str; 11] = [
        "authorization",
        "auth",
        "key",
        "apikey",
        "token",
        "secret",
        "password",
        "credential",
        "credentials",
        "sig",
        "signature",
    ];
    let name = crate::path::decoded(name).to_ascii_lowercase();
    let last = name.rsplit(['-', '_']).next().unwrap_or_default();
    !name.contains("idempotency")
        && !name.starts_with("sec-websocket-")
        && CREDENTIALS.contains(&last)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn head(uri: &str, headers: &[(&'static str, &'static str)]) -> http::request::Parts {
        let mut req = http::Request::new(());
        *req.uri_mut() = uri.parse().unwrap_or_default();
        for (name, value) in headers {
            let value = HeaderValue::from_static(value);
            req.headers_mut().append(*name, value);
        }
        req.into_parts().0
    }

    #[test]
    fn keys_a_browser_offers_are_taken_and_the_rest_stays() {
        let mut parts = head(
            "/v1/x?key=gw&access_token=other&alt=sse",
            &[
                (
                    "sec-websocket-protocol",
                    "realtime, openai-insecure-api-key.gw",
                ),
                ("authorization", "Bearer sk-1"),
            ],
        );
        let mut credential = Credential::lift(&mut parts).unwrap_or_default();
        assert!(credential.take_keys(|k| k == "gw"));
        assert_eq!(
            credential.query().collect::<Vec<_>>(),
            ["access_token=other"]
        );
        assert_eq!(credential.get("sec-websocket-protocol"), None);
        assert_eq!(credential.bearer(), Some("sk-1"));
        assert!(!credential.take_keys(|k| k == "gw"));
        credential.restore(&mut parts);
        assert_eq!(parts.uri.query(), Some("alt=sse&access_token=other"));
        assert_eq!(parts.headers["sec-websocket-protocol"], "realtime");
    }

    #[test]
    fn credentials_are_lifted_and_put_back() {
        let mut parts = head(
            "/v1/x?key=k1&alt=sse",
            &[
                ("authorization", "Bearer sk-1"),
                ("x-api-key", "k2"),
                ("idempotency-key", "i"),
                ("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ=="),
                ("sec-websocket-accept", "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="),
                ("sec-websocket-extensions", "permessage-deflate"),
                ("x-custom", "c"),
            ],
        );
        let credential = Credential::lift(&mut parts);
        let Some(credential) = credential else {
            panic!("no credential lifted")
        };
        assert_eq!(credential.bearer(), Some("sk-1"));
        assert_eq!(credential.query().collect::<Vec<_>>(), ["key=k1"]);
        assert!(parts.headers.get("authorization").is_none());
        assert!(parts.headers.get("x-api-key").is_none());
        assert_eq!(parts.headers["idempotency-key"], "i");
        for handshake in ["key", "accept", "extensions"] {
            let name = format!("sec-websocket-{handshake}");
            assert!(parts.headers.contains_key(&name), "{name} was lifted");
            assert!(credential.get(&name).is_none(), "{name} is a credential");
        }
        assert_eq!(parts.uri, "/v1/x?alt=sse");
        let debug = format!("{credential:?}");
        assert!(!debug.contains("sk-1") && debug.contains("authorization"));
        credential.restore(&mut parts);
        assert_eq!(parts.headers["authorization"], "Bearer sk-1");
        assert_eq!(parts.uri, "/v1/x?alt=sse&key=k1");
    }

    #[test]
    fn a_subprotocol_key_is_lifted_and_its_other_entries_stay() {
        let mut parts = head(
            "/v1/realtime",
            &[(
                "sec-websocket-protocol",
                "realtime, openai-insecure-api-key.sk-1, openai-beta.realtime-v1",
            )],
        );
        let credential = Credential::lift(&mut parts).unwrap_or_default();
        assert_eq!(
            parts.headers["sec-websocket-protocol"],
            "realtime, openai-beta.realtime-v1"
        );
        assert_eq!(
            credential
                .get("sec-websocket-protocol")
                .map(HeaderValue::as_bytes),
            Some(&b"openai-insecure-api-key.sk-1"[..])
        );
        assert!(!format!("{credential:?}").contains("sk-1"));
        credential.restore(&mut parts);
        assert_eq!(
            parts.headers["sec-websocket-protocol"],
            "realtime, openai-beta.realtime-v1, openai-insecure-api-key.sk-1"
        );
        // Alone, the header goes whole.
        let mut parts = head("/", &[("sec-websocket-protocol", "x-key.k")]);
        assert!(Credential::lift(&mut parts).is_some());
        assert!(parts.headers.get("sec-websocket-protocol").is_none());
    }

    #[test]
    fn a_bare_subprotocol_key_after_a_credential_name_is_lifted_and_the_name_stays() {
        let mut parts = head("/", &[("sec-websocket-protocol", "token, dg-secret")]);
        let credential = Credential::lift(&mut parts).unwrap_or_default();
        assert_eq!(parts.headers["sec-websocket-protocol"], "token");
        let keys = credential.get("sec-websocket-protocol");
        assert_eq!(keys.map(HeaderValue::as_bytes), Some(&b"dg-secret"[..]));
        assert!(!format!("{credential:?}").contains("dg-secret"));
        credential.restore(&mut parts);
        assert_eq!(parts.headers["sec-websocket-protocol"], "token, dg-secret");
    }

    #[test]
    fn empty_subprotocol_entries_go_with_a_lift() {
        let mut parts = head(
            "/",
            &[(
                "sec-websocket-protocol",
                "realtime, , openai-insecure-api-key.k",
            )],
        );
        assert!(Credential::lift(&mut parts).is_some());
        assert_eq!(parts.headers["sec-websocket-protocol"], "realtime");
    }

    #[test]
    fn an_idempotency_key_is_no_credential_in_any_spelling() {
        let mut parts = head(
            "/x?idempotency_key=a&Idempotency-Key=b",
            &[("idempotency_key", "a"), ("x-idempotency-key", "b")],
        );
        assert_eq!(Credential::lift(&mut parts), None);
    }

    #[test]
    fn apikey_and_percent_encoded_names_are_credentials() {
        let mut parts = head(
            "/v1/x?apikey=a&api%5Fkey=b&%6Bey=c&alt=sse",
            &[("x-apikey", "d")],
        );
        let credential = Credential::lift(&mut parts).unwrap_or_default();
        assert_eq!(parts.uri, "/v1/x?alt=sse");
        assert_eq!(credential.query().count(), 3);
        assert!(credential.get("x-apikey").is_some());
    }

    #[test]
    fn a_url_signature_is_a_credential() {
        let mut parts = head(
            "/blob?sv=2024&sig=abc&X-Amz-Signature=def&X-Amz-Date=1",
            &[],
        );
        let credential = Credential::lift(&mut parts).unwrap_or_default();
        assert_eq!(parts.uri, "/blob?sv=2024&X-Amz-Date=1");
        assert_eq!(credential.query().count(), 2);
    }

    #[test]
    fn a_head_without_credentials_lifts_none() {
        let mut parts = head("/v1/x?alt=sse", &[("x-custom", "c")]);
        assert_eq!(Credential::lift(&mut parts), None);
    }

    #[test]
    fn only_a_marker_name_has_the_next_entry_lifted() {
        // Deepgram's temporary token: `bearer, <jwt>`.
        let mut parts = head("/", &[("sec-websocket-protocol", "bearer, eyJ.jwt.sig")]);
        assert!(Credential::lift(&mut parts).is_some());
        assert_eq!(parts.headers["sec-websocket-protocol"], "bearer");
        // A name that merely ends in a credential word marks nothing.
        let mut parts = head("/", &[("sec-websocket-protocol", "v1.x-auth, realtime")]);
        assert!(Credential::lift(&mut parts).is_none());
        assert_eq!(
            parts.headers["sec-websocket-protocol"],
            "v1.x-auth, realtime"
        );
        // A marker at the end has no key to lift.
        let mut parts = head("/", &[("sec-websocket-protocol", "realtime, token")]);
        assert!(Credential::lift(&mut parts).is_none());
        assert_eq!(parts.headers["sec-websocket-protocol"], "realtime, token");
        // The key on the next header line.
        let mut parts = head(
            "/",
            &[
                ("sec-websocket-protocol", "token"),
                ("sec-websocket-protocol", "dg-secret"),
            ],
        );
        assert!(Credential::lift(&mut parts).is_some());
        assert_eq!(parts.headers["sec-websocket-protocol"], "token");
    }
}
