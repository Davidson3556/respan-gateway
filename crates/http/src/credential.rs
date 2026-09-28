//! The client's credentials, lifted out of its head by the adapter.

use http::header::AUTHORIZATION;
use http::{HeaderMap, HeaderName, HeaderValue};
use respan_gateway_core::http;

/// The credentials a client sent, which the adapter lifts out of its head
/// ([`handle`](crate::handle)) so no plugin, format or relay passes them
/// on by accident: its credential headers and query parameters. A header
/// or parameter is one if the last `-` or `_` segment of its name is
/// `authorization`, `auth`, `key`, `token`, `secret`, `password`,
/// `credential` or `credentials`, but not an idempotency key.
///
/// It sits in the request's extensions, which calls copy. A transport
/// without keys of its own passes it through ([`credentials`](crate::credentials));
/// a plugin that authenticates the client with it removes it
/// (`cx.extensions_mut().remove::<Credential>()`), so the gateway's own key
/// never reaches a provider.
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
    pub fn is_empty(&self) -> bool {
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

    /// Put its headers and parameters on `head`, each where `head` has
    /// none of its name.
    pub(crate) fn restore(&self, head: &mut http::request::Parts) {
        for name in self.headers.keys() {
            if !head.headers.contains_key(name) {
                for value in self.headers.get_all(name) {
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

/// Whether a header or query parameter `name` names a credential.
pub(crate) fn is_credential(name: &str) -> bool {
    const CREDENTIALS: [&str; 8] = [
        "authorization",
        "auth",
        "key",
        "token",
        "secret",
        "password",
        "credential",
        "credentials",
    ];
    let name = name.to_ascii_lowercase();
    let last = name.rsplit(['-', '_']).next().unwrap_or_default();
    let idempotency = matches!(&*name, "idempotency-key" | "x-idempotency-key");
    !idempotency && CREDENTIALS.contains(&last)
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
    fn credentials_are_lifted_and_put_back() {
        let mut parts = head(
            "/v1/x?key=k1&alt=sse",
            &[
                ("authorization", "Bearer sk-1"),
                ("x-api-key", "k2"),
                ("idempotency-key", "i"),
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
        assert_eq!(parts.uri, "/v1/x?alt=sse");
        let debug = format!("{credential:?}");
        assert!(!debug.contains("sk-1") && debug.contains("authorization"));
        credential.restore(&mut parts);
        assert_eq!(parts.headers["authorization"], "Bearer sk-1");
        assert_eq!(parts.uri, "/v1/x?alt=sse&key=k1");
    }

    #[test]
    fn a_head_without_credentials_lifts_none() {
        let mut parts = head("/v1/x?alt=sse", &[("x-custom", "c")]);
        assert_eq!(Credential::lift(&mut parts), None);
    }
}
