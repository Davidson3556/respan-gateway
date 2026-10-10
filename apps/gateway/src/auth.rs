//! Whose keys go where. Client keys: when the config sets `keys`, every
//! request carries one, as `Authorization: Bearer`, `x-api-key` or
//! `x-goog-api-key`, and the gateway takes it off: no provider sees it.
//! Clients' own provider keys go only to a provider of their format that
//! has none of its own.

use std::collections::HashSet;
use std::sync::Arc;

use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use respan_gateway_core::{Fault, Plugin, ProviderContext, async_trait};
use respan_gateway_http::http::header::{AUTHORIZATION, HeaderName, SEC_WEBSOCKET_PROTOCOL};
use respan_gateway_http::http::{HeaderMap, StatusCode};
use respan_gateway_http::{CallHeaders, Credential};

const X_API_KEY: HeaderName = HeaderName::from_static("x-api-key");
const X_GOOG_API_KEY: HeaderName = HeaderName::from_static("x-goog-api-key");

/// Sends a client's own credential headers on a call to a provider of the
/// client's format that has no key of its own, as the client would have
/// sent them to the vendor. No key crosses a translation.
#[derive(Debug)]
pub struct ClientCredentials {
    /// The core providers without a key of their own.
    keyless: HashSet<String>,
}

impl ClientCredentials {
    pub fn new(keyless: HashSet<String>) -> Self {
        Self { keyless }
    }
}

#[async_trait]
impl Plugin for ClientCredentials {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        let own_format = px.format() == px.client().format();
        if !own_format || !self.keyless.contains(px.provider()) {
            return Ok(());
        }
        let Some(credential) = px.extensions().get::<Credential>() else {
            return Ok(());
        };
        let mut headers = credential.headers().clone();
        // A subprotocol is the session's to offer, keys and all.
        headers.remove(SEC_WEBSOCKET_PROTOCOL);
        if !headers.is_empty() {
            px.extensions_mut().insert(CallHeaders(headers));
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct ClientKeys(Vec<String>);

impl ClientKeys {
    pub fn new(keys: Vec<String>) -> Arc<Self> {
        Arc::new(Self(keys))
    }

    pub fn required(&self) -> bool {
        !self.0.is_empty()
    }

    /// The header that carries a valid key, if one does; `Some(None)` when
    /// no key is needed.
    fn holder(&self, headers: &HeaderMap) -> Option<Option<HeaderName>> {
        if !self.required() {
            return Some(None);
        }
        let valid = |key: &str| {
            self.0
                .iter()
                .any(|k| constant_time_eq(k.as_bytes(), key.trim().as_bytes()))
        };
        let value = |name: &HeaderName| headers.get(name).and_then(|v| v.to_str().ok());
        let bearer = value(&AUTHORIZATION).and_then(|v| {
            let (scheme, token) = v.split_once(' ')?;
            scheme.eq_ignore_ascii_case("bearer").then_some(token)
        });
        if bearer.is_some_and(valid) {
            return Some(Some(AUTHORIZATION));
        }
        [X_API_KEY, X_GOOG_API_KEY]
            .into_iter()
            .find(|name| value(name).is_some_and(valid))
            .map(Some)
    }

    pub fn allows(&self, headers: &HeaderMap) -> bool {
        self.holder(headers).is_some()
    }
}

/// Whether `a` and `b` are equal, in time that depends on their lengths alone.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// The 401 of a request without a valid key, in OpenAI's error shape.
pub fn unauthorized() -> Response {
    let body = serde_json::json!({"error": {
        "message": "a valid gateway key is required: send it as `Authorization: Bearer <key>`, `x-api-key` or `x-goog-api-key`",
        "type": "authentication_error",
    }});
    (StatusCode::UNAUTHORIZED, axum::Json(body)).into_response()
}

/// Refuses a request without a valid key, and takes the key off one with.
pub async fn consume(
    State(keys): State<Arc<ClientKeys>>,
    mut req: Request,
    next: Next,
) -> Response {
    match keys.holder(req.headers()) {
        None => unauthorized(),
        Some(holder) => {
            if let Some(name) = holder {
                req.headers_mut().remove(name);
            }
            next.run(req).await
        }
    }
}

/// Refuses a request without a valid key, leaving it as it is.
pub async fn require(State(keys): State<Arc<ClientKeys>>, req: Request, next: Next) -> Response {
    match keys.allows(req.headers()) {
        true => next.run(req).await,
        false => unauthorized(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&'static str, &str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in pairs {
            let value = value.parse().unwrap_or_else(|_| panic!("{value}"));
            headers.insert(*name, value);
        }
        headers
    }

    #[test]
    fn a_key_is_read_from_any_of_three_headers() {
        let keys = ClientKeys(vec!["k1".into(), "k2".into()]);
        let holder = |pairs: &[(&'static str, &str)]| keys.holder(&headers(pairs));
        assert_eq!(
            holder(&[("authorization", "Bearer k1")]),
            Some(Some(AUTHORIZATION))
        );
        assert_eq!(holder(&[("x-api-key", "k2")]), Some(Some(X_API_KEY)));
        assert_eq!(
            holder(&[("x-goog-api-key", "k1")]),
            Some(Some(X_GOOG_API_KEY))
        );
        // The client's provider key in one header, the gateway's in another.
        let both = [("authorization", "Bearer sk-provider"), ("x-api-key", "k1")];
        assert_eq!(holder(&both), Some(Some(X_API_KEY)));
        assert_eq!(holder(&[("authorization", "Bearer k3")]), None);
        assert_eq!(holder(&[("authorization", "k1")]), None);
        assert_eq!(holder(&[]), None);
    }

    #[test]
    fn without_keys_everything_passes() {
        let keys = ClientKeys::default();
        assert!(!keys.required());
        assert_eq!(keys.holder(&HeaderMap::new()), Some(None));
    }
}
