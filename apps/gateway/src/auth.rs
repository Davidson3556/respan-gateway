//! Whose keys go where. Client keys: when the config sets `client_keys`, every
//! request carries one, as `Authorization: Bearer`, `x-api-key` or
//! `x-goog-api-key`, and the gateway takes it off: no provider sees it. A
//! browser's WebSocket can set no header, so on an upgrade a key may also
//! come as a subprotocol (`realtime, openai-insecure-api-key.<key>`) or as a
//! query parameter (`?key=<key>`), and goes from there likewise.
//! Clients' own provider keys go only to a provider of their format that
//! has none of its own.

use std::collections::HashSet;
use std::hash::{BuildHasher, RandomState};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, OnceLock};

use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use respan_gateway_core::{Extensions, Fault, Plugin, ProviderContext, async_trait};
use respan_gateway_http::http::header::{
    AUTHORIZATION, HOST, HeaderName, ORIGIN, SEC_WEBSOCKET_PROTOCOL, UPGRADE,
};
use respan_gateway_http::http::{HeaderMap, HeaderValue, Method, StatusCode};
use respan_gateway_http::{CallHeaders, Credential};
use respan_gateway_mcp_http::Owner;

const X_API_KEY: HeaderName = HeaderName::from_static("x-api-key");
const X_GOOG_API_KEY: HeaderName = HeaderName::from_static("x-goog-api-key");
const SEC_FETCH_SITE: HeaderName = HeaderName::from_static("sec-fetch-site");

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

/// An opaque id of a client key: a keyed hash, different for every process,
/// never the key. The gateway hands it on as the call's [`Owner`], for
/// whatever must tell clients apart, such as whose an MCP session is.
fn client_id(key: &str) -> Owner {
    static SALT: OnceLock<RandomState> = OnceLock::new();
    let hash = SALT.get_or_init(RandomState::new).hash_one(key.trim());
    Owner(format!("{hash:016x}"))
}

/// What a request's headers say to the client keys.
#[derive(Debug, PartialEq, Eq)]
enum Presented {
    /// No key is needed.
    Open,
    /// The headers that hold a valid key, each one to take off.
    Valid(Vec<HeaderName>),
    Refused,
}

impl ClientKeys {
    pub fn new(keys: Vec<String>) -> Arc<Self> {
        Arc::new(Self(keys))
    }

    pub fn required(&self) -> bool {
        !self.0.is_empty()
    }

    /// Whether `key` is one of the client keys.
    fn is_key(&self, key: &str) -> bool {
        let key = key.trim().as_bytes();
        !key.is_empty() && self.0.iter().any(|k| constant_time_eq(k.as_bytes(), key))
    }

    fn presented(&self, headers: &HeaderMap) -> Presented {
        if !self.required() {
            return Presented::Open;
        }
        let valid = |key: &str| self.is_key(key);
        let value = |name: &HeaderName| headers.get(name).and_then(|v| v.to_str().ok());
        let holds_key = |name: &HeaderName| match *name == AUTHORIZATION {
            true => value(name)
                .and_then(|v| v.split_once(' '))
                .is_some_and(|(scheme, token)| {
                    scheme.eq_ignore_ascii_case("bearer") && valid(token)
                }),
            false => value(name).is_some_and(valid),
        };
        let holders: Vec<HeaderName> = [AUTHORIZATION, X_API_KEY, X_GOOG_API_KEY]
            .into_iter()
            .filter(holds_key)
            .collect();
        match holders.is_empty() {
            true => Presented::Refused,
            false => Presented::Valid(holders),
        }
    }

    /// The id of the client key `headers` present, if any.
    fn identify(&self, headers: &HeaderMap) -> Option<Owner> {
        let bearer = headers.get(AUTHORIZATION).and_then(|v| v.to_str().ok());
        let bearer = bearer
            .and_then(|v| v.split_once(' '))
            .map(|(_, token)| token);
        let others = [X_API_KEY, X_GOOG_API_KEY]
            .into_iter()
            .filter_map(|name| headers.get(name)?.to_str().ok());
        let mut presented = bearer.into_iter().chain(others);
        presented.find(|key| self.is_key(key)).map(client_id)
    }

    pub fn allows(&self, headers: &HeaderMap) -> bool {
        self.presented(headers) != Presented::Refused
    }
}

/// Whether `a` and `b` are equal, in time that depends on their lengths alone.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// The 401 of a request without a valid key, in OpenAI's error shape.
pub fn unauthorized() -> Response {
    let message = "a valid gateway key is required: send it as `Authorization: Bearer <key>`, `x-api-key` or `x-goog-api-key` (a WebSocket from a browser: as a subprotocol `openai-insecure-api-key.<key>` or `?key=<key>`)";
    error(StatusCode::UNAUTHORIZED, "authentication_error", message)
}

/// An error of the gateway's own, in OpenAI's error shape.
pub fn error(status: StatusCode, kind: &str, message: &str) -> Response {
    let body = serde_json::json!({"error": {"message": message, "type": kind}});
    (status, axum::Json(body)).into_response()
}

/// Refuses a request without a valid key, and takes every header holding
/// one off one with: a client's own provider key in another header stays.
pub async fn consume(
    State(keys): State<Arc<ClientKeys>>,
    mut req: Request,
    next: Next,
) -> Response {
    match keys.presented(req.headers()) {
        Presented::Refused if is_upgrade(req.headers()) => match keys.browser(req) {
            Some(req) => next.run(req).await,
            None => unauthorized(),
        },
        Presented::Refused => unauthorized(),
        Presented::Open => next.run(req).await,
        Presented::Valid(holders) => {
            if let Some(owner) = keys.identify(req.headers()) {
                // The gateway's own extensions ride in the request's.
                let mut calls = req.extensions_mut().remove::<Extensions>();
                let calls = calls.get_or_insert_with(Extensions::new);
                calls.insert(owner);
                let calls = std::mem::take(calls);
                req.extensions_mut().insert(calls);
            }
            for name in holders {
                req.headers_mut().remove(name);
            }
            next.run(req).await
        }
    }
}

/// Whether the request asks to become a WebSocket.
fn is_upgrade(headers: &HeaderMap) -> bool {
    let upgrade = headers.get(UPGRADE).and_then(|v| v.to_str().ok());
    upgrade.is_some_and(|v| v.eq_ignore_ascii_case("websocket"))
}

impl ClientKeys {
    /// The WebSocket upgrade `req` without the gateway key a browser
    /// offered as a subprotocol or query parameter, if it offered one. The
    /// rest of the client's credentials stay.
    fn browser(&self, req: Request) -> Option<Request> {
        let (mut head, body) = req.into_parts();
        let mut credential = Credential::lift(&mut head)?;
        let found = credential.take_keys(|key| self.is_key(key));
        credential.restore(&mut head);
        found.then(|| Request::from_parts(head, body))
    }
}

/// Refuses a request without a valid key, leaving it as it is.
pub async fn require(State(keys): State<Arc<ClientKeys>>, req: Request, next: Next) -> Response {
    match keys.allows(req.headers()) {
        true => next.run(req).await,
        false => unauthorized(),
    }
}

/// Browser access: which origins' pages may call the gateway.
///
/// A preflight (`OPTIONS` with `Origin` and `Access-Control-Request-Method`)
/// from an allowed origin is answered here, without a key, since a browser
/// sends none with it. Other requests carry the gateway's usual key, and an
/// allowed origin's get the `Access-Control-*` headers a page needs to read
/// the answer, including the MCP session header. Origins that are not
/// allowed get nothing, so the browser refuses them.
#[derive(Debug)]
pub struct Cors {
    origins: Vec<String>,
    any: bool,
}

/// The headers a browser client may send: the keys, the SDKs' own, and
/// MCP's.
const ALLOWED_HEADERS: &str = "Authorization, x-api-key, x-goog-api-key, anthropic-version, \
    anthropic-beta, openai-beta, content-type, mcp-session-id, mcp-protocol-version, \
    last-event-id, x-respan-agent";
/// The headers a page may read.
const EXPOSED_HEADERS: &str = "mcp-session-id, mcp-protocol-version, retry-after";
const ALLOWED_METHODS: &str = "GET, POST, PUT, DELETE, OPTIONS";

impl Cors {
    /// The allowed `origins`: `scheme://host[:port]` each, or `*`, which
    /// needs `keys`, since without them any page could spend the gateway's
    /// provider keys.
    pub fn new(origins: &[String], keys: bool) -> Result<Arc<Self>, String> {
        if origins.is_empty() {
            return Err("cors: `origins` names no origin".to_owned());
        }
        let any = origins.iter().any(|o| o == "*");
        if any && !keys {
            return Err(
                "cors: origin '*' needs `client_keys:`; without keys any web page \
                        could spend the gateway's provider keys"
                    .to_owned(),
            );
        }
        for origin in origins.iter().filter(|o| *o != "*") {
            let (scheme, host) = origin.split_once("://").unwrap_or(("", origin));
            let plain = !host.is_empty() && !host.contains(['/', '?', '#', '@', ' ']);
            if !matches!(scheme, "http" | "https") || !plain {
                return Err(format!(
                    "cors: origin '{origin}' is not `scheme://host[:port]` (http or https, \
                     no path, no trailing slash), or `*`"
                ));
            }
        }
        let origins = origins.iter().map(|o| o.to_ascii_lowercase()).collect();
        Ok(Arc::new(Self { origins, any }))
    }

    fn allows(&self, origin: &str) -> bool {
        self.any || self.origins.iter().any(|o| o.eq_ignore_ascii_case(origin))
    }
}

/// Answers an allowed origin's preflight, and marks its other answers.
pub async fn cors(State(cors): State<Arc<Cors>>, req: Request, next: Next) -> Response {
    let origin = req.headers().get(ORIGIN).cloned();
    let Some(origin) = origin.filter(|o| o.to_str().is_ok_and(|o| cors.allows(o))) else {
        return next.run(req).await;
    };
    let preflight = req.method() == Method::OPTIONS
        && req.headers().contains_key("access-control-request-method");
    let mut resp = match preflight {
        true => {
            let mut resp = Response::new(axum::body::Body::empty());
            *resp.status_mut() = StatusCode::NO_CONTENT;
            let headers = resp.headers_mut();
            headers.insert(
                "access-control-allow-methods",
                HeaderValue::from_static(ALLOWED_METHODS),
            );
            headers.insert(
                "access-control-allow-headers",
                HeaderValue::from_static(ALLOWED_HEADERS),
            );
            headers.insert("access-control-max-age", HeaderValue::from_static("86400"));
            resp
        }
        false => next.run(req).await,
    };
    let headers = resp.headers_mut();
    headers.insert("access-control-allow-origin", origin);
    headers.insert(
        "access-control-expose-headers",
        HeaderValue::from_static(EXPOSED_HEADERS),
    );
    headers.append("vary", HeaderValue::from_static("Origin"));
    resp
}

/// Without client keys, whoever reaches the gateway spends its provider
/// keys. On loopback that is meant to be local programs alone, but a web
/// page the user opens reaches it too: by a cross-site request, which a
/// browser sends without asking when it is "simple", or by a DNS name of
/// the attacker's rebound to 127.0.0.1, which makes its pages same-origin
/// with the gateway. So a gateway without keys refuses a browser's
/// cross-origin request, and a `Host` that is not one of its own names.
/// With keys neither is needed: a browser never adds a gateway key itself.
#[derive(Debug)]
pub struct LocalOnly {
    configured: SocketAddr,
    /// Where it was bound, once served.
    bound: OnceLock<SocketAddr>,
    /// Origins the config lets call it (`cors:`), which are not cross-site
    /// here.
    allowed: Option<Arc<Cors>>,
}

impl LocalOnly {
    pub fn new(configured: SocketAddr, allowed: Option<Arc<Cors>>) -> Arc<Self> {
        let bound = OnceLock::new();
        Arc::new(Self {
            configured,
            bound,
            allowed,
        })
    }

    pub fn serving_on(&self, addr: SocketAddr) {
        let _ = self.bound.set(addr);
    }

    /// Whether it answers to the host `name`: any, when it listens on every
    /// address.
    fn answers_to(&self, name: &str) -> bool {
        let listen = self.bound.get().unwrap_or(&self.configured);
        let own = match listen.ip() {
            IpAddr::V4(ip) => ip.to_string(),
            IpAddr::V6(ip) => format!("[{ip}]"),
        };
        let names = ["localhost", "127.0.0.1", "[::1]", "respan-gateway"];
        listen.ip().is_unspecified() || names.contains(&name) || name == own
    }

    /// Why a request is refused, if it is.
    fn refusal(&self, req: &Request) -> Option<String> {
        let headers = req.headers();
        let value = |name| {
            headers
                .get(name)
                .and_then(|v: &HeaderValue| v.to_str().ok())
        };
        let host = value(HOST).or_else(|| req.uri().authority().map(|a| a.as_str()));
        if let Some(host) = host {
            let name = host_name(host).to_ascii_lowercase();
            if !self.answers_to(&name) {
                return Some(format!(
                    "Host '{name}' is not this gateway's: without client keys it answers \
                     only to localhost; set `client_keys:` to reach it by another name"
                ));
            }
        }
        if let (Some(cors), Some(origin)) = (&self.allowed, value(ORIGIN))
            && cors.allows(origin)
        {
            return None;
        }
        let cross_site = value(SEC_FETCH_SITE) == Some("cross-site");
        let origin = value(ORIGIN).map(|origin| origin.split_once("://").map(|(_, at)| at));
        let same_origin =
            |at: Option<&str>| at.zip(host).is_some_and(|(a, h)| a.eq_ignore_ascii_case(h));
        if cross_site || origin.is_some_and(|at| !same_origin(at)) {
            return Some(
                "a cross-origin browser request is refused: without client keys any web \
                 page could spend the gateway's provider keys; set `client_keys:` to allow it"
                    .to_owned(),
            );
        }
        None
    }
}

/// `host` without its port.
fn host_name(host: &str) -> &str {
    match host.strip_prefix('[') {
        Some(rest) => rest.find(']').map_or(host, |end| &host[..end + 2]),
        None => host.rsplit_once(':').map_or(host, |(name, _)| name),
    }
}

/// Refuses a request [`LocalOnly`] refuses, 403.
pub async fn local_only(State(local): State<Arc<LocalOnly>>, req: Request, next: Next) -> Response {
    match local.refusal(&req) {
        Some(why) => error(StatusCode::FORBIDDEN, "permission_error", &why),
        None => next.run(req).await,
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
        let presented = |pairs: &[(&'static str, &str)]| keys.presented(&headers(pairs));
        let valid = |names: &[HeaderName]| Presented::Valid(names.to_vec());
        assert_eq!(
            presented(&[("authorization", "Bearer k1")]),
            valid(&[AUTHORIZATION])
        );
        assert_eq!(presented(&[("x-api-key", "k2")]), valid(&[X_API_KEY]));
        assert_eq!(
            presented(&[("x-goog-api-key", "k1")]),
            valid(&[X_GOOG_API_KEY])
        );
        // The client's provider key in one header, the gateway's in another.
        let both = [("authorization", "Bearer sk-provider"), ("x-api-key", "k1")];
        assert_eq!(presented(&both), valid(&[X_API_KEY]));
        assert_eq!(
            presented(&[("authorization", "Bearer k3")]),
            Presented::Refused
        );
        assert_eq!(presented(&[("authorization", "k1")]), Presented::Refused);
        assert_eq!(presented(&[]), Presented::Refused);
    }

    #[test]
    fn a_client_key_has_an_id_that_is_not_the_key() {
        let keys = ClientKeys(vec!["k1".into(), "k2".into()]);
        let id = |pairs: &[(&'static str, &str)]| keys.identify(&headers(pairs));
        let one = id(&[("authorization", "Bearer k1")]);
        assert_eq!(one, id(&[("x-api-key", "k1")]));
        assert_ne!(one, id(&[("x-api-key", "k2")]));
        assert!(one.is_some_and(|Owner(id)| !id.contains("k1")));
        assert_eq!(id(&[("authorization", "Bearer sk-provider")]), None);
        // The gateway's key is the one named, not the client's own.
        let both = [("authorization", "Bearer sk-provider"), ("x-api-key", "k2")];
        assert_eq!(id(&both), id(&[("x-api-key", "k2")]));
    }

    #[test]
    fn every_header_holding_a_valid_key_is_taken_off() {
        let keys = ClientKeys(vec!["k1".into()]);
        let all = [
            ("authorization", "Bearer k1"),
            ("x-api-key", "k1"),
            ("x-goog-api-key", " k1 "),
        ];
        assert_eq!(
            keys.presented(&headers(&all)),
            Presented::Valid(vec![AUTHORIZATION, X_API_KEY, X_GOOG_API_KEY])
        );
    }

    #[test]
    fn an_empty_key_never_matches() {
        let keys = ClientKeys(vec![String::new()]);
        assert_eq!(
            keys.presented(&headers(&[("x-api-key", "")])),
            Presented::Refused
        );
        assert_eq!(
            keys.presented(&headers(&[("authorization", "Bearer ")])),
            Presented::Refused
        );
    }

    #[test]
    fn without_keys_everything_passes() {
        let keys = ClientKeys::default();
        assert!(!keys.required());
        assert_eq!(keys.presented(&HeaderMap::new()), Presented::Open);
    }

    fn refused(local: &LocalOnly, pairs: &[(&'static str, &str)]) -> bool {
        let mut req = Request::new(axum::body::Body::empty());
        *req.headers_mut() = headers(pairs);
        local.refusal(&req).is_some()
    }

    #[test]
    fn without_keys_a_cross_origin_browser_request_is_refused() {
        let local = LocalOnly::new(
            "127.0.0.1:8080".parse().unwrap_or_else(|e| panic!("{e}")),
            None,
        );
        let host = ("host", "127.0.0.1:8080");
        assert!(!refused(&local, &[host]), "curl");
        assert!(
            !refused(&local, &[host, ("origin", "http://127.0.0.1:8080")]),
            "the UI"
        );
        assert!(refused(&local, &[host, ("origin", "https://evil.example")]));
        assert!(refused(
            &local,
            &[host, ("origin", "http://localhost:3000")]
        ));
        assert!(refused(&local, &[host, ("origin", "null")]));
        assert!(refused(&local, &[host, ("sec-fetch-site", "cross-site")]));
    }

    #[test]
    fn without_keys_only_the_gateways_own_host_names_are_answered() {
        let local = LocalOnly::new(
            "127.0.0.1:8080".parse().unwrap_or_else(|e| panic!("{e}")),
            None,
        );
        for host in [
            "localhost:8080",
            "127.0.0.1",
            "[::1]:8080",
            "respan-gateway",
            "LOCALHOST",
        ] {
            assert!(!refused(&local, &[("host", host)]), "{host}");
        }
        assert!(!refused(&local, &[]), "no Host");
        assert!(refused(&local, &[("host", "evil.example:8080")]));
        assert!(refused(&local, &[("host", "localhost.evil.example")]));
        let everywhere = LocalOnly::new(
            "0.0.0.0:8080".parse().unwrap_or_else(|e| panic!("{e}")),
            None,
        );
        assert!(!refused(&everywhere, &[("host", "gateway.lan:8080")]));
        let own = LocalOnly::new(
            "10.0.0.2:8080".parse().unwrap_or_else(|e| panic!("{e}")),
            None,
        );
        assert!(!refused(&own, &[("host", "10.0.0.2:8080")]));
        // Served elsewhere than configured, as a host may.
        local.serving_on("0.0.0.0:9".parse().unwrap_or_else(|e| panic!("{e}")));
        assert!(!refused(&local, &[("host", "gateway.lan:9")]));
    }
}
