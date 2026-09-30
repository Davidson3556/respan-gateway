//! Whose keys reach the provider over WebSocket: the client's pass only on a
//! same-name relay and only where the transport has none of its own; the
//! connection's own headers are never the client's.
#![allow(clippy::unwrap_used)]

mod support;

use respan_gateway_core::{Fault, Gateway, Plugin, ProviderContext, async_trait};
use respan_gateway_http::CallHeaders;
use respan_gateway_http::http::{HeaderMap, HeaderValue};
use respan_gateway_ws::ClientEvent;
use respan_gateway_ws::{JsonEvents, WsTransport};
use support::*;

const PATH: &str = "/v1/realtime?model=m&key=AIza-client";

/// What the client sends, credentials and the rest.
const CLIENT: &[(&str, &str)] = &[
    ("authorization", "Bearer sk-client"),
    ("x-api-key", "client-key"),
    ("cookie", "session=1"),
    ("x-forwarded-for", "10.0.0.1"),
    ("sec-websocket-extensions", "permessage-deflate"),
    ("openai-beta", "realtime=v1"),
];

/// The upgrade the probe's first connection made.
async fn first(probe: &Probe) -> Upgrade {
    probe.connected(0).await;
    probe.upgrades().remove(0)
}

fn query(uri: &str) -> Vec<String> {
    let query = uri.split_once('?').map_or("", |(_, q)| q);
    query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(str::to_owned)
        .collect()
}

fn header(upgrade: &Upgrade, name: &str) -> Option<String> {
    upgrade
        .headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
}

// A relay in the client's own format passes its keys, header and query, and
// its other end-to-end headers; not its origin's, nor the client's
// connection.
#[tokio::test(flavor = "multi_thread")]
async fn a_relay_passes_the_clients_keys_and_not_its_origin() {
    let probe = Probe::start().await;
    let (gateway, ..) = relay(&probe.base());
    let server = serve(gateway).await;
    let _ws = connect(server.addr, PATH, CLIENT).await.unwrap();
    let up = first(&probe).await;
    assert_eq!(
        header(&up, "authorization").as_deref(),
        Some("Bearer sk-client")
    );
    assert_eq!(header(&up, "x-api-key").as_deref(), Some("client-key"));
    assert_eq!(query(&up.uri), ["model=m", "key=AIza-client"]);
    assert_eq!(header(&up, "openai-beta").as_deref(), Some("realtime=v1"));
    // The client's origin and its own connection are not passed on.
    assert_eq!(header(&up, "cookie"), None);
    assert_eq!(header(&up, "x-forwarded-for"), None);
    assert_eq!(header(&up, "sec-websocket-extensions"), None);
    assert!(header(&up, "sec-websocket-key").is_some());
    assert_eq!(header(&up, "upgrade").as_deref(), Some("websocket"));
}

// A transport with keys of its own sends them, and none of the client's,
// header or query.
#[tokio::test(flavor = "multi_thread")]
async fn a_transport_with_its_own_key_hides_the_clients() {
    let probe = Probe::start().await;
    let mut own = HeaderMap::new();
    own.insert("authorization", HeaderValue::from_static("Bearer sk-own"));
    let transport = WsTransport::new(probe.base())
        .unwrap()
        .with_headers(own)
        .unwrap();
    let gateway = Gateway::new()
        .client::<<Raw as respan_gateway_http::Codec>::Input, _, _>(Raw)
        .unwrap()
        .provider::<<Raw as respan_gateway_http::Codec>::Input, _, _>(
            "p",
            Raw,
            respan_gateway_http::connect(transport),
        )
        .unwrap()
        .target("p")
        .unwrap();
    let server = serve(gateway).await;
    let _ws = connect(server.addr, PATH, CLIENT).await.unwrap();
    let up = first(&probe).await;
    assert_eq!(
        header(&up, "authorization").as_deref(),
        Some("Bearer sk-own")
    );
    assert_eq!(header(&up, "x-api-key"), None);
    assert_eq!(query(&up.uri), ["model=m"]);
    assert_eq!(header(&up, "openai-beta").as_deref(), Some("realtime=v1"));
}

// A call in another format (formats of another name that translate through
// the kind) carries none of the client's keys, even to a provider that has
// none of its own: the key is for its own protocol's provider.
#[tokio::test(flavor = "multi_thread")]
async fn a_translated_call_carries_none_of_the_clients_keys() {
    let probe = Probe::start().await;
    let client_side = JsonEvents::new("one-name", "/v1/realtime");
    let provider_side = JsonEvents::new("another-name", "/other");
    let gateway = Gateway::new()
        .client::<ClientEvent, _, _>(client_side)
        .unwrap()
        .provider::<ClientEvent, _, _>(
            "p",
            provider_side,
            respan_gateway_http::connect(WsTransport::new(probe.base()).unwrap()),
        )
        .unwrap()
        .target("p")
        .unwrap();
    let server = serve(gateway).await;
    let mut ws = connect(server.addr, PATH, CLIENT).await.unwrap();
    // The session works, translated, and its head is the provider's own.
    say(&mut ws, r#"{"hello":1}"#).await;
    let up = first(&probe).await;
    probe.tell(0, Cmd::Send(r#"{"hi":  2}"#.into()));
    // Written by the other format, not relayed, so from the JSON.
    assert_eq!(heard_text(&mut ws).await, r#"{"hi":2}"#);
    eventually("the provider read it", || probe.texts().len() == 1).await;
    assert_eq!(probe.texts(), [r#"{"hello":1}"#]);
    assert_eq!(up.uri, "/other");
    for name in [
        "authorization",
        "x-api-key",
        "cookie",
        "x-forwarded-for",
        "openai-beta",
    ] {
        assert_eq!(header(&up, name), None, "{name}");
    }
    assert!(!up.uri.contains("key"));
}

/// Sets a tenant's key for each call, as a plugin that knows the tenant does.
struct Tenant;

#[async_trait]
impl Plugin for Tenant {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        let mut key = HeaderMap::new();
        key.insert(
            "authorization",
            HeaderValue::from_static("Bearer sk-tenant"),
        );
        px.extensions_mut().insert(CallHeaders(key));
        Ok(())
    }
}

// A key a plugin sets for one call replaces the client's on a relay.
#[tokio::test(flavor = "multi_thread")]
async fn a_key_set_for_the_call_replaces_the_clients() {
    let probe = Probe::start().await;
    let gateway = Gateway::new()
        .client::<<Raw as respan_gateway_http::Codec>::Input, _, _>(Raw)
        .unwrap()
        .provider::<<Raw as respan_gateway_http::Codec>::Input, _, _>(
            "p",
            Raw,
            respan_gateway_http::connect(WsTransport::new(probe.base()).unwrap()),
        )
        .unwrap()
        .plugin("tenant", Tenant)
        .unwrap()
        .target("p")
        .unwrap();
    let server = serve(gateway).await;
    let _ws = connect(server.addr, PATH, CLIENT).await.unwrap();
    let up = first(&probe).await;
    assert_eq!(
        header(&up, "authorization").as_deref(),
        Some("Bearer sk-tenant")
    );
    assert_eq!(header(&up, "x-api-key"), None);
    assert_eq!(query(&up.uri), ["model=m"]);
}

// A subprotocol the client offers and the provider picks is the client's.
#[tokio::test(flavor = "multi_thread")]
async fn the_subprotocol_the_provider_picks_is_the_clients() {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let probe = Probe::start().await;
    let (gateway, ..) = relay(&probe.base());
    let server = serve(gateway).await;
    let mut req = format!("ws://{}/v1/realtime", server.addr)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("sec-websocket-protocol", "realtime".parse().unwrap());
    let (_ws, resp) = tokio_tungstenite::connect_async(req).await.unwrap();
    assert_eq!(resp.headers()["sec-websocket-protocol"], "realtime");
}

#[test]
fn a_transport_takes_only_ws_urls_and_no_connection_headers() {
    for bad in [
        "http://h",
        "https://h",
        "ws://",
        "h:1",
        "ws://user:pw@h",
        "ws://h?key=k",
        "wss://h/#frag",
    ] {
        let refused = WsTransport::new(bad).unwrap_err();
        assert_eq!(refused.code(), "config", "{bad}");
        assert!(
            !refused.message().contains("pw") && !refused.message().contains("key=k"),
            "{bad}"
        );
    }
    for good in ["ws://h", "ws://h:8080/v1", "wss://h/v1/"] {
        assert!(WsTransport::new(good).is_ok(), "{good}");
    }
    let mut hop = HeaderMap::new();
    hop.insert("connection", HeaderValue::from_static("close"));
    assert_eq!(
        WsTransport::new("ws://h")
            .unwrap()
            .with_headers(hop)
            .unwrap_err()
            .code(),
        "config"
    );
    let mut key = HeaderMap::new();
    key.insert("sec-websocket-key", HeaderValue::from_static("x"));
    assert!(
        WsTransport::new("ws://h")
            .unwrap()
            .with_headers(key)
            .is_err()
    );
    // Values may be keys: a transport's Debug shows names only.
    let mut own = HeaderMap::new();
    own.insert(
        "authorization",
        HeaderValue::from_static("Bearer sk-secret"),
    );
    let debug = format!(
        "{:?}",
        WsTransport::new("ws://h")
            .unwrap()
            .with_headers(own)
            .unwrap()
    );
    assert!(!debug.contains("sk-secret"), "{debug}");
}
