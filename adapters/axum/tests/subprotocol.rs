#![cfg(feature = "ws")]
//! A key carried as a WebSocket subprotocol, as OpenAI's Realtime API takes
//! it from browsers (`openai-insecure-api-key.<key>`) and Deepgram's takes it
//! (`token, <key>`), is a client credential too.
#![allow(clippy::unwrap_used)]

mod support;

use respan_gateway_core::Gateway;
use respan_gateway_http::http::{HeaderMap, HeaderValue};
use respan_gateway_ws::WsTransport;
use support::*;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

/// A gateway whose transport has a key of its own.
fn keyed(probe: &Probe) -> Gateway {
    let mut own = HeaderMap::new();
    own.insert("authorization", HeaderValue::from_static("Bearer sk-own"));
    let transport = WsTransport::new(probe.base())
        .unwrap()
        .with_headers(own)
        .unwrap();
    Gateway::new()
        .client::<<Raw as respan_gateway_http::Codec>::Input, _, _>(Raw)
        .unwrap()
        .provider::<<Raw as respan_gateway_http::Codec>::Input, _, _>(
            "p",
            Raw,
            respan_gateway_http::connect(transport),
        )
        .unwrap()
        .target("p")
        .unwrap()
}

/// What the provider was offered as subprotocols, if anything, when a client
/// offered `offered` through a gateway whose transport has its own key.
async fn offered_to_provider(offered: &str) -> Option<String> {
    let probe = Probe::start().await;
    let server = serve(keyed(&probe)).await;
    let path = "/v1/realtime";
    let _ws = connect(server.addr, path, &[("sec-websocket-protocol", offered)]).await;
    probe.connected(0).await;
    let up = probe.upgrades().remove(0);
    assert_eq!(up.headers["authorization"], "Bearer sk-own");
    let sent = up.headers.get("sec-websocket-protocol");
    sent.map(|v| v.to_str().unwrap().to_owned())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_transport_with_its_own_key_hides_a_subprotocol_key() {
    let sent = offered_to_provider("realtime, openai-insecure-api-key.sk-client-secret").await;
    assert_eq!(sent.as_deref(), Some("realtime"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_key_after_a_bare_credential_name_is_hidden_with_its_name() {
    // The provider is keyed: a marker with no key after it would be refused.
    let sent = offered_to_provider("realtime, token, dg-client-secret-0123456789").await;
    assert_eq!(sent.as_deref(), Some("realtime"));
}

#[tokio::test(flavor = "multi_thread")]
async fn an_empty_entry_leaves_the_provider_a_valid_list() {
    let sent = offered_to_provider("realtime, , openai-insecure-api-key.k").await;
    assert_eq!(sent.as_deref(), Some("realtime"));
}

/// A client that offered nothing but a key gets a subprotocol echoed all the
/// same: a browser fails a handshake that echoes none.
#[tokio::test(flavor = "multi_thread")]
async fn a_client_that_offered_only_a_key_still_gets_one_echoed() {
    let probe = Probe::start().await;
    let server = serve(keyed(&probe)).await;
    let mut req = format!("ws://{}/v1/realtime", server.addr)
        .into_client_request()
        .unwrap();
    let offered = HeaderValue::from_static("openai-insecure-api-key.k");
    req.headers_mut()
        .insert("sec-websocket-protocol", offered.clone());
    let (_ws, resp) = tokio_tungstenite::connect_async(req).await.unwrap();
    probe.connected(0).await;
    let upgrade = &probe.upgrades()[0];
    assert!(upgrade.headers.get("sec-websocket-protocol").is_none());
    assert_eq!(resp.headers().get("sec-websocket-protocol"), Some(&offered));
}
