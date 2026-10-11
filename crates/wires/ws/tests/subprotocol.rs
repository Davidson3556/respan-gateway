//! A key carried as a WebSocket subprotocol, as OpenAI's Realtime API takes
//! it from browsers (`openai-insecure-api-key.<key>`) and Deepgram's takes it
//! (`token, <key>`), is a client credential too.
#![allow(clippy::unwrap_used)]

mod support;

use respan_gateway_core::Gateway;
use respan_gateway_http::http::{HeaderMap, HeaderValue};
use respan_gateway_realtime::Input;
use respan_gateway_realtime_ws::OpenAiRealtime;
use respan_gateway_ws::WsTransport;
use support::*;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::error::{Error, ProtocolError};

/// A gateway whose transport has a key of its own.
fn keyed(probe: &Probe) -> Gateway {
    let mut own = HeaderMap::new();
    own.insert("authorization", HeaderValue::from_static("Bearer sk-own"));
    let transport = WsTransport::new(probe.base())
        .unwrap()
        .with_headers(own)
        .unwrap();
    Gateway::new()
        .client::<respan_gateway_http::formats::opaque::Input<Raw>, _, _>(Raw)
        .unwrap()
        .provider::<respan_gateway_http::formats::opaque::Input<Raw>, _, _>(
            "p",
            Raw,
            transport.connect(),
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
    let path = "/v1/realtime?model=m";
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

/// A client that offered nothing but a key gets none echoed: a key is never
/// sent back, so a client that needs a subprotocol (as a browser does)
/// refuses the handshake.
#[tokio::test(flavor = "multi_thread")]
async fn a_key_offered_as_the_only_subprotocol_is_never_echoed() {
    let probe = Probe::start().await;
    let server = serve(keyed(&probe)).await;
    let mut req = format!("ws://{}/v1/realtime?model=m", server.addr)
        .into_client_request()
        .unwrap();
    let offered = HeaderValue::from_static("openai-insecure-api-key.k");
    req.headers_mut().insert("sec-websocket-protocol", offered);
    let refused = tokio_tungstenite::connect_async(req).await;
    let Err(Error::Protocol(ProtocolError::SecWebSocketSubProtocolError(_))) = refused else {
        panic!("a subprotocol was echoed: {refused:?}")
    };
    probe.connected(0).await;
    let upgrade = &probe.upgrades()[0];
    assert!(upgrade.headers.get("sec-websocket-protocol").is_none());
}

/// A typed gateway of OpenAI Realtime, which speaks the subprotocol
/// `realtime`.
fn speaking(probe: &Probe) -> Gateway {
    Gateway::new()
        .client::<Input, _, _>(OpenAiRealtime)
        .unwrap()
        .provider::<Input, _, _>(
            "p",
            OpenAiRealtime,
            WsTransport::new(probe.base()).unwrap().connect(),
        )
        .unwrap()
        .target("p")
        .unwrap()
}

// A format's subprotocol is offered to the provider, and given back to a
// client that offered it, its key never.
#[tokio::test(flavor = "multi_thread")]
async fn a_formats_subprotocol_is_offered_and_given_back_to_a_client_that_offered_it() {
    let probe = Probe::start().await;
    let server = serve_typed(speaking(&probe)).await;
    let mut req = format!("ws://{}/v1/realtime?model=m", server.addr)
        .into_client_request()
        .unwrap();
    let offered = HeaderValue::from_static("realtime, openai-insecure-api-key.k");
    req.headers_mut().insert("sec-websocket-protocol", offered);
    let (_ws, resp) = tokio_tungstenite::connect_async(req).await.unwrap();
    assert_eq!(resp.headers()["sec-websocket-protocol"], "realtime");
    probe.connected(0).await;
    let upgrade = &probe.upgrades()[0];
    assert_eq!(upgrade.headers["sec-websocket-protocol"], "realtime");
}

// The subprotocols a client offers besides the format's reach the provider,
// its key not among them; the format's is added only if it is missing.
#[tokio::test(flavor = "multi_thread")]
async fn a_clients_other_subprotocols_are_offered_to_the_provider_beside_the_formats() {
    for (offered, sent) in [
        (
            "realtime, openai-beta.realtime-v1, openai-insecure-api-key.k",
            "realtime, openai-beta.realtime-v1",
        ),
        (
            "openai-beta.realtime-v1",
            "openai-beta.realtime-v1, realtime",
        ),
    ] {
        let probe = Probe::start().await;
        let server = serve_typed(speaking(&probe)).await;
        let mut req = format!("ws://{}/v1/realtime?model=m", server.addr)
            .into_client_request()
            .unwrap();
        let offered = HeaderValue::from_static(offered);
        req.headers_mut().insert("sec-websocket-protocol", offered);
        let _ws = tokio_tungstenite::connect_async(req).await;
        probe.connected(0).await;
        let upgrade = &probe.upgrades()[0];
        assert_eq!(upgrade.headers["sec-websocket-protocol"], sent);
    }
}

// A client that offered none gets none, though the provider is offered it.
#[tokio::test(flavor = "multi_thread")]
async fn a_client_that_offered_no_subprotocol_gets_none() {
    let probe = Probe::start().await;
    let server = serve_typed(speaking(&probe)).await;
    let ws = format!("ws://{}/v1/realtime?model=m", server.addr);
    let (_ws, resp) = tokio_tungstenite::connect_async(ws).await.unwrap();
    assert!(resp.headers().get("sec-websocket-protocol").is_none());
    probe.connected(0).await;
    let upgrade = &probe.upgrades()[0];
    assert_eq!(upgrade.headers["sec-websocket-protocol"], "realtime");
}

// A relay passes the provider's head on, but a key the provider picked as
// its subprotocol (here the client's own, relayed) is never sent back.
#[tokio::test(flavor = "multi_thread")]
async fn a_key_the_provider_picks_is_never_echoed() {
    let probe = Probe::start().await;
    let (gateway, ..) = relay(&probe.base());
    let server = serve(gateway).await;
    let mut req = format!("ws://{}/v1/realtime?model=m", server.addr)
        .into_client_request()
        .unwrap();
    let offered = HeaderValue::from_static("openai-insecure-api-key.k");
    req.headers_mut().insert("sec-websocket-protocol", offered);
    let refused = tokio_tungstenite::connect_async(req).await;
    let Err(Error::Protocol(ProtocolError::SecWebSocketSubProtocolError(_))) = refused else {
        panic!("a subprotocol was echoed: {refused:?}")
    };
    probe.connected(0).await;
    let upgrade = &probe.upgrades()[0];
    assert_eq!(
        upgrade.headers["sec-websocket-protocol"],
        "openai-insecure-api-key.k"
    );
}
