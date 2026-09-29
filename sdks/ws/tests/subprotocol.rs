//! A key carried as a WebSocket subprotocol, as
//! OpenAI's Realtime API takes it from browsers
//! (`openai-insecure-api-key.<key>`), is a client credential too.
#![allow(clippy::unwrap_used)]

mod support;

use respan_gateway_core::Gateway;
use respan_gateway_http::http::{HeaderMap, HeaderValue};
use respan_gateway_ws::WsTransport;
use support::*;

#[tokio::test(flavor = "multi_thread")]
async fn a_transport_with_its_own_key_hides_a_subprotocol_key() {
    let probe = Probe::start().await;
    let mut own = HeaderMap::new();
    own.insert("authorization", HeaderValue::from_static("Bearer sk-own"));
    let transport = WsTransport::new(probe.base())
        .unwrap()
        .with_headers(own)
        .unwrap();
    let gateway = Gateway::new()
        .serve(Raw)
        .unwrap()
        .provider("p", Raw, transport)
        .unwrap()
        .target("p")
        .unwrap();
    let server = serve(gateway).await;
    let offered = "realtime, openai-insecure-api-key.sk-client-secret";
    let _ws = connect(
        server.addr,
        "/v1/realtime",
        &[("sec-websocket-protocol", offered)],
    )
    .await;
    probe.connected(0).await;
    let up = probe.upgrades().remove(0);
    assert_eq!(up.headers["authorization"], "Bearer sk-own");
    let sent = up
        .headers
        .get("sec-websocket-protocol")
        .map(|v| v.to_str().unwrap().to_owned());
    assert!(
        !sent
            .as_deref()
            .unwrap_or_default()
            .contains("sk-client-secret"),
        "the client's key reached a provider that has its own: {sent:?}"
    );
}
