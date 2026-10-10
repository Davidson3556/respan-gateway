//! A WebSocket upgrade relayed over an `HttpTransport`, which cannot switch
//! protocols, fails for the client instead of ending as a clean session.
#![allow(clippy::unwrap_used)]

use respan_gateway_core::Gateway;
use respan_gateway_http::{
    HttpTransport,
    formats::opaque::{Input, Passthrough},
    relay::Relay,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, handshake::derive_accept_key};

#[tokio::test(flavor = "multi_thread")]
async fn an_upgrade_over_http_is_refused_to_the_client() {
    let provider = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let provider_addr = provider.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut socket, _) = provider.accept().await.unwrap();
        let mut buf = vec![0u8; 8192];
        let n = socket.read(&mut buf).await.unwrap();
        let head = String::from_utf8_lossy(&buf[..n]).to_string();
        let key = head
            .lines()
            .find_map(|l| l.strip_prefix("sec-websocket-key: "))
            .unwrap_or("x")
            .trim()
            .to_owned();
        let accept = derive_accept_key(key.as_bytes());
        let answer = format!(
            "HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\nconnection: upgrade\r\nsec-websocket-accept: {accept}\r\n\r\n"
        );
        socket.write_all(answer.as_bytes()).await.unwrap();
        let _ = socket.read(&mut buf).await;
    });
    let transport = HttpTransport::new(format!("http://{provider_addr}")).unwrap();
    let gateway = Gateway::new()
        .client::<Input<Passthrough>, _, _>(Passthrough)
        .unwrap()
        .provider::<Input<Passthrough>, _, _>("p", Passthrough, transport.connect())
        .unwrap()
        .plugin(Relay::RAW)
        .target("p")
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = respan_gateway_ws::axum::router(
        gateway,
        |_| 1 << 20,
        Default::default(),
        std::future::pending(),
    );
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let request = format!("ws://{addr}/v1/other")
        .into_client_request()
        .unwrap();
    let refused = tokio_tungstenite::connect_async(request).await;
    assert!(refused.is_err(), "the client was upgraded");
}
