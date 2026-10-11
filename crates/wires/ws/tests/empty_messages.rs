//! An empty text message is a message: it crosses a relayed session both ways.
#![allow(clippy::unwrap_used)]

use futures::{SinkExt, StreamExt};
use respan_gateway_core::Gateway;
use respan_gateway_http::{
    framing::passthrough::{Input, Passthrough},
    relay::Relay,
};
use respan_gateway_ws::WsTransport;
use std::time::Duration;
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};

const WAIT: Duration = Duration::from_secs(2);

#[tokio::test(flavor = "multi_thread")]
async fn empty_text_messages_cross_both_ways() {
    let provider = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let provider_addr = provider.local_addr().unwrap();
    let (heard, mut provider_heard) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        let (socket, _) = provider.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(socket).await.unwrap();
        ws.send(Message::Text("".into())).await.unwrap();
        ws.send(Message::Text("after".into())).await.unwrap();
        while let Ok(Some(Ok(message))) = tokio::time::timeout(WAIT, ws.next()).await {
            let _ = heard.send(message);
        }
    });
    let transport = WsTransport::new(format!("ws://{provider_addr}")).unwrap();
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

    let request = format!("ws://{addr}/v1/x").into_client_request().unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    let mut texts = Vec::new();
    for _ in 0..2 {
        match tokio::time::timeout(WAIT, ws.next()).await {
            Ok(Some(Ok(Message::Text(text)))) => texts.push(text.to_string()),
            other => panic!("expected a text message, got {other:?}"),
        }
    }
    assert_eq!(texts, ["", "after"]);

    ws.send(Message::Text("".into())).await.unwrap();
    ws.send(Message::Text("after".into())).await.unwrap();
    let mut said = Vec::new();
    for _ in 0..2 {
        let message = tokio::time::timeout(WAIT, provider_heard.recv()).await;
        said.push(message.unwrap().unwrap());
    }
    assert_eq!(
        said,
        [Message::Text("".into()), Message::Text("after".into())]
    );
}
