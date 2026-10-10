#![allow(dead_code, clippy::unwrap_used)]
use futures::{SinkExt, StreamExt};
use respan_gateway_core::Gateway;
use serde_json::Value;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};
pub type Socket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;
pub struct Live {
    pub socket: Socket,
    pub gateway: Gateway,
    pub received: Arc<Mutex<Vec<Value>>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}
impl Drop for Live {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}
pub async fn start(
    path: &str,
    script: impl Fn(Value) -> Vec<Value> + Send + Sync + 'static,
    make: impl FnOnce(String) -> Gateway,
) -> Live {
    let received = Arc::new(Mutex::new(Vec::new()));
    let read = received.clone();
    let provider = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let at = provider.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (socket, _) = provider.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(socket).await.unwrap();
        while let Some(Ok(message)) = socket.next().await {
            match message {
                Message::Text(text) => {
                    let event: Value = serde_json::from_str(&text).unwrap();
                    read.lock().unwrap().push(event.clone());
                    for reply in script(event) {
                        if socket
                            .send(Message::Text(reply.to_string().into()))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                }
                Message::Close(_) => {
                    let _ = socket.flush().await;
                    break;
                }
                _ => {}
            }
        }
    });
    let gateway = make(format!("ws://{at}"));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let at = listener.local_addr().unwrap();
    let app = respan_gateway_ws::axum::router(
        gateway.clone(),
        |_| respan_gateway_examples::LIMIT,
        respan_gateway_ws::Limits::default(),
        std::future::pending(),
    );
    let served = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let (socket, _) = tokio_tungstenite::connect_async(format!("ws://{at}{path}"))
        .await
        .unwrap();
    Live {
        socket,
        gateway,
        received,
        tasks: vec![task, served],
    }
}
pub async fn send(live: &mut Live, event: Value) {
    live.socket
        .send(Message::Text(event.to_string().into()))
        .await
        .unwrap();
}
pub async fn next(live: &mut Live) -> Option<Value> {
    match tokio::time::timeout(Duration::from_secs(3), live.socket.next())
        .await
        .unwrap()
    {
        Some(Ok(Message::Text(text))) => Some(serde_json::from_str(&text).unwrap()),
        _ => None,
    }
}
pub async fn close(live: &mut Live) {
    let _ = live.socket.close(None).await;
    tokio::time::timeout(Duration::from_secs(3), live.gateway.idle())
        .await
        .unwrap();
}
