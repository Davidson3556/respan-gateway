//! Gemini's quirks.

use std::time::Duration;

use respan_gateway_core::Upstream;
use respan_gateway_core::futures::{SinkExt, StreamExt, stream};
use respan_gateway_http::Body;
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::http;
use respan_gateway_providers::{gemini, openai};
use tokio_tungstenite::tungstenite::Message;

/// Live's server sends JSON in binary frames, taken as text if UTF-8;
/// OpenAI's are refused.
#[tokio::test]
async fn binary_text() {
    for (format, bytes, accepted) in [
        (openai::REALTIME, "雪😀".as_bytes(), false),
        (gemini::LIVE, "雪😀".as_bytes(), true),
        (gemini::LIVE, &[0xff, 0xfe][..], false),
    ] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listen");
        let base = format!("ws://{}", listener.local_addr().expect("address"));
        let provider = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept");
            let mut socket = tokio_tungstenite::accept_async(socket)
                .await
                .expect("handshake");
            socket
                .send(Message::Binary(Bytes::copy_from_slice(bytes)))
                .await
                .expect("binary");
            socket.send(Message::Close(None)).await.expect("close");
            while let Some(Ok(_)) = socket.next().await {}
        });
        let transport = format.transport(base).expect("transport");
        let request = http::Request::get("/events")
            .body(Body::stream(stream::pending()))
            .expect("request");
        let (answer, completion) = transport.send(request).await.expect("upgrade");
        let mut body = answer.into_body();
        let frame = tokio::time::timeout(Duration::from_secs(3), body.next())
            .await
            .expect("message deadline")
            .expect("message");
        if accepted {
            assert_eq!(frame.expect("UTF-8 text"), bytes);
            assert!(body.next().await.is_none());
        } else {
            assert!(frame.is_err_and(|f| f.is::<Upstream>()));
        }
        drop(body);
        tokio::time::timeout(Duration::from_secs(3), completion.expect("completion"))
            .await
            .expect("session deadline")
            .expect("upload ended");
        tokio::time::timeout(Duration::from_secs(3), provider)
            .await
            .expect("server deadline")
            .expect("server task");
    }
}

#[test]
fn model_id_prefix() {
    let id = gemini::PROVIDER.model_id("models/gemini-2.5-flash");
    assert_eq!(id, "gemini-2.5-flash");
    assert_eq!(gemini::PROVIDER.model_id("gemini-2.5-flash"), id);
}
