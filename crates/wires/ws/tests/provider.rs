//! The provider edge of a session ([`WsTransport`]) against real sockets,
//! through a gateway relaying a JSON event format.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use respan_gateway_core::futures::{SinkExt, Stream, StreamExt, stream};
use respan_gateway_core::{Fault, Gateway, Plugin, ProviderContext, Upstream, async_trait};
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::http::{self, StatusCode, header};
use respan_gateway_http::http_body_util::BodyExt;
use respan_gateway_http::{Body, ProviderError};
use respan_gateway_realtime::Input;
use respan_gateway_realtime_ws::OpenAiRealtime;
use respan_gateway_ws::{Limits, WsTransport};
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;

#[tokio::test]
async fn fragmented_unicode_with_interleaved_ping_and_no_selected_subprotocol() {
    use tokio_tungstenite::tungstenite::protocol::frame::{
        Frame,
        coding::{Data, OpCode},
    };
    let text = "雪😀";
    for max_message in [text.len(), text.len() - 1] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listen");
        let base = format!("ws://{}", listener.local_addr().expect("address"));
        let provider = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept");
            // No selected subprotocol, despite the client's offer.
            let mut socket = tokio_tungstenite::accept_async(socket)
                .await
                .expect("handshake");
            let bytes = text.as_bytes();
            socket
                .send(Message::Frame(Frame::message(
                    bytes[..2].to_vec(),
                    OpCode::Data(Data::Text),
                    false,
                )))
                .await
                .expect("first fragment");
            socket
                .send(Message::Ping(Bytes::from_static(b"ping")))
                .await
                .expect("ping");
            socket
                .send(Message::Frame(Frame::message(
                    bytes[2..].to_vec(),
                    OpCode::Data(Data::Continue),
                    true,
                )))
                .await
                .expect("continuation");
            socket.send(Message::Close(None)).await.expect("close");
            while let Some(Ok(_)) = socket.next().await {}
        });
        let transport = WsTransport::new(base)
            .expect("transport")
            .with_limits(Limits {
                max_message,
                ..Limits::default()
            });
        let request = http::Request::get("/realtime")
            .header(header::SEC_WEBSOCKET_PROTOCOL, "optional")
            .body(Body::stream(stream::pending()))
            .expect("request");
        let (answer, completion) = transport.send(request).await.expect("valid upgrade");
        assert_eq!(answer.status(), StatusCode::SWITCHING_PROTOCOLS);
        assert!(
            !answer
                .headers()
                .contains_key(header::SEC_WEBSOCKET_PROTOCOL)
        );
        let mut body = answer.into_body();
        let first = tokio::time::timeout(Duration::from_secs(3), body.next())
            .await
            .expect("message deadline")
            .expect("message");
        if max_message == text.len() {
            assert_eq!(first.expect("complete Unicode message"), text);
            assert!(body.next().await.is_none());
        } else {
            assert!(first.is_err_and(|f| f.is::<Upstream>()));
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

/// A server that reads one handshake, to `GET /realtime`, and writes
/// `head` then `body`, then holds the connection until the test ends.
fn answering(head: String, body: Vec<u8>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind local handshake server");
    let address = listener.local_addr().expect("local address");
    std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().expect("receive handshake");
        socket
            .set_read_timeout(Some(Duration::from_secs(3)))
            .expect("read timeout");
        let mut headers = Vec::new();
        let mut chunk = [0u8; 1024];
        while !headers.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
            let n = socket.read(&mut chunk).expect("read handshake");
            assert!(
                n > 0 && headers.len() + n < 16 * 1024,
                "incomplete handshake"
            );
            headers.extend_from_slice(&chunk[..n]);
        }
        assert!(String::from_utf8_lossy(&headers).contains("GET /realtime?model=m HTTP/1.1"));
        let _ = socket.write_all(head.as_bytes());
        let _ = socket.write_all(&body);
        // Hold the connection open: its end is the test's.
        let _ = socket.read(&mut chunk);
    });
    address
}

type EndRecord = (bool, Result<(), Fault>);

/// How each provider call ended, and whether it was refused.
#[derive(Clone, Default)]
struct Ended(Arc<Mutex<Vec<EndRecord>>>);

#[async_trait]
impl Plugin for Ended {
    async fn on_provider_end(&self, px: &ProviderContext, end: &Result<(), Fault>) {
        self.0
            .lock()
            .expect("record end")
            .push((px.is_refused(), end.clone()));
    }
}

/// A session through `transport` whose client never sends a frame, with
/// the client's answer, read whole, how its one call ended, and whether
/// its upload was dropped.
async fn session_through(
    transport: WsTransport,
) -> (http::Response<Bytes>, EndRecord, Arc<AtomicBool>) {
    use respan_gateway_http::http_body::Frame;
    use respan_gateway_http::http_body_util::StreamBody;

    /// An upload that never sends, and says when it was dropped.
    struct NeverUpload(Arc<AtomicBool>);
    impl Stream for NeverUpload {
        type Item = Result<Frame<Bytes>, Fault>;
        fn poll_next(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            std::task::Poll::Pending
        }
    }
    impl Drop for NeverUpload {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    let ended = Ended::default();
    let limit = transport.limits().max_message;
    let gateway = Gateway::new()
        .provider::<Input, _, _>("ws", OpenAiRealtime, transport.connect())
        .and_then(|g| g.client::<Input, _, _>(OpenAiRealtime))
        .map(|g| g.plugin(ended.clone()))
        .and_then(|g| g.target("ws"))
        .expect("gateway");
    let dropped = Arc::new(AtomicBool::new(false));
    let request = http::Request::builder()
        .uri("/realtime?model=m")
        .header(header::UPGRADE, "websocket")
        .header(header::CONNECTION, "upgrade")
        .body(StreamBody::new(NeverUpload(dropped.clone())))
        .expect("request");
    let (response, completion) = tokio::time::timeout(
        Duration::from_secs(3),
        respan_gateway_http::handle(&gateway, request, respan_gateway_http::Framing::Live, limit),
    )
    .await
    .expect("refusal must not wait for an upload frame");
    let (head, body) = response.into_parts();
    let bytes = tokio::time::timeout(Duration::from_secs(3), BodyExt::collect(body))
        .await
        .expect("complete refusal body")
        .expect("read refusal body")
        .to_bytes();
    let _ = tokio::time::timeout(Duration::from_secs(3), completion)
        .await
        .expect("finish refused call");
    gateway.idle().await;
    let mut records = ended.0.lock().expect("recorded provider end").clone();
    assert_eq!(records.len(), 1);
    let response = http::Response::from_parts(head, bytes);
    (response, records.remove(0), dropped)
}

#[tokio::test]
async fn non_upgrade_200_and_204_are_original_refusals_without_waiting_for_upload() {
    for (status, body) in [
        (StatusCode::OK, "not upgraded"),
        (StatusCode::NO_CONTENT, ""),
    ] {
        let head = format!(
            "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nX-Provider: original\r\nConnection: close\r\n\r\n",
            status.as_u16(),
            status.canonical_reason().unwrap_or(""),
            body.len(),
        );
        let address = answering(head, body.as_bytes().to_vec());
        let transport = WsTransport::new(format!("ws://{address}")).expect("transport");
        let (response, (refused, end), dropped) = session_through(transport).await;
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        assert!(!response.headers().contains_key("x-provider"));
        // The client's format answers with the error code alone.
        assert_eq!(response.body(), "upstream");
        assert!(refused, "provider refused before opening");
        let fault = end.expect_err("refused call");
        let original = fault
            .get::<ProviderError>()
            .map(|e| &e.response)
            .expect("fault retains original answer");
        assert_eq!(
            (
                original.status(),
                &original.headers()["x-provider"],
                original.body()
            ),
            (
                status,
                &http::HeaderValue::from_static("original"),
                &Bytes::copy_from_slice(body.as_bytes())
            )
        );
        assert!(
            dropped.load(Ordering::SeqCst),
            "unread upload was cancelled"
        );
    }
}

#[tokio::test]
async fn an_incomplete_refusal_body_fails_at_the_handshake_deadline() {
    let head = "HTTP/1.1 403 Forbidden\r\nContent-Length: 1000000\r\n\r\n".to_owned();
    let address = answering(head, b"partial".to_vec());
    let deadline = Duration::from_millis(300);
    let transport = WsTransport::new(format!("ws://{address}"))
        .expect("transport")
        .with_handshake_timeout(deadline);
    let request = http::Request::get("/realtime?model=m")
        .body(Body::full(Bytes::new()))
        .expect("request");
    let start = Instant::now();
    let (answer, completion) = transport.send(request).await.expect("the refusal's head");
    assert!(completion.is_none());
    assert_eq!(answer.status(), StatusCode::FORBIDDEN);
    let read = tokio::time::timeout(deadline * 10, BodyExt::collect(answer.into_body()))
        .await
        .expect("an incomplete refusal stops at the handshake's deadline");
    let fault = read.expect_err("the body is incomplete");
    assert!(fault.is::<Upstream>(), "{fault}");
    assert!(start.elapsed() >= deadline);
}

#[tokio::test]
async fn a_refusal_body_is_read_up_to_the_message_limit() {
    let limits = Limits {
        max_message: 1024,
        ..Limits::default()
    };
    for size in [1024, 1025] {
        let head = format!("HTTP/1.1 403 Forbidden\r\nContent-Length: {size}\r\n\r\n");
        let address = answering(head, vec![b'x'; size]);
        let transport = WsTransport::new(format!("ws://{address}"))
            .expect("transport")
            .with_limits(limits);
        let (_, (refused, end), _) = session_through(transport).await;
        assert!(refused);
        let fault = end.expect_err("refused call");
        match fault.get::<ProviderError>() {
            Some(original) => {
                assert_eq!(size, 1024);
                assert_eq!(original.response.status(), StatusCode::FORBIDDEN);
                assert_eq!(original.response.body().len(), 1024);
            }
            None => {
                assert_eq!(size, 1025);
                assert!(fault.is::<Upstream>());
                assert!(fault.message().contains("over the limit"), "{fault}");
            }
        }
    }
}
