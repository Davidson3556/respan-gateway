use respan_gateway_http::http;

use respan_gateway_http_transport::HttpTransport;

#[test]
fn a_base_url_needs_http_or_https_and_a_host() {
    for url in [
        "http://[::1]",
        "http://[::1]:8080",
        "https://h.test:443/v1beta",
        "https://h.test/models/m@2",
    ] {
        assert!(HttpTransport::new(url).is_ok(), "{url}");
    }
    // A base URL may contain a key, so the error does not echo it.
    let expected = "bad base URL: http or https with a host, and no query, fragment or credentials (those go in its headers)";
    for url in [
        "api.openai.com",
        "https://h.test?api-version=1",
        "https://h.test/#f",
        "https://user:ab/cd@h.test",
        "https://key:sec ret@h.test",
        "https://key:secret@h.test",
        "https://key@h.test",
        "https://:443",
        "ftp://h.test",
        "https://h.test:99999",
        "http://127.0.0.1:0",
        "http://1.2.3.256",
        "http://[::1%25eth0]",
    ] {
        let fault = HttpTransport::new(url).err();
        assert_eq!(fault.as_ref().map(|f| f.message()), Some(expected), "{url}");
    }
}

#[test]
fn a_connection_header_is_refused() {
    let one = |name: &str| {
        let name = http::HeaderName::from_bytes(name.as_bytes()).unwrap_or_else(|e| panic!("{e}"));
        http::HeaderMap::from_iter([(name, http::HeaderValue::from_static("4"))])
    };
    for name in [
        "host",
        "content-length",
        "connection",
        "transfer-encoding",
        "te",
        "upgrade",
        "expect",
        "proxy-authorization",
    ] {
        let fault = HttpTransport::new("https://h.test")
            .unwrap_or_else(|e| panic!("{e}"))
            .with_headers(one(name))
            .err();
        let expected = format!("header '{name}' is the connection's");
        assert_eq!(fault.as_ref().map(|f| f.message()), Some(&*expected));
    }
    assert!(
        HttpTransport::new("https://h.test")
            .unwrap_or_else(|e| panic!("{e}"))
            .with_headers(one("x-team"))
            .is_ok()
    );
}

#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn a_switch_of_protocols_is_an_upstream_fault() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let provider = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = provider.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut socket, _) = provider.accept().await.unwrap();
        let _ = socket.read(&mut [0; 4096]).await;
        let answer =
            "HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\nconnection: upgrade\r\n\r\n";
        socket.write_all(answer.as_bytes()).await.unwrap();
    });
    let transport = HttpTransport::new(format!("http://{addr}")).unwrap();
    let mut call = http::Request::new(respan_gateway_http::Body::full(Vec::new()));
    call.headers_mut()
        .insert("upgrade", http::HeaderValue::from_static("websocket"));
    call.headers_mut()
        .insert("connection", http::HeaderValue::from_static("upgrade"));
    let fault = transport.send(call).await.err().expect("a 101 is refused");
    assert!(
        fault.is(respan_gateway_core::FaultKind::Provider),
        "{fault:?}"
    );
}

type Seen = std::sync::Arc<std::sync::Mutex<Vec<String>>>;

/// A local server that answers every request with `answer` and keeps what
/// it was sent, lowercased.
#[allow(clippy::unwrap_used)]
async fn serving(answer: String) -> (std::net::SocketAddr, Seen) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let server = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = server.local_addr().unwrap();
    let seen = Seen::default();
    let kept = seen.clone();
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = server.accept().await.unwrap();
            let mut buf = [0; 4096];
            let n = socket.read(&mut buf).await.unwrap_or(0);
            let request = String::from_utf8_lossy(&buf[..n]).to_lowercase();
            kept.lock().unwrap().push(request);
            let _ = socket.write_all(answer.as_bytes()).await;
        }
    });
    (addr, seen)
}

const STORED: &str = "HTTP/1.1 200 OK\r\ncontent-length: 5\r\nconnection: close\r\n\r\nvideo";

fn redirecting_to(location: &str) -> String {
    format!(
        "HTTP/1.1 302 Found\r\nlocation: {location}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
    )
}

fn keyed(addr: std::net::SocketAddr) -> HttpTransport {
    let key = http::HeaderValue::from_static("secret-key");
    let name = http::HeaderName::from_static("x-goog-api-key");
    HttpTransport::new(format!("http://{addr}"))
        .and_then(|t| t.with_headers(http::HeaderMap::from_iter([(name, key)])))
        .unwrap_or_else(|e| panic!("{e}"))
}

fn get() -> http::Request<respan_gateway_http::Body> {
    http::Request::new(respan_gateway_http::Body::default())
}

#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn a_followed_redirect_to_another_origin_leaves_the_key_behind() {
    let (storage, stored) = serving(STORED.into()).await;
    let (api, asked) = serving(redirecting_to(&format!("http://{storage}/file?sig=1"))).await;
    let transport = keyed(api).follow_redirects(|_| true);
    let (resp, _) = transport.send(get()).await.unwrap();
    assert_eq!(resp.status(), http::StatusCode::OK);
    assert!(asked.lock().unwrap()[0].contains("x-goog-api-key: secret-key"));
    let stored = stored.lock().unwrap();
    assert_eq!(stored.len(), 1);
    assert!(stored[0].starts_with("get /file?sig=1 "), "{}", stored[0]);
    assert!(!stored[0].contains("secret-key"), "{}", stored[0]);
}

#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn a_redirect_within_the_origin_keeps_the_key() {
    let (api, asked) = serving(redirecting_to("/elsewhere")).await;
    let transport = keyed(api).follow_redirects(|_| true);
    let (resp, _) = transport.send(get()).await.unwrap();
    // It redirects to itself, so it stops at the limit.
    assert_eq!(resp.status(), http::StatusCode::FOUND);
    let asked = asked.lock().unwrap();
    assert!(asked.len() > 1);
    let key = "x-goog-api-key: secret-key";
    assert!(asked.iter().all(|request| request.contains(key)));
}

#[tokio::test]
#[allow(clippy::unwrap_used)]
async fn a_redirect_not_allowed_is_the_clients_to_follow() {
    let (storage, stored) = serving(STORED.into()).await;
    let (api, _) = serving(redirecting_to(&format!("http://{storage}/file"))).await;
    for transport in [keyed(api), keyed(api).follow_redirects(|_| false)] {
        let (resp, _) = transport.send(get()).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::FOUND);
    }
    assert!(stored.lock().unwrap().is_empty());
}
