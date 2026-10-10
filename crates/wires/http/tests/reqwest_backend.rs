#![cfg(feature = "reqwest")]

use respan_gateway_http::{HttpTransport, http};

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
    assert!(fault.is::<respan_gateway_core::Upstream>(), "{fault:?}");
}
