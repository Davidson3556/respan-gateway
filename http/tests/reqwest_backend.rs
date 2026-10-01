#![cfg(feature = "reqwest")]

use respan_gateway_http::{HttpClient, http};

#[test]
fn a_base_url_needs_http_or_https_and_a_host() {
    for url in [
        "http://[::1]",
        "http://[::1]:8080",
        "https://h.test:443/v1beta",
        "https://h.test/models/m@2",
    ] {
        assert!(HttpClient::new(url).is_ok(), "{url}");
    }
    // A base URL may contain a key, so the error does not echo it.
    let expected = "bad base URL: an http or https one with a host, and no query, fragment or credentials (those go in its headers)";
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
        let fault = HttpClient::new(url).err();
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
        let fault = HttpClient::new("https://h.test")
            .unwrap_or_else(|e| panic!("{e}"))
            .with_headers(one(name))
            .err();
        let expected = format!("header '{name}' is the connection's");
        assert_eq!(fault.as_ref().map(|f| f.message()), Some(&*expected));
    }
    assert!(
        HttpClient::new("https://h.test")
            .unwrap_or_else(|e| panic!("{e}"))
            .with_headers(one("x-team"))
            .is_ok()
    );
}
