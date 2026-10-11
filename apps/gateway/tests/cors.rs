//! `cors:` lets pages of named origins call the gateway from a browser:
//! their preflight is answered without a key, their answers carry the
//! headers a page needs (the MCP session header included), and any other
//! origin gets none. Without `cors:` a cross-site request is still refused
//! when there are no client keys.

#![allow(clippy::unwrap_used)]

mod support;

use axum::body::Body;
use axum::http::{Request, Response};
use axum::routing::any;
use reqwest::Method;
use support::{Gateway, client};

const APP: &str = "https://app.example.com";
const KEY: &str = "gw-key-1";

/// A streamable-HTTP MCP server that opens sessions.
async fn mcp_server() -> String {
    let app = axum::Router::new().route(
        "/mcp",
        any(|req: Request<Body>| async move {
            let bytes = axum::body::to_bytes(req.into_body(), 1 << 20)
                .await
                .unwrap();
            let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
            let result = serde_json::json!({"protocolVersion": "2025-06-18", "capabilities": {},
                "serverInfo": {"name": "f", "version": "1"}});
            let answer = serde_json::json!({"jsonrpc": "2.0", "id": body["id"], "result": result});
            Response::builder()
                .header("content-type", "application/json")
                .header("mcp-session-id", "s-up")
                .body(Body::from(answer.to_string()))
                .unwrap()
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    base
}

async fn gateway(extra: &str) -> Gateway {
    let up = mcp_server().await;
    let config = format!("mcp:\n  files: {{ url: '{up}/mcp' }}\n{extra}");
    Gateway::start(&config).await
}

async fn preflight(gw: &Gateway, path: &str, origin: &str) -> reqwest::Response {
    client()
        .request(Method::OPTIONS, gw.url(path))
        .header("origin", origin)
        .header("access-control-request-method", "POST")
        .header(
            "access-control-request-headers",
            "authorization, content-type",
        )
        .send()
        .await
        .unwrap()
}

fn header(resp: &reqwest::Response, name: &str) -> Option<String> {
    let value = resp.headers().get(name)?;
    Some(value.to_str().unwrap().to_owned())
}

#[tokio::test]
async fn an_allowed_origins_preflight_is_answered_without_a_key() {
    let gw = gateway(&format!(
        "client_keys: [{KEY}]\ncors: {{ origins: ['{APP}'] }}\n"
    ))
    .await;
    for path in ["/v1/chat/completions", "/mcp/files", "/v1/models"] {
        let resp = preflight(&gw, path, APP).await;
        assert_eq!(resp.status(), 204, "{path}");
        assert_eq!(
            header(&resp, "access-control-allow-origin").as_deref(),
            Some(APP)
        );
        let allowed = header(&resp, "access-control-allow-headers").unwrap();
        for name in [
            "authorization",
            "x-api-key",
            "x-goog-api-key",
            "anthropic-version",
            "content-type",
            "mcp-session-id",
            "mcp-protocol-version",
        ] {
            assert!(allowed.to_lowercase().contains(name), "{name}: {allowed}");
        }
        let methods = header(&resp, "access-control-allow-methods").unwrap();
        assert!(
            methods.contains("POST") && methods.contains("DELETE"),
            "{methods}"
        );
    }
}

#[tokio::test]
async fn an_allowed_origin_reads_the_mcp_session_header_and_needs_a_key() {
    let gw = gateway(&format!(
        "client_keys: [{KEY}]\ncors: {{ origins: ['{APP}'] }}\n"
    ))
    .await;
    let hello = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#;
    let post = || {
        client()
            .post(gw.url("/mcp/files"))
            .header("origin", APP)
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream")
            .body(hello)
    };
    let resp = post()
        .header("authorization", format!("Bearer {KEY}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        header(&resp, "access-control-allow-origin").as_deref(),
        Some(APP)
    );
    let exposed = header(&resp, "access-control-expose-headers").unwrap();
    assert!(exposed.contains("mcp-session-id"), "{exposed}");
    assert!(header(&resp, "mcp-session-id").is_some());
    assert!(header(&resp, "vary").unwrap().contains("Origin"));
    // No key, still refused, and the page can read why.
    let resp = post().send().await.unwrap();
    assert_eq!(resp.status(), 401);
    assert_eq!(
        header(&resp, "access-control-allow-origin").as_deref(),
        Some(APP)
    );
}

#[tokio::test]
async fn another_origin_gets_no_cors_headers() {
    let gw = gateway(&format!(
        "client_keys: [{KEY}]\ncors: {{ origins: ['{APP}'] }}\n"
    ))
    .await;
    let resp = preflight(&gw, "/v1/chat/completions", "https://evil.example").await;
    assert_ne!(resp.status(), 204);
    assert!(header(&resp, "access-control-allow-origin").is_none());
    let resp = client()
        .get(gw.url("/v1/models"))
        .header("origin", "https://evil.example")
        .header("authorization", format!("Bearer {KEY}"))
        .send()
        .await
        .unwrap();
    assert!(header(&resp, "access-control-allow-origin").is_none());
    // A request with no Origin is a plain client's: untouched.
    let resp = client().get(gw.url("/healthz")).send().await.unwrap();
    assert!(header(&resp, "access-control-allow-origin").is_none());
}

#[tokio::test]
async fn without_keys_only_the_listed_origin_gets_through_and_without_cors_none() {
    let gw = gateway(&format!("cors: {{ origins: ['{APP}'] }}\n")).await;
    let from = |origin: &'static str| {
        client()
            .get(gw.url("/v1/models"))
            .header("origin", origin)
            .send()
    };
    assert_eq!(from(APP).await.unwrap().status(), 200);
    assert_eq!(from("https://evil.example").await.unwrap().status(), 403);

    let plain = gateway("").await;
    let resp = client()
        .get(plain.url("/v1/models"))
        .header("origin", APP)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
    assert!(header(&resp, "access-control-allow-origin").is_none());
    let resp = preflight(&plain, "/v1/chat/completions", APP).await;
    assert_ne!(resp.status(), 204);
}

#[tokio::test]
async fn star_needs_client_keys_and_origins_are_checked() {
    let any = gateway(&format!(
        "client_keys: [{KEY}]\ncors: {{ origins: ['*'] }}\n"
    ))
    .await;
    let resp = preflight(&any, "/v1/chat/completions", "https://whatever.example").await;
    assert_eq!(resp.status(), 204);

    let check = |yaml: &str| {
        let path = support::write_config(yaml);
        let out = std::process::Command::new(env!("CARGO_BIN_EXE_respan-gateway"))
            .args(["check", "--config", path.to_str().unwrap()])
            .env_clear()
            .output()
            .unwrap();
        let text = String::from_utf8_lossy(&out.stderr).into_owned()
            + &String::from_utf8_lossy(&out.stdout);
        (out.status.success(), text)
    };
    let base = "providers:\n  p: { formats: openai-chat, base_url: 'http://x.test/v1', key: k }\n";
    let (ok, text) = check(&format!("{base}cors: {{ origins: ['*'] }}\n"));
    assert!(!ok && text.contains("client_keys"), "{text}");
    for bad in [
        "https://app.example.com/",
        "app.example.com",
        "ftp://a.test",
        "https://a.test/x",
    ] {
        let (ok, text) = check(&format!(
            "{base}client_keys: [k]\ncors: {{ origins: ['{bad}'] }}\n"
        ));
        assert!(!ok && text.contains(bad), "{bad}: {text}");
    }
    let (ok, text) = check(&format!(
        "{base}client_keys: [k]\ncors: {{ origins: [] }}\n"
    ));
    assert!(!ok && text.contains("names no origin"), "{text}");
    let (ok, text) = check(&format!(
        "{base}client_keys: [k]\ncors: {{ origins: ['{APP}', 'http://localhost:3000'] }}\n"
    ));
    assert!(ok, "{text}");
}
