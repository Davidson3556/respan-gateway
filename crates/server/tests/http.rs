use axum::Router;
use axum::response::IntoResponse;
use axum::routing::post;
use respan_gateway_core::{Builder, Config};
use respan_gateway_formats::OpenAiChat;
use serde_json::json;
use tokio::net::TcpListener;

const SSE: &str =
    "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n";

async fn spawn(app: Router) -> String {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|e| panic!("bind: {e}"));
    let addr = listener
        .local_addr()
        .unwrap_or_else(|e| panic!("addr: {e}"));
    tokio::spawn(async move { axum::serve(listener, app).await });
    format!("http://{addr}")
}

#[tokio::test]
async fn relays_a_stream_over_real_http() {
    let provider = spawn(Router::new().route(
        "/v1/chat/completions",
        post(|| async { ([("content-type", "text/event-stream")], SSE).into_response() }),
    ))
    .await;

    let config: Config = serde_json::from_value(json!({
        "providers": { "local": { "format": "openai-chat", "base_url": provider } },
        "routes": [{ "name": "chat", "format": "openai-chat", "http": { "path": "/v1/chat/completions" }, "target": "local" }],
    }))
    .unwrap_or_else(|e| panic!("config: {e}"));
    let gateway = Builder::new()
        .format(OpenAiChat)
        .build(&config)
        .unwrap_or_else(|e| panic!("build: {e}"));
    let base = spawn(respan_gateway_server::router(
        gateway,
        respan_gateway_server::MAX_BODY_BYTES,
    ))
    .await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/v1/chat/completions"))
        .body(r#"{"model":"m","stream":true,"messages":[]}"#)
        .send()
        .await
        .unwrap_or_else(|e| panic!("send: {e}"));
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers().get("content-type").map(|v| v.as_bytes()),
        Some(&b"text/event-stream"[..])
    );
    let body = resp.text().await.unwrap_or_default();
    assert_eq!(body, SSE);
}
