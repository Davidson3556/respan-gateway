//! Respan observability (`plugins.respan`) through the shipped binary: a
//! request is posted to Respan's OTLP endpoint as a trace, its root span
//! naming the client key's customer and a span for its call to the model.
//!
//! Run: `cargo test -p respan-gateway --test respan`.

#![allow(clippy::unwrap_used)]

mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Json;
use axum::http::HeaderMap;
use axum::routing::post;
use serde_json::{Value, json};
use support::{Gateway, client};

/// A Chat Completions provider that answers `Hello`.
async fn model() -> String {
    let app = axum::Router::new().route(
        "/v1/chat/completions",
        post(|| async {
            Json(
                json!({ "id": "c1", "object": "chat.completion", "model": "gpt-4o-mini-2024",
                "choices": [{ "index": 0, "finish_reason": "stop",
                    "message": { "role": "assistant", "content": "Hello" } }],
                "usage": { "prompt_tokens": 9, "completion_tokens": 1, "total_tokens": 10 } }),
            )
        }),
    );
    serve(app).await
}

/// Respan's trace endpoint, recording each post's authorization and body.
async fn respan() -> (String, Arc<Mutex<Vec<(String, Value)>>>) {
    let posted: Arc<Mutex<Vec<(String, Value)>>> = Arc::default();
    let seen = posted.clone();
    let app = axum::Router::new().route(
        "/api/v2/traces",
        post(move |headers: HeaderMap, Json(body): Json<Value>| {
            let seen = seen.clone();
            async move {
                let auth = headers["authorization"]
                    .to_str()
                    .unwrap_or_default()
                    .to_owned();
                seen.lock().unwrap().push((auth, body));
                Json(json!({ "partialSuccess": { "rejectedSpans": 0, "errorMessage": "" } }))
            }
        }),
    );
    (serve(app).await, posted)
}

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    base
}

fn attr(span: &Value, key: &str) -> Value {
    let attrs = span["attributes"].as_array().unwrap();
    let found = attrs
        .iter()
        .find(|a| a["key"] == key)
        .map(|a| a["value"].clone());
    found.unwrap_or(Value::Null)
}

#[tokio::test]
async fn a_request_is_sent_to_respan_as_a_trace() {
    let (model, (respan, posted)) = (model().await, respan().await);
    let gw = Gateway::start(&format!(
        "client_keys = [{{ name = \"ops\", key = \"k-ops\" }}]\n\
         providers = {{ p = {{ formats = \"openai-chat\", base_url = \"{model}/v1\" }} }}\n\
         plugins = {{ respan = {{ key = \"rk-1\", url = \"{respan}/api/v2/traces\", service = \"test-gw\" }} }}\n"
    ))
    .await;
    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .bearer_auth("k-ops")
        .header("content-type", "application/json")
        .body(json!({ "model": "gpt-4o-mini", "messages": [{ "role": "user", "content": "Say hi" }] }).to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Posted within a batch's wait.
    let mut waited = Duration::ZERO;
    while posted.lock().unwrap().is_empty() && waited < Duration::from_secs(10) {
        tokio::time::sleep(Duration::from_millis(100)).await;
        waited += Duration::from_millis(100);
    }
    let posted = posted.lock().unwrap().clone();
    let (auth, body) = posted.first().expect("a trace posted");
    assert_eq!(auth, "Bearer rk-1");
    let resource = &body["resourceSpans"][0];
    assert_eq!(
        attr(&resource["resource"], "service.name")["stringValue"],
        "test-gw"
    );
    let spans = resource["scopeSpans"][0]["spans"].as_array().unwrap();
    assert_eq!(spans.len(), 2, "{spans:?}");
    let (root, chat) = (&spans[0], &spans[1]);
    assert_eq!(chat["traceId"], root["traceId"]);
    assert_eq!(chat["parentSpanId"], root["spanId"]);
    assert_eq!(
        attr(root, "respan.customer_params.customer_identifier")["stringValue"],
        "ops"
    );
    assert_eq!(attr(chat, "llm.request.type")["stringValue"], "chat");
    assert_eq!(
        attr(chat, "gen_ai.request.model")["stringValue"],
        "gpt-4o-mini"
    );
    assert_eq!(
        attr(chat, "gen_ai.response.model")["stringValue"],
        "gpt-4o-mini-2024"
    );
    assert_eq!(attr(chat, "gen_ai.usage.total_tokens")["intValue"], "10");
    assert_eq!(
        attr(chat, "gen_ai.prompt.0.content")["stringValue"],
        "Say hi"
    );
    assert_eq!(
        attr(chat, "gen_ai.completion.0.content")["stringValue"],
        "Hello"
    );
}
