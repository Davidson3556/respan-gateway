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
    respan_answering(
        200,
        json!({ "partialSuccess": { "rejectedSpans": 0, "errorMessage": "" } }),
    )
    .await
}

/// Respan's trace endpoint answering `status` with `answer`.
async fn respan_answering(
    status: u16,
    answer: Value,
) -> (String, Arc<Mutex<Vec<(String, Value)>>>) {
    let posted: Arc<Mutex<Vec<(String, Value)>>> = Arc::default();
    let seen = posted.clone();
    let app = axum::Router::new().route(
        "/api/v2/traces",
        post(move |headers: HeaderMap, Json(body): Json<Value>| {
            let seen = seen.clone();
            let answer = answer.clone();
            async move {
                let auth = headers["authorization"]
                    .to_str()
                    .unwrap_or_default()
                    .to_owned();
                seen.lock().unwrap().push((auth, body));
                let status = axum::http::StatusCode::from_u16(status).unwrap();
                (status, Json(answer.clone()))
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

async fn chat(gw: &Gateway) {
    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .bearer_auth("k-ops")
        .header("content-type", "application/json")
        .body(
            json!({ "model": "m", "messages": [{ "role": "user", "content": "hi" }] }).to_string(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

/// The overview's `respan` once `ready` holds of it, within a batch's wait.
async fn respan_health(gw: &Gateway, ready: impl Fn(&Value) -> bool) -> Value {
    let mut waited = Duration::ZERO;
    loop {
        let overview = client()
            .get(gw.url("/ui/api/overview"))
            .bearer_auth("k-ops")
            .send();
        let overview: Value = support::json(&overview.await.unwrap().text().await.unwrap());
        if ready(&overview["respan"]) || waited > Duration::from_secs(10) {
            return overview["respan"].clone();
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        waited += Duration::from_millis(100);
    }
}

async fn gateway_to(respan: &str) -> Gateway {
    let model = model().await;
    Gateway::start(&format!(
        "client_keys = [{{ name = \"ops\", key = \"k-ops\", admin = true }}]\n\
         providers = {{ p = {{ formats = \"openai-chat\", base_url = \"{model}/v1\" }} }}\n\
         plugins = {{ respan = {{ key = \"rk-1\", url = \"{respan}/api/v2/traces\" }} }}\n"
    ))
    .await
}

#[tokio::test]
async fn the_overview_says_how_sending_to_respan_goes() {
    let (respan, _) = respan().await;
    let gw = gateway_to(&respan).await;
    chat(&gw).await;
    let health = respan_health(&gw, |h| h["sent"].as_u64() > Some(0)).await;
    assert_eq!(health["sent"], 2, "{health}");
    assert_eq!(
        (health["failing"].clone(), health["error"].clone()),
        (json!(false), Value::Null)
    );
    assert!(health["ok_at"].is_string());
}

#[tokio::test]
async fn a_failing_or_rejecting_respan_is_said() {
    let (refusing, _) = respan_answering(401, json!({ "detail": "Invalid API key" })).await;
    let gw = gateway_to(&refusing).await;
    chat(&gw).await;
    let health = respan_health(&gw, |h| h["failing"] == true).await;
    assert_eq!(health["failed"], 2, "{health}");
    let said = health["error"]["message"].as_str().unwrap_or_default();
    assert!(
        said.contains("401") && said.contains("Invalid API key"),
        "{said}"
    );

    let partial = json!({ "partialSuccess": { "rejectedSpans": "1", "errorMessage": "bad span" } });
    let (rejecting, _) = respan_answering(200, partial).await;
    let gw = gateway_to(&rejecting).await;
    chat(&gw).await;
    let health = respan_health(&gw, |h| h["rejected"].as_u64() > Some(0)).await;
    assert_eq!(
        (health["sent"].clone(), health["rejected"].clone()),
        (json!(1), json!(1)),
        "{health}"
    );
    let said = health["error"]["message"].as_str().unwrap_or_default();
    assert!(said.contains("rejected 1 of 2 spans: bad span"), "{said}");
}

#[tokio::test]
async fn the_dashboards_look_at_a_tool_server_is_not_traced() {
    let server = serde_json::to_string(
        "import sys, json\n\
         for line in sys.stdin:\n\
         \x20   m = json.loads(line)\n\
         \x20   i = m.get('id')\n\
         \x20   if m.get('method') == 'initialize':\n\
         \x20       r = {'protocolVersion': '2025-06-18', 'capabilities': {'tools': {}}, 'serverInfo': {'name': 's', 'version': '1'}}\n\
         \x20   elif m.get('method') == 'tools/list':\n\
         \x20       r = {'tools': [{'name': 'echo', 'inputSchema': {'type': 'object'}}]}\n\
         \x20   else:\n\
         \x20       continue\n\
         \x20   print(json.dumps({'jsonrpc': '2.0', 'id': i, 'result': r}), flush=True)\n",
    )
    .unwrap();
    let (respan, posted) = respan().await;
    let python = if cfg!(windows) { "python" } else { "python3" };
    let gw = Gateway::start(&format!(
        "client_keys = [{{ name = \"ops\", key = \"k-ops\", admin = true }}]\n\
         mcp = {{ py = {{ command = \"{python}\", args = [\"-c\", {server}] }} }}\n\
         plugins = {{ respan = {{ key = \"rk-1\", url = \"{respan}/api/v2/traces\" }} }}\n"
    ))
    .await;
    let tools = client()
        .get(gw.url("/ui/api/mcp/py/tools"))
        .bearer_auth("k-ops")
        .send();
    let tools: Value = support::json(&tools.await.unwrap().text().await.unwrap());
    assert_eq!(tools["tools"][0]["name"], "echo", "{tools}");
    // Longer than a batch's wait.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(posted.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_request_another_plugin_answers_is_traced_too() {
    // A route's dispatch answers the request itself, as the tool loop does.
    let (model, (respan, posted)) = (model().await, respan().await);
    let gw = Gateway::start(&format!(
        "client_keys = [{{ name = \"ops\", key = \"k-ops\" }}]\n\
         providers = {{ p = {{ formats = \"openai-chat\", base_url = \"{model}/v1\" }} }}\n\
         plugins = {{ respan = {{ key = \"rk-1\", url = \"{respan}/api/v2/traces\" }} }}\n\
         [routes.smart]\nto = \"p/gpt-4o-mini\"\n"
    ))
    .await;
    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .bearer_auth("k-ops")
        .header("content-type", "application/json")
        .body(
            json!({ "model": "smart", "messages": [{ "role": "user", "content": "hi" }] })
                .to_string(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let mut waited = Duration::ZERO;
    while posted.lock().unwrap().is_empty() && waited < Duration::from_secs(10) {
        tokio::time::sleep(Duration::from_millis(100)).await;
        waited += Duration::from_millis(100);
    }
    let posted = posted.lock().unwrap().clone();
    let (_, body) = posted.first().expect("a trace posted");
    let spans = body["resourceSpans"][0]["scopeSpans"][0]["spans"]
        .as_array()
        .unwrap();
    assert_eq!(spans.len(), 2, "the request and its call: {spans:?}");
    assert_eq!(
        attr(&spans[1], "gen_ai.request.model")["stringValue"],
        "gpt-4o-mini"
    );
}
