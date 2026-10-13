//! The decision kind through the shipped binary: a System One request over
//! real HTTP to a process of `respan-gateway`, on to a fake of TypeSafe's
//! API, answered and logged with its usage.
//!
//! Run: `cargo test -p respan-gateway --test decision`.

#![allow(clippy::unwrap_used)]

mod support;

use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::post;
use serde_json::{Value, json};
use support::{Gateway, client, json as parse};

/// TypeSafe's `POST /v1/systemone`, which answers every question it is
/// asked, by type, and refuses a call without its key.
async fn fake() -> String {
    let systemone = |headers: HeaderMap, body: axum::Json<Value>| async move {
        if headers["authorization"] != "Bearer ts-key" {
            return (
                StatusCode::UNAUTHORIZED,
                axum::Json(json!({"detail": "no key"})),
            )
                .into_response();
        }
        let answers: serde_json::Map<String, Value> = body["questions"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(id, q)| {
                let answer = match q["type"].as_str().unwrap() {
                    "noul" => json!({"type": "noul", "noul": 0.97}),
                    _ => json!({"type": "choice", "choice": "calm", "probabilities": {"calm": 0.6, "angry": 0.4}, "confidence": 0.7}),
                };
                (id.clone(), answer)
            })
            .collect();
        let usage = json!({"input_tokens": 31, "output_tokens": 0});
        axum::Json(json!({"model": "jev-1.13.0", "answers": answers, "usage": usage}))
            .into_response()
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let app = axum::Router::new().route("/v1/systemone", post(systemone));
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    base
}

#[tokio::test]
async fn a_decision_is_answered_and_its_tokens_counted() {
    let base = fake().await;
    let gw = Gateway::start(&format!(
        "providers = {{ ts = {{ vendor = \"typesafe\", key = \"ts-key\", base_url = \"{base}/v1\" }} }}\n"
    ))
    .await;
    let asked = json!({
        "model": "jev-latest",
        "state": "The sky is blue.",
        "questions": {
            "blue": {"type": "noul", "instructions": "Is the sky blue?"},
            "tone": {"type": "choice", "criteria": {"calm": null, "angry": null}},
        },
    });
    let resp = client()
        .post(gw.url("/v1/systemone"))
        .header("content-type", "application/json")
        .body(asked.to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = parse(&resp.text().await.unwrap());
    assert_eq!(
        body["answers"]["blue"],
        json!({"type": "noul", "noul": 0.97})
    );
    assert_eq!(body["answers"]["tone"]["choice"], "calm");
    assert_eq!(body["usage"]["input_tokens"], 31);

    for _ in 0..200 {
        let log = client()
            .get(gw.url("/ui/api/requests?limit=50"))
            .send()
            .await
            .unwrap();
        let log = parse(&log.text().await.unwrap());
        let done = log["requests"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["path"] == "/v1/systemone" && !r["duration_ms"].is_null());
        if let Some(record) = done {
            assert_eq!(record["kind"], "decision");
            assert_eq!(
                record["usage"],
                json!({"input_tokens": 31, "output_tokens": 0})
            );
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("no finished /v1/systemone in the request log");
}

#[tokio::test]
async fn a_question_of_no_type_is_refused_before_the_provider() {
    let gw = Gateway::start(
        "providers = { ts = { vendor = \"typesafe\", key = \"k\", base_url = \"http://127.0.0.1:9/v1\" } }\n",
    )
    .await;
    let resp = client()
        .post(gw.url("/v1/systemone"))
        .header("content-type", "application/json")
        .body(json!({"state": "s", "questions": {"q": {"instructions": "?"}}}).to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body = parse(&resp.text().await.unwrap());
    assert_eq!(body["error"]["param"], "questions.q", "{body}");
}
