//! Routes (`[routes.<model>]`) through the shipped binary: a route's name
//! is a model clients list and ask for, its candidates are picked by price,
//! fall back in turn, and the admin API shows each and explains a pick.
//!
//! Run: `cargo test -p respan-gateway --test routes`.

#![allow(clippy::unwrap_used)]

mod support;

use mockllm::{Answer, FakeOpenAi};
use serde_json::{Value, json};
use support::{Gateway, client};

async fn get_json(gw: &Gateway, path: &str) -> Value {
    let resp = client().get(gw.url(path)).send().await.unwrap();
    assert_eq!(resp.status(), 200, "{path}");
    support::json(&resp.text().await.unwrap())
}

async fn chat(gw: &Gateway, model: &str) -> (u16, String) {
    let body = json!({ "model": model, "messages": [{ "role": "user", "content": "hi" }] });
    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .header("content-type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    (resp.status().as_u16(), resp.text().await.unwrap())
}

fn models_asked(mock: &FakeOpenAi) -> Vec<String> {
    let model = |r: &mockllm::Request| r.object()?.get("model")?.as_str().map(str::to_owned);
    mock.requests().iter().filter_map(model).collect()
}

/// Two providers, `dear` and `cheap`, priced in the config, and a route
/// `smart` to both by price.
async fn start() -> (Gateway, FakeOpenAi, FakeOpenAi) {
    let (dear, cheap) = (FakeOpenAi::start().await, FakeOpenAi::start().await);
    let gw = Gateway::start(&format!(
        "providers = {{ dear = {{ formats = \"openai-chat\", base_url = \"{}\", key = \"k\" }}, \
         cheap = {{ formats = \"openai-chat\", base_url = \"{}\", key = \"k\" }} }}\n\
         [models.\"dear/big\"]\npricing = {{ input = 10.0, output = 30.0 }}\n\
         [models.\"cheap/small\"]\npricing = {{ input = 0.1, output = 0.4 }}\n\
         [routes.smart]\nto = [\"dear/big\", \"cheap/small\"]\npick = \"cheapest\"\n\
         [routes.\"llama-*\"]\nto = \"cheap/{{model}}-turbo\"\n",
        dear.base_url(),
        cheap.base_url()
    ))
    .await;
    (gw, dear, cheap)
}

#[tokio::test]
async fn a_route_by_price_goes_to_the_cheapest_then_falls_back() {
    let (gw, dear, cheap) = start().await;
    cheap.answer(Answer::text("cheap"));
    let (status, body) = chat(&gw, "smart").await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(models_asked(&cheap), ["small"]);
    assert!(models_asked(&dear).is_empty());

    // The cheapest refuses: the next by price answers.
    cheap.answer(Answer::error(503));
    dear.answer(Answer::text("dear"));
    let (status, body) = chat(&gw, "smart").await;
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("dear"), "{body}");
    assert_eq!(models_asked(&dear), ["big"]);

    // A glob sends its own model.
    cheap.answer(Answer::text("llama"));
    assert_eq!(chat(&gw, "llama-3").await.0, 200);
    assert_eq!(
        models_asked(&cheap).last().map(String::as_str),
        Some("llama-3-turbo")
    );
}

#[tokio::test]
async fn a_routes_name_is_a_model_clients_list() {
    let (gw, ..) = start().await;
    let list = get_json(&gw, "/v1/models").await;
    let smart = list["data"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["id"] == "smart");
    let smart = smart.cloned().unwrap_or_default();
    assert_eq!(smart["owned_by"], "routes");
    assert_eq!(
        smart["route"],
        json!({ "pick": "cheapest", "to": ["dear/big", "cheap/small"] })
    );
    // A glob names no one model.
    assert!(!list.to_string().contains("llama-*"));
}

#[tokio::test]
async fn the_admin_api_shows_each_candidate_and_explains_a_pick() {
    let (gw, dear, cheap) = start().await;
    cheap.answer(Answer::text("cheap"));
    dear.answer(Answer::text("dear"));
    assert_eq!(chat(&gw, "smart").await.0, 200);

    let routes = get_json(&gw, "/ui/api/routes").await;
    let smart = &routes["rules"][0];
    assert_eq!(smart["name"], "smart");
    assert_eq!(smart["pick"], "cheapest");
    assert_eq!(smart["to"][1]["model"], "small");
    assert_eq!(
        smart["to"][1]["price"],
        json!({ "input": 0.1, "output": 0.4, "from": "config" })
    );
    assert_eq!(smart["to"][1]["seen"]["calls"], 1);
    assert!(smart["to"][1]["seen"]["latency_ms"].is_number());
    assert_eq!(smart["to"][0]["seen"], Value::Null);
    let llama = &routes["rules"][1];
    assert_eq!(
        (llama["glob"].clone(), llama["to"][0]["model"].clone()),
        (json!(true), Value::Null)
    );

    let explained = get_json(&gw, "/ui/api/routes/explain?model=smart&kind=chat").await;
    assert_eq!(explained["by"], json!({ "type": "rule", "name": "smart" }));
    assert_eq!(explained["mode"], "fallback");
    let order: Vec<&Value> = explained["to"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| &c["provider"])
        .collect();
    assert_eq!(order, ["cheap", "dear"]);
    let named = get_json(&gw, "/ui/api/routes/explain?model=dear/x").await;
    assert_eq!(named["by"]["type"], "named");
    assert_eq!(named["to"][0]["model"], "x");
}
