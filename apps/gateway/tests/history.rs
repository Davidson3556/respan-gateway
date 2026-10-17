//! The request history: its export and the cost of each request.

#![allow(clippy::unwrap_used)]

mod support;

use std::time::Duration;

use mockllm::{Answer, FakeOpenAi};
use serde_json::{Value, json};
use support::{Gateway, client};

const CHAT: &str = "/v1/chat/completions";

fn config(up: &FakeOpenAi, extra: &str) -> String {
    let url = up.base_url();
    format!(
        "{extra}providers = {{ openai = {{ formats = \"openai-chat\", base_url = \"{url}\", key = \"k\" }} }}\n"
    )
}

async fn chat(gw: &Gateway, model: &str) -> u16 {
    let body = json!({"model": model, "messages": [{"role": "user", "content": "hi"}]});
    let resp = client()
        .post(gw.url(CHAT))
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    resp.text().await.unwrap();
    status
}

async fn get(gw: &Gateway, path: &str) -> (u16, String) {
    let resp = client().get(gw.url(path)).send().await.unwrap();
    (resp.status().as_u16(), resp.text().await.unwrap())
}

async fn records(gw: &Gateway) -> Vec<Value> {
    let (_, text) = get(gw, "/ui/api/requests?limit=1000").await;
    let all: Value = support::json(&text);
    let mut all = all["requests"].as_array().unwrap().clone();
    all.reverse();
    all
}

/// Waits, bounded, until every request has finished.
async fn settled(gw: &Gateway) {
    for _ in 0..500 {
        let all = records(gw).await;
        if all.iter().all(|r| !r["duration_ms"].is_null()) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("requests still open");
}

#[tokio::test]
async fn the_history_is_exported_as_json_lines_or_csv_and_filtered() {
    let up = FakeOpenAi::start().await;
    up.answer(Answer::text("hello"));
    let gw = Gateway::start(&config(&up, "client_keys = [\"secret\"]\n")).await;
    let auth = |path: &str| client().get(gw.url(path)).bearer_auth("secret");

    // Keys guard it like the rest of the admin API.
    let (status, _) = get(&gw, "/ui/api/requests/export").await;
    assert_eq!(status, 401);

    let ask = |model: &str| {
        let body = json!({"model": model, "messages": [{"role": "user", "content": "hi"}]});
        let request = client().post(gw.url(CHAT)).bearer_auth("secret");
        request.body(body.to_string()).send()
    };
    for model in ["gpt-4o-mini", "GPT-4o", "o3"] {
        assert_eq!(ask(model).await.unwrap().status(), 200);
    }
    client()
        .get(gw.url("/nowhere"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap();
    settled_with(&gw, "secret").await;

    let export = |query: &str| {
        let path = format!("/ui/api/requests/export{query}");
        async move {
            let resp = auth(&path).send().await.unwrap();
            assert_eq!(resp.status(), 200, "{path}");
            let kind = resp.headers()["content-type"].to_str().unwrap().to_owned();
            let disposition = resp.headers()["content-disposition"].to_str().unwrap();
            assert!(disposition.starts_with("attachment"), "{disposition}");
            (kind, resp.text().await.unwrap())
        }
    };
    let ids = |text: &str| -> Vec<Value> {
        text.lines()
            .map(|l| serde_json::from_str::<Value>(l).unwrap()["model"].clone())
            .collect()
    };

    // Oldest first, the records of the API.
    let (kind, text) = export("").await;
    assert_eq!(kind, "application/x-ndjson");
    assert_eq!(
        ids(&text),
        [
            json!("gpt-4o-mini"),
            json!("GPT-4o"),
            json!("o3"),
            Value::Null
        ]
    );
    let (_, same) = export("?format=jsonl").await;
    assert_eq!(same, text);

    // The UI's filters.
    let (_, text) = export("?model=GPT-4o").await;
    assert_eq!(ids(&text), [json!("GPT-4o")]);
    let (_, text) = export("?q=GPT-4O").await;
    assert_eq!(ids(&text), [json!("gpt-4o-mini"), json!("GPT-4o")]);
    let (_, text) = export("?status=2xx&kind=chat&provider=openai").await;
    assert_eq!(text.lines().count(), 3);
    let (_, text) = export("?status=4xx").await;
    assert_eq!(text.lines().count(), 1);
    let (_, text) = export("?status=error").await;
    assert_eq!(text.lines().count(), 1);
    let (_, text) = export("?status=ok").await;
    assert_eq!(text.lines().count(), 3);
    let (_, text) = export("?provider=nobody").await;
    assert!(text.is_empty());
    let (_, text) = export("?since=2999-01-01").await;
    assert!(text.is_empty());
    let (_, text) = export("?since=2000-01-01").await;
    assert_eq!(text.lines().count(), 4);
    // A time in another zone is a time, not text.
    let (_, text) = export("?since=2999-01-01T00:00:00%2B14:00").await;
    assert!(text.is_empty());
    let (_, text) = export("?since=2000-01-01T09:00:00-05:00").await;
    assert_eq!(text.lines().count(), 4);

    // CSV: a header, a row each, the model quoted if need be.
    let (kind, text) = export("?format=csv&kind=chat").await;
    assert_eq!(kind, "text/csv; charset=utf-8");
    let rows: Vec<&str> = text.lines().collect();
    assert_eq!(rows.len(), 4, "{text}");
    assert!(rows[0].starts_with("id,started_at,method,path,"));
    assert!(
        rows[1]
            .contains(",POST,/v1/chat/completions,openai-chat,chat,gpt-4o-mini,openai,key-1,200,")
    );

    let bad = auth("/ui/api/requests/export?format=xml")
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), 400);
}

async fn settled_with(gw: &Gateway, key: &str) {
    for _ in 0..500 {
        let resp = client()
            .get(gw.url("/ui/api/requests"))
            .bearer_auth(key)
            .send()
            .await
            .unwrap();
        let all: Value = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
        let all = all["requests"].as_array().unwrap();
        if all.len() >= 4 && all.iter().all(|r| !r["duration_ms"].is_null()) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("requests still open");
}

#[tokio::test]
async fn a_declared_models_pricing_gives_each_request_a_cost_and_the_totals_their_sum() {
    let up = FakeOpenAi::start().await;
    up.answer(Answer::text("hello"));
    let prices = "models = { \"openai/gpt-4o\" = { pricing = { input = 1000000, output = 2000000 } }, \
                  \"openai/gpt-4o-mini\" = { pricing = { input = 100000, output = 200000, cached_input = 0 } } }\n";
    let gw = Gateway::start(&config(&up, prices)).await;
    for model in ["gpt-4o", "gpt-4o-mini", "o3"] {
        assert_eq!(chat(&gw, model).await, 200);
    }
    settled(&gw).await;
    let all = records(&gw).await;
    let usage = |r: &Value| {
        let (i, o) = (
            r["usage"]["input_tokens"].as_f64(),
            r["usage"]["output_tokens"].as_f64(),
        );
        (i.unwrap(), o.unwrap())
    };
    let (i, o) = usage(&all[0]);
    assert!(i > 0.0 && o > 0.0, "{}", all[0]);
    // $1M per million tokens is $1 a token.
    assert_eq!(all[0]["cost_usd"].as_f64().unwrap(), i + 2.0 * o);
    // Each model its own.
    let (i, o) = usage(&all[1]);
    let cost = all[1]["cost_usd"].as_f64().unwrap();
    assert!((cost - (0.1 * i + 0.2 * o)).abs() < 1e-9, "{cost}");
    // No price for the model, no cost.
    assert!(all[2].get("cost_usd").is_none());
    let (_, overview) = get(&gw, "/ui/api/overview").await;
    let total = support::json(&overview)["totals"]["cost_usd"]
        .as_f64()
        .unwrap();
    let sum: f64 = all.iter().filter_map(|r| r["cost_usd"].as_f64()).sum();
    assert!((total - sum).abs() < 1e-9);

    // Without prices there is no cost at all.
    let plain = Gateway::start(&config(&up, "")).await;
    assert_eq!(chat(&plain, "gpt-4o").await, 200);
    settled(&plain).await;
    let (_, overview) = get(&plain, "/ui/api/overview").await;
    assert!(support::json(&overview)["totals"].get("cost_usd").is_none());
}

#[tokio::test]
async fn bad_prices_are_refused() {
    let openai = "providers = { openai = { formats = \"openai-chat\", base_url = \"http://x.test\", key = \"k\" } }\n";
    for text in [
        format!(
            "{openai}models = {{ \"openai/gpt-4o\" = {{ pricing = {{ input = -1, output = 1 }} }} }}\n"
        ),
        format!("{openai}models = {{ \"openai/gpt-4o\" = {{ pricing = {{ input = 1 }} }} }}\n"),
        format!(
            "{openai}models = {{ \"gpt-4o\" = {{ pricing = {{ input = 1, output = 1 }} }} }}\n"
        ),
        format!(
            "{openai}models = {{ \"groq/x\" = {{ pricing = {{ input = 1, output = 1 }} }} }}\n"
        ),
        // Retired, saying what replaced it.
        format!("{openai}prices = {{ gpt-4o = {{ input = 1, output = 1 }} }}\n"),
    ] {
        let path = support::write_config(&format!("listen = \"127.0.0.1:0\"\n{text}\n"));
        // A config it takes would serve until killed: a deadline, not a hang.
        let run = tokio::process::Command::new(env!("CARGO_BIN_EXE_respan-gateway"))
            .args(["--config", path.to_str().unwrap()])
            .kill_on_drop(true)
            .output();
        let out = tokio::time::timeout(Duration::from_secs(30), run).await;
        let out = out.unwrap_or_else(|_| panic!("taken, and served: {text}"));
        assert!(!out.unwrap().status.success(), "{text}");
    }
}

#[tokio::test]
async fn requests_carry_the_name_of_their_client_key() {
    let up = FakeOpenAi::start().await;
    up.answer(Answer::text("ok"));
    let keys = "client_keys = [{ name = \"app\", key = \"k-app\" }, \"k-plain\", { name = \"ci\", key = \"k-ci\" }]\n";
    let gw = Gateway::start(&config(&up, keys)).await;
    for (key, model) in [
        ("k-app", "m1"),
        ("k-plain", "m2"),
        ("k-ci", "m3"),
        ("k-app", "m4"),
    ] {
        let body = json!({"model": model, "messages": [{"role": "user", "content": "hi"}]});
        let resp = client()
            .post(gw.url(CHAT))
            .bearer_auth(key)
            .body(body.to_string())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        resp.text().await.unwrap();
    }
    let ask = |path: &'static str| async {
        let resp = client()
            .get(gw.url(path))
            .bearer_auth("k-app")
            .send()
            .await
            .unwrap();
        resp.text().await.unwrap()
    };
    let all: Value = support::json(&ask("/ui/api/requests").await);
    let mut clients: Vec<_> = all["requests"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["path"] == CHAT)
        .map(|r| r["client"].as_str().unwrap().to_owned())
        .collect();
    clients.reverse();
    assert_eq!(clients, ["app", "key-2", "ci", "app"]);
    let key_leaks = ask("/ui/api/requests").await;
    assert!(!key_leaks.contains("k-plain") && !key_leaks.contains("k-ci"));

    let only: Value = support::json(&ask("/ui/api/requests?client=app&limit=1").await);
    assert_eq!(only["requests"].as_array().unwrap().len(), 1);
    let none: Value = support::json(&ask("/ui/api/requests?client=nobody").await);
    assert!(none["requests"].as_array().unwrap().is_empty());
    let jsonl = ask("/ui/api/requests/export?client=ci&kind=chat").await;
    let rows: Vec<Value> = jsonl.lines().map(support::json).collect();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["model"], "m3");
    let csv = ask("/ui/api/requests/export?format=csv&client=key-2").await;
    let mut lines = csv.lines();
    let header: Vec<&str> = lines.next().unwrap().split(',').collect();
    let column = header.iter().position(|c| *c == "client").unwrap();
    let row: Vec<&str> = lines.next().unwrap().split(',').collect();
    assert_eq!(row[column], "key-2");
    assert!(lines.next().is_none());
}

#[tokio::test]
async fn a_request_is_priced_by_the_model_that_served_it() {
    let up = FakeOpenAi::start().await;
    up.answer(Answer::text("hello"));
    // Only the route's candidate is priced, not the name the client asks.
    let extra = "models = { \"openai/gpt-4o-mini\" = { pricing = { input = 1000000, output = 1000000 } } }\nroutes = { smart = { to = [\"openai/gpt-4o-mini\"] } }\n";
    let gw = Gateway::start(&config(&up, extra)).await;
    assert_eq!(chat(&gw, "smart").await, 200);
    settled(&gw).await;
    let all = records(&gw).await;
    assert_eq!(all[0]["model"], "smart");
    let (i, o) = (
        all[0]["usage"]["input_tokens"].as_f64().unwrap(),
        all[0]["usage"]["output_tokens"].as_f64().unwrap(),
    );
    assert_eq!(all[0]["cost_usd"].as_f64(), Some(i + o), "{}", all[0]);
}
