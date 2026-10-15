//! The request history: its export, its file (kept across restarts,
//! rotated, never failing a request) and the cost of each request.

#![allow(clippy::unwrap_used)]

mod support;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use mockllm::{Answer, FakeOpenAi};
use serde_json::{Value, json};
use support::{Gateway, client};

const CHAT: &str = "/v1/chat/completions";

fn scratch(name: &str) -> PathBuf {
    static N: AtomicUsize = AtomicUsize::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR"));
    dir.join(format!("history-{}-{n}-{name}", std::process::id()))
}

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

/// Waits, bounded, until `file` has `lines` lines.
async fn lines_in(file: &Path, lines: usize) -> Vec<String> {
    for _ in 0..500 {
        let text = std::fs::read_to_string(file).unwrap_or_default();
        if text.lines().count() >= lines {
            return text.lines().map(str::to_owned).collect();
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("{} never had {lines} lines", file.display());
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
    let (_, text) = export("?model=GPT-4O").await;
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
async fn a_history_file_keeps_the_history_across_a_restart() {
    let file = scratch("kept.jsonl");
    let up = FakeOpenAi::start().await;
    up.answer(Answer::text("hello"));
    let text = config(
        &up,
        &format!(
            "plugins = {{ request_log = {{ file = \"{}\" }} }}\n",
            file.display()
        ),
    );

    let mut first = Gateway::start(&text).await;
    for model in ["gpt-4o", "o3"] {
        assert_eq!(chat(&first, model).await, 200);
    }
    settled(&first).await;
    let written = lines_in(&file, 2).await;
    first.child.kill().await.unwrap();

    // The file holds the API's records, and nothing of the conversation.
    let line: Value = serde_json::from_str(&written[0]).unwrap();
    assert_eq!(line["model"], "gpt-4o");
    assert_eq!(line["provider"], "openai");
    assert!(!written.join("").contains("hello"));

    let second = Gateway::start(&text).await;
    let kept = records(&second).await;
    let models: Vec<&Value> = kept.iter().map(|r| &r["model"]).collect();
    assert_eq!(models, ["gpt-4o", "o3"]);
    assert_eq!(kept[0]["id"], line["id"]);
    // Detail is served for them, and new requests number on.
    let (status, _) = get(
        &second,
        &format!("/ui/api/requests/{}", line["id"].as_str().unwrap()),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(chat(&second, "gpt-4o").await, 200);
    settled(&second).await;
    let all = records(&second).await;
    assert_eq!(all.len(), 3);
    assert_ne!(all[2]["id"], all[1]["id"]);
    assert!(all[2]["id"].as_str() > all[1]["id"].as_str());
    // They were not this run's requests.
    let (_, overview) = get(&second, "/ui/api/overview").await;
    assert_eq!(support::json(&overview)["totals"]["requests"], 1);
}

#[tokio::test]
async fn only_the_last_request_history_records_are_read_back() {
    let file = scratch("tail.jsonl");
    let up = FakeOpenAi::start().await;
    up.answer(Answer::text("hello"));
    let text = config(
        &up,
        &format!(
            "plugins = {{ request_log = {{ file = \"{}\", history = 2 }} }}\n",
            file.display()
        ),
    );
    let mut first = Gateway::start(&text).await;
    for model in ["a", "b", "c"] {
        assert_eq!(chat(&first, model).await, 200);
    }
    lines_in(&file, 3).await;
    first.child.kill().await.unwrap();
    let second = Gateway::start(&text).await;
    let models: Vec<Value> = records(&second)
        .await
        .iter()
        .map(|r| r["model"].clone())
        .collect();
    assert_eq!(models, [json!("b"), json!("c")]);
}

#[tokio::test]
async fn the_file_is_rotated_by_size_keeping_one_old_file() {
    let file = scratch("rotated.jsonl");
    let old = PathBuf::from(format!("{}.1", file.display()));
    let up = FakeOpenAi::start().await;
    up.answer(Answer::text("hello"));
    let text = config(
        &up,
        &format!(
            "plugins = {{ request_log = {{ file = \"{}\", file_max = \"3KiB\" }} }}\n",
            file.display()
        ),
    );
    let gw = Gateway::start(&text).await;
    for _ in 0..30 {
        assert_eq!(chat(&gw, "gpt-4o").await, 200);
    }
    settled(&gw).await;
    // Every record lands in the file or the old one, and neither is past
    // the limit; the oldest have gone with the replaced old file.
    for _ in 0..500 {
        let size = |p: &Path| std::fs::metadata(p).map_or(0, |m| m.len());
        if old.exists() && size(&file) + size(&old) > 0 {
            let kept = lines_in(&file, 1).await.len() + lines_in(&old, 1).await.len();
            if kept >= 10 {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(old.exists(), "no rotated file");
    assert!(std::fs::metadata(&file).unwrap().len() <= 3 << 10);
    assert!(std::fs::metadata(&old).unwrap().len() <= 3 << 10);
    assert!(!PathBuf::from(format!("{}.2", file.display())).exists());
    for line in lines_in(&old, 1).await {
        serde_json::from_str::<Value>(&line).unwrap();
    }
}

#[tokio::test]
async fn a_file_that_cannot_be_written_does_not_fail_requests() {
    // Its parent is a file, so it can neither be read nor created.
    let parent = scratch("not-a-dir");
    std::fs::write(&parent, "x").unwrap();
    let file = parent.join("requests.jsonl");
    let up = FakeOpenAi::start().await;
    up.answer(Answer::text("hello"));
    let text = config(
        &up,
        &format!(
            "plugins = {{ request_log = {{ file = \"{}\" }} }}\n",
            file.display()
        ),
    );
    let gw = Gateway::start(&text).await;
    for _ in 0..3 {
        assert_eq!(chat(&gw, "gpt-4o").await, 200);
    }
    settled(&gw).await;
    assert_eq!(records(&gw).await.len(), 3);
    assert!(!file.exists());
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
async fn bad_prices_and_sizes_are_refused() {
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
        "plugins = { request_log = { file_max = 0 } }\n".to_owned(),
    ] {
        let path = support::write_config(&format!("listen = \"127.0.0.1:0\"\n{text}\n"));
        let out = tokio::process::Command::new(env!("CARGO_BIN_EXE_respan-gateway"))
            .args(["--config", path.to_str().unwrap()])
            .output()
            .await
            .unwrap();
        assert!(!out.status.success(), "{text}");
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

#[cfg(unix)]
#[tokio::test]
async fn the_history_file_is_private() {
    use std::os::unix::fs::PermissionsExt;
    let file = scratch("private.jsonl");
    let up = FakeOpenAi::start().await;
    up.answer(Answer::text("hello"));
    let text = config(
        &up,
        &format!(
            "plugins = {{ request_log = {{ file = \"{}\" }} }}\n",
            file.display()
        ),
    );
    let gw = Gateway::start(&text).await;
    assert_eq!(chat(&gw, "gpt-4o").await, 200);
    lines_in(&file, 1).await;
    let mode = std::fs::metadata(&file).unwrap().permissions().mode();
    assert_eq!(mode & 0o777, 0o600);
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
