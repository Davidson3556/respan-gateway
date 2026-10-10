//! The application around the gateway, end to end: zero-config start from
//! the environment, `init`, client keys, `/healthz`, `/v1/models`, the web
//! UI and its admin API.

#![allow(clippy::unwrap_used)]

mod support;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use futures::StreamExt;
use mockllm::{Answer, FakeAnthropic, FakeOpenAi};
use serde_json::{Value, json};
use support::{BOUND, Gateway, client, closed_port};

const CHAT: &str = "/v1/chat/completions";

fn chat(model: &str) -> String {
    json!({"model": model, "messages": [{"role": "user", "content": "hi"}]}).to_string()
}

/// A fresh, empty directory: no `gateway.yaml` in it.
fn empty_dir() -> PathBuf {
    static N: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"));
    let dir = dir.join(format!("app-{}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn post_chat(gw: &Gateway, model: &str, headers: &[(&str, &str)]) -> reqwest::Response {
    let mut req = client().post(gw.url(CHAT)).body(chat(model));
    for (name, value) in headers {
        req = req.header(*name, *value);
    }
    req.send().await.unwrap()
}

async fn get_json(gw: &Gateway, path: &str) -> Value {
    let resp = client().get(gw.url(path)).send().await.unwrap();
    assert_eq!(resp.status(), 200, "{path}");
    support::json(&resp.text().await.unwrap())
}

#[tokio::test]
async fn with_no_config_every_vendor_with_a_key_in_the_environment_is_a_provider() {
    let openai = FakeOpenAi::start().await;
    let anthropic = FakeAnthropic::start().await;
    let env = [
        ("OPENAI_API_KEY", "sk-env-openai"),
        ("RESPAN_OPENAI_BASE_URL", &*openai.base_url()),
        ("ANTHROPIC_API_KEY", "sk-env-anthropic"),
        ("RESPAN_ANTHROPIC_BASE_URL", &*anthropic.base_url()),
    ];
    let gw = Gateway::run_with(&["--listen", "127.0.0.1:0"], &empty_dir(), &env).await;
    let banner = gw.stdout_until("try").await;
    assert!(banner.contains("config     environment"), "{banner}");
    assert!(
        banner.contains("openai     openai-chat, openai-realtime, responses-ws"),
        "{banner}"
    );
    assert!(banner.contains("anthropic  anthropic-messages"), "{banner}");
    assert!(banner.contains("/ui/"), "{banner}");
    assert!(banner.contains("curl http://127.0.0.1:"), "{banner}");

    // Each model goes to its vendor by name, with the vendor's key.
    openai.answer(Answer::text("hi"));
    anthropic.answer(Answer::text("hi"));
    assert_eq!(post_chat(&gw, "gpt-4.1-mini", &[]).await.status(), 200);
    assert_eq!(post_chat(&gw, "claude-sonnet-4-5", &[]).await.status(), 200);
    let auth = openai.requests()[0]
        .header("authorization")
        .map(str::to_owned);
    assert_eq!(auth.as_deref(), Some("Bearer sk-env-openai"));
    assert_eq!(
        anthropic.requests()[0].header("x-api-key"),
        Some("sk-env-anthropic")
    );

    // A model that names its provider goes there, without the name; one
    // no vendor claims goes to the first provider.
    openai.answer(Answer::text("hi"));
    anthropic.answer(Answer::text("hi"));
    assert_eq!(
        post_chat(&gw, "anthropic/claude-sonnet-4-5", &[])
            .await
            .status(),
        200
    );
    assert_eq!(post_chat(&gw, "llama-3", &[]).await.status(), 200);
    let sent = anthropic.requests()[1].object().unwrap()["model"].clone();
    assert_eq!(sent, "claude-sonnet-4-5");
    assert_eq!(openai.requests()[1].object().unwrap()["model"], "llama-3");
}

#[test]
fn with_no_config_and_no_key_it_says_what_to_do() {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_respan-gateway"))
        .current_dir(empty_dir())
        .env_clear()
        .output()
        .unwrap();
    assert!(!out.status.success());
    let said = String::from_utf8_lossy(&out.stderr);
    assert!(said.contains("OPENAI_API_KEY"), "{said}");
    assert!(said.contains("ANTHROPIC_API_KEY"), "{said}");
    assert!(said.contains("respan-gateway init"), "{said}");
}

#[test]
fn init_writes_a_config_that_check_accepts() {
    let dir = empty_dir();
    let run = |args: &[&str]| {
        std::process::Command::new(env!("CARGO_BIN_EXE_respan-gateway"))
            .args(args)
            .current_dir(&dir)
            .env_clear()
            .env("ANTHROPIC_API_KEY", "sk-ant")
            .output()
            .unwrap()
    };
    let out = run(&["init"]);
    assert!(out.status.success(), "{out:?}");
    let written = std::fs::read_to_string(dir.join("gateway.yaml")).unwrap();
    assert!(
        written.contains("\n  anthropic: { vendor: anthropic }"),
        "{written}"
    );
    assert!(
        written.contains("\n  # openai: { vendor: openai }"),
        "{written}"
    );

    let out = run(&["check"]);
    assert!(out.status.success(), "{out:?}");
    assert_eq!(String::from_utf8_lossy(&out.stdout), "gateway.yaml: ok\n");

    let out = run(&["init"]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("--force"));
    let out = run(&["init", "--force", "-o", "other.yaml"]);
    assert!(out.status.success(), "{out:?}");
    assert!(dir.join("other.yaml").is_file());
}

#[tokio::test]
async fn client_keys_are_required_and_never_forwarded() {
    let openai = FakeOpenAi::start().await;
    let gw = Gateway::start(&format!(
        "keys: [gw-key]
providers:
  own: {{ format: openai-chat, base_url: '{}', key: sk-gw-openai }}
  theirs: {{ format: openai-chat, base_url: '{}' }}
",
        openai.base_url(),
        openai.base_url(),
    ))
    .await;

    for missing in [vec![], vec![("authorization", "Bearer wrong")]] {
        let resp = post_chat(&gw, "gpt-4.1-mini", &missing).await;
        assert_eq!(resp.status(), 401);
        let error = support::json(&resp.text().await.unwrap());
        assert_eq!(error["error"]["type"], "authentication_error", "{error}");
    }
    let admin = client()
        .get(gw.url("/ui/api/overview"))
        .send()
        .await
        .unwrap();
    assert_eq!(admin.status(), 401);
    assert!(openai.requests().is_empty());

    openai.answer(Answer::text("hi"));
    let resp = post_chat(&gw, "gpt-4.1-mini", &[("authorization", "Bearer gw-key")]).await;
    assert_eq!(resp.status(), 200);
    let auth = openai.requests()[0]
        .header("authorization")
        .map(str::to_owned);
    assert_eq!(auth.as_deref(), Some("Bearer sk-gw-openai"));

    // As x-api-key, beside a key of the client's own: the provider's key
    // goes, or, to a provider without one, the client's; never the gateway's.
    for (provider, sent) in [
        ("own", "Bearer sk-gw-openai"),
        ("theirs", "Bearer sk-client"),
    ] {
        openai.answer(Answer::text("hi"));
        let headers = [
            ("x-api-key", "gw-key"),
            ("authorization", "Bearer sk-client"),
        ];
        let model = format!("{provider}/gpt-4.1-mini");
        let resp = post_chat(&gw, &model, &headers).await;
        assert_eq!(resp.status(), 200, "{provider}");
        let got = openai.requests().pop().unwrap();
        assert_eq!(got.header("authorization"), Some(sent));
        assert_eq!(got.header("x-api-key"), None);
    }

    let overview = client()
        .get(gw.url("/ui/api/overview"))
        .bearer_auth("gw-key");
    let overview = support::json(&overview.send().await.unwrap().text().await.unwrap());
    assert_eq!(overview["auth_required"], true);
}

#[tokio::test]
async fn health_and_the_web_ui_are_served() {
    let gw = Gateway::start("providers: {}\n").await;
    let health = client().get(gw.url("/healthz")).send().await.unwrap();
    assert_eq!(health.status(), 200);
    assert_eq!(health.text().await.unwrap(), "ok");

    let no_redirects = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(BOUND)
        .build()
        .unwrap();
    let root = no_redirects.get(gw.url("/")).send().await.unwrap();
    assert!(root.status().is_redirection(), "{root:?}");
    assert_eq!(root.headers()["location"], "/ui/");

    let page = |path: &'static str| {
        let url = gw.url(path);
        async move {
            let resp = client().get(url).send().await.unwrap();
            assert_eq!(resp.status(), 200, "{path}");
            let content_type = resp.headers()["content-type"].to_str().unwrap().to_owned();
            assert_eq!(content_type, "text/html; charset=utf-8", "{path}");
            resp.text().await.unwrap()
        }
    };
    let index = page("/ui/").await;
    assert!(index.contains("<html"), "{index}");
    // A path of the app's own is its index too.
    assert_eq!(page("/ui/requests/r_000001").await, index);
}

/// An OpenAI-compatible server listing `ids` at `/v1/models`, and the
/// `Authorization` of each request it got.
async fn listing(ids: &'static [&'static str]) -> (String, Arc<Mutex<Vec<String>>>) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let saw = seen.clone();
    let list = move |headers: axum::http::HeaderMap| {
        let auth = headers.get("authorization").and_then(|v| v.to_str().ok());
        saw.lock()
            .unwrap()
            .push(auth.unwrap_or_default().to_owned());
        let data: Vec<Value> = ids
            .iter()
            .map(|id| json!({"id": id, "object": "model", "created": 1, "owned_by": "x"}))
            .collect();
        async move { axum::Json(json!({"object": "list", "data": data})) }
    };
    let app = axum::Router::new().route("/v1/models", axum::routing::get(list));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await });
    (format!("http://{addr}/v1"), seen)
}

#[tokio::test]
async fn models_are_merged_from_every_provider() {
    let anthropic = FakeAnthropic::start().await;
    let (openai, seen) = listing(&["gpt-4o", "llama-x"]).await;
    let gw = Gateway::start(&format!(
        "providers:
  anthropic: {{ vendor: anthropic, base_url: '{}', key: sk-ant }}
  openai: {{ vendor: openai, base_url: '{openai}', key: sk-openai }}
  down: {{ format: openai-chat, base_url: '{}/v1' }}
",
        anthropic.base_url(),
        closed_port().await,
    ))
    .await;
    let list = get_json(&gw, "/v1/models").await;
    assert_eq!(list["object"], "list");
    let ids: Vec<(&str, &str)> = list["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| (m["id"].as_str().unwrap(), m["owned_by"].as_str().unwrap()))
        .collect();
    // As routing reaches them: Claude's and GPT's by name, the rest by
    // their provider's.
    assert!(
        ids.contains(&("claude-sonnet-4-5-20250929", "anthropic")),
        "{ids:?}"
    );
    assert!(ids.contains(&("gpt-4o", "openai")), "{ids:?}");
    assert!(ids.contains(&("openai/llama-x", "openai")), "{ids:?}");
    assert!(ids.iter().all(|(_, owner)| *owner != "down"), "{ids:?}");
    assert_eq!(list["data"][0]["kind"], "chat");
    assert_eq!(*seen.lock().unwrap(), ["Bearer sk-openai"]);
    let asked = &anthropic.requests()[0];
    assert_eq!(asked.header("x-api-key"), Some("sk-ant"));
    assert!(asked.header("anthropic-version").is_some());

    // Cached: asked once.
    get_json(&gw, "/v1/models").await;
    assert_eq!(seen.lock().unwrap().len(), 1);
}

/// The `request` events of an SSE body read so far.
fn request_events(body: &str) -> Vec<Value> {
    body.split("\n\n")
        .filter(|frame| frame.lines().any(|l| l == "event: request"))
        .filter_map(|frame| frame.lines().find_map(|l| l.strip_prefix("data: ")))
        .map(|data| serde_json::from_str(data).unwrap())
        .collect()
}

#[tokio::test]
async fn the_admin_api_reports_requests_as_they_happen() {
    let openai = FakeOpenAi::start().await;
    let gw = Gateway::start(&format!(
        "providers:
  openai: {{ vendor: openai, base_url: '{}', key: sk }}
",
        openai.base_url()
    ))
    .await;
    let events = client().get(gw.url("/ui/api/events")).send().await.unwrap();
    assert_eq!(events.status(), 200);
    assert!(
        events.headers()["content-type"]
            .to_str()
            .unwrap()
            .starts_with("text/event-stream")
    );
    let mut events = events.bytes_stream();

    openai.answer(Answer::text("hello there"));
    let body = json!({"model": "gpt-4.1-mini", "stream": true,
        "messages": [{"role": "user", "content": "hi"}],
        "stream_options": {"include_usage": true}});
    let resp = client()
        .post(gw.url(CHAT))
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    resp.text().await.unwrap();

    let mut heard = String::new();
    let finished = |heard: &str| request_events(heard).iter().any(|e| !e["status"].is_null());
    while !finished(&heard) {
        let next = tokio::time::timeout(BOUND, events.next()).await.unwrap();
        heard.push_str(&String::from_utf8_lossy(&next.unwrap().unwrap()));
    }
    let heard = request_events(&heard);
    let (first, last) = (&heard[0], &heard[heard.len() - 1]);
    assert_eq!(first["id"], "r_000001");
    assert!(first["status"].is_null(), "{first}");
    assert_eq!(last["id"], "r_000001");
    assert_eq!(last["status"], 200);
    assert_eq!(last["method"], "POST");
    assert_eq!(last["path"], CHAT);
    assert_eq!(last["format"], "openai-chat");
    assert_eq!(last["kind"], "chat");
    assert_eq!(last["model"], "gpt-4.1-mini");
    assert_eq!(last["provider"], "openai");
    assert_eq!(last["stream"], true);
    assert!(
        last["duration_ms"].is_u64() && last["ttfb_ms"].is_u64(),
        "{last}"
    );
    assert!(
        last["usage"]["output_tokens"].as_u64().unwrap() > 0,
        "{last}"
    );
    assert!(last["error"].is_null(), "{last}");

    let requests = get_json(&gw, "/ui/api/requests?limit=10").await;
    assert_eq!(requests["requests"][0], *last);
    assert_eq!(get_json(&gw, "/ui/api/requests/r_000001").await, *last);
    let gone = client()
        .get(gw.url("/ui/api/requests/r_999999"))
        .send()
        .await
        .unwrap();
    assert_eq!(gone.status(), 404);

    let overview = get_json(&gw, "/ui/api/overview").await;
    assert_eq!(overview["totals"]["requests"], 1);
    assert_eq!(overview["totals"]["errors"], 0);
    assert_eq!(overview["totals"]["in_flight"], 0);
    assert_eq!(overview["auth_required"], false);
    assert_eq!(overview["listen"], gw.base.trim_start_matches("http://"));
    assert!(
        overview["config_source"]
            .as_str()
            .unwrap()
            .ends_with(".yaml")
    );

    let providers = get_json(&gw, "/ui/api/providers").await;
    let openai_info = &providers["providers"][0];
    assert_eq!(openai_info["name"], "openai");
    assert_eq!(openai_info["vendor"], "openai");
    assert_eq!(
        openai_info["kinds"],
        json!([
            "chat",
            "realtime",
            "image",
            "transcription",
            "speech",
            "video"
        ])
    );
    assert_eq!(openai_info["credentials"], "gateway");
    assert_eq!(openai_info["requests"], 1);
    assert!(openai_info["avg_latency_ms"].is_u64());

    let routes = get_json(&gw, "/ui/api/routes").await;
    let serve = &routes["serve"][0];
    assert_eq!(serve["format"], "openai-chat");
    assert_eq!(serve["endpoints"], json!(["POST /v1/chat/completions"]));
    assert!(
        routes["routes"]
            .as_array()
            .unwrap()
            .contains(&json!({"prefix": "gpt", "provider": "openai"}))
    );
    assert_eq!(
        routes["defaults"][0],
        json!({"kind": "chat", "provider": "openai"})
    );
}
