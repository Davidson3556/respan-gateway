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

/// A fresh, empty directory: no `gateway.toml` in it.
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
        banner.contains("openai     openai-chat, openai-responses, openai-realtime, responses-ws"),
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
    assert!(said.contains("respan-gateway --help"), "{said}");
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
    let written = std::fs::read_to_string(dir.join("gateway.toml")).unwrap();
    assert!(written.contains("[providers.anthropic]"), "{written}");
    assert!(!written.contains("[providers.openai]"), "{written}");

    let out = run(&["check"]);
    assert!(out.status.success(), "{out:?}");
    let report = String::from_utf8_lossy(&out.stdout);
    assert!(report.contains("config     gateway.toml"), "{report}");
    assert!(report.ends_with("gateway.toml: ok\n"), "{report}");

    let out = run(&["init"]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("--force"));
    let out = run(&["init", "--force", "-o", "other.toml"]);
    assert!(out.status.success(), "{out:?}");
    assert!(dir.join("other.toml").is_file());
}

#[tokio::test]
async fn client_keys_are_required_and_never_forwarded() {
    let openai = FakeOpenAi::start().await;
    let gw = Gateway::start(&format!(
        "client_keys = [\"gw-key\"]\nproviders = {{ own = {{ formats = \"openai-chat\", base_url = \"{}\", key = \"sk-gw-openai\" }}, theirs = {{ formats = \"openai-chat\", base_url = \"{}\" }} }}\n",
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

    // The key in more than one header: none of them goes on, to a
    // provider without a key of its own either.
    openai.answer(Answer::text("hi"));
    let headers = [
        ("authorization", "Bearer sk-client"),
        ("x-api-key", "gw-key"),
        ("x-goog-api-key", "gw-key"),
    ];
    let resp = post_chat(&gw, "theirs/gpt-4.1-mini", &headers).await;
    assert_eq!(resp.status(), 200);
    let got = openai.requests().pop().unwrap();
    assert_eq!(got.header("authorization"), Some("Bearer sk-client"));
    assert_eq!(got.header("x-api-key"), None);
    assert_eq!(got.header("x-goog-api-key"), None);

    let overview = client()
        .get(gw.url("/ui/api/overview"))
        .bearer_auth("gw-key");
    let overview = support::json(&overview.send().await.unwrap().text().await.unwrap());
    assert_eq!(overview["auth_required"], true);
}

#[tokio::test]
async fn health_and_the_web_ui_are_served() {
    let gw = Gateway::start(
        "providers = { p = { formats = \"openai-chat\", base_url = \"http://127.0.0.1:9/v1\" } }\n",
    )
    .await;
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
    // A path of the app's own is its index too, but a missing file is not.
    assert_eq!(page("/ui/requests/r_000001").await, index);
    let missing = client()
        .get(gw.url("/ui/assets/missing.js"))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 404);
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
        "providers = {{ anthropic = {{ vendor = \"anthropic\", base_url = \"{}\", key = \"sk-ant\" }}, openai = {{ vendor = \"openai\", base_url = \"{openai}\", key = \"sk-openai\" }}, down = {{ formats = \"openai-chat\", base_url = \"{}/v1\" }} }}\n",
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

    // Not kept while a provider fails to list: asked again.
    get_json(&gw, "/v1/models").await;
    assert_eq!(seen.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn models_are_listed_in_anthropics_shape_for_a_request_with_its_version() {
    let (openai, _) = listing(&["gpt-4o", "text-embedding-3-small"]).await;
    let gw = Gateway::start(&format!(
        "providers = {{ openai = {{ vendor = \"openai\", base_url = \"{openai}\", key = \"sk-openai\" }} }}\n"
    ))
    .await;
    let list = client()
        .get(gw.url("/v1/models"))
        .header("anthropic-version", "2023-06-01")
        .header("x-api-key", "sk-ant")
        .send()
        .await
        .unwrap();
    assert_eq!(list.status(), 200);
    let list: Value = support::json(&list.text().await.unwrap());
    assert_eq!(list["has_more"], false);
    assert_eq!(list["first_id"], "gpt-4o");
    assert_eq!(list["last_id"], "gpt-4o");
    assert_eq!(
        list["data"],
        json!([{
            "type": "model",
            "id": "gpt-4o",
            "display_name": "gpt-4o",
            "created_at": "1970-01-01T00:00:01.000Z",
        }])
    );
    // Without the header, OpenAI's.
    let plain = get_json(&gw, "/v1/models").await;
    assert_eq!(plain["object"], "list");
    assert_eq!(plain["data"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn models_are_listed_in_geminis_shape_for_its_sdks() {
    let (openai, seen) = listing(&["gpt-4o", "text-embedding-3-small"]).await;
    let gw = Gateway::start(&format!(
        "providers = {{ openai = {{ vendor = \"openai\", base_url = \"{openai}\", key = \"sk-openai\" }} }}\nclient_keys = [\"gw-key\"]\n"
    ))
    .await;
    let get = |path: &'static str| {
        let request = client().get(gw.url(path));
        async move {
            request
                .header("x-goog-api-key", "gw-key")
                .send()
                .await
                .unwrap()
        }
    };
    let list = support::json(&get("/v1beta/models").await.text().await.unwrap());
    let models = list["models"].as_array().unwrap();
    // Chat models only: the ones generateContent serves.
    assert_eq!(models.len(), 1, "{list}");
    assert_eq!(models[0]["name"], "models/gpt-4o");
    assert_eq!(models[0]["displayName"], "gpt-4o");
    assert!(
        models[0]["supportedGenerationMethods"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m == "streamGenerateContent")
    );
    let one = get("/v1beta/models/gpt-4o").await;
    assert_eq!(one.status(), 200);
    let one = support::json(&one.text().await.unwrap());
    assert_eq!(one["name"], "models/gpt-4o");
    let prefixed = get("/v1beta/models/models/gpt-4o").await;
    assert_eq!(prefixed.status(), 200);
    let missing = get("/v1beta/models/nope").await;
    assert_eq!(missing.status(), 404);
    let error = support::json(&missing.text().await.unwrap());
    assert_eq!(error["error"]["status"], "NOT_FOUND", "{error}");
    // Without the gateway's key it is refused, like /v1/models.
    let anon = client().get(gw.url("/v1beta/models")).send().await.unwrap();
    assert_eq!(anon.status(), 401);
    assert_eq!(seen.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn the_merged_models_win_over_a_served_passthrough() {
    let (openai, seen) = listing(&["gpt-4o"]).await;
    let origin = openai.trim_end_matches("/v1");
    let gw = Gateway::start(&format!(
        "providers = {{ openai = {{ vendor = \"openai\", base_url = \"{openai}\", key = \"sk-openai\" }}, any = {{ formats = \"passthrough\", base_url = \"{origin}\" }} }}\nformats = [\"openai-chat\", \"passthrough\"]\n"
    ))
    .await;
    let list = get_json(&gw, "/v1/models").await;
    assert_eq!(list["data"][0]["id"], "gpt-4o", "{list}");
    assert_eq!(list["data"][0]["kind"], "chat", "{list}");
    assert_eq!(*seen.lock().unwrap(), ["Bearer sk-openai"]);
}

#[tokio::test]
async fn models_are_kept_a_while_once_every_provider_listed() {
    let (openai, seen) = listing(&["gpt-4o"]).await;
    let gw = Gateway::start(&format!(
        "providers = {{ openai = {{ vendor = \"openai\", base_url = \"{openai}\", key = \"sk\" }} }}\n"
    ))
    .await;
    get_json(&gw, "/v1/models").await;
    get_json(&gw, "/v1/models").await;
    assert_eq!(seen.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn models_from_no_provider_is_an_error_naming_each() {
    let gw = Gateway::start(&format!(
        "providers = {{ down = {{ formats = \"openai-chat\", base_url = \"{}/v1\" }} }}\n",
        closed_port().await
    ))
    .await;
    let resp = client().get(gw.url("/v1/models")).send().await.unwrap();
    assert_eq!(resp.status(), 502);
    let error = support::json(&resp.text().await.unwrap());
    let message = error["error"]["message"].as_str().unwrap();
    assert!(
        message.starts_with("no provider listed its models: down: could not be reached"),
        "{message}"
    );
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
        "providers = {{ openai = {{ vendor = \"openai\", base_url = \"{}\", key = \"sk\" }} }}\n",
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
    assert_eq!(overview["config_source"], "file");
    assert!(overview["config_path"].as_str().unwrap().ends_with(".toml"));

    let providers = get_json(&gw, "/ui/api/providers").await;
    let openai_info = &providers["providers"][0];
    assert_eq!(openai_info["name"], "openai");
    assert_eq!(openai_info["vendor"], "openai");
    assert_eq!(
        openai_info["kinds"],
        json!([
            "chat",
            "realtime",
            "responses",
            "image",
            "transcription",
            "speech",
            "video",
            "embedding"
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

#[test]
fn an_empty_client_key_from_the_environment_is_refused() {
    let path = support::write_config(
        "client_keys = [\"env:GK\"]\nproviders = { p = { formats = \"openai-chat\", base_url = \"http://x.test/v1\" } }\n",
    );
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_respan-gateway"))
        .args(["check", "--config", path.to_str().unwrap()])
        .env("GK", "")
        .output()
        .unwrap();
    assert!(!out.status.success());
    let said = String::from_utf8_lossy(&out.stderr);
    assert!(
        said.contains("client_keys: environment variable GK is empty"),
        "{said}"
    );
}

#[test]
fn init_keeps_the_base_url_the_environment_gives_ollama() {
    let dir = empty_dir();
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_respan-gateway"))
        .arg("init")
        .current_dir(&dir)
        .env_clear()
        .env("OLLAMA_HOST", "127.0.0.1:11999")
        .output()
        .unwrap();
    assert!(out.status.success(), "{out:?}");
    let written = std::fs::read_to_string(dir.join("gateway.toml")).unwrap();
    assert!(
        written.contains("base_url = \"http://127.0.0.1:11999/v1\""),
        "{written}"
    );
}

#[tokio::test]
async fn listen_takes_a_bare_port_or_colon_port() {
    let env = [("OPENAI_API_KEY", "sk")];
    for listen in ["0", ":0"] {
        let gw = Gateway::run_with(&["--listen", listen], &empty_dir(), &env).await;
        assert!(
            gw.base.starts_with("http://127.0.0.1:"),
            "{listen}: {}",
            gw.base
        );
    }
}

#[tokio::test]
async fn without_keys_a_cross_origin_page_cannot_spend_or_read() {
    let openai = FakeOpenAi::start().await;
    let gw = Gateway::start(&format!(
        "providers = {{ openai = {{ vendor = \"openai\", base_url = \"{}\", key = \"sk\" }} }}\n",
        openai.base_url()
    ))
    .await;
    // A "simple" cross-site request, which a browser sends unasked.
    let resp = client()
        .post(gw.url(CHAT))
        .header("origin", "https://evil.example")
        .header("content-type", "text/plain")
        .body(chat("gpt-4.1-mini"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
    let error = support::json(&resp.text().await.unwrap());
    assert_eq!(error["error"]["type"], "permission_error", "{error}");
    assert!(openai.requests().is_empty());

    // A DNS name rebound to 127.0.0.1: same-origin to the browser.
    let rebound = client()
        .get(gw.url("/ui/api/overview"))
        .header("host", "evil.example")
        .send()
        .await
        .unwrap();
    assert_eq!(rebound.status(), 403);

    // The UI, same-origin, and curl, with no Origin, are served.
    openai.answer(Answer::text("hi"));
    let same = client()
        .post(gw.url(CHAT))
        .header("origin", &gw.base)
        .body(chat("gpt-4.1-mini"))
        .send()
        .await
        .unwrap();
    assert_eq!(same.status(), 200);
    get_json(&gw, "/ui/api/overview").await;
    let local = client()
        .get(gw.url("/healthz"))
        .header("host", "localhost")
        .send()
        .await
        .unwrap();
    assert_eq!(local.status(), 200);
}

#[tokio::test]
async fn an_unreachable_provider_is_named_with_its_host_and_cause() {
    let down = closed_port().await;
    let gw = Gateway::start(&format!(
        "providers = {{ down = {{ formats = \"openai-chat\", base_url = \"{down}/v1\", key = \"sk-secret\" }} }}\n"
    ))
    .await;
    let resp = post_chat(&gw, "gpt-4.1-mini", &[]).await;
    assert_eq!(resp.status(), 502);
    let error = support::json(&resp.text().await.unwrap());
    let message = error["error"]["message"].as_str().unwrap().to_owned();
    let host = down.trim_start_matches("http://");
    let expected = format!("provider 'down' at {host} could not be reached: ");
    assert!(message.starts_with(&expected), "{message}");
    assert!(message.to_lowercase().contains("refused"), "{message}");
    assert!(!message.contains("sk-secret"), "{message}");
    let logged = get_json(&gw, "/ui/api/requests").await;
    assert_eq!(logged["requests"][0]["error"], message.as_str());
}

#[cfg(unix)]
#[tokio::test]
async fn an_open_event_stream_does_not_hold_up_shutdown() {
    let gw = Gateway::start(
        "providers = { p = { formats = \"openai-chat\", base_url = \"http://127.0.0.1:9/v1\" } }\n",
    )
    .await;
    let events = client().get(gw.url("/ui/api/events")).send().await.unwrap();
    assert_eq!(events.status(), 200);
    let mut gw = gw;
    let pid = gw.child.id().unwrap().to_string();
    let sent = std::process::Command::new("kill")
        .args(["-TERM", &pid])
        .status()
        .unwrap();
    assert!(sent.success());
    let exited = tokio::time::timeout(BOUND, gw.child.wait()).await;
    assert!(exited.is_ok(), "still running {BOUND:?} after SIGTERM");
    drop(events);
}
