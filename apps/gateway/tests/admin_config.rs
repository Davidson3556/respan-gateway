//! The admin API's config endpoints: the redacted view, validation, and
//! writes behind `admin_writes`.

#![allow(clippy::unwrap_used)]

mod support;

use serde_json::Value;
use support::{Gateway, client, json, write_config};

const KEY: &str = "gw-key-1";

const BODY: &str = "
client_keys: [gw-key-1, env:SOME_OTHER_KEY_VAR]
providers:
  alpha: { formats: openai-chat, base_url: 'http://127.0.0.1:9', key: sk-prov-secret-1 }
  beta:
    formats: openai-chat
    base_url: 'http://127.0.0.1:9'
    auth: { type: header, name: x-api, key: sk-prov-secret-2 }
  gamma: { formats: openai-chat, base_url: 'http://127.0.0.1:9', key: 'env:GAMMA_KEY_REF' }
mcp:
  tools:
    command: [sh, --token, mcp-arg-secret-3]
    env: { TOKEN: mcp-env-secret-4 }
  remote: { url: 'https://example.invalid/mcp/', key: mcp-key-secret-5 }
";

const SECRETS: [&str; 5] = [
    "sk-prov-secret-1",
    "sk-prov-secret-2",
    "mcp-arg-secret-3",
    "mcp-env-secret-4",
    "mcp-key-secret-5",
];

/// The binary on `config` with the variables it names set, and `PATH` to
/// find the MCP program.
async fn start(config: &str) -> Gateway {
    let path = write_config(&format!("listen: 127.0.0.1:0\n{config}"));
    let search = std::env::var("PATH").unwrap_or_default();
    let env = [
        ("PATH", search.as_str()),
        ("GAMMA_KEY_REF", "sk-gamma"),
        ("SOME_OTHER_KEY_VAR", "gw-key-2"),
    ];
    let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"));
    Gateway::run_with(&["--config", path.to_str().unwrap()], dir, &env).await
}

fn with_writes(body: &str) -> String {
    format!("admin_writes: true\n{body}")
}

async fn call(gw: &Gateway, method: &str, path: &str, body: Option<&str>) -> (u16, Value) {
    let method = reqwest::Method::from_bytes(method.as_bytes()).unwrap();
    let mut request = client()
        .request(method, gw.url(path))
        .bearer_auth(KEY)
        .header("content-type", "text/yaml");
    if let Some(body) = body {
        request = request.body(body.to_owned());
    }
    let resp = request.send().await.unwrap();
    let status = resp.status().as_u16();
    (status, json(&resp.text().await.unwrap()))
}

async fn providers(gw: &Gateway) -> Vec<String> {
    let (_, said) = call(gw, "GET", "/ui/api/providers", None).await;
    let list = said["providers"].as_array().unwrap();
    list.iter()
        .map(|p| p["name"].as_str().unwrap().to_owned())
        .collect()
}

#[tokio::test]
async fn every_config_endpoint_needs_a_client_key() {
    let gw = start(&with_writes(BODY)).await;
    for (method, path) in [
        ("GET", "/ui/api/config"),
        ("POST", "/ui/api/config/validate"),
        ("PUT", "/ui/api/config"),
    ] {
        let method = reqwest::Method::from_bytes(method.as_bytes()).unwrap();
        let resp = client()
            .request(method, gw.url(path))
            .body("providers: {}")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 401, "{path}");
    }
}

#[tokio::test]
async fn the_view_redacts_every_secret_and_says_whether_writes_are_on() {
    let gw = start(&with_writes(BODY)).await;
    let (status, said) = call(&gw, "GET", "/ui/api/config", None).await;
    assert_eq!(status, 200, "{said}");
    let text = said.to_string();
    for secret in SECRETS.iter().chain(&["gw-key-1"]) {
        assert!(!text.contains(secret), "{secret} leaked: {text}");
    }
    assert!(!said["yaml"].as_str().unwrap().contains("secret"));
    let yaml = said["yaml"].as_str().unwrap();
    assert!(yaml.contains("env:SOME_OTHER_KEY_VAR"), "{yaml}");
    assert!(yaml.contains("env:GAMMA_KEY_REF"), "{yaml}");
    assert!(yaml.contains("https://example.invalid/mcp/"), "{yaml}");
    assert_eq!(said["config"]["providers"]["alpha"]["key"], "…");
    assert_eq!(said["config"]["client_keys"][0], "…");
    assert_eq!(said["source"], "file");
    assert!(said["path"].as_str().unwrap().ends_with(".yaml"));
    assert_eq!(said["writes_enabled"], true);
    assert_eq!(said["writes_disabled_reason"], Value::Null);

    // Off unless the config says so.
    let gw = start(BODY).await;
    let (_, said) = call(&gw, "GET", "/ui/api/config", None).await;
    assert_eq!(said["writes_enabled"], false);
    assert!(
        said["writes_disabled_reason"]
            .as_str()
            .unwrap()
            .contains("admin_writes")
    );
}

#[tokio::test]
async fn validation_composes_like_a_reload_and_says_where_and_what_without_secrets() {
    let gw = start(&with_writes(BODY)).await;
    let (status, said) = call(&gw, "POST", "/ui/api/config/validate", Some(BODY)).await;
    assert_eq!((status, &said["ok"]), (200, &Value::Bool(true)), "{said}");

    // Syntax: the place.
    let (_, said) = call(
        &gw,
        "POST",
        "/ui/api/config/validate",
        Some("a: 1\nb: [x\n"),
    )
    .await;
    assert_eq!(said["ok"], false);
    assert!(said["line"].as_u64().is_some(), "{said}");

    // A field that is not one, and a secret in a wrong place not echoed.
    let (_, said) = call(&gw, "POST", "/ui/api/config/validate", Some("nope: 1")).await;
    assert_eq!(said["ok"], false);
    assert!(said["error"].as_str().unwrap().contains("nope"), "{said}");
    let (_, said) = call(
        &gw,
        "POST",
        "/ui/api/config/validate",
        Some("client_keys: sk-secret-9"),
    )
    .await;
    assert_eq!(said["ok"], false);
    assert!(!said.to_string().contains("sk-secret-9"), "{said}");

    // Composition: a route to a provider that does not exist.
    let (_, said) = call(
        &gw,
        "POST",
        "/ui/api/config/validate",
        Some("default: missing\nproviders: { a: { formats: openai-chat, base_url: 'http://x.test', key: k } }"),
    )
    .await;
    assert_eq!(said["ok"], false, "{said}");
    // And nothing changed.
    assert_eq!(providers(&gw).await, ["alpha", "beta", "gamma"]);
}

#[tokio::test]
async fn writes_are_refused_unless_enabled_with_a_file_and_client_keys() {
    let gw = start(BODY).await;
    let path = call(&gw, "GET", "/ui/api/config", None).await.1["path"]
        .as_str()
        .unwrap()
        .to_owned();
    let before = std::fs::read_to_string(&path).unwrap();
    let (status, said) = call(&gw, "PUT", "/ui/api/config", Some("providers: {}")).await;
    assert_eq!(status, 403, "{said}");
    assert_eq!(said["error"]["type"], "admin_writes_disabled");
    assert_eq!(std::fs::read_to_string(&path).unwrap(), before);

    // admin_writes without client keys is refused too (a local page could
    // otherwise rewrite the file).
    let open = start("admin_writes: true\nproviders: { a: { formats: openai-chat, base_url: 'http://x.test', key: k } }").await;
    let resp = client()
        .put(open.url("/ui/api/config"))
        .body("providers: {}")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
    let said = json(&resp.text().await.unwrap());
    assert!(
        said["error"]["message"]
            .as_str()
            .unwrap()
            .contains("client_keys")
    );
}

#[tokio::test]
async fn a_saved_view_keeps_the_secrets_of_the_file_and_reloads() {
    let gw = start(&with_writes(BODY)).await;
    let (_, view) = call(&gw, "GET", "/ui/api/config", None).await;
    let path = view["path"].as_str().unwrap().to_owned();
    let before = std::fs::read_to_string(&path).unwrap();

    // Edit the redacted view: a provider added, one secret given anew.
    let mut config: Value = view["config"].clone();
    config["providers"]["delta"] = serde_json::json!({
        "formats": "openai-chat", "base_url": "http://127.0.0.1:9", "key": "sk-new-secret-6"
    });
    let body = serde_json::to_string(&config).unwrap();

    let (status, said) = call(&gw, "PUT", "/ui/api/config", Some(&body)).await;
    assert_eq!(status, 200, "{said}");
    assert_eq!(said["saved"], true);
    assert!(said["reloaded_at"].as_str().unwrap().ends_with('Z'));
    assert_eq!(said["restart_required"], serde_json::json!([]));

    // The file holds the real secrets and the new one; the reload took it.
    let after = std::fs::read_to_string(&path).unwrap();
    for secret in SECRETS.iter().chain(&["gw-key-1", "sk-new-secret-6"]) {
        assert!(after.contains(secret), "{secret} missing from {after}");
    }
    assert!(after.contains("env:GAMMA_KEY_REF"));
    assert!(!after.contains('…'), "{after}");
    assert_eq!(providers(&gw).await, ["alpha", "beta", "gamma", "delta"]);
    let (_, overview) = call(&gw, "GET", "/ui/api/overview", None).await;
    assert_eq!(overview["reloaded_at"], said["reloaded_at"]);

    // The old file is kept beside it, and no temporary file is left.
    let backup = said["backup"].as_str().unwrap();
    assert_eq!(std::fs::read_to_string(backup).unwrap(), before);
    let dir = std::path::Path::new(&path).parent().unwrap();
    let file = std::path::Path::new(&path).file_name().unwrap();
    let stray = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(Result::ok)
        .any(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            name.starts_with(&format!(".{}", file.to_string_lossy())) && name.contains(".tmp-")
        });
    assert!(!stray);

    // A real new value for a secret replaces it.
    let mut config = call(&gw, "GET", "/ui/api/config", None).await.1["config"].clone();
    config["providers"]["alpha"]["key"] = "env:ALPHA_NOW".into();
    let (status, _) = call(
        &gw,
        "PUT",
        "/ui/api/config",
        Some(&serde_json::to_string(&config).unwrap()),
    )
    .await;
    // That variable is not set, so the config would not reload: refused.
    assert_eq!(status, 422);
    assert!(
        std::fs::read_to_string(&path)
            .unwrap()
            .contains("sk-prov-secret-1")
    );
}

#[tokio::test]
async fn an_invalid_save_changes_nothing_and_a_placeholder_with_nothing_to_keep_is_refused() {
    let gw = start(&with_writes(BODY)).await;
    let path = call(&gw, "GET", "/ui/api/config", None).await.1["path"]
        .as_str()
        .unwrap()
        .to_owned();
    let before = std::fs::read_to_string(&path).unwrap();

    let (status, said) = call(&gw, "PUT", "/ui/api/config", Some("bogus: [")).await;
    assert_eq!(status, 422, "{said}");
    assert_eq!(said["error"]["type"], "invalid_config");
    assert!(said["error"]["line"].as_u64().is_some(), "{said}");

    let fresh = "admin_writes: true\nclient_keys: ['…']\nproviders: { n: { formats: openai-chat, base_url: 'http://x.test', key: '…' } }";
    let (status, said) = call(&gw, "PUT", "/ui/api/config", Some(fresh)).await;
    assert_eq!(status, 422, "{said}");
    assert!(
        said["error"]["message"]
            .as_str()
            .unwrap()
            .contains("providers.n.key"),
        "{said}"
    );
    assert_eq!(std::fs::read_to_string(&path).unwrap(), before);
    assert!(!std::path::Path::new(&format!("{path}.bak")).exists());
    assert_eq!(providers(&gw).await, ["alpha", "beta", "gamma"]);
}
