//! The read-only admin config view.

#![allow(clippy::unwrap_used)]

mod support;

use serde_json::Value;
use support::{Gateway, client, json, write_config};

const KEY: &str = "gw-key-1";

const BODY: &str = "client_keys = [{ name = \"ops\", key = \"gw-key-1\", admin = true }, \"env:SOME_OTHER_KEY_VAR\"]\nproviders = { alpha = { formats = \"openai-chat\", base_url = \"http://127.0.0.1:9\", key = \"sk-prov-secret-1\" }, beta = { formats = \"openai-chat\", base_url = \"http://127.0.0.1:9\", auth = { type = \"header\", name = \"x-api\", key = \"sk-prov-secret-2\" } }, gamma = { formats = \"openai-chat\", base_url = \"http://127.0.0.1:9\", key = \"env:GAMMA_KEY_REF\" } }\nmcp = { tools = { command = [\"sh\", \"--token\", \"mcp-arg-secret-3\"], env = { TOKEN = \"mcp-env-secret-4\" } }, remote = { url = \"https://example.invalid/mcp/\", key = \"mcp-key-secret-5\" } }\n";

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
    let path = write_config(&format!("listen = \"127.0.0.1:0\"\n{config}"));
    let search = std::env::var("PATH").unwrap_or_default();
    let env = [
        ("PATH", search.as_str()),
        ("GAMMA_KEY_REF", "sk-gamma"),
        ("SOME_OTHER_KEY_VAR", "gw-key-2"),
    ];
    let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"));
    Gateway::run_with(&["--config", path.to_str().unwrap()], dir, &env).await
}

async fn call(gw: &Gateway, method: &str, path: &str, body: Option<&str>) -> (u16, Value) {
    let method = reqwest::Method::from_bytes(method.as_bytes()).unwrap();
    let mut request = client()
        .request(method, gw.url(path))
        .bearer_auth(KEY)
        .header("content-type", "application/toml");
    if let Some(body) = body {
        request = request.body(body.to_owned());
    }
    let resp = request.send().await.unwrap();
    let status = resp.status().as_u16();
    (status, json(&resp.text().await.unwrap()))
}

#[tokio::test]
async fn the_view_redacts_every_secret() {
    let gw = start(BODY).await;
    let (status, said) = call(&gw, "GET", "/ui/api/config", None).await;
    assert_eq!(status, 200, "{said}");
    let text = said.to_string();
    for secret in SECRETS.iter().chain(&["gw-key-1"]) {
        assert!(!text.contains(secret), "{secret} leaked: {text}");
    }
    assert!(!said["text"].as_str().unwrap().contains("secret"));
    let text = said["text"].as_str().unwrap();
    assert!(text.contains("env:SOME_OTHER_KEY_VAR"), "{text}");
    assert!(text.contains("env:GAMMA_KEY_REF"), "{text}");
    assert!(text.contains("https://example.invalid/mcp/"), "{text}");
    assert_eq!(said["config"]["providers"]["alpha"]["key"], "…");
    assert_eq!(said["config"]["client_keys"][0]["key"], "…");
    assert_eq!(said["config"]["client_keys"][0]["admin"], true);
    assert_eq!(said["source"], "file");
    assert!(said["path"].as_str().unwrap().ends_with(".toml"));
}

#[tokio::test]
async fn config_is_read_only_and_requires_an_admin_key() {
    let gw = start(BODY).await;
    let response = client().get(gw.url("/ui/api/config")).send().await.unwrap();
    assert_eq!(response.status(), 401);
    let (_, before) = call(&gw, "GET", "/ui/api/config", None).await;
    for (method, path) in [
        ("PUT", "/ui/api/config"),
        ("POST", "/ui/api/config/validate"),
    ] {
        let response = client()
            .request(
                reqwest::Method::from_bytes(method.as_bytes()).unwrap(),
                gw.url(path),
            )
            .bearer_auth(KEY)
            .body("providers = {}\n")
            .send()
            .await
            .unwrap();
        assert!(matches!(response.status().as_u16(), 404 | 405));
    }
    let (_, after) = call(&gw, "GET", "/ui/api/config", None).await;
    assert_eq!(before, after);
}
