//! The gateway as a library: a host builds an [`App`] from a [`Config`]
//! and hands it requests in process, with no server.

#![allow(clippy::unwrap_used)]

use axum::body::Body;
use axum::http::Request;
use mockllm::{Answer, FakeOpenAi};
use respan_gateway::{App, Config};
use tower::ServiceExt;

#[tokio::test]
async fn a_host_hands_requests_to_the_app_in_process() {
    let openai = FakeOpenAi::start().await;
    openai.answer(Answer::text("hi there"));
    let text = format!(
        "log = \"off\"\nproviders = {{ openai = {{ vendor = \"openai\", base_url = \"{}\", key = \"sk\" }} }}\n",
        openai.base_url()
    );
    let app = App::new(quiet(Config::parse(&text).unwrap())).unwrap();

    let health = Request::get("/healthz").body(Body::empty()).unwrap();
    let health = app.router().oneshot(health).await.unwrap();
    assert_eq!(health.status(), 200);

    let chat = r#"{"model":"gpt-4.1-mini","messages":[{"role":"user","content":"hi"}]}"#;
    let chat = Request::post("/v1/chat/completions")
        .body(Body::from(chat))
        .unwrap();
    let resp = app.router().oneshot(chat).await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["choices"][0]["message"]["content"], "hi there");
    app.gateway().idle().await;
    assert_eq!(openai.requests().len(), 1);
}

#[test]
fn an_app_is_built_outside_a_runtime() {
    let config = "providers = { openai = { vendor = \"openai\", key = \"sk\" }, gemini = { vendor = \"gemini\", key = \"k\" } }\n";
    let app = App::new(Config::parse(config).unwrap()).unwrap();
    assert_eq!(app.listen(), respan_gateway::DEFAULT_LISTEN);
    let banner = app.banner(app.listen());
    assert!(banner.starts_with("respan-gateway "), "{banner}");
}

#[test]
fn a_bad_config_says_what_is_wrong() {
    let config = Config::parse("providers = { p = { vendor = \"nope\" } }\n").unwrap();
    let error = App::new(config).unwrap_err();
    assert!(
        error
            .to_string()
            .starts_with("provider 'p': unknown vendor 'nope'"),
        "{error}"
    );
    assert!(Config::parse("listen = [").is_err());
}

/// `config`, reading no provider's model list: a provider sees only the
/// calls a test makes.
fn quiet(mut config: Config) -> Config {
    config.plugins.routing.listings = Some(false);
    config
}
