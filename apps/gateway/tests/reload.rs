//! Reloading the config of a running gateway: on SIGHUP, through the admin
//! API and when the file changes.

#![allow(clippy::unwrap_used)]

mod support;

use std::path::Path;
use std::time::Duration;

use futures::StreamExt;
use mockllm::{Answer, FakeOpenAi};
use serde_json::{Value, json};
use support::{Gateway, client, write_config};

const CHAT: &str = "/v1/chat/completions";

/// The gateway on `path`, which says to listen on a free port.
async fn serve(path: &Path, extra: &[&str]) -> Gateway {
    let mut args = vec!["--config", path.to_str().unwrap()];
    args.extend_from_slice(extra);
    Gateway::run(&args).await
}

fn config(listen: &str, providers: &[(&str, &str)], extra: &str) -> String {
    let providers: String = providers
        .iter()
        .map(|(name, url)| {
            format!("  {name}: {{ formats: openai-chat, base_url: '{url}', key: k }}\n")
        })
        .collect();
    format!("listen: {listen}\n{extra}\nproviders:\n{providers}")
}

async fn get(gw: &Gateway, path: &str) -> Value {
    let resp = client().get(gw.url(path)).send().await.unwrap();
    assert_eq!(resp.status(), 200, "{path}");
    support::json(&resp.text().await.unwrap())
}

async fn names(gw: &Gateway) -> Vec<String> {
    let providers = get(gw, "/ui/api/providers").await;
    let names = providers["providers"].as_array().unwrap().iter();
    names
        .map(|p| p["name"].as_str().unwrap().to_owned())
        .collect()
}

async fn chat(gw: &Gateway, model: &str) -> reqwest::Response {
    let body = json!({"model": model, "messages": [{"role": "user", "content": "hi"}]});
    let request = client().post(gw.url(CHAT)).body(body.to_string());
    request.send().await.unwrap()
}

#[tokio::test]
async fn the_admin_api_reloads_the_file_and_keeps_the_old_config_when_the_new_is_invalid() {
    let (a, b) = (FakeOpenAi::start().await, FakeOpenAi::start().await);
    let path = write_config(&config("127.0.0.1:0", &[("alpha", &a.base_url())], ""));
    let gw = serve(&path, &[]).await;
    assert_eq!(names(&gw).await, ["alpha"]);
    assert_eq!(chat(&gw, "alpha/m").await.status(), 200);
    assert_eq!(
        get(&gw, "/ui/api/overview").await["reloaded_at"],
        Value::Null
    );

    // A provider added: new requests reach it, the log carries over.
    let both = config(
        "127.0.0.1:0",
        &[("alpha", &a.base_url()), ("beta", &b.base_url())],
        "",
    );
    std::fs::write(&path, both).unwrap();
    let resp = client()
        .post(gw.url("/ui/api/reload"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let said: Value = support::json(&resp.text().await.unwrap());
    assert_eq!(said["restart_required"], json!([]));
    assert!(said["reloaded_at"].as_str().unwrap().ends_with('Z'));
    assert_eq!(names(&gw).await, ["alpha", "beta"]);
    assert_eq!(chat(&gw, "beta/m").await.status(), 200);
    assert_eq!(b.requests().len(), 1);
    let overview = get(&gw, "/ui/api/overview").await;
    assert_eq!(overview["reloaded_at"], said["reloaded_at"]);
    assert_eq!(overview["totals"]["requests"], 2, "{overview}");

    // An invalid file: refused with the reason, the previous config serves.
    std::fs::write(&path, "providers: [nonsense").unwrap();
    let resp = client()
        .post(gw.url("/ui/api/reload"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 422);
    let error: Value = support::json(&resp.text().await.unwrap());
    assert_eq!(error["error"]["type"], "invalid_config");
    assert!(
        error["error"]["message"]
            .as_str()
            .unwrap()
            .contains(".yaml")
    );
    assert_eq!(names(&gw).await, ["alpha", "beta"]);
    assert_eq!(chat(&gw, "alpha/m").await.status(), 200);

    // A value of the bad file is not echoed: it may be a secret.
    std::fs::write(&path, "client_keys: sk-secret-77\n").unwrap();
    let resp = client()
        .post(gw.url("/ui/api/reload"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 422);
    let body = resp.text().await.unwrap();
    assert!(body.contains("invalid type"), "{body}");
    assert!(!body.contains("sk-secret-77"), "{body}");

    // A moved listen address is said to need a restart.
    let moved = config("127.0.0.1:1", &[("alpha", &a.base_url())], "");
    std::fs::write(&path, moved).unwrap();
    let resp = client()
        .post(gw.url("/ui/api/reload"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let said: Value = support::json(&resp.text().await.unwrap());
    let notes = said["restart_required"].as_array().unwrap();
    assert!(notes[0].as_str().unwrap().contains("restart"), "{said}");
    assert_eq!(names(&gw).await, ["alpha"]);
}

#[cfg(unix)]
#[tokio::test]
async fn sighup_reloads_the_file() {
    let (a, b) = (FakeOpenAi::start().await, FakeOpenAi::start().await);
    let path = write_config(&config("127.0.0.1:0", &[("alpha", &a.base_url())], ""));
    let gw = serve(&path, &[]).await;
    let both = config(
        "127.0.0.1:0",
        &[("alpha", &a.base_url()), ("beta", &b.base_url())],
        "",
    );
    std::fs::write(&path, both).unwrap();
    let pid = gw.child.id().unwrap().to_string();
    let status = tokio::process::Command::new("kill")
        .args(["-HUP", &pid])
        .status();
    assert!(status.await.unwrap().success());
    let deadline = tokio::time::Instant::now() + support::BOUND;
    while names(&gw).await.len() != 2 {
        assert!(tokio::time::Instant::now() < deadline, "not reloaded");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn watch_reloads_when_the_file_changes() {
    let (a, b) = (FakeOpenAi::start().await, FakeOpenAi::start().await);
    let path = write_config(&config("127.0.0.1:0", &[("alpha", &a.base_url())], ""));
    let gw = serve(&path, &["--watch"]).await;
    // Past the file system's timestamp granularity.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let both = config(
        "127.0.0.1:0",
        &[("alpha", &a.base_url()), ("beta", &b.base_url())],
        "",
    );
    std::fs::write(&path, both).unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while names(&gw).await.len() != 2 {
        assert!(tokio::time::Instant::now() < deadline, "not reloaded");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn reloading_needs_a_client_key_when_there_are_keys_and_a_source_to_read() {
    let a = FakeOpenAi::start().await;
    let extra = "client_keys: [gw-key]";
    let path = write_config(&config("0.0.0.0:0", &[("alpha", &a.base_url())], extra));
    let gw = serve(&path, &[]).await;
    let resp = client()
        .post(gw.url("/ui/api/reload"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
    let resp = client().post(gw.url("/ui/api/reload"));
    let resp = resp.bearer_auth("gw-key").send().await.unwrap();
    assert_eq!(resp.status(), 200);

    // Dropping the keys on an address beyond loopback is refused.
    let open = config("0.0.0.0:0", &[("alpha", &a.base_url())], "");
    std::fs::write(&path, open).unwrap();
    let resp = client().post(gw.url("/ui/api/reload"));
    let resp = resp.bearer_auth("gw-key").send().await.unwrap();
    assert_eq!(resp.status(), 422);
}

#[tokio::test]
async fn the_request_log_keeps_the_configured_number_of_requests_across_reloads() {
    let a = FakeOpenAi::start().await;
    let path = write_config(&config(
        "127.0.0.1:0",
        &[("alpha", &a.base_url())],
        "plugins: { request_log: { history: 3 } }",
    ));
    let gw = serve(&path, &[]).await;
    for _ in 0..5 {
        assert_eq!(chat(&gw, "alpha/m").await.status(), 200);
    }
    let kept = get(&gw, "/ui/api/requests").await;
    assert_eq!(kept["requests"].as_array().unwrap().len(), 3);
    std::fs::write(
        &path,
        config(
            "127.0.0.1:0",
            &[("alpha", &a.base_url())],
            "plugins: { request_log: { history: 2 } }",
        ),
    )
    .unwrap();
    let resp = client()
        .post(gw.url("/ui/api/reload"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let kept = get(&gw, "/ui/api/requests").await;
    let kept = kept["requests"].as_array().unwrap();
    assert_eq!(kept.len(), 2);
    assert_eq!(get(&gw, "/ui/api/overview").await["totals"]["requests"], 5);
}

/// The text of an OpenAI stream, joined.
fn streamed_text(body: &str) -> String {
    let data = body.lines().filter_map(|l| l.strip_prefix("data: "));
    let events = data.filter_map(|d| serde_json::from_str::<Value>(d).ok());
    let deltas = events.filter_map(|e| {
        e["choices"][0]["delta"]["content"]
            .as_str()
            .map(String::from)
    });
    deltas.collect()
}

async fn chat_stream(gw: &Gateway, model: &str) -> reqwest::Response {
    let body = json!({"model": model, "stream": true,
                      "messages": [{"role": "user", "content": "hi"}]});
    let request = client().post(gw.url(CHAT)).body(body.to_string());
    request.send().await.unwrap()
}

#[tokio::test]
async fn a_request_in_flight_finishes_on_the_old_config_across_a_reload() {
    let (old, new) = (FakeOpenAi::start().await, FakeOpenAi::start().await);
    old.answer(Answer::text("old config").pace(300));
    new.answer(Answer::text("new config"));
    let path = write_config(&config("127.0.0.1:0", &[("alpha", &old.base_url())], ""));
    let gw = serve(&path, &[]).await;

    let mut slow = chat_stream(&gw, "alpha/m").await;
    assert_eq!(slow.status(), 200);
    // Open: its first chunk is here, the rest 300 ms apart.
    let first = slow.chunk().await.unwrap().unwrap();

    std::fs::write(
        &path,
        config("127.0.0.1:0", &[("alpha", &new.base_url())], ""),
    )
    .unwrap();
    let resp = client()
        .post(gw.url("/ui/api/reload"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // A request after the reload goes to the new provider, while the old
    // stream is still open.
    let fresh = chat_stream(&gw, "alpha/m").await;
    assert_eq!(fresh.status(), 200);
    assert_eq!(streamed_text(&fresh.text().await.unwrap()), "new config");
    assert_eq!(new.requests().len(), 1);

    let (rest, ended) = support::read_all(slow).await;
    assert_eq!(ended, Ok(()));
    let body = format!("{}{rest}", String::from_utf8_lossy(&first));
    assert_eq!(streamed_text(&body), "old config", "{body}");
    assert_eq!(old.requests().len(), 1);
}

/// An OpenAI-shaped server whose streams send two events and then stall for
/// a minute: the base URL.
#[cfg_attr(not(unix), allow(dead_code))]
async fn stalled() -> String {
    let event = |content: &str| {
        let delta = json!({"role": "assistant", "content": content});
        let chunk = json!({"id": "c", "object": "chat.completion.chunk", "created": 1,
            "model": "m", "choices": [{"index": 0, "delta": delta, "finish_reason": null}]});
        format!("data: {chunk}\n\n")
    };
    let (one, two) = (event("x"), event("y"));
    let app = axum::Router::new().route(
        "/v1/chat/completions",
        axum::routing::post(move || {
            let events = futures::stream::iter([one.clone(), two.clone()]);
            let body = events.map(Ok::<_, std::io::Error>);
            let body = body.chain(futures::stream::pending());
            async move {
                let head = [("content-type", "text/event-stream")];
                (head, axum::body::Body::from_stream(body))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}/v1", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await });
    base
}

/// Sends SIGTERM to the gateway.
#[cfg(unix)]
fn terminate(gw: &Gateway) {
    let pid = gw.child.id().unwrap().to_string();
    let killed = std::process::Command::new("kill")
        .args(["-TERM", &pid])
        .status();
    assert!(killed.unwrap().success());
}

#[cfg(unix)]
#[tokio::test]
async fn shutdown_grace_lets_a_fast_request_finish_and_cuts_a_slow_one() {
    let fast = FakeOpenAi::start().await;
    fast.answer(Answer::text("a b").pace(150));
    let slow = stalled().await;
    let config = config(
        "127.0.0.1:0",
        &[("fast", &fast.base_url()), ("slow", &slow)],
        "shutdown_grace: 2s",
    );
    let path = write_config(&config);
    let mut gw = serve(&path, &[]).await;

    let long = client()
        .post(gw.url(CHAT))
        .timeout(Duration::from_secs(30))
        .body(
            json!({"model": "slow/m", "stream": true,
                   "messages": [{"role": "user", "content": "hi"}]})
            .to_string(),
        )
        .send()
        .await
        .unwrap();
    let mut long = long;
    assert_eq!(long.status(), 200);
    let first = long.chunk().await.unwrap().unwrap();
    assert!(!first.is_empty());
    let mut quick = chat_stream(&gw, "fast/m").await;
    let quick_first = quick.chunk().await.unwrap().unwrap();

    let started = tokio::time::Instant::now();
    terminate(&gw);

    // The fast one completes: its remaining events are well inside the grace.
    let (rest, ended) = support::read_all(quick).await;
    assert_eq!(ended, Ok(()));
    let body = format!("{}{rest}", String::from_utf8_lossy(&quick_first));
    assert_eq!(streamed_text(&body), "a b", "{body}");
    assert!(body.contains("[DONE]"), "{body}");

    // The slow one is cut at the grace, and the process ends then.
    let status = tokio::time::timeout(Duration::from_secs(10), gw.child.wait())
        .await
        .expect("the gateway must exit after its grace")
        .unwrap();
    assert!(status.success(), "{status}");
    let took = started.elapsed();
    assert!(
        took >= Duration::from_millis(1800) && took < Duration::from_secs(8),
        "{took:?}"
    );
    let (rest, _) = support::read_all(long).await;
    assert!(!rest.contains("[DONE]"), "{rest}");
}
