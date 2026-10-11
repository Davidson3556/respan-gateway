//! The command line: `check`'s report, errors, `.env`, the version and
//! `models`, by running the shipped binary.

#![allow(clippy::unwrap_used)]

mod support;

use std::path::{Path, PathBuf};

use serde_json::json;
use support::{Gateway, client};

struct Output {
    ok: bool,
    stdout: String,
    stderr: String,
}

fn empty_dir() -> PathBuf {
    static N: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let dir =
        Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("cli-{}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Runs the binary in `dir` with `env` as its whole environment.
async fn run(dir: &Path, args: &[&str], env: &[(&str, &str)]) -> Output {
    let out = tokio::process::Command::new(env!("CARGO_BIN_EXE_respan-gateway"))
        .args(args)
        .current_dir(dir)
        .env_clear()
        .envs(env.iter().copied())
        .output()
        .await
        .unwrap();
    Output {
        ok: out.status.success(),
        stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
    }
}

#[tokio::test]
async fn check_reports_what_the_environment_configures_without_the_key() {
    let out = run(
        &empty_dir(),
        &["check"],
        &[("OPENAI_API_KEY", "sk-very-secret")],
    )
    .await;
    assert!(out.ok, "{}", out.stderr);
    let report = &out.stdout;
    assert!(report.contains("config     environment"), "{report}");
    assert!(
        report.contains("openai at https://api.openai.com/v1"),
        "{report}"
    );
    assert!(report.contains("key     env OPENAI_API_KEY"), "{report}");
    assert!(report.contains("POST /v1/chat/completions"), "{report}");
    assert!(report.contains("chat -> openai"), "{report}");
    assert!(report.contains("gpt-"), "{report}");
    assert!(
        report.contains("auth       off: no client keys"),
        "{report}"
    );
    assert!(report.ends_with("environment: ok\n"), "{report}");
    assert!(!report.contains("sk-very-secret") && !out.stderr.contains("sk-very-secret"));
}

#[tokio::test]
async fn check_says_where_each_key_comes_from() {
    let dir = empty_dir();
    std::fs::write(
        dir.join("gateway.yaml"),
        "client_keys: [one, two]
providers:
  literal: { formats: openai-chat, base_url: 'http://a.test/v1', key: sk-literal }
  named: { formats: openai-chat, base_url: 'http://b.test/v1', auth: { type: bearer, key: 'env:B_KEY' } }
  open: { formats: openai-chat, base_url: 'http://c.test/v1' }
",
    )
    .unwrap();
    let out = run(&dir, &["check"], &[("B_KEY", "b")]).await;
    assert!(out.ok, "{}", out.stderr);
    let report = &out.stdout;
    assert!(report.contains("config     gateway.yaml"), "{report}");
    assert!(report.contains("key     config"), "{report}");
    assert!(report.contains("key     env B_KEY"), "{report}");
    assert!(report.contains("key     client credentials"), "{report}");
    assert!(report.contains("on: 2 client keys"), "{report}");
    assert!(!report.contains("sk-literal"), "{report}");
}

#[tokio::test]
async fn check_composes_with_proxy_by_a_flag() {
    let out = run(&empty_dir(), &["proxy", "--anthropic", "--check"], &[]).await;
    assert!(out.ok, "{}", out.stderr);
    assert!(
        out.stdout.contains("anthropic-messages, passthrough"),
        "{}",
        out.stdout
    );
    assert!(out.stdout.ends_with("proxy: ok\n"), "{}", out.stdout);
    // The old hidden positional is gone.
    let old = run(&empty_dir(), &["proxy", "--anthropic", "check"], &[]).await;
    assert!(!old.ok);
}

#[tokio::test]
async fn problems_are_one_precise_error_and_a_failure() {
    let dir = empty_dir();
    let write = |yaml: &str| std::fs::write(dir.join("gateway.yaml"), yaml).unwrap();

    write("log: text\nserve: [openai-chat]\n");
    let out = run(&dir, &["check"], &[]).await;
    assert!(!out.ok && out.stdout.is_empty());
    assert!(
        out.stderr.starts_with("error: gateway.yaml: "),
        "{}",
        out.stderr
    );
    assert!(
        out.stderr.contains("unknown field `serve`"),
        "{}",
        out.stderr
    );
    assert!(out.stderr.contains("`formats`"), "{}", out.stderr);
    assert!(out.stderr.contains("line 2"), "{}", out.stderr);

    write("providers: { p: { vendor: nope } }\n");
    let out = run(&dir, &["check"], &[]).await;
    assert!(!out.ok);
    assert!(
        out.stderr
            .contains("unknown vendor 'nope'; presets: [openai"),
        "{}",
        out.stderr
    );

    write("providers: { p: { formats: nope, base_url: 'http://x' } }\n");
    let out = run(&dir, &["check"], &[]).await;
    assert!(
        out.stderr
            .contains("unknown format 'nope'; compiled-in formats: [openai-chat"),
        "{}",
        out.stderr
    );

    write("providers: { openai: { vendor: openai } }\n");
    let out = run(&dir, &["check"], &[]).await;
    assert!(!out.ok);
    assert!(out.stderr.contains("OPENAI_API_KEY"), "{}", out.stderr);
    assert!(out.stderr.contains("set `key:`"), "{}", out.stderr);
}

#[tokio::test]
async fn the_version_is_the_workspaces() {
    let version = env!("CARGO_PKG_VERSION");
    assert_ne!(version, "0.0.0");
    assert_eq!(respan_gateway::VERSION, version);
    let out = run(&empty_dir(), &["--version"], &[]).await;
    assert_eq!(out.stdout.trim(), format!("respan-gateway {version}"));

    let gw =
        Gateway::start("providers: { p: { formats: openai-chat, base_url: 'http://x.test/v1' } }")
            .await;
    let resp = client()
        .get(gw.url("/ui/api/overview"))
        .send()
        .await
        .unwrap();
    let overview = support::json(&resp.text().await.unwrap());
    assert_eq!(overview["version"], version);
    let banner = gw.stdout_until("config").await;
    assert!(
        banner.starts_with(&format!("respan-gateway {version} listening")),
        "{banner}"
    );
}

#[tokio::test]
async fn dotenv_gives_keys_and_settings_but_never_beats_the_environment() {
    let dir = empty_dir();
    std::fs::write(
        dir.join("a.yaml"),
        "providers: { a: { formats: openai-chat, base_url: 'http://a.test/v1' } }",
    )
    .unwrap();
    std::fs::write(
        dir.join("b.yaml"),
        "providers: { b: { formats: openai-chat, base_url: 'http://b.test/v1' } }",
    )
    .unwrap();
    std::fs::write(
        dir.join(".env"),
        "# comment\nexport OPENAI_API_KEY=\"sk-from-dotenv\"\nRESPAN_CONFIG=b.yaml\n",
    )
    .unwrap();
    // The key comes from .env, and so does the config file.
    let out = run(&dir, &["check"], &[]).await;
    assert!(out.ok, "{}", out.stderr);
    assert!(out.stdout.contains("config     b.yaml"), "{}", out.stdout);

    // The environment wins over .env.
    let out = run(&dir, &["check"], &[("RESPAN_CONFIG", "a.yaml")]).await;
    assert!(out.stdout.contains("config     a.yaml"), "{}", out.stdout);

    // Without a config, the key in .env makes a provider; PORT sets the port.
    std::fs::write(dir.join(".env"), "OPENAI_API_KEY=sk-from-dotenv\nPORT=0\n").unwrap();
    let gw = Gateway::run_with(&[], &dir, &[]).await;
    let banner = gw.stdout_until("config").await;
    assert!(banner.contains("config     environment"), "{banner}");
    assert!(banner.contains("openai"), "{banner}");
    assert!(!gw.base.ends_with(":8080"), "{}", gw.base);

    // A broken .env is an error naming it.
    std::fs::write(dir.join(".env"), "BROKEN LINE\n").unwrap();
    let out = run(&dir, &["check"], &[]).await;
    assert!(!out.ok);
    assert!(out.stderr.starts_with("error: .env: "), "{}", out.stderr);
}

#[tokio::test]
async fn a_port_in_use_says_what_to_do() {
    let taken = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = taken.local_addr().unwrap().to_string();
    let out = run(
        &empty_dir(),
        &["--listen", &addr],
        &[("OPENAI_API_KEY", "sk")],
    )
    .await;
    assert!(!out.ok);
    assert!(
        out.stderr
            .starts_with(&format!("error: listen on {addr}: ")),
        "{}",
        out.stderr
    );
    assert!(
        out.stderr.contains("--listen 127.0.0.1:0"),
        "{}",
        out.stderr
    );
}

#[tokio::test]
async fn models_lists_each_providers_models_with_their_kind() {
    let server = axum::Router::new().route(
        "/v1/models",
        axum::routing::get(|| async {
            axum::Json(json!({"object": "list", "data": [
                {"id": "gpt-4o"}, {"id": "whisper-1"}, {"id": "text-embedding-3-small"},
            ]}))
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}/v1", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, server).await });
    let dir = empty_dir();
    std::fs::write(
        dir.join("gateway.yaml"),
        format!("providers: {{ openai: {{ vendor: openai, base_url: '{base}', key: sk }} }}"),
    )
    .unwrap();
    let out = run(&dir, &["models"], &[]).await;
    assert!(out.ok, "{}", out.stderr);
    let lines: Vec<Vec<&str>> = out
        .stdout
        .lines()
        .map(|l| l.split_whitespace().collect())
        .collect();
    assert_eq!(lines[0], ["PROVIDER", "KIND", "MODEL"]);
    assert!(
        lines.contains(&vec!["openai", "chat", "gpt-4o"]),
        "{}",
        out.stdout
    );
    assert!(
        lines.contains(&vec!["openai", "transcription", "whisper-1"]),
        "{}",
        out.stdout
    );
    assert!(
        lines.contains(&vec!["openai", "embedding", "text-embedding-3-small"]),
        "{}",
        out.stdout
    );

    // Nobody answering is a failure naming the provider.
    let down = support::closed_port().await;
    std::fs::write(
        dir.join("gateway.yaml"),
        format!("providers: {{ openai: {{ vendor: openai, base_url: '{down}/v1', key: sk }} }}"),
    )
    .unwrap();
    let out = run(&dir, &["models"], &[]).await;
    assert!(!out.ok);
    assert!(
        out.stderr.contains("could not list openai"),
        "{}",
        out.stderr
    );
}

/// A provider that lists models only for `key`, answering 401 to the rest.
async fn lists_for(key: &'static str) -> String {
    let server = axum::Router::new().route(
        "/v1/models",
        axum::routing::get(move |headers: axum::http::HeaderMap| async move {
            let sent = headers.get("authorization").and_then(|v| v.to_str().ok());
            match sent == Some(&format!("Bearer {key}")) {
                true => (
                    axum::http::StatusCode::OK,
                    axum::Json(json!({"object": "list", "data": [{"id": "gpt-4o"}]})),
                ),
                false => (
                    axum::http::StatusCode::UNAUTHORIZED,
                    axum::Json(json!({"error": {"message": "bad key"}})),
                ),
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}/v1", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, server).await });
    base
}

#[tokio::test]
async fn check_online_says_per_provider_whether_it_answers_the_key() {
    let (good, wrong) = (lists_for("sk-good").await, lists_for("sk-other").await);
    let down = support::closed_port().await;
    let dir = empty_dir();
    std::fs::write(
        dir.join("gateway.yaml"),
        format!(
            "providers:
  fine: {{ vendor: openai, base_url: '{good}', key: sk-good }}
  stale: {{ vendor: groq, base_url: '{wrong}', key: sk-good }}
  gone: {{ vendor: mistral, base_url: '{down}/v1', key: sk-good }}"
        ),
    )
    .unwrap();

    // Offline, `check` calls nobody.
    let out = run(&dir, &["check"], &[]).await;
    assert!(
        out.ok && !out.stdout.contains("reachable"),
        "{}",
        out.stdout
    );

    let out = run(&dir, &["check", "--online"], &[]).await;
    assert!(!out.ok, "{}", out.stdout);
    let online = out.stdout.split("online (").nth(1).unwrap();
    let state = |name: &str| {
        let line = online.lines().find(|l| l.trim_start().starts_with(name));
        line.unwrap_or_else(|| panic!("{online}")).to_owned()
    };
    assert!(state("fine ").contains(" reachable "), "{online}");
    assert!(state("stale").contains("unauthorized"), "{online}");
    assert!(state("stale").contains("answered 401"), "{online}");
    assert!(state("gone ").contains("unreachable"), "{online}");
    assert!(!format!("{}{}", out.stdout, out.stderr).contains("sk-good"));
    assert!(!online.contains("127.0.0.1"), "{online}");

    // All reachable is success.
    std::fs::write(
        dir.join("gateway.yaml"),
        format!("providers: {{ fine: {{ vendor: openai, base_url: '{good}', key: sk-good }} }}"),
    )
    .unwrap();
    let out = run(&dir, &["check", "--online"], &[]).await;
    assert!(out.ok, "{}{}", out.stdout, out.stderr);
}

#[tokio::test]
async fn init_enables_a_vendor_by_an_env_reference_and_says_how_to_set_it() {
    let dir = empty_dir();
    let args = ["init", "-p", "groq", "-p", "mistral=env:MY_MISTRAL"];
    let out = run(&dir, &args, &[]).await;
    assert!(out.ok, "{}", out.stderr);
    assert!(out.stdout.contains("set GROQ_API_KEY"), "{}", out.stdout);
    assert!(out.stdout.contains("set MY_MISTRAL"), "{}", out.stdout);
    let file = std::fs::read_to_string(dir.join("gateway.yaml")).unwrap();
    assert!(
        file.contains("vendor: groq, key: 'env:GROQ_API_KEY'"),
        "{file}"
    );
    assert!(
        file.contains("vendor: mistral, key: 'env:MY_MISTRAL'"),
        "{file}"
    );

    // With the variable set, the file checks and nothing is left to say.
    let env = [("GROQ_API_KEY", "gsk-x"), ("MY_MISTRAL", "m-x")];
    let out = run(&dir, &["check"], &env).await;
    assert!(out.ok, "{}", out.stderr);
    let out = run(&dir, &["init", "-p", "groq", "--force"], &env).await;
    assert!(out.ok && !out.stdout.contains("set GROQ"), "{}", out.stdout);

    let out = run(&dir, &["init", "-p", "nope", "--force"], &[]).await;
    assert!(!out.ok, "{}", out.stdout);
    assert!(out.stderr.contains("unknown vendor nope"), "{}", out.stderr);
}

#[cfg(unix)]
#[tokio::test]
async fn init_writes_a_literal_key_only_when_given_and_keeps_the_file_private() {
    use std::os::unix::fs::PermissionsExt;
    let dir = empty_dir();
    let out = run(&dir, &["init", "-p", "groq=gsk-literal"], &[]).await;
    assert!(out.ok, "{}", out.stderr);
    assert!(out.stderr.contains("plain text"), "{}", out.stderr);
    assert!(!out.stdout.contains("gsk-literal") && !out.stderr.contains("gsk-literal"));
    let path = dir.join("gateway.yaml");
    let file = std::fs::read_to_string(&path).unwrap();
    assert!(file.contains("key: 'gsk-literal'"), "{file}");
    let mode = std::fs::metadata(&path).unwrap().permissions().mode();
    assert_eq!(mode & 0o077, 0, "{mode:o}");
    let out = run(&dir, &["check"], &[]).await;
    assert!(out.ok, "{}", out.stderr);
    assert!(!out.stdout.contains("gsk-literal"), "{}", out.stdout);
}
