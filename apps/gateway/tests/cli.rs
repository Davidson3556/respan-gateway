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
        dir.join("gateway.toml"),
        "client_keys = [\"one\", \"two\"]\nproviders = { literal = { formats = \"openai-chat\", base_url = \"http://a.test/v1\", key = \"sk-literal\" }, named = { formats = \"openai-chat\", base_url = \"http://b.test/v1\", auth = { type = \"bearer\", key = \"env:B_KEY\" } }, open = { formats = \"openai-chat\", base_url = \"http://c.test/v1\" } }\n",
    )
    .unwrap();
    let out = run(&dir, &["check"], &[("B_KEY", "b")]).await;
    assert!(out.ok, "{}", out.stderr);
    let report = &out.stdout;
    assert!(report.contains("config     gateway.toml"), "{report}");
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
    let write = |text: &str| std::fs::write(dir.join("gateway.toml"), text).unwrap();

    write("log = \"text\"\nserve = [\"openai-chat\"]\n");
    let out = run(&dir, &["check"], &[]).await;
    assert!(!out.ok && out.stdout.is_empty());
    assert!(
        out.stderr.starts_with("error: gateway.toml: "),
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

    write("providers = { p = { vendor = \"nope\" } }\n");
    let out = run(&dir, &["check"], &[]).await;
    assert!(!out.ok);
    assert!(
        out.stderr
            .contains("unknown vendor 'nope'; vendors: [openai"),
        "{}",
        out.stderr
    );

    write("providers = { p = { formats = \"nope\", base_url = \"http://x\" } }\n");
    let out = run(&dir, &["check"], &[]).await;
    assert!(
        out.stderr
            .contains("unknown format 'nope'; compiled-in formats: [openai-chat"),
        "{}",
        out.stderr
    );

    write("providers = { openai = { vendor = \"openai\" } }\n");
    let out = run(&dir, &["check"], &[]).await;
    assert!(!out.ok);
    assert!(out.stderr.contains("OPENAI_API_KEY"), "{}", out.stderr);
    assert!(out.stderr.contains("set `key`"), "{}", out.stderr);
}

#[tokio::test]
async fn the_version_is_the_workspaces() {
    let version = env!("CARGO_PKG_VERSION");
    assert_ne!(version, "0.0.0");
    assert_eq!(respan_gateway::VERSION, version);
    let out = run(&empty_dir(), &["--version"], &[]).await;
    assert_eq!(out.stdout.trim(), format!("respan-gateway {version}"));

    let gw = Gateway::start(
        "providers = { p = { formats = \"openai-chat\", base_url = \"http://x.test/v1\" } }\n",
    )
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
        dir.join("a.toml"),
        "providers = { a = { formats = \"openai-chat\", base_url = \"http://a.test/v1\" } }\n",
    )
    .unwrap();
    std::fs::write(
        dir.join("b.toml"),
        "providers = { b = { formats = \"openai-chat\", base_url = \"http://b.test/v1\" } }\n",
    )
    .unwrap();
    std::fs::write(
        dir.join(".env"),
        "# comment\nexport OPENAI_API_KEY=\"sk-from-dotenv\"\nRESPAN_CONFIG=b.toml\n",
    )
    .unwrap();
    // The key comes from .env, and so does the config file.
    let out = run(&dir, &["check"], &[]).await;
    assert!(out.ok, "{}", out.stderr);
    assert!(out.stdout.contains("config     b.toml"), "{}", out.stdout);

    // The environment wins over .env.
    let out = run(&dir, &["check"], &[("RESPAN_CONFIG", "a.toml")]).await;
    assert!(out.stdout.contains("config     a.toml"), "{}", out.stdout);

    // Without a config, the key in .env makes a provider; PORT sets the port (with a client key).
    std::fs::write(
        dir.join(".env"),
        "OPENAI_API_KEY=sk-from-dotenv\nPORT=0\nRESPAN_CLIENT_KEYS=k\n",
    )
    .unwrap();
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
        dir.join("gateway.toml"),
        format!("providers = {{ openai = {{ vendor = \"openai\", base_url = \"{base}\", key = \"sk\" }} }}\n"),
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
        dir.join("gateway.toml"),
        format!("providers = {{ openai = {{ vendor = \"openai\", base_url = \"{down}/v1\", key = \"sk\" }} }}\n"),
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
        dir.join("gateway.toml"),
        format!(
            "providers = {{ fine = {{ vendor = \"openai\", base_url = \"{good}\", key = \"sk-good\" }}, stale = {{ vendor = \"groq\", base_url = \"{wrong}\", key = \"sk-good\" }}, gone = {{ vendor = \"mistral\", base_url = \"{down}/v1\", key = \"sk-good\" }} }}\n"
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
        dir.join("gateway.toml"),
        format!("providers = {{ fine = {{ vendor = \"openai\", base_url = \"{good}\", key = \"sk-good\" }} }}\n"),
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
    let file = std::fs::read_to_string(dir.join("gateway.toml")).unwrap();
    assert!(file.contains("key = \"env:GROQ_API_KEY\""), "{file}");
    assert!(file.contains("key = \"env:MY_MISTRAL\""), "{file}");

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
    let path = dir.join("gateway.toml");
    let file = std::fs::read_to_string(&path).unwrap();
    assert!(file.contains("key = \"gsk-literal\""), "{file}");
    let mode = std::fs::metadata(&path).unwrap().permissions().mode();
    assert_eq!(mode & 0o077, 0, "{mode:o}");
    let out = run(&dir, &["check"], &[]).await;
    assert!(out.ok, "{}", out.stderr);
    assert!(!out.stdout.contains("gsk-literal"), "{}", out.stdout);
}

#[tokio::test]
async fn port_binds_every_address_but_only_when_nothing_else_says_where() {
    let dir = empty_dir();
    let env = [
        ("OPENAI_API_KEY", "sk"),
        ("RESPAN_CLIENT_KEYS", "k"),
        ("PORT", "0"),
    ];
    let gw = Gateway::run_with(&[], &dir, &env).await;
    assert!(gw.base.starts_with("http://0.0.0.0:"), "{}", gw.base);

    // The config's `listen`, RESPAN_LISTEN and --listen all beat PORT.
    std::fs::write(
        dir.join("gateway.toml"),
        "listen = \"127.0.0.1:0\"\nproviders = { openai = { vendor = \"openai\" } }\n",
    )
    .unwrap();
    let gw = Gateway::run_with(&[], &dir, &env).await;
    assert!(gw.base.starts_with("http://127.0.0.1:"), "{}", gw.base);
    std::fs::remove_file(dir.join("gateway.toml")).unwrap();
    let with = |extra: (&'static str, &'static str)| [env[0], env[1], env[2], extra];
    let gw = Gateway::run_with(&[], &dir, &with(("RESPAN_LISTEN", "127.0.0.1:0"))).await;
    assert!(gw.base.starts_with("http://127.0.0.1:"), "{}", gw.base);
    let gw = Gateway::run_with(&["--listen", "127.0.0.1:0"], &dir, &env).await;
    assert!(gw.base.starts_with("http://127.0.0.1:"), "{}", gw.base);

    let out = run(&dir, &["check"], &[env[0], ("PORT", "http")]).await;
    assert!(
        out.stderr.contains("PORT=http: not a port"),
        "{}",
        out.stderr
    );
}

#[tokio::test]
async fn an_open_gateway_beyond_loopback_is_refused_with_what_to_do() {
    let dir = empty_dir();
    let env = [("OPENAI_API_KEY", "sk")];
    for args in [
        &["--listen", "0.0.0.0:0"][..],
        &["check", "--listen", "[::]:0"],
    ] {
        let out = run(&dir, args, &env).await;
        assert!(!out.ok, "{}", out.stdout);
        assert!(
            out.stderr.starts_with("error: refusing to listen on "),
            "{}",
            out.stderr
        );
        assert!(
            out.stderr.contains("spend your provider keys"),
            "{}",
            out.stderr
        );
        assert!(out.stderr.contains("init --client-key"), "{}", out.stderr);
    }
    // PORT alone is the same open gateway.
    let out = run(&dir, &[], &[env[0], ("PORT", "9")]).await;
    assert!(
        !out.ok && out.stderr.contains("0.0.0.0:9"),
        "{}",
        out.stderr
    );
    assert!(!out.stderr.contains("wins over PORT"), "{}", out.stderr);
    // A config's `listen` beats PORT, and the refusal says so.
    std::fs::write(
        dir.join("gateway.toml"),
        "listen = \"0.0.0.0:8\"\nproviders = { openai = { vendor = \"openai\" } }\n",
    )
    .unwrap();
    let out = run(&dir, &[], &[env[0], ("PORT", "9")]).await;
    assert!(
        !out.ok && out.stderr.contains("0.0.0.0:8"),
        "{}",
        out.stderr
    );
    assert!(
        out.stderr.contains("`listen` wins over PORT") && out.stderr.contains("remove `listen`"),
        "{}",
        out.stderr
    );
    let out = run(&dir, &[], &[env[0]]).await;
    assert!(!out.stderr.contains("wins over PORT"), "{}", out.stderr);
    std::fs::remove_file(dir.join("gateway.toml")).unwrap();
    // Loopback is fine without keys (`check` shows the plan).
    let out = run(&dir, &["check", "--listen", "127.0.0.1:0"], &env).await;
    assert!(out.ok, "{}", out.stderr);
}

#[tokio::test]
async fn with_client_keys_a_remote_host_and_origin_are_served() {
    let dir = empty_dir();
    let env = [("OPENAI_API_KEY", "sk"), ("RESPAN_CLIENT_KEYS", "k1, k2")];
    let gw = Gateway::run_with(&["--listen", "0.0.0.0:0"], &dir, &env).await;
    let overview = format!("{}/ui/api/overview", gw.base);
    let remote = client()
        .get(&overview)
        .header("host", "gateway.example.com")
        .header("origin", "https://app.example.com")
        .header("authorization", "Bearer k2");
    let res = remote.send().await.unwrap();
    assert_eq!(res.status(), 200);
    let body: serde_json::Value = serde_json::from_slice(&res.bytes().await.unwrap()).unwrap();
    assert_eq!(body["auth_required"], true);
    assert_eq!(body["config_source"], "environment");
    assert!(body["config_path"].is_null());
    let res = client()
        .get(&overview)
        .header("host", "gateway.example.com")
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 401);
}

#[tokio::test]
async fn proxy_holds_no_keys_so_it_may_listen_beyond_loopback() {
    let dir = empty_dir();
    let gw = Gateway::run_with(
        &["proxy", "--anthropic", "--listen", "0.0.0.0:0"],
        &dir,
        &[],
    )
    .await;
    assert!(gw.base.starts_with("http://0.0.0.0:"), "{}", gw.base);
}

#[tokio::test]
async fn init_client_key_writes_a_strong_key_and_prints_it_once() {
    let dir = empty_dir();
    let out = run(&dir, &["init", "--client-key"], &[("OPENAI_API_KEY", "sk")]).await;
    assert!(out.ok, "{}", out.stderr);
    let file = std::fs::read_to_string(dir.join("gateway.toml")).unwrap();
    let key = out
        .stdout
        .split_whitespace()
        .find(|w| w.starts_with("sk-gw-") && w.len() == 38)
        .unwrap_or_else(|| panic!("{}", out.stdout));
    assert!(
        file.contains(&format!("client_keys = [\"{key}\"]")),
        "{file}"
    );
    assert_eq!(out.stdout.matches(key).count(), 3, "{}", out.stdout);
    assert!(
        out.stdout.contains("Authorization: Bearer"),
        "{}",
        out.stdout
    );
    assert!(out.stdout.contains("x-api-key"), "{}", out.stdout);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(dir.join("gateway.toml"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o077, 0, "{mode:o}");
    }
    // The file serves beyond loopback now.
    let out = run(
        &dir,
        &["check", "--listen", "0.0.0.0:0"],
        &[("OPENAI_API_KEY", "sk")],
    )
    .await;
    assert!(
        out.ok && out.stdout.contains("on: 1 client key"),
        "{}{}",
        out.stdout,
        out.stderr
    );

    // Without the flag, the key stays a hint.
    let out = run(&dir, &["init", "--force"], &[]).await;
    assert!(out.ok && !out.stdout.contains("sk-gw-"), "{}", out.stdout);
    let file = std::fs::read_to_string(dir.join("gateway.toml")).unwrap();
    assert!(file.contains("\n# client_keys = ["), "{file}");
}

#[tokio::test]
async fn toml_discovery_reports_errors_without_falling_back() {
    let dir = empty_dir();
    std::fs::write(
        dir.join("gateway.toml"),
        "[providers.local]\nvendor = 'ollama'\n",
    )
    .unwrap();
    let out = run(&dir, &["check"], &[]).await;
    assert!(out.ok, "{}", out.stderr);
    assert!(out.stdout.contains("gateway.toml"));
    assert!(!out.stdout.contains("\x1b["));
    let out = run(&dir, &["check", "--color", "always"], &[]).await;
    assert!(out.ok && out.stdout.contains("\x1b["));
    let out = run(&dir, &["check", "--color", "never"], &[]).await;
    assert!(out.ok && !out.stdout.contains("\x1b["));
    std::fs::write(dir.join("gateway.toml"), "log = 'sk-secret-value'").unwrap();
    let out = run(&dir, &["check"], &[]).await;
    assert!(!out.ok);
    assert!(out.stderr.contains("gateway.toml") && out.stderr.contains("line 1"));
    assert!(!out.stderr.contains("sk-secret-value"));
}

#[tokio::test]
async fn init_writes_toml_to_an_explicit_path() {
    let dir = empty_dir();
    let out = run(&dir, &["init", "-o", "custom.toml", "-p", "ollama"], &[]).await;
    assert!(out.ok, "{}", out.stderr);
    let out = run(&dir, &["check", "-c", "custom.toml"], &[]).await;
    assert!(out.ok, "{}", out.stderr);
}

#[tokio::test]
async fn dotenv_searches_parents_preserves_exports_and_redacts_parse_errors() {
    let root = empty_dir();
    let child = root.join("nested");
    std::fs::create_dir(&child).unwrap();
    std::fs::write(
        root.join(".env"),
        "OPENAI_API_KEY=sk-dotenv\nRESPAN_LISTEN=8100\n",
    )
    .unwrap();
    let out = run(&child, &["check"], &[("RESPAN_LISTEN", "8101")]).await;
    assert!(out.ok, "{}", out.stderr);
    assert!(out.stdout.contains("127.0.0.1:8101"));
    assert!(!format!("{}{}", out.stdout, out.stderr).contains("sk-dotenv"));
    std::fs::write(child.join(".env"), "OPENAI_API_KEY='sk-broken-secret\n").unwrap();
    let out = run(&child, &["check"], &[]).await;
    assert!(!out.ok && out.stderr.contains(".env: invalid syntax"));
    assert!(!out.stderr.contains("sk-broken-secret"));
}

#[tokio::test]
async fn yaml_is_not_discovered_or_accepted() {
    let dir = empty_dir();
    std::fs::write(
        dir.join("gateway.yaml"),
        "providers: { local: { vendor: ollama } }",
    )
    .unwrap();
    let out = run(&dir, &["check"], &[]).await;
    assert!(!out.ok && out.stderr.contains("no configuration found"));
    assert!(!out.stderr.contains("ANTHROPIC_API_KEY"));
    assert!(out.stderr.lines().all(|line| line.chars().count() < 90));
    let out = run(&dir, &["check", "-c", "gateway.yaml"], &[]).await;
    assert!(!out.ok && out.stderr.contains("line 1"));
}
