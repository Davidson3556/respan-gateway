//! End-to-end tests of the shipped binary as an MCP gateway: a streamable-HTTP
//! server and a stdio child process, each served at `/mcp/{name}` behind the
//! gateway's client keys.
//!
//! The HTTP server is a small strict fake MCP server in this file; the stdio
//! one is a Python script given on the command line. Neither is a fixture
//! file.
//!
//! Run: `cargo test -p respan-gateway --test e2e_mcp`.

#![allow(clippy::unwrap_used)]

mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, Response, StatusCode};
use axum::routing::any;
use serde_json::{Value, json};
use support::{BOUND, Gateway, client, json};

const KEY: &str = "gw-key-1";
const UPSTREAM_KEY: &str = "sk-upstream-secret";
const PROTOCOL: &str = "2025-06-18";

/// What the fake HTTP MCP server was sent: method, headers and body.
type Sent = (String, Vec<(String, String)>, String);
type Seen = Arc<Mutex<Vec<Sent>>>;

struct FakeMcp {
    base: String,
    seen: Seen,
}

impl FakeMcp {
    /// A server at `/mcp` that wants `Authorization: Bearer <UPSTREAM_KEY>`
    /// if `wants_key`, and names the session `s-up`.
    async fn start(wants_key: bool) -> Self {
        let seen = Seen::default();
        let log = seen.clone();
        let app = axum::Router::new().route(
            "/mcp",
            any(move |req: Request<Body>| {
                let log = log.clone();
                async move { fake(req, log, wants_key).await }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self { base, seen }
    }

    fn requests(&self) -> Vec<Sent> {
        self.seen.lock().unwrap().clone()
    }

    /// The value of header `name` on every request, in order.
    fn header_values(&self, name: &str) -> Vec<Option<String>> {
        let find = |headers: &Vec<(String, String)>| {
            headers
                .iter()
                .find(|(n, _)| n == name)
                .map(|(_, v)| v.clone())
        };
        self.requests().iter().map(|(_, h, _)| find(h)).collect()
    }
}

fn reply(status: StatusCode, content_type: &str, body: String) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", content_type)
        .header("mcp-session-id", "s-up")
        .body(Body::from(body))
        .unwrap()
}

fn sse(events: &[Value]) -> String {
    let event = |(i, e): (usize, &Value)| format!("id: {i}\nevent: message\ndata: {e}\n\n");
    events.iter().enumerate().map(event).collect()
}

async fn fake(req: Request<Body>, log: Seen, wants_key: bool) -> Response<Body> {
    let method = req.method().to_string();
    let headers: Vec<(String, String)> = req
        .headers()
        .iter()
        .map(|(n, v)| (n.to_string(), v.to_str().unwrap_or("").to_owned()))
        .collect();
    let bytes = axum::body::to_bytes(req.into_body(), 1 << 20)
        .await
        .unwrap();
    let body = String::from_utf8_lossy(&bytes).into_owned();
    log.lock()
        .unwrap()
        .push((method.clone(), headers.clone(), body.clone()));
    let header = |name: &str| {
        headers
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    };
    let key = format!("Bearer {UPSTREAM_KEY}");
    if wants_key && header("authorization") != Some(key.as_str()) {
        return reply(StatusCode::UNAUTHORIZED, "text/plain", "no key".into());
    }
    let json_rpc = "application/json";
    match method.as_str() {
        "DELETE" => reply(StatusCode::NO_CONTENT, json_rpc, String::new()),
        "GET" => {
            let note = json!({"jsonrpc": "2.0", "method": "notifications/message",
                              "params": {"level": "info", "data": "hello from the stream"}});
            reply(StatusCode::OK, "text/event-stream", sse(&[note]))
        }
        _ => {
            let m: Value = serde_json::from_str(&body).unwrap();
            let id = m["id"].clone();
            let answer = |result: Value| json!({"jsonrpc": "2.0", "id": id, "result": result});
            match m["method"].as_str().unwrap() {
                "initialize" => reply(
                    StatusCode::OK,
                    json_rpc,
                    answer(json!({
                        "protocolVersion": PROTOCOL,
                        "capabilities": {"tools": {}},
                        "serverInfo": {"name": "fake-http", "version": "1"}
                    }))
                    .to_string(),
                ),
                "notifications/initialized" => reply(StatusCode::ACCEPTED, json_rpc, String::new()),
                "tools/list" => reply(
                    StatusCode::OK,
                    json_rpc,
                    answer(json!({"tools": [{"name": "echo", "inputSchema": {"type": "object"}}]}))
                        .to_string(),
                ),
                "tools/call" => {
                    let note = json!({"jsonrpc": "2.0", "method": "notifications/progress",
                                      "params": {"progressToken": "t", "progress": 1}});
                    let said = m["params"]["arguments"]["message"].clone();
                    let done = answer(json!({"content": [{"type": "text", "text": said}]}));
                    reply(StatusCode::OK, "text/event-stream", sse(&[note, done]))
                }
                other => panic!("unexpected method {other}"),
            }
        }
    }
}

/// The JSON-RPC messages a response body holds, whole or as events.
fn messages(body: &str) -> Vec<Value> {
    if let Ok(whole) = serde_json::from_str::<Value>(body) {
        return vec![whole];
    }
    let data = body.lines().filter_map(|l| l.strip_prefix("data:"));
    data.map(|d| json(d.trim())).collect()
}

struct Answer {
    status: u16,
    session: Option<String>,
    messages: Vec<Value>,
}

impl Answer {
    /// The response to request `id`.
    fn response(&self, id: u64) -> &Value {
        self.messages
            .iter()
            .find(|m| m["id"] == id && m.get("method").is_none())
            .unwrap_or_else(|| panic!("no response {id} in {:?}", self.messages))
    }
}

/// A client's call to `path`: `body` posted if given, else `method`.
async fn call(
    gw: &Gateway,
    path: &str,
    method: &str,
    session: Option<&str>,
    body: Option<Value>,
    extra: &[(&str, &str)],
) -> Answer {
    let mut req = client()
        .request(method.parse().unwrap(), gw.url(path))
        .header("accept", "application/json, text/event-stream")
        .header("mcp-protocol-version", PROTOCOL);
    if let Some(session) = session {
        req = req.header("mcp-session-id", session);
    }
    for (name, value) in extra {
        req = req.header(*name, *value);
    }
    if let Some(body) = body {
        req = req
            .header("content-type", "application/json")
            .body(body.to_string());
    }
    let resp = req.send().await.unwrap();
    let status = resp.status().as_u16();
    let session = resp
        .headers()
        .get("mcp-session-id")
        .map(|v| v.to_str().unwrap().to_owned());
    let text = tokio::time::timeout(BOUND, resp.text())
        .await
        .unwrap()
        .unwrap();
    let messages = if text.trim().is_empty() {
        Vec::new()
    } else {
        messages(&text)
    };
    Answer {
        status,
        session,
        messages,
    }
}

fn request(id: u64, method: &str, params: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params})
}

fn initialize() -> Value {
    request(
        1,
        "initialize",
        json!({"protocolVersion": PROTOCOL, "capabilities": {},
               "clientInfo": {"name": "e2e", "version": "1"}}),
    )
}

fn initialized() -> Value {
    json!({"jsonrpc": "2.0", "method": "notifications/initialized"})
}

/// Starts a session at `path`: its id and the `initialize` answer.
async fn open(gw: &Gateway, path: &str, extra: &[(&str, &str)]) -> (String, Answer) {
    let answer = call(gw, path, "POST", None, Some(initialize()), extra).await;
    assert_eq!(answer.status, 200, "{:?}", answer.messages);
    let session = answer.session.clone().expect("a session id");
    let done = call(gw, path, "POST", Some(&session), Some(initialized()), extra).await;
    assert_eq!(done.status, 202);
    (session, answer)
}

/// The finished log record of the request with `model`, once there.
async fn record(gw: &Gateway, path: &str, model: &str) -> Value {
    for _ in 0..200 {
        let get = client().get(gw.url("/ui/api/requests?limit=100"));
        let resp = get.header("authorization", bearer(KEY)).send().await;
        let log = json(&resp.unwrap().text().await.unwrap());
        let found = log["requests"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["path"] == path && r["model"] == model && !r["duration_ms"].is_null());
        if let Some(found) = found {
            return found.clone();
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("no finished request {model} at {path}");
}

fn bearer(key: &str) -> String {
    format!("Bearer {key}")
}

#[tokio::test]
async fn an_http_server_is_served_behind_client_keys_with_its_own_key() {
    let up = FakeMcp::start(true).await;
    let gw = Gateway::start(&format!(
        "client_keys: [{KEY}]\nmcp:\n  github: {{ url: '{}/mcp', key: {UPSTREAM_KEY} }}\n",
        up.base
    ))
    .await;
    let path = "/mcp/github";
    let auth = bearer(KEY);
    let who = [
        ("authorization", auth.as_str()),
        ("x-api-key", "clients-own-provider-key"),
    ];

    // No gateway key: refused before the server is reached.
    let refused = call(&gw, path, "POST", None, Some(initialize()), &[]).await;
    assert_eq!(refused.status, 401);
    assert!(up.requests().is_empty());

    let (session, init) = open(&gw, path, &who).await;
    assert_eq!(
        session, "s-up",
        "the server's own session id reaches the client"
    );
    assert_eq!(
        init.response(1)["result"]["serverInfo"]["name"],
        "fake-http"
    );

    let list = call(
        &gw,
        path,
        "POST",
        Some(&session),
        Some(request(2, "tools/list", json!({}))),
        &who,
    )
    .await;
    assert_eq!(list.response(2)["result"]["tools"][0]["name"], "echo");

    // A call answered by an event stream: the notification, then the result.
    let echo = request(
        3,
        "tools/call",
        json!({"name": "echo", "arguments": {"message": "hi"}}),
    );
    let called = call(&gw, path, "POST", Some(&session), Some(echo), &who).await;
    assert_eq!(called.messages.len(), 2, "{:?}", called.messages);
    assert_eq!(called.messages[0]["method"], "notifications/progress");
    assert_eq!(called.response(3)["result"]["content"][0]["text"], "hi");

    // The server's own stream, then the end of the session.
    let stream = call(&gw, path, "GET", Some(&session), None, &who).await;
    assert_eq!(
        stream.messages[0]["params"]["data"],
        "hello from the stream"
    );
    let ended = call(&gw, path, "DELETE", Some(&session), None, &who).await;
    assert_eq!(ended.status, 204);

    // Only the gateway's upstream key went on; no client credential did.
    let seen = up.header_values("authorization");
    assert!(seen.len() >= 6, "{seen:?}");
    assert!(
        seen.iter()
            .all(|a| a.as_deref() == Some(&*bearer(UPSTREAM_KEY))),
        "{seen:?}"
    );
    assert!(up.header_values("x-api-key").iter().all(Option::is_none));
    let everything = format!("{:?}", up.requests());
    assert!(!everything.contains(KEY) && !everything.contains("clients-own-provider-key"));

    // The log: kind mcp, what each request did, which server.
    for model in [
        "initialize",
        "tools/list",
        "tools/call echo",
        "GET",
        "DELETE",
    ] {
        let r = record(&gw, path, model).await;
        assert_eq!(
            (r["kind"].as_str(), r["format"].as_str()),
            (Some("mcp"), Some("mcp"))
        );
        assert_eq!(r["provider"], "github", "{r}");
    }

    // The admin API and the unknown name.
    let admin = |p: &'static str| {
        let gw = gw.url(p);
        async move {
            let r = client()
                .get(gw)
                .header("authorization", bearer(KEY))
                .send()
                .await
                .unwrap();
            json(&r.text().await.unwrap())
        }
    };
    let servers = admin("/ui/api/mcp").await;
    let github = &servers["servers"][0];
    assert_eq!(github["name"], "github");
    assert_eq!(github["transport"], "http");
    assert_eq!(github["endpoint"], path);
    assert_eq!(github["credentials"], "gateway");
    assert_eq!(github["status"], "ok");
    assert!(github["requests"].as_u64().unwrap() >= 6, "{github}");
    assert!(!servers.to_string().contains(UPSTREAM_KEY));
    let routes = admin("/ui/api/routes").await;
    assert!(
        routes["serve"].to_string().contains("/mcp/{name}"),
        "{routes}"
    );
    let nothing = call(&gw, "/mcp/nope", "POST", None, Some(initialize()), &who).await;
    assert_eq!(nothing.status, 404);
}

#[tokio::test]
async fn a_clients_credentials_go_on_only_when_the_config_says_so() {
    let up = FakeMcp::start(false).await;
    let gw = Gateway::start(&format!(
        "mcp:\n  closed: {{ url: '{0}/mcp' }}\n  open: {{ url: '{0}/mcp', forward_auth: true }}\n",
        up.base
    ))
    .await;
    let who = [("x-api-key", "clients-own-key")];
    open(&gw, "/mcp/closed", &who).await;
    assert!(up.header_values("x-api-key").iter().all(Option::is_none));
    // Without client keys, a web page's request is refused as for any route.
    let evil = [("origin", "https://evil.example")];
    let page = call(&gw, "/mcp/closed", "POST", None, Some(initialize()), &evil).await;
    assert_eq!(page.status, 403);
    let before = up.requests().len();
    open(&gw, "/mcp/open", &who).await;
    let after = up.header_values("x-api-key");
    assert!(
        after[before..]
            .iter()
            .all(|k| k.as_deref() == Some("clients-own-key")),
        "{after:?}"
    );
}

/// A stdio MCP server in Python: answers `initialize` (its pid is its
/// version), `tools/list` and `tools/call echo` (after a notification), and
/// dies on `tools/call crash`.
const STDIO_SERVER: &str = r#"
import sys, json, os
def send(m):
    sys.stdout.write(json.dumps(m) + "\n")
    sys.stdout.flush()
for line in sys.stdin:
    m = json.loads(line)
    method, i = m.get("method"), m.get("id")
    if method == "initialize":
        send({"jsonrpc": "2.0", "id": i, "result": {"protocolVersion": "2025-06-18",
              "capabilities": {"tools": {}},
              "serverInfo": {"name": "fake-stdio", "version": str(os.getpid())}}})
    elif method == "tools/list":
        send({"jsonrpc": "2.0", "id": i, "result": {"tools": [
              {"name": "echo", "inputSchema": {"type": "object"}}]}})
    elif method == "tools/call":
        if m["params"]["name"] == "crash":
            os._exit(3)
        send({"jsonrpc": "2.0", "method": "notifications/message",
              "params": {"level": "info", "data": "working"}})
        send({"jsonrpc": "2.0", "id": i, "result": {"content": [
              {"type": "text", "text": os.environ.get("GREETING", "") + m["params"]["arguments"]["message"]}]}})
"#;

fn stdio_config() -> String {
    let script = serde_json::to_string(STDIO_SERVER).unwrap();
    format!(
        "client_keys: [{KEY}]\nmcp:\n  files:\n    command: [python3, -c, {script}]\n    env: {{ GREETING: 'hello:' }}\n"
    )
}

fn pid(init: &Answer) -> u32 {
    let version = &init.response(1)["result"]["serverInfo"]["version"];
    version.as_str().unwrap().parse().unwrap()
}

/// Whether process `pid` runs (a zombie does not).
fn alive(pid: u32) -> bool {
    match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(stat) => stat
            .rsplit_once(") ")
            .is_some_and(|(_, rest)| !rest.starts_with('Z')),
        Err(_) => false,
    }
}

async fn gone(pid: u32) {
    for _ in 0..400 {
        if !alive(pid) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("process {pid} still runs");
}

#[tokio::test]
async fn a_stdio_server_runs_a_child_per_session_and_a_dead_one_is_replaced() {
    let gw = Gateway::start(&stdio_config()).await;
    let path = "/mcp/files";
    let auth = bearer(KEY);
    let who = [("authorization", auth.as_str())];

    let refused = call(&gw, path, "POST", None, Some(initialize()), &[]).await;
    assert_eq!(refused.status, 401);

    let (first, init) = open(&gw, path, &who).await;
    let first_pid = pid(&init);
    assert!(alive(first_pid));
    assert_ne!(first.len(), 0);

    let list = call(
        &gw,
        path,
        "POST",
        Some(&first),
        Some(request(2, "tools/list", json!({}))),
        &who,
    )
    .await;
    assert_eq!(list.response(2)["result"]["tools"][0]["name"], "echo");
    let echo = request(
        3,
        "tools/call",
        json!({"name": "echo", "arguments": {"message": "world"}}),
    );
    let called = call(&gw, path, "POST", Some(&first), Some(echo), &who).await;
    assert_eq!(called.messages[0]["method"], "notifications/message");
    assert_eq!(
        called.response(3)["result"]["content"][0]["text"],
        "hello:world"
    );

    // Another session has a child of its own.
    let (second, init) = open(&gw, path, &who).await;
    let second_pid = pid(&init);
    assert_ne!(first_pid, second_pid);

    // The child dies mid-call: that call fails, its session is gone ...
    let crash = request(4, "tools/call", json!({"name": "crash", "arguments": {}}));
    let crashed = call(&gw, path, "POST", Some(&first), Some(crash), &who).await;
    assert!(crashed.status >= 500, "{}", crashed.status);
    gone(first_pid).await;
    let after = call(
        &gw,
        path,
        "POST",
        Some(&first),
        Some(request(5, "tools/list", json!({}))),
        &who,
    )
    .await;
    assert_eq!(after.status, 404);
    // ... and its client starts a new session with a fresh child.
    let (third, init) = open(&gw, path, &who).await;
    let third_pid = pid(&init);
    assert!(alive(third_pid) && ![first_pid, second_pid].contains(&third_pid));
    // The other session never noticed.
    let list = call(
        &gw,
        path,
        "POST",
        Some(&second),
        Some(request(2, "tools/list", json!({}))),
        &who,
    )
    .await;
    assert_eq!(list.status, 200);

    // A client ending its session ends its child.
    let ended = call(&gw, path, "DELETE", Some(&third), None, &who).await;
    assert_eq!(ended.status, 204);
    gone(third_pid).await;

    // The log and the admin API.
    let r = record(&gw, path, "tools/call echo").await;
    assert_eq!(
        (r["kind"].as_str(), r["provider"].as_str()),
        (Some("mcp"), Some("files"))
    );
    let admin = client()
        .get(gw.url("/ui/api/mcp"))
        .header("authorization", auth.as_str());
    let admin = json(&admin.send().await.unwrap().text().await.unwrap());
    assert_eq!(admin["servers"][0]["transport"], "stdio");
    assert_eq!(admin["servers"][0]["credentials"], "none");

    // Stopping the gateway ends the children still running.
    let mut gw = gw;
    let stop = tokio::process::Command::new("kill")
        .args(["-TERM", &gw.child.id().unwrap().to_string()])
        .status()
        .await
        .unwrap();
    assert!(stop.success());
    tokio::time::timeout(BOUND, gw.child.wait())
        .await
        .unwrap()
        .unwrap();
    gone(second_pid).await;
}

#[tokio::test]
async fn bad_mcp_config_is_refused_at_start() {
    let path = support::write_config("mcp:\n  x: { command: [no-such-program-anywhere] }\n");
    let said = std::process::Command::new(env!("CARGO_BIN_EXE_respan-gateway"))
        .args(["check", "--config", path.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(!said.status.success());
    let err = String::from_utf8_lossy(&said.stderr);
    assert!(
        err.contains("mcp 'x'") && err.contains("not found on PATH"),
        "{err}"
    );

    let ok = support::write_config("mcp:\n  x: { command: [sh] }\n");
    let said = std::process::Command::new(env!("CARGO_BIN_EXE_respan-gateway"))
        .args(["check", "--config", ok.to_str().unwrap()])
        .output()
        .unwrap();
    let out = String::from_utf8_lossy(&said.stdout);
    assert!(
        said.status.success(),
        "{out}{}",
        String::from_utf8_lossy(&said.stderr)
    );
    assert!(
        out.contains("mcp") && out.contains("/mcp/x (stdio)"),
        "{out}"
    );
}

const KEY_2: &str = "gw-key-2";

#[tokio::test]
async fn an_http_session_belongs_to_the_client_key_that_opened_it() {
    let up = FakeMcp::start(false).await;
    let gw = Gateway::start(&format!(
        "client_keys: [{KEY}, {KEY_2}]\nmcp:\n  github: {{ url: '{}/mcp' }}\n",
        up.base
    ))
    .await;
    let path = "/mcp/github";
    let one = bearer(KEY);
    let (one, two) = ([("authorization", one.as_str())], [("x-api-key", KEY_2)]);
    let (session, _) = open(&gw, path, &one).await;

    let list = || Some(request(2, "tools/list", json!({})));
    // The other key names the session: as if it were not there, and the
    // server is not asked.
    let before = up.requests().len();
    let theirs = call(&gw, path, "POST", Some(&session), list(), &two).await;
    assert_eq!(theirs.status, 404, "{:?}", theirs.messages);
    assert_eq!(theirs.messages[0]["error"]["code"], -32003);
    let ended = call(&gw, path, "DELETE", Some(&session), None, &two).await;
    assert_eq!(ended.status, 404);
    let stream = call(&gw, path, "GET", Some(&session), None, &two).await;
    assert_eq!(stream.status, 404);
    assert_eq!(up.requests().len(), before, "the server was reached");

    // Its own key still uses it, and a session id nobody was given is
    // unknown to everyone.
    let mine = call(&gw, path, "POST", Some(&session), list(), &one).await;
    assert_eq!(mine.status, 200);
    let made_up = call(&gw, path, "POST", Some("s-other"), list(), &one).await;
    assert_eq!(made_up.status, 404);

    // Only the owner ends it; then it is gone for everyone.
    let ended = call(&gw, path, "DELETE", Some(&session), None, &one).await;
    assert_eq!(ended.status, 204);
    let gone = call(&gw, path, "POST", Some(&session), list(), &one).await;
    assert_eq!(gone.status, 404);
}

#[tokio::test]
async fn a_stdio_session_belongs_to_the_client_key_that_opened_it() {
    let script = serde_json::to_string(STDIO_SERVER).unwrap();
    let gw = Gateway::start(&format!(
        "client_keys: [{KEY}, {KEY_2}]\nmcp:\n  files: {{ command: [python3, -c, {script}] }}\n"
    ))
    .await;
    let path = "/mcp/files";
    let (one, two) = ([("x-api-key", KEY)], [("x-api-key", KEY_2)]);
    let (session, init) = open(&gw, path, &one).await;
    let child = pid(&init);

    let list = || Some(request(2, "tools/list", json!({})));
    let theirs = call(&gw, path, "POST", Some(&session), list(), &two).await;
    assert_eq!(theirs.status, 404, "{:?}", theirs.messages);
    let ended = call(&gw, path, "DELETE", Some(&session), None, &two).await;
    assert_eq!(ended.status, 404);
    assert!(alive(child), "another key ended the session");
    let mine = call(&gw, path, "POST", Some(&session), list(), &one).await;
    assert_eq!(mine.status, 200);

    // The other key has sessions of its own, apart.
    let (other, init) = open(&gw, path, &two).await;
    assert_ne!(pid(&init), child);
    let back = call(&gw, path, "POST", Some(&other), list(), &one).await;
    assert_eq!(back.status, 404);
    let own = call(&gw, path, "POST", Some(&other), list(), &two).await;
    assert_eq!(own.status, 200);
}

#[tokio::test]
async fn check_online_probes_each_mcp_server_without_naming_keys() {
    let up = FakeMcp::start(true).await;
    let down = support::closed_port().await;
    let script = serde_json::to_string(STDIO_SERVER).unwrap();
    let pidfile = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("probe-{}.pid", std::process::id()));
    let wrapper = "echo $$ > \"$0\"; exec python3 -c \"$1\"";
    let wrapper = serde_json::to_string(wrapper).unwrap();
    let pidfile_text = serde_json::to_string(pidfile.to_str().unwrap()).unwrap();
    let config = support::write_config(&format!(
        "mcp:
  good: {{ url: '{base}/mcp', key: {UPSTREAM_KEY} }}
  stale: {{ url: '{base}/mcp', key: sk-wrong-key }}
  gone: {{ url: '{down}/mcp', key: {UPSTREAM_KEY} }}
  child: {{ command: [sh, -c, {wrapper}, {pidfile_text}, {script}] }}
  quits: {{ command: [sh, -c, 'exit 0'] }}
",
        base = up.base
    ));
    let out = tokio::process::Command::new(env!("CARGO_BIN_EXE_respan-gateway"))
        .args(["check", "--online", "--config", config.to_str().unwrap()])
        .output()
        .await
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success(), "{stdout}");
    let online = stdout.split("online (").nth(1).unwrap();
    let state = |name: &str| {
        let line = online.lines().find(|l| l.trim_start().starts_with(name));
        line.unwrap_or_else(|| panic!("{online}")).to_owned()
    };
    assert!(state("mcp/good ").contains(" reachable "), "{online}");
    assert!(state("mcp/stale").contains("unauthorized"), "{online}");
    assert!(state("mcp/stale").contains("answered 401"), "{online}");
    assert!(state("mcp/gone ").contains("unreachable"), "{online}");
    assert!(state("mcp/child").contains(" reachable "), "{online}");
    assert!(state("mcp/quits").contains("unreachable"), "{online}");
    for secret in [UPSTREAM_KEY, "sk-wrong-key"] {
        assert!(!format!("{stdout}{stderr}").contains(secret));
    }
    assert!(!online.contains("127.0.0.1"), "{online}");

    // The server saw an `initialize` and then the session ended; the
    // child was stopped.
    let methods: Vec<_> = up.requests().iter().map(|r| r.0.clone()).collect();
    assert_eq!(methods.iter().filter(|m| *m == "DELETE").count(), 1);
    let child = std::fs::read_to_string(&pidfile).unwrap();
    gone(child.trim().parse().unwrap()).await;
}
