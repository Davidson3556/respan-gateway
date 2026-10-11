//! End-to-end tests of the agent loop in the shipped binary: an OpenAI
//! client asks a fake provider that calls a tool of an MCP server (a small
//! strict fake over HTTP, or a Python script over stdio, neither a fixture
//! file), and gets only the final answer; the request log shows the steps.
//!
//! Run: `cargo test -p respan-gateway --test e2e_agent`.

#![allow(clippy::unwrap_used)]

mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, Response, StatusCode};
use axum::routing::any;
use mockllm::{Answer, FakeOpenAi};
use serde_json::{Value, json};
use support::{Gateway, client, json};

const KEY: &str = "gw-key-1";
const UPSTREAM_KEY: &str = "sk-upstream-secret";

/// What the fake MCP server was sent: method, authorization and JSON body.
type Seen = Arc<Mutex<Vec<(String, Option<String>, Value)>>>;

/// A streamable-HTTP MCP server with the tools `read` and `boom`.
struct FakeMcp {
    base: String,
    seen: Seen,
}

impl FakeMcp {
    async fn start() -> Self {
        let seen = Seen::default();
        let log = seen.clone();
        let app = axum::Router::new().route(
            "/mcp",
            any(move |req: Request<Body>| {
                let log = log.clone();
                async move { serve(req, log).await }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self { base, seen }
    }

    fn requests(&self) -> Vec<(String, Option<String>, Value)> {
        self.seen.lock().unwrap().clone()
    }

    /// The JSON-RPC methods it was sent, in order.
    fn methods(&self) -> Vec<String> {
        let methods = self.requests().into_iter().filter(|(m, _, _)| m == "POST");
        methods
            .map(|(_, _, body)| body["method"].as_str().unwrap_or_default().to_owned())
            .collect()
    }
}

fn reply(status: StatusCode, body: String) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .header("mcp-session-id", "s-up")
        .body(Body::from(body))
        .unwrap()
}

async fn serve(req: Request<Body>, log: Seen) -> Response<Body> {
    let method = req.method().to_string();
    let auth = req
        .headers()
        .get("authorization")
        .map(|v| v.to_str().unwrap().to_owned());
    let bytes = axum::body::to_bytes(req.into_body(), 1 << 20)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    log.lock()
        .unwrap()
        .push((method.clone(), auth.clone(), body.clone()));
    if auth.as_deref() != Some(&format!("Bearer {UPSTREAM_KEY}")) {
        return reply(StatusCode::UNAUTHORIZED, "no key".into());
    }
    if method == "DELETE" {
        return reply(StatusCode::NO_CONTENT, String::new());
    }
    let id = body["id"].clone();
    let answer = |result: Value| json!({"jsonrpc": "2.0", "id": id, "result": result}).to_string();
    match body["method"].as_str().unwrap_or_default() {
        "initialize" => reply(
            StatusCode::OK,
            answer(
                json!({"protocolVersion": "2025-06-18", "capabilities": {"tools": {}},
                          "serverInfo": {"name": "fake", "version": "1"}}),
            ),
        ),
        "notifications/initialized" => reply(StatusCode::ACCEPTED, String::new()),
        "tools/list" => {
            let schema = json!({"type": "object", "properties": {"path": {"type": "string"}}});
            let tools = json!({"tools": [
                {"name": "read", "description": "read a file", "inputSchema": schema},
                {"name": "boom", "description": "always fails", "inputSchema": {"type": "object"}},
            ]});
            reply(StatusCode::OK, answer(tools))
        }
        "tools/call" => match body["params"]["name"].as_str().unwrap() {
            "read" => {
                let path = body["params"]["arguments"]["path"].as_str().unwrap_or("?");
                let content = json!([{"type": "text", "text": format!("contents of {path}")}]);
                reply(StatusCode::OK, answer(json!({"content": content})))
            }
            _ => {
                let content = json!([{"type": "text", "text": "it broke"}]);
                reply(
                    StatusCode::OK,
                    answer(json!({"content": content, "isError": true})),
                )
            }
        },
        other => panic!("unexpected method {other}"),
    }
}

fn bearer() -> String {
    format!("Bearer {KEY}")
}

fn config(openai: &FakeOpenAi, mcp: &str, agent: &str) -> String {
    format!(
        "client_keys: [{KEY}]\nproviders:\n  openai: {{ formats: openai-chat, base_url: '{}', key: sk-gw-openai }}\n{mcp}{agent}",
        openai.base_url()
    )
}

fn http_server(up: &FakeMcp) -> String {
    format!(
        "mcp:\n  files: {{ url: '{}/mcp', key: {UPSTREAM_KEY} }}\n",
        up.base
    )
}

async fn ask(gw: &Gateway, extra: &[(&str, &str)], body: Value) -> (u16, Value) {
    let mut req = client()
        .post(gw.url("/v1/chat/completions"))
        .header("authorization", bearer());
    for (name, value) in extra {
        req = req.header(*name, *value);
    }
    let resp = req
        .header("content-type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    (status, json(&resp.text().await.unwrap()))
}

fn hello() -> Value {
    json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "read a.txt"}]})
}

/// The request log's finished entry for the `n`th chat request.
async fn logged(gw: &Gateway, n: usize) -> Value {
    for _ in 0..200 {
        let get = client().get(gw.url("/ui/api/requests?limit=100"));
        let resp = get.header("authorization", bearer()).send().await.unwrap();
        let log = json(&resp.text().await.unwrap());
        let chats: Vec<Value> = log["requests"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|r| r["path"] == "/v1/chat/completions" && !r["duration_ms"].is_null())
            .rev()
            .cloned()
            .collect();
        if let Some(found) = chats.get(n) {
            return found.clone();
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("no finished chat request {n}");
}

#[tokio::test]
async fn the_gateway_runs_a_tool_and_the_client_sees_only_the_answer() {
    let up = FakeMcp::start().await;
    let openai = FakeOpenAi::start().await;
    openai.answer(Answer::tool_call("files__read", json!({"path": "a.txt"})));
    openai.answer(Answer::text("The file says hello"));
    openai.answer(Answer::tool_call("files__boom", json!({})));
    openai.answer(Answer::text("It broke"));
    let gw = Gateway::start(&config(
        &openai,
        &http_server(&up),
        "agent:\n  tools: [files]\n",
    ))
    .await;

    let (status, reply) = ask(&gw, &[], hello()).await;
    assert_eq!(status, 200, "{reply}");
    assert_eq!(
        reply["choices"][0]["message"]["content"],
        "The file says hello"
    );
    assert!(reply["choices"][0]["message"]["tool_calls"].is_null());
    assert!(reply["usage"]["prompt_tokens"].as_u64().unwrap() > 0);

    // The provider saw the tools, then the call and its result.
    let sent = openai.requests();
    assert_eq!(sent.len(), 2);
    let names: Vec<&str> = (sent[0].body["tools"].as_array().unwrap().iter())
        .map(|t| t["function"]["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["files__read", "files__boom"]);
    let messages = sent[1].body["messages"].as_array().unwrap();
    assert_eq!(messages[2]["role"], "tool");
    assert_eq!(messages[2]["content"], "contents of a.txt");

    // The server was reached with its own key alone.
    assert_eq!(
        up.methods(),
        [
            "initialize",
            "notifications/initialized",
            "tools/list",
            "tools/call"
        ]
    );
    assert!(
        up.requests()
            .iter()
            .all(|(_, a, _)| a.as_deref() == Some(&format!("Bearer {UPSTREAM_KEY}")))
    );
    assert!(!format!("{:?}", up.requests()).contains(KEY));

    // The log: two steps, the tool call, the summed usage, the provider.
    let record = logged(&gw, 0).await;
    assert_eq!(record["provider"], "openai", "{record}");
    let steps = record["steps"].as_array().unwrap();
    assert_eq!(steps.len(), 2, "{record}");
    assert_eq!(steps[0]["finish"], "tool_use");
    assert_eq!(steps[0]["tools"][0]["name"], "files__read");
    assert_eq!(steps[0]["tools"][0]["error"], false);
    assert!(steps[1]["tools"].as_array().unwrap().is_empty());
    let (a, b) = (
        steps[0]["input_tokens"].as_u64().unwrap(),
        steps[1]["input_tokens"].as_u64().unwrap(),
    );
    assert_eq!(record["usage"]["input_tokens"], a + b);

    // A second request reuses the session and the cached tool list; a tool
    // that fails is a result the model reads.
    let (status, reply) = ask(&gw, &[], hello()).await;
    assert_eq!(status, 200, "{reply}");
    assert_eq!(reply["choices"][0]["message"]["content"], "It broke");
    assert_eq!(&up.methods()[4..], ["tools/call"]);
    let sent = openai.requests();
    assert_eq!(sent[3].body["messages"][2]["content"], "Error: it broke");
    let record = logged(&gw, 1).await;
    assert_eq!(record["steps"][0]["tools"][0]["error"], true, "{record}");
}

#[tokio::test]
async fn a_request_can_leave_the_loop_and_a_models_list_scopes_it() {
    let up = FakeMcp::start().await;
    let openai = FakeOpenAi::start().await;
    let agent = "agent:\n  tools: [files]\n  models: [gpt-4o]\n";
    let gw = Gateway::start(&config(&openai, &http_server(&up), agent)).await;

    let (status, _) = ask(&gw, &[("x-respan-agent", "off")], hello()).await;
    assert_eq!(status, 200);
    let mut other = hello();
    other["model"] = json!("o3-mini");
    let (status, _) = ask(&gw, &[], other).await;
    assert_eq!(status, 200);
    for sent in openai.requests() {
        assert!(sent.body["tools"].is_null(), "{}", sent.body);
    }
    assert!(up.requests().is_empty());
}

#[tokio::test]
async fn a_streaming_client_gets_the_final_step_streamed() {
    let up = FakeMcp::start().await;
    let openai = FakeOpenAi::start().await;
    openai.answer(Answer::tool_call("files__read", json!({"path": "b.txt"})));
    openai.answer(Answer::text("It really says hello"));
    let gw = Gateway::start(&config(
        &openai,
        &http_server(&up),
        "agent:\n  tools: [files]\n",
    ))
    .await;
    let mut body = hello();
    body["stream"] = json!(true);
    let resp = client()
        .post(gw.url("/v1/chat/completions"))
        .header("authorization", bearer())
        .header("content-type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let (text, ended) = support::read_all(resp).await;
    assert!(ended.is_ok());
    let said: String = (text.split("\n\n").filter_map(|f| f.strip_prefix("data: ")))
        .filter_map(|d| serde_json::from_str::<Value>(d).ok())
        .filter_map(|c| {
            c["choices"][0]["delta"]["content"]
                .as_str()
                .map(str::to_owned)
        })
        .collect();
    assert_eq!(said, "It really says hello", "{text}");
    assert!(!text.contains("tool_calls"), "{text}");
    let record = logged(&gw, 0).await;
    assert_eq!(record["stream"], true);
    assert_eq!(record["steps"].as_array().unwrap().len(), 2);
}

/// A stdio MCP server in Python with one tool, `who`, which says its pid.
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
              "capabilities": {"tools": {}}, "serverInfo": {"name": "s", "version": "1"}}})
    elif method == "tools/list":
        send({"jsonrpc": "2.0", "id": i, "result": {"tools": [
              {"name": "who", "description": "says its pid", "inputSchema": {"type": "object"}}]}})
    elif method == "tools/call":
        send({"jsonrpc": "2.0", "id": i, "result": {"content": [
              {"type": "text", "text": "pid " + str(os.getpid())}]}})
"#;

#[tokio::test]
async fn a_stdio_server_serves_the_loop_from_one_child() {
    let openai = FakeOpenAi::start().await;
    for _ in 0..2 {
        openai.answer(Answer::tool_call("tools__who", json!({})));
        openai.answer(Answer::text("done"));
    }
    let script = serde_json::to_string(STDIO_SERVER).unwrap();
    let mcp = format!("mcp:\n  tools:\n    command: [python3, -c, {script}]\n");
    let gw = Gateway::start(&config(&openai, &mcp, "agent:\n  tools: [tools]\n")).await;
    for _ in 0..2 {
        let (status, reply) = ask(&gw, &[], hello()).await;
        assert_eq!(status, 200, "{reply}");
    }
    let sent = openai.requests();
    let pid = |i: usize| {
        sent[i].body["messages"][2]["content"]
            .as_str()
            .unwrap()
            .to_owned()
    };
    assert!(pid(1).starts_with("pid "), "{}", pid(1));
    assert_eq!(pid(1), pid(3), "one child for both requests");
}

/// Serves `yaml` from a file, so it can be reloaded.
async fn served_from_file(yaml: &str) -> (Gateway, std::path::PathBuf) {
    let path = support::write_config(&format!("listen: 127.0.0.1:0\n{yaml}"));
    (
        Gateway::run(&["--config", path.to_str().unwrap()]).await,
        path,
    )
}

async fn reload(gw: &Gateway) {
    let resp = client()
        .post(gw.url("/ui/api/reload"))
        .header("authorization", bearer())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "{}", resp.text().await.unwrap());
}

/// Whether `up` was sent a `DELETE`.
fn deleted(up: &FakeMcp) -> bool {
    up.requests().iter().any(|(m, _, _)| m == "DELETE")
}

async fn until(what: &str, mut done: impl FnMut() -> bool) {
    for _ in 0..200 {
        if done() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("never happened: {what}");
}

#[tokio::test]
async fn a_reload_ends_the_old_sessions_once_requests_on_it_have_ended() {
    let up = FakeMcp::start().await;
    let openai = FakeOpenAi::start().await;
    openai.answer(Answer::tool_call("files__read", json!({"path": "a.txt"})));
    openai.answer(Answer::text("late").delay_first(700));
    let yaml = config(&openai, &http_server(&up), "agent:\n  tools: [files]\n");
    let (gw, _) = served_from_file(&yaml).await;

    // A loop is in flight, its tool run, when the config reloads.
    let asked = {
        let (url, auth) = (gw.url("/v1/chat/completions"), bearer());
        tokio::spawn(async move {
            let req = client().post(url).header("authorization", auth);
            let sent = req.header("content-type", "application/json");
            sent.body(hello().to_string())
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap()
        })
    };
    until("the tool ran", || {
        up.methods().contains(&"tools/call".to_owned())
    })
    .await;
    reload(&gw).await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(!deleted(&up), "the loop still uses the session");

    // It ends; so does the session.
    let reply = json(&asked.await.unwrap());
    assert_eq!(reply["choices"][0]["message"]["content"], "late", "{reply}");
    until("the session was deleted", || deleted(&up)).await;
    let ended = up.requests().into_iter().find(|(m, _, _)| m == "DELETE");
    let auth = ended.unwrap().1;
    assert_eq!(auth.as_deref(), Some(&*format!("Bearer {UPSTREAM_KEY}")));
    assert!(
        !up.requests()
            .iter()
            .any(|(_, a, _)| a.as_deref() == Some(&*bearer()))
    );

    // The new build starts a session of its own.
    openai.answer(Answer::tool_call("files__read", json!({"path": "b.txt"})));
    openai.answer(Answer::text("again"));
    let (status, reply) = ask(&gw, &[], hello()).await;
    assert_eq!(status, 200, "{reply}");
    let methods = up.methods();
    assert_eq!(methods.iter().filter(|m| *m == "initialize").count(), 2);
}

#[tokio::test]
async fn a_reload_stops_the_old_stdio_child() {
    let openai = FakeOpenAi::start().await;
    openai.answer(Answer::tool_call("tools__who", json!({})));
    openai.answer(Answer::text("done"));
    let script = serde_json::to_string(STDIO_SERVER).unwrap();
    let mcp = format!("mcp:\n  tools:\n    command: [python3, -c, {script}]\n");
    let (gw, _) = served_from_file(&config(&openai, &mcp, "agent:\n  tools: [tools]\n")).await;
    let (status, reply) = ask(&gw, &[], hello()).await;
    assert_eq!(status, 200, "{reply}");
    let said = openai.requests()[1].body["messages"][2]["content"].clone();
    let pid = said.as_str().unwrap().trim_start_matches("pid ").to_owned();
    let alive = || std::path::Path::new(&format!("/proc/{pid}")).exists();
    assert!(alive());
    reload(&gw).await;
    until("the child stopped", || !alive()).await;
}

fn check(yaml: &str) -> (bool, String) {
    let path = support::write_config(yaml);
    let said = std::process::Command::new(env!("CARGO_BIN_EXE_respan-gateway"))
        .args(["check", "--config", path.to_str().unwrap()])
        .output()
        .unwrap();
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&said.stdout),
        String::from_utf8_lossy(&said.stderr)
    );
    (said.status.success(), text)
}

#[test]
fn the_agent_section_is_checked_against_the_mcp_servers() {
    let base = "providers:\n  p: { formats: openai-chat, base_url: 'http://x.test/v1', key: k }\n";
    let server = "mcp:\n  files: { url: 'http://m.test/mcp', key: k }\n  open: { url: 'http://m.test/mcp', forward_auth: true }\n";
    let (ok, text) = check(&format!(
        "{base}{server}agent:\n  tools: [files]\n  max_steps: 4\n  timeout: 2m\n"
    ));
    assert!(ok, "{text}");
    let said = |agent: &str| {
        let (ok, text) = check(&format!("{base}{server}{agent}"));
        assert!(!ok, "{text}");
        text
    };
    assert!(said("agent:\n  tools: [nope]\n").contains("unknown mcp server 'nope'"));
    assert!(said("agent:\n  tools: [open]\n").contains("forward_auth"));
    assert!(said("agent:\n  tools: []\n").contains("names no mcp server"));
    assert!(said("agent:\n  tools: [files, files]\n").contains("listed twice"));
    assert!(said("agent:\n  tools: [files]\n  max_steps: 0\n").contains("max_steps"));
    assert!(said("agent:\n  tools: [files]\n  tool_timeout: 0\n").contains("tool_timeout"));
    assert!(said("agent:\n  tools: [files]\n  bogus: 1\n").contains("bogus"));
    let formats = "formats: [openai-chat]\nagent:\n  tools: [files]\n";
    assert!(said(formats).contains("add it to `formats`"));
}
