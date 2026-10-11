//! The stdio MCP path of the shipped binary on every platform, Windows
//! included: a Python child given on the command line (or behind a shim
//! found by `PATHEXT`), a session per child, and the end of a session
//! ending the child and everything it started.
//!
//! The server is a script in this file, not a fixture file.
//!
//! Run: `cargo test -p respan-gateway --test e2e_mcp_stdio`.

#![allow(clippy::unwrap_used)]

mod support;

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::{Value, json};
use support::{BOUND, Gateway, client, json};

const PROTOCOL: &str = "2025-06-18";

/// An MCP server: `initialize` (its pid is its version), `tools/list`, and
/// `tools/call` of `echo`, or of `spawn`, which starts a grandchild that
/// sleeps and answers with its pid.
const SERVER: &str = r#"
import sys, json, os, subprocess
def send(m):
    sys.stdout.write(json.dumps(m) + "\n")
    sys.stdout.flush()
for line in sys.stdin:
    m = json.loads(line)
    method, i = m.get("method"), m.get("id")
    if method == "initialize":
        send({"jsonrpc": "2.0", "id": i, "result": {"protocolVersion": "2025-06-18",
              "capabilities": {"tools": {}},
              "serverInfo": {"name": "stdio-py", "version": str(os.getpid())}}})
    elif method == "tools/list":
        send({"jsonrpc": "2.0", "id": i, "result": {"tools": [
              {"name": "echo", "inputSchema": {"type": "object"}},
              {"name": "spawn", "inputSchema": {"type": "object"}}]}})
    elif method == "tools/call":
        if m["params"]["name"] == "spawn":
            grandchild = subprocess.Popen(
                [sys.executable, "-c", "import time; time.sleep(60)"],
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            text = str(grandchild.pid)
        else:
            text = os.environ.get("GREETING", "") + m["params"]["arguments"]["message"]
        send({"jsonrpc": "2.0", "id": i, "result": {"content": [{"type": "text", "text": text}]}})
"#;

/// The Python interpreter's name on this platform.
fn python() -> &'static str {
    if cfg!(windows) { "python" } else { "python3" }
}

/// Whether process `pid` runs (a zombie does not).
fn alive(pid: u32) -> bool {
    let pid = pid.to_string();
    if cfg!(windows) {
        let filter = format!("PID eq {pid}");
        let out = std::process::Command::new("tasklist")
            .args(["/FI", &filter, "/NH", "/FO", "CSV"])
            .output()
            .unwrap();
        return String::from_utf8_lossy(&out.stdout).contains(&format!("\"{pid}\""));
    }
    let up = std::process::Command::new("kill")
        .args(["-0", &pid])
        .output()
        .unwrap();
    let state = std::process::Command::new("ps")
        .args(["-o", "stat=", "-p", &pid])
        .output()
        .unwrap();
    up.status.success()
        && !String::from_utf8_lossy(&state.stdout)
            .trim()
            .starts_with('Z')
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

/// The JSON-RPC messages a response body holds, whole or as events.
fn messages(body: &str) -> Vec<Value> {
    if let Ok(whole) = serde_json::from_str::<Value>(body) {
        return vec![whole];
    }
    let data = body.lines().filter_map(|l| l.strip_prefix("data:"));
    data.map(|d| json(d.trim())).collect()
}

struct Client<'a> {
    gw: &'a Gateway,
    path: &'a str,
    session: Option<String>,
}

impl Client<'_> {
    /// Sends `method` with `body`; the status and the messages answered.
    async fn send(&mut self, method: &str, body: Option<Value>) -> (u16, Vec<Value>) {
        let url = self.gw.url(self.path);
        let mut req = client()
            .request(method.parse().unwrap(), url)
            .header("accept", "application/json, text/event-stream")
            .header("mcp-protocol-version", PROTOCOL);
        if let Some(session) = &self.session {
            req = req.header("mcp-session-id", session);
        }
        if let Some(body) = body {
            req = req
                .header("content-type", "application/json")
                .body(body.to_string());
        }
        let resp = req.send().await.unwrap();
        let status = resp.status().as_u16();
        if let Some(id) = resp.headers().get("mcp-session-id") {
            self.session = Some(id.to_str().unwrap().to_owned());
        }
        let text = tokio::time::timeout(BOUND, resp.text())
            .await
            .unwrap()
            .unwrap();
        (status, messages(&text))
    }

    /// The `result` of request `id` of `method`.
    async fn ask(&mut self, id: u64, method: &str, params: Value) -> Value {
        let body = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        let (status, answered) = self.send("POST", Some(body)).await;
        assert_eq!(status, 200, "{answered:?}");
        let found = answered
            .iter()
            .find(|m| m["id"] == id && m.get("method").is_none());
        found.unwrap_or_else(|| panic!("no answer in {answered:?}"))["result"].clone()
    }

    async fn start(&mut self) -> Value {
        let init = json!({"protocolVersion": PROTOCOL, "capabilities": {},
                          "clientInfo": {"name": "e2e", "version": "1"}});
        let result = self.ask(1, "initialize", init).await;
        let done = json!({"jsonrpc": "2.0", "method": "notifications/initialized"});
        assert_eq!(self.send("POST", Some(done)).await.0, 202);
        result
    }

    async fn call(&mut self, id: u64, tool: &str, arguments: Value) -> String {
        let params = json!({"name": tool, "arguments": arguments});
        let result = self.ask(id, "tools/call", params).await;
        result["content"][0]["text"].as_str().unwrap().to_owned()
    }
}

fn pid_of(init: &Value) -> u32 {
    init["serverInfo"]["version"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap()
}

/// A fresh directory for one test.
fn scratch(name: &str) -> PathBuf {
    let dir =
        Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("stdio-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[tokio::test]
async fn a_python_child_serves_a_session_and_the_end_kills_its_whole_tree() {
    let script = serde_json::to_string(SERVER).unwrap();
    let gw = Gateway::start(&format!(
        "mcp:\n  py:\n    command: [{}, -c, {script}]\n    env: {{ GREETING: 'hello:' }}\n",
        python()
    ))
    .await;
    let mut c = Client {
        gw: &gw,
        path: "/mcp/py",
        session: None,
    };

    let init = c.start().await;
    assert_eq!(init["serverInfo"]["name"], "stdio-py");
    let child = pid_of(&init);
    assert!(alive(child));

    let list = c.ask(2, "tools/list", json!({})).await;
    assert_eq!(list["tools"][0]["name"], "echo");
    assert_eq!(
        c.call(3, "echo", json!({"message": "world"})).await,
        "hello:world"
    );

    let grandchild: u32 = c.call(4, "spawn", json!({})).await.parse().unwrap();
    assert!(alive(grandchild));

    let (status, _) = c.send("DELETE", None).await;
    assert_eq!(status, 204);
    gone(child).await;
    gone(grandchild).await;
}

/// A shim named `pyshim` in a directory of its own (a `.cmd` on Windows,
/// found only through `PATHEXT`) that runs the server written beside it.
fn shim(dir: &Path) {
    std::fs::write(dir.join("server.py"), SERVER).unwrap();
    #[cfg(windows)]
    {
        let cmd = format!("@echo off\r\n{} \"%~dp0server.py\" %*\r\n", python());
        std::fs::write(dir.join("pyshim.cmd"), cmd).unwrap();
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let run = "exec {} \"$(dirname \"$0\")/server.py\" \"$@\"";
        let sh = format!("#!/bin/sh\n{}\n", run.replace("{}", python()));
        let path = dir.join("pyshim");
        std::fs::write(&path, sh).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
}

#[tokio::test]
async fn a_bare_command_name_is_found_on_path_with_the_extensions_of_pathext() {
    let dir = scratch("shim");
    shim(&dir);
    let config =
        support::write_config("listen: 127.0.0.1:0\nmcp:\n  shimmed: { command: [pyshim] }\n");

    // The binary's whole environment: the shim's directory first on PATH.
    let mut paths = vec![dir.clone()];
    paths.extend(std::env::split_paths(&std::env::var_os("PATH").unwrap()));
    let path = std::env::join_paths(paths).unwrap().into_string().unwrap();
    let mut env = vec![("PATH".to_owned(), path)];
    for name in ["SystemRoot", "ComSpec", "PATHEXT", "TEMP", "TMP"] {
        if let Ok(value) = std::env::var(name) {
            env.push((name.to_owned(), value));
        }
    }
    let env: Vec<(&str, &str)> = env.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    let args = ["--config", config.to_str().unwrap()];
    let gw = Gateway::run_with(&args, &dir, &env).await;

    let mut c = Client {
        gw: &gw,
        path: "/mcp/shimmed",
        session: None,
    };
    let init = c.start().await;
    let child = pid_of(&init);
    assert!(alive(child));
    assert_eq!(
        c.call(2, "echo", json!({"message": "via shim"})).await,
        "via shim"
    );
    let grandchild: u32 = c.call(3, "spawn", json!({})).await.parse().unwrap();

    // The shim's own process (`cmd.exe` on Windows) goes with the rest.
    let (status, _) = c.send("DELETE", None).await;
    assert_eq!(status, 204);
    gone(child).await;
    gone(grandchild).await;
}
