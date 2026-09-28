//! A tiny MCP server over stdio, for the tests: one JSON-RPC message per
//! line on stdin and stdout.
//!
//! - `initialize`, `ping`, `tools/list` (tools `echo`, `announce`, `wipe`).
//! - `tools/call` of `echo` sends a progress notification first.
//! - `tools/call` of `announce` answers, then 100 ms later sends an
//!   unsolicited `notifications/message`, which only a `GET` stream sees.

use std::io::{BufRead, Write};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use respan_gateway_core::serde_json::{self, Value, json};

fn main() {
    let out = Arc::new(Mutex::new(std::io::stdout()));
    let say = {
        let out = out.clone();
        move |v: Value| {
            if let Ok(mut out) = out.lock() {
                let _ = writeln!(out, "{v}");
                let _ = out.flush();
            }
        }
    };
    for line in std::io::stdin().lock().lines() {
        let Ok(line) = line else { break };
        let Ok(m) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let (Some(id), Some(method)) = (m.get("id").cloned(), m["method"].as_str()) else {
            continue;
        };
        let reply = |result: Value| json!({ "jsonrpc": "2.0", "id": id, "result": result });
        match method {
            "initialize" => say(reply(json!({
                "protocolVersion": m["params"]["protocolVersion"],
                "capabilities": { "tools": {} },
                "serverInfo": { "name": "echo", "version": "1" },
            }))),
            "ping" => say(reply(json!({}))),
            "tools/list" => say(reply(json!({ "tools": [
                { "name": "echo", "inputSchema": { "type": "object" } },
                { "name": "announce", "inputSchema": { "type": "object" } },
                { "name": "wipe", "inputSchema": { "type": "object" } },
            ]}))),
            "tools/call" => {
                let args = &m["params"]["arguments"];
                let text = args["text"].as_str().unwrap_or_default().to_owned();
                match m["params"]["name"].as_str() {
                    Some("echo") => {
                        say(
                            json!({ "jsonrpc": "2.0", "method": "notifications/progress",
                            "params": { "progressToken": 1, "progress": 1 } }),
                        );
                        say(reply(
                            json!({ "content": [{ "type": "text", "text": text }] }),
                        ));
                    }
                    Some("announce") => {
                        say(reply(json!({ "content": [] })));
                        let say = say.clone();
                        std::thread::spawn(move || {
                            std::thread::sleep(Duration::from_millis(100));
                            say(json!({ "jsonrpc": "2.0", "method": "notifications/message",
                                "params": { "level": "info", "data": text } }));
                        });
                    }
                    _ => say(reply(json!({ "content": [], "isError": true }))),
                }
            }
            _ => say(json!({ "jsonrpc": "2.0", "id": id,
                "error": { "code": -32601, "message": "no such method" } })),
        }
    }
}
