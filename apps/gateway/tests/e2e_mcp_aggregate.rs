//! End-to-end tests of the shipped binary's aggregate MCP endpoint (`/mcp`,
//! `plugins.mcp_aggregate`) and of OAuth for an HTTP MCP server (`oauth`).
//!
//! The upstream servers and the token endpoint are small strict fakes in
//! this file. Run: `cargo test -p respan-gateway --test e2e_mcp_aggregate`.

#![allow(clippy::unwrap_used)]

mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Body;
use axum::extract::State;
use axum::http::{Request, Response, StatusCode};
use axum::routing::{any, post};
use serde_json::{Value, json};
use support::{BOUND, Gateway, client, json};

const KEY: &str = "gw-key-1";
const OTHER: &str = "gw-key-2";
const PROTOCOL: &str = "2025-06-18";

/// What a fake was sent: method, session header, authorization, body.
#[derive(Debug, Clone)]
struct Sent {
    method: String,
    session: Option<String>,
    authorization: Option<String>,
    body: Value,
}

struct Fake {
    base: String,
    seen: Arc<Mutex<Vec<Sent>>>,
    /// The bearer token it takes, if it wants one; changed by a test.
    wants: Arc<Mutex<Option<String>>>,
}

#[derive(Clone)]
struct Shape {
    label: &'static str,
    /// Pages of tools: the first answers `nextCursor` for the second.
    paged: bool,
    seen: Arc<Mutex<Vec<Sent>>>,
    wants: Arc<Mutex<Option<String>>>,
}

impl Fake {
    async fn start(label: &'static str, paged: bool, wants: Option<&str>) -> Self {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let wants = Arc::new(Mutex::new(wants.map(str::to_owned)));
        let shape = Shape {
            label,
            paged,
            seen: seen.clone(),
            wants: wants.clone(),
        };
        let app = axum::Router::new()
            .route("/mcp", any(serve))
            .with_state(shape);
        let base = listen(app).await;
        Self { base, seen, wants }
    }

    fn url(&self) -> String {
        format!("{}/mcp", self.base)
    }

    fn sent(&self) -> Vec<Sent> {
        self.seen.lock().unwrap().clone()
    }

    /// The methods of the JSON-RPC requests it was sent.
    fn methods(&self) -> Vec<String> {
        let all = self.sent();
        let methods = all.iter().filter_map(|s| s.body["method"].as_str());
        methods.map(str::to_owned).collect()
    }

    /// The `params` of every request of `method`.
    fn params(&self, method: &str) -> Vec<Value> {
        let all = self.sent();
        let of = all.iter().filter(|s| s.body["method"] == method);
        of.map(|s| s.body["params"].clone()).collect()
    }
}

async fn listen(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    base
}

fn reply(shape: &Shape, status: StatusCode, content_type: &str, body: String) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", content_type)
        .header("mcp-session-id", format!("s-{}", shape.label))
        .body(Body::from(body))
        .unwrap()
}

fn events(messages: &[Value]) -> String {
    messages
        .iter()
        .map(|m| format!("event: message\ndata: {m}\n\n"))
        .collect()
}

async fn serve(State(shape): State<Shape>, req: Request<Body>) -> Response<Body> {
    let method = req.method().to_string();
    let (session, authorization) = {
        let header = |name: &str| {
            let value = req.headers().get(name);
            value.map(|v| v.to_str().unwrap().to_owned())
        };
        (header("mcp-session-id"), header("authorization"))
    };
    let bytes = axum::body::to_bytes(req.into_body(), 1 << 20)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    shape.seen.lock().unwrap().push(Sent {
        method: method.clone(),
        session,
        authorization: authorization.clone(),
        body: body.clone(),
    });
    let wants = shape.wants.lock().unwrap().clone();
    if let Some(token) = wants
        && authorization != Some(format!("Bearer {token}"))
    {
        return reply(&shape, StatusCode::UNAUTHORIZED, "text/plain", "no".into());
    }
    let label = shape.label;
    let json_type = "application/json";
    match method.as_str() {
        "DELETE" => reply(&shape, StatusCode::NO_CONTENT, json_type, String::new()),
        "GET" => {
            let changed = json!({"jsonrpc": "2.0", "method": "notifications/tools/list_changed"});
            let updated = json!({"jsonrpc": "2.0", "method": "notifications/resources/updated",
                                 "params": {"uri": format!("file:///{label}.txt")}});
            let own = json!({"jsonrpc": "2.0", "method": "notifications/progress",
                             "params": {"progressToken": "x", "progress": 1}});
            let body = events(&[changed, updated, own]);
            reply(&shape, StatusCode::OK, "text/event-stream", body)
        }
        _ => {
            let id = body["id"].clone();
            let answer = |result: Value| json!({"jsonrpc": "2.0", "id": id, "result": result});
            let params = &body["params"];
            match body["method"].as_str().unwrap() {
                "initialize" => {
                    let caps = match label {
                        "alpha" => json!({"tools": {"listChanged": true}, "prompts": {},
                                          "resources": {"subscribe": true}}),
                        _ => json!({"tools": {}}),
                    };
                    let result = answer(json!({
                        "protocolVersion": PROTOCOL, "capabilities": caps,
                        "serverInfo": {"name": label, "version": "1"}
                    }));
                    reply(&shape, StatusCode::OK, json_type, result.to_string())
                }
                "notifications/initialized" | "notifications/cancelled" => {
                    reply(&shape, StatusCode::ACCEPTED, json_type, String::new())
                }
                "tools/list" => {
                    let tool =
                        |name: &str| json!({"name": name, "inputSchema": {"type": "object"}});
                    let (tools, next) = match (shape.paged, params["cursor"].as_str()) {
                        (true, None) => (vec![tool("echo")], Some("page-2")),
                        (true, Some(_)) => (vec![tool("late")], None),
                        (false, _) => (vec![tool("echo"), tool("secret")], None),
                    };
                    let mut result = json!({ "tools": tools });
                    if let Some(next) = next {
                        result["nextCursor"] = json!(next);
                    }
                    reply(
                        &shape,
                        StatusCode::OK,
                        json_type,
                        answer(result).to_string(),
                    )
                }
                "tools/call" => {
                    let note = json!({"jsonrpc": "2.0", "method": "notifications/progress",
                                      "params": {"progressToken": "t", "progress": 1}});
                    let said = format!(
                        "{label}:{}",
                        params["arguments"]["message"].as_str().unwrap_or("")
                    );
                    let done = answer(json!({"content": [
                        {"type": "text", "text": said},
                        {"type": "resource_link", "uri": format!("file:///{label}.txt")},
                    ]}));
                    let body = events(&[note, done]);
                    reply(&shape, StatusCode::OK, "text/event-stream", body)
                }
                "prompts/list" => reply(
                    &shape,
                    StatusCode::OK,
                    json_type,
                    answer(json!({"prompts": [{"name": "greet"}]})).to_string(),
                ),
                "prompts/get" => reply(
                    &shape,
                    StatusCode::OK,
                    json_type,
                    answer(json!({"messages": [{"role": "user", "content":
                        {"type": "text", "text": format!("hi from {}", params["name"])}}]}))
                    .to_string(),
                ),
                "resources/list" => reply(
                    &shape,
                    StatusCode::OK,
                    json_type,
                    answer(json!({"resources": [{"uri": "file:///alpha.txt", "name": "a"}]}))
                        .to_string(),
                ),
                "resources/templates/list" => reply(
                    &shape,
                    StatusCode::OK,
                    json_type,
                    answer(
                        json!({"resourceTemplates": [{"uriTemplate": "file:///{p}", "name": "t"}]}),
                    )
                    .to_string(),
                ),
                "resources/read" => reply(
                    &shape,
                    StatusCode::OK,
                    json_type,
                    answer(json!({"contents": [{"uri": params["uri"], "text": "body"}]}))
                        .to_string(),
                ),
                "resources/subscribe" => reply(
                    &shape,
                    StatusCode::OK,
                    json_type,
                    answer(json!({})).to_string(),
                ),
                other => panic!("unexpected method {other}"),
            }
        }
    }
}

type Forms = Arc<Mutex<Vec<Vec<(String, String)>>>>;

/// A token endpoint: what it was sent (form fields), and the token it issues.
struct Tokens {
    url: String,
    forms: Forms,
}

#[derive(Clone)]
struct Issuer {
    forms: Forms,
}

impl Tokens {
    async fn start() -> Self {
        let forms = Arc::new(Mutex::new(Vec::new()));
        let app = axum::Router::new()
            .route("/token", post(issue))
            .with_state(Issuer {
                forms: forms.clone(),
            });
        let base = listen(app).await;
        Self {
            url: format!("{base}/token"),
            forms,
        }
    }

    fn forms(&self) -> Vec<Vec<(String, String)>> {
        self.forms.lock().unwrap().clone()
    }
}

fn field<'a>(form: &'a [(String, String)], name: &str) -> Option<&'a str> {
    let found = form.iter().find(|(n, _)| n == name);
    found.map(|(_, v)| v.as_str())
}

/// Issues `tok-1`, `tok-2`, ...; a refresh token is rotated to `refresh-N`.
async fn issue(State(issuer): State<Issuer>, body: String) -> Response<Body> {
    let form: Vec<(String, String)> = url_form(&body);
    let n = {
        let mut forms = issuer.forms.lock().unwrap();
        forms.push(form.clone());
        forms.len()
    };
    let mut answer = json!({"access_token": format!("tok-{n}"), "token_type": "Bearer",
                            "expires_in": 3600});
    if field(&form, "grant_type") == Some("refresh_token") {
        answer["refresh_token"] = json!(format!("refresh-{n}"));
    }
    Response::builder()
        .header("content-type", "application/json")
        .body(Body::from(answer.to_string()))
        .unwrap()
}

fn url_form(body: &str) -> Vec<(String, String)> {
    let decode = |s: &str| {
        let s = s.replace('+', " ");
        let mut out = Vec::new();
        let bytes = s.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'%' && i + 2 < bytes.len() {
                out.push(u8::from_str_radix(&s[i + 1..i + 3], 16).unwrap());
                i += 3;
            } else {
                out.push(bytes[i]);
                i += 1;
            }
        }
        String::from_utf8(out).unwrap()
    };
    body.split('&')
        .filter_map(|pair| pair.split_once('='))
        .map(|(n, v)| (decode(n), decode(v)))
        .collect()
}

// --- the client side ---

struct Answer {
    status: u16,
    session: Option<String>,
    messages: Vec<Value>,
}

impl Answer {
    fn response(&self, id: u64) -> &Value {
        let found = self
            .messages
            .iter()
            .find(|m| m["id"] == id && m.get("method").is_none());
        found.unwrap_or_else(|| panic!("no response {id} in {:?}", self.messages))
    }

    fn result(&self, id: u64) -> &Value {
        let response = self.response(id);
        assert!(response.get("result").is_some(), "{response}");
        &response["result"]
    }
}

async fn call(
    gw: &Gateway,
    method: &str,
    session: Option<&str>,
    body: Option<Value>,
    key: Option<&str>,
) -> Answer {
    let mut req = client()
        .request(method.parse().unwrap(), gw.url("/mcp"))
        .header("accept", "application/json, text/event-stream")
        .header("mcp-protocol-version", PROTOCOL);
    if let Some(session) = session {
        req = req.header("mcp-session-id", session);
    }
    if let Some(key) = key {
        req = req.header("authorization", format!("Bearer {key}"));
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
    let messages = match serde_json::from_str::<Value>(&text) {
        Ok(whole) => vec![whole],
        Err(_) => {
            let data = text.lines().filter_map(|l| l.strip_prefix("data:"));
            data.map(|d| json(d.trim())).collect()
        }
    };
    let messages = messages.into_iter().filter(|m| !m.is_null()).collect();
    Answer {
        status,
        session,
        messages,
    }
}

fn request(id: u64, method: &str, params: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params})
}

async fn open(gw: &Gateway, key: &str) -> (String, Answer) {
    let hello = request(
        1,
        "initialize",
        json!({"protocolVersion": PROTOCOL, "capabilities": {},
               "clientInfo": {"name": "e2e", "version": "1"}}),
    );
    let answer = call(gw, "POST", None, Some(hello), Some(key)).await;
    assert_eq!(answer.status, 200, "{:?}", answer.messages);
    let session = answer.session.clone().expect("a session id");
    let done = json!({"jsonrpc": "2.0", "method": "notifications/initialized"});
    let done = call(gw, "POST", Some(&session), Some(done), Some(key)).await;
    assert_eq!(done.status, 202);
    (session, answer)
}

async fn ask(gw: &Gateway, session: &str, id: u64, method: &str, params: Value) -> Answer {
    let body = request(id, method, params);
    call(gw, "POST", Some(session), Some(body), Some(KEY)).await
}

async fn record(gw: &Gateway, model: &str) -> Value {
    for _ in 0..200 {
        let get = client().get(gw.url("/ui/api/requests?limit=100"));
        let resp = get
            .header("authorization", format!("Bearer {KEY}"))
            .send()
            .await;
        let log = json(&resp.unwrap().text().await.unwrap());
        let found =
            log["requests"].as_array().unwrap().iter().find(|r| {
                r["path"] == "/mcp" && r["model"] == model && !r["duration_ms"].is_null()
            });
        if let Some(found) = found {
            return found.clone();
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("no finished request {model} at /mcp");
}

fn aggregate_config(alpha: &Fake, beta: &Fake, extra: &str) -> String {
    format!(
        "client_keys = [\"{KEY}\", \"{OTHER}\"]\nmcp = {{ alpha = {{ url = \"{}\" }}, beta = {{ url = \"{}\" }} }}\n{extra}",
        alpha.url(),
        beta.url()
    )
}

// --- the aggregate ---

#[tokio::test]
async fn one_endpoint_fronts_every_server() {
    let alpha = Fake::start("alpha", true, None).await;
    let beta = Fake::start("beta", false, None).await;
    let config = aggregate_config(
        &alpha,
        &beta,
        "plugins = { mcp_aggregate = { deny = [\"beta__secret\"] } }\n",
    );
    let gw = Gateway::start(&config).await;

    // Behind the client keys.
    let hello = request(1, "initialize", json!({"protocolVersion": PROTOCOL}));
    let refused = call(&gw, "POST", None, Some(hello), None).await;
    assert_eq!(refused.status, 401);
    assert!(alpha.sent().is_empty());

    let (session, init) = open(&gw, KEY).await;
    assert_ne!(session, "s-alpha", "the aggregate has its session ids");
    let result = init.result(1);
    assert_eq!(result["serverInfo"]["name"], "respan-gateway");
    assert_eq!(result["protocolVersion"], PROTOCOL);
    assert_eq!(
        result["capabilities"],
        json!({"tools": {"listChanged": true}, "prompts": {}, "resources": {"subscribe": true}})
    );

    // Tools, namespaced and filtered; the first page of each server.
    let list = ask(&gw, &session, 2, "tools/list", json!({})).await;
    let names = |a: &Answer, id| -> Vec<String> {
        let tools = a.result(id)["tools"].as_array().unwrap();
        tools
            .iter()
            .map(|t| t["name"].as_str().unwrap().to_owned())
            .collect()
    };
    assert_eq!(names(&list, 2), ["alpha__echo", "beta__echo"]);
    let cursor = list.result(2)["nextCursor"].as_str().unwrap().to_owned();
    // The cursor asks for the servers that have more, and only those.
    let before = beta.methods().len();
    let more = ask(&gw, &session, 3, "tools/list", json!({"cursor": cursor})).await;
    assert_eq!(names(&more, 3), ["alpha__late"]);
    assert!(more.result(3).get("nextCursor").is_none());
    assert_eq!(beta.methods().len(), before, "beta had no more pages");
    let bad = ask(
        &gw,
        &session,
        4,
        "tools/list",
        json!({"cursor": "nonsense"}),
    )
    .await;
    assert_eq!(bad.response(4)["error"]["code"], -32602);

    // A call goes to its server under the name it knows, with its progress.
    let echo = json!({"name": "beta__echo", "arguments": {"message": "hi"}});
    let called = ask(&gw, &session, 5, "tools/call", echo).await;
    assert_eq!(called.messages.len(), 2, "{:?}", called.messages);
    assert_eq!(called.messages[0]["method"], "notifications/progress");
    let content = &called.result(5)["content"];
    assert_eq!(content[0]["text"], "beta:hi");
    assert_eq!(content[1]["uri"], "mcp://beta/file:///beta.txt");
    assert_eq!(beta.params("tools/call")[0]["name"], "echo");
    assert!(alpha.params("tools/call").is_empty());

    // Unknown, and withheld, tools are not called.
    for (id, name) in [(6, "gamma__echo"), (7, "beta__secret"), (8, "echo")] {
        let call = json!({"name": name, "arguments": {}});
        let refused = ask(&gw, &session, id, "tools/call", call).await;
        assert_eq!(refused.response(id)["error"]["code"], -32602, "{name}");
    }
    assert_eq!(beta.params("tools/call").len(), 1);

    // Prompts are named like tools.
    let prompts = ask(&gw, &session, 9, "prompts/list", json!({})).await;
    assert_eq!(prompts.result(9)["prompts"][0]["name"], "alpha__greet");
    let get = ask(
        &gw,
        &session,
        10,
        "prompts/get",
        json!({"name": "alpha__greet"}),
    )
    .await;
    let text = &get.result(10)["messages"][0]["content"]["text"];
    assert_eq!(text, "hi from \"greet\"");

    // Resources keep their URIs under the server's.
    let resources = ask(&gw, &session, 11, "resources/list", json!({})).await;
    assert_eq!(
        resources.result(11)["resources"][0]["uri"],
        "mcp://alpha/file:///alpha.txt"
    );
    let templates = ask(&gw, &session, 12, "resources/templates/list", json!({})).await;
    assert_eq!(
        templates.result(12)["resourceTemplates"][0]["uriTemplate"],
        "mcp://alpha/file:///{p}"
    );
    let uri = "mcp://alpha/file:///alpha.txt";
    let read = ask(&gw, &session, 13, "resources/read", json!({"uri": uri})).await;
    assert_eq!(read.result(13)["contents"][0]["uri"], uri);
    assert_eq!(
        alpha.params("resources/read")[0]["uri"],
        "file:///alpha.txt"
    );
    let sub = ask(
        &gw,
        &session,
        14,
        "resources/subscribe",
        json!({"uri": uri}),
    )
    .await;
    assert!(sub.response(14).get("result").is_some());
    let lost = ask(
        &gw,
        &session,
        15,
        "resources/read",
        json!({"uri": "file:///x"}),
    )
    .await;
    assert_eq!(lost.response(15)["error"]["code"], -32602);
    let ping = ask(&gw, &session, 16, "ping", json!({})).await;
    assert_eq!(ping.result(16), &json!({}));
    let odd = ask(&gw, &session, 17, "completion/complete", json!({})).await;
    assert_eq!(odd.response(17)["error"]["code"], -32601);

    // The servers' notifications, merged: their lists' changes and updates,
    // a resource's URI prefixed; not a progress report with no request.
    let stream = call(&gw, "GET", Some(&session), None, Some(KEY)).await;
    let methods: Vec<&str> = stream
        .messages
        .iter()
        .map(|m| m["method"].as_str().unwrap())
        .collect();
    assert_eq!(
        methods
            .iter()
            .filter(|m| **m == "notifications/tools/list_changed")
            .count(),
        2,
        "{methods:?}"
    );
    assert!(!methods.contains(&"notifications/progress"), "{methods:?}");
    let updated: Vec<&Value> = stream
        .messages
        .iter()
        .filter(|m| m["method"] == "notifications/resources/updated")
        .collect();
    let mut uris: Vec<&str> = updated
        .iter()
        .map(|m| m["params"]["uri"].as_str().unwrap())
        .collect();
    uris.sort_unstable();
    assert_eq!(
        uris,
        [
            "mcp://alpha/file:///alpha.txt",
            "mcp://beta/file:///beta.txt"
        ]
    );

    // A cancellation reaches the servers.
    let cancel = json!({"jsonrpc": "2.0", "method": "notifications/cancelled",
                        "params": {"requestId": 5}});
    let cancelled = call(&gw, "POST", Some(&session), Some(cancel), Some(KEY)).await;
    assert_eq!(cancelled.status, 202);
    assert_eq!(alpha.params("notifications/cancelled").len(), 1);
    assert_eq!(beta.params("notifications/cancelled").len(), 1);

    // The session belongs to its key; a stranger and a missing id are told so.
    let other = call(&gw, "GET", Some(&session), None, Some(OTHER)).await;
    assert_eq!(other.status, 404);
    let none = call(&gw, "GET", None, None, Some(KEY)).await;
    assert_eq!(none.status, 400);

    // The servers got the gateway's own sessions, with their ids.
    for fake in [&alpha, &beta] {
        let sent = fake.sent();
        let post = |s: &&Sent| s.method == "POST" && s.body["method"] != "initialize";
        assert!(sent.iter().filter(post).all(|s| s.session.is_some()));
        assert_eq!(fake.methods()[0], "initialize");
    }

    // The end of the session ends theirs.
    let ended = call(&gw, "DELETE", Some(&session), None, Some(KEY)).await;
    assert_eq!(ended.status, 204);
    for fake in [&alpha, &beta] {
        assert!(fake.sent().iter().any(|s| s.method == "DELETE"));
    }
    let gone = ask(&gw, &session, 18, "ping", json!({})).await;
    assert_eq!(gone.status, 404);

    // The log has the request and the server it was routed to.
    let r = record(&gw, "tools/call beta__echo").await;
    assert_eq!(
        (r["kind"].as_str(), r["provider"].as_str()),
        (Some("mcp"), Some("beta"))
    );
    let r = record(&gw, "resources/read mcp://alpha/file:///alpha.txt").await;
    assert_eq!(r["provider"], "alpha");
}

#[tokio::test]
async fn a_client_keys_tools_limit_what_the_aggregate_lists_and_calls() {
    let alpha = Fake::start("alpha", false, None).await;
    let beta = Fake::start("beta", false, None).await;
    let config = format!(
        "client_keys = [{{ name = \"wide\", key = \"{KEY}\" }}, {{ name = \"narrow\", key = \"{OTHER}\", tools = [\"beta__*\"] }}]\nmcp = {{ alpha = {{ url = \"{}\" }}, beta = {{ url = \"{}\" }} }}\nplugins = {{ mcp_aggregate = {{  }} }}\n",
        alpha.url(),
        beta.url()
    );
    let gw = Gateway::start(&config).await;

    let names = |a: &Answer, id: u64| -> Vec<String> {
        let tools = a.result(id)["tools"].as_array().unwrap();
        tools
            .iter()
            .map(|t| t["name"].as_str().unwrap().to_owned())
            .collect()
    };
    let as_key = |key: &'static str, session: String, id: u64, method: &'static str, params| {
        let gw = &gw;
        async move {
            let body = request(id, method, params);
            call(gw, "POST", Some(&session), Some(body), Some(key)).await
        }
    };

    let (wide, _) = open(&gw, KEY).await;
    let (narrow, _) = open(&gw, OTHER).await;

    let all = as_key(KEY, wide.clone(), 2, "tools/list", json!({})).await;
    assert_eq!(
        names(&all, 2),
        ["alpha__echo", "alpha__secret", "beta__echo", "beta__secret"]
    );
    let some = as_key(OTHER, narrow.clone(), 2, "tools/list", json!({})).await;
    assert_eq!(names(&some, 2), ["beta__echo", "beta__secret"]);

    // A tool outside the scope is refused, as one that is denied.
    let echo = |server: &str| json!({"name": format!("{server}__echo"), "arguments": {}});
    let refused = as_key(OTHER, narrow.clone(), 3, "tools/call", echo("alpha")).await;
    assert!(
        refused.response(3).get("error").is_some(),
        "{:?}",
        refused.messages
    );
    assert!(alpha.params("tools/call").is_empty());
    let allowed = as_key(OTHER, narrow, 4, "tools/call", echo("beta")).await;
    assert!(allowed.response(4).get("result").is_some());
    let wide_call = as_key(KEY, wide, 5, "tools/call", echo("alpha")).await;
    assert!(wide_call.response(5).get("result").is_some());
}

#[tokio::test]
async fn a_reload_ends_the_old_builds_sessions_with_the_servers() {
    let alpha = Fake::start("alpha", false, None).await;
    let beta = Fake::start("beta", false, None).await;
    let config = aggregate_config(&alpha, &beta, "plugins = { mcp_aggregate = {  } }\n");
    let path = support::write_config(&format!("listen = \"127.0.0.1:0\"\n{config}"));
    let gw = Gateway::run(&["--config", path.to_str().unwrap()]).await;

    let (session, _) = open(&gw, KEY).await;
    for fake in [&alpha, &beta] {
        assert!(!fake.sent().iter().any(|s| s.method == "DELETE"));
    }

    let reloaded = client()
        .post(gw.url("/ui/api/reload"))
        .header("authorization", format!("Bearer {KEY}"))
        .send()
        .await
        .unwrap();
    assert_eq!(reloaded.status(), 200);

    // The servers' sessions end, without a word from the client.
    for _ in 0..200 {
        if [&alpha, &beta]
            .iter()
            .all(|f| f.sent().iter().any(|s| s.method == "DELETE"))
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    for fake in [&alpha, &beta] {
        let ended = fake.sent().into_iter().find(|s| s.method == "DELETE");
        let ended = ended.expect("the session was deleted");
        assert!(ended.session.is_some());
    }

    // The new build has no such session; the client opens another.
    let gone = ask(&gw, &session, 2, "ping", json!({})).await;
    assert_eq!(gone.status, 404);
    let (again, _) = open(&gw, KEY).await;
    assert_ne!(again, session);
}

#[tokio::test]
async fn the_aggregate_names_a_subset_and_refuses_what_it_cannot_front() {
    let alpha = Fake::start("alpha", false, None).await;
    let beta = Fake::start("beta", false, None).await;
    let config = aggregate_config(
        &alpha,
        &beta,
        "plugins = { mcp_aggregate = { servers = [\"beta\"] } }\n",
    );
    let gw = Gateway::start(&config).await;
    let (session, _) = open(&gw, KEY).await;
    let list = ask(&gw, &session, 2, "tools/list", json!({})).await;
    let tools = list.result(2)["tools"].as_array().unwrap();
    assert!(
        tools
            .iter()
            .all(|t| t["name"].as_str().unwrap().starts_with("beta__"))
    );
    assert!(
        alpha.sent().is_empty(),
        "a server not fronted is not reached"
    );
    // Each server is still at its own path.
    let one = client()
        .post(gw.url("/mcp/alpha"))
        .header("authorization", format!("Bearer {KEY}"))
        .header("accept", "application/json, text/event-stream")
        .header("content-type", "application/json")
        .body(request(1, "initialize", json!({"protocolVersion": PROTOCOL})).to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(one.status(), 200);

    for (extra, said) in [
        (
            "plugins = { mcp_aggregate = { servers = [\"nope\"] } }\n",
            "unknown mcp server 'nope'",
        ),
        (
            "plugins = { mcp_aggregate = { servers = [\"alpha\", \"alpha\"] } }\n",
            "listed twice",
        ),
        (
            "plugins = { mcp_aggregate = { deny = [\"\"] } }\n",
            "pattern",
        ),
    ] {
        let config = aggregate_config(&alpha, &beta, extra);
        let path = support::write_config(&format!("listen = \"127.0.0.1:0\"\n{config}"));
        let out = std::process::Command::new(env!("CARGO_BIN_EXE_respan-gateway"))
            .args(["check", "--config", path.to_str().unwrap()])
            .output()
            .unwrap();
        let text = String::from_utf8_lossy(&out.stdout).to_string()
            + &String::from_utf8_lossy(&out.stderr);
        assert!(
            !out.status.success() && text.contains(said),
            "{extra}: {text}"
        );
    }
}

// --- OAuth ---

#[tokio::test]
async fn client_credentials_get_a_token_and_a_refusal_gets_a_new_one() {
    let tokens = Tokens::start().await;
    let up = Fake::start("beta", false, Some("tok-1")).await;
    let config = format!(
        "client_keys = [\"{KEY}\"]\nmcp = {{ beta = {{ url = \"{}\", auth = {{ oauth = {{ token_url = \"{}\", client_id = \"the-id\", client_secret = \"s3cret & more\", scopes = [\"read\", \"write\"] }} }} }} }}\n",
        up.url(),
        tokens.url
    );
    let gw = Gateway::start(&config).await;
    let one = |id: u64, session: &str, method: &str, params: Value| {
        let body = request(id, method, params);
        let session = session.to_owned();
        let gw = &gw;
        async move {
            let req = client()
                .post(gw.url("/mcp/beta"))
                .header("authorization", format!("Bearer {KEY}"))
                .header("accept", "application/json, text/event-stream")
                .header("content-type", "application/json");
            let req = if session.is_empty() {
                req
            } else {
                req.header("mcp-session-id", session)
            };
            let resp = req.body(body.to_string()).send().await.unwrap();
            let status = resp.status().as_u16();
            let text = resp.text().await.unwrap();
            (status, text)
        }
    };
    let (status, _) = one(1, "", "initialize", json!({"protocolVersion": PROTOCOL})).await;
    assert_eq!(status, 200);
    let forms = tokens.forms();
    assert_eq!(forms.len(), 1);
    assert_eq!(field(&forms[0], "grant_type"), Some("client_credentials"));
    assert_eq!(field(&forms[0], "client_id"), Some("the-id"));
    assert_eq!(field(&forms[0], "client_secret"), Some("s3cret & more"));
    assert_eq!(field(&forms[0], "scope"), Some("read write"));
    assert!(field(&forms[0], "refresh_token").is_none());

    // The token is kept: more calls, one token.
    let (status, _) = one(2, "s-beta", "tools/list", json!({})).await;
    assert_eq!(status, 200);
    assert_eq!(tokens.forms().len(), 1);
    let sent = up.sent();
    assert!(
        sent.iter()
            .all(|s| s.authorization.as_deref() == Some("Bearer tok-1"))
    );

    // The server stops taking it: one 401, a new token, the call goes on.
    *up.wants.lock().unwrap() = Some("tok-2".into());
    let (status, text) = one(3, "s-beta", "tools/list", json!({})).await;
    assert_eq!(status, 200, "{text}");
    assert!(text.contains("echo"));
    assert_eq!(tokens.forms().len(), 2);
    let last = up.sent().last().unwrap().clone();
    assert_eq!(last.authorization.as_deref(), Some("Bearer tok-2"));

    // A server that refuses the new token too is not asked again for ever.
    *up.wants.lock().unwrap() = Some("never".into());
    let (status, _) = one(4, "s-beta", "tools/list", json!({})).await;
    assert_eq!(status, 401);
    assert_eq!(tokens.forms().len(), 3);

    // Neither the secret nor a token is in what the gateway printed.
    let printed = gw.stdout.lock().unwrap().join("\n");
    for secret in ["s3cret", "tok-1", "tok-2", "tok-3"] {
        assert!(!printed.contains(secret), "{printed}");
    }
}

#[tokio::test]
async fn a_refresh_token_is_used_and_its_rotation_kept() {
    let tokens = Tokens::start().await;
    let up = Fake::start("beta", false, Some("tok-1")).await;
    let config = format!(
        "client_keys = [\"{KEY}\"]\nmcp = {{ beta = {{ url = \"{}\", auth = {{ oauth = {{ token_url = \"{}\", client_id = \"the-id\", client_secret = \"env:OAUTH_TEST_SECRET\", refresh_token = \"env:OAUTH_TEST_REFRESH\" }} }} }} }}\n",
        up.url(),
        tokens.url
    );
    let path = support::write_config(&format!("listen = \"127.0.0.1:0\"\n{config}"));
    let env = [
        ("OAUTH_TEST_SECRET", "from-env"),
        ("OAUTH_TEST_REFRESH", "refresh-0"),
    ];
    let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"));
    let gw = Gateway::run_with(&["--config", path.to_str().unwrap()], dir, &env).await;
    let post = |id: u64| {
        let body = request(id, "initialize", json!({"protocolVersion": PROTOCOL}));
        let req = client()
            .post(gw.url("/mcp/beta"))
            .header("authorization", format!("Bearer {KEY}"))
            .header("accept", "application/json, text/event-stream")
            .header("content-type", "application/json");
        async move {
            req.body(body.to_string())
                .send()
                .await
                .unwrap()
                .status()
                .as_u16()
        }
    };
    assert_eq!(post(1).await, 200);
    let forms = tokens.forms();
    assert_eq!(field(&forms[0], "grant_type"), Some("refresh_token"));
    assert_eq!(field(&forms[0], "refresh_token"), Some("refresh-0"));
    assert_eq!(field(&forms[0], "client_secret"), Some("from-env"));

    // The next token is asked for with the refresh token the last one gave.
    *up.wants.lock().unwrap() = Some("tok-2".into());
    assert_eq!(post(2).await, 200);
    let forms = tokens.forms();
    assert_eq!(forms.len(), 2);
    assert_eq!(field(&forms[1], "refresh_token"), Some("refresh-1"));
}

#[tokio::test]
async fn oauth_mistakes_are_named_at_load() {
    let check = |extra: &str| {
        let config = format!("[mcp.m]\nurl = \"http://127.0.0.1:1/mcp\"\n{extra}");
        let path = support::write_config(&format!("listen = \"127.0.0.1:0\"\n{config}"));
        let out = std::process::Command::new(env!("CARGO_BIN_EXE_respan-gateway"))
            .args(["check", "--config", path.to_str().unwrap()])
            .env_remove("NO_SUCH_OAUTH_VAR")
            .output()
            .unwrap();
        let text = String::from_utf8_lossy(&out.stdout).to_string()
            + &String::from_utf8_lossy(&out.stderr);
        (out.status.success(), text)
    };
    let client =
        "{ token_url = \"http://127.0.0.1:1/t\", client_id = \"i\", client_secret = \"s\" }";
    let (ok, text) = check(&format!(
        "headers = {{ Authorization = \"k\" }}\nauth = {{ oauth = {client} }}\n"
    ));
    assert!(!ok && text.contains("set one"), "{text}");
    let (ok, text) = check(&format!("auth = {{ bearer = \"k\", oauth = {client} }}\n"));
    assert!(!ok && text.contains("not also"), "{text}");
    let (ok, text) = check(&format!("oauth = {client}\n"));
    assert!(
        !ok && text.contains("is now `auth = { oauth = { .. } }`"),
        "{text}"
    );
    let missing = "auth = { oauth = { token_url = \"http://127.0.0.1:1/t\", client_id = \"i\", client_secret = \"env:NO_SUCH_OAUTH_VAR\" } }\n";
    let (ok, text) = check(missing);
    assert!(!ok && text.contains("NO_SUCH_OAUTH_VAR"), "{text}");
}
