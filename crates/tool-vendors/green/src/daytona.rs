//! Daytona (<https://www.daytona.io>): code sandboxes. Its API at
//! `https://app.daytona.io/api` creates, lists and deletes them
//! (`POST /sandbox`, `GET /sandbox`, `GET` and `DELETE /sandbox/{id}`,
//! `POST /sandbox/{id}/start`); each sandbox's toolbox, at the
//! `toolboxProxyUrl` the sandbox names followed by its id, runs commands
//! and code and takes files (`POST /process/execute`,
//! `POST /process/code-run`, `POST /files/upload`). The key is a bearer
//! token to both. A toolbox call first waits for its sandbox to be
//! started, starting a stopped one.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use respan_gateway_tool_vendor_kit::args::{
    object, optional_bool, optional_enum, optional_int, optional_str, optional_str_list,
    required_str,
};
use respan_gateway_tool_vendor_kit::reqwest::{Method, multipart};
use respan_gateway_tool_vendor_kit::{
    Api, Content, KeyIn, ToolError, ToolOutput, ToolSpec, ToolVendor, Tools, decode_base64,
};
use serde_json::{Map, Value, json};

/// Daytona.
pub const VENDOR: ToolVendor = ToolVendor {
    id: "daytona",
    key_env: &["DAYTONA_API_KEY"],
    key_in: KeyIn::Bearer,
    base_url: "https://app.daytona.io/api",
    tools: &DaytonaTools,
};

/// Daytona's tools: `create_sandbox`, `list_sandboxes`, `delete_sandbox`,
/// `run_command`, `run_code` and `write_file`.
#[derive(Debug, Clone, Copy)]
pub struct DaytonaTools;

const LANGUAGES: &[&str] = &["python", "javascript", "typescript"];

/// The label Daytona's SDKs keep a sandbox's code language in.
const LANGUAGE_LABEL: &str = "code-toolbox-language";

/// The longest a command may run: the kit's client gives up at 120 s.
const MAX_TIMEOUT: u64 = 110;

/// How long a toolbox call waits for its sandbox to start.
const START_WAIT: Duration = Duration::from_secs(60);

/// How often a starting sandbox is looked at.
const POLL: Duration = Duration::from_millis(500);

/// The most of a command's output a result quotes: its head and its tail.
const HEAD: usize = 4_000;
const TAIL: usize = 16_000;

/// States a sandbox passes through on its way to `started` or `stopped`.
const MOVING: &[&str] = &[
    "creating",
    "restoring",
    "starting",
    "stopping",
    "pausing",
    "resuming",
    "archiving",
    "resizing",
    "snapshotting",
    "forking",
    "pending_build",
    "building_snapshot",
    "pulling_snapshot",
];

/// States `POST /sandbox/{id}/start` brings back.
const RESTARTABLE: &[&str] = &["stopped", "archived", "paused"];

const TOOLS: &[ToolSpec] = &[
    ToolSpec {
        name: "create_sandbox",
        description: "Create a Daytona sandbox, an isolated Linux machine (1 vCPU, 1 GiB RAM, \
            3 GiB disk by default) to run commands and code in, and wait until it has started. \
            Returns its id, for the other tools. It bills the Daytona account per second while \
            it runs; by default it stops after 15 idle minutes and is deleted as soon as it \
            stops. Delete it with `delete_sandbox` when done.",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "language": {"type": "string", "enum": ["python", "javascript", "typescript"], "default": "python", "description": "The language `run_code` runs in it by default."},
    "snapshot": {"type": "string", "description": "The snapshot (image) to create it from, by name or id. Daytona's default when omitted."},
    "name": {"type": "string", "description": "A name for it, unique in the organization. Its id when omitted."},
    "env": {"type": "object", "additionalProperties": {"type": "string"}, "description": "Environment variables set in it."},
    "auto_stop_minutes": {"type": "integer", "minimum": 0, "maximum": 1440, "default": 15, "description": "Stop it after this many minutes without activity; 0 never stops it, so it bills until deleted."},
    "auto_delete_minutes": {"type": "integer", "minimum": 0, "maximum": 43200, "default": 0, "description": "Delete it this many minutes after it stops; 0 deletes it as soon as it stops. A stopped sandbox keeps its disk until then."},
    "network_block_all": {"type": "boolean", "default": false, "description": "Block all of its network access."}
  }
}"#,
    },
    ToolSpec {
        name: "list_sandboxes",
        description: "List the Daytona account's sandboxes: id, name, state, snapshot, \
            resources and creation time, newest first. Use it to find ones left running, \
            which keep billing.",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "limit": {"type": "integer", "minimum": 1, "maximum": 200, "default": 50, "description": "At most this many."}
  }
}"#,
    },
    ToolSpec {
        name: "delete_sandbox",
        description: "Delete a Daytona sandbox and everything in it, for good. It stops \
            billing.",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "sandbox": {"type": "string", "description": "The sandbox's id or name."}
  },
  "required": ["sandbox"]
}"#,
    },
    ToolSpec {
        name: "run_command",
        description: "Run a shell command in a Daytona sandbox and get its exit code and \
            output (stdout and stderr together). Starts the sandbox first if it has stopped. \
            The call waits for the command, at most 110 seconds: start longer jobs in the \
            background (`nohup … > log 2>&1 &`) and check on them later. Read files with `cat`.",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "sandbox": {"type": "string", "description": "The sandbox's id or name."},
    "command": {"type": "string", "description": "The command, run by the sandbox's shell, e.g. \"pip install requests && python main.py\"."},
    "cwd": {"type": "string", "description": "The directory to run it in. The sandbox's working directory when omitted."},
    "env": {"type": "object", "additionalProperties": {"type": "string"}, "description": "Environment variables for this command."},
    "timeout_seconds": {"type": "integer", "minimum": 1, "maximum": 110, "default": 60, "description": "Kill it after this many seconds."}
  },
  "required": ["sandbox", "command"]
}"#,
    },
    ToolSpec {
        name: "run_code",
        description: "Run a Python, JavaScript or TypeScript program in a Daytona sandbox and \
            get its exit code and output. Charts matplotlib draws come back as images. Starts \
            the sandbox first if it has stopped. The call waits at most 110 seconds.",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "sandbox": {"type": "string", "description": "The sandbox's id or name."},
    "code": {"type": "string", "description": "The program's source."},
    "language": {"type": "string", "enum": ["python", "javascript", "typescript"], "description": "Its language: the sandbox's own (set when it was created), else python."},
    "argv": {"type": "array", "items": {"type": "string"}, "description": "Command-line arguments for it."},
    "env": {"type": "object", "additionalProperties": {"type": "string"}, "description": "Environment variables for it."},
    "timeout_seconds": {"type": "integer", "minimum": 1, "maximum": 110, "default": 60, "description": "Kill it after this many seconds."}
  },
  "required": ["sandbox", "code"]
}"#,
    },
    ToolSpec {
        name: "write_file",
        description: "Write a text file in a Daytona sandbox, replacing any file there. \
            Starts the sandbox first if it has stopped.",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "sandbox": {"type": "string", "description": "The sandbox's id or name."},
    "path": {"type": "string", "description": "Where to write it: absolute, or relative to the sandbox's working directory."},
    "content": {"type": "string", "description": "The file's content."}
  },
  "required": ["sandbox", "path", "content"]
}"#,
    },
];

#[async_trait]
impl Tools for DaytonaTools {
    fn list(&self) -> &'static [ToolSpec] {
        TOOLS
    }

    async fn call(&self, api: &Api, name: &str, arguments: Value) -> Result<ToolOutput, ToolError> {
        let args = object(&arguments)?;
        match name {
            "create_sandbox" => {
                let body = create_body(args)?;
                let since = Instant::now();
                let created = api.post_json("/sandbox", &body).await?;
                let sandbox = started(api, sandbox_id(&created)?).await?;
                Ok(timed(render_sandbox(&sandbox).into(), since))
            }
            "list_sandboxes" => {
                let limit = optional_int(args, "limit", 1, 200)?.unwrap_or(50);
                let query = [
                    ("limit", limit.to_string()),
                    ("sort", "createdAt".to_owned()),
                    ("order", "desc".to_owned()),
                ];
                Ok(render_list(&api.get_json("/sandbox", &query).await?).into())
            }
            "delete_sandbox" => {
                let id = sandbox_arg(args)?;
                let req = api.request(Method::DELETE, &format!("/sandbox/{id}"));
                api.json(req).await?;
                Ok(format!("Deleted sandbox {id}.").into())
            }
            "run_command" => {
                let body = execute_body(args)?;
                let since = Instant::now();
                let sandbox = started(api, sandbox_arg(args)?).await?;
                let url = toolbox_url(&sandbox, "/process/execute")?;
                let text = render_execute(&api.post_json(&url, &body).await?)?;
                Ok(timed(text.into(), since))
            }
            "run_code" => {
                let id = sandbox_arg(args)?;
                let since = Instant::now();
                let sandbox = started(api, id).await?;
                let body = code_body(args, sandbox_language(&sandbox))?;
                let url = toolbox_url(&sandbox, "/process/code-run")?;
                Ok(timed(
                    render_code(&api.post_json(&url, &body).await?),
                    since,
                ))
            }
            "write_file" => {
                let path = required_str(args, "path")?;
                let content = match args.get("content") {
                    Some(Value::String(s)) => s.clone(),
                    None | Some(Value::Null) => {
                        return Err(ToolError::new("`content` is required"));
                    }
                    Some(_) => return Err(ToolError::new("`content` must be a string")),
                };
                let since = Instant::now();
                let sandbox = started(api, sandbox_arg(args)?).await?;
                let url = toolbox_url(&sandbox, "/files/upload")?;
                let bytes = content.len();
                let file_name = path.rsplit('/').next().unwrap_or(path).to_owned();
                let form = multipart::Form::new().part(
                    "file",
                    multipart::Part::bytes(content.into_bytes()).file_name(file_name),
                );
                let req = api
                    .request(Method::POST, &url)
                    .query(&[("path", path)])
                    .multipart(form);
                api.json(req).await?;
                Ok(timed(
                    format!("Wrote {bytes} bytes to {path}.").into(),
                    since,
                ))
            }
            _ => Err(ToolError::new(format!("no tool named {name:?}"))),
        }
    }
}

/// `out`, with the `seconds` since `since`: Daytona bills a sandbox per
/// second while it is creating, starting or started
/// (<https://www.daytona.io/docs/en/billing/>) and its answers report no
/// usage, so a call is timed from its first request to the sandbox until
/// its answer; the sandbox's life outside calls bills outside them.
fn timed(out: ToolOutput, since: Instant) -> ToolOutput {
    out.with_usage("seconds", since.elapsed().as_secs_f64())
}

/// The `sandbox` argument, an id or name safe in a URL path.
fn sandbox_arg(args: &Map<String, Value>) -> Result<&str, ToolError> {
    let id = required_str(args, "sandbox")?.trim();
    if id
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        && !id.starts_with('.')
    {
        Ok(id)
    } else {
        Err(ToolError::new(format!(
            "`sandbox` must be a sandbox's id or name, not {id:?}"
        )))
    }
}

/// The object of strings `key`, if there and not empty.
fn optional_env(args: &Map<String, Value>, key: &str) -> Result<Option<Value>, ToolError> {
    let invalid = || ToolError::new(format!("`{key}` must be an object of strings"));
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Object(map)) if map.values().all(Value::is_string) => {
            Ok((!map.is_empty()).then(|| Value::Object(map.clone())))
        }
        Some(_) => Err(invalid()),
    }
}

/// `POST /sandbox`'s body from the tool's arguments.
fn create_body(args: &Map<String, Value>) -> Result<Value, ToolError> {
    let language = optional_enum(args, "language", LANGUAGES)?.unwrap_or("python");
    let mut body = Map::new();
    for (arg, field) in [("snapshot", "snapshot"), ("name", "name")] {
        if let Some(v) = optional_str(args, arg)? {
            body.insert(field.into(), v.into());
        }
    }
    if let Some(env) = optional_env(args, "env")? {
        body.insert("env".into(), env);
    }
    body.insert("labels".into(), json!({ LANGUAGE_LABEL: language }));
    let stop = optional_int(args, "auto_stop_minutes", 0, 1440)?.unwrap_or(15);
    body.insert("autoStopInterval".into(), stop.into());
    let delete = optional_int(args, "auto_delete_minutes", 0, 43200)?.unwrap_or(0);
    body.insert("autoDeleteInterval".into(), delete.into());
    if let Some(v) = optional_bool(args, "network_block_all")? {
        body.insert("networkBlockAll".into(), v.into());
    }
    Ok(Value::Object(body))
}

/// The toolbox's `/process/execute` body from the tool's arguments.
fn execute_body(args: &Map<String, Value>) -> Result<Value, ToolError> {
    let mut body = Map::new();
    body.insert("command".into(), required_str(args, "command")?.into());
    if let Some(v) = optional_str(args, "cwd")? {
        body.insert("cwd".into(), v.into());
    }
    if let Some(env) = optional_env(args, "env")? {
        body.insert("envs".into(), env);
    }
    let timeout = optional_int(args, "timeout_seconds", 1, MAX_TIMEOUT)?.unwrap_or(60);
    body.insert("timeout".into(), timeout.into());
    Ok(Value::Object(body))
}

/// The toolbox's `/process/code-run` body from the tool's arguments, in
/// `default_language` unless they name one.
fn code_body(args: &Map<String, Value>, default_language: &str) -> Result<Value, ToolError> {
    let mut body = Map::new();
    body.insert("code".into(), required_str(args, "code")?.into());
    let language = optional_enum(args, "language", LANGUAGES)?.unwrap_or(default_language);
    body.insert("language".into(), language.into());
    if let Some(argv) = optional_str_list(args, "argv")? {
        body.insert("argv".into(), json!(argv));
    }
    if let Some(env) = optional_env(args, "env")? {
        body.insert("envs".into(), env);
    }
    let timeout = optional_int(args, "timeout_seconds", 1, MAX_TIMEOUT)?.unwrap_or(60);
    body.insert("timeout".into(), timeout.into());
    Ok(Value::Object(body))
}

fn str_of<'a>(v: &'a Value, key: &str) -> Option<&'a str> {
    v.get(key).and_then(Value::as_str).filter(|s| !s.is_empty())
}

fn sandbox_id(sandbox: &Value) -> Result<&str, ToolError> {
    str_of(sandbox, "id").ok_or_else(|| ToolError::new("Daytona answered with no sandbox id"))
}

/// The language a sandbox was created for, else python.
fn sandbox_language(sandbox: &Value) -> &str {
    sandbox
        .get("labels")
        .and_then(|l| str_of(l, LANGUAGE_LABEL))
        .filter(|l| LANGUAGES.contains(l))
        .unwrap_or("python")
}

/// The sandbox `id` once started: started if stopped, waited for while it
/// moves, looked at every [`POLL`].
async fn started(api: &Api, id: &str) -> Result<Value, ToolError> {
    let path = format!("/sandbox/{id}");
    let since = Instant::now();
    let mut sandbox = api.get_json(&path, &[]).await?;
    let mut restarted = false;
    loop {
        let state = str_of(&sandbox, "state").unwrap_or("unknown");
        match state {
            "started" => return Ok(sandbox),
            s if RESTARTABLE.contains(&s) && !restarted => {
                restarted = true;
                let req = api.request(Method::POST, &format!("{path}/start"));
                api.json(req).await?;
            }
            s if MOVING.contains(&s) || RESTARTABLE.contains(&s) || s == "unknown" => {}
            s => {
                let reason = str_of(&sandbox, "errorReason").unwrap_or("no reason given");
                return Err(ToolError::new(format!(
                    "sandbox {id} is {s}, not started: {reason}"
                )));
            }
        }
        if since.elapsed() > START_WAIT {
            return Err(ToolError::new(format!(
                "sandbox {id} is still {state} after {} s; try again shortly",
                START_WAIT.as_secs()
            )));
        }
        tokio::time::sleep(POLL).await;
        sandbox = api.get_json(&path, &[]).await?;
    }
}

/// `path` on the sandbox's toolbox.
fn toolbox_url(sandbox: &Value, path: &str) -> Result<String, ToolError> {
    let proxy = str_of(sandbox, "toolboxProxyUrl")
        .ok_or_else(|| ToolError::new("Daytona named no toolbox URL for the sandbox"))?;
    Ok(format!(
        "{}/{}{path}",
        proxy.trim_end_matches('/'),
        sandbox_id(sandbox)?
    ))
}

/// `text` with its middle cut when longer than [`HEAD`] and [`TAIL`].
fn cap(text: &str) -> String {
    if text.len() <= HEAD + TAIL {
        return text.to_owned();
    }
    let mut head = HEAD;
    while !text.is_char_boundary(head) {
        head -= 1;
    }
    let mut tail = text.len() - TAIL;
    while !text.is_char_boundary(tail) {
        tail += 1;
    }
    format!(
        "{}\n… ({} bytes cut) …\n{}",
        &text[..head],
        tail - head,
        &text[tail..]
    )
}

fn minutes(n: i64) -> String {
    if n == 1 {
        "1 minute".into()
    } else {
        format!("{n} minutes")
    }
}

/// A sandbox: its id, state, size and when it stops and goes.
fn render_sandbox(s: &Value) -> String {
    let id = str_of(s, "id").unwrap_or("?");
    let mut out = format!("Sandbox {id}");
    if let Some(name) = str_of(s, "name").filter(|n| *n != id) {
        out.push_str(&format!(" ({name})"));
    }
    out.push_str(&format!(
        " is {}.",
        str_of(s, "state").unwrap_or("in an unknown state")
    ));
    let int = |k: &str| s.get(k).and_then(Value::as_f64).map(|n| n as i64);
    if let (Some(cpu), Some(mem), Some(disk)) = (int("cpu"), int("memory"), int("disk")) {
        out.push_str(&format!("\n{cpu} vCPU, {mem} GiB RAM, {disk} GiB disk"));
        if let Some(snapshot) = str_of(s, "snapshot") {
            out.push_str(&format!(", from {snapshot}"));
        }
        out.push('.');
    }
    out.push_str(&format!("\nLanguage: {}.", sandbox_language(s)));
    match int("autoStopInterval") {
        Some(0) => out.push_str("\nIt never stops by itself: it bills until deleted."),
        Some(n) => out.push_str(&format!("\nIt stops after {} idle.", minutes(n))),
        None => {}
    }
    match int("autoDeleteInterval") {
        Some(n) if n < 0 => out.push_str(" Once stopped it is kept, with its disk."),
        Some(0) => out.push_str(" It is deleted as soon as it stops."),
        Some(n) => out.push_str(&format!(" It is deleted {} after it stops.", minutes(n))),
        None => {}
    }
    out
}

/// One line per sandbox.
fn render_list(response: &Value) -> String {
    let items = response
        .get("items")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    if items.is_empty() {
        return "No sandboxes.".into();
    }
    let mut lines: Vec<String> = items
        .iter()
        .map(|s| {
            let id = str_of(s, "id").unwrap_or("?");
            let mut line = format!("- {id}");
            if let Some(name) = str_of(s, "name").filter(|n| *n != id) {
                line.push_str(&format!(" ({name})"));
            }
            line.push_str(&format!(": {}", str_of(s, "state").unwrap_or("unknown")));
            let n = |k: &str| s.get(k).and_then(Value::as_f64).map(|n| n as i64);
            if let (Some(cpu), Some(mem), Some(disk)) = (n("cpu"), n("memory"), n("disk")) {
                line.push_str(&format!(", {cpu} vCPU / {mem} GiB / {disk} GiB"));
            }
            if let Some(snapshot) = str_of(s, "snapshot") {
                line.push_str(&format!(", {snapshot}"));
            }
            if let Some(created) = str_of(s, "createdAt") {
                line.push_str(&format!(", created {created}"));
            }
            line
        })
        .collect();
    if str_of(response, "nextCursor").is_some() {
        lines.push("(more not shown)".into());
    }
    lines.join("\n")
}

/// A command's exit code and output; a command that could not start is a
/// [`ToolError`].
fn render_execute(response: &Value) -> Result<String, ToolError> {
    let output = str_of(response, "result").unwrap_or_default();
    match response.get("exitCode").and_then(Value::as_i64) {
        Some(-1) if !output.is_empty() => Err(ToolError::new(format!(
            "the command failed to run: {}",
            cap(output.trim_end())
        ))),
        code => Ok(render_run(code.unwrap_or(0), output)),
    }
}

fn render_run(code: i64, output: &str) -> String {
    let output = output.trim_end();
    if output.is_empty() {
        format!("Exit code {code}. No output.")
    } else {
        format!("Exit code {code}. Output:\n{}", cap(output))
    }
}

/// A program's exit code and output, then each chart: its title as text,
/// its PNG as an image.
fn render_code(response: &Value) -> ToolOutput {
    let code = response
        .get("exitCode")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let mut text = render_run(code, str_of(response, "result").unwrap_or_default());
    let charts = response
        .pointer("/artifacts/charts")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    let mut images = Vec::new();
    for (i, chart) in charts.iter().enumerate() {
        text.push_str(&format!(
            "\nChart {}: {}",
            i + 1,
            str_of(chart, "title").unwrap_or("untitled")
        ));
        if let Some(kind) = str_of(chart, "type") {
            text.push_str(&format!(" ({kind})"));
        }
        if let Some(png) = str_of(chart, "png").and_then(decode_base64) {
            images.push(Content::Image {
                data: png,
                mime_type: "image/png".into(),
            });
        }
    }
    let mut blocks = vec![Content::Text(text)];
    blocks.extend(images);
    blocks.into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(v: Value) -> Map<String, Value> {
        match v {
            Value::Object(m) => m,
            _ => Map::new(),
        }
    }

    #[test]
    fn create_body_defaults_to_an_ephemeral_sandbox() {
        assert_eq!(
            create_body(&args(json!({}))).expect("valid"),
            json!({
                "labels": {"code-toolbox-language": "python"},
                "autoStopInterval": 15, "autoDeleteInterval": 0
            })
        );
        assert_eq!(
            create_body(&args(json!({
                "language": "typescript", "snapshot": "daytona-small", "env": {"A": "1"},
                "auto_stop_minutes": 0, "auto_delete_minutes": 60, "network_block_all": true
            })))
            .expect("valid"),
            json!({
                "snapshot": "daytona-small", "env": {"A": "1"},
                "labels": {"code-toolbox-language": "typescript"},
                "autoStopInterval": 0, "autoDeleteInterval": 60, "networkBlockAll": true
            })
        );
    }

    #[test]
    fn create_body_rejects_bad_arguments() {
        assert!(create_body(&args(json!({"language": "ruby"}))).is_err());
        assert!(create_body(&args(json!({"env": {"A": 1}}))).is_err());
        assert!(create_body(&args(json!({"env": ["A=1"]}))).is_err());
        assert!(create_body(&args(json!({"auto_stop_minutes": 2000}))).is_err());
    }

    #[test]
    fn sandbox_ids_are_path_safe() {
        assert_eq!(
            sandbox_arg(&args(json!({"sandbox": "a1-b_2.c"}))),
            Ok("a1-b_2.c")
        );
        assert!(sandbox_arg(&args(json!({"sandbox": "../api-keys"}))).is_err());
        assert!(sandbox_arg(&args(json!({"sandbox": "a/b"}))).is_err());
        assert!(sandbox_arg(&args(json!({"sandbox": "a?b"}))).is_err());
        assert!(sandbox_arg(&args(json!({}))).is_err());
    }

    #[test]
    fn execute_and_code_bodies_map_arguments() {
        assert_eq!(
            execute_body(&args(
                json!({"command": "ls", "cwd": "/tmp", "env": {"X": "y"}})
            ))
            .expect("valid"),
            json!({"command": "ls", "cwd": "/tmp", "envs": {"X": "y"}, "timeout": 60})
        );
        assert!(execute_body(&args(json!({"command": " "}))).is_err());
        assert!(execute_body(&args(json!({"command": "ls", "timeout_seconds": 111}))).is_err());
        assert_eq!(
            code_body(
                &args(json!({"code": "print(1)", "argv": ["a"], "timeout_seconds": 5})),
                "python"
            )
            .expect("valid"),
            json!({"code": "print(1)", "language": "python", "argv": ["a"], "timeout": 5})
        );
        assert_eq!(
            code_body(
                &args(json!({"code": "1", "language": "javascript"})),
                "python"
            )
            .expect("valid")["language"],
            "javascript"
        );
        assert!(code_body(&args(json!({"code": "1", "language": "go"})), "python").is_err());
        assert!(code_body(&args(json!({})), "python").is_err());
    }

    fn sandbox() -> Value {
        json!({
            "id": "sb-1", "organizationId": "org", "name": "sb-1", "snapshot": "daytona-small",
            "user": "daytona", "env": {}, "labels": {"code-toolbox-language": "typescript"},
            "public": false, "networkBlockAll": false, "kvm": false, "target": "us",
            "cpu": 1, "gpu": 0, "memory": 1, "disk": 3, "state": "started",
            "autoStopInterval": 15, "autoDeleteInterval": 0,
            "toolboxProxyUrl": "https://proxy.app.daytona.io/toolbox/"
        })
    }

    #[test]
    fn toolbox_urls_and_language_come_from_the_sandbox() {
        let s = sandbox();
        assert_eq!(
            toolbox_url(&s, "/process/execute").expect("url"),
            "https://proxy.app.daytona.io/toolbox/sb-1/process/execute"
        );
        assert_eq!(sandbox_language(&s), "typescript");
        assert_eq!(sandbox_language(&json!({"labels": {}})), "python");
        assert!(toolbox_url(&json!({"id": "x"}), "/files").is_err());
    }

    #[test]
    fn renders_sandboxes() {
        assert_eq!(
            render_sandbox(&sandbox()),
            "Sandbox sb-1 is started.\n1 vCPU, 1 GiB RAM, 3 GiB disk, from daytona-small.\n\
             Language: typescript.\nIt stops after 15 minutes idle. It is deleted as soon as it stops."
        );
        let list = render_list(&json!({
            "items": [sandbox(), {"id": "sb-2", "name": "web", "state": "stopped", "cpu": 2,
                "memory": 4, "disk": 10, "createdAt": "2026-10-01T12:00:00Z"}],
            "nextCursor": "c"
        }));
        assert_eq!(
            list,
            "- sb-1: started, 1 vCPU / 1 GiB / 3 GiB, daytona-small\n\
             - sb-2 (web): stopped, 2 vCPU / 4 GiB / 10 GiB, created 2026-10-01T12:00:00Z\n\
             (more not shown)"
        );
        assert_eq!(
            render_list(&json!({"items": [], "nextCursor": null})),
            "No sandboxes."
        );
    }

    #[test]
    fn renders_runs() {
        assert_eq!(
            render_execute(&json!({"exitCode": 2, "result": "ls: x: No such file\n"})),
            Ok("Exit code 2. Output:\nls: x: No such file".into())
        );
        assert_eq!(
            render_execute(&json!({"exitCode": 0, "result": ""})),
            Ok("Exit code 0. No output.".into())
        );
        assert!(render_execute(&json!({"exitCode": -1, "result": "exec: no shell"})).is_err());
        let long = format!("{}{}", "a".repeat(HEAD + 10), "b".repeat(TAIL + 10));
        let capped = cap(&long);
        assert!(capped.starts_with(&"a".repeat(HEAD)));
        assert!(capped.ends_with(&"b".repeat(TAIL)));
        assert!(capped.contains("(20 bytes cut)"));
    }

    #[test]
    fn calls_are_timed_in_seconds() {
        let since = Instant::now() - Duration::from_secs(2);
        let out = timed("x".into(), since);
        let seconds = out.usage.get("seconds").copied().unwrap_or_default();
        assert!((2.0..60.0).contains(&seconds), "{seconds}");
        assert_eq!(out.usage.len(), 1);
    }

    #[test]
    fn renders_code_with_charts() {
        let out = render_code(&json!({
            "exitCode": 0, "result": "done\n",
            "artifacts": {"charts": [{"type": "line", "title": "Sine", "png": "iVBORw0K", "elements": []}]}
        }));
        assert_eq!(
            out,
            ToolOutput::from(vec![
                Content::Text("Exit code 0. Output:\ndone\nChart 1: Sine (line)".into()),
                Content::Image {
                    data: vec![0x89, b'P', b'N', b'G', 0x0d, 0x0a],
                    mime_type: "image/png".into()
                },
            ])
        );
    }
}
