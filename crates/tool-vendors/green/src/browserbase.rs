//! Browserbase (<https://www.browserbase.com>): web search
//! (`POST /search`), page fetches (`POST /fetch`), its browser agent
//! (`POST /agents/runs`, asynchronous: `GET /agents/runs/{id}` polls it,
//! `POST /agents/runs/{id}/stop` stops it) and a session's live view
//! (`GET /sessions/{id}/debug`), from its API at
//! `https://api.browserbase.com/v1`, the key in `X-BB-API-Key`. Driving a
//! session of one's own takes CDP, which a tool cannot speak, so there are
//! no tools for creating one; the project is the key's own.

use async_trait::async_trait;
use respan_gateway_tool_vendor_kit::args::{
    object, optional_bool, optional_enum, optional_int, optional_str, pretty, required_str,
};
use respan_gateway_tool_vendor_kit::{
    Api, KeyIn, ToolError, ToolOutput, ToolSpec, ToolVendor, Tools,
};
use serde_json::{Map, Value, json};

/// Browserbase.
pub const VENDOR: ToolVendor = ToolVendor {
    id: "browserbase",
    key_env: &["BROWSERBASE_API_KEY"],
    key_in: KeyIn::Header("X-BB-API-Key"),
    base_url: "https://api.browserbase.com/v1",
    tools: &BrowserbaseTools,
};

/// Browserbase's tools: `search`, `fetch`, `run_agent`, `get_agent_run`,
/// `stop_agent_run` and `live_view`.
#[derive(Debug, Clone, Copy)]
pub struct BrowserbaseTools;

/// The most of a fetched page's content a result carries, in bytes.
const CONTENT_CAP: usize = 100_000;

const TOOLS: &[ToolSpec] = &[
    ToolSpec {
        name: "search",
        description: "Search the web with Browserbase and get up to 25 results: title, URL, and \
            author and publication date when known (no snippets). Billed per search beyond the \
            plan's monthly allowance: about $5 per 1,000 by default, $1 in parallel's fast mode, \
            $7 with exa.",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "query": {"type": "string", "minLength": 1, "maxLength": 200, "description": "The search query."},
    "num_results": {"type": "integer", "minimum": 1, "maximum": 25, "default": 10, "description": "How many results."},
    "provider": {"type": "string", "enum": ["parallel", "exa"], "description": "The search provider; the project's default when omitted."},
    "mode": {"type": "string", "enum": ["fast", "advanced"], "description": "parallel's mode (advanced by default); only with provider parallel."}
  },
  "required": ["query"]
}"#,
    },
    ToolSpec {
        name: "fetch",
        description: "Fetch one URL through Browserbase without a browser (no JavaScript runs; \
            5 MB and 60 s at most) and get its content as markdown, as the raw body, or as JSON \
            extracted to a schema, with its HTTP status. Billed per call beyond the plan's \
            allowance; markdown and JSON (extraction) cost more than raw, and proxies more \
            again. For pages that need JavaScript or clicking, use `run_agent`.",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "url": {"type": "string", "description": "The http(s) URL to fetch."},
    "format": {"type": "string", "enum": ["markdown", "raw", "json"], "default": "markdown", "description": "markdown: the page as markdown; raw: the body unchanged (cheapest); json: data extracted to `schema`."},
    "schema": {"type": "object", "description": "A JSON Schema of the data to extract; required with format json, and only with it."},
    "follow_redirects": {"type": "boolean", "default": true, "description": "Follow HTTP redirects."},
    "proxies": {"type": "boolean", "default": false, "description": "Route through Browserbase's proxy network, for sites that block the request (costs more)."},
    "allow_insecure_ssl": {"type": "boolean", "default": false, "description": "Skip TLS certificate verification."}
  },
  "required": ["url"]
}"#,
    },
    ToolSpec {
        name: "run_agent",
        description: "Start Browserbase's browser agent on a task in plain language: it searches, \
            browses, clicks and fills forms in a browser session of its own, and returns a result \
            (JSON shaped by `result_schema` when given). Asynchronous: this returns the run's id \
            at once; a run takes tens of seconds to minutes, so poll `get_agent_run` for its \
            result. Each run is a billed agent call (the free plan has 3 a month) plus its \
            session's browser time.",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "task": {"type": "string", "minLength": 1, "description": "What to accomplish and what to return, e.g. \"Find the pricing page on example.com and list each plan's name and monthly price\"."},
    "result_schema": {"type": "object", "description": "A JSON Schema the run's result should conform to."},
    "agent_id": {"type": "string", "description": "Run a custom agent made in Browserbase, with its system prompt and result schema."},
    "verified": {"type": "boolean", "default": false, "description": "Use Browserbase Verified, for sites that block bots."},
    "proxies": {"type": "boolean", "default": false, "description": "Browse through Browserbase's proxy network."},
    "proxy_country": {"type": "string", "minLength": 2, "maxLength": 2, "description": "Browse through a Browserbase proxy in this country, an ISO 3166-1 alpha-2 code such as US or DE."}
  },
  "required": ["task"]
}"#,
    },
    ToolSpec {
        name: "get_agent_run",
        description: "Get a Browserbase agent run's status and, once it has completed, its \
            result; also the id of its browser session, for `live_view`. Free.",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "run_id": {"type": "string", "description": "The run's id, from `run_agent`."}
  },
  "required": ["run_id"]
}"#,
    },
    ToolSpec {
        name: "stop_agent_run",
        description: "Stop a Browserbase agent run that has not finished, ending its billing. \
            It winds down to STOPPED; a finished run cannot be stopped.",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "run_id": {"type": "string", "description": "The run's id, from `run_agent`."}
  },
  "required": ["run_id"]
}"#,
    },
    ToolSpec {
        name: "live_view",
        description: "Get live view URLs of a running Browserbase session, such as an agent \
            run's, for a person to watch (or take over) the browser in theirs: the whole \
            session's, and each open page's with its title and URL. Free.",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "session_id": {"type": "string", "description": "The session's id, such as an agent run's `session_id`."},
    "expires_in": {"type": "integer", "minimum": 60, "maximum": 21600, "description": "Seconds the URLs stay valid; by default as long as the session."}
  },
  "required": ["session_id"]
}"#,
    },
];

#[async_trait]
impl Tools for BrowserbaseTools {
    fn list(&self) -> &'static [ToolSpec] {
        TOOLS
    }

    async fn call(&self, api: &Api, name: &str, arguments: Value) -> Result<ToolOutput, ToolError> {
        let args = object(&arguments)?;
        let output: ToolOutput = match name {
            "search" => {
                let body = search_body(args)?;
                Ok(render_search(&api.post_json("/search", &body).await?).into())
            }
            "fetch" => {
                let body = fetch_body(args)?;
                Ok(render_fetch(&api.post_json("/fetch", &body).await?).into())
            }
            "run_agent" => {
                let body = run_body(args)?;
                render_run(&api.post_json("/agents/runs", &body).await?).map(Into::into)
            }
            "get_agent_run" => {
                let id = path_id(args, "run_id")?;
                let path = format!("/agents/runs/{id}");
                render_run(&api.get_json(&path, &[]).await?).map(Into::into)
            }
            "stop_agent_run" => {
                let id = path_id(args, "run_id")?;
                let path = format!("/agents/runs/{id}/stop");
                let request =
                    api.request(respan_gateway_tool_vendor_kit::reqwest::Method::POST, &path);
                render_run(&api.json(request).await?).map(Into::into)
            }
            "live_view" => {
                let id = path_id(args, "session_id")?;
                let mut query = Vec::new();
                if let Some(v) = optional_int(args, "expires_in", 60, 21600)? {
                    query.push(("expiresIn", v.to_string()));
                }
                let path = format!("/sessions/{id}/debug");
                Ok(render_live_view(&api.get_json(&path, &query).await?).into())
            }
            _ => Err(ToolError::new(format!("no tool named {name:?}"))),
        }?;
        Ok(match usage(name) {
            Some((unit, n)) => output.with_usage(unit, n),
            None => output,
        })
    }
}

/// What a call of `name` uses, as Browserbase bills it
/// (<https://www.browserbase.com/pricing>), its answers saying nothing of
/// cost: a search is one of the searches it bills per 1,000; a fetch one of
/// its calls per 1,000 (dearer with proxies or extraction); `run_agent` one
/// agent run, its browser time and model tokens billed to its session and
/// the model gateway. The other tools are free.
fn usage(name: &str) -> Option<(&'static str, f64)> {
    match name {
        "search" => Some(("searches", 1.0)),
        "fetch" => Some(("requests", 1.0)),
        "run_agent" => Some(("runs", 1.0)),
        _ => None,
    }
}

/// The id `key`, safe in a path: letters, digits, `-` and `_`.
fn path_id<'a>(args: &'a Map<String, Value>, key: &str) -> Result<&'a str, ToolError> {
    let id = required_str(args, key)?.trim();
    if id
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        Ok(id)
    } else {
        Err(ToolError::new(format!("`{key}` is not an id: {id:?}")))
    }
}

/// The object `key`, if there.
fn optional_object<'a>(
    args: &'a Map<String, Value>,
    key: &str,
) -> Result<Option<&'a Map<String, Value>>, ToolError> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Object(m)) => Ok(Some(m)),
        Some(_) => Err(ToolError::new(format!("`{key}` must be a JSON object"))),
    }
}

/// `POST /search`'s body from the tool's arguments.
fn search_body(args: &Map<String, Value>) -> Result<Value, ToolError> {
    let query = required_str(args, "query")?;
    if query.chars().count() > 200 {
        return Err(ToolError::new("`query` is at most 200 characters"));
    }
    let mut body = Map::new();
    body.insert("query".into(), query.into());
    if let Some(v) = optional_int(args, "num_results", 1, 25)? {
        body.insert("numResults".into(), v.into());
    }
    let provider = optional_enum(args, "provider", &["parallel", "exa"])?;
    let mode = optional_enum(args, "mode", &["fast", "advanced"])?;
    if mode.is_some() && provider != Some("parallel") {
        return Err(ToolError::new("`mode` needs `provider` parallel"));
    }
    if let Some(v) = provider {
        body.insert("provider".into(), v.into());
    }
    if let Some(v) = mode {
        body.insert("mode".into(), v.into());
    }
    Ok(Value::Object(body))
}

/// `POST /fetch`'s body from the tool's arguments.
fn fetch_body(args: &Map<String, Value>) -> Result<Value, ToolError> {
    let url = required_str(args, "url")?;
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err(ToolError::new(format!(
            "`url` must be an http(s) URL, not {url:?}"
        )));
    }
    let format = optional_enum(args, "format", &["markdown", "raw", "json"])?.unwrap_or("markdown");
    let schema = optional_object(args, "schema")?;
    match (format, schema) {
        ("json", None) => return Err(ToolError::new("format json needs a `schema`")),
        (f, Some(_)) if f != "json" => {
            return Err(ToolError::new("`schema` goes only with format json"));
        }
        _ => {}
    }
    let mut body = Map::new();
    body.insert("url".into(), url.into());
    body.insert("format".into(), format.into());
    if let Some(s) = schema {
        body.insert("schema".into(), Value::Object(s.clone()));
    }
    body.insert(
        "allowRedirects".into(),
        optional_bool(args, "follow_redirects")?
            .unwrap_or(true)
            .into(),
    );
    if let Some(v) = optional_bool(args, "proxies")? {
        body.insert("proxies".into(), v.into());
    }
    if let Some(v) = optional_bool(args, "allow_insecure_ssl")? {
        body.insert("allowInsecureSsl".into(), v.into());
    }
    Ok(Value::Object(body))
}

/// `POST /agents/runs`'s body from the tool's arguments.
fn run_body(args: &Map<String, Value>) -> Result<Value, ToolError> {
    let mut body = Map::new();
    body.insert("task".into(), required_str(args, "task")?.into());
    if let Some(s) = optional_object(args, "result_schema")? {
        body.insert("resultSchema".into(), Value::Object(s.clone()));
    }
    if let Some(v) = optional_str(args, "agent_id")? {
        body.insert("agentId".into(), v.into());
    }
    let mut browser = Map::new();
    if let Some(v) = optional_bool(args, "verified")? {
        browser.insert("verified".into(), v.into());
    }
    match optional_str(args, "proxy_country")? {
        Some(c) if c.len() == 2 && c.bytes().all(|b| b.is_ascii_alphabetic()) => {
            browser.insert(
                "proxies".into(),
                json!([{"type": "browserbase", "geolocation": {"country": c.to_ascii_uppercase()}}]),
            );
        }
        Some(c) => {
            return Err(ToolError::new(format!(
                "`proxy_country` must be a two-letter country code, not {c:?}"
            )));
        }
        None => {
            if let Some(v) = optional_bool(args, "proxies")? {
                browser.insert("proxies".into(), v.into());
            }
        }
    }
    if !browser.is_empty() {
        body.insert("browserSettings".into(), Value::Object(browser));
    }
    Ok(Value::Object(body))
}

fn str_of<'a>(v: &'a Value, key: &str) -> Option<&'a str> {
    v.get(key).and_then(Value::as_str).filter(|s| !s.is_empty())
}

fn array_of<'a>(v: &'a Value, key: &str) -> &'a [Value] {
    v.get(key)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default()
}

/// `text` up to [`CONTENT_CAP`] bytes, cut at a character and marked so.
fn capped(text: &str) -> String {
    if text.len() <= CONTENT_CAP {
        return text.to_owned();
    }
    let mut end = CONTENT_CAP;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!(
        "{}\n\n[cut at {CONTENT_CAP} of {} bytes]",
        &text[..end],
        text.len()
    )
}

/// Search results as JSON, with the fields worth reading.
fn render_search(response: &Value) -> String {
    let results: Vec<Value> = array_of(response, "results")
        .iter()
        .map(|r| {
            let kept: Map<String, Value> = [
                ("title", "title"),
                ("url", "url"),
                ("author", "author"),
                ("publishedDate", "published_date"),
            ]
            .iter()
            .filter_map(|(from, to)| str_of(r, from).map(|v| ((*to).to_owned(), v.into())))
            .collect();
            Value::Object(kept)
        })
        .collect();
    if results.is_empty() {
        return "No results.".into();
    }
    pretty(&Value::Array(results))
}

/// The page's status and type, then its content.
fn render_fetch(response: &Value) -> String {
    let mut out = String::from("HTTP ");
    match response.get("statusCode").and_then(Value::as_u64) {
        Some(code) => out.push_str(&code.to_string()),
        None => out.push('?'),
    }
    if let Some(mime) = str_of(response, "contentType") {
        out.push_str(&format!(" ({mime})"));
    }
    if let Some(location) = response
        .get("headers")
        .and_then(|h| str_of(h, "location").or_else(|| str_of(h, "Location")))
    {
        out.push_str(&format!("\nLocation: {location}"));
    }
    let content = match response.get("content") {
        Some(Value::String(s)) => s.trim().to_owned(),
        Some(Value::Null) | None => String::new(),
        Some(other) => pretty(other),
    };
    out.push_str("\n\n");
    out.push_str(&if content.is_empty() {
        "(no content)".to_owned()
    } else {
        capped(&content)
    });
    out
}

/// A run's id, status, session and result; a failed or timed-out run is a
/// [`ToolError`] saying why.
fn render_run(response: &Value) -> Result<String, ToolError> {
    let status = str_of(response, "status").unwrap_or("UNKNOWN");
    let mut head = format!("Run {}: {status}", str_of(response, "runId").unwrap_or("?"));
    if let Some(session) = str_of(response, "sessionId") {
        head.push_str(&format!("\nsession_id: {session}"));
    }
    if let Some(agent) = str_of(response, "agentId") {
        head.push_str(&format!("\nagent_id: {agent}"));
    }
    let cause = response.get("cause").map(|c| {
        let mut s = str_of(c, "message").unwrap_or("no reason given").to_owned();
        if let Some(code) = str_of(c, "code") {
            s.push_str(&format!(" ({code})"));
        }
        s
    });
    match status {
        "FAILED" | "TIMED_OUT" => {
            let why = cause.unwrap_or_else(|| "no reason given".into());
            Err(ToolError::new(format!("{head}\n{why}")))
        }
        "PENDING" | "RUNNING" => Ok(format!(
            "{head}\nNot finished: call `get_agent_run` again shortly."
        )),
        "PAUSED" => Ok(format!(
            "{head}\nPaused, waiting for input it was given no way to receive here: stop it \
            with `stop_agent_run`."
        )),
        _ => {
            if let Some(why) = cause {
                head.push_str(&format!("\n{why}"));
            }
            match response.get("result").filter(|r| !r.is_null()) {
                Some(result) => Ok(format!("{head}\nResult:\n{}", pretty(result))),
                None => Ok(head),
            }
        }
    }
}

/// The session's live view URLs and each page's.
fn render_live_view(response: &Value) -> String {
    let mut out = String::new();
    if let Some(url) = str_of(response, "debuggerFullscreenUrl") {
        out.push_str(&format!("Live view: {url}"));
    }
    if let Some(url) = str_of(response, "debuggerUrl") {
        out.push_str(&format!("\nWith browser controls: {url}"));
    }
    let pages = array_of(response, "pages");
    if !pages.is_empty() {
        out.push_str("\n\nPages:");
        for p in pages {
            out.push_str(&format!(
                "\n- {} ({}): {}",
                str_of(p, "title").unwrap_or("untitled"),
                str_of(p, "url").unwrap_or("?"),
                str_of(p, "debuggerFullscreenUrl").unwrap_or("?")
            ));
        }
    }
    if out.is_empty() {
        return "No live view: the session may have ended.".into();
    }
    out.trim_start().to_owned()
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
    fn search_body_maps_and_validates() {
        assert_eq!(
            search_body(&args(json!({
                "query": "rust", "num_results": 5, "provider": "parallel", "mode": "fast"
            })))
            .expect("valid"),
            json!({"query": "rust", "numResults": 5, "provider": "parallel", "mode": "fast"})
        );
        assert_eq!(
            search_body(&args(json!({"query": "rust"}))).expect("valid"),
            json!({"query": "rust"})
        );
        assert!(search_body(&args(json!({}))).is_err());
        assert!(search_body(&args(json!({"query": "x".repeat(201)}))).is_err());
        assert!(search_body(&args(json!({"query": "x", "num_results": 26}))).is_err());
        assert!(search_body(&args(json!({"query": "x", "mode": "fast"}))).is_err());
        assert!(
            search_body(&args(
                json!({"query": "x", "provider": "exa", "mode": "fast"})
            ))
            .is_err()
        );
    }

    #[test]
    fn fetch_body_maps_and_validates() {
        assert_eq!(
            fetch_body(&args(json!({"url": "https://a.b"}))).expect("valid"),
            json!({"url": "https://a.b", "format": "markdown", "allowRedirects": true})
        );
        assert_eq!(
            fetch_body(&args(json!({
                "url": "https://a.b", "format": "json", "schema": {"type": "object"},
                "follow_redirects": false, "proxies": true
            })))
            .expect("valid"),
            json!({
                "url": "https://a.b", "format": "json", "schema": {"type": "object"},
                "allowRedirects": false, "proxies": true
            })
        );
        assert!(fetch_body(&args(json!({}))).is_err());
        assert!(fetch_body(&args(json!({"url": "ftp://a"}))).is_err());
        assert!(fetch_body(&args(json!({"url": "https://a", "format": "json"}))).is_err());
        assert!(fetch_body(&args(json!({"url": "https://a", "schema": {}}))).is_err());
        assert!(fetch_body(&args(json!({"url": "https://a", "format": "pdf"}))).is_err());
    }

    #[test]
    fn run_body_maps_and_validates() {
        assert_eq!(
            run_body(&args(json!({
                "task": "find prices", "result_schema": {"type": "object"},
                "verified": true, "proxy_country": "de"
            })))
            .expect("valid"),
            json!({
                "task": "find prices", "resultSchema": {"type": "object"},
                "browserSettings": {
                    "verified": true,
                    "proxies": [{"type": "browserbase", "geolocation": {"country": "DE"}}]
                }
            })
        );
        assert_eq!(
            run_body(&args(json!({"task": "t", "proxies": true}))).expect("valid"),
            json!({"task": "t", "browserSettings": {"proxies": true}})
        );
        assert!(run_body(&args(json!({}))).is_err());
        assert!(run_body(&args(json!({"task": "t", "proxy_country": "USA"}))).is_err());
        assert!(run_body(&args(json!({"task": "t", "result_schema": "x"}))).is_err());
    }

    #[test]
    fn usage_is_per_call() {
        assert_eq!(usage("search"), Some(("searches", 1.0)));
        assert_eq!(usage("fetch"), Some(("requests", 1.0)));
        assert_eq!(usage("run_agent"), Some(("runs", 1.0)));
        for free in ["get_agent_run", "stop_agent_run", "live_view"] {
            assert_eq!(usage(free), None);
        }
    }

    #[test]
    fn ids_must_be_ids() {
        assert_eq!(
            path_id(&args(json!({"run_id": "a1-b_2"})), "run_id"),
            Ok("a1-b_2")
        );
        assert!(path_id(&args(json!({"run_id": "../sessions"})), "run_id").is_err());
        assert!(path_id(&args(json!({})), "run_id").is_err());
    }

    #[test]
    fn renders_search() {
        let text = render_search(&json!({"requestId": "r", "query": "q", "results": [
            {"id": "1", "url": "https://rust-lang.org", "title": "Rust", "author": "Rust team",
             "publishedDate": "2025-01-01T00:00:00Z", "image": "https://i", "favicon": "https://f"}
        ]}));
        let parsed: Value = serde_json::from_str(&text).expect("json");
        assert_eq!(
            parsed,
            json!([{"title": "Rust", "url": "https://rust-lang.org", "author": "Rust team",
                "published_date": "2025-01-01T00:00:00Z"}])
        );
        assert_eq!(render_search(&json!({"results": []})), "No results.");
    }

    #[test]
    fn renders_fetch() {
        assert_eq!(
            render_fetch(&json!({
                "id": "f", "statusCode": 200, "headers": {"server": "x"},
                "content": "# Hello\n", "contentType": "text/html", "encoding": "utf-8"
            })),
            "HTTP 200 (text/html)\n\n# Hello"
        );
        assert_eq!(
            render_fetch(&json!({
                "statusCode": 200, "headers": {}, "content": {"price": 5},
                "contentType": "text/html", "encoding": "utf-8"
            })),
            "HTTP 200 (text/html)\n\n{\n  \"price\": 5\n}"
        );
        assert_eq!(
            render_fetch(&json!({
                "statusCode": 301, "headers": {"location": "https://b"}, "content": "",
                "contentType": "text/html", "encoding": "utf-8"
            })),
            "HTTP 301 (text/html)\nLocation: https://b\n\n(no content)"
        );
        let long = render_fetch(&json!({"statusCode": 200, "content": "é".repeat(CONTENT_CAP)}));
        assert!(long.len() < CONTENT_CAP + 100);
        assert!(long.ends_with("bytes]"));
    }

    #[test]
    fn renders_runs() {
        assert_eq!(
            render_run(&json!({
                "runId": "r1", "task": "t", "status": "COMPLETED", "sessionId": "s1",
                "result": {"price": "$799"}, "createdAt": "x", "updatedAt": "y"
            })),
            Ok("Run r1: COMPLETED\nsession_id: s1\nResult:\n{\n  \"price\": \"$799\"\n}".into())
        );
        assert_eq!(
            render_run(&json!({"runId": "r1", "task": "t", "status": "PENDING"})),
            Ok("Run r1: PENDING\nNot finished: call `get_agent_run` again shortly.".into())
        );
        assert_eq!(
            render_run(&json!({
                "runId": "r1", "status": "FAILED", "sessionId": "s1",
                "cause": {"code": "RUNNER_HEARTBEAT_LOST", "message": "Runner went away"}
            })),
            Err(ToolError::new(
                "Run r1: FAILED\nsession_id: s1\nRunner went away (RUNNER_HEARTBEAT_LOST)"
            ))
        );
        assert_eq!(
            render_run(&json!({"runId": "r1", "status": "STOPPED"})),
            Ok("Run r1: STOPPED".into())
        );
    }

    #[test]
    fn renders_live_view() {
        assert_eq!(
            render_live_view(&json!({
                "debuggerFullscreenUrl": "https://v/full", "debuggerUrl": "https://v/dbg",
                "wsUrl": "wss://w",
                "pages": [{"id": "p", "url": "https://a.b", "faviconUrl": "https://f",
                    "title": "A", "debuggerUrl": "https://v/p", "debuggerFullscreenUrl": "https://v/pf"}]
            })),
            "Live view: https://v/full\nWith browser controls: https://v/dbg\n\nPages:\n- A (https://a.b): https://v/pf"
        );
        assert_eq!(
            render_live_view(&json!({})),
            "No live view: the session may have ended."
        );
    }
}
