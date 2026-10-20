//! Context7 by Upstash (<https://context7.com>): up-to-date documentation
//! and code examples for software libraries, from its API at
//! `https://context7.com/api`: library search (`GET /v2/libs/search`), a
//! library's documentation for a question (`GET /v2/context`, as text) and
//! documentation search across up to four libraries in one request
//! (`GET /v3/search`, as text), the key (`ctx7sk-…`) as a bearer token.

use async_trait::async_trait;
use respan_gateway_tool_vendor_kit::args::{object, optional_str, optional_str_list, required_str};
use respan_gateway_tool_vendor_kit::reqwest::Method;
use respan_gateway_tool_vendor_kit::{
    Api, KeyIn, ToolError, ToolOutput, ToolSpec, ToolVendor, Tools,
};
use serde_json::{Map, Value};

/// Context7.
pub const VENDOR: ToolVendor = ToolVendor {
    id: "context7",
    key_env: &["CONTEXT7_API_KEY"],
    key_in: KeyIn::Bearer,
    base_url: "https://context7.com/api",
    tools: &Context7Tools,
};

/// Context7's tools: `resolve_library_id`, `query_docs` and `search_docs`.
#[derive(Debug, Clone, Copy)]
pub struct Context7Tools;

/// The most of a documentation answer a result carries.
const MAX_DOCS: usize = 100_000;

const TOOLS: &[ToolSpec] = &[
    ToolSpec {
        name: "resolve_library_id",
        description: "Find a library's Context7 ID (`/org/project`) by its name, ranked against \
            what you want to look up; each match has its ID, description, snippet count, source \
            reputation, benchmark score and versions. Call it before `query_docs` unless the ID \
            is already known. Each call is one request against the Context7 account's quota; \
            requests past it are billed on paid plans.",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "library_name": {"type": "string", "maxLength": 500, "description": "The library's official name, with its punctuation, e.g. \"Next.js\", \"Three.js\"."},
    "query": {"type": "string", "maxLength": 500, "description": "What you want to look up in its documentation, to rank the matches. No secrets or private code: it is sent to Context7."}
  },
  "required": ["library_name", "query"]
}"#,
    },
    ToolSpec {
        name: "query_docs",
        description: "Get up-to-date documentation and code examples from one library on \
            Context7 for a question, as markdown with each snippet's source URL. Needs the \
            library's Context7 ID, from `resolve_library_id`. Each call is one request against \
            the Context7 account's quota; requests past it are billed on paid plans.",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "library_id": {"type": "string", "maxLength": 500, "description": "The library's Context7 ID, e.g. \"/vercel/next.js\", optionally pinned to a version: \"/vercel/next.js/v14.3.0\" or \"/vercel/next.js@v14.3.0\"."},
    "query": {"type": "string", "maxLength": 500, "description": "One specific question or concept, e.g. \"React useEffect cleanup function examples\"; ask about separate concepts in separate calls. No secrets or private code: it is sent to Context7."},
    "language": {"type": "string", "maxLength": 40, "description": "The programming language you write in, e.g. \"Python\", to rank its examples first."}
  },
  "required": ["library_id", "query"]
}"#,
    },
    ToolSpec {
        name: "search_docs",
        description: "Search Context7's documentation for a question in one request, without \
            resolving a library first: it picks up to four relevant libraries (or the ones \
            hinted), and returns their most relevant documentation and code examples as \
            markdown with source URLs. Each call is one request against the Context7 account's \
            quota; requests past it are billed on paid plans.",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "query": {"type": "string", "maxLength": 2000, "description": "The question or task; long questions and pasted error output are fine. No secrets or private code: it is sent to Context7."},
    "libraries": {"type": "array", "items": {"type": "string", "maxLength": 120}, "maxItems": 4, "description": "Libraries to search, each a name (\"next.js\") or Context7 ID (\"/vercel/next.js\", \"/vercel/next.js@15\")."},
    "version": {"type": "string", "description": "A version to prefer, e.g. \"15.2\" or \"v5\"; \"latest\" is the same as none. When it is not indexed, the newest matching release or current documentation is served instead."},
    "language": {"type": "string", "maxLength": 40, "description": "The programming language you write in, e.g. \"TypeScript\", to rank its examples first."}
  },
  "required": ["query"]
}"#,
    },
];

#[async_trait]
impl Tools for Context7Tools {
    fn list(&self) -> &'static [ToolSpec] {
        TOOLS
    }

    async fn call(&self, api: &Api, name: &str, arguments: Value) -> Result<ToolOutput, ToolError> {
        let args = object(&arguments)?;
        match name {
            "resolve_library_id" => {
                let query = resolve_query(args)?;
                let text = render_libraries(&api.get_json("v2/libs/search", &query).await?);
                Ok(one_request(text))
            }
            "query_docs" => {
                let query = context_query(args)?;
                let req = api.request(Method::GET, "v2/context").query(&query);
                Ok(one_request(render_docs(&api.text(req).await?)?))
            }
            "search_docs" => {
                let query = search_query(args)?;
                let req = api.request(Method::GET, "v3/search").query(&query);
                Ok(one_request(render_docs(&api.text(req).await?)?))
            }
            _ => Err(ToolError::new(format!("no tool named {name:?}"))),
        }
    }
}

/// `text`, having used one request: Context7 bills each API call as one
/// against the plan's monthly allowance, its answers saying nothing of it
/// (<https://context7.com/plans>).
fn one_request(text: String) -> ToolOutput {
    ToolOutput::from(text).with_usage("requests", 1.0)
}

/// The string `key`, which must be there, of at most `max` characters.
fn bounded<'a>(args: &'a Map<String, Value>, key: &str, max: usize) -> Result<&'a str, ToolError> {
    let v = required_str(args, key)?;
    within(key, v, max)
}

fn within<'a>(key: &str, v: &'a str, max: usize) -> Result<&'a str, ToolError> {
    if v.chars().count() > max {
        return Err(ToolError::new(format!(
            "`{key}` must be at most {max} characters"
        )));
    }
    Ok(v)
}

/// The optional `language`, of at most 40 characters.
fn language(args: &Map<String, Value>) -> Result<Option<&str>, ToolError> {
    optional_str(args, "language")?
        .map(|v| within("language", v, 40))
        .transpose()
}

/// `GET /v2/libs/search`'s query from the tool's arguments.
fn resolve_query(args: &Map<String, Value>) -> Result<Vec<(&'static str, String)>, ToolError> {
    Ok(vec![
        (
            "libraryName",
            bounded(args, "library_name", 500)?.to_owned(),
        ),
        ("query", bounded(args, "query", 500)?.to_owned()),
    ])
}

/// Whether `id` is shaped as a Context7 ID: `/owner/repo`, optionally
/// with `/<version>` or `@<version>`.
fn is_library_id(id: &str) -> bool {
    let Some(rest) = id.strip_prefix('/') else {
        return false;
    };
    let parts: Vec<&str> = rest.split('/').collect();
    let (owner, repo) = match parts.as_slice() {
        [owner, repo] | [owner, repo, _] => (*owner, *repo),
        _ => return false,
    };
    if parts.iter().any(|p| p.is_empty()) || owner.contains('@') {
        return false;
    }
    match repo.split_once('@') {
        Some((repo, version)) => parts.len() == 2 && !repo.is_empty() && !version.is_empty(),
        None => true,
    }
}

/// `GET /v2/context`'s query from the tool's arguments.
fn context_query(args: &Map<String, Value>) -> Result<Vec<(&'static str, String)>, ToolError> {
    let id = bounded(args, "library_id", 500)?;
    if !is_library_id(id) {
        return Err(ToolError::new(format!(
            "`library_id` must be a Context7 ID such as \"/vercel/next.js\" or \
             \"/vercel/next.js/v14.3.0\", not {id:?}; find it with resolve_library_id"
        )));
    }
    let mut query = vec![
        ("libraryId", id.to_owned()),
        ("query", bounded(args, "query", 500)?.to_owned()),
    ];
    if let Some(v) = language(args)? {
        query.push(("language", v.to_owned()));
    }
    Ok(query)
}

/// Whether `v` is a version `/v3/search` takes: `latest`, or digits in up
/// to three parts, optionally after a `v` and before a `-`/`+` suffix.
fn is_version(v: &str) -> bool {
    if v.eq_ignore_ascii_case("latest") {
        return true;
    }
    let v = v.strip_prefix(['v', 'V']).unwrap_or(v);
    let (core, suffix) = match v.find(['-', '+']) {
        Some(i) => (&v[..i], Some(&v[i + 1..])),
        None => (v, None),
    };
    let parts: Vec<&str> = core.split(['.', '_']).collect();
    parts.len() <= 3
        && parts
            .iter()
            .all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))
        && suffix.is_none_or(|s| {
            !s.is_empty()
                && s.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'-')
        })
}

/// `GET /v3/search`'s query from the tool's arguments; `library` repeats.
fn search_query(args: &Map<String, Value>) -> Result<Vec<(&'static str, String)>, ToolError> {
    let mut query = vec![("query", bounded(args, "query", 2000)?.to_owned())];
    if let Some(libraries) = optional_str_list(args, "libraries")? {
        if libraries.len() > 4 {
            return Err(ToolError::new("`libraries` must have at most 4 entries"));
        }
        for library in libraries {
            within("libraries", &library, 120)?;
            query.push(("library", library));
        }
    }
    if let Some(v) = optional_str(args, "version")? {
        if !is_version(v) {
            return Err(ToolError::new(format!(
                "`version` must be \"latest\" or a version such as \"15.2\" or \"v5.0.0\", not {v:?}"
            )));
        }
        query.push(("version", v.to_owned()));
    }
    if let Some(v) = language(args)? {
        query.push(("language", v.to_owned()));
    }
    Ok(query)
}

fn str_of<'a>(v: &'a Value, key: &str) -> &'a str {
    v.get(key).and_then(Value::as_str).unwrap_or_default()
}

/// Matching libraries, one block each: ID, title, description, then what
/// there is of snippets, reputation, benchmark, state and versions.
/// Branches, dates, token totals and stars are dropped.
fn render_libraries(response: &Value) -> String {
    let results = response
        .get("results")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    if results.is_empty() {
        return "No libraries matched.".into();
    }
    let mut blocks = Vec::new();
    if response.get("searchFilterApplied").and_then(Value::as_bool) == Some(true) {
        blocks.push("Only libraries the teamspace's filters allow are listed.".to_owned());
    }
    for r in results {
        let mut lines = vec![format!("{} — {}", str_of(r, "id"), str_of(r, "title"))];
        let description = str_of(r, "description").trim();
        if !description.is_empty() {
            lines.push(description.replace('\n', " "));
        }
        let mut facts = Vec::new();
        if let Some(n) = r
            .get("totalSnippets")
            .and_then(Value::as_i64)
            .filter(|n| *n >= 0)
        {
            facts.push(format!("snippets: {n}"));
        }
        if let Some(n) = r
            .get("trustScore")
            .and_then(Value::as_f64)
            .filter(|n| *n >= 0.0)
        {
            let label = match n {
                n if n >= 7.0 => "high",
                n if n >= 4.0 => "medium",
                _ => "low",
            };
            facts.push(format!("reputation: {label}"));
        }
        if let Some(n) = r
            .get("benchmarkScore")
            .and_then(Value::as_f64)
            .filter(|n| *n > 0.0)
        {
            facts.push(format!("benchmark: {n}"));
        }
        let state = str_of(r, "state");
        if !state.is_empty() && state != "finalized" {
            facts.push(format!("state: {state}"));
        }
        if !facts.is_empty() {
            lines.push(facts.join(", "));
        }
        let versions: Vec<&str> = r
            .get("versions")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .collect();
        if !versions.is_empty() {
            lines.push(format!("versions: {}", versions.join(", ")));
        }
        blocks.push(lines.join("\n"));
    }
    blocks.join("\n\n")
}

/// A documentation answer's markdown, cut at [`MAX_DOCS`]. A 2xx answer
/// that is a JSON error, as `202 library_not_finalized` is, fails with
/// its message.
fn render_docs(text: &str) -> Result<String, ToolError> {
    let text = text.trim();
    if text.is_empty() {
        return Ok("No documentation matched.".into());
    }
    if text.starts_with('{')
        && let Ok(v) = serde_json::from_str::<Value>(text)
    {
        let message = str_of(&v, "message");
        if !str_of(&v, "error").is_empty() && !message.is_empty() {
            return Err(ToolError::new(message));
        }
    }
    if text.len() <= MAX_DOCS {
        return Ok(text.to_owned());
    }
    let mut end = MAX_DOCS;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    Ok(format!("{}\n\n[cut at {MAX_DOCS} bytes]", &text[..end]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn args(v: Value) -> Map<String, Value> {
        match v {
            Value::Object(m) => m,
            _ => Map::new(),
        }
    }

    fn owned(q: &[(&'static str, &str)]) -> Vec<(&'static str, String)> {
        q.iter().map(|(k, v)| (*k, (*v).to_owned())).collect()
    }

    #[test]
    fn resolve_query_maps_and_validates() {
        assert_eq!(
            resolve_query(&args(json!({"library_name": "Next.js", "query": "auth"})))
                .expect("valid"),
            owned(&[("libraryName", "Next.js"), ("query", "auth")])
        );
        assert!(resolve_query(&args(json!({"library_name": "Next.js"}))).is_err());
        assert!(
            resolve_query(&args(
                json!({"library_name": "x", "query": "q".repeat(501)})
            ))
            .is_err()
        );
    }

    #[test]
    fn context_query_maps_and_validates() {
        assert_eq!(
            context_query(&args(json!({
                "library_id": "/vercel/next.js/v14.3.0", "query": "middleware", "language": "TypeScript"
            })))
            .expect("valid"),
            owned(&[
                ("libraryId", "/vercel/next.js/v14.3.0"),
                ("query", "middleware"),
                ("language", "TypeScript")
            ])
        );
        for id in [
            "/vercel/next.js",
            "/websites/uploadcare_com",
            "/vercel/next.js@v14.3.0",
        ] {
            assert!(is_library_id(id), "{id}");
        }
        for id in [
            "next.js",
            "/vercel",
            "/vercel/next.js/v1/x",
            "//next.js",
            "/a/b@",
            "/a/b@1/c",
        ] {
            assert!(!is_library_id(id), "{id}");
        }
        assert!(context_query(&args(json!({"library_id": "react", "query": "x"}))).is_err());
        assert!(context_query(&args(json!({"library_id": "/a/b"}))).is_err());
        assert!(
            context_query(&args(
                json!({"library_id": "/a/b", "query": "x", "language": "l".repeat(41)})
            ))
            .is_err()
        );
    }

    #[test]
    fn search_query_maps_and_validates() {
        assert_eq!(
            search_query(&args(json!({
                "query": "stream a response", "libraries": ["Next.js", "/openai/openai-node"],
                "version": "15.2", "language": "TypeScript"
            })))
            .expect("valid"),
            owned(&[
                ("query", "stream a response"),
                ("library", "Next.js"),
                ("library", "/openai/openai-node"),
                ("version", "15.2"),
                ("language", "TypeScript")
            ])
        );
        for v in ["latest", "LATEST", "15", "v5.0.0", "2_3", "1.0.0-rc.1"] {
            assert!(is_version(v), "{v}");
        }
        for v in ["next", "1.2.3.4", "v", "1.", "1.0-"] {
            assert!(!is_version(v), "{v}");
        }
        assert!(search_query(&args(json!({}))).is_err());
        assert!(
            search_query(&args(
                json!({"query": "x", "libraries": ["a", "b", "c", "d", "e"]})
            ))
            .is_err()
        );
        assert!(search_query(&args(json!({"query": "x", "version": "newest"}))).is_err());
        assert!(search_query(&args(json!({"query": "x", "libraries": [1]}))).is_err());
    }

    #[test]
    fn renders_libraries() {
        let text = render_libraries(&json!({
            "results": [{
                "id": "/facebook/react", "title": "React",
                "description": "A JavaScript library for building user interfaces",
                "branch": "main", "lastUpdateDate": "2025-01-15T10:30:00.000Z", "state": "finalized",
                "totalTokens": 500000, "totalSnippets": 2500, "stars": 220000, "trustScore": 10,
                "benchmarkScore": 95.5, "versions": ["v18.2.0", "v17.0.2"]
            }, {
                "id": "/x/y", "title": "Y", "description": "", "state": "initial",
                "totalSnippets": -1, "trustScore": -1
            }],
            "searchFilterApplied": true
        }));
        assert_eq!(
            text,
            "Only libraries the teamspace's filters allow are listed.\n\n\
             /facebook/react — React\n\
             A JavaScript library for building user interfaces\n\
             snippets: 2500, reputation: high, benchmark: 95.5\n\
             versions: v18.2.0, v17.0.2\n\n\
             /x/y — Y\n\
             state: initial"
        );
        assert_eq!(
            render_libraries(&json!({"results": [], "searchFilterApplied": false})),
            "No libraries matched."
        );
    }

    #[test]
    fn renders_docs() {
        let md = "### Middleware Authentication Example\n\nSource: https://github.com/vercel/next.js/blob/canary/docs/middleware.mdx\n\n```typescript\nimport { NextResponse } from 'next/server'\n```";
        assert_eq!(render_docs(&format!("{md}\n")), Ok(md.to_owned()));
        assert_eq!(render_docs(" "), Ok("No documentation matched.".into()));
        assert_eq!(
            render_docs(
                r#"{"error": "library_not_finalized", "message": "Library /owner/repo is still being processed. Please try again shortly."}"#
            ),
            Err(ToolError::new(
                "Library /owner/repo is still being processed. Please try again shortly."
            ))
        );
        let long = "é".repeat(MAX_DOCS);
        let cut = render_docs(&long).expect("text");
        assert!(cut.ends_with("[cut at 100000 bytes]"));
        assert!(cut.len() < MAX_DOCS + 40);
    }

    #[test]
    fn each_call_is_one_request() {
        assert_eq!(one_request("x".into()).usage.get("requests"), Some(&1.0));
    }
}
