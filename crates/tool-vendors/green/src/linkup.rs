//! Linkup (<https://linkup.so>): web search, sourced answers and page
//! fetches, from its API at `https://api.linkup.so/v1` (`POST /search`,
//! `POST /fetch`), the key as a bearer token.

use async_trait::async_trait;
use respan_gateway_tool_vendor_kit::args::{
    object, optional_bool, optional_date, optional_enum, optional_int, optional_str_list, pretty,
    required_str,
};
use respan_gateway_tool_vendor_kit::{
    Api, KeyIn, ToolError, ToolOutput, ToolSpec, ToolVendor, Tools,
};
use serde_json::{Map, Value, json};

/// Linkup.
pub const VENDOR: ToolVendor = ToolVendor {
    id: "linkup",
    key_env: &["LINKUP_API_KEY"],
    key_in: KeyIn::Bearer,
    base_url: "https://api.linkup.so/v1",
    tools: &LinkupTools,
};

/// Linkup's tools: `search`, `answer` and `fetch`.
#[derive(Debug, Clone, Copy)]
pub struct LinkupTools;

const DEPTHS: &[&str] = &["flash", "fast", "standard", "deep"];

const TOOLS: &[ToolSpec] = &[
    ToolSpec {
        name: "search",
        description: "Search the web with Linkup and get ranked sources: each a title, URL and \
            the relevant extracted text. Each call is billed to the Linkup account; `deep` costs \
            more and is slower (it searches iteratively, for multi-hop questions).",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "q": {"type": "string", "description": "The question or query, in natural language."},
    "depth": {"type": "string", "enum": ["flash", "fast", "standard", "deep"], "default": "standard", "description": "flash: lowest latency (sub-second); fast: one-shot, about a second; standard: one agentic pass, for queries spanning several topics; deep: several iterations, slower and costlier."},
    "include_domains": {"type": "array", "items": {"type": "string"}, "maxItems": 100, "description": "Only search these domains, e.g. [\"arxiv.org\"]."},
    "exclude_domains": {"type": "array", "items": {"type": "string"}, "description": "Never return these domains."},
    "from_date": {"type": "string", "description": "Only results from this date on, YYYY-MM-DD."},
    "to_date": {"type": "string", "description": "Only results up to this date, YYYY-MM-DD."},
    "max_results": {"type": "integer", "minimum": 1, "maximum": 100, "description": "At most this many results."},
    "include_images": {"type": "boolean", "default": false, "description": "Also return image results (title and URL)."}
  },
  "required": ["q"]
}"#,
    },
    ToolSpec {
        name: "answer",
        description: "Ask Linkup a question: it searches the web and writes an answer, with the \
            sources (title, URL, snippet) it used. Each call is billed to the Linkup account; \
            `deep` costs more and is slower.",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "q": {"type": "string", "description": "The question, in natural language."},
    "depth": {"type": "string", "enum": ["flash", "fast", "standard", "deep"], "default": "standard", "description": "flash: lowest latency; fast: one-shot, about a second; standard: one agentic pass; deep: several iterations, slower and costlier."},
    "include_domains": {"type": "array", "items": {"type": "string"}, "maxItems": 100, "description": "Only search these domains."},
    "exclude_domains": {"type": "array", "items": {"type": "string"}, "description": "Never use these domains."},
    "from_date": {"type": "string", "description": "Only sources from this date on, YYYY-MM-DD."},
    "to_date": {"type": "string", "description": "Only sources up to this date, YYYY-MM-DD."},
    "inline_citations": {"type": "boolean", "default": false, "description": "Cite the sources inline in the answer."}
  },
  "required": ["q"]
}"#,
    },
    ToolSpec {
        name: "fetch",
        description: "Fetch one web page with Linkup and get it as clean markdown. Each call is \
            billed to the Linkup account; `render_js` and `mode: \"pro\"` are slower and may \
            cost more.",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "url": {"type": "string", "description": "The page's URL."},
    "render_js": {"type": "boolean", "default": false, "description": "Render the page's JavaScript first, for pages that build their content in the browser."},
    "mode": {"type": "string", "enum": ["standard", "pro"], "default": "standard", "description": "pro: a stronger fetch for hard-to-retrieve pages."},
    "extract_images": {"type": "boolean", "default": false, "description": "Also list the page's images (alt text and URL)."}
  },
  "required": ["url"]
}"#,
    },
];

#[async_trait]
impl Tools for LinkupTools {
    fn list(&self) -> &'static [ToolSpec] {
        TOOLS
    }

    async fn call(&self, api: &Api, name: &str, arguments: Value) -> Result<ToolOutput, ToolError> {
        let args = object(&arguments)?;
        match name {
            "search" => {
                let body = search_body(args, "searchResults")?;
                let text = render_search(&api.post_json("search", &body).await?);
                Ok(ToolOutput::from(text).with_usage("usd", usd(&body)))
            }
            "answer" => {
                let body = search_body(args, "sourcedAnswer")?;
                let text = render_answer(&api.post_json("search", &body).await?);
                Ok(ToolOutput::from(text).with_usage("usd", usd(&body)))
            }
            "fetch" => {
                let body = fetch_body(args)?;
                let text = render_fetch(&api.post_json("fetch", &body).await?);
                Ok(ToolOutput::from(text).with_usage("usd", usd(&body)))
            }
            _ => Err(ToolError::new(format!("no tool named {name:?}"))),
        }
    }
}

/// `POST /search`'s body for `output_type` from the tool's arguments.
fn search_body(args: &Map<String, Value>, output_type: &str) -> Result<Value, ToolError> {
    let mut body = Map::new();
    body.insert("q".into(), required_str(args, "q")?.into());
    let depth = optional_enum(args, "depth", DEPTHS)?.unwrap_or("standard");
    body.insert("depth".into(), depth.into());
    body.insert("outputType".into(), output_type.into());
    if let Some(v) = optional_str_list(args, "include_domains")? {
        body.insert("includeDomains".into(), json!(v));
    }
    if let Some(v) = optional_str_list(args, "exclude_domains")? {
        body.insert("excludeDomains".into(), json!(v));
    }
    if let Some(v) = optional_date(args, "from_date")? {
        body.insert("fromDate".into(), v.into());
    }
    if let Some(v) = optional_date(args, "to_date")? {
        body.insert("toDate".into(), v.into());
    }
    if let Some(v) = optional_int(args, "max_results", 1, 100)? {
        body.insert("maxResults".into(), v.into());
    }
    if let Some(v) = optional_bool(args, "include_images")? {
        body.insert("includeImages".into(), v.into());
    }
    if let Some(v) = optional_bool(args, "inline_citations")? {
        body.insert("includeInlineCitations".into(), v.into());
    }
    Ok(Value::Object(body))
}

/// `POST /fetch`'s body from the tool's arguments.
fn fetch_body(args: &Map<String, Value>) -> Result<Value, ToolError> {
    let url = required_str(args, "url")?;
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err(ToolError::new("`url` must be an http(s) URL"));
    }
    let mut body = Map::new();
    body.insert("url".into(), url.into());
    if let Some(v) = optional_bool(args, "render_js")? {
        body.insert("renderJs".into(), v.into());
    }
    if let Some(v) = optional_enum(args, "mode", &["standard", "pro"])? {
        body.insert("mode".into(), v.into());
    }
    if let Some(v) = optional_bool(args, "extract_images")? {
        body.insert("extractImages".into(), v.into());
    }
    Ok(Value::Object(body))
}

/// What the call with `body` costs in US dollars, by Linkup's price table
/// (<https://docs.linkup.so/pages/documentation/development/pricing>); its
/// answers don't say. A search is $0.005 (`deep` $0.05), $0.001 more for a
/// sourced answer; a fetch $0.001, $0.005 rendering JavaScript or `pro`,
/// $0.01 both.
fn usd(body: &Value) -> f64 {
    let answer = str_of(body, "outputType") == "sourcedAnswer";
    match (str_of(body, "depth"), answer) {
        ("deep", false) => 0.05,
        ("deep", true) => 0.055,
        ("", _) => {
            let js = body.get("renderJs").and_then(Value::as_bool) == Some(true);
            match (str_of(body, "mode") == "pro", js) {
                (false, false) => 0.001,
                (true, true) => 0.01,
                _ => 0.005,
            }
        }
        (_, false) => 0.005,
        (_, true) => 0.006,
    }
}

fn str_of<'a>(v: &'a Value, key: &str) -> &'a str {
    v.get(key).and_then(Value::as_str).unwrap_or_default()
}

fn array_of<'a>(v: &'a Value, key: &str) -> &'a [Value] {
    v.get(key)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default()
}

/// Search results as JSON: each a title, URL and text, or an image's
/// title and URL. Favicons are dropped.
fn render_search(response: &Value) -> String {
    let results: Vec<Value> = array_of(response, "results")
        .iter()
        .map(|r| match str_of(r, "type") {
            "image" => json!({"type": "image", "title": str_of(r, "name"), "url": str_of(r, "url")}),
            _ => json!({"title": str_of(r, "name"), "url": str_of(r, "url"), "content": str_of(r, "content")}),
        })
        .collect();
    if results.is_empty() {
        return "No results.".into();
    }
    pretty(&Value::Array(results))
}

/// An answer, then its sources.
fn render_answer(response: &Value) -> String {
    let mut out = str_of(response, "answer").trim().to_owned();
    let sources = array_of(response, "sources");
    if !sources.is_empty() {
        out.push_str("\n\nSources:");
        for (i, s) in sources.iter().enumerate() {
            out.push_str(&format!(
                "\n[{}] {} <{}>",
                i + 1,
                str_of(s, "name"),
                str_of(s, "url")
            ));
            let snippet = str_of(s, "snippet").trim();
            if !snippet.is_empty() {
                out.push_str("\n    ");
                out.push_str(&snippet.replace('\n', " "));
            }
        }
    }
    out
}

/// The page's markdown, then its images if asked for.
fn render_fetch(response: &Value) -> String {
    let mut out = str_of(response, "markdown").trim().to_owned();
    if out.is_empty() {
        out = "The page had no extractable content.".into();
    }
    let images = array_of(response, "images");
    if !images.is_empty() {
        out.push_str("\n\nImages:");
        for img in images {
            out.push_str(&format!(
                "\n- {} <{}>",
                str_of(img, "alt"),
                str_of(img, "url")
            ));
        }
    }
    out
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
    fn search_body_maps_arguments() {
        let body = search_body(
            &args(json!({
                "q": "rust", "include_domains": ["rust-lang.org"], "from_date": "2025-01-01",
                "max_results": 5, "include_images": true
            })),
            "searchResults",
        )
        .expect("valid");
        assert_eq!(
            body,
            json!({
                "q": "rust", "depth": "standard", "outputType": "searchResults",
                "includeDomains": ["rust-lang.org"], "fromDate": "2025-01-01",
                "maxResults": 5, "includeImages": true
            })
        );
    }

    #[test]
    fn search_body_rejects_bad_arguments() {
        assert!(search_body(&args(json!({})), "searchResults").is_err());
        assert!(search_body(&args(json!({"q": "x", "depth": "huge"})), "searchResults").is_err());
        assert!(
            search_body(
                &args(json!({"q": "x", "to_date": "May 1"})),
                "sourcedAnswer"
            )
            .is_err()
        );
        assert!(search_body(&args(json!({"q": "x", "max_results": 0})), "searchResults").is_err());
    }

    #[test]
    fn fetch_body_maps_and_validates() {
        assert_eq!(
            fetch_body(&args(
                json!({"url": "https://a.b", "render_js": true, "mode": "pro"})
            ))
            .expect("valid"),
            json!({"url": "https://a.b", "renderJs": true, "mode": "pro"})
        );
        assert!(fetch_body(&args(json!({"url": "a.b"}))).is_err());
        assert!(fetch_body(&args(json!({}))).is_err());
    }

    #[test]
    fn renders_search_results() {
        let text = render_search(&json!({"results": [
            {"type": "text", "name": "Rust", "url": "https://rust-lang.org", "content": "A language", "favicon": "x"},
            {"type": "image", "name": "Logo", "url": "https://rust-lang.org/logo.png"}
        ]}));
        let parsed: Value = serde_json::from_str(&text).expect("json");
        assert_eq!(
            parsed,
            json!([
                {"title": "Rust", "url": "https://rust-lang.org", "content": "A language"},
                {"type": "image", "title": "Logo", "url": "https://rust-lang.org/logo.png"}
            ])
        );
        assert_eq!(render_search(&json!({"results": []})), "No results.");
    }

    #[test]
    fn renders_answer_and_fetch() {
        let text = render_answer(&json!({
            "answer": "42.",
            "sources": [{"name": "Guide", "url": "https://h.g", "snippet": "the\nanswer", "favicon": ""}]
        }));
        assert_eq!(
            text,
            "42.\n\nSources:\n[1] Guide <https://h.g>\n    the answer"
        );
        let text = render_fetch(&json!({
            "markdown": "# Hi", "favicon": "f", "images": [{"alt": "a", "url": "https://i"}]
        }));
        assert_eq!(text, "# Hi\n\nImages:\n- a <https://i>");
    }

    #[test]
    fn prices_calls_by_their_body() {
        let search = |a: Value, kind| usd(&search_body(&args(a), kind).expect("valid"));
        assert_eq!(search(json!({"q": "x"}), "searchResults"), 0.005);
        assert_eq!(
            search(json!({"q": "x", "depth": "flash"}), "sourcedAnswer"),
            0.006
        );
        assert_eq!(
            search(json!({"q": "x", "depth": "deep"}), "searchResults"),
            0.05
        );
        assert_eq!(
            search(json!({"q": "x", "depth": "deep"}), "sourcedAnswer"),
            0.055
        );
        let fetch = |a: Value| usd(&fetch_body(&args(a)).expect("valid"));
        assert_eq!(fetch(json!({"url": "https://a.b"})), 0.001);
        assert_eq!(
            fetch(json!({"url": "https://a.b", "render_js": true})),
            0.005
        );
        assert_eq!(fetch(json!({"url": "https://a.b", "mode": "pro"})), 0.005);
        assert_eq!(
            fetch(json!({"url": "https://a.b", "mode": "pro", "render_js": true})),
            0.01
        );
    }
}
