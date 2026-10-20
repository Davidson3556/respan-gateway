//! Tavily (<https://tavily.com>): web search with an optional answer, page
//! extraction, site crawls and site maps, from its API at
//! `https://api.tavily.com` (`POST /search`, `POST /extract`,
//! `POST /crawl`, `POST /map`), the key as a bearer token.

use std::time::Duration;

use async_trait::async_trait;
use respan_gateway_tool_vendor_kit::args::{
    object, optional_bool, optional_date, optional_enum, optional_int, optional_str,
    optional_str_list, pretty, required_str,
};
use respan_gateway_tool_vendor_kit::{
    Api, KeyIn, ToolError, ToolOutput, ToolSpec, ToolVendor, Tools, reqwest,
};
use serde_json::{Map, Value, json};

/// Tavily.
pub const VENDOR: ToolVendor = ToolVendor {
    id: "tavily",
    key_env: &["TAVILY_API_KEY"],
    key_in: KeyIn::Bearer,
    base_url: "https://api.tavily.com",
    tools: &TavilyTools,
};

/// Tavily's tools: `search`, `extract`, `crawl` and `map_site`.
#[derive(Debug, Clone, Copy)]
pub struct TavilyTools;

/// The most of one page's content a crawl's result quotes.
const PAGE_CAP: usize = 6000;
/// The most of a search result's raw content quoted.
const RAW_CAP: usize = 4000;
/// How long a crawl or map may take: past Tavily's own 150 s ceiling, which
/// the kit's 120 s client timeout would cut short.
const LONG: Duration = Duration::from_secs(160);

const TOOLS: &[ToolSpec] = &[
    ToolSpec {
        name: "search",
        description: "Search the web with Tavily and get ranked results: title, URL, the \
            relevant content and a relevance score each, optionally with an LLM-written answer \
            first. Billed to the Tavily account: 1 credit, 2 for `search_depth: advanced`.",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "query": {"type": "string", "description": "The search query."},
    "search_depth": {"type": "string", "enum": ["ultra-fast", "fast", "basic", "advanced"], "default": "basic", "description": "Latency against relevance: advanced returns the most relevant content and costs 2 credits; the others 1."},
    "topic": {"type": "string", "enum": ["general", "news", "finance"], "default": "general", "description": "The kind of search; news includes publication dates."},
    "max_results": {"type": "integer", "minimum": 1, "maximum": 20, "default": 10, "description": "At most this many results."},
    "time_range": {"type": "string", "enum": ["day", "week", "month", "year"], "description": "Only results from this long back to now."},
    "start_date": {"type": "string", "description": "Only results from this date on, YYYY-MM-DD."},
    "end_date": {"type": "string", "description": "Only results up to this date, YYYY-MM-DD."},
    "answer": {"type": "string", "enum": ["basic", "advanced"], "description": "Also write an answer to the query from the results: basic is quick, advanced more detailed."},
    "raw_content": {"type": "boolean", "default": false, "description": "Also return each result's whole page as markdown (cut when long)."},
    "include_domains": {"type": "array", "items": {"type": "string"}, "maxItems": 300, "description": "Only results from these domains."},
    "exclude_domains": {"type": "array", "items": {"type": "string"}, "maxItems": 150, "description": "No results from these domains."},
    "country": {"type": "string", "description": "Boost results from this country, by its lowercase English name, e.g. \"united states\". General topic only."},
    "include_images": {"type": "boolean", "default": false, "description": "Also return images related to the query (URL and description)."}
  },
  "required": ["query"]
}"#,
    },
    ToolSpec {
        name: "extract",
        description: "Extract 1 to 20 web pages with Tavily and get each as markdown; with \
            `query`, only the chunks most relevant to it. Pages that fail are listed with the \
            reason; the others still return. Billed to the Tavily account: 1 credit per 5 pages \
            extracted, 2 with `extract_depth: advanced`.",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "urls": {"type": "array", "items": {"type": "string"}, "minItems": 1, "maxItems": 20, "description": "The pages' URLs."},
    "query": {"type": "string", "description": "Return only the chunks of each page most relevant to this."},
    "chunks_per_source": {"type": "integer", "minimum": 1, "maximum": 5, "default": 3, "description": "With `query`: at most this many chunks (about 800 characters each) per page."},
    "extract_depth": {"type": "string", "enum": ["basic", "advanced"], "default": "basic", "description": "advanced: also tables and embedded content, slower and twice the cost."},
    "include_images": {"type": "boolean", "default": false, "description": "Also list each page's image URLs."}
  },
  "required": ["urls"]
}"#,
    },
    ToolSpec {
        name: "crawl",
        description: "Crawl a website with Tavily from a root URL, following links, and get \
            each page found as markdown (each cut when long). Slow: up to a couple of minutes. \
            Billed to the Tavily account: 1 credit per 10 pages found (2 with `instructions`) \
            plus 1 per 5 pages extracted (2 with `extract_depth: advanced`). Prefer `map_site` \
            to only list URLs, or `extract` for known pages.",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "url": {"type": "string", "description": "The root URL to start from, e.g. docs.tavily.com."},
    "instructions": {"type": "string", "description": "What to look for, in natural language, e.g. \"pages about the Python SDK\". Doubles the crawl's cost."},
    "max_depth": {"type": "integer", "minimum": 1, "maximum": 5, "default": 1, "description": "How many links away from the root to go."},
    "max_breadth": {"type": "integer", "minimum": 1, "maximum": 500, "default": 20, "description": "At most this many links followed per page."},
    "limit": {"type": "integer", "minimum": 1, "maximum": 100, "default": 50, "description": "Stop after this many pages."},
    "select_paths": {"type": "array", "items": {"type": "string"}, "description": "Only URLs whose path matches one of these regexes, e.g. \"/docs/.*\"."},
    "exclude_paths": {"type": "array", "items": {"type": "string"}, "description": "No URLs whose path matches one of these regexes."},
    "allow_external": {"type": "boolean", "default": true, "description": "Also return pages on other domains."},
    "chunks_per_source": {"type": "integer", "minimum": 1, "maximum": 5, "default": 3, "description": "With `instructions`: at most this many relevant chunks per page."},
    "extract_depth": {"type": "string", "enum": ["basic", "advanced"], "default": "basic", "description": "advanced: also tables and embedded content, slower and twice the extraction cost."}
  },
  "required": ["url"]
}"#,
    },
    ToolSpec {
        name: "map_site",
        description: "Map a website with Tavily: follow links from a root URL and list the URLs \
            found, without their content. Can take up to a couple of minutes. Billed to the \
            Tavily account: 1 credit per 10 pages found, 2 with `instructions`.",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "url": {"type": "string", "description": "The root URL to start from, e.g. docs.tavily.com."},
    "instructions": {"type": "string", "description": "What to look for, in natural language. Doubles the cost."},
    "max_depth": {"type": "integer", "minimum": 1, "maximum": 5, "default": 1, "description": "How many links away from the root to go."},
    "max_breadth": {"type": "integer", "minimum": 1, "maximum": 500, "default": 20, "description": "At most this many links followed per page."},
    "limit": {"type": "integer", "minimum": 1, "maximum": 500, "default": 50, "description": "Stop after this many pages."},
    "select_paths": {"type": "array", "items": {"type": "string"}, "description": "Only URLs whose path matches one of these regexes."},
    "exclude_paths": {"type": "array", "items": {"type": "string"}, "description": "No URLs whose path matches one of these regexes."},
    "allow_external": {"type": "boolean", "default": true, "description": "Also list URLs on other domains."}
  },
  "required": ["url"]
}"#,
    },
];

#[async_trait]
impl Tools for TavilyTools {
    fn list(&self) -> &'static [ToolSpec] {
        TOOLS
    }

    async fn call(&self, api: &Api, name: &str, arguments: Value) -> Result<ToolOutput, ToolError> {
        let args = object(&arguments)?;
        match name {
            "search" => {
                let response = api.post_json("search", &search_body(args)?).await?;
                Ok(billed(render_search(&response).into(), &response))
            }
            "extract" => {
                let response = api.post_json("extract", &extract_body(args)?).await?;
                Ok(billed(render_extract(&response).into(), &response))
            }
            "crawl" => {
                let response = long_post(api, "crawl", &site_body(args, true)?).await?;
                Ok(billed(render_crawl(&response).into(), &response))
            }
            "map_site" => {
                let response = long_post(api, "map", &site_body(args, false)?).await?;
                Ok(billed(render_map(&response).into(), &response))
            }
            _ => Err(ToolError::new(format!("no tool named {name:?}"))),
        }
    }
}

/// `output`, having used the credits Tavily reports in `usage.credits`,
/// which every body asks for with `include_usage`
/// (<https://docs.tavily.com/documentation/api-reference/endpoint/search>).
fn billed(output: ToolOutput, response: &Value) -> ToolOutput {
    match response.pointer("/usage/credits").and_then(Value::as_f64) {
        Some(credits) => output.with_usage("credits", credits),
        None => output,
    }
}

/// `POST` `body` to `path`, waiting up to [`LONG`].
async fn long_post(api: &Api, path: &str, body: &Value) -> Result<Value, ToolError> {
    let request = api
        .request(reqwest::Method::POST, path)
        .json(body)
        .timeout(LONG);
    api.json(request).await
}

/// `POST /search`'s body from the tool's arguments.
fn search_body(args: &Map<String, Value>) -> Result<Value, ToolError> {
    let mut body = Map::new();
    body.insert("query".into(), required_str(args, "query")?.into());
    body.insert("include_usage".into(), true.into());
    let depths = ["ultra-fast", "fast", "basic", "advanced"];
    if let Some(v) = optional_enum(args, "search_depth", &depths)? {
        body.insert("search_depth".into(), v.into());
    }
    if let Some(v) = optional_enum(args, "topic", &["general", "news", "finance"])? {
        body.insert("topic".into(), v.into());
    }
    if let Some(v) = optional_int(args, "max_results", 1, 20)? {
        body.insert("max_results".into(), v.into());
    }
    let ranges = ["day", "week", "month", "year"];
    if let Some(v) = optional_enum(args, "time_range", &ranges)? {
        body.insert("time_range".into(), v.into());
    }
    for key in ["start_date", "end_date"] {
        if let Some(v) = optional_date(args, key)? {
            body.insert(key.into(), v.into());
        }
    }
    if let Some(v) = optional_enum(args, "answer", &["basic", "advanced"])? {
        body.insert("include_answer".into(), v.into());
    }
    if optional_bool(args, "raw_content")? == Some(true) {
        body.insert("include_raw_content".into(), "markdown".into());
    }
    for key in ["include_domains", "exclude_domains"] {
        if let Some(v) = optional_str_list(args, key)? {
            body.insert(key.into(), json!(v));
        }
    }
    if let Some(v) = optional_str(args, "country")? {
        body.insert("country".into(), v.trim().to_lowercase().into());
    }
    if let Some(v) = optional_bool(args, "include_images")? {
        body.insert("include_images".into(), v.into());
        if v {
            body.insert("include_image_descriptions".into(), true.into());
        }
    }
    Ok(Value::Object(body))
}

/// `POST /extract`'s body from the tool's arguments.
fn extract_body(args: &Map<String, Value>) -> Result<Value, ToolError> {
    let urls =
        optional_str_list(args, "urls")?.ok_or_else(|| ToolError::new("`urls` is required"))?;
    if urls.len() > 20 {
        return Err(ToolError::new("`urls` holds at most 20 URLs"));
    }
    for url in &urls {
        http_url(url, "urls")?;
    }
    let mut body = Map::new();
    body.insert("urls".into(), json!(urls));
    body.insert("format".into(), "markdown".into());
    body.insert("include_usage".into(), true.into());
    let query = optional_str(args, "query")?;
    if let Some(v) = query {
        body.insert("query".into(), v.into());
    }
    if let Some(v) = optional_int(args, "chunks_per_source", 1, 5)? {
        if query.is_none() {
            return Err(ToolError::new("`chunks_per_source` needs `query`"));
        }
        body.insert("chunks_per_source".into(), v.into());
    }
    if let Some(v) = optional_enum(args, "extract_depth", &["basic", "advanced"])? {
        body.insert("extract_depth".into(), v.into());
    }
    if let Some(v) = optional_bool(args, "include_images")? {
        body.insert("include_images".into(), v.into());
    }
    Ok(Value::Object(body))
}

/// `POST /crawl`'s body (`crawl`) or `POST /map`'s from the tool's
/// arguments.
fn site_body(args: &Map<String, Value>, crawl: bool) -> Result<Value, ToolError> {
    let mut body = Map::new();
    body.insert("url".into(), required_str(args, "url")?.into());
    body.insert("include_usage".into(), true.into());
    let instructions = optional_str(args, "instructions")?;
    if let Some(v) = instructions {
        body.insert("instructions".into(), v.into());
    }
    if let Some(v) = optional_int(args, "max_depth", 1, 5)? {
        body.insert("max_depth".into(), v.into());
    }
    if let Some(v) = optional_int(args, "max_breadth", 1, 500)? {
        body.insert("max_breadth".into(), v.into());
    }
    let most = if crawl { 100 } else { 500 };
    if let Some(v) = optional_int(args, "limit", 1, most)? {
        body.insert("limit".into(), v.into());
    }
    for key in ["select_paths", "exclude_paths"] {
        if let Some(v) = optional_str_list(args, key)? {
            body.insert(key.into(), json!(v));
        }
    }
    if let Some(v) = optional_bool(args, "allow_external")? {
        body.insert("allow_external".into(), v.into());
    }
    if crawl {
        body.insert("format".into(), "markdown".into());
        if let Some(v) = optional_int(args, "chunks_per_source", 1, 5)? {
            if instructions.is_none() {
                return Err(ToolError::new("`chunks_per_source` needs `instructions`"));
            }
            body.insert("chunks_per_source".into(), v.into());
        }
        if let Some(v) = optional_enum(args, "extract_depth", &["basic", "advanced"])? {
            body.insert("extract_depth".into(), v.into());
        }
    }
    Ok(Value::Object(body))
}

fn http_url(url: &str, key: &str) -> Result<(), ToolError> {
    if url.starts_with("http://") || url.starts_with("https://") {
        Ok(())
    } else {
        Err(ToolError::new(format!(
            "`{key}` must hold http(s) URLs, not {url:?}"
        )))
    }
}

fn str_of<'a>(v: &'a Value, key: &str) -> Option<&'a str> {
    v.get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

fn array_of<'a>(v: &'a Value, key: &str) -> &'a [Value] {
    v.get(key)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default()
}

/// `text` up to `max` bytes, cut at a character, saying so.
fn cap(text: &str, max: usize) -> String {
    if text.len() <= max {
        return text.to_owned();
    }
    let mut end = max;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}… (cut)", &text[..end])
}

/// The answer if asked for, then the results as JSON (scores rounded, ids
/// and favicons dropped), then any images.
fn render_search(response: &Value) -> String {
    let mut parts = Vec::new();
    if let Some(answer) = str_of(response, "answer") {
        parts.push(format!("Answer: {answer}"));
    }
    let results: Vec<Value> = array_of(response, "results")
        .iter()
        .map(|r| {
            let mut kept = Map::new();
            for key in ["title", "url", "content", "published_date"] {
                if let Some(v) = str_of(r, key) {
                    kept.insert(key.into(), v.into());
                }
            }
            if let Some(score) = r.get("score").and_then(Value::as_f64) {
                kept.insert("score".into(), json!((score * 100.0).round() / 100.0));
            }
            if let Some(raw) = str_of(r, "raw_content") {
                kept.insert("raw_content".into(), cap(raw, RAW_CAP).into());
            }
            Value::Object(kept)
        })
        .collect();
    parts.push(if results.is_empty() {
        "No results.".into()
    } else {
        pretty(&Value::Array(results))
    });
    let images = array_of(response, "images");
    if !images.is_empty() {
        let mut out = String::from("Images:");
        for img in images {
            // An image is a URL, or `{url, description}` with descriptions on.
            let (url, description) = match img {
                Value::String(url) => (url.as_str(), None),
                _ => (
                    str_of(img, "url").unwrap_or_default(),
                    str_of(img, "description"),
                ),
            };
            match description {
                Some(d) => out.push_str(&format!("\n- {d} <{url}>")),
                None => out.push_str(&format!("\n- <{url}>")),
            }
        }
        parts.push(out);
    }
    parts.join("\n\n")
}

/// One page as markdown under its URL, with its image URLs.
fn render_page(page: &Value, max: Option<usize>) -> String {
    let url = str_of(page, "url").unwrap_or("?");
    let text = str_of(page, "raw_content").unwrap_or("(no content)");
    let text = max.map_or_else(|| text.to_owned(), |m| cap(text, m));
    let mut out = format!("# {url}\n\n{text}");
    let images: Vec<&str> = array_of(page, "images")
        .iter()
        .filter_map(Value::as_str)
        .collect();
    if !images.is_empty() {
        out.push_str("\n\nImages:");
        for img in images {
            out.push_str("\n- ");
            out.push_str(img);
        }
    }
    out
}

/// Each page, then the failures.
fn render_extract(response: &Value) -> String {
    let mut pages: Vec<String> = array_of(response, "results")
        .iter()
        .map(|p| render_page(p, None))
        .collect();
    let failed = array_of(response, "failed_results");
    if !failed.is_empty() {
        let mut out = String::from("Failed:");
        for f in failed {
            out.push_str(&format!(
                "\n- {}: {}",
                str_of(f, "url").unwrap_or("?"),
                str_of(f, "error").unwrap_or("unknown error")
            ));
        }
        pages.push(out);
    }
    if pages.is_empty() {
        return "Nothing was extracted.".into();
    }
    pages.join("\n\n---\n\n")
}

/// How many pages were crawled from where, then each page, cut when long.
fn render_crawl(response: &Value) -> String {
    let results = array_of(response, "results");
    if results.is_empty() {
        return "No pages were crawled.".into();
    }
    let mut pages = vec![format!(
        "{} pages crawled from {}.",
        results.len(),
        str_of(response, "base_url").unwrap_or("?")
    )];
    pages.extend(results.iter().map(|p| render_page(p, Some(PAGE_CAP))));
    pages.join("\n\n---\n\n")
}

/// The URLs found, one a line.
fn render_map(response: &Value) -> String {
    let urls: Vec<&str> = array_of(response, "results")
        .iter()
        .filter_map(Value::as_str)
        .collect();
    if urls.is_empty() {
        return "No URLs were found.".into();
    }
    format!(
        "{} URLs found from {}:\n{}",
        urls.len(),
        str_of(response, "base_url").unwrap_or("?"),
        urls.join("\n")
    )
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
        let body = search_body(&args(json!({
            "query": "rust", "search_depth": "advanced", "topic": "news", "max_results": 5,
            "start_date": "2025-01-01", "answer": "basic", "raw_content": true,
            "include_domains": ["rust-lang.org"], "country": "United States", "include_images": true
        })))
        .expect("valid");
        assert_eq!(
            body,
            json!({
                "query": "rust", "include_usage": true, "search_depth": "advanced", "topic": "news",
                "max_results": 5, "start_date": "2025-01-01", "include_answer": "basic",
                "include_raw_content": "markdown", "include_domains": ["rust-lang.org"],
                "country": "united states", "include_images": true,
                "include_image_descriptions": true
            })
        );
        assert_eq!(
            search_body(&args(json!({"query": "x", "raw_content": false}))).expect("valid"),
            json!({"query": "x", "include_usage": true})
        );
    }

    #[test]
    fn search_body_rejects_bad_arguments() {
        assert!(search_body(&args(json!({}))).is_err());
        assert!(search_body(&args(json!({"query": "x", "search_depth": "deep"}))).is_err());
        assert!(search_body(&args(json!({"query": "x", "max_results": 21}))).is_err());
        assert!(search_body(&args(json!({"query": "x", "time_range": "d"}))).is_err());
        assert!(search_body(&args(json!({"query": "x", "end_date": "May 1"}))).is_err());
        assert!(search_body(&args(json!({"query": "x", "answer": true}))).is_err());
    }

    #[test]
    fn extract_body_maps_and_validates() {
        assert_eq!(
            extract_body(&args(json!({
                "urls": ["https://a.b"], "query": "price", "chunks_per_source": 2,
                "extract_depth": "advanced"
            })))
            .expect("valid"),
            json!({
                "urls": ["https://a.b"], "format": "markdown", "include_usage": true, "query": "price",
                "chunks_per_source": 2, "extract_depth": "advanced"
            })
        );
        assert!(extract_body(&args(json!({"urls": []}))).is_err());
        assert!(extract_body(&args(json!({"urls": ["a.b"]}))).is_err());
        assert!(
            extract_body(&args(
                json!({"urls": ["https://a.b"], "chunks_per_source": 2})
            ))
            .is_err()
        );
        let many: Vec<String> = (0..21).map(|i| format!("https://a/{i}")).collect();
        assert!(extract_body(&args(json!({ "urls": many }))).is_err());
    }

    #[test]
    fn site_body_maps_and_validates() {
        assert_eq!(
            site_body(
                &args(json!({
                    "url": "docs.tavily.com", "instructions": "SDK pages", "max_depth": 2,
                    "limit": 10, "select_paths": ["/docs/.*"], "allow_external": false,
                    "chunks_per_source": 1
                })),
                true
            )
            .expect("valid"),
            json!({
                "url": "docs.tavily.com", "include_usage": true, "instructions": "SDK pages",
                "max_depth": 2, "limit": 10, "select_paths": ["/docs/.*"], "allow_external": false,
                "format": "markdown", "chunks_per_source": 1
            })
        );
        assert_eq!(
            site_body(
                &args(json!({"url": "a.b", "limit": 300, "extract_depth": "advanced"})),
                false
            )
            .expect("valid"),
            json!({"url": "a.b", "include_usage": true, "limit": 300})
        );
        assert!(site_body(&args(json!({})), false).is_err());
        assert!(site_body(&args(json!({"url": "a.b", "limit": 300})), true).is_err());
        assert!(site_body(&args(json!({"url": "a.b", "max_depth": 6})), false).is_err());
        assert!(site_body(&args(json!({"url": "a.b", "chunks_per_source": 2})), true).is_err());
    }

    #[test]
    fn renders_search() {
        let text = render_search(&json!({
            "query": "q", "answer": "Rust is a language.", "response_time": 1.2,
            "images": [{"url": "https://i/1.png", "description": "a crab"}, "https://i/2.png"],
            "results": [{"title": "Rust", "url": "https://rust-lang.org", "content": "A language",
                "score": 0.81234, "raw_content": null, "favicon": "f", "id": "x"}],
            "request_id": "r"
        }));
        let (answer, rest) = text.split_once("\n\n").expect("parts");
        assert_eq!(answer, "Answer: Rust is a language.");
        let (results, images) = rest.split_once("\n\nImages:").expect("images");
        let parsed: Value = serde_json::from_str(results).expect("json");
        assert_eq!(
            parsed,
            json!([{"title": "Rust", "url": "https://rust-lang.org", "content": "A language", "score": 0.81}])
        );
        assert_eq!(images, "\n- a crab <https://i/1.png>\n- <https://i/2.png>");
        assert_eq!(
            render_search(&json!({"results": [], "images": []})),
            "No results."
        );
    }

    #[test]
    fn renders_extract() {
        let text = render_extract(&json!({
            "results": [{"url": "https://a", "raw_content": "Hello", "images": ["https://a/i.png"]}],
            "failed_results": [{"url": "https://c", "error": "timeout"}],
            "response_time": 0.5, "request_id": "r"
        }));
        assert_eq!(
            text,
            "# https://a\n\nHello\n\nImages:\n- https://a/i.png\n\n---\n\nFailed:\n- https://c: timeout"
        );
        assert_eq!(
            render_extract(&json!({"results": [], "failed_results": []})),
            "Nothing was extracted."
        );
    }

    #[test]
    fn renders_crawl_and_map() {
        let long = "x".repeat(PAGE_CAP + 10);
        let text = render_crawl(&json!({
            "base_url": "docs.tavily.com",
            "results": [{"url": "https://docs.tavily.com/a", "raw_content": long, "favicon": "f"}]
        }));
        assert!(text.starts_with(
            "1 pages crawled from docs.tavily.com.\n\n---\n\n# https://docs.tavily.com/a\n\nxxx"
        ));
        assert!(text.ends_with("x… (cut)"));
        assert_eq!(
            render_map(&json!({"base_url": "a.b", "results": ["https://a.b/", "https://a.b/c"]})),
            "2 URLs found from a.b:\nhttps://a.b/\nhttps://a.b/c"
        );
        assert_eq!(render_map(&json!({"results": []})), "No URLs were found.");
    }

    #[test]
    fn reads_credits_used() {
        let used = billed("x".into(), &json!({"results": [], "usage": {"credits": 2}}));
        assert_eq!(used, ToolOutput::from("x").with_usage("credits", 2.0));
        assert!(billed("x".into(), &json!({"results": []})).usage.is_empty());
    }
}
