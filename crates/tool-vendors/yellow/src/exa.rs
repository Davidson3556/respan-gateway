//! Exa (<https://exa.ai>): web search and page contents from
//! `https://api.exa.ai` (`POST /search`, `POST /contents`), with a bearer key.

use async_trait::async_trait;
use respan_gateway_tool_vendor_kit::args::{object, optional_int, pretty, required_str};
use respan_gateway_tool_vendor_kit::reqwest::Url;
use respan_gateway_tool_vendor_kit::{
    Api, KeyIn, ToolError, ToolOutput, ToolSpec, ToolVendor, Tools,
};
use serde_json::{Map, Value, json};

/// Exa.
pub const VENDOR: ToolVendor = ToolVendor {
    id: "exa",
    key_env: &["EXA_API_KEY"],
    key_in: KeyIn::Bearer,
    base_url: "https://api.exa.ai",
    tools: &ExaTools,
};

/// Exa's `search` and `get_contents` tools.
#[derive(Debug, Clone, Copy)]
pub struct ExaTools;

const TOOLS: &[ToolSpec] = &[
    ToolSpec {
        name: "search",
        description: "Search the web with Exa and get ranked pages with relevant highlights. \
            Each call is billed to the Exa account.",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "query": {"type": "string", "description": "The search query."},
    "num_results": {"type": "integer", "minimum": 1, "maximum": 100, "default": 10, "description": "At most this many results."}
  },
  "required": ["query"]
}"#,
    },
    ToolSpec {
        name: "get_contents",
        description: "Get the text of web pages with Exa. Each page is billed to the Exa \
            account; the result includes a status for each URL, including fetch failures.",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "urls": {"type": "array", "items": {"type": "string"}, "minItems": 1, "maxItems": 100, "description": "HTTP or HTTPS page URLs to read."},
    "max_characters": {"type": "integer", "minimum": 1, "maximum": 1000000, "default": 10000, "description": "Maximum text characters per page."}
  },
  "required": ["urls"]
}"#,
    },
];

#[async_trait]
impl Tools for ExaTools {
    fn list(&self) -> &'static [ToolSpec] {
        TOOLS
    }

    async fn call(&self, api: &Api, name: &str, arguments: Value) -> Result<ToolOutput, ToolError> {
        let args = object(&arguments)?;
        let (path, body, fields) = match name {
            "search" => (
                "search",
                search_body(args)?,
                &["title", "url", "highlights", "publishedDate"][..],
            ),
            "get_contents" => (
                "contents",
                contents_body(args)?,
                &["title", "url", "text"][..],
            ),
            _ => return Err(ToolError::new(format!("no tool named {name:?}"))),
        };
        let response = api.post_json(path, &body).await?;
        render(&response, fields, name == "get_contents")
    }
}

fn search_body(args: &Map<String, Value>) -> Result<Value, ToolError> {
    let mut body = json!({
        "query": required_str(args, "query")?,
        "contents": {"highlights": true}
    });
    if let Some(n) = optional_int(args, "num_results", 1, 100)? {
        body["numResults"] = json!(n);
    }
    Ok(body)
}

fn contents_body(args: &Map<String, Value>) -> Result<Value, ToolError> {
    let urls = args
        .get("urls")
        .and_then(Value::as_array)
        .ok_or_else(|| ToolError::new("`urls` must be an array of 1 to 100 URLs"))?;
    if urls.is_empty() || urls.len() > 100 {
        return Err(ToolError::new("`urls` must be an array of 1 to 100 URLs"));
    }
    for item in urls {
        let url = item
            .as_str()
            .ok_or_else(|| ToolError::new("`urls` must contain HTTP(S) URLs"))?;
        let valid = Url::parse(url)
            .ok()
            .is_some_and(|u| matches!(u.scheme(), "http" | "https") && u.host().is_some());
        if !valid || url.len() > 2048 {
            return Err(ToolError::new(
                "`urls` must contain HTTP(S) URLs up to 2048 characters",
            ));
        }
    }
    let max = optional_int(args, "max_characters", 1, 1_000_000)?.unwrap_or(10_000);
    Ok(json!({"ids": urls, "text": {"maxCharacters": max}}))
}

fn render(response: &Value, fields: &[&str], contents: bool) -> Result<ToolOutput, ToolError> {
    let results = response
        .get("results")
        .and_then(Value::as_array)
        .ok_or_else(|| ToolError::new("the API answered without results"))?;
    let results: Vec<Value> = results
        .iter()
        .map(|result| {
            let mut kept = Map::new();
            for field in fields {
                if let Some(value) = result.get(*field) {
                    kept.insert((*field).into(), value.clone());
                }
            }
            Value::Object(kept)
        })
        .collect();
    let mut output = json!({"results": results});
    let extra = if contents { "statuses" } else { "notices" };
    if let Some(value) = response.get(extra) {
        output[extra] = value.clone();
    }
    let mut output = ToolOutput::from(pretty(&output));
    if let Some(total) = response
        .pointer("/costDollars/total")
        .and_then(Value::as_f64)
    {
        output = output.with_usage("usd", total);
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use respan_gateway_tool_vendor_kit::Content;

    fn args(value: Value) -> Map<String, Value> {
        value.as_object().expect("object").clone()
    }

    #[test]
    fn maps_and_checks_requests() {
        assert_eq!(
            search_body(&args(json!({"query": "Rust", "num_results": 2}))).expect("search body"),
            json!({"query": "Rust", "numResults": 2, "contents": {"highlights": true}})
        );
        assert!(search_body(&args(json!({}))).is_err());
        assert!(search_body(&args(json!({"query": "Rust", "num_results": 101}))).is_err());
        assert_eq!(
            contents_body(&args(json!({"urls": ["https://example.com"]}))).expect("contents body"),
            json!({"ids": ["https://example.com"], "text": {"maxCharacters": 10000}})
        );
        assert!(contents_body(&args(json!({"urls": []}))).is_err());
        assert!(contents_body(&args(json!({"urls": ["file:///etc/passwd"]}))).is_err());
        assert!(
            contents_body(&args(
                json!({"urls": ["https://example.com"], "max_characters": 0})
            ))
            .is_err()
        );
    }

    #[test]
    fn keeps_useful_results_failures_and_usage() {
        let search = render(
            &json!({"results": [{"title": "Rust", "url": "https://rust-lang.org", "highlights": ["A language"], "favicon": "skip"}], "costDollars": {"total": 0.007}}),
            &["title", "url", "highlights", "publishedDate"],
            false,
        ).expect("search output");
        assert_eq!(search.usage.get("usd"), Some(&0.007));
        let Content::Text(text) = &search.content[0] else {
            panic!("text")
        };
        assert_eq!(
            serde_json::from_str::<Value>(text).expect("search JSON"),
            json!({"results": [{"title": "Rust", "url": "https://rust-lang.org", "highlights": ["A language"]}]})
        );

        let contents = render(
            &json!({"results": [{"title": "Rust", "url": "https://rust-lang.org", "text": "Hello"}], "statuses": [{"id": "https://rust-lang.org", "status": "success"}, {"id": "https://bad.example", "status": "error", "error": {"tag": "FETCH_FAILED"}}]}),
            &["title", "url", "text"],
            true,
        ).expect("contents output");
        assert!(contents.usage.is_empty());
        let Content::Text(text) = &contents.content[0] else {
            panic!("text")
        };
        let value: Value = serde_json::from_str(text).expect("contents JSON");
        assert_eq!(value["statuses"][1]["status"], "error");
        assert_eq!(value["statuses"][1]["error"]["tag"], "FETCH_FAILED");
        assert!(render(&json!({}), &[], false).is_err());
    }
}
