//! Reducto (<https://reducto.ai>): document parsing (OCR, layout, tables)
//! into markdown, schema-driven field extraction and section splitting, from
//! its API at `https://platform.reducto.ai` (`POST /parse`, `POST /extract`,
//! `POST /split`, all synchronous), the key as a bearer token. A result too
//! large to answer inline comes back as `{"type": "url", "url": ...}`, a
//! presigned link fetched here without the key.

use async_trait::async_trait;
use respan_gateway_tool_vendor_kit::args::{
    object, optional_bool, optional_enum, optional_int, optional_str, pretty, required_str,
};
use respan_gateway_tool_vendor_kit::{
    Api, KeyIn, ToolError, ToolOutput, ToolSpec, ToolVendor, Tools,
};
use serde_json::{Map, Value, json};

/// Reducto.
pub const VENDOR: ToolVendor = ToolVendor {
    id: "reducto",
    key_env: &["REDUCTO_API_KEY"],
    key_in: KeyIn::Bearer,
    base_url: "https://platform.reducto.ai",
    tools: &ReductoTools,
};

/// Reducto's tools: `parse_document`, `extract_fields` and `split_document`.
#[derive(Debug, Clone, Copy)]
pub struct ReductoTools;

/// The most of a parsed document's text a result carries by default.
const DEFAULT_MAX_CHARS: u64 = 50_000;

const CHUNK_MODES: &[&str] = &["disabled", "variable", "section", "page", "page_sections"];
const TABLE_FORMATS: &[&str] = &["dynamic", "md", "html", "csv"];

const TOOLS: &[ToolSpec] = &[
    ToolSpec {
        name: "parse_document",
        description: "Parse a document (PDF, image, DOCX, XLSX, PPTX and more, by URL) with \
            Reducto's OCR and layout models into markdown: text, tables and figure \
            descriptions in reading order. Returns a job id that `extract_fields` and \
            `split_document` take as `jobid://<id>` to reuse this parse. Billed per page to the \
            Reducto account (spreadsheets per cell); limit `page_start`/`page_end` on long \
            documents. Results are kept by Reducto for 24 hours.",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "document_url": {"type": "string", "description": "The document: a public or presigned http(s) URL, a reducto:// file id, or jobid://<id> from an earlier parse."},
    "page_start": {"type": "integer", "minimum": 1, "description": "First page to parse, from 1."},
    "page_end": {"type": "integer", "minimum": 1, "description": "Last page to parse, inclusive."},
    "chunk_mode": {"type": "string", "enum": ["disabled", "variable", "section", "page", "page_sections"], "default": "disabled", "description": "How to cut the text: disabled: one piece; variable: semantic chunks of about 250-1500 characters (for retrieval); section: at headings; page: one per page; page_sections: by page, then section."},
    "table_format": {"type": "string", "enum": ["dynamic", "md", "html", "csv"], "default": "dynamic", "description": "Tables as markdown, HTML (for merged cells), CSV, or dynamic: markdown or HTML by complexity."},
    "page_markers": {"type": "boolean", "default": false, "description": "Mark where each page starts in the text."},
    "max_chars": {"type": "integer", "minimum": 1000, "maximum": 1000000, "default": 50000, "description": "Cut the returned text after this many characters."}
  },
  "required": ["document_url"]
}"#,
    },
    ToolSpec {
        name: "extract_fields",
        description: "Extract specific fields from a document (by URL, or jobid://<id> from \
            `parse_document`) with Reducto, as JSON matching a JSON Schema you give: field \
            names and descriptions guide it. Billed per page to the Reducto account; \
            `deep_extract` is an agentic mode costing about twice as much and slower, for long \
            or array-heavy documents.",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "document_url": {"type": "string", "description": "The document: a public or presigned http(s) URL, a reducto:// file id, or jobid://<id> from parse_document."},
    "schema": {"type": "object", "description": "A JSON Schema (type object) of the fields to extract, each with a description, e.g. {\"type\": \"object\", \"properties\": {\"total\": {\"type\": \"number\", \"description\": \"Total amount due\"}}}."},
    "instructions": {"type": "string", "description": "Context for the extraction, e.g. what the document is and how to treat ambiguous values."},
    "page_start": {"type": "integer", "minimum": 1, "description": "First page to read, from 1."},
    "page_end": {"type": "integer", "minimum": 1, "description": "Last page to read, inclusive."},
    "deep_extract": {"type": "boolean", "default": false, "description": "Iteratively refine the output for higher accuracy; slower and costlier."}
  },
  "required": ["document_url", "schema"]
}"#,
    },
    ToolSpec {
        name: "split_document",
        description: "Find which pages of a document (by URL, or jobid://<id> from \
            `parse_document`) belong to each section you describe, with Reducto, e.g. to locate \
            the financial statements in an annual report before parsing just those pages. \
            Billed per page to the Reducto account.",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "document_url": {"type": "string", "description": "The document: a public or presigned http(s) URL, a reducto:// file id, or jobid://<id> from parse_document."},
    "sections": {"type": "array", "minItems": 1, "items": {"type": "object", "properties": {"name": {"type": "string"}, "description": {"type": "string", "description": "What pages of this section contain."}}, "required": ["name", "description"]}, "description": "The sections to find."},
    "rules": {"type": "string", "description": "How to split, in plain language, e.g. \"each page belongs to exactly one section\"."},
    "allow_page_overlap": {"type": "boolean", "default": true, "description": "Whether a page may belong to more than one section."}
  },
  "required": ["document_url", "sections"]
}"#,
    },
];

#[async_trait]
impl Tools for ReductoTools {
    fn list(&self) -> &'static [ToolSpec] {
        TOOLS
    }

    async fn call(&self, api: &Api, name: &str, arguments: Value) -> Result<ToolOutput, ToolError> {
        let args = object(&arguments)?;
        match name {
            "parse_document" => {
                let body = parse_body(args)?;
                let max =
                    optional_int(args, "max_chars", 1000, 1_000_000)?.unwrap_or(DEFAULT_MAX_CHARS);
                let mut response = api.post_json("parse", &body).await?;
                resolve(api, &mut response).await?;
                Ok(used(render_parse(&response, max as usize), &response))
            }
            "extract_fields" => {
                let body = extract_body(args)?;
                let mut response = api.post_json("extract", &body).await?;
                resolve(api, &mut response).await?;
                Ok(used(render_extract(&response), &response))
            }
            "split_document" => {
                let body = split_body(args)?;
                let mut response = api.post_json("split", &body).await?;
                resolve(api, &mut response).await?;
                Ok(used(render_split(&response), &response))
            }
            _ => Err(ToolError::new(format!("no tool named {name:?}"))),
        }
    }
}

/// `text`, having used what the response's `usage` says: `num_pages` as
/// `pages` and, on accounts billed in credits, `credits` (absent on the
/// newer pricing model). <https://docs.reducto.ai/api-reference/parse>
fn used(text: String, response: &Value) -> ToolOutput {
    let mut out = ToolOutput::from(text);
    for (unit, key) in [("pages", "/usage/num_pages"), ("credits", "/usage/credits")] {
        if let Some(n) = response.pointer(key).and_then(Value::as_f64) {
            out = out.with_usage(unit, n);
        }
    }
    out
}

/// The URL a `{"type": "url"}` result points at, if it is one.
fn url_result(response: &Value) -> Option<&str> {
    let result = response.get("result")?;
    (result.get("type").and_then(Value::as_str) == Some("url"))
        .then(|| result.get("url").and_then(Value::as_str))
        .flatten()
}

/// Replace a URL result with what its link holds. The link is presigned, so
/// it goes without the key, which storage would refuse beside a signature.
async fn resolve(api: &Api, response: &mut Value) -> Result<(), ToolError> {
    let Some(url) = url_result(response).map(str::to_owned) else {
        return Ok(());
    };
    let fetched = api.json(api.client().get(url)).await?;
    response["result"] = fetched;
    Ok(())
}

/// `document_url` as Reducto's `input`: a URL, a file id or a job id.
fn document(args: &Map<String, Value>) -> Result<&str, ToolError> {
    let input = required_str(args, "document_url")?.trim();
    let ok = ["http://", "https://", "reducto://", "jobid://"]
        .iter()
        .any(|p| input.starts_with(p));
    if ok {
        Ok(input)
    } else {
        Err(ToolError::new(format!(
            "`document_url` must be an http(s) URL, a reducto:// file id or a jobid://, not {input:?}"
        )))
    }
}

/// `page_start`/`page_end` as a `page_range`, if either is there.
fn page_range(args: &Map<String, Value>) -> Result<Option<Value>, ToolError> {
    let start = optional_int(args, "page_start", 1, 100_000)?;
    let end = optional_int(args, "page_end", 1, 100_000)?;
    if let (Some(s), Some(e)) = (start, end)
        && e < s
    {
        return Err(ToolError::new("`page_end` must not be before `page_start`"));
    }
    if start.is_none() && end.is_none() {
        return Ok(None);
    }
    let mut range = Map::new();
    if let Some(s) = start {
        range.insert("start".into(), s.into());
    }
    if let Some(e) = end {
        range.insert("end".into(), e.into());
    }
    Ok(Some(Value::Object(range)))
}

/// `POST /parse`'s body from the tool's arguments.
fn parse_body(args: &Map<String, Value>) -> Result<Value, ToolError> {
    let mut body = Map::new();
    body.insert("input".into(), document(args)?.into());
    if let Some(mode) = optional_enum(args, "chunk_mode", CHUNK_MODES)? {
        body.insert(
            "retrieval".into(),
            json!({"chunking": {"chunk_mode": mode}}),
        );
    }
    let mut formatting = Map::new();
    if let Some(v) = optional_enum(args, "table_format", TABLE_FORMATS)? {
        formatting.insert("table_output_format".into(), v.into());
    }
    if let Some(v) = optional_bool(args, "page_markers")? {
        formatting.insert("add_page_markers".into(), v.into());
    }
    if !formatting.is_empty() {
        body.insert("formatting".into(), Value::Object(formatting));
    }
    if let Some(range) = page_range(args)? {
        body.insert("settings".into(), json!({"page_range": range}));
    }
    Ok(Value::Object(body))
}

/// `POST /extract`'s body from the tool's arguments.
fn extract_body(args: &Map<String, Value>) -> Result<Value, ToolError> {
    let input = document(args)?;
    let schema = match args.get("schema") {
        Some(Value::Object(s)) if !s.is_empty() => Value::Object(s.clone()),
        Some(Value::String(s)) => match serde_json::from_str::<Value>(s) {
            Ok(v @ Value::Object(_)) => v,
            _ => return Err(ToolError::new("`schema` must be a JSON Schema object")),
        },
        None | Some(Value::Null) => return Err(ToolError::new("`schema` is required")),
        Some(_) => return Err(ToolError::new("`schema` must be a JSON Schema object")),
    };
    let mut instructions = Map::new();
    instructions.insert("schema".into(), schema);
    if let Some(v) = optional_str(args, "instructions")? {
        instructions.insert("system_prompt".into(), v.into());
    }
    let mut settings = Map::new();
    if let Some(v) = optional_bool(args, "deep_extract")? {
        settings.insert("deep_extract".into(), v.into());
    }
    if let Some(range) = page_range(args)? {
        settings.insert("page_range".into(), range);
    }
    let mut body = Map::new();
    body.insert("input".into(), input.into());
    body.insert("instructions".into(), Value::Object(instructions));
    if !settings.is_empty() {
        body.insert("settings".into(), Value::Object(settings));
    }
    Ok(Value::Object(body))
}

/// `POST /split`'s body from the tool's arguments.
fn split_body(args: &Map<String, Value>) -> Result<Value, ToolError> {
    let input = document(args)?;
    let invalid = || ToolError::new("`sections` must be an array of {name, description} objects");
    let sections = match args.get("sections") {
        Some(Value::Array(items)) if !items.is_empty() => items,
        None | Some(Value::Null) => return Err(ToolError::new("`sections` is required")),
        _ => return Err(invalid()),
    };
    let sections: Vec<Value> = sections
        .iter()
        .map(|s| {
            let s = s.as_object().ok_or_else(invalid)?;
            Ok(json!({
                "name": required_str(s, "name")?,
                "description": required_str(s, "description")?
            }))
        })
        .collect::<Result<_, ToolError>>()?;
    let mut body = Map::new();
    body.insert("input".into(), input.into());
    body.insert("split_description".into(), Value::Array(sections));
    if let Some(v) = optional_str(args, "rules")? {
        body.insert("split_rules".into(), v.into());
    }
    if let Some(v) = optional_bool(args, "allow_page_overlap")? {
        body.insert("settings".into(), json!({"allow_page_overlap": v}));
    }
    Ok(Value::Object(body))
}

fn str_of<'a>(v: &'a Value, key: &str) -> Option<&'a str> {
    v.get(key).and_then(Value::as_str).filter(|s| !s.is_empty())
}

/// The job id, page count and Studio link, as one line.
fn job_line(response: &Value) -> String {
    let mut parts = Vec::new();
    if let Some(id) = str_of(response, "job_id") {
        parts.push(format!("job jobid://{id}"));
    }
    if let Some(n) = response.pointer("/usage/num_pages").and_then(Value::as_u64) {
        parts.push(format!("{n} page{}", if n == 1 { "" } else { "s" }));
    }
    if let Some(link) = str_of(response, "studio_link") {
        parts.push(link.to_owned());
    }
    parts.join(" · ")
}

/// `text` up to `max` bytes, cut at a character, and whether it was cut.
fn cap(text: &str, max: usize) -> (&str, bool) {
    if text.len() <= max {
        return (text, false);
    }
    let mut end = max;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    (&text[..end], true)
}

/// The parsed document's chunks as markdown, between rules when there are
/// several, under the job line; blocks and boxes are dropped. The result may
/// be inline (`{"type": "full", "chunks"}`) or fetched from a URL result,
/// which holds the chunks or the result around them.
fn render_parse(response: &Value, max: usize) -> String {
    let result = response.get("result").unwrap_or(&Value::Null);
    let chunks = match result {
        Value::Array(chunks) => chunks.as_slice(),
        other => other
            .get("chunks")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or_default(),
    };
    let text = chunks
        .iter()
        .filter_map(|c| str_of(c, "content").map(str::trim))
        .filter(|c| !c.is_empty())
        .collect::<Vec<_>>()
        .join("\n\n---\n\n");
    let mut out = job_line(response);
    if !out.is_empty() {
        out.push_str("\n\n");
    }
    if text.is_empty() {
        out.push_str("The document had no extractable content.");
        return out;
    }
    let (kept, cut) = cap(&text, max);
    out.push_str(kept);
    if cut {
        out.push_str(&format!(
            "\n\n[cut at {max} of {} characters; parse fewer pages or raise `max_chars`]",
            text.len()
        ));
    }
    out
}

/// The extracted fields as JSON: one document's object itself, not a list
/// of one.
fn render_extract(response: &Value) -> String {
    let result = match response.get("result") {
        Some(Value::Array(items)) if items.len() == 1 => &items[0],
        Some(v) => v,
        None => &Value::Null,
    };
    let mut out = job_line(response);
    if let Some(conf) = str_of(response, "confidence") {
        out.push_str(&format!(" · confidence {conf}"));
        if let Some(reason) = str_of(response, "confidence_reason") {
            out.push_str(&format!(": {reason}"));
        }
    }
    if !out.is_empty() {
        out.push_str("\n\n");
    }
    out.push_str(&pretty(result));
    out
}

/// Pages as numbers: plain, or deep split's `{page_number, evidence}`.
fn pages(v: &Value) -> Vec<u64> {
    v.get("pages")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|p| {
            p.as_u64()
                .or_else(|| p.get("page_number").and_then(Value::as_u64))
        })
        .collect()
}

/// Pages as ranges, e.g. `1-3, 7`.
fn ranges(pages: &[u64]) -> String {
    let mut out: Vec<String> = Vec::new();
    let mut i = 0;
    while i < pages.len() {
        let start = pages[i];
        let mut end = start;
        while i + 1 < pages.len() && pages[i + 1] == end + 1 {
            i += 1;
            end = pages[i];
        }
        out.push(if start == end {
            start.to_string()
        } else {
            format!("{start}-{end}")
        });
        i += 1;
    }
    if out.is_empty() {
        "none".into()
    } else {
        out.join(", ")
    }
}

/// Each section's pages, with its confidence and partitions.
fn render_split(response: &Value) -> String {
    let splits = response
        .pointer("/result/splits")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    let mut out = job_line(response);
    if !out.is_empty() {
        out.push_str("\n\n");
    }
    if splits.is_empty() {
        out.push_str("No sections were found.");
        return out;
    }
    let lines: Vec<String> = splits
        .iter()
        .map(|s| {
            let mut line = format!(
                "- {}: pages {}",
                str_of(s, "name").unwrap_or("?"),
                ranges(&pages(s))
            );
            if let Some(conf) = str_of(s, "conf") {
                line.push_str(&format!(" ({conf} confidence)"));
            }
            for p in s
                .get("partitions")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                line.push_str(&format!(
                    "\n  - {}: pages {}",
                    str_of(p, "name").unwrap_or("?"),
                    ranges(&pages(p))
                ));
            }
            line
        })
        .collect();
    out.push_str(&lines.join("\n"));
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
    fn parse_body_maps_arguments() {
        let body = parse_body(&args(json!({
            "document_url": "https://a.b/doc.pdf", "chunk_mode": "page", "table_format": "html",
            "page_markers": true, "page_start": 2, "page_end": 5, "max_chars": 9000
        })))
        .expect("valid");
        assert_eq!(
            body,
            json!({
                "input": "https://a.b/doc.pdf",
                "retrieval": {"chunking": {"chunk_mode": "page"}},
                "formatting": {"table_output_format": "html", "add_page_markers": true},
                "settings": {"page_range": {"start": 2, "end": 5}}
            })
        );
        assert_eq!(
            parse_body(&args(json!({"document_url": "jobid://abc"}))).expect("valid"),
            json!({"input": "jobid://abc"})
        );
    }

    #[test]
    fn parse_body_rejects_bad_arguments() {
        assert!(parse_body(&args(json!({}))).is_err());
        assert!(parse_body(&args(json!({"document_url": "/tmp/a.pdf"}))).is_err());
        assert!(
            parse_body(&args(
                json!({"document_url": "https://a", "chunk_mode": "x"})
            ))
            .is_err()
        );
        assert!(
            parse_body(&args(
                json!({"document_url": "https://a", "page_start": 5, "page_end": 2})
            ))
            .is_err()
        );
        assert!(parse_body(&args(json!({"document_url": "https://a", "page_start": 0}))).is_err());
    }

    #[test]
    fn extract_body_maps_and_validates() {
        let schema = json!({"type": "object", "properties": {"total": {"type": "number"}}});
        let body = extract_body(&args(json!({
            "document_url": "reducto://f1", "schema": schema, "instructions": "An invoice.",
            "deep_extract": true, "page_end": 3
        })))
        .expect("valid");
        assert_eq!(
            body,
            json!({
                "input": "reducto://f1",
                "instructions": {"schema": schema, "system_prompt": "An invoice."},
                "settings": {"deep_extract": true, "page_range": {"end": 3}}
            })
        );
        let from_text = extract_body(&args(
            json!({"document_url": "https://a", "schema": schema.to_string()}),
        ))
        .expect("valid");
        assert_eq!(from_text["instructions"]["schema"], schema);
        assert!(extract_body(&args(json!({"document_url": "https://a"}))).is_err());
        assert!(extract_body(&args(json!({"document_url": "https://a", "schema": {}}))).is_err());
        assert!(extract_body(&args(json!({"document_url": "https://a", "schema": "no"}))).is_err());
        assert!(extract_body(&args(json!({"schema": schema}))).is_err());
    }

    #[test]
    fn split_body_maps_and_validates() {
        let body = split_body(&args(json!({
            "document_url": "https://a",
            "sections": [{"name": "Summary", "description": "Executive summary"}],
            "rules": "One section per page.", "allow_page_overlap": false
        })))
        .expect("valid");
        assert_eq!(
            body,
            json!({
                "input": "https://a",
                "split_description": [{"name": "Summary", "description": "Executive summary"}],
                "split_rules": "One section per page.",
                "settings": {"allow_page_overlap": false}
            })
        );
        assert!(split_body(&args(json!({"document_url": "https://a"}))).is_err());
        assert!(split_body(&args(json!({"document_url": "https://a", "sections": []}))).is_err());
        assert!(
            split_body(&args(
                json!({"document_url": "https://a", "sections": [{"name": "x"}]})
            ))
            .is_err()
        );
        assert!(
            split_body(&args(
                json!({"document_url": "https://a", "sections": ["x"]})
            ))
            .is_err()
        );
    }

    #[test]
    fn finds_url_results() {
        let url = json!({"job_id": "j", "result": {"type": "url", "url": "https://s/r.json", "result_id": "r"}});
        assert_eq!(url_result(&url), Some("https://s/r.json"));
        assert_eq!(
            url_result(&json!({"result": {"type": "full", "chunks": []}})),
            None
        );
        assert_eq!(url_result(&json!({"result": [{"total": 1}]})), None);
    }

    #[test]
    fn renders_parse() {
        let response = json!({
            "job_id": "j1", "duration": 3.2, "studio_link": "https://studio.reducto.ai/job/j1",
            "usage": {"num_pages": 2, "credits": 2.0},
            "result": {"type": "full", "chunks": [
                {"content": "# Title\n\nText", "embed": "Title. Text", "enriched": null,
                 "blocks": [{"type": "Title", "content": "Title", "bbox": {"left": 0.1, "top": 0.1, "width": 0.2, "height": 0.05, "page": 1}}]},
                {"content": "| a | b |", "embed": "", "enriched": null, "blocks": []}
            ]}
        });
        assert_eq!(
            render_parse(&response, 1000),
            "job jobid://j1 · 2 pages · https://studio.reducto.ai/job/j1\n\n# Title\n\nText\n\n---\n\n| a | b |"
        );
        let cut = render_parse(&response, 5);
        assert!(cut.contains("\n\n# Tit\n\n[cut at 5 of "), "{cut}");
        // A URL result's link may hold the chunks themselves.
        let fetched = json!({"job_id": "j2", "result": [{"content": "Body", "blocks": []}]});
        assert_eq!(render_parse(&fetched, 1000), "job jobid://j2\n\nBody");
        assert_eq!(
            render_parse(&json!({"result": {"type": "full", "chunks": []}}), 1000),
            "The document had no extractable content."
        );
    }

    #[test]
    fn renders_extract() {
        let text = render_extract(&json!({
            "job_id": "j", "usage": {"num_pages": 1, "num_fields": 1, "credits": 2.0},
            "result": [{"total": 1250.0}]
        }));
        assert_eq!(text, "job jobid://j · 1 page\n\n{\n  \"total\": 1250.0\n}");
        let deep = render_extract(&json!({
            "usage": {"num_pages": 3, "num_fields": 2}, "confidence": "low",
            "confidence_reason": "blurry scan", "result": [{"a": 1}, {"a": 2}]
        }));
        assert!(
            deep.starts_with("3 pages · confidence low: blurry scan\n\n["),
            "{deep}"
        );
    }

    #[test]
    fn usage_is_the_responses() {
        let out = used(
            "x".into(),
            &json!({"usage": {"num_pages": 3, "num_fields": 2, "credits": 4.5}}),
        );
        assert_eq!(
            out,
            ToolOutput::from("x")
                .with_usage("pages", 3.0)
                .with_usage("credits", 4.5)
        );
        let newer = used(
            "x".into(),
            &json!({"usage": {"num_pages": 1, "credits": null}}),
        );
        assert_eq!(newer, ToolOutput::from("x").with_usage("pages", 1.0));
        assert_eq!(used("x".into(), &json!({})), ToolOutput::from("x"));
    }

    #[test]
    fn renders_split() {
        let text = render_split(&json!({
            "job_id": "j", "usage": {"num_pages": 9},
            "result": {"section_mapping": null, "splits": [
                {"name": "Summary", "pages": [1, 2], "conf": "high"},
                {"name": "Financials", "pages": [3, 4, 5, 8], "conf": "low",
                 "partitions": [{"name": "Q1", "pages": [3, 4], "conf": "high"}]},
                {"name": "Risks", "pages": [{"page_number": 9, "evidence": "Risk Factors"}]}
            ]}
        }));
        assert_eq!(
            text,
            "job jobid://j · 9 pages\n\n- Summary: pages 1-2 (high confidence)\n- Financials: pages 3-5, 8 (low confidence)\n  - Q1: pages 3-4\n- Risks: pages 9"
        );
        assert_eq!(
            render_split(&json!({"result": {"splits": []}})),
            "No sections were found."
        );
    }
}
