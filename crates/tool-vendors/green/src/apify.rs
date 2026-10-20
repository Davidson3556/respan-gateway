//! Apify (<https://apify.com>): its Store of Actors (ready-made scrapers and
//! crawlers), from its API at `https://api.apify.com/v2`: searching the Store
//! (`GET /store`), running an Actor and reading what it scraped in one call
//! (`POST /actors/{actor}/run-sync-get-dataset-items`), and reading a past
//! run's items (`GET /actor-runs/{run}/dataset/items`,
//! `GET /actors/{actor}/runs/last/dataset/items`), the key (an Apify API
//! token) as a bearer token. `browse_web` is a run of `apify~rag-web-browser`.

use std::time::Duration;

use async_trait::async_trait;
use respan_gateway_tool_vendor_kit::args::{
    object, optional_enum, optional_int, optional_str, optional_str_list, pretty, required_str,
};
use respan_gateway_tool_vendor_kit::reqwest::Method;
use respan_gateway_tool_vendor_kit::{
    Api, KeyIn, ToolError, ToolOutput, ToolSpec, ToolVendor, Tools,
};
use serde_json::{Map, Value, json};

/// Apify.
pub const VENDOR: ToolVendor = ToolVendor {
    id: "apify",
    key_env: &["APIFY_TOKEN", "APIFY_API_TOKEN"],
    key_in: KeyIn::Bearer,
    base_url: "https://api.apify.com/v2",
    tools: &ApifyTools,
};

/// Apify's tools: `search_actors`, `run_actor`, `get_run_items` and
/// `browse_web`.
#[derive(Debug, Clone, Copy)]
pub struct ApifyTools;

/// The Actor `browse_web` runs.
const RAG_BROWSER: &str = "apify~rag-web-browser";

/// The most of a result, in characters, the model is given.
const MAX_CHARS: usize = 40_000;

/// The longest a synchronous run may take: Apify answers 408 past it.
const MAX_RUN_SECS: u64 = 300;

const SORTS: &[&str] = &["relevance", "popularity", "newest", "lastUpdate"];
const PRICING: &[&str] = &[
    "FREE",
    "FLAT_PRICE_PER_MONTH",
    "PRICE_PER_DATASET_ITEM",
    "PAY_PER_EVENT",
];

const TOOLS: &[ToolSpec] = &[
    ToolSpec {
        name: "search_actors",
        description: "Search the Apify Store for Actors (ready-made scrapers, crawlers and \
            automations, e.g. for Google Maps, Instagram or Amazon): each its id to run with \
            `run_actor`, title, description, Store URL, users, rating and pricing model. Free: \
            it starts no run. Read an Actor's Store page for the input it takes.",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "query": {"type": "string", "description": "What to look for, e.g. \"google maps reviews\"; matched against titles, names, descriptions and READMEs."},
    "sort_by": {"type": "string", "enum": ["relevance", "popularity", "newest", "lastUpdate"], "default": "relevance", "description": "How to order the Actors."},
    "category": {"type": "string", "description": "Only Actors in this Store category, e.g. AI, ECOMMERCE, LEAD_GENERATION."},
    "pricing_model": {"type": "string", "enum": ["FREE", "FLAT_PRICE_PER_MONTH", "PRICE_PER_DATASET_ITEM", "PAY_PER_EVENT"], "description": "Only Actors priced this way."},
    "limit": {"type": "integer", "minimum": 1, "maximum": 50, "default": 10, "description": "At most this many Actors."},
    "offset": {"type": "integer", "minimum": 0, "maximum": 10000, "default": 0, "description": "Skip this many first, to page."}
  },
  "required": ["query"]
}"#,
    },
    ToolSpec {
        name: "run_actor",
        description: "Run an Apify Actor with an input and wait for it (up to 300 seconds), \
            returning the items it scraped as JSON (long results are cut; read the rest with \
            `get_run_items`). Each run is billed to the Apify account: compute usage, plus the \
            Actor's own price (per result, per event or a monthly rental); `max_items` and \
            `max_total_charge_usd` cap what pay-per-result and pay-per-event Actors charge. A run \
            that outlasts the wait keeps running on Apify.",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "actor": {"type": "string", "description": "The Actor: its id, or owner and name as \"owner~name\" or \"owner/name\", e.g. \"apify~website-content-crawler\"."},
    "input": {"type": "object", "description": "The Actor's input, as its input schema (on its Store page) describes it."},
    "timeout_secs": {"type": "integer", "minimum": 1, "maximum": 300, "default": 120, "description": "Stop the run after this many seconds."},
    "max_items": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 20, "description": "Return at most this many items, and pay for at most this many from a pay-per-result Actor."},
    "max_total_charge_usd": {"type": "number", "exclusiveMinimum": 0, "description": "Charge at most this many US dollars for the run, under any pricing model."},
    "fields": {"type": "array", "items": {"type": "string"}, "description": "Only these fields of each item, e.g. [\"url\", \"title\"]."}
  },
  "required": ["actor"]
}"#,
    },
    ToolSpec {
        name: "get_run_items",
        description: "Read the items a past Apify Actor run scraped, by its run id, or the latest \
            run of an Actor (e.g. one `run_actor` stopped waiting for), as JSON, a page at a time. \
            Starts no run.",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "run_id": {"type": "string", "description": "The run's id."},
    "actor": {"type": "string", "description": "Instead of `run_id`: the Actor (\"owner~name\" or its id) whose latest run to read."},
    "offset": {"type": "integer", "minimum": 0, "default": 0, "description": "Skip this many items first."},
    "limit": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 20, "description": "At most this many items."},
    "fields": {"type": "array", "items": {"type": "string"}, "description": "Only these fields of each item."}
  }
}"#,
    },
    ToolSpec {
        name: "browse_web",
        description: "Search Google and read the top result pages, or read one web page, as \
            markdown, by running Apify's RAG Web Browser Actor (apify~rag-web-browser) and \
            waiting for it. Each call is a billed run: per Google search, per page fetched and \
            the run's start.",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "query": {"type": "string", "description": "Google search keywords, or the URL of one page to read."},
    "max_results": {"type": "integer", "minimum": 1, "maximum": 10, "default": 3, "description": "Read this many top results (ignored for a URL)."},
    "scraping_tool": {"type": "string", "enum": ["browser-playwright", "raw-http"], "default": "browser-playwright", "description": "browser-playwright renders JavaScript; raw-http is about twice as fast, for static pages."},
    "timeout_secs": {"type": "integer", "minimum": 1, "maximum": 300, "default": 120, "description": "Stop the run after this many seconds."}
  },
  "required": ["query"]
}"#,
    },
];

#[async_trait]
impl Tools for ApifyTools {
    fn list(&self) -> &'static [ToolSpec] {
        TOOLS
    }

    async fn call(&self, api: &Api, name: &str, arguments: Value) -> Result<ToolOutput, ToolError> {
        let args = object(&arguments)?;
        match name {
            "search_actors" => {
                let query = store_query(args)?;
                Ok(render_store(&api.get_json("/store", &query).await?).into())
            }
            "run_actor" => {
                let run = run_request(args)?;
                let items = run_sync(api, &run).await?;
                let more = format!("`get_run_items` with `actor` {:?}", run.actor);
                Ok(counted(&items, render_items(&items, 0, &more)))
            }
            "get_run_items" => {
                let (path, query) = items_request(args)?;
                let items = api.get_json(&path, &query).await?;
                let offset = query
                    .iter()
                    .find(|(k, _)| *k == "offset")
                    .and_then(|(_, v)| v.parse().ok())
                    .unwrap_or(0);
                Ok(render_items(&items, offset, "`get_run_items`").into())
            }
            "browse_web" => {
                let run = browse_request(args)?;
                let items = run_sync(api, &run).await?;
                Ok(counted(&items, render_pages(&items)))
            }
            _ => Err(ToolError::new(format!("no tool named {name:?}"))),
        }
    }
}

/// A query string.
type Query = Vec<(&'static str, String)>;

/// A synchronous run: the Actor, its input, and the query string.
#[derive(Debug, PartialEq)]
struct Run {
    actor: String,
    input: Value,
    timeout: u64,
    query: Query,
}

/// Run `run` and read its items. The wait outlasts the run's own timeout,
/// the kit's client waiting only 120 seconds.
async fn run_sync(api: &Api, run: &Run) -> Result<Value, ToolError> {
    let path = format!("/actors/{}/run-sync-get-dataset-items", run.actor);
    let request = api
        .request(Method::POST, &path)
        .query(&run.query)
        .json(&run.input)
        .timeout(Duration::from_secs(run.timeout + 30));
    api.json(request).await.map_err(|e| {
        if e.0.contains(" 408 ") {
            ToolError::new(format!(
                "{e}\nThe run is still going on Apify; read its items later with \
                 `get_run_items` and `actor` {:?}.",
                run.actor
            ))
        } else {
            e
        }
    })
}

/// `text` from a run, with the dataset `items` it made: what Store Actors
/// priced per result bill by. The run's compute is not in the answer.
fn counted(items: &Value, text: String) -> ToolOutput {
    let made = items.as_array().map_or(0, Vec::len);
    ToolOutput::from(text).with_usage("items", made as f64)
}

/// An Actor's id as its paths take it: `owner/name` becomes `owner~name`.
fn actor_id(args: &Map<String, Value>, key: &str) -> Result<String, ToolError> {
    let actor = required_str(args, key)?.trim().replacen('/', "~", 1);
    let valid = actor.matches('~').count() <= 1
        && actor.split('~').all(|part| {
            !part.is_empty()
                && !part.starts_with('.')
                && part
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        });
    if valid {
        Ok(actor)
    } else {
        Err(ToolError::new(format!(
            "`{key}` must be an Actor id or \"owner~name\", not {actor:?}"
        )))
    }
}

/// `timeout_secs`, or the default, for the run's `timeout`.
fn timeout(args: &Map<String, Value>) -> Result<u64, ToolError> {
    Ok(optional_int(args, "timeout_secs", 1, MAX_RUN_SECS)?.unwrap_or(120))
}

/// `GET /store`'s query string from the tool's arguments.
fn store_query(args: &Map<String, Value>) -> Result<Vec<(&'static str, String)>, ToolError> {
    let mut q = vec![("search", required_str(args, "query")?.to_owned())];
    if let Some(v) = optional_enum(args, "sort_by", SORTS)? {
        q.push(("sortBy", v.to_owned()));
    }
    if let Some(v) = optional_str(args, "category")? {
        q.push(("category", v.to_owned()));
    }
    if let Some(v) = optional_enum(args, "pricing_model", PRICING)? {
        q.push(("pricingModel", v.to_owned()));
    }
    let limit = optional_int(args, "limit", 1, 50)?.unwrap_or(10);
    q.push(("limit", limit.to_string()));
    if let Some(v) = optional_int(args, "offset", 0, 10_000)? {
        q.push(("offset", v.to_string()));
    }
    Ok(q)
}

/// `run_actor`'s run from the tool's arguments.
fn run_request(args: &Map<String, Value>) -> Result<Run, ToolError> {
    let actor = actor_id(args, "actor")?;
    let input = match args.get("input") {
        None | Some(Value::Null) => json!({}),
        Some(v @ Value::Object(_)) => v.clone(),
        Some(_) => return Err(ToolError::new("`input` must be a JSON object")),
    };
    let timeout = timeout(args)?;
    let max_items = optional_int(args, "max_items", 1, 1000)?.unwrap_or(20);
    let mut query = vec![
        ("timeout", timeout.to_string()),
        ("maxItems", max_items.to_string()),
        ("limit", max_items.to_string()),
        ("clean", "true".to_owned()),
    ];
    match args.get("max_total_charge_usd") {
        None | Some(Value::Null) => {}
        Some(v) => match v.as_f64().filter(|usd| *usd > 0.0) {
            Some(usd) => query.push(("maxTotalChargeUsd", usd.to_string())),
            None => {
                return Err(ToolError::new(
                    "`max_total_charge_usd` must be a number above 0",
                ));
            }
        },
    }
    if let Some(v) = optional_str_list(args, "fields")? {
        query.push(("fields", v.join(",")));
    }
    Ok(Run {
        actor,
        input,
        timeout,
        query,
    })
}

/// `get_run_items`' path and query string from the tool's arguments.
fn items_request(args: &Map<String, Value>) -> Result<(String, Query), ToolError> {
    let path = match (optional_str(args, "run_id")?, optional_str(args, "actor")?) {
        (Some(_), Some(_)) => return Err(ToolError::new("give `run_id` or `actor`, not both")),
        (Some(run), None) => {
            let run = run.trim();
            if !run.bytes().all(|b| b.is_ascii_alphanumeric()) {
                return Err(ToolError::new(format!(
                    "`run_id` must be a run's id, not {run:?}"
                )));
            }
            format!("/actor-runs/{run}/dataset/items")
        }
        (None, Some(_)) => format!(
            "/actors/{}/runs/last/dataset/items",
            actor_id(args, "actor")?
        ),
        (None, None) => return Err(ToolError::new("`run_id` or `actor` is required")),
    };
    let mut q = vec![("clean", "true".to_owned())];
    if let Some(v) = optional_int(args, "offset", 0, u64::MAX)? {
        q.push(("offset", v.to_string()));
    }
    let limit = optional_int(args, "limit", 1, 1000)?.unwrap_or(20);
    q.push(("limit", limit.to_string()));
    if let Some(v) = optional_str_list(args, "fields")? {
        q.push(("fields", v.join(",")));
    }
    Ok((path, q))
}

/// `browse_web`'s run of the RAG Web Browser from the tool's arguments.
fn browse_request(args: &Map<String, Value>) -> Result<Run, ToolError> {
    let max_results = optional_int(args, "max_results", 1, 10)?.unwrap_or(3);
    let mut input = Map::new();
    input.insert("query".into(), required_str(args, "query")?.into());
    input.insert("maxResults".into(), max_results.into());
    input.insert("outputFormats".into(), json!(["markdown"]));
    if let Some(v) = optional_enum(args, "scraping_tool", &["browser-playwright", "raw-http"])? {
        input.insert("scrapingTool".into(), v.into());
    }
    let timeout = timeout(args)?;
    Ok(Run {
        actor: RAG_BROWSER.to_owned(),
        input: Value::Object(input),
        timeout,
        query: vec![
            ("timeout", timeout.to_string()),
            ("maxItems", max_results.to_string()),
        ],
    })
}

fn str_of<'a>(v: &'a Value, key: &str) -> Option<&'a str> {
    v.get(key).and_then(Value::as_str).filter(|s| !s.is_empty())
}

/// `text` up to `max` bytes, cut at a character, saying so.
fn cut(text: &str, max: usize) -> String {
    if text.len() <= max {
        return text.to_owned();
    }
    let mut end = max;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}… (cut)", &text[..end])
}

/// Store Actors as JSON, with what choosing one takes.
fn render_store(response: &Value) -> String {
    let data = response.get("data").unwrap_or(response);
    let actors: Vec<Value> = data
        .get("items")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|a| {
            let mut kept = Map::new();
            if let (Some(owner), Some(name)) = (str_of(a, "username"), str_of(a, "name")) {
                kept.insert("actor".into(), format!("{owner}~{name}").into());
            }
            for (from, to) in [("title", "title"), ("url", "url")] {
                if let Some(v) = str_of(a, from) {
                    kept.insert(to.into(), v.into());
                }
            }
            if let Some(v) = str_of(a, "description") {
                kept.insert("description".into(), cut(v, 300).into());
            }
            let stats = a.get("stats").unwrap_or(&Value::Null);
            if let Some(v) = stats.get("totalUsers").filter(|v| !v.is_null()) {
                kept.insert("users".into(), v.clone());
            }
            if let Some(v) = a
                .get("actorReviewRating")
                .or_else(|| stats.get("actorReviewRating"))
                .filter(|v| !v.is_null())
            {
                kept.insert("rating".into(), v.clone());
            }
            let pricing = a.get("currentPricingInfo").unwrap_or(&Value::Null);
            if let Some(model) = str_of(pricing, "pricingModel") {
                let mut price = model.to_owned();
                if let Some(usd) = pricing.get("pricePerUnitUsd").and_then(Value::as_f64) {
                    let unit = str_of(pricing, "unitName").unwrap_or("unit");
                    price.push_str(&format!(", ${usd} per {unit}"));
                }
                kept.insert("pricing".into(), price.into());
            }
            if let Some(v) = str_of(a, "notice") {
                kept.insert("notice".into(), v.into());
            }
            Value::Object(kept)
        })
        .collect();
    if actors.is_empty() {
        return "No Actors found.".into();
    }
    let total = data.get("total").and_then(Value::as_u64);
    let mut text = pretty(&Value::Array(actors.clone()));
    if let Some(total) = total.filter(|t| *t as usize > actors.len()) {
        text.push_str(&format!("\n\n{} of {total} Actors shown.", actors.len()));
    }
    text
}

/// Dataset items as JSON, as many as fit; `more` is where to read the rest,
/// the next from `offset` plus those shown.
fn render_items(items: &Value, offset: u64, more: &str) -> String {
    let Some(list) = items.as_array() else {
        return cut(&pretty(items), MAX_CHARS);
    };
    if list.is_empty() {
        return "No items.".into();
    }
    let mut shown = Vec::new();
    let mut size = 0;
    for item in list {
        let len = pretty(item).len();
        if size + len > MAX_CHARS && !shown.is_empty() {
            break;
        }
        size += len;
        shown.push(item.clone());
    }
    let count = shown.len();
    let mut text = cut(&pretty(&Value::Array(shown)), MAX_CHARS);
    if count < list.len() {
        text.push_str(&format!(
            "\n\n{} more items not shown (the output is capped); read them with {more}, \
             `offset` {}, or ask for fewer `fields`.",
            list.len() - count,
            offset + count as u64
        ));
    }
    text
}

/// The RAG Web Browser's pages as markdown under their titles and URLs,
/// sharing the output cap.
fn render_pages(items: &Value) -> String {
    let list = items.as_array().map(Vec::as_slice).unwrap_or_default();
    if list.is_empty() {
        return "Nothing was found or read.".into();
    }
    let each = MAX_CHARS / list.len();
    let pages: Vec<String> = list
        .iter()
        .map(|item| {
            let meta = item.get("metadata").unwrap_or(&Value::Null);
            let search = item.get("searchResult").unwrap_or(&Value::Null);
            let url = str_of(meta, "url")
                .or_else(|| str_of(search, "url"))
                .unwrap_or_default();
            let title = str_of(meta, "title")
                .or_else(|| str_of(search, "title"))
                .unwrap_or(url);
            let mut page = format!("# {title}\nURL: {url}");
            let crawl = item.get("crawl").unwrap_or(&Value::Null);
            if let Some(status) = str_of(crawl, "requestStatus").filter(|s| *s != "handled") {
                page.push_str(&format!("\nStatus: {status}"));
            }
            if let Some(code) = crawl
                .get("httpStatusCode")
                .and_then(Value::as_u64)
                .filter(|c| *c >= 400)
            {
                page.push_str(&format!("\nHTTP {code}"));
            }
            let body = str_of(item, "markdown")
                .or_else(|| str_of(item, "text"))
                .or_else(|| str_of(search, "description"))
                .map(str::trim)
                .unwrap_or("(no content)");
            page.push_str("\n\n");
            page.push_str(&cut(body, each));
            page
        })
        .collect();
    pages.join("\n\n---\n\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_run_counts_its_items() {
        let out = counted(&json!([{"a": 1}, {"a": 2}]), "two".into());
        assert_eq!(out, ToolOutput::from("two").with_usage("items", 2.0));
    }

    fn args(v: Value) -> Map<String, Value> {
        match v {
            Value::Object(m) => m,
            _ => Map::new(),
        }
    }

    #[test]
    fn store_query_maps_and_validates() {
        let q = store_query(&args(json!({
            "query": "google maps", "sort_by": "popularity", "pricing_model": "FREE", "limit": 5
        })))
        .expect("valid");
        assert_eq!(
            q,
            vec![
                ("search", "google maps".to_owned()),
                ("sortBy", "popularity".to_owned()),
                ("pricingModel", "FREE".to_owned()),
                ("limit", "5".to_owned()),
            ]
        );
        assert!(store_query(&args(json!({}))).is_err());
        assert!(store_query(&args(json!({"query": "x", "sort_by": "stars"}))).is_err());
        assert!(store_query(&args(json!({"query": "x", "limit": 51}))).is_err());
    }

    #[test]
    fn run_request_maps_and_validates() {
        let run = run_request(&args(json!({
            "actor": "apify/website-content-crawler",
            "input": {"startUrls": [{"url": "https://a.b"}]},
            "timeout_secs": 60, "max_items": 5, "max_total_charge_usd": 0.5,
            "fields": ["url", "text"]
        })))
        .expect("valid");
        assert_eq!(
            run,
            Run {
                actor: "apify~website-content-crawler".into(),
                input: json!({"startUrls": [{"url": "https://a.b"}]}),
                timeout: 60,
                query: vec![
                    ("timeout", "60".to_owned()),
                    ("maxItems", "5".to_owned()),
                    ("limit", "5".to_owned()),
                    ("clean", "true".to_owned()),
                    ("maxTotalChargeUsd", "0.5".to_owned()),
                    ("fields", "url,text".to_owned()),
                ],
            }
        );
        let plain = run_request(&args(json!({"actor": "moJRLRc85AitArpNN"}))).expect("valid");
        assert_eq!(plain.input, json!({}));
        assert_eq!(plain.timeout, 120);
        assert!(run_request(&args(json!({}))).is_err());
        assert!(run_request(&args(json!({"actor": "a/b/c"}))).is_err());
        assert!(run_request(&args(json!({"actor": "../x"}))).is_err());
        assert!(run_request(&args(json!({"actor": "a~b", "input": "x"}))).is_err());
        assert!(run_request(&args(json!({"actor": "a~b", "timeout_secs": 301}))).is_err());
        assert!(run_request(&args(json!({"actor": "a~b", "max_total_charge_usd": 0}))).is_err());
    }

    #[test]
    fn items_request_maps_and_validates() {
        assert_eq!(
            items_request(&args(json!({"run_id": "3KH8gEpp4d8uQSe8T", "offset": 20})))
                .expect("valid"),
            (
                "/actor-runs/3KH8gEpp4d8uQSe8T/dataset/items".to_owned(),
                vec![
                    ("clean", "true".to_owned()),
                    ("offset", "20".to_owned()),
                    ("limit", "20".to_owned()),
                ]
            )
        );
        let (path, _) =
            items_request(&args(json!({"actor": "compass/crawler-google-places"}))).expect("valid");
        assert_eq!(
            path,
            "/actors/compass~crawler-google-places/runs/last/dataset/items"
        );
        assert!(items_request(&args(json!({}))).is_err());
        assert!(items_request(&args(json!({"run_id": "a", "actor": "b~c"}))).is_err());
        assert!(items_request(&args(json!({"run_id": "a/../b"}))).is_err());
    }

    #[test]
    fn browse_request_maps_and_validates() {
        let run = browse_request(&args(
            json!({"query": "rust async", "max_results": 2, "scraping_tool": "raw-http"}),
        ))
        .expect("valid");
        assert_eq!(run.actor, RAG_BROWSER);
        assert_eq!(
            run.input,
            json!({"query": "rust async", "maxResults": 2, "outputFormats": ["markdown"],
                "scrapingTool": "raw-http"})
        );
        assert_eq!(
            run.query,
            vec![("timeout", "120".to_owned()), ("maxItems", "2".to_owned())]
        );
        assert!(browse_request(&args(json!({}))).is_err());
        assert!(browse_request(&args(json!({"query": "x", "max_results": 11}))).is_err());
        assert!(browse_request(&args(json!({"query": "x", "scraping_tool": "curl"}))).is_err());
    }

    #[test]
    fn renders_store() {
        let text = render_store(
            &json!({"data": {"total": 100, "offset": 0, "limit": 1, "count": 1,
            "items": [{
                "id": "zdc3Pyhyz3m8vjDeM", "title": "My Public Actor", "name": "my-public-actor",
                "username": "jane35", "userFullName": "Jane Doe", "description": "My public Actor!",
                "pictureUrl": "https://p", "url": "https://apify.com/jane35/my-public-actor",
                "stats": {"totalBuilds": 9, "totalRuns": 16, "totalUsers": 6},
                "currentPricingInfo": {"pricingModel": "PRICE_PER_DATASET_ITEM",
                    "pricePerUnitUsd": 0.002, "unitName": "result"},
                "actorReviewRating": 4.7
            }]}}),
        );
        let (json_part, note) = text.split_once("\n\n").expect("a note");
        let parsed: Value = serde_json::from_str(json_part).expect("json");
        assert_eq!(
            parsed,
            json!([{
                "actor": "jane35~my-public-actor", "title": "My Public Actor",
                "url": "https://apify.com/jane35/my-public-actor", "description": "My public Actor!",
                "users": 6, "rating": 4.7, "pricing": "PRICE_PER_DATASET_ITEM, $0.002 per result"
            }])
        );
        assert_eq!(note, "1 of 100 Actors shown.");
        assert_eq!(
            render_store(&json!({"data": {"total": 0, "items": []}})),
            "No Actors found."
        );
    }

    #[test]
    fn renders_items_within_the_cap() {
        let small = render_items(&json!([{"url": "https://a", "title": "A"}]), 0, "x");
        assert_eq!(
            serde_json::from_str::<Value>(&small).expect("json"),
            json!([{"url": "https://a", "title": "A"}])
        );
        assert_eq!(render_items(&json!([]), 0, "x"), "No items.");
        let big: Vec<Value> = (0..10)
            .map(|i| json!({"i": i, "text": "x".repeat(MAX_CHARS / 4)}))
            .collect();
        let text = render_items(&Value::Array(big), 40, "`get_run_items`");
        assert!(text.len() < MAX_CHARS + 300, "{}", text.len());
        assert!(
            text.ends_with(
                "7 more items not shown (the output is capped); read them with \
                 `get_run_items`, `offset` 43, or ask for fewer `fields`."
            ),
            "{text}"
        );
        let huge = render_items(&json!([{"text": "y".repeat(MAX_CHARS * 2)}]), 0, "x");
        assert!(huge.len() < MAX_CHARS + 20 && huge.ends_with("… (cut)"));
    }

    #[test]
    fn renders_pages() {
        let text = render_pages(&json!([
            {"crawl": {"httpStatusCode": 200, "loadedAt": "2024-11-25T21:23:58.336Z",
                "uniqueKey": "k", "requestStatus": "handled"},
             "searchResult": {"title": "Apify", "description": "Web scraping", "url": "https://apify.com",
                "resultType": "ORGANIC"},
             "metadata": {"title": "Apify: Full-stack web scraping", "url": "https://apify.com/",
                "languageCode": "en"},
             "markdown": "# Apify\nScrape anything.", "html": null},
            {"crawl": {"httpStatusCode": 404, "requestStatus": "failed"},
             "searchResult": {"title": "Gone", "description": "Was here", "url": "https://gone.test"},
             "metadata": {}}
        ]));
        assert_eq!(
            text,
            "# Apify: Full-stack web scraping\nURL: https://apify.com/\n\n# Apify\nScrape anything.\
             \n\n---\n\n# Gone\nURL: https://gone.test\nStatus: failed\nHTTP 404\n\nWas here"
        );
        assert_eq!(render_pages(&json!([])), "Nothing was found or read.");
    }
}
