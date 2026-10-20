//! Zep (<https://getzep.com>): agent memory in temporal knowledge graphs,
//! from its v4 API at `https://api.getzep.com/api/v4` (`POST
//! /graphs/{uuid}/search/{edges,nodes,episodes}`, `POST
//! /graphs/{uuid}/context`, `GET /threads/{uuid}/context`, `POST
//! /threads/{uuid}/messages`, `POST /graphs/{uuid}/episodes`), the key as
//! `Authorization: Api-Key <key>`. v4 addresses users, threads and graphs
//! by the UUIDs Zep assigns; a user's UUID also names its graph.

use async_trait::async_trait;
use respan_gateway_tool_vendor_kit::args::{
    object, optional_bool, optional_enum, optional_int, optional_str, pretty, required_str,
};
use respan_gateway_tool_vendor_kit::reqwest::Method;
use respan_gateway_tool_vendor_kit::{
    Api, KeyIn, ToolError, ToolOutput, ToolSpec, ToolVendor, Tools,
};
use serde_json::{Map, Value};

/// Zep.
pub const VENDOR: ToolVendor = ToolVendor {
    id: "zep",
    key_env: &["ZEP_API_KEY"],
    key_in: KeyIn::Scheme("Api-Key"),
    base_url: "https://api.getzep.com/api/v4",
    tools: &ZepTools,
};

/// Zep's tools: `search`, `get_graph_context`, `get_thread_context`,
/// `add_messages` and `add_data`.
#[derive(Debug, Clone, Copy)]
pub struct ZepTools;

const SCOPES: &[&str] = &["edges", "nodes", "episodes"];
const RERANKERS: &[&str] = &["rrf", "episode_mentions", "cross_encoder"];
const ROLES: &[&str] = &["system", "assistant", "user", "function", "tool"];

/// The most of one episode's content a search result quotes.
const EPISODE_CHARS: usize = 2000;

const TOOLS: &[ToolSpec] = &[
    ToolSpec {
        name: "search",
        description: "Search a Zep knowledge graph (a user's, or a standalone one): edges are \
            facts with when they held, nodes are entities with summaries, episodes are the raw \
            messages and data they came from. Retrieval is not billed.",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "graph_uuid": {"type": "string", "description": "The graph's UUID; for a user's graph, the user's UUID."},
    "query": {"type": "string", "description": "What to look for, in natural language."},
    "scope": {"type": "string", "enum": ["edges", "nodes", "episodes"], "default": "edges", "description": "edges: facts; nodes: entities; episodes: source messages and data."},
    "limit": {"type": "integer", "minimum": 1, "maximum": 50, "default": 10, "description": "At most this many results."},
    "reranker": {"type": "string", "enum": ["rrf", "episode_mentions", "cross_encoder"], "default": "rrf", "description": "rrf: reciprocal rank fusion; episode_mentions: favor what is mentioned most; cross_encoder: most precise, slower."}
  },
  "required": ["graph_uuid", "query"]
}"#,
    },
    ToolSpec {
        name: "get_graph_context",
        description: "Assemble a context block from a Zep knowledge graph for a query: the \
            relevant facts, entities, observations and thread summaries, as text to give a model \
            as untrusted data. Retrieval is not billed.",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "graph_uuid": {"type": "string", "description": "The graph's UUID; for a user's graph, the user's UUID."},
    "query": {"type": "string", "description": "What the context is for, in natural language."},
    "max_characters": {"type": "integer", "minimum": 1, "maximum": 1000000, "description": "At most this many characters of context."},
    "recency_bias": {"type": "string", "enum": ["off", "mild", "strong"], "description": "Favor more recent graph data."}
  },
  "required": ["graph_uuid", "query"]
}"#,
    },
    ToolSpec {
        name: "get_thread_context",
        description: "Get the context block for a Zep thread: what its user's whole graph holds \
            that is relevant to the thread's latest messages, as text to give a model as \
            untrusted data. Retrieval is not billed.",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "thread_uuid": {"type": "string", "description": "The thread's UUID."}
  },
  "required": ["thread_uuid"]
}"#,
    },
    ToolSpec {
        name: "add_messages",
        description: "Add chat messages to a Zep thread; Zep stores them and extracts facts \
            into the thread user's graph in the background. Each message is an episode billed \
            in credits by size (1 credit per 350 bytes or part).",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "thread_uuid": {"type": "string", "description": "The thread's UUID."},
    "messages": {"type": "array", "minItems": 1, "items": {
      "type": "object",
      "properties": {
        "content": {"type": "string", "description": "The message's text."},
        "role": {"type": "string", "enum": ["system", "assistant", "user", "function", "tool"], "description": "Who sent it."},
        "name": {"type": "string", "description": "The sender's name, e.g. \"john\" or \"sales_agent\"."},
        "reference_time": {"type": "string", "description": "When it was said, ISO 8601, if not now."}
      },
      "required": ["content"]
    }, "description": "The messages, oldest first."},
    "return_context": {"type": "boolean", "default": false, "description": "Also return the thread's context block after adding them."}
  },
  "required": ["thread_uuid", "messages"]
}"#,
    },
    ToolSpec {
        name: "add_data",
        description: "Add data (text, JSON or a message) to a Zep knowledge graph as an \
            episode; Zep extracts its entities and facts in the background, searchable once \
            processed. Billed in credits by size (1 credit per 350 bytes or part).",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "graph_uuid": {"type": "string", "description": "The graph's UUID; for a user's graph, the user's UUID."},
    "data": {"type": "string", "description": "The content: text, a JSON document, or a message such as \"Alice: hi\"."},
    "type": {"type": "string", "enum": ["text", "json", "message"], "default": "text", "description": "The format of `data`."},
    "source_description": {"type": "string", "description": "Where the data came from, e.g. \"CRM record\"."},
    "reference_time": {"type": "string", "description": "When it happened, ISO 8601, if not now."}
  },
  "required": ["graph_uuid", "data"]
}"#,
    },
];

#[async_trait]
impl Tools for ZepTools {
    fn list(&self) -> &'static [ToolSpec] {
        TOOLS
    }

    async fn call(&self, api: &Api, name: &str, arguments: Value) -> Result<ToolOutput, ToolError> {
        let args = object(&arguments)?;
        match name {
            "search" => {
                let (path, limit, body) = search_request(args)?;
                let request = api
                    .request(Method::POST, &path)
                    .query(&[("page_size", limit)])
                    .json(&body);
                let scope = optional_enum(args, "scope", SCOPES)?.unwrap_or("edges");
                Ok(render_search(scope, &api.json(request).await?).into())
            }
            "get_graph_context" => {
                let graph = uuid_arg(args, "graph_uuid")?;
                let body = graph_context_body(args)?;
                let path = format!("graphs/{graph}/context");
                Ok(render_context(&api.post_json(&path, &body).await?).into())
            }
            "get_thread_context" => {
                let thread = uuid_arg(args, "thread_uuid")?;
                let path = format!("threads/{thread}/context");
                Ok(render_context(&api.get_json(&path, &[]).await?).into())
            }
            "add_messages" => {
                let thread = uuid_arg(args, "thread_uuid")?;
                let body = messages_body(args)?;
                let path = format!("threads/{thread}/messages");
                let text = render_added_messages(&api.post_json(&path, &body).await?);
                let contents = array_of(&body, "messages").iter();
                Ok(billed(
                    text.into(),
                    contents.filter_map(|m| str_of(m, "content")),
                ))
            }
            "add_data" => {
                let graph = uuid_arg(args, "graph_uuid")?;
                let body = episode_body(args)?;
                let path = format!("graphs/{graph}/episodes");
                let text = render_added_episode(&api.post_json(&path, &body).await?);
                Ok(billed(text.into(), str_of(&body, "data")))
            }
            _ => Err(ToolError::new(format!("no tool named {name:?}"))),
        }
    }
}

/// The UUID `key`, which must be there, so it is a safe path segment.
fn uuid_arg<'a>(args: &'a Map<String, Value>, key: &str) -> Result<&'a str, ToolError> {
    let v = required_str(args, key)?.trim();
    let shaped = v.len() == 36
        && v.bytes().enumerate().all(|(i, b)| match i {
            8 | 13 | 18 | 23 => b == b'-',
            _ => b.is_ascii_hexdigit(),
        });
    if shaped {
        Ok(v)
    } else {
        Err(ToolError::new(format!("`{key}` must be a UUID, not {v:?}")))
    }
}

/// A search's path, page size and body from the tool's arguments.
fn search_request(args: &Map<String, Value>) -> Result<(String, u64, Value), ToolError> {
    let graph = uuid_arg(args, "graph_uuid")?;
    let scope = optional_enum(args, "scope", SCOPES)?.unwrap_or("edges");
    let limit = optional_int(args, "limit", 1, 50)?.unwrap_or(10);
    let mut body = Map::new();
    body.insert("query".into(), required_str(args, "query")?.into());
    if let Some(v) = optional_enum(args, "reranker", RERANKERS)? {
        body.insert("reranker".into(), v.into());
    }
    Ok((
        format!("graphs/{graph}/search/{scope}"),
        limit,
        Value::Object(body),
    ))
}

/// `POST /graphs/{uuid}/context`'s body from the tool's arguments.
fn graph_context_body(args: &Map<String, Value>) -> Result<Value, ToolError> {
    let mut body = Map::new();
    body.insert("query".into(), required_str(args, "query")?.into());
    if let Some(v) = optional_int(args, "max_characters", 1, 1_000_000)? {
        body.insert("max_characters".into(), v.into());
    }
    if let Some(v) = optional_enum(args, "recency_bias", &["off", "mild", "strong"])? {
        body.insert("recency_bias".into(), v.into());
    }
    Ok(Value::Object(body))
}

/// `POST /threads/{uuid}/messages`' body from the tool's arguments.
fn messages_body(args: &Map<String, Value>) -> Result<Value, ToolError> {
    let items = match args.get("messages") {
        Some(Value::Array(items)) if !items.is_empty() => items,
        None | Some(Value::Null) => return Err(ToolError::new("`messages` is required")),
        _ => {
            return Err(ToolError::new(
                "`messages` must be a non-empty array of messages",
            ));
        }
    };
    let mut messages = Vec::with_capacity(items.len());
    for (i, item) in items.iter().enumerate() {
        let Value::Object(m) = item else {
            return Err(ToolError::new(format!("`messages[{i}]` must be an object")));
        };
        let at = |e: ToolError| ToolError::new(format!("messages[{i}]: {e}"));
        let mut message = Map::new();
        message.insert(
            "content".into(),
            required_str(m, "content").map_err(at)?.into(),
        );
        if let Some(v) = optional_enum(m, "role", ROLES).map_err(at)? {
            message.insert("role".into(), v.into());
        }
        for key in ["name", "reference_time"] {
            if let Some(v) = optional_str(m, key).map_err(at)? {
                message.insert(key.into(), v.into());
            }
        }
        messages.push(Value::Object(message));
    }
    let mut body = Map::new();
    body.insert("messages".into(), Value::Array(messages));
    if let Some(v) = optional_bool(args, "return_context")? {
        body.insert("return_context".into(), v.into());
    }
    Ok(Value::Object(body))
}

/// `POST /graphs/{uuid}/episodes`' body from the tool's arguments.
fn episode_body(args: &Map<String, Value>) -> Result<Value, ToolError> {
    let mut body = Map::new();
    body.insert("data".into(), required_str(args, "data")?.into());
    if let Some(v) = optional_enum(args, "type", &["text", "json", "message"])? {
        body.insert("type".into(), v.into());
    }
    for key in ["source_description", "reference_time"] {
        if let Some(v) = optional_str(args, key)? {
            body.insert(key.into(), v.into());
        }
    }
    Ok(Value::Object(body))
}

/// The bytes one credit covers: Zep bills an episode 1 credit per 350
/// bytes or part (<https://www.getzep.com/pricing>); retrieval is free.
const CREDIT_BYTES: usize = 350;

/// `out`, having added `episodes`, one per content: their count, and
/// `credits` by Zep's published rule, as no answer reports them.
fn billed<'a>(out: ToolOutput, episodes: impl IntoIterator<Item = &'a str>) -> ToolOutput {
    let (mut count, mut credits) = (0, 0);
    for content in episodes {
        count += 1;
        credits += content.len().div_ceil(CREDIT_BYTES).max(1);
    }
    out.with_usage("episodes", count as f64)
        .with_usage("credits", credits as f64)
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

/// `text` up to `max` characters.
fn cap(text: &str, max: usize) -> String {
    match text.char_indices().nth(max) {
        Some((end, _)) => format!("{}…", &text[..end]),
        None => text.to_owned(),
    }
}

/// `keep` of `item`, those present, renamed as `keep` says.
fn kept(item: &Value, keep: &[(&str, &str)]) -> Map<String, Value> {
    keep.iter()
        .filter_map(|(from, to)| {
            item.get(*from)
                .filter(|v| {
                    !v.is_null()
                        && v.as_str() != Some("")
                        && v.as_array().is_none_or(|a| !a.is_empty())
                })
                .map(|v| ((*to).to_owned(), v.clone()))
        })
        .collect()
}

/// Search results as JSON, with the fields worth reading: provenance
/// lists, attributes and scores are dropped, episodes' content capped.
fn render_search(scope: &str, response: &Value) -> String {
    let keep: &[(&str, &str)] = match scope {
        "nodes" => &[
            ("name", "name"),
            ("labels", "labels"),
            ("summary", "summary"),
            ("uuid", "uuid"),
        ],
        "episodes" => &[
            ("content", "content"),
            ("role", "role"),
            ("role_name", "name"),
            ("source", "source"),
            ("source_description", "source_description"),
            ("reference_time", "reference_time"),
            ("thread_uuid", "thread_uuid"),
            ("uuid", "uuid"),
        ],
        _ => &[
            ("fact", "fact"),
            ("source_node_name", "source"),
            ("name", "relation"),
            ("target_node_name", "target"),
            ("valid_at", "valid_at"),
            ("invalid_at", "invalid_at"),
            ("expired_at", "expired_at"),
            ("uuid", "uuid"),
        ],
    };
    let results: Vec<Value> = array_of(response, "items")
        .iter()
        .map(|item| {
            let mut out = kept(item, keep);
            if let Some(Value::String(content)) = out.get_mut("content") {
                *content = cap(content.trim(), EPISODE_CHARS);
            }
            Value::Object(out)
        })
        .collect();
    if results.is_empty() {
        return "No results.".into();
    }
    pretty(&Value::Array(results))
}

/// A context block, noting when its budget cut it short.
fn render_context(response: &Value) -> String {
    let mut out = str_of(response, "context")
        .map(str::trim)
        .unwrap_or_default()
        .to_owned();
    if out.is_empty() {
        out = "No context: the graph holds nothing relevant yet.".into();
    }
    if response.get("truncated").and_then(Value::as_bool) == Some(true) {
        out.push_str("\n\n(cut to max_characters)");
    }
    out
}

/// The background task's id and status, as a sentence's end.
fn task_note(response: &Value) -> String {
    let task = response.get("task").unwrap_or(&Value::Null);
    match (str_of(task, "uuid"), str_of(task, "status")) {
        (Some(id), Some(status)) => format!(" Extraction task {id}: {status}."),
        (Some(id), None) => format!(" Extraction task {id}."),
        _ => String::new(),
    }
}

/// The added messages' ids and the task, then the context if asked for.
fn render_added_messages(response: &Value) -> String {
    let ids: Vec<&str> = array_of(response, "messages")
        .iter()
        .filter_map(|m| str_of(m, "uuid"))
        .collect();
    let mut out = format!("Added {} message(s)", ids.len());
    if !ids.is_empty() {
        out.push_str(&format!(" ({})", ids.join(", ")));
    }
    out.push('.');
    out.push_str(&task_note(response));
    if let Some(context) = str_of(response, "context") {
        out.push_str("\n\nContext:\n");
        out.push_str(context.trim());
    }
    out
}

/// The added episode's id and graph, and the task.
fn render_added_episode(response: &Value) -> String {
    let episode = response.get("episode").unwrap_or(&Value::Null);
    let mut out = String::from("Added episode");
    if let Some(id) = str_of(episode, "uuid") {
        out.push_str(&format!(" {id}"));
    }
    if let Some(graph) = str_of(episode, "graph_uuid") {
        out.push_str(&format!(" to graph {graph}"));
    }
    out.push_str("; it is searchable once processed.");
    out.push_str(&task_note(response));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use respan_gateway_tool_vendor_kit::ToolUsage;
    use serde_json::json;

    const G: &str = "a1b2c3d4-e5f6-7890-abcd-ef1234567890";

    fn args(v: Value) -> Map<String, Value> {
        match v {
            Value::Object(m) => m,
            _ => Map::new(),
        }
    }

    #[test]
    fn search_request_maps_arguments() {
        let (path, limit, body) = search_request(&args(json!({
            "graph_uuid": G, "query": "diet", "scope": "nodes", "limit": 5,
            "reranker": "cross_encoder"
        })))
        .expect("valid");
        assert_eq!(path, format!("graphs/{G}/search/nodes"));
        assert_eq!(limit, 5);
        assert_eq!(body, json!({"query": "diet", "reranker": "cross_encoder"}));
        let (path, limit, body) =
            search_request(&args(json!({"graph_uuid": G, "query": "q"}))).expect("valid");
        assert_eq!(path, format!("graphs/{G}/search/edges"));
        assert_eq!(limit, 10);
        assert_eq!(body, json!({"query": "q"}));
    }

    #[test]
    fn search_request_rejects_bad_arguments() {
        assert!(search_request(&args(json!({"graph_uuid": G}))).is_err());
        assert!(search_request(&args(json!({"query": "q"}))).is_err());
        assert!(search_request(&args(json!({"graph_uuid": "../users", "query": "q"}))).is_err());
        assert!(
            search_request(&args(
                json!({"graph_uuid": G, "query": "q", "scope": "all"})
            ))
            .is_err()
        );
        assert!(
            search_request(&args(json!({"graph_uuid": G, "query": "q", "limit": 51}))).is_err()
        );
        assert!(
            search_request(&args(
                json!({"graph_uuid": G, "query": "q", "reranker": "mmr"})
            ))
            .is_err()
        );
    }

    #[test]
    fn uuids_are_checked() {
        assert_eq!(uuid_arg(&args(json!({"u": G})), "u"), Ok(G));
        assert!(uuid_arg(&args(json!({"u": "alice"})), "u").is_err());
        assert!(
            uuid_arg(
                &args(json!({"u": "a1b2c3d4-e5f6-7890-abcd-ef123456789/"})),
                "u"
            )
            .is_err()
        );
    }

    #[test]
    fn graph_context_and_episode_bodies() {
        assert_eq!(
            graph_context_body(&args(json!({
                "query": "plans", "max_characters": 2000, "recency_bias": "mild"
            })))
            .expect("valid"),
            json!({"query": "plans", "max_characters": 2000, "recency_bias": "mild"})
        );
        assert!(graph_context_body(&args(json!({"query": "q", "recency_bias": "max"}))).is_err());
        assert_eq!(
            episode_body(&args(json!({
                "data": "{\"plan\": \"pro\"}", "type": "json", "source_description": "CRM"
            })))
            .expect("valid"),
            json!({"data": "{\"plan\": \"pro\"}", "type": "json", "source_description": "CRM"})
        );
        assert!(episode_body(&args(json!({}))).is_err());
        assert!(episode_body(&args(json!({"data": "x", "type": "csv"}))).is_err());
    }

    #[test]
    fn messages_body_maps_and_validates() {
        assert_eq!(
            messages_body(&args(json!({
                "messages": [
                    {"content": "I'm vegan.", "role": "user", "name": "Jane"},
                    {"content": "Noted.", "role": "assistant", "extra": 1}
                ],
                "return_context": true
            })))
            .expect("valid"),
            json!({
                "messages": [
                    {"content": "I'm vegan.", "role": "user", "name": "Jane"},
                    {"content": "Noted.", "role": "assistant"}
                ],
                "return_context": true
            })
        );
        assert!(messages_body(&args(json!({}))).is_err());
        assert!(messages_body(&args(json!({"messages": []}))).is_err());
        assert!(messages_body(&args(json!({"messages": ["hi"]}))).is_err());
        assert!(messages_body(&args(json!({"messages": [{"role": "user"}]}))).is_err());
        let e = messages_body(&args(
            json!({"messages": [{"content": "x", "role": "bot"}]}),
        ))
        .expect_err("bad role");
        assert!(e.0.starts_with("messages[0]:"), "{e}");
    }

    #[test]
    fn episodes_are_billed_by_size() {
        let usage = |contents: &[&str]| billed("".into(), contents.iter().copied()).usage;
        let one = "x".repeat(350);
        let two = "x".repeat(640);
        let four = "x".repeat(1200);
        assert_eq!(
            usage(&[&one, &two, &four, "é"]),
            ToolUsage::from([("credits", 8.0), ("episodes", 4.0)])
        );
        let body = messages_body(&args(
            json!({"messages": [{"content": "hi"}, {"content": two}]}),
        ))
        .expect("valid");
        let contents = array_of(&body, "messages").iter();
        assert_eq!(
            billed("".into(), contents.filter_map(|m| str_of(m, "content"))).usage,
            ToolUsage::from([("credits", 3.0), ("episodes", 2.0)])
        );
    }

    #[test]
    fn renders_search() {
        let edges = render_search(
            "edges",
            &json!({"items": [{
                "fact": "Jane is vegan", "name": "HAS_DIET", "source_node_name": "Jane",
                "target_node_name": "vegan", "valid_at": "2024-03-15T14:30:00Z",
                "invalid_at": null, "episode_uuids": ["e"], "attributes": {}, "score": 0.9,
                "uuid": "u1"
            }], "next_cursor": "c"}),
        );
        let parsed: Value = serde_json::from_str(&edges).expect("json");
        assert_eq!(
            parsed,
            json!([{
                "fact": "Jane is vegan", "source": "Jane", "relation": "HAS_DIET",
                "target": "vegan", "valid_at": "2024-03-15T14:30:00Z", "uuid": "u1"
            }])
        );
        let nodes = render_search(
            "nodes",
            &json!({"items": [{
                "name": "Jane", "labels": ["Person"], "summary": "A user.", "uuid": "n1",
                "episode_uuids": ["e"], "episode_uuids_truncated": false
            }]}),
        );
        let parsed: Value = serde_json::from_str(&nodes).expect("json");
        assert_eq!(
            parsed,
            json!([{"name": "Jane", "labels": ["Person"], "summary": "A user.", "uuid": "n1"}])
        );
        let long = "x".repeat(EPISODE_CHARS + 10);
        let episodes = render_search(
            "episodes",
            &json!({"items": [{"content": long, "role": "user", "role_name": "Jane", "source": "message", "uuid": "p1"}]}),
        );
        let parsed: Value = serde_json::from_str(&episodes).expect("json");
        assert_eq!(parsed[0]["name"], "Jane");
        assert_eq!(
            parsed[0]["content"].as_str().map(|s| s.chars().count()),
            Some(EPISODE_CHARS + 1)
        );
        assert_eq!(render_search("edges", &json!({"items": []})), "No results.");
    }

    #[test]
    fn renders_context_and_additions() {
        assert_eq!(
            render_context(&json!({"context": "## Facts\n- Jane is vegan\n", "truncated": true})),
            "## Facts\n- Jane is vegan\n\n(cut to max_characters)"
        );
        assert_eq!(
            render_context(&json!({})),
            "No context: the graph holds nothing relevant yet."
        );
        assert_eq!(
            render_added_messages(&json!({
                "messages": [{"uuid": "m1", "content": "hi"}, {"uuid": "m2"}],
                "task": {"uuid": "t1", "status": "pending"},
                "context": "- Jane is vegan"
            })),
            "Added 2 message(s) (m1, m2). Extraction task t1: pending.\n\nContext:\n- Jane is vegan"
        );
        assert_eq!(
            render_added_episode(&json!({
                "episode": {"uuid": "p1", "graph_uuid": G, "processed": false},
                "task": {"uuid": "t2", "status": "processing"}
            })),
            format!(
                "Added episode p1 to graph {G}; it is searchable once processed. \
                 Extraction task t2: processing."
            )
        );
    }
}
