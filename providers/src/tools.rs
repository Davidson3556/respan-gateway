//! Tools in both conversation formats, as the kind has them: the tools a
//! request offers and its choice, an assistant's calls and their results,
//! and the calls of a whole response being written. What a format says of
//! tools that the kind has no form for, such as a tool the provider runs,
//! stays in its `unsupported` fields.

use respan_gateway_sdk::Fault;
use serde_json::{Map, Value, json};

use crate::anthropic::ANTHROPIC;
use crate::common::{
    Names, only, patch_content, read_content, untranslatable, unwritable_content, write_content,
    write_message,
};
use crate::conversation::{Content, Conversation, Message, Role, Tool, ToolChoice};
use crate::openai::OPENAI;

/// The start a tool call piece names: its id and the tool's name.
pub(crate) fn start<'a>(
    id: &'a Option<String>,
    name: &'a Option<String>,
) -> Option<(&'a str, &'a str)> {
    id.as_deref().zip(name.as_deref())
}

/// A tool call's arguments as JSON: an object, `{}` for none.
pub(crate) fn arguments_json(arguments: &str) -> Result<Value, Fault> {
    if arguments.trim().is_empty() {
        return Ok(json!({}));
    }
    let not_json = |_| Fault::bad_request("a tool call's arguments are not JSON");
    serde_json::from_str(arguments).map_err(not_json)
}

/// `fields` but those named `taken`.
fn rest_of(fields: &Value, taken: &[&str]) -> Map<String, Value> {
    let fields = fields.as_object().into_iter().flatten();
    let rest = fields.filter(|(k, v)| !taken.contains(&k.as_str()) && !v.is_null());
    rest.map(|(k, v)| (k.clone(), v.clone())).collect()
}

/// Set `key` to `value`, or remove it if there is none.
fn set(v: &mut Map<String, Value>, key: &str, value: Option<Value>) {
    match value {
        Some(value) => _ = v.insert(key.into(), value),
        None => _ = v.shift_remove(key),
    }
}

/// Read `tools`, `tool_choice` and whether calls may be parallel with
/// `read` each, taking each the kind can say out of `chat.unsupported`.
fn read_tools(
    chat: &mut Conversation,
    tools: Option<Vec<Tool>>,
    choice: Option<Option<ToolChoice>>,
    parallel: Option<Option<bool>>,
    keys: [&str; 3],
) {
    if let Some(tools) = tools {
        chat.tools = tools;
        chat.unsupported.shift_remove(keys[0]);
    }
    if let Some(choice) = choice {
        chat.tool_choice = choice;
        chat.unsupported.shift_remove(keys[1]);
    }
    if let Some(parallel) = parallel {
        chat.parallel_tool_calls = parallel;
        if !keys[2].is_empty() {
            chat.unsupported.shift_remove(keys[2]);
        }
    }
}

/// The tools of `v`, if each is one the kind can say, by `tool`.
fn each_tool(v: &Value, tool: fn(&Value) -> Option<Tool>) -> Option<Vec<Tool>> {
    match v {
        Value::Null => Some(Vec::new()),
        Value::Array(tools) => tools.iter().map(tool).collect(),
        _ => None,
    }
}

/// OpenAI's function tools, choice and `parallel_tool_calls`, as the kind's.
pub(crate) fn read_openai_tools(v: &Value, chat: &mut Conversation) {
    let tools = each_tool(&v["tools"], openai_tool);
    let choice = match &v["tool_choice"] {
        Value::Null => Some(None),
        choice => openai_choice(choice).map(Some),
    };
    let parallel = match &v["parallel_tool_calls"] {
        Value::Null => Some(None),
        parallel => parallel.as_bool().map(Some),
    };
    let keys = ["tools", "tool_choice", "parallel_tool_calls"];
    read_tools(chat, tools, choice, parallel, keys);
}

fn openai_tool(tool: &Value) -> Option<Tool> {
    let function = &tool["function"];
    if tool["type"] != "function" || !only(tool, &["type", "function"]) {
        return None;
    }
    let description = match &function["description"] {
        Value::Null => None,
        text => Some(text.as_str()?.to_owned()),
    };
    let mut kind = Tool::new(function["name"].as_str()?, function["parameters"].clone());
    kind.description = description;
    kind.unsupported = rest_of(function, &["name", "description", "parameters"]);
    Some(kind)
}

fn openai_choice(choice: &Value) -> Option<ToolChoice> {
    Some(match choice {
        Value::String(choice) => match choice.as_str() {
            "auto" => ToolChoice::Auto,
            "none" => ToolChoice::None,
            "required" => ToolChoice::Required,
            _ => return None,
        },
        choice if choice["type"] == "function" && only(choice, &["type", "function"]) => {
            if !only(&choice["function"], &["name"]) {
                return None;
            }
            ToolChoice::Tool(choice["function"]["name"].as_str()?.into())
        }
        _ => return None,
    })
}

/// The kind's tools as OpenAI's fields of a request `v`.
pub(crate) fn write_openai_tools(
    chat: &Conversation,
    v: &mut Map<String, Value>,
) -> Result<(), Fault> {
    let tool = |t: &Tool| {
        let mut function = Map::new();
        function.insert("name".into(), t.name.as_str().into());
        if let Some(text) = &t.description {
            function.insert("description".into(), text.as_str().into());
        }
        if !t.parameters.is_null() {
            function.insert("parameters".into(), t.parameters.clone());
        }
        function.extend(t.unsupported.clone());
        json!({ "type": "function", "function": function })
    };
    let tools: Vec<Value> = chat.tools.iter().map(tool).collect();
    set(v, "tools", (!tools.is_empty()).then(|| tools.into()));
    let choice = match &chat.tool_choice {
        None => None,
        Some(ToolChoice::Auto) => Some("auto".into()),
        Some(ToolChoice::None) => Some("none".into()),
        Some(ToolChoice::Required) => Some("required".into()),
        Some(ToolChoice::Tool(name)) => {
            Some(json!({ "type": "function", "function": { "name": name } }))
        }
        Some(_) => return Err(untranslatable("that tool choice")),
    };
    set(v, "tool_choice", choice);
    set(
        v,
        "parallel_tool_calls",
        chat.parallel_tool_calls.map(Into::into),
    );
    Ok(())
}

/// Anthropic's custom tools and choice, with whether calls may be
/// parallel, as the kind's.
pub(crate) fn read_anthropic_tools(v: &Value, chat: &mut Conversation) {
    let tools = each_tool(&v["tools"], anthropic_tool);
    let choice = match &v["tool_choice"] {
        Value::Null => Some((None, None)),
        choice => anthropic_choice(choice),
    };
    let (choice, parallel) = choice.unzip();
    read_tools(chat, tools, choice, parallel, ["tools", "tool_choice", ""]);
}

fn anthropic_tool(tool: &Value) -> Option<Tool> {
    let custom = tool["type"].is_null() || tool["type"] == "custom";
    if !custom {
        return None;
    }
    let description = match &tool["description"] {
        Value::Null => None,
        text => Some(text.as_str()?.to_owned()),
    };
    let mut kind = Tool::new(tool["name"].as_str()?, tool["input_schema"].clone());
    kind.description = description;
    kind.unsupported = rest_of(tool, &["type", "name", "description", "input_schema"]);
    Some(kind)
}

/// Anthropic's `tool_choice`, and whether calls may be parallel if it says.
fn anthropic_choice(choice: &Value) -> Option<(Option<ToolChoice>, Option<bool>)> {
    if !only(choice, &["type", "name", "disable_parallel_tool_use"]) {
        return None;
    }
    let kind = match (choice["type"].as_str()?, choice["name"].as_str()) {
        ("auto", None) => ToolChoice::Auto,
        ("none", None) => ToolChoice::None,
        ("any", None) => ToolChoice::Required,
        ("tool", Some(name)) => ToolChoice::Tool(name.into()),
        _ => return None,
    };
    let parallel = match &choice["disable_parallel_tool_use"] {
        Value::Null => None,
        disable => Some(!disable.as_bool()?),
    };
    Some((Some(kind), parallel))
}

/// The kind's tools as Anthropic's fields of a request `v`. Calls one at a
/// time are said inside `tool_choice`.
pub(crate) fn write_anthropic_tools(
    chat: &Conversation,
    v: &mut Map<String, Value>,
) -> Result<(), Fault> {
    let tool = |t: &Tool| {
        let mut tool = Map::new();
        tool.insert("name".into(), t.name.as_str().into());
        if let Some(text) = &t.description {
            tool.insert("description".into(), text.as_str().into());
        }
        let schema = match t.parameters.is_object() {
            true => t.parameters.clone(),
            false => json!({ "type": "object" }),
        };
        tool.insert("input_schema".into(), schema);
        tool.extend(t.unsupported.clone());
        Value::Object(tool)
    };
    let tools: Vec<Value> = chat.tools.iter().map(tool).collect();
    set(v, "tools", (!tools.is_empty()).then(|| tools.into()));
    let one_at_a_time = chat.parallel_tool_calls == Some(false);
    let mut choice = match &chat.tool_choice {
        Some(ToolChoice::Auto) => Some(json!({ "type": "auto" })),
        Some(ToolChoice::None) => Some(json!({ "type": "none" })),
        Some(ToolChoice::Required) => Some(json!({ "type": "any" })),
        Some(ToolChoice::Tool(name)) => Some(json!({ "type": "tool", "name": name })),
        None => one_at_a_time.then(|| json!({ "type": "auto" })),
        Some(_) => return Err(untranslatable("that tool choice")),
    };
    if let Some(c) = choice
        .as_mut()
        .filter(|c| one_at_a_time && c["type"] != "none")
    {
        c["disable_parallel_tool_use"] = true.into();
    }
    set(v, "tool_choice", choice);
    Ok(())
}

/// An Anthropic `tool_use` or `tool_result` block as the kind's part, if it
/// can say all of it. A field it has no place for, such as a cache marker,
/// on the block or on a text block of a result, keeps it the format's own:
/// translating it is refused, not dropped.
pub(crate) fn anthropic_part(block: &Value) -> Option<Content> {
    match block["type"].as_str()? {
        "tool_use" if only(block, &["type", "id", "name", "input"]) => {
            let input = block["input"].as_object()?;
            let arguments = Value::Object(input.clone()).to_string();
            let (id, name) = (block["id"].as_str()?, block["name"].as_str()?);
            Some(Content::tool_call(id, name, arguments))
        }
        "tool_result" => {
            if !only(block, &["type", "tool_use_id", "content", "is_error"]) {
                return None;
            }
            let is_error = match &block["is_error"] {
                Value::Null => false,
                is_error => is_error.as_bool()?,
            };
            let blocks = block["content"].as_array().into_iter().flatten();
            let marked = |b: &Value| b["type"] == "text" && !only(b, &["type", "text"]);
            if blocks.into_iter().any(marked) {
                return None;
            }
            let content = read_content(ANTHROPIC, &block["content"]);
            let id = block["tool_use_id"].as_str()?;
            Some(Content::tool_result(id, content, is_error))
        }
        _ => None,
    }
}

/// The kind's tool call or result as an Anthropic block.
pub(crate) fn anthropic_block(part: &Content) -> Result<Value, Fault> {
    match part {
        Content::ToolCall {
            id,
            name,
            arguments,
            ..
        } => {
            let input = arguments_json(arguments)?;
            Ok(json!({ "type": "tool_use", "id": id, "name": name, "input": input }))
        }
        Content::ToolResult {
            id,
            content,
            is_error,
            ..
        } => {
            let mut block = json!({ "type": "tool_result", "tool_use_id": id });
            if !content.is_empty() {
                block["content"] = write_content(ANTHROPIC, content)?;
            }
            if *is_error {
                block["is_error"] = true.into();
            }
            Ok(block)
        }
        _ => Err(untranslatable("an unsupported kind of part")),
    }
}

/// An Anthropic tool block `old`, which read `was`, changed to read `now`,
/// keeping what else it says, such as a cache marker.
pub(crate) fn update_anthropic_block(
    old: &Value,
    was: &Content,
    now: &Content,
) -> Result<Value, Fault> {
    let mut block = old.clone();
    match (was, now) {
        (
            Content::ToolCall {
                arguments: before, ..
            },
            Content::ToolCall {
                id,
                name,
                arguments,
                ..
            },
        ) => {
            block["id"] = id.as_str().into();
            block["name"] = name.as_str().into();
            if before != arguments {
                block["input"] = arguments_json(arguments)?;
            }
        }
        (
            Content::ToolResult {
                content: before, ..
            },
            Content::ToolResult {
                id,
                content,
                is_error,
                ..
            },
        ) => {
            block["tool_use_id"] = id.as_str().into();
            if before != content {
                let old = block.get("content");
                block["content"] = patch_content(ANTHROPIC, old, before, content)?;
            }
            match is_error {
                true => block["is_error"] = true.into(),
                false => _ = block.as_object_mut().map(|b| b.shift_remove("is_error")),
            }
        }
        _ => return anthropic_block(now),
    }
    Ok(block)
}

/// An OpenAI message read as the kind's ([`read_message`]
/// (crate::common::read_message)) with its tools: a tool message as a user
/// message of one result, an assistant's `tool_calls` as calls after its
/// content. What the kind cannot say of them stays unsupported.
pub(crate) fn read_openai_message(m: &Value, message: &mut Message) {
    if message.role == Role::Other("tool".into()) {
        let Some(id) = m["tool_call_id"].as_str() else {
            return;
        };
        let content = std::mem::take(&mut message.content);
        message.content = vec![Content::tool_result(id, content, false)];
        message.role = Role::User;
        message.unsupported.shift_remove("tool_call_id");
        return;
    }
    let calls = m["tool_calls"].as_array().into_iter().flatten();
    let calls: Option<Vec<Content>> = calls.map(openai_call).collect();
    if let Some(calls) = calls.filter(|calls| !calls.is_empty()) {
        message.content.extend(calls);
        message.unsupported.shift_remove("tool_calls");
    }
}

fn openai_call(call: &Value) -> Option<Content> {
    let function = &call["function"];
    let function_call = call["type"].is_null() || call["type"] == "function";
    let known = only(call, &["id", "type", "function"]) && only(function, &["name", "arguments"]);
    if !function_call || !known {
        return None;
    }
    let (id, name) = (call["id"].as_str()?, function["name"].as_str()?);
    Some(Content::tool_call(
        id,
        name,
        function["arguments"].as_str()?,
    ))
}

/// The kind's calls as OpenAI's `tool_calls`.
fn openai_calls(calls: &[&Content]) -> Vec<Value> {
    let call = |c: &&Content| match c {
        Content::ToolCall {
            id,
            name,
            arguments,
            ..
        } => {
            let function = json!({ "name": name, "arguments": arguments });
            Some(json!({ "id": id, "type": "function", "function": function }))
        }
        _ => None,
    };
    calls.iter().filter_map(call).collect()
}

/// A tool message answering call `id` with `content`.
fn openai_result(
    id: &str,
    content: &[Content],
    is_error: bool,
) -> Result<Map<String, Value>, Fault> {
    // OpenAI's tool messages cannot say the call failed.
    if is_error {
        return Err(untranslatable("a tool result that is an error"));
    }
    let mut v = Map::new();
    v.insert("role".into(), "tool".into());
    v.insert("tool_call_id".into(), id.into());
    v.insert("content".into(), write_content(OPENAI, content)?);
    Ok(v)
}

/// A message as OpenAI's: each result a tool message of its own, then, if
/// it says more, the message, its calls in `tool_calls`.
pub(crate) fn write_openai_message(m: &Message, roles: &Names<Role>) -> Result<Vec<Value>, Fault> {
    let is = |p: &&Content, result: bool| matches!(p, Content::ToolResult { .. }) == result;
    let results: Vec<&Content> = m.content.iter().filter(|p| is(p, true)).collect();
    let rest: Vec<&Content> = m.content.iter().filter(|p| is(p, false)).collect();
    let mut out = Vec::new();
    for result in &results {
        if let Content::ToolResult {
            id,
            content,
            is_error,
            ..
        } = result
        {
            out.push(openai_result(id, content, *is_error)?);
        }
    }
    // A message of one result keeps its other fields, such as a name.
    if let ([only], true) = (&mut out[..], rest.is_empty()) {
        only.extend(m.unsupported.clone());
        return Ok(out.into_iter().map(Value::Object).collect());
    }
    let mut out: Vec<Value> = out.into_iter().map(Value::Object).collect();
    if !rest.is_empty() || results.is_empty() || !m.unsupported.is_empty() {
        let call = |p: &&Content| matches!(p, Content::ToolCall { .. });
        let calls: Vec<&Content> = rest.iter().copied().filter(call).collect();
        let mut said = m.clone();
        said.content = rest.into_iter().filter(|p| !call(p)).cloned().collect();
        let mut v = write_message(OPENAI, &said, roles)?;
        if !calls.is_empty() {
            v["tool_calls"] = openai_calls(&calls).into();
            if said.content.is_empty() {
                v["content"] = Value::Null;
            }
        }
        out.push(v);
    }
    Ok(out)
}

/// An OpenAI message `old`, which read `was`, changed to read `now`, if its
/// tools let it stay one message: a tool message's result, or an
/// assistant's calls, written in. `None` if it must be written anew.
pub(crate) fn update_openai_message(
    old: &Value,
    was: &Message,
    now: &Message,
    update: impl Fn(&Value, &Message, &Message) -> Result<Value, Fault>,
) -> Result<Option<Value>, Fault> {
    let result = |m: &Message| match &m.content[..] {
        [
            Content::ToolResult {
                id,
                content,
                is_error,
                ..
            },
        ] => Some((id.clone(), content.clone(), *is_error)),
        _ => None,
    };
    let has_result = |m: &Message| {
        m.content
            .iter()
            .any(|p| matches!(p, Content::ToolResult { .. }))
    };
    match (result(was), result(now)) {
        (Some((_, before, _)), Some((id, content, is_error))) => {
            if is_error {
                return Err(untranslatable("a tool result that is an error"));
            }
            let (mut was, mut now) = (was.clone(), now.clone());
            (was.content, now.content) = (before, content);
            let mut v = update(old, &was, &now)?;
            v["tool_call_id"] = id.into();
            return Ok(Some(v));
        }
        _ if has_result(was) || has_result(now) => return Ok(None),
        _ => {}
    }
    let call = |p: &&Content| matches!(p, Content::ToolCall { .. });
    let split = |m: &Message| {
        let calls: Vec<&Content> = m.content.iter().filter(call).collect();
        let mut said = m.clone();
        said.content
            .retain(|p| !matches!(p, Content::ToolCall { .. }));
        (said, openai_calls(&calls))
    };
    let ((was, before), (now, calls)) = (split(was), split(now));
    let mut v = update(old, &was, &now)?;
    if before != calls {
        match calls.is_empty() {
            true => _ = v.as_object_mut().map(|v| v.shift_remove("tool_calls")),
            false => v["tool_calls"] = calls.into(),
        }
    }
    // A message of calls alone has no content.
    if was.content != now.content && now.content.is_empty() && v.get("tool_calls").is_some() {
        v["content"] = Value::Null;
    }
    Ok(Some(v))
}

/// Tool calls of a whole response as they are written: id, name, and
/// arguments in JSON text.
#[derive(Debug, Default)]
pub(crate) struct Calls(Vec<Pending>);

#[derive(Debug)]
struct Pending {
    /// The call's number in the response.
    key: u32,
    id: String,
    name: String,
    arguments: String,
}

impl Calls {
    /// Add a piece of call `call`: a start makes the call, arguments go to
    /// it. A start of a known call repeats it, as some providers send.
    pub(crate) fn add(
        &mut self,
        call: u32,
        start: Option<(&str, &str)>,
        arguments: &str,
    ) -> Result<(), Fault> {
        match (start, self.0.iter().position(|c| c.key == call)) {
            (Some((id, name)), None) => self.0.push(Pending {
                key: call,
                id: id.into(),
                name: name.into(),
                arguments: arguments.into(),
            }),
            (_, Some(at)) => self.0[at].arguments.push_str(arguments),
            (None, None) => return Err(unwritable_content()),
        }
        Ok(())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    /// As OpenAI's `tool_calls`.
    pub(crate) fn openai(&self) -> Vec<Value> {
        let call = |c: &Pending| {
            let arguments = if c.arguments.is_empty() {
                "{}"
            } else {
                &c.arguments
            };
            let function = json!({ "name": c.name, "arguments": arguments });
            json!({ "id": c.id, "type": "function", "function": function })
        };
        self.0.iter().map(call).collect()
    }

    /// As Anthropic's `tool_use` blocks; arguments cut off are no input.
    pub(crate) fn anthropic(&self) -> Result<Vec<Value>, Fault> {
        let block = |c: &Pending| {
            let input = arguments_json(&c.arguments).map_err(|_| unwritable_content())?;
            Ok(json!({ "type": "tool_use", "id": c.id, "name": c.name, "input": input }))
        };
        self.0.iter().map(block).collect()
    }
}
