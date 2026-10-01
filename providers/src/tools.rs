//! Tools between OpenAI chat and Anthropic messages, for the fallback that
//! crosses formats. A request is adopted whole: `tools`, `tool_choice`, the
//! calls of assistant messages and the results after them are rewritten as
//! the target has them, or the request is refused. A response's tool calls
//! go the other way as [`Fragment`]s, which each format's writer writes
//! anew, whole or streamed.

use std::borrow::Cow;

use respan_gateway_sdk::Fault;
use serde_json::{Value, json};

use crate::anthropic::ANTHROPIC;
use crate::common::{refuse_leftovers, says, untranslatable, unwritable_content};
use crate::conversation::{Content, Conversation, Message, Role};
use crate::openai::{INERT_CHOICE, OPENAI};

/// `chat` as the format `target` has it: as it is if it came in that
/// format; with its tools rewritten if it came in the other; else with
/// nothing the kind has no place for. What is left over cannot be sent in
/// another format, so it is refused.
pub(crate) fn adopt<'a>(
    chat: &'a Conversation,
    target: &str,
) -> Result<Cow<'a, Conversation>, Fault> {
    let from = chat.unsupported_in.as_deref();
    if from == Some(target) {
        return Ok(Cow::Borrowed(chat));
    }
    let mut chat = chat.clone();
    match (from, target) {
        (Some(OPENAI), ANTHROPIC) => to_anthropic(&mut chat)?,
        (Some(ANTHROPIC), OPENAI) => to_openai(&mut chat)?,
        _ => refuse_leftovers(&chat)?,
    }
    Ok(Cow::Owned(chat))
}

/// The request's `tools` mapped by `tool`, and its `tool_choice` and
/// `parallel_tool_calls` taken out and returned; any other field it has
/// no place for is refused.
fn take_tools(
    chat: &mut Conversation,
    tool: fn(&Value) -> Result<Value, Fault>,
) -> Result<(Option<Value>, Option<Value>), Fault> {
    let (mut choice, mut parallel) = (None, None);
    for (key, value) in std::mem::take(&mut chat.unsupported) {
        match key.as_str() {
            "tools" => {
                let tools = value.as_array().into_iter().flatten();
                let tools: Vec<Value> = tools.map(tool).collect::<Result<_, _>>()?;
                chat.unsupported.insert(key, tools.into());
            }
            "tool_choice" => choice = Some(value),
            "parallel_tool_calls" => parallel = Some(value),
            _ => return Err(untranslatable(&format!("'{key}' in the request"))),
        }
    }
    Ok((choice, parallel))
}

/// Rewrites every message with `each`, which may make several of one.
fn map_messages(
    chat: &mut Conversation,
    each: fn(Message) -> Result<Vec<Message>, Fault>,
) -> Result<(), Fault> {
    let mut all = Vec::new();
    for message in std::mem::take(&mut chat.messages) {
        all.extend(each(message)?);
    }
    chat.messages = all;
    Ok(())
}

fn to_anthropic(chat: &mut Conversation) -> Result<(), Fault> {
    let (choice, parallel) = take_tools(chat, anthropic_tool)?;
    // Anthropic says it inside `tool_choice`.
    let one_at_a_time = parallel == Some(Value::Bool(false));
    let mut choice = match choice {
        Some(c) => Some(anthropic_choice(&c)?),
        None => one_at_a_time.then(|| json!({ "type": "auto" })),
    };
    if let Some(c) = choice
        .as_mut()
        .filter(|c| one_at_a_time && c["type"] != "none")
    {
        c["disable_parallel_tool_use"] = true.into();
    }
    if let Some(choice) = choice {
        chat.unsupported.insert("tool_choice".into(), choice);
    }
    map_messages(chat, anthropic_message)
}

fn to_openai(chat: &mut Conversation) -> Result<(), Fault> {
    let (choice, _) = take_tools(chat, openai_tool)?;
    if let Some(choice) = choice {
        let (choice, one_at_a_time) = openai_choice(&choice)?;
        chat.unsupported.insert("tool_choice".into(), choice);
        if one_at_a_time {
            chat.unsupported
                .insert("parallel_tool_calls".into(), false.into());
        }
    }
    map_messages(chat, openai_message)
}

/// `object` if it has only the fields `known` (`null`s aside): what has no
/// equivalent in the other format is refused, not dropped.
fn known_fields<'a>(object: &'a Value, known: &[&str], what: &str) -> Result<&'a Value, Fault> {
    let unknown = object.as_object().into_iter().flatten();
    let mut unknown = unknown.filter(|(k, v)| !v.is_null() && !known.contains(&k.as_str()));
    match unknown.next() {
        None => Ok(object),
        Some((key, _)) => Err(untranslatable(&format!("{what}'s '{key}'"))),
    }
}

fn anthropic_tool(tool: &Value) -> Result<Value, Fault> {
    let tool = known_fields(tool, &["type", "function"], "a tool")?;
    let function = known_fields(
        &tool["function"],
        &["name", "description", "parameters"],
        "a tool's function",
    )?;
    let (true, Some(name)) = (tool["type"] == "function", function["name"].as_str()) else {
        return Err(untranslatable("a tool that is not a function"));
    };
    let mut out = json!({ "name": name });
    if let Some(text) = function["description"].as_str() {
        out["description"] = text.into();
    }
    let schema = &function["parameters"];
    out["input_schema"] = match schema.is_object() {
        true => schema.clone(),
        false => json!({ "type": "object" }),
    };
    Ok(out)
}

fn openai_tool(tool: &Value) -> Result<Value, Fault> {
    let known = ["type", "name", "description", "input_schema"];
    let tool = known_fields(tool, &known, "a tool")?;
    let custom = tool["type"].is_null() || tool["type"] == "custom";
    let (true, Some(name)) = (custom, tool["name"].as_str()) else {
        return Err(untranslatable("a tool that Anthropic runs"));
    };
    let mut function = json!({ "name": name, "parameters": tool["input_schema"] });
    if let Some(text) = tool["description"].as_str() {
        function["description"] = text.into();
    }
    Ok(json!({ "type": "function", "function": function }))
}

fn anthropic_choice(choice: &Value) -> Result<Value, Fault> {
    Ok(
        match (choice.as_str(), choice["function"]["name"].as_str()) {
            (Some("auto"), _) => json!({ "type": "auto" }),
            (Some("none"), _) => json!({ "type": "none" }),
            (Some("required"), _) => json!({ "type": "any" }),
            (None, Some(name)) => json!({ "type": "tool", "name": name }),
            _ => return Err(untranslatable("that 'tool_choice'")),
        },
    )
}

/// OpenAI's `tool_choice`, and whether the model may call one tool only.
fn openai_choice(choice: &Value) -> Result<(Value, bool), Fault> {
    let choice = known_fields(
        choice,
        &["type", "name", "disable_parallel_tool_use"],
        "a 'tool_choice'",
    )?;
    let one = choice["disable_parallel_tool_use"] == true;
    let choice = match (choice["type"].as_str(), choice["name"].as_str()) {
        (Some("auto"), _) => "auto".into(),
        (Some("none"), _) => "none".into(),
        (Some("any"), _) => "required".into(),
        (Some("tool"), Some(name)) => json!({ "type": "function", "function": { "name": name } }),
        _ => return Err(untranslatable("that 'tool_choice'")),
    };
    Ok((choice, one))
}

/// A message's fields `keys` removed and returned; any other is refused.
fn take_fields<const N: usize>(
    m: &mut Message,
    keys: [&str; N],
) -> Result<[Option<Value>; N], Fault> {
    let taken = keys.map(|k| m.unsupported.shift_remove(k));
    match m.unsupported.keys().next() {
        None => Ok(taken),
        Some(key) => Err(untranslatable(&format!("a message's '{key}'"))),
    }
}

/// A tool call's arguments as JSON: an object, `{}` for none.
fn arguments_json(arguments: &str) -> Result<Value, Fault> {
    if arguments.trim().is_empty() {
        return Ok(json!({}));
    }
    let not_json = |_| Fault::bad_request("a tool call's arguments are not JSON");
    serde_json::from_str(arguments).map_err(not_json)
}

fn anthropic_message(mut m: Message) -> Result<Vec<Message>, Fault> {
    let [calls, id] = take_fields(&mut m, ["tool_calls", "tool_call_id"])?;
    if let Role::Other(role) = &m.role
        && role != "tool"
    {
        return Err(untranslatable(&format!("a message of role '{role}'")));
    }
    if matches!(&m.role, Role::Other(_)) {
        let text = crate::common::text_of(&m.content)
            .ok_or_else(|| untranslatable("a tool result that is not text"))?;
        let id = id.ok_or_else(|| Fault::bad_request("a tool result names no call"))?;
        let block = json!({ "type": "tool_result", "tool_use_id": id, "content": text });
        m.role = Role::User;
        m.content = vec![Content::other(ANTHROPIC, block)];
    } else if let Some(calls) = calls {
        // Anthropic refuses empty text.
        let empty = |c: &Content| matches!(c, Content::Text { text, .. } if text.is_empty());
        m.content.retain(|c| !empty(c));
        for call in calls.as_array().into_iter().flatten() {
            let call = known_fields(call, &["id", "type", "function"], "a tool call")?;
            let function = known_fields(&call["function"], &["name", "arguments"], "a call")?;
            let (Some(id), Some(name)) = (call["id"].as_str(), function["name"].as_str()) else {
                return Err(untranslatable("a tool call that is not a function"));
            };
            let input = arguments_json(function["arguments"].as_str().unwrap_or_default())?;
            let block = json!({ "type": "tool_use", "id": id, "name": name, "input": input });
            m.content.push(Content::other(ANTHROPIC, block));
        }
    }
    Ok(vec![m])
}

/// The text of a tool result's content: a string, or text blocks.
fn result_text(content: &Value) -> Result<String, Fault> {
    let blocks = match content {
        Value::String(text) => return Ok(text.clone()),
        Value::Array(blocks) => blocks.as_slice(),
        _ => &[],
    };
    for block in blocks {
        known_fields(block, &["type", "text"], "a tool result's text block")?;
    }
    let all: Option<String> = blocks
        .iter()
        .map(|b| b["text"].as_str().filter(|_| b["type"] == "text"))
        .collect();
    all.ok_or_else(|| untranslatable("a tool result that is not text"))
}

/// An Anthropic message as OpenAI's: its tool calls a field of it, and each
/// result a message of its own, before what else the turn says.
fn openai_message(mut m: Message) -> Result<Vec<Message>, Fault> {
    take_fields(&mut m, [])?;
    if let Role::Other(role) = &m.role {
        return Err(untranslatable(&format!("a message of role '{role}'")));
    }
    let (mut calls, mut results) = (Vec::new(), Vec::new());
    let mut rest = Vec::new();
    for part in std::mem::take(&mut m.content) {
        let block = match &part {
            Content::Other { format, raw, .. } if **format == *ANTHROPIC => Some(raw.clone()),
            _ => None,
        };
        match block.as_ref().and_then(|b| Some((b, b["type"].as_str()?))) {
            Some((b, "tool_use")) => {
                let b = known_fields(b, &["type", "id", "name", "input"], "a tool call")?;
                calls.push(json!({
                    "id": b["id"],
                    "type": "function",
                    "function": { "name": b["name"], "arguments": b["input"].to_string() },
                }))
            }
            Some((b, "tool_result")) => {
                let known = ["type", "tool_use_id", "content", "is_error"];
                let b = known_fields(b, &known, "a tool result")?;
                // OpenAI's tool messages cannot say the call failed.
                if b["is_error"] == true {
                    return Err(untranslatable("a tool result that is an error"));
                }
                let mut result =
                    Message::new(Role::Other("tool".into()), result_text(&b["content"])?);
                result
                    .unsupported
                    .insert("tool_call_id".into(), b["tool_use_id"].clone());
                results.push(result);
            }
            _ => rest.push(part),
        }
    }
    m.content = rest;
    if !calls.is_empty() {
        m.unsupported.insert("tool_calls".into(), calls.into());
    }
    let keep = results.is_empty() || !m.content.is_empty() || !m.unsupported.is_empty();
    results.extend(keep.then_some(m));
    Ok(results)
}

/// A piece of a tool call as a response says it, in either format: the
/// call's start (its id and name), and its arguments, whole or a part.
#[derive(Debug, Clone, Default)]
pub(crate) struct Fragment {
    /// Which call, if the format numbers them (OpenAI does, and streams
    /// parallel calls interleaved); else it is the last one.
    pub(crate) index: Option<usize>,
    pub(crate) start: Option<(String, String)>,
    pub(crate) arguments: String,
}

impl Fragment {
    /// The place, among calls of these `keys`, of the call it continues: the
    /// call it numbers, else the last if it is no start. A start that
    /// numbers a known call is a repeat of it, as some providers send.
    pub(crate) fn continues(
        &self,
        mut keys: impl ExactSizeIterator<Item = usize>,
    ) -> Option<usize> {
        match self.index {
            Some(key) => keys.position(|k| k == key),
            None => keys.len().checked_sub(1).filter(|_| self.start.is_none()),
        }
    }
}

/// The tool calls a provider's content says, if it says nothing else:
/// `raw` is `format`'s other content, such as the choices of an OpenAI
/// chunk or one Anthropic block or delta.
pub(crate) fn fragments(format: &str, raw: &Value) -> Option<Vec<Fragment>> {
    match format {
        OPENAI => openai_fragments(raw),
        ANTHROPIC => anthropic_fragment(raw).map(|f| vec![f]),
        _ => None,
    }
}

/// Whether `object` says nothing but in the fields `these`.
fn only(object: &Value, these: &[&str]) -> bool {
    let fields = object.as_object();
    fields.is_some_and(|o| {
        o.iter()
            .all(|(k, v)| these.contains(&k.as_str()) || !says(v))
    })
}

fn openai_fragments(choices: &Value) -> Option<Vec<Fragment>> {
    let [choice] = choices.as_array()?.as_slice() else {
        return None;
    };
    // Each choice of `n > 1` numbers its own calls: not one list of calls.
    if choice["index"].as_u64().is_some_and(|i| i != 0) {
        return None;
    }
    let mut fields = vec!["index", "delta", "message"];
    fields.extend(INERT_CHOICE);
    let body = [&choice["delta"], &choice["message"]];
    let body = body.into_iter().find(|b| b.is_object())?;
    if !only(choice, &fields) || !only(body, &["role", "tool_calls"]) {
        return None;
    }
    let mut all = Vec::new();
    for call in body["tool_calls"].as_array()? {
        if !(call["type"].is_null() || call["type"] == "function") {
            return None;
        }
        let function = &call["function"];
        let start = call["id"].as_str().zip(function["name"].as_str());
        all.push(Fragment {
            index: call["index"].as_u64().and_then(|i| usize::try_from(i).ok()),
            start: start.map(|(id, name)| (id.into(), name.into())),
            arguments: function["arguments"].as_str().unwrap_or_default().into(),
        });
    }
    Some(all)
}

fn anthropic_fragment(raw: &Value) -> Option<Fragment> {
    match raw["type"].as_str()? {
        "tool_use" => {
            let start = (raw["id"].as_str()?.into(), raw["name"].as_str()?.into());
            // A stream opens the block with an empty input.
            let input = raw["input"].as_object().filter(|o| !o.is_empty());
            let arguments = input.map(|o| Value::Object(o.clone()).to_string());
            Some(Fragment {
                start: Some(start),
                arguments: arguments.unwrap_or_default(),
                ..Fragment::default()
            })
        }
        "input_json_delta" => Some(Fragment {
            start: None,
            arguments: raw["partial_json"].as_str()?.into(),
            ..Fragment::default()
        }),
        _ => None,
    }
}

/// Tool calls of a whole response as they are written: id, name, and
/// arguments in JSON text.
#[derive(Debug, Default)]
pub(crate) struct Calls(Vec<Pending>);

#[derive(Debug)]
struct Pending {
    /// The provider's number of the call, else its place.
    key: usize,
    id: String,
    name: String,
    arguments: String,
}

impl Calls {
    /// Add a fragment: a start makes a call, arguments go to the call they
    /// continue ([`Fragment::continues`]).
    pub(crate) fn add(&mut self, fragment: Fragment) -> Result<(), Fault> {
        let at = fragment.continues(self.0.iter().map(|c| c.key));
        match (fragment.start, at) {
            (Some((id, name)), None) => self.0.push(Pending {
                key: fragment.index.unwrap_or(self.0.len()),
                id,
                name,
                arguments: fragment.arguments,
            }),
            (_, Some(at)) => self.0[at].arguments.push_str(&fragment.arguments),
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
