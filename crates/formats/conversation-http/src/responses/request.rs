//! What a Responses request says: its input items, tools and choice, text
//! format, and media. What the kind has no form for, such as a tool the
//! provider runs or an item it stored, stays in the `native` of the request,
//! the message, the tool or the part.
//!
//! The input's items are the conversation's messages: consecutive items an
//! assistant produced (messages, function calls, reasoning) are one assistant
//! message, and consecutive function call outputs are one user message of
//! results. A message item's `id` and `status` are inert (kept under
//! [`INERT`], dropped by any other format); on an assistant message the item's
//! other fields are under `message`, on the first text part, which opens a new
//! message item when it carries them. An item the kind has no form for is a
//! message of the native role `item`, holding the item as a native part.

use super::RESPONSES;
use crate::schema::{
    INERT, Keep, Names, NativeFieldsExt, decode_tools, each_tool, effort_name, encode_native,
    kept_fields, merge_missing, native_fields, only, own_native, parse_effort, reasoning_of,
    remainder, rest_of, says, set, tool_field, untranslatable,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use respan_gateway_conversation::{
    Content, Input, Media, MediaSource, Message, Reasoning, ResponseFormat, Role, Tool, ToolChoice,
};
use respan_gateway_core::{Fault, NativeFields};
use serde_json::{Map, Value, json};

/// Roles an input message can have.
const ROLES: Names<Role> = Names(
    &[
        ("system", Role::System),
        ("developer", Role::Developer),
        ("user", Role::User),
        ("assistant", Role::Assistant),
    ],
    RESPONSES,
);

/// The role of a message that holds an item the kind has no form for.
const ITEM_ROLE: &str = "item";

/// The key of an assistant message item's fields beyond its inert ones, on
/// its first text part.
const MESSAGE: &str = "message";

// ------------------------------------------------------------------ reading

/// The fields of `item` beyond `taken` as a native: its `id` and `status`
/// under [`INERT`].
fn item_fields(item: &Value, taken: &[&str]) -> Map<String, Value> {
    let mut rest = remainder(item, taken);
    let mut inert = Map::new();
    for key in ["id", "status"] {
        if let Some(value) = rest.shift_remove(key) {
            inert.insert(key.into(), value);
        }
    }
    if !inert.is_empty() {
        rest.insert(INERT.into(), inert.into());
    }
    rest
}

/// A text part of `text` with these fields of its own.
fn text_part(text: &str, fields: Map<String, Value>) -> Content {
    let mut part = Content::from(text);
    if let Content::Text { native, .. } = &mut part {
        *native = native_fields(RESPONSES, fields);
    }
    part
}

/// A message content: a string, or parts.
pub(super) fn read_content(v: &Value) -> Vec<Content> {
    match v {
        Value::String(text) => vec![Content::from(text.as_str())],
        Value::Array(parts) => parts.iter().map(read_part).collect(),
        Value::Null => Vec::new(),
        other => vec![Content::native(RESPONSES, other.clone())],
    }
}

fn read_part(p: &Value) -> Content {
    let part = match p["type"].as_str() {
        Some("input_text" | "output_text") => read_text(p),
        _ => read_media(p),
    };
    part.unwrap_or_else(|| Content::native(RESPONSES, p.clone()))
}

/// An `input_text` or `output_text` part; its fields that say something,
/// such as annotations, are its native.
fn read_text(p: &Value) -> Option<Content> {
    let text = p["text"].as_str()?;
    let fields = remainder(p, &["type", "text"]);
    let fields = fields.into_iter().filter(|(_, v)| says(v)).collect();
    Some(text_part(text, fields))
}

/// An image or file part the kind can say, whose other fields are its
/// media's native.
fn read_media(p: &Value) -> Option<Content> {
    let mut fields = match p["type"].as_str()? {
        "input_image" => remainder(p, &["type", "image_url"]),
        "input_file" => remainder(p, &["type", "file_data", "file_url", "filename"]),
        _ => return None,
    };
    fields.retain(|_, v| !v.is_null());
    let mut media = if p["type"] == "input_image" {
        // The default detail says nothing.
        if fields.get("detail") == Some(&json!("auto")) {
            fields.shift_remove("detail");
        }
        let (mime, source) = source_url(p["image_url"].as_str()?, "image/*")?;
        Media::new(mime, source)
    } else {
        let (mime, source) = match (p["file_data"].as_str(), p["file_url"].as_str()) {
            (Some(data), None) if data.starts_with("data:") => {
                source_url(data, "application/octet-stream")?
            }
            (None, Some(url)) => (
                "application/octet-stream".into(),
                MediaSource::Url(url.into()),
            ),
            _ => return None,
        };
        let mut media = Media::new(mime, source);
        media.name = p["filename"].as_str().map(str::to_owned);
        media
    };
    media.native = native_fields(RESPONSES, fields);
    Some(match p["type"] == "input_image" {
        true => Content::Image(media),
        false => Content::Document(media),
    })
}

/// The media type and source of a URL: the data of a `data:` URL, else the
/// URL, of type `fallback`.
fn source_url(url: &str, fallback: &str) -> Option<(String, MediaSource)> {
    match url.strip_prefix("data:") {
        Some(data) => {
            let (mime, data) = data.split_once(";base64,")?;
            let bytes = STANDARD.decode(data).ok()?;
            Some((mime.into(), MediaSource::Bytes(bytes.into())))
        }
        None => Some((fallback.into(), MediaSource::Url(url.into()))),
    }
}

/// The request's `instructions` and `input` as messages.
pub(super) fn read_input(v: &Value, chat: &mut Input) {
    if says(&v["instructions"]) {
        let mut system = Message::new(Role::System, "");
        system.content = read_content(&v["instructions"]);
        chat.messages.push(system);
    }
    match &v["input"] {
        Value::Null => {}
        Value::String(text) => chat.messages.push(Message::new(Role::User, text.as_str())),
        Value::Array(items) => items.iter().for_each(|i| push_item(&mut chat.messages, i)),
        other => chat.messages.push(opaque(other)),
    }
}

/// An item the kind has no form for.
fn opaque(item: &Value) -> Message {
    let mut message = Message::new(Role::native(RESPONSES, ITEM_ROLE), "");
    message.content = vec![Content::native(RESPONSES, item.clone())];
    message
}

/// The assistant message the next item of an assistant joins.
fn assistant(out: &mut Vec<Message>) -> &mut Message {
    if !matches!(out.last(), Some(m) if m.role == Role::Assistant) {
        let mut message = Message::new(Role::Assistant, "");
        message.content.clear();
        out.push(message);
    }
    let last = out.len() - 1;
    &mut out[last]
}

fn push_item(out: &mut Vec<Message>, item: &Value) {
    let kind = match item.get("type") {
        None | Some(Value::Null) => "message",
        Some(kind) => kind.as_str().unwrap_or_default(),
    };
    let done = match kind {
        "message" => push_message(out, item),
        "function_call" => push_call(out, item),
        "function_call_output" => push_result(out, item),
        "reasoning" => push_reasoning(out, item),
        _ => None,
    };
    if done.is_none() {
        out.push(opaque(item));
    }
}

fn push_message(out: &mut Vec<Message>, item: &Value) -> Option<()> {
    let name = item["role"].as_str()?;
    let mut content = read_content(&item["content"]);
    let extras = item_fields(item, &["type", "role", "content"]);
    match ROLES.value(name) {
        Some(Role::Assistant) => {
            // The item's own fields ride on its first text part.
            let first = content.iter_mut().find_map(|c| match c {
                Content::Text { native, .. } => Some(native),
                _ => None,
            })?;
            let mut fields = first.take().map(|n| n.fields).unwrap_or_default();
            let mut item_level = Map::new();
            for (key, value) in extras {
                match key == INERT {
                    true => fields.insert(key, value),
                    false => item_level.insert(key, value),
                };
            }
            if !item_level.is_empty() {
                fields.insert(MESSAGE.into(), item_level.into());
            }
            *first = native_fields(RESPONSES, fields);
            assistant(out).content.extend(content);
        }
        role => {
            let role = role.unwrap_or_else(|| Role::native(RESPONSES, name));
            let mut message = Message::new(role, "");
            message.content = content;
            message.native = native_fields(RESPONSES, extras);
            out.push(message);
        }
    }
    Some(())
}

fn push_call(out: &mut Vec<Message>, item: &Value) -> Option<()> {
    let (id, name) = (item["call_id"].as_str()?, item["name"].as_str()?);
    let mut call = Content::tool_call(id, name, item["arguments"].as_str()?);
    if let Content::ToolCall { native, .. } = &mut call {
        let taken = ["type", "call_id", "name", "arguments"];
        *native = native_fields(RESPONSES, item_fields(item, &taken));
    }
    assistant(out).content.push(call);
    Some(())
}

fn push_result(out: &mut Vec<Message>, item: &Value) -> Option<()> {
    let id = item["call_id"].as_str()?;
    let content = match &item["output"] {
        Value::String(_) | Value::Array(_) => read_content(&item["output"]),
        _ => return None,
    };
    let mut result = Content::tool_result(id, content, false);
    if let Content::ToolResult { native, .. } = &mut result {
        let taken = ["type", "call_id", "output"];
        *native = native_fields(RESPONSES, item_fields(item, &taken));
    }
    let results = |m: &Message| {
        m.role == Role::User
            && m.native.is_empty()
            && m.content
                .iter()
                .all(|c| matches!(c, Content::ToolResult { .. }))
    };
    match out.last_mut().filter(|m| results(m)) {
        Some(last) => last.content.push(result),
        None => {
            let mut message = Message::new(Role::User, "");
            message.content = vec![result];
            out.push(message);
        }
    }
    Some(())
}

fn push_reasoning(out: &mut Vec<Message>, item: &Value) -> Option<()> {
    let mut texts = Vec::new();
    for part in item["summary"].as_array()? {
        match (part["type"].as_str()?, part["text"].as_str()) {
            ("summary_text", Some(text)) if only(part, &["type", "text"]) => texts.push(text),
            _ => return None,
        }
    }
    let native = native_fields(RESPONSES, item_fields(item, &["type", "summary"]));
    let text = texts.join("\n\n");
    assistant(out)
        .content
        .push(Content::Reasoning { text, native });
    Some(())
}

// ------------------------------------------------------------------ writing

/// The fields of `native` to write in an item, its inert ones at its top.
fn written(native: &Option<NativeFields>, what: &str) -> Result<Map<String, Value>, Fault> {
    let what = |key: &str| format!("{what} '{key}'");
    let mut fields = kept_fields(RESPONSES, native, what, Keep::CACHE_MARKER)?;
    if let Some(Value::Object(inert)) = fields.shift_remove(INERT) {
        fields.extend(inert);
    }
    Ok(fields)
}

/// The request's `instructions` and `input`: a leading system message of
/// one plain text is the instructions.
pub(super) fn write_input(chat: &Input, v: &mut Map<String, Value>) -> Result<(), Fault> {
    let mut rest = &chat.messages[..];
    if let Some((first, after)) = rest.split_first()
        && first.role == Role::System
        && first.native.is_empty()
        && let [
            Content::Text {
                text, native: None, ..
            },
        ] = &first.content[..]
        && !text.is_empty()
    {
        v.insert("instructions".into(), text.as_str().into());
        rest = after;
    }
    let mut items = Vec::new();
    for m in rest {
        write_message(m, &mut items)?;
    }
    v.insert("input".into(), items.into());
    Ok(())
}

fn write_message(m: &Message, items: &mut Vec<Value>) -> Result<(), Fault> {
    let no_role = || untranslatable("a role this format has no place for");
    let role = match &m.role {
        Role::Native { format, name, .. } if **format == *RESPONSES && name == ITEM_ROLE => {
            return write_opaque(m, items);
        }
        Role::Native { format, name, .. } if **format == *RESPONSES => name.as_str(),
        Role::Native { .. } => return Err(no_role()),
        role => ROLES.name(role).ok_or_else(no_role)?,
    };
    match m.role {
        Role::Assistant => write_assistant(m, items),
        _ => write_other(m, role, items),
    }
}

fn write_opaque(m: &Message, items: &mut Vec<Value>) -> Result<(), Fault> {
    for part in &m.content {
        match part {
            Content::Native(n) if *n.format == *RESPONSES => items.push(n.value.as_ref().clone()),
            _ => return Err(untranslatable("an item in another shape")),
        }
    }
    Ok(())
}

/// A user's, system's or developer's message: its text and media are one
/// message item, each result an output item of its own.
fn write_other(m: &Message, role: &str, items: &mut Vec<Value>) -> Result<(), Fault> {
    let before = items.len();
    let mut extras = written(&m.native, "a message's")?;
    let mut parts: Vec<Value> = Vec::new();
    for c in &m.content {
        match c {
            Content::ToolResult {
                id,
                content,
                is_error,
                native,
                ..
            } => {
                flush_other(role, &mut parts, &mut extras, items);
                items.push(write_result(id, content, *is_error, native)?);
            }
            Content::Reasoning { .. } | Content::ToolCall { .. } => {
                return Err(untranslatable("an assistant's content in another role"));
            }
            part => parts.push(write_input_part(part)?),
        }
    }
    flush_other(role, &mut parts, &mut extras, items);
    if items.len() == before {
        items.push(json!({ "role": role, "content": "" }));
    }
    match extras.is_empty() {
        true => Ok(()),
        false => Err(untranslatable("a message's fields beside its results")),
    }
}

/// The message item of `parts`, if there are any.
fn flush_other(
    role: &str,
    parts: &mut Vec<Value>,
    extras: &mut Map<String, Value>,
    items: &mut Vec<Value>,
) {
    if parts.is_empty() {
        return;
    }
    let plain = match &parts[..] {
        [one] if only_keys(one, &["type", "text"]) => one["text"].as_str().map(str::to_owned),
        _ => None,
    };
    let mut item = Map::new();
    if !extras.is_empty() {
        item.insert("type".into(), "message".into());
    }
    item.insert("role".into(), role.into());
    let content = match plain.filter(|_| extras.is_empty()) {
        Some(text) => text.into(),
        None => std::mem::take(parts).into(),
    };
    item.insert("content".into(), content);
    item.extend(std::mem::take(extras));
    parts.clear();
    items.push(item.into());
}

fn only_keys(v: &Value, keys: &[&str]) -> bool {
    v.as_object()
        .is_some_and(|o| o.len() == keys.len() && keys.iter().all(|k| o.contains_key(*k)))
}

/// A part of an input message or of a result's output.
fn write_input_part(c: &Content) -> Result<Value, Fault> {
    match c {
        Content::Text { text, native, .. } => {
            let what = |k: &str| format!("a text part's '{k}'");
            let mut part = Map::new();
            part.insert("type".into(), "input_text".into());
            part.insert("text".into(), text.as_str().into());
            encode_native(RESPONSES, native, &mut part, what, Keep::CACHE_MARKER)?;
            Ok(part.into())
        }
        Content::Image(media) | Content::Document(media) => write_media(c, media),
        Content::Native(n) if *n.format == *RESPONSES => Ok(n.value.as_ref().clone()),
        Content::Native(n) => Err(untranslatable(&format!("a part in {}'s shape", n.format))),
        Content::Audio(_) => Err(untranslatable("audio in Responses")),
        _ => Err(untranslatable("an unsupported kind of part")),
    }
}

fn write_media(c: &Content, media: &Media) -> Result<Value, Fault> {
    let image = matches!(c, Content::Image(_));
    if image && media.name.is_some() {
        return Err(untranslatable("a display name on image content"));
    }
    let what = |k: &str| format!("media field '{k}'");
    let native = kept_fields(RESPONSES, &media.native, what, Keep::CACHE_MARKER)?;
    let url = || match &media.source {
        MediaSource::Url(url) => url.clone(),
        MediaSource::Bytes(data) => {
            format!("data:{};base64,{}", media.mime_type, STANDARD.encode(data))
        }
    };
    let mut v = match (image, &media.source) {
        (true, _) => json!({ "type": "input_image", "image_url": url() }),
        (false, MediaSource::Url(url)) => json!({ "type": "input_file", "file_url": url }),
        (false, MediaSource::Bytes(_)) => json!({ "type": "input_file", "file_data": url() }),
    };
    if let Some(name) = &media.name {
        v["filename"] = name.as_str().into();
    }
    merge_missing(&mut v, native);
    if image && v.get("detail").is_none() {
        v["detail"] = "auto".into();
    }
    Ok(v)
}

fn write_result(
    id: &str,
    content: &[Content],
    is_error: bool,
    native: &Option<NativeFields>,
) -> Result<Value, Fault> {
    // A function call output cannot say that the call failed.
    if is_error {
        return Err(untranslatable("a tool result that is an error"));
    }
    let plain = |c: &Content| matches!(c, Content::Text { native: None, .. });
    let output: Value = match content.iter().all(plain) {
        true => content
            .iter()
            .map(|c| c.as_text())
            .collect::<String>()
            .into(),
        false => content
            .iter()
            .map(write_input_part)
            .collect::<Result<Vec<_>, _>>()?
            .into(),
    };
    let mut v = Map::new();
    v.insert("type".into(), "function_call_output".into());
    v.insert("call_id".into(), id.into());
    v.insert("output".into(), output);
    v.extend(written(native, "a tool part's")?);
    Ok(v.into())
}

/// The text of a plain text part; others have none.
trait AsText {
    fn as_text(&self) -> &str;
}

impl AsText for Content {
    fn as_text(&self) -> &str {
        match self {
            Content::Text { text, .. } => text,
            _ => "",
        }
    }
}

/// An assistant message: its text runs are message items, its calls and
/// reasoning items of their own.
fn write_assistant(m: &Message, items: &mut Vec<Value>) -> Result<(), Fault> {
    if !m.native.is_empty() {
        return Err(untranslatable("an assistant message's fields"));
    }
    let before = items.len();
    let mut run: Vec<&Content> = Vec::new();
    for c in &m.content {
        match c {
            Content::Text { native, .. } => {
                // A text that carries its item's fields starts that item.
                if !run.is_empty() && native.fields().contains_key(INERT) {
                    flush_assistant(&mut run, items)?;
                }
                run.push(c);
            }
            Content::Native(n) if *n.format == *RESPONSES => run.push(c),
            Content::ToolCall {
                id,
                name,
                arguments,
                native,
                ..
            } => {
                flush_assistant(&mut run, items)?;
                let arguments = match arguments.trim().is_empty() {
                    true => "{}",
                    false => arguments.as_str(),
                };
                let mut v = Map::new();
                v.insert("type".into(), "function_call".into());
                v.insert("call_id".into(), id.as_str().into());
                v.insert("name".into(), name.as_str().into());
                v.insert("arguments".into(), arguments.into());
                v.extend(written(native, "a tool part's")?);
                items.push(v.into());
            }
            Content::Reasoning { text, native } => {
                flush_assistant(&mut run, items)?;
                let summary = match text.is_empty() {
                    true => json!([]),
                    false => json!([{ "type": "summary_text", "text": text }]),
                };
                let mut v = Map::new();
                v.insert("type".into(), "reasoning".into());
                v.insert("summary".into(), summary);
                // Reasoning is only valid with the id of the item it was.
                if native.format() != Some(RESPONSES) {
                    return Err(untranslatable("reasoning from another format"));
                }
                v.extend(written(native, "reasoning field")?);
                items.push(v.into());
            }
            _ => return Err(untranslatable("an unsupported kind of part")),
        }
    }
    flush_assistant(&mut run, items)?;
    if items.len() == before {
        items.push(json!({ "role": "assistant", "content": "" }));
    }
    Ok(())
}

/// The message item of a run of an assistant's text: by its text alone if
/// it keeps nothing of an item, else in full.
fn flush_assistant(run: &mut Vec<&Content>, items: &mut Vec<Value>) -> Result<(), Fault> {
    let parts = std::mem::take(run);
    if parts.is_empty() {
        return Ok(());
    }
    let mut item = Map::new();
    let mut plain = true;
    let mut texts = String::new();
    let mut written_parts = Vec::new();
    for (i, c) in parts.iter().enumerate() {
        match c {
            Content::Text { text, native, .. } => {
                let what = |k: &str| format!("a text part's '{k}'");
                let mut fields = kept_fields(RESPONSES, native, what, Keep::CACHE_MARKER)?;
                if i == 0 {
                    if let Some(Value::Object(inert)) = fields.shift_remove(INERT) {
                        item.extend(inert);
                    }
                    if let Some(Value::Object(more)) = fields.shift_remove(MESSAGE) {
                        item.extend(more);
                    }
                }
                plain &= fields.is_empty();
                texts.push_str(text);
                let mut part = Map::new();
                part.insert("type".into(), "output_text".into());
                part.insert("text".into(), text.as_str().into());
                part.extend(fields);
                part.entry("annotations").or_insert_with(|| json!([]));
                written_parts.push(Value::Object(part));
            }
            Content::Native(n) => {
                plain = false;
                written_parts.push(n.value.as_ref().clone());
            }
            _ => {}
        }
    }
    if plain && item.is_empty() {
        items.push(json!({ "role": "assistant", "content": texts }));
        return Ok(());
    }
    let mut v = Map::new();
    v.insert("type".into(), "message".into());
    v.insert("role".into(), "assistant".into());
    v.insert("content".into(), written_parts.into());
    v.entry("status").or_insert_with(|| "completed".into());
    v.extend(item);
    items.push(v.into());
    Ok(())
}

// -------------------------------------------------------------------- tools

/// Function tools, the choice and whether calls may be parallel, as the
/// kind's.
pub(super) fn read_tools(v: &Value, chat: &mut Input) {
    let tools = each_tool(&v["tools"], function_tool);
    let choice = match &v["tool_choice"] {
        Value::Null => Some(None),
        choice => read_choice(choice).map(Some),
    };
    let parallel = match &v["parallel_tool_calls"] {
        Value::Null => Some(None),
        parallel => parallel.as_bool().map(Some),
    };
    let keys = ["tools", "tool_choice", "parallel_tool_calls"];
    decode_tools(chat, tools, choice, parallel, keys);
}

fn function_tool(tool: &Value) -> Option<Tool> {
    if tool["type"] != "function" {
        return None;
    }
    let description = match &tool["description"] {
        Value::Null => None,
        text => Some(text.as_str()?.to_owned()),
    };
    let mut kind = Tool::new(tool["name"].as_str()?, tool["parameters"].clone());
    kind.description = description;
    let mut fields = rest_of(tool, &["type", "name", "description", "parameters"]);
    // Strict is on unless it is said; no other format has that default.
    if !tool.as_object()?.contains_key("strict") {
        fields.insert(INERT.into(), json!({ "strict": "unsaid" }));
    }
    kind.native = native_fields(RESPONSES, fields);
    Some(kind)
}

fn read_choice(choice: &Value) -> Option<ToolChoice> {
    Some(match choice {
        Value::String(choice) => match choice.as_str() {
            "auto" => ToolChoice::Auto,
            "none" => ToolChoice::None,
            "required" => ToolChoice::Required,
            _ => return None,
        },
        choice if choice["type"] == "function" && only(choice, &["type", "name"]) => {
            ToolChoice::Tool(choice["name"].as_str()?.into())
        }
        _ => return None,
    })
}

/// The kind's tools, choice and parallelism as the request's fields.
pub(super) fn write_tools(chat: &Input, v: &mut Map<String, Value>) -> Result<(), Fault> {
    let tool = |t: &Tool| -> Result<Value, Fault> {
        let mut tool = Map::new();
        tool.insert("type".into(), "function".into());
        tool.insert("name".into(), t.name.as_str().into());
        if let Some(text) = &t.description {
            tool.insert("description".into(), text.as_str().into());
        }
        tool.insert("parameters".into(), t.parameters.clone());
        let mut fields = kept_fields(RESPONSES, &t.native, tool_field, Keep::CACHE_MARKER)?;
        let unsaid = fields
            .shift_remove(INERT)
            .is_some_and(|inert| inert["strict"] == "unsaid");
        tool.extend(fields);
        // Another format's tools are not strict, which is not the default.
        if !unsaid && !tool.contains_key("strict") {
            tool.insert("strict".into(), false.into());
        }
        Ok(Value::Object(tool))
    };
    let tools: Vec<Value> = chat.tools.iter().map(tool).collect::<Result<_, Fault>>()?;
    set(v, "tools", (!tools.is_empty()).then(|| tools.into()));
    let choice = match &chat.options.tool_choice {
        None => None,
        Some(ToolChoice::Auto) => Some("auto".into()),
        Some(ToolChoice::None) => Some("none".into()),
        Some(ToolChoice::Required) => Some("required".into()),
        Some(ToolChoice::Tool(name)) => Some(json!({ "type": "function", "name": name })),
        Some(_) => return Err(untranslatable("a tool choice this format has no form for")),
    };
    set(v, "tool_choice", choice);
    set(
        v,
        "parallel_tool_calls",
        chat.options.parallel_tool_calls.map(Into::into),
    );
    Ok(())
}

// -------------------------------------------------------------- text format

/// The structured output requirement of `text.format`, if the kind can say
/// all of it: then it leaves the request's `native`.
pub(super) fn read_format(v: &Value, input: &mut Input) {
    let raw = &v["text"]["format"];
    let alone = raw.as_object().is_some_and(|m| m.len() == 1);
    let parsed = match raw["type"].as_str() {
        Some("text") if alone => Some(ResponseFormat::Text),
        Some("json_object") if alone => Some(ResponseFormat::JsonObject),
        Some("json_schema") => {
            let known = ["type", "name", "description", "schema", "strict"];
            let clean = raw
                .as_object()
                .is_some_and(|m| m.keys().all(|k| known.contains(&k.as_str())));
            let typed = raw["name"].is_string()
                && raw.get("description").is_none_or(Value::is_string)
                && raw.get("strict").is_none_or(Value::is_boolean)
                && raw["schema"].is_object();
            (clean && typed).then(|| ResponseFormat::JsonSchema {
                name: raw["name"].as_str().unwrap_or("response").into(),
                description: raw["description"].as_str().map(str::to_owned),
                schema: raw["schema"].clone(),
                strict: raw["strict"].as_bool(),
            })
        }
        _ => None,
    };
    let Some(parsed) = parsed else {
        return;
    };
    input.options.response_format = Some(parsed);
    if let Some(native) = &mut input.native
        && let Some(Value::Object(text)) = native.fields.get_mut("text")
    {
        text.shift_remove("format");
        if text.is_empty() {
            native.fields.shift_remove("text");
        }
    }
    if input.native.is_empty() {
        input.native = None;
    }
}

/// The request's `text`: its own fields, with the format the kind's
/// structured output requirement says.
pub(super) fn write_format(input: &Input, out: &mut Map<String, Value>) {
    let own = input.native.format() == Some(RESPONSES);
    let mut text = own
        .then(|| input.native.fields().get("text").cloned())
        .flatten()
        .unwrap_or_else(|| json!({}));
    let format = match &input.options.response_format {
        None => None,
        Some(ResponseFormat::Text) => Some(json!({ "type": "text" })),
        Some(ResponseFormat::JsonObject) => Some(json!({ "type": "json_object" })),
        Some(ResponseFormat::JsonSchema {
            name,
            description,
            schema,
            strict,
        }) => {
            let mut spec = json!({ "type": "json_schema", "name": name, "schema": schema });
            if let Some(description) = description {
                spec["description"] = description.as_str().into();
            }
            if let Some(strict) = strict {
                spec["strict"] = (*strict).into();
            }
            Some(spec)
        }
    };
    if let (Some(format), Some(text)) = (format, text.as_object_mut()) {
        text.insert("format".into(), format);
    }
    let empty = text.as_object().is_none_or(Map::is_empty);
    set(out, "text", (!empty).then_some(text));
}

/// A part the kind has a form for beyond text: an image or a file.
pub(super) fn decode_part(p: &Value) -> Option<Content> {
    read_media(p)
}

/// A part of the kind's beyond text, as Responses writes it.
pub(super) fn encode_part(c: &Content) -> Result<Value, Fault> {
    match c {
        Content::Image(m) | Content::Document(m) => write_media(c, m),
        Content::Audio(_) => Err(untranslatable("audio in Responses")),
        Content::Reasoning { .. } => Err(untranslatable("reasoning outside an assistant")),
        _ => Err(untranslatable("an unsupported kind of part")),
    }
}

/// The kind's reasoning of `reasoning`: its effort, and a summary of `auto`
/// (the summaries `concise` and `detailed` have no form in the kind). What
/// else it holds stays in the request's `native`.
pub(super) fn read_reasoning(input: &mut Input) {
    let Some(native) = input.native.as_mut() else {
        return;
    };
    let Some(Value::Object(config)) = native.fields.get_mut("reasoning") else {
        return;
    };
    let mut r = Reasoning::default();
    if let Some(effort) = config
        .get("effort")
        .and_then(Value::as_str)
        .and_then(parse_effort)
    {
        config.shift_remove("effort");
        r.effort = Some(effort);
    }
    if config.get("summary").and_then(Value::as_str) == Some("auto") {
        config.shift_remove("summary");
        r.summary = Some(true);
    }
    if config.is_empty() {
        native.fields.shift_remove("reasoning");
    }
    if native.is_empty() {
        input.native = None;
    }
    if r != Reasoning::default() {
        input.options.reasoning = Some(r);
    }
}

/// The request's `reasoning`: its own fields, with the kind's effort and
/// summary. Responses has no token budget.
pub(super) fn write_reasoning(input: &Input, out: &mut Map<String, Value>) -> Result<(), Fault> {
    let mut config = own_native(input, RESPONSES, "reasoning").unwrap_or_else(|| json!({}));
    if let Some(r) = reasoning_of(input)? {
        if r.budget_tokens.is_some() {
            return Err(untranslatable("a reasoning token budget in Responses"));
        }
        if let Some(config) = config.as_object_mut() {
            if let Some(effort) = r.effort {
                config.insert("effort".into(), effort_name(effort).into());
            }
            match r.summary {
                Some(true) => _ = config.insert("summary".into(), "auto".into()),
                Some(false) => _ = config.shift_remove("summary"),
                None => {}
            }
        }
    }
    let empty = config.as_object().is_none_or(Map::is_empty);
    set(out, "reasoning", (!empty).then_some(config));
    Ok(())
}
