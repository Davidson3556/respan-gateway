//! What a request says beyond text, in Anthropic's terms: tools and their
//! choice, tool blocks, media and reasoning, and structured output. What
//! the kind has no form for, such as a tool the provider runs, stays in the
//! `native` of the request, the tool or the part.

use crate::schema::{
    Keep, NativeFieldsExt, decode_content, decode_tools, each_tool, encode_content, encode_native,
    kept_fields, merge_missing, native_fields, only, remainder, rest_of, set, tool_field,
    untranslatable,
};
use crate::{Content, Input, Media, MediaSource, NativeFields, ResponseFormat, Tool, ToolChoice};
use base64::{Engine, engine::general_purpose::STANDARD};
use respan_gateway_core::{BadRequest, Fault};
use serde_json::{Map, Value, json};

use crate::anthropic::{ANTHROPIC, AnthropicMessages};

/// A tool call's arguments as JSON: an object, `{}` for none.
pub(crate) fn arguments_json(arguments: &str) -> Result<Value, Fault> {
    if arguments.trim().is_empty() {
        return Ok(json!({}));
    }
    let not_json = |_| BadRequest::fault("a tool call's arguments are not JSON");
    serde_json::from_str(arguments).map_err(not_json)
}

/// Anthropic's custom tools and choice, with whether calls may be
/// parallel, as the kind's.
pub(crate) fn read_tools_of(v: &Value, chat: &mut Input) {
    let tools = each_tool(&v["tools"], anthropic_tool);
    let choice = match &v["tool_choice"] {
        Value::Null => Some((None, None)),
        choice => anthropic_choice(choice),
    };
    let (choice, parallel) = choice.unzip();
    decode_tools(chat, tools, choice, parallel, ["tools", "tool_choice", ""]);
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
    let rest = rest_of(tool, &["type", "name", "description", "input_schema"]);
    kind.native = native_fields(ANTHROPIC, rest);
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
pub(crate) fn write_tools(chat: &Input, v: &mut Map<String, Value>) -> Result<(), Fault> {
    let tool = |t: &Tool| -> Result<Value, Fault> {
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
        encode_native(ANTHROPIC, &t.native, &mut tool, tool_field, Keep::default())?;
        Ok(Value::Object(tool))
    };
    let tools: Vec<Value> = chat.tools.iter().map(tool).collect::<Result<_, Fault>>()?;
    set(v, "tools", (!tools.is_empty()).then(|| tools.into()));
    let one_at_a_time = chat.options.parallel_tool_calls == Some(false);
    let mut choice = match &chat.options.tool_choice {
        Some(ToolChoice::Auto) => Some(json!({ "type": "auto" })),
        Some(ToolChoice::None) => Some(json!({ "type": "none" })),
        Some(ToolChoice::Required) => Some(json!({ "type": "any" })),
        Some(ToolChoice::Tool(name)) => Some(json!({ "type": "tool", "name": name })),
        None => one_at_a_time.then(|| json!({ "type": "auto" })),
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

/// A part the kind has a form for: an image or a document, reasoning, or a
/// tool block.
pub(crate) fn decode_part(p: &Value) -> Option<Content> {
    match p["type"].as_str()? {
        "image" | "document" => read_media(p),
        "thinking" => Some(Content::Reasoning {
            text: p["thinking"].as_str()?.into(),
            native: native_fields(ANTHROPIC, remainder(p, &["type", "thinking"])),
        }),
        _ => tool_part(p),
    }
}

/// A part of the kind's as Anthropic writes it.
pub(crate) fn encode_part(c: &Content) -> Result<Value, Fault> {
    match c {
        Content::Image(media) => write_media("image", media),
        Content::Document(media) => write_media("document", media),
        Content::Reasoning { text, native } => {
            let mut v = json!({"type":"thinking", "thinking":text});
            let what = |k: &str| format!("reasoning field '{k}'");
            merge_missing(
                &mut v,
                kept_fields(ANTHROPIC, native, what, Keep::default())?,
            );
            Ok(v)
        }
        Content::ToolCall { .. } | Content::ToolResult { .. } => tool_block(c),
        Content::Audio(_) => Err(untranslatable("this media kind")),
        _ => Err(untranslatable("an unsupported kind of part")),
    }
}

/// An image or document block, if the kind can say its source: its other
/// fields are its media's `native`.
fn read_media(v: &Value) -> Option<Content> {
    if v.get("title").is_some_and(|v| !v.is_string()) {
        return None;
    }
    let source = &v["source"];
    let (mut mime, src) = match source["type"].as_str()? {
        "base64" => (
            source["media_type"].as_str()?.to_owned(),
            MediaSource::Bytes(STANDARD.decode(source["data"].as_str()?).ok()?.into()),
        ),
        "url" => (
            "image/*".to_owned(),
            MediaSource::Url(source["url"].as_str()?.into()),
        ),
        _ => return None,
    };
    let image = v["type"] == "image";
    if !image && mime == "image/*" {
        mime = "application/pdf".into();
    }
    let mut fields = remainder(v, &["type", "source", "title"]);
    let known = if source["type"] == "base64" {
        &["type", "media_type", "data"][..]
    } else {
        &["type", "url"][..]
    };
    let nested = remainder(source, known);
    if !nested.is_empty() {
        fields.insert("source".into(), nested.into());
    }
    let mut media = Media::new(mime, src);
    media.name = v["title"].as_str().map(str::to_owned);
    media.native = native_fields(ANTHROPIC, fields);
    Some(match image {
        true => Content::Image(media),
        false => Content::Document(media),
    })
}

/// An image or document block of `media`, refusing what Anthropic cannot say.
fn write_media(kind: &str, media: &Media) -> Result<Value, Fault> {
    if media.name.is_some() && kind != "document" {
        return Err(untranslatable("a display name on image or audio content"));
    }
    let what = |k: &str| format!("media field '{k}'");
    let native = kept_fields(ANTHROPIC, &media.native, what, Keep::default())?;
    let source = match &media.source {
        MediaSource::Url(url) => json!({"type":"url","url":url}),
        MediaSource::Bytes(bytes) => {
            json!({"type":"base64","media_type":media.mime_type,"data":STANDARD.encode(bytes)})
        }
    };
    let mut v = json!({"type":kind,"source":source});
    if let Some(name) = &media.name {
        v["title"] = name.clone().into();
    }
    merge_missing(&mut v, native);
    Ok(v)
}

/// An Anthropic `tool_use` or `tool_result` block as the kind's part, if it
/// can say all of it. A field it has no place for, other than a cache marker,
/// on the block or on a text block of a result, keeps it the format's own:
/// translating it is refused, not dropped.
fn tool_part(block: &Value) -> Option<Content> {
    match block["type"].as_str()? {
        "tool_use" => {
            let input = block["input"].as_object()?;
            let arguments = Value::Object(input.clone()).to_string();
            let (id, name) = (block["id"].as_str()?, block["name"].as_str()?);
            let mut part = Content::tool_call(id, name, arguments);
            if let Content::ToolCall { native, .. } = &mut part {
                let rest = rest_of(block, &["type", "id", "name", "input"]);
                *native = native_fields(ANTHROPIC, rest);
            }
            Some(part)
        }
        "tool_result" => {
            let is_error = match &block["is_error"] {
                Value::Null => false,
                is_error => is_error.as_bool()?,
            };
            let blocks = block["content"].as_array().into_iter().flatten();
            let marked =
                |b: &Value| b["type"] == "text" && !only(b, &["type", "text", "cache_control"]);
            if blocks.into_iter().any(marked) {
                return None;
            }
            let content = decode_content::<AnthropicMessages>(&block["content"]);
            let id = block["tool_use_id"].as_str()?;
            let mut part = Content::tool_result(id, content, is_error);
            if let Content::ToolResult { native, .. } = &mut part {
                let rest = rest_of(block, &["type", "tool_use_id", "content", "is_error"]);
                *native = native_fields(ANTHROPIC, rest);
            }
            Some(part)
        }
        _ => None,
    }
}

/// Write the fields of a tool part's `native` into its Anthropic `block`:
/// another format's are refused.
fn block_fields(block: &mut Value, native: &Option<NativeFields>) -> Result<(), Fault> {
    let what = |key: &str| format!("a tool part's '{key}'");
    let fields = kept_fields(ANTHROPIC, native, what, Keep::default())?;
    if let Some(block) = block.as_object_mut() {
        block.extend(fields);
    }
    Ok(())
}

/// The kind's tool call or result as an Anthropic block.
fn tool_block(part: &Content) -> Result<Value, Fault> {
    match part {
        Content::ToolCall {
            id,
            name,
            arguments,
            native,
            ..
        } => {
            let input = arguments_json(arguments)?;
            let mut block = json!({ "type": "tool_use", "id": id, "name": name, "input": input });
            block_fields(&mut block, native)?;
            Ok(block)
        }
        Content::ToolResult {
            id,
            content,
            is_error,
            native,
            ..
        } => {
            let mut block = json!({ "type": "tool_result", "tool_use_id": id });
            block_fields(&mut block, native)?;
            if !content.is_empty() {
                block["content"] = encode_content::<AnthropicMessages>(content)?;
            }
            if *is_error {
                block["is_error"] = true.into();
            }
            Ok(block)
        }
        _ => Err(untranslatable("an unsupported kind of part")),
    }
}

/// The structured output requirement of `output_config.format`, if the
/// kind can say all of it: then it leaves the request's `native`.
pub(crate) fn read_output(v: &Value, input: &mut Input) {
    let spec = &v["output_config"]["format"];
    let clean = spec
        .as_object()
        .is_some_and(|m| m.keys().all(|k| ["type", "schema"].contains(&k.as_str())));
    if spec["type"] != "json_schema" || !clean || !spec["schema"].is_object() {
        return;
    }
    // Anthropic always constrains the result to the schema.
    input.options.response_format = Some(ResponseFormat::JsonSchema {
        name: "response".into(),
        description: None,
        schema: spec["schema"].clone(),
        strict: Some(true),
    });
    if let Some(native) = &mut input.native
        && let Some(Value::Object(config)) = native.fields.get_mut("output_config")
    {
        config.remove("format");
        if config.is_empty() {
            native.fields.remove("output_config");
        }
    }
    if input.native.is_empty() {
        input.native = None;
    }
}

/// The request's own `output_config`, with the format the kind's structured
/// output requirement says.
pub(crate) fn write_output(input: &Input, out: &mut Map<String, Value>) -> Result<(), Fault> {
    let own = input.native.format() == Some(ANTHROPIC);
    let mut config = own
        .then(|| input.native.fields().get("output_config").cloned())
        .flatten();
    let schema = match &input.options.response_format {
        None | Some(ResponseFormat::Text) => None,
        Some(ResponseFormat::JsonObject) => {
            return Err(untranslatable(
                "unconstrained JSON object output in Anthropic; supply a schema",
            ));
        }
        Some(ResponseFormat::JsonSchema {
            schema,
            description,
            strict,
            ..
        }) => {
            // OpenAI uses the wrapper description to instruct the model, and
            // Anthropic always constrains the result. Do not silently discard
            // those requests when there is no equivalent target field.
            if description.is_some() || *strict == Some(false) {
                return Err(untranslatable(
                    "a schema wrapper description or non-strict schema output in Anthropic",
                ));
            }
            Some(schema)
        }
    };
    if let Some(schema) = schema {
        let config = config.get_or_insert_with(|| json!({}));
        if !config.is_object() {
            return Err(untranslatable("non-object output_config"));
        }
        config["format"] = json!({"type":"json_schema","schema":schema});
    }
    set(out, "output_config", config);
    Ok(())
}
