//! What the model said, replayed to it, and the numbering of its blocks.

use std::collections::HashMap;

use respan_gateway_conversation::{Assembled, AssembledItem, Block, Content, Output};
use respan_gateway_core::{NativeFields, NativeValue};
use serde_json::{Map, Value};

/// The block an event names, by `index` (Anthropic's) or `block` (the
/// others' natives).
pub(super) fn block_of(raw: &Value) -> Option<u32> {
    let named = raw["index"].as_u64().or_else(|| raw["block"].as_u64());
    u32::try_from(named?).ok()
}

/// `native`, which names `block`, naming `number`.
pub(super) fn renumbered(native: &NativeValue, block: u32, number: u32) -> Output {
    let mut value = (*native.value).clone();
    if let Some(object) = value.as_object_mut() {
        let key = if object.contains_key("index") && object["index"] == block {
            "index"
        } else {
            "block"
        };
        object.insert(key.into(), number.into());
    }
    Output::Native(NativeValue::new(native.format.clone(), value))
}

/// What the model said, as the assistant message holds it: its text,
/// reasoning and tool calls in order, each with the fields its format kept
/// beside it.
pub(super) fn replay(said: &Assembled) -> Vec<Content> {
    let mut kept: HashMap<u32, (NativeValue, Map<String, Value>)> = HashMap::new();
    for item in said.items() {
        let AssembledItem::Native(native) = item else {
            continue;
        };
        let Some(block) = block_of(&native.value) else {
            continue;
        };
        let raw = &native.value;
        let entry = kept
            .entry(block)
            .or_insert_with(|| (native.clone(), Map::new()));
        if let Some(fields) = raw["fields"].as_object() {
            let fields = fields.iter().map(|(k, v)| (k.clone(), v.clone()));
            entry.1.extend(fields);
        }
        if let Some(signature) = raw["delta"]["signature"].as_str() {
            entry.1.insert("signature".into(), signature.into());
        }
    }
    let native_of = |block: u32| {
        let (from, fields) = kept.get(&block)?;
        Some(NativeFields::new(from.format.clone(), fields.clone())).filter(|n| !n.is_empty())
    };
    let mut parts = Vec::new();
    for item in said.items() {
        match item {
            AssembledItem::Block(b) => {
                let native = native_of(b.block);
                match &b.kind {
                    Block::Text if !b.text.is_empty() => {
                        let mut part = Content::from(b.text.clone());
                        if let Content::Text { native: n, .. } = &mut part {
                            *n = native;
                        }
                        parts.push(part);
                    }
                    Block::Reasoning if !b.text.is_empty() || native.is_some() => {
                        parts.push(Content::Reasoning {
                            text: b.text.clone(),
                            native,
                        });
                    }
                    Block::ToolCall { id, name } => {
                        let arguments = Some(b.text.as_str()).filter(|t| !t.trim().is_empty());
                        let mut part = Content::tool_call(id, name, arguments.unwrap_or("{}"));
                        if let Content::ToolCall { native: n, .. } = &mut part {
                            *n = native;
                        }
                        parts.push(part);
                    }
                    _ => {}
                }
            }
            // Thinking the provider redacted is only a native block; it must
            // be sent back as it came.
            AssembledItem::Native(native) => {
                let block = &native.value["content_block"];
                if native.value["type"] == "content_block_start"
                    && block["type"] == "redacted_thinking"
                {
                    parts.push(Content::native(native.format.clone(), block.clone()));
                }
            }
        }
    }
    parts
}
