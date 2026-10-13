//! What vendors of Chat Completions share: reasoning as `reasoning_content`,
//! and fields of a choice that only describe it. Both are native to
//! `openai-chat`, which keeps them where they appeared.

use std::sync::Arc;

use respan_gateway_conversation::{Block, Content, Delta, Input, Message, Output, Role};
use respan_gateway_conversation_http::Schema;
use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_core::{Filter, NativeFields, ProviderContext, Result};
use serde_json::{Map, Value};

/// Whether the call's client speaks another format than its provider, so
/// what is native to the provider's cannot reach it.
fn translated(px: &ProviderContext) -> bool {
    px.client().format() != px.format()
}

/// Reasoning is `reasoning_content`: an assistant message's reasoning is
/// written there, and, for a client of another format, a response's is read
/// from there as reasoning blocks. A client of Chat Completions gets the
/// field as it came.
pub(crate) fn reasoning_content(px: &mut ProviderContext) -> Result<()> {
    if !px.is_kind::<Input>() {
        return Ok(());
    }
    px.filter(|input: &mut Input| {
        input.messages.iter_mut().for_each(write_reasoning);
        Ok(())
    })?;
    match translated(px) {
        true => px.filter(Reasoning::default()),
        false => Ok(()),
    }
}

/// Fields of a choice that only describe it, such as its provider's own
/// finish reason, are dropped for a client of another format, which would
/// refuse them; a client of Chat Completions gets them as they came.
pub(crate) fn inert_choice(
    px: &mut ProviderContext,
    fields: &'static [&'static str],
) -> Result<()> {
    if !px.is_kind::<Input>() || !translated(px) {
        return Ok(());
    }
    px.filter(Inert(fields))
}

/// Drops these fields of the first choice.
struct Inert(&'static [&'static str]);

impl Filter<Output> for Inert {
    fn edit(&mut self, output: Output, out: &mut Vec<Output>) -> Result<()> {
        out.extend(first_choice(output, |choice| {
            self.0.iter().for_each(|f| _ = choice.shift_remove(*f));
        }));
        Ok(())
    }
}

/// An assistant message's reasoning, but signed reasoning, which the format
/// refuses, as its native `reasoning_content`.
fn write_reasoning(message: &mut Message) {
    let plain = |p: &Content| matches!(p, Content::Reasoning { native, .. } if native.as_ref().is_none_or(NativeFields::is_empty));
    let own = message
        .native
        .as_ref()
        .is_none_or(|n| n.is_empty() || &*n.format == OpenAiChat::NAME);
    if !matches!(message.role, Role::Assistant) || !own || !message.content.iter().any(plain) {
        return;
    }
    let text: String = message
        .content
        .iter()
        .filter(|p| plain(p))
        .filter_map(|p| match p {
            Content::Reasoning { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect();
    message.content.retain(|p| !plain(p));
    let native = message
        .native
        .get_or_insert_with(|| NativeFields::new(OpenAiChat::NAME, Map::new()));
    native.format = OpenAiChat::NAME.into();
    native
        .fields
        .insert("reasoning_content".into(), text.into());
}

/// Reads a response's `reasoning_content` as reasoning blocks of its own
/// numbers, counting down from the last, which the format never gives: one
/// opens with the first reasoning and ends before any other block's event,
/// or a finish.
#[derive(Default)]
struct Reasoning {
    open: Option<u32>,
    opened: u32,
}

impl Reasoning {
    fn close(&mut self, out: &mut Vec<Output>) {
        if let Some(block) = self.open.take() {
            out.push(Output::block_end(block));
        }
    }
}

impl Filter<Output> for Reasoning {
    fn edit(&mut self, output: Output, out: &mut Vec<Output>) -> Result<()> {
        match output {
            Output::Native(_) => {
                let mut said = None;
                out.extend(first_choice(output, |choice| {
                    let delta = choice.get_mut("delta").and_then(Value::as_object_mut);
                    let text = delta.and_then(|d| d.shift_remove("reasoning_content"));
                    said = text.and_then(|t| t.as_str().map(str::to_owned));
                }));
                let Some(text) = said.filter(|t| !t.is_empty()) else {
                    return Ok(());
                };
                let block = match self.open {
                    Some(block) => block,
                    None => {
                        let block = u32::MAX - self.opened;
                        self.opened += 1;
                        self.open = Some(block);
                        out.push(Output::block_start(block, Block::Reasoning));
                        block
                    }
                };
                out.push(Output::Delta {
                    block,
                    delta: Delta::Reasoning(text),
                });
            }
            Output::BlockStart { .. } | Output::BlockEnd { .. } | Output::Finish(_) => {
                self.close(out);
                out.push(output);
            }
            output => out.push(output),
        }
        Ok(())
    }

    fn end(&mut self, end: &Result<()>, out: &mut Vec<Output>) -> Result<()> {
        if end.is_ok() {
            self.close(out);
        }
        Ok(())
    }
}

/// `output` with `edit` made to its first choice, if it is a native of
/// Chat Completions that has one; `None` once nothing is left of it.
fn first_choice(output: Output, edit: impl FnOnce(&mut Map<String, Value>)) -> Option<Output> {
    let native = match output {
        Output::Native(native) if &*native.format == OpenAiChat::NAME => native,
        output => return Some(output),
    };
    if !native.value["choices"].is_array() {
        return Some(Output::Native(native));
    }
    let mut value = Arc::unwrap_or_clone(native.value);
    let choices = value["choices"].as_array_mut()?;
    let first = choices
        .iter_mut()
        .find(|c| c["index"].as_u64().unwrap_or(0) == 0);
    if let Some(choice) = first.and_then(Value::as_object_mut) {
        edit(choice);
        if choice
            .get("delta")
            .and_then(Value::as_object)
            .is_some_and(Map::is_empty)
        {
            choice.shift_remove("delta");
        }
    }
    choices.retain(|c| c.as_object().is_none_or(|c| c.keys().any(|k| k != "index")));
    if choices.is_empty() && value.as_object().is_some_and(|o| o.len() == 1) {
        return None;
    }
    Some(Output::native(native.format, value))
}
