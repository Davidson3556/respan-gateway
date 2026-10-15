//! What vendors of Chat Completions share: reasoning as `reasoning_content`,
//! and fields of a choice that only describe it. Both are native to
//! `openai-chat`, which keeps them where they appeared; a vendor's
//! [`Dialect`](respan_gateway_conversation_http::dialect::Dialect) reads
//! them with these.

use std::borrow::Cow;
use std::sync::Arc;

use respan_gateway_conversation::{Block, Content, Delta, Input, Message, Output, Role};
use respan_gateway_conversation_http::Schema;
use respan_gateway_conversation_http::dialect::Call;
use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_core::{Filter, NativeFields, Result};
use serde_json::{Map, Value};

/// `input` with an assistant message's reasoning written as its
/// `reasoning_content`: `input` itself if it has none.
pub fn reasoning_written(input: &Input) -> Cow<'_, Input> {
    if !input.messages.iter().any(has_reasoning) {
        return Cow::Borrowed(input);
    }
    let mut input = input.clone();
    input.messages.iter_mut().for_each(write_reasoning);
    Cow::Owned(input)
}

/// A response's `reasoning_content` read as reasoning blocks, for a client
/// of another format; a client of Chat Completions gets the field as it
/// came.
pub fn reasoning_read(call: &Call) -> Option<Box<dyn Filter<Output>>> {
    call.translated
        .then(|| Box::new(Reasoning::default()) as Box<dyn Filter<Output>>)
}

/// Fields of a choice that only describe it, such as its provider's own
/// finish reason, dropped for a client of another format, which would
/// refuse them; a client of Chat Completions gets them as they came.
pub fn inert_choice(
    call: &Call,
    fields: &'static [&'static str],
) -> Option<Box<dyn Filter<Output>>> {
    call.translated
        .then(|| Box::new(Inert(fields)) as Box<dyn Filter<Output>>)
}

/// `first`, then `then`, each if there.
pub fn both(
    first: Option<Box<dyn Filter<Output>>>,
    then: Option<Box<dyn Filter<Output>>>,
) -> Option<Box<dyn Filter<Output>>> {
    match (first, then) {
        (Some(first), Some(then)) => Some(Box::new(Both(first, then))),
        (first, then) => first.or(then),
    }
}

/// Fields of a choice's message that only describe it, such as a
/// signature its provider alone reads back, dropped for a client of another
/// format, which would refuse them; a client of Chat Completions gets them
/// as they came.
pub fn inert_message(
    call: &Call,
    fields: &'static [&'static str],
) -> Option<Box<dyn Filter<Output>>> {
    call.translated
        .then(|| Box::new(InertMessage(fields)) as Box<dyn Filter<Output>>)
}

/// One filter's events through another.
struct Both(Box<dyn Filter<Output>>, Box<dyn Filter<Output>>);

impl Filter<Output> for Both {
    fn edit(&mut self, output: Output, out: &mut Vec<Output>) -> Result<()> {
        let mut first = Vec::new();
        self.0.edit(output, &mut first)?;
        first.into_iter().try_for_each(|o| self.1.edit(o, out))
    }

    fn end(&mut self, end: &Result<()>, out: &mut Vec<Output>) -> Result<()> {
        // The second ends as the first did, if the first failed it.
        let mut first = Vec::new();
        let failed = self
            .0
            .end(end, &mut first)
            .and_then(|()| first.into_iter().try_for_each(|o| self.1.edit(o, out)))
            .err();
        let end = failed.clone().map_or_else(|| end.clone(), Err);
        self.1.end(&end, out)?;
        failed.map_or(Ok(()), Err)
    }
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

/// Drops these fields of the first choice's message.
struct InertMessage(&'static [&'static str]);

impl Filter<Output> for InertMessage {
    fn edit(&mut self, output: Output, out: &mut Vec<Output>) -> Result<()> {
        out.extend(first_choice(output, |choice| {
            if let Some(delta) = choice.get_mut("delta").and_then(Value::as_object_mut) {
                self.0.iter().for_each(|f| _ = delta.shift_remove(*f));
            }
        }));
        Ok(())
    }
}

/// Reasoning but signed reasoning, which the format refuses.
fn plain(p: &Content) -> bool {
    matches!(p, Content::Reasoning { native, .. } if native.as_ref().is_none_or(NativeFields::is_empty))
}

/// Whether `message` is an assistant's with reasoning to write as its
/// `reasoning_content`.
fn has_reasoning(message: &Message) -> bool {
    let own = message
        .native
        .as_ref()
        .is_none_or(|n| n.is_empty() || &*n.format == OpenAiChat::NAME);
    matches!(message.role, Role::Assistant) && own && message.content.iter().any(plain)
}

/// An assistant message's reasoning, but signed reasoning, which the format
/// refuses, as its native `reasoning_content`.
fn write_reasoning(message: &mut Message) {
    if !has_reasoning(message) {
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
