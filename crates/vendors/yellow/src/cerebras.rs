//! Cerebras: Chat Completions.

use std::borrow::Cow;

use respan_gateway_conversation::{Input, Output};
use respan_gateway_conversation_http::Schema;
use respan_gateway_conversation_http::dialect::{Call, Dialect, Dialected};
use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_core::{Filter, NativeValue, Result};
use serde_json::Value;

use respan_gateway_vendor_kit::{Vendor, chat};

/// Where its API is.
const BASE_URL: &str = "https://api.cerebras.ai/v1";

/// Cerebras.
pub const VENDOR: Vendor = Vendor {
    id: "cerebras",
    key_env: &["CEREBRAS_API_KEY"],
    formats: &[(&Dialected::new(OpenAiChat, Cerebras), Some(BASE_URL))],
    ..Vendor::DEFAULT
};

/// Cerebras's Chat Completions: reasoning is `reasoning`, where most
/// vendors' is `reasoning_content`, in a response and in an assistant
/// message sent back.
#[derive(Debug, Clone, Copy)]
pub struct Cerebras;

impl Dialect for Cerebras {
    fn input<'a>(&self, input: &'a Input, _: &Call) -> Result<Cow<'a, Input>> {
        let mut input = match chat::reasoning_written(input) {
            Cow::Borrowed(input) => return Ok(Cow::Borrowed(input)),
            Cow::Owned(input) => input,
        };
        rename_reasoning(&mut input);
        Ok(Cow::Owned(input))
    }

    fn output(&self, call: &Call) -> Option<Box<dyn Filter<Output>>> {
        let read = chat::read(call, &[], true)?;
        Some(Box::new(ReasoningAsContent { read }))
    }
}

/// Renames the `reasoning_content` [`chat::reasoning_written`] wrote on a
/// message to `reasoning`, as Cerebras takes it back.
fn rename_reasoning(input: &mut Input) {
    for message in &mut input.messages {
        let fields = message.native.as_mut().map(|n| &mut n.fields);
        if let Some(fields) = fields
            && let Some(text) = fields.shift_remove("reasoning_content")
        {
            fields.insert("reasoning".into(), text);
        }
    }
}

/// Renames a delta's `reasoning` to `reasoning_content`, then passes it to
/// `read` ([`chat::read`]), which reads that as reasoning.
struct ReasoningAsContent {
    read: Box<dyn Filter<Output>>,
}

impl Filter<Output> for ReasoningAsContent {
    fn edit(&mut self, output: Output, out: &mut Vec<Output>) -> Result<()> {
        let output = match output {
            Output::Native(NativeValue { format, value }) if &*format == OpenAiChat::NAME => {
                let mut value = std::sync::Arc::unwrap_or_clone(value);
                let choices = value.get_mut("choices").and_then(Value::as_array_mut);
                let deltas = choices
                    .into_iter()
                    .flatten()
                    .filter_map(|c| c.get_mut("delta").and_then(Value::as_object_mut));
                for delta in deltas {
                    if let Some(text) = delta.shift_remove("reasoning") {
                        delta.insert("reasoning_content".into(), text);
                    }
                }
                Output::native(format, value)
            }
            output => output,
        };
        self.read.edit(output, out)
    }

    fn end(&mut self, end: &Result<()>, out: &mut Vec<Output>) -> Result<()> {
        self.read.end(end, out)
    }
}
