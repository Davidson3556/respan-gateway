//! Anthropic Messages: `POST /v1/messages`, JSON or SSE, of the
//! conversation kind, as a [`Schema`].
//!
//! # Responses as events
//!
//! A stream and a whole body decode into the same events, by these rules:
//!
//! - [`Output::Start`] comes from `message_start`, or the body's `id` and
//!   `model`; its `native` holds the message's other fields.
//! - Blocks are numbered by their `index`. A `text`, `tool_use` or
//!   `thinking` block opens a [`Block::Text`], [`Block::ToolCall`] or
//!   [`Block::Reasoning`]; `text_delta`, `input_json_delta` and
//!   `thinking_delta` are its deltas, and `content_block_stop` its end. A
//!   whole body's blocks are the same events, one delta each.
//! - `message_delta`'s `stop_reason` is the [`Output::Finish`]; `usage` of
//!   `message_start` and `message_delta` are [`Output::Usage`] increments.
//! - What the kind has no form for is a [`Output::Native`] of this format, as
//!   the stream event that carries it: `ping`, a `signature_delta`, a block
//!   of another type (and its deltas and stop), citations, a `stop_sequence`
//!   (as a `message_delta`), an event of an unknown type. In a whole body a
//!   block of another type is a `content_block_start` of the whole block.
//!   `message_stop` ends the stream.
//!
//! Encoders do the reverse. A stream's blocks are written one at a time, as
//! Anthropic does: a block that opens while another is open waits, with its
//! deltas, until that one ends. A whole body is written from the events
//! collected, empty text blocks left out, as Anthropic refuses them.

mod request;
#[cfg(test)]
mod tests;

use crate::is_inert;
use crate::schema::{
    Asked, Decoder, Gate, Names, NativeFieldsExt, Schema, Totals, count, decode_content,
    decode_message, encode_content, encode_message, ended, error_event, fits, merge_details,
    merge_fields, native_fields, says, says_error, set, stop_json, unreadable_event,
    unsupported_fields, unwritable_content, unwritable_usage, usage_extra,
};
use respan_gateway_conversation::{
    Assembled, AssembledItem, Block, Content, Delta, Finish, Input, Message, Output, Role, Start,
    Usage,
};
use respan_gateway_core::{BadRequest, Connection, Fault, Format, NativeValue};
use respan_gateway_http::bytes::{Bytes, BytesMut};
use respan_gateway_http::framing::sse::{event, event_json};
use respan_gateway_http::{ClientIo, Encoder, ProviderIo, http};
use serde_json::{Map, Value, json};
use std::sync::Arc;

use request::{
    arguments_json, read_output, read_thinking, read_tools_of, write_output, write_thinking,
    write_tools,
};

/// The `max_tokens` of a translated request without one, as it is required.
const DEFAULT_MAX_TOKENS: u64 = 4096;

const FINISH: Names<Finish> = Names(
    &[
        ("end_turn", Finish::Stop),
        ("stop_sequence", Finish::Stop),
        ("max_tokens", Finish::Length),
        ("tool_use", Finish::ToolUse),
        ("refusal", Finish::Filtered),
    ],
    ANTHROPIC,
);

/// A reason as a [`Finish`]: a full context window is the token limit to
/// another format.
fn finish_of(reason: &str) -> Finish {
    match reason {
        "model_context_window_exceeded" => Finish::native_or(ANTHROPIC, reason, Finish::Length),
        _ => FINISH.finish(reason),
    }
}

/// This format's name.
const ANTHROPIC: &str = "anthropic-messages";

/// Anthropic Messages, named `anthropic-messages`: a `POST` to any path
/// ending in `/v1/messages`. The provider's base URL leaves out the
/// version (`https://api.anthropic.com`); `anthropic-version: 2023-06-01`
/// is sent if none is set.
#[derive(Debug, Clone, Copy, Default)]
pub struct AnthropicMessages;

impl AnthropicMessages {
    /// This format's name, `anthropic-messages`.
    pub fn name(&self) -> &'static str {
        ANTHROPIC
    }
}

impl Format<ClientIo, Input, Output> for AnthropicMessages {
    fn name(&self) -> &str {
        ANTHROPIC
    }
    fn matches(&self, io: &ClientIo) -> bool {
        self.accepts(io.request())
    }
    fn bind(&self, io: ClientIo) -> Result<Connection<Input, Output>, Fault> {
        self.bind_client(io)
    }
}

impl Format<ProviderIo, Output, Input> for AnthropicMessages {
    fn name(&self) -> &str {
        ANTHROPIC
    }
    fn bind(&self, io: ProviderIo) -> Result<Connection<Output, Input>, Fault> {
        self.bind_provider(io)
    }
}

/// Keys a conversation has a place for, or that change no output.
const KNOWN: &[&str] = &[
    "model",
    "messages",
    "system",
    "max_tokens",
    "temperature",
    "top_p",
    "top_k",
    "stop_sequences",
    "stream",
    "metadata",
];

/// Roles a message can have; system prompts have their own field.
const ROLES: Names<Role> = Names(
    &[("user", Role::User), ("assistant", Role::Assistant)],
    ANTHROPIC,
);

/// Whether a message is instructions: Anthropic has one system prompt for a
/// system's and a developer's alike.
fn is_system(m: &Message) -> bool {
    matches!(m.role, Role::System | Role::Developer)
}

/// The parts of the leading system messages, as the system prompt, and
/// the rest; a later system message is refused.
fn split_system(chat: &Input) -> Result<(Vec<Content>, &[Message]), Fault> {
    let lead = chat.messages.iter().take_while(|m| is_system(m)).count();
    let (system, rest) = chat.messages.split_at(lead);
    if rest.iter().any(is_system) {
        return Err(BadRequest::fault(
            "Anthropic has no place for a system message after the conversation starts",
        ));
    }
    if system.iter().any(|m| !m.native.is_empty()) {
        return Err(BadRequest::fault(
            "Anthropic's system prompt has no place for a message's other fields",
        ));
    }
    let parts = system.iter().flat_map(|m| m.content.iter().cloned());
    Ok((parts.collect(), rest))
}

/// `m` without its empty text parts, which Anthropic refuses, if it says
/// something else, such as a tool call.
fn without_empty_text(m: &Message) -> std::borrow::Cow<'_, Message> {
    let empty = |c: &Content| matches!(c, Content::Text { text, .. } if text.is_empty());
    if !m.content.iter().any(empty) || m.content.iter().all(empty) {
        return std::borrow::Cow::Borrowed(m);
    }
    let mut m = m.clone();
    m.content.retain(|c| !empty(c));
    std::borrow::Cow::Owned(m)
}

/// Whether a message says nothing: no content, or only empty text.
fn is_empty(m: &Message) -> bool {
    let empty = |c: &Content| matches!(c, Content::Text { text, .. } if text.is_empty());
    m.native.is_empty() && m.content.iter().all(empty)
}

impl Schema for AnthropicMessages {
    const NAME: &'static str = ANTHROPIC;
    const PATH: &'static str = "/v1/messages";
    /// The API version sent when the client names none.
    const HEADERS: &'static [(&'static str, &'static str)] = &[("anthropic-version", "2023-06-01")];
    const OWN: &'static [&'static str] = &["metadata", "output_config", "thinking"];
    type Decoding = AnthropicDecoder;

    fn decoder(&self) -> AnthropicDecoder {
        AnthropicDecoder::default()
    }

    fn decode_part(p: &Value) -> Option<Content> {
        request::decode_part(p)
    }

    fn encode_part(c: &Content) -> Result<Value, Fault> {
        request::encode_part(c)
    }

    fn stream_encoder(&self, _asked: Asked) -> Box<dyn Encoder<Output>> {
        Box::new(EventStream::default())
    }

    /// Anthropic's 529 (`overloaded_error`) says what HTTP's 503 does: the
    /// service is overloaded, try later. So it reads as a 503, which retry
    /// ([`retryable`](respan_gateway_http::retryable)), routing and a client
    /// of any format understand; the answer as it came, 529, stays for a
    /// raw relay ([`ProviderError`](respan_gateway_http::ProviderError)).
    fn status(status: http::StatusCode) -> http::StatusCode {
        match status.as_u16() {
            529 => http::StatusCode::SERVICE_UNAVAILABLE,
            _ => status,
        }
    }

    /// A ping, or what is kept beside a response: a signature, a stop
    /// sequence, usage fields with no unit, a tool call's fields.
    fn is_inert(raw: &Value) -> bool {
        matches!(
            raw["type"].as_str(),
            Some("ping" | "message_delta" | "content_block_fields")
        ) || raw["delta"]["type"] == "signature_delta"
    }

    /// The system prompt becomes a leading system message.
    fn decode_json(&self, v: &Value) -> Input {
        let mut chat = Input::default();
        chat.options.max_output_tokens = v["max_tokens"].as_u64();
        chat.options.temperature = v["temperature"].as_f64();
        chat.options.top_p = v["top_p"].as_f64();
        chat.options.top_k = v["top_k"].as_u64();
        chat.options.stop = v["stop_sequences"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|s| s.as_str().map(Into::into))
            .collect();
        chat.user = v["metadata"]["user_id"].as_str().map(Into::into);
        chat.native = native_fields(ANTHROPIC, unsupported_fields(v, KNOWN, &[]));
        // Metadata beside the user id is kept for this format to write again.
        let others = v["metadata"].as_object().into_iter().flatten();
        let others: Map<String, Value> = others
            .filter(|(k, x)| *k != "user_id" && says(x))
            .map(|(k, x)| (k.clone(), x.clone()))
            .collect();
        if !others.is_empty() {
            chat.native
                .fields_mut(ANTHROPIC)
                .insert("metadata".into(), others.into());
        }
        if says(&v["system"]) {
            let mut system = Message::new(Role::System, "");
            system.content = decode_content::<Self>(&v["system"]);
            chat.messages.push(system);
        }
        for m in v["messages"].as_array().into_iter().flatten() {
            chat.messages.push(decode_message::<Self>(m, &ROLES));
        }
        read_tools_of(v, &mut chat);
        chat.native = native_fields(ANTHROPIC, std::mem::take(chat.native.fields_mut(ANTHROPIC)));
        read_output(v, &mut chat);
        read_thinking(&mut chat);
        chat
    }

    fn encode_json(&self, chat: &Input) -> Result<Map<String, Value>, Fault> {
        let (system, rest) = split_system(chat)?;
        if rest.is_empty() {
            return Err(BadRequest::fault(
                "Anthropic needs at least one user or assistant message",
            ));
        }
        if chat.options.temperature.is_some_and(|t| t > 1.0) {
            return Err(BadRequest::fault(
                "Anthropic's temperature is at most 1, so a higher one cannot be translated",
            ));
        }
        let mut v = Map::new();
        let max_tokens = chat.options.max_output_tokens.unwrap_or(DEFAULT_MAX_TOKENS);
        v.insert("max_tokens".into(), max_tokens.into());
        set(
            &mut v,
            "system",
            (!system.is_empty())
                .then(|| encode_content::<Self>(&system))
                .transpose()?,
        );
        // Anthropic refuses an empty turn but the last; dropping one leaves
        // consecutive turns of a role, which it combines.
        let last = rest.len().saturating_sub(1);
        let kept = rest
            .iter()
            .enumerate()
            .filter(|&(i, m)| i == last || !is_empty(m));
        let messages = kept.map(|(_, m)| encode_message::<Self>(&without_empty_text(m), &ROLES));
        let messages: Vec<Value> = messages.collect::<Result<_, _>>()?;
        v.insert("messages".into(), messages.into());
        set(
            &mut v,
            "temperature",
            chat.options.temperature.map(Into::into),
        );
        set(&mut v, "top_p", chat.options.top_p.map(Into::into));
        set(&mut v, "top_k", chat.options.top_k.map(Into::into));
        set(&mut v, "stop_sequences", stop_json(&chat.options.stop));
        let mut metadata = Value::Object(Map::new());
        if let Some(user) = &chat.user {
            metadata["user_id"] = user.as_str().into();
        }
        let own = chat.native.format() == Some(ANTHROPIC);
        if let Some(more) = chat
            .native
            .fields()
            .get("metadata")
            .and_then(Value::as_object)
        {
            merge_fields(
                &mut metadata,
                &more
                    .iter()
                    .filter(|_| own)
                    .map(|(k, x)| (k.clone(), x.clone()))
                    .collect(),
            );
        }
        let empty = metadata.as_object().is_none_or(Map::is_empty);
        set(&mut v, "metadata", (!empty).then_some(metadata));
        write_tools(chat, &mut v)?;
        write_output(chat, &mut v)?;
        set(&mut v, "thinking", write_thinking(chat)?);
        Ok(v)
    }

    fn is_response(v: &Value) -> bool {
        v["type"] == "message"
    }

    /// The blocks in the order they came, empty text left out.
    fn encode_whole(&self, whole: &Assembled) -> Result<Value, Fault> {
        check_usage(&whole.usage)?;
        let mut content: Vec<Value> = Vec::new();
        // Where each block, by number, is in `content`, and the JSON text of
        // the input of an event-made block, which is parsed at the end.
        let mut at: Vec<(u32, usize)> = Vec::new();
        let mut input: Vec<(usize, String)> = Vec::new();
        // The fields of blocks that come next.
        let mut fields: Vec<(u32, Map<String, Value>)> = Vec::new();
        let mut stop = StopFields::default();
        for item in whole.items() {
            match item {
                AssembledItem::Block(part) => {
                    let block = match &part.kind {
                        Block::Text if part.text.is_empty() => continue,
                        Block::Text => json!({ "type": "text", "text": part.text }),
                        Block::Reasoning => json!({ "type": "thinking", "thinking": part.text }),
                        Block::ToolCall { id, name } => {
                            let input =
                                arguments_json(&part.text).map_err(|_| unwritable_content())?;
                            json!({ "type": "tool_use", "id": id, "name": name, "input": input })
                        }
                        _ => return Err(unwritable_content()),
                    };
                    let mut block = block;
                    if let Some(i) = fields.iter().position(|(b, _)| *b == part.block) {
                        merge_fields(&mut block, &fields.remove(i).1);
                    }
                    at.push((part.block, content.len()));
                    content.push(block);
                }
                AssembledItem::Native(NativeValue { format, value: raw })
                    if **format == *ANTHROPIC && fields_of(raw).is_some() =>
                {
                    fields.extend(fields_of(raw).map(|(b, f)| (b, f.clone())));
                }
                AssembledItem::Native(NativeValue { format, value: raw })
                    if **format == *ANTHROPIC =>
                {
                    let place = raw["index"]
                        .as_u64()
                        .and_then(|i| u32::try_from(i).ok())
                        .and_then(|i| at.iter().find(|(b, _)| *b == i))
                        .map(|(_, place)| *place);
                    match (raw["type"].as_str(), place) {
                        (Some("content_block_start"), _) => {
                            let index = raw["index"].as_u64().and_then(|i| u32::try_from(i).ok());
                            at.extend(index.map(|i| (i, content.len())));
                            content.push(raw["content_block"].clone());
                        }
                        (Some("content_block_stop" | "ping"), _) => {}
                        (Some("message_delta"), _) => stop.add(raw),
                        (Some("content_block_delta"), Some(place)) => {
                            let delta = &raw["delta"];
                            match delta["type"].as_str() {
                                Some("signature_delta") => {
                                    content[place]["signature"] = delta["signature"].clone();
                                }
                                Some("citations_delta") => {
                                    let cites = &mut content[place]["citations"];
                                    if !cites.is_array() {
                                        *cites = json!([]);
                                    }
                                    if let Some(cites) = cites.as_array_mut() {
                                        cites.push(delta["citation"].clone());
                                    }
                                }
                                Some("input_json_delta") => {
                                    let json = delta["partial_json"].as_str().unwrap_or_default();
                                    match input.iter_mut().find(|(p, _)| *p == place) {
                                        Some((_, text)) => text.push_str(json),
                                        None => input.push((place, json.to_owned())),
                                    }
                                }
                                _ => return Err(unwritable_content()),
                            }
                        }
                        _ => return Err(unwritable_content()),
                    }
                }
                AssembledItem::Native(NativeValue { format, value: raw })
                    if is_inert(format, raw) => {}
                AssembledItem::Native(..) => return Err(unwritable_content()),
            }
        }
        for (place, text) in input {
            content[place]["input"] = arguments_json(&text).map_err(|_| unwritable_content())?;
        }
        let finish = whole.finish.as_ref().unwrap_or(&Finish::Stop);
        let (id, model) = whole
            .start
            .as_ref()
            .map(|s| (s.id.as_str(), s.model.as_str()))
            .unwrap_or_default();
        let mut body = Map::new();
        body.insert("id".into(), id.into());
        body.insert("type".into(), "message".into());
        body.insert("role".into(), "assistant".into());
        body.insert("model".into(), model.into());
        body.extend(own_fields(whole.start.as_ref()));
        body.insert("content".into(), content.into());
        body.insert("stop_reason".into(), stop.reason(finish)?.into());
        let sequence = stop.delta.get("stop_sequence").cloned();
        body.insert("stop_sequence".into(), sequence.unwrap_or(Value::Null));
        body.insert("usage".into(), usage_body(&whole.usage, &stop.usage));
        body.extend(stop.top);
        Ok(body.into())
    }

    fn error_body(fault: &Fault) -> Value {
        let error =
            json!({ "type": respan_gateway_http::error_code(fault), "message": fault.message() });
        json!({ "type": "error", "error": merge_details(error, fault) })
    }
}

/// The start's own fields, if they are in this format.
fn own_fields(start: Option<&Start>) -> Map<String, Value> {
    let start = start.filter(|s| s.native.format() == Some(ANTHROPIC));
    start.map(|s| s.native.fields().clone()).unwrap_or_default()
}

/// What a `message_delta` [`Output::Native`] says beyond the finish and usage:
/// fields of its `delta`, such as the stop sequence, and its own.
#[derive(Debug, Default)]
struct StopFields {
    delta: Map<String, Value>,
    top: Map<String, Value>,
    /// The usage fields that have no unit, such as the cache breakdown.
    usage: Map<String, Value>,
}

impl StopFields {
    /// The reason to write for `finish`: a stop that hit a sequence says so.
    fn reason<'a>(&self, finish: &'a Finish) -> Result<&'a str, Fault> {
        match (finish, self.delta.get("stop_sequence")) {
            (Finish::Stop, Some(Value::String(_))) => Ok("stop_sequence"),
            _ => FINISH.reason(finish),
        }
    }

    fn add(&mut self, raw: &Value) {
        for (key, value) in raw.as_object().into_iter().flatten() {
            match (key.as_str(), value.as_object()) {
                ("type", _) => {}
                ("usage", Some(usage)) => {
                    let mut all = Value::Object(std::mem::take(&mut self.usage));
                    merge_fields(&mut all, usage);
                    if let Value::Object(all) = all {
                        self.usage = all;
                    }
                }
                ("delta", Some(delta)) => self.delta.extend(delta.clone()),
                _ => _ = self.top.insert(key.clone(), value.clone()),
            }
        }
    }
}

/// Usage as this format reports it: the cache reads and writes beside the
/// input they are part of, and reasoning as `thinking_tokens`.
fn usage_body(u: &Usage, native: &Map<String, Value>) -> Value {
    let (cached, written) = (u.cached_input_tokens, u.cache_write_input_tokens);
    let mut body = json!({
        "input_tokens": u.input_tokens.saturating_sub(cached).saturating_sub(written),
        "output_tokens": u.output_tokens,
    });
    if cached > 0 {
        body["cache_read_input_tokens"] = json!(cached);
    }
    if written > 0 {
        body["cache_creation_input_tokens"] = json!(written);
    }
    if u.reasoning_tokens > 0 {
        body["output_tokens_details"] = json!({ "thinking_tokens": u.reasoning_tokens });
    }
    merge_fields(&mut body, native);
    body
}

/// What a usage object says that has no unit: the cache breakdown, the
/// service tier, and details beside the thinking count.
fn extra_usage(u: &Value) -> Map<String, Value> {
    let known = [
        "input_tokens",
        "output_tokens",
        "cache_read_input_tokens",
        "cache_creation_input_tokens",
    ];
    let details: [(&str, &[&str]); 1] = [("output_tokens_details", &["thinking_tokens"])];
    usage_extra(u, &known, &details)
}

fn check_usage(usage: &Usage) -> Result<(), Fault> {
    let (cached, written) = (usage.cached_input_tokens, usage.cache_write_input_tokens);
    if cached
        .checked_add(written)
        .is_none_or(|both| both > usage.input_tokens)
    {
        return Err(unwritable_usage());
    }
    Ok(())
}

/// Usage totals; [`Usage::input_tokens`] includes the cache reads and
/// writes Anthropic counts apart.
fn usage_of(u: &Value) -> Result<Usage, Fault> {
    let n = |key: &str| count(u.get(key));
    let mut usage = Usage::default();
    if let Some(input) = n("input_tokens")? {
        let cached = n("cache_read_input_tokens")?.unwrap_or(0);
        let written = n("cache_creation_input_tokens")?.unwrap_or(0);
        usage.input_tokens = input.saturating_add(cached).saturating_add(written);
        usage.cached_input_tokens = cached;
        usage.cache_write_input_tokens = written;
    }
    usage.output_tokens = n("output_tokens")?.unwrap_or(0);
    let thinking = u.pointer("/output_tokens_details/thinking_tokens");
    usage.reasoning_tokens = count(thinking)?.unwrap_or(0);
    Ok(usage)
}

/// Decodes events and whole bodies of Anthropic Messages into events, as
/// the module says.
#[derive(Debug)]
pub struct AnthropicDecoder {
    name: Arc<str>,
    totals: Totals,
    done: bool,
    /// The usage fields with no unit that were last said, to say them once.
    usage_extra: Map<String, Value>,
    /// The indexes of blocks the kind has no form for: all their events are
    /// [`Output::Native`].
    others: Vec<u32>,
}

impl Default for AnthropicDecoder {
    fn default() -> Self {
        Self {
            name: Arc::from(ANTHROPIC),
            totals: Totals::default(),
            done: false,
            usage_extra: Map::new(),
            others: Vec::new(),
        }
    }
}

/// The type of an [`Output::Native`] that carries the fields of a block's
/// start: not an event of the wire, but of this format's words.
pub(crate) const BLOCK_FIELDS: &str = "content_block_fields";

/// The fields a `Native` of [`BLOCK_FIELDS`] carries for block `block`.
fn fields_of(raw: &Value) -> Option<(u32, &Map<String, Value>)> {
    let block = u32::try_from(raw["index"].as_u64()?).ok()?;
    (raw["type"] == BLOCK_FIELDS).then_some(())?;
    Some((block, raw["fields"].as_object()?))
}

/// A block's event, as the stream says it, for what the kind has no form for.
fn event_of(kind: &str, index: u32, fields: Value) -> Value {
    let mut v = json!({ "type": kind, "index": index });
    if let (Some(v), Some(fields)) = (v.as_object_mut(), fields.as_object()) {
        v.extend(fields.clone());
    }
    v
}

impl AnthropicDecoder {
    fn other(&self, v: Value, out: &mut Vec<Output>) {
        out.push(Output::native(self.name.clone(), v));
    }

    /// The events of a message: its start and usage so far.
    fn message(&mut self, m: &Value, out: &mut Vec<Output>) -> Result<(), Fault> {
        let known = [
            "id",
            "model",
            "type",
            "role",
            "content",
            "usage",
            "stop_reason",
            "stop_sequence",
        ];
        let mut start = Start::new(
            m["id"].as_str().unwrap_or_default(),
            m["model"].as_str().unwrap_or_default(),
        );
        start.native = native_fields(ANTHROPIC, unsupported_fields(m, &known, &[]));
        out.push(Output::Start(start));
        let usage = self.totals.increment(&usage_of(&m["usage"])?);
        if !usage.is_empty() {
            out.push(Output::Usage(usage));
        }
        self.usage_other(&m["usage"], out);
        Ok(())
    }

    /// The fields of the usage object `u` that have no unit, as one `Native`
    /// that carries them in a `message_delta`'s usage.
    fn usage_other(&mut self, u: &Value, out: &mut Vec<Output>) {
        let native = extra_usage(u);
        if !native.is_empty() && native != self.usage_extra {
            let event = json!({ "type": "message_delta", "delta": {}, "usage": native });
            self.other(event, out);
            self.usage_extra = native;
        }
    }

    /// The events that open block `block`, `b`, with its first content; a
    /// block of a kind the kind has no form for is one `Native` of `event`.
    fn open(&mut self, block: u32, b: &Value, event: Value, out: &mut Vec<Output>) {
        let text = || b["text"].as_str().filter(|t| !t.is_empty());
        match b["type"].as_str() {
            Some("text") => {
                self.fields(block, b, &["type", "text"], out);
                out.push(Output::block_start(block, Block::Text));
                out.extend(text().map(|t| Output::text(block, t)));
            }
            Some("tool_use") => {
                let call = b["id"].as_str().zip(b["name"].as_str());
                let input = b["input"].as_object().filter(|_| call.is_some());
                let (Some((id, name)), Some(input)) = (call, input) else {
                    return self.opaque(block, event, out);
                };
                let kind = Block::ToolCall {
                    id: id.into(),
                    name: name.into(),
                };
                self.fields(block, b, &["type", "id", "name", "input"], out);
                out.push(Output::block_start(block, kind));
                if !input.is_empty() {
                    out.push(Output::arguments(block, b["input"].to_string()));
                }
            }
            Some("thinking") => {
                self.fields(block, b, &["type", "thinking", "signature"], out);
                out.push(Output::block_start(block, Block::Reasoning));
                let thinking = b["thinking"].as_str().filter(|t| !t.is_empty());
                out.extend(thinking.map(|t| Output::reasoning(block, t)));
                if says(&b["signature"]) {
                    let delta = json!({ "type": "signature_delta", "signature": b["signature"] });
                    self.other(
                        event_of("content_block_delta", block, json!({ "delta": delta })),
                        out,
                    );
                }
            }
            _ => self.opaque(block, event, out),
        }
    }

    /// The fields of block `b` the kind has no place for, beyond its `known`
    /// ones, as one `Native` before the block opens, which the encoder merges
    /// into the block's start.
    fn fields(&self, block: u32, b: &Value, known: &[&str], out: &mut Vec<Output>) {
        let rest = b.as_object().into_iter().flatten();
        let rest: Map<String, Value> = rest
            // Kept even when null or empty: an unknown field's presence is
            // part of what its format said.
            .filter(|(k, _)| !known.contains(&k.as_str()))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        if !rest.is_empty() {
            let event = json!({ "type": BLOCK_FIELDS, "index": block, "fields": rest });
            self.other(event, out);
        }
    }

    fn opaque(&mut self, block: u32, event: Value, out: &mut Vec<Output>) {
        self.others.push(block);
        self.other(event, out);
    }

    /// What a `message_delta` says beyond the finish and usage, as one
    /// `Native`, if it says anything: `delta` is its `delta` and `top` its own
    /// fields.
    fn stop_fields(&self, delta: &Value, top: &Value, out: &mut Vec<Output>) {
        let delta: Map<String, Value> = delta
            .as_object()
            .into_iter()
            .flatten()
            .filter(|(k, v)| *k != "stop_reason" && says(v))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let top = top.as_object().into_iter().flatten();
        let top = top.filter(|(k, v)| !["type", "delta", "usage"].contains(&k.as_str()) && says(v));
        let mut event = Map::new();
        event.insert("type".into(), "message_delta".into());
        event.insert("delta".into(), delta.clone().into());
        event.extend(top.map(|(k, v)| (k.clone(), v.clone())));
        if !delta.is_empty() || event.len() > 2 {
            self.other(event.into(), out);
        }
    }

    fn finish(&self, reason: &Value, out: &mut Vec<Output>) {
        let finish = reason.as_str().map(finish_of);
        out.extend(finish.map(Output::finished));
    }
}

impl Decoder for AnthropicDecoder {
    fn event(&mut self, raw: Bytes, out: &mut Vec<Output>) -> Result<(), Fault> {
        if self.done {
            return Ok(());
        }
        let Some(v) = event_json(&raw) else {
            unreadable_event(&self.name, &raw, out);
            return Ok(());
        };
        if says_error(&v) || v["type"] == "error" {
            return Err(error_event(ANTHROPIC, &raw, &v));
        }
        let block = v["index"]
            .as_u64()
            .and_then(|i| u32::try_from(i).ok())
            .unwrap_or(0);
        let opaque = self.others.contains(&block);
        match v["type"].as_str() {
            Some("message_start") => self.message(&v["message"], out)?,
            Some("content_block_start") => self.open(block, &v["content_block"], v.clone(), out),
            Some("content_block_delta") if !opaque => {
                let delta = &v["delta"];
                let text = |key: &str| delta[key].as_str().unwrap_or_default();
                match delta["type"].as_str() {
                    Some("text_delta") => out.push(Output::text(block, text("text"))),
                    Some("input_json_delta") => {
                        out.push(Output::arguments(block, text("partial_json")))
                    }
                    Some("thinking_delta") => out.push(Output::reasoning(block, text("thinking"))),
                    _ => self.other(v, out),
                }
            }
            Some("content_block_stop") if !opaque => out.push(Output::block_end(block)),
            Some("message_delta") => {
                let usage = self.totals.increment(&usage_of(&v["usage"])?);
                if !usage.is_empty() {
                    out.push(Output::Usage(usage));
                }
                self.usage_other(&v["usage"], out);
                self.stop_fields(&v["delta"], &v, out);
                self.finish(&v["delta"]["stop_reason"], out);
            }
            Some("message_stop") => self.done = true,
            _ => self.other(v, out),
        }
        Ok(())
    }

    fn end(&mut self, _out: &mut Vec<Output>) -> Result<(), Fault> {
        ended(self.done)
    }

    fn body(&mut self, v: &Value, out: &mut Vec<Output>) -> Result<(), Fault> {
        self.message(v, out)?;
        for (at, b) in v["content"].as_array().into_iter().flatten().enumerate() {
            let block = u32::try_from(at).unwrap_or(u32::MAX);
            let start = event_of("content_block_start", block, json!({ "content_block": b }));
            let before = out.len();
            self.open(block, b, start, out);
            let opaque = self.others.contains(&block);
            let stop = match opaque {
                true => event_of("content_block_stop", block, json!({})),
                false => {
                    out.push(Output::block_end(block));
                    continue;
                }
            };
            debug_assert!(out.len() > before);
            self.other(stop, out);
        }
        self.stop_fields(
            &json!({ "stop_sequence": v["stop_sequence"] }),
            &json!({}),
            out,
        );
        self.finish(&v["stop_reason"], out);
        self.done = true;
        Ok(())
    }
}

/// Writes events as Anthropic's stream: `message_start` at the first need,
/// each block's events, and at the end `message_delta` and `message_stop`.
/// Blocks are written one at a time: one that opens while another is open
/// waits, and goes out when the open one ends.
#[derive(Debug, Default)]
struct EventStream {
    start: Option<Start>,
    began: bool,
    usage: Usage,
    finish: Option<Finish>,
    stop: StopFields,
    blocks: Vec<Slot>,
    /// The block open on the wire.
    current: Option<u32>,
    /// The blocks waiting to open, in order.
    waiting: Vec<u32>,
    /// The index the next block takes.
    next: u32,
    /// The fields of blocks that have not opened, by block.
    fields: Vec<(u32, Map<String, Value>)>,
    gate: Gate,
}

/// A block, by number, as the stream has it so far.
#[derive(Debug)]
struct Slot {
    block: u32,
    kind: Block,
    /// Its index once it is open on the wire.
    index: Option<u32>,
    /// What came for it while it waited.
    held: Option<Delta>,
    ended: bool,
}

impl EventStream {
    /// Writes `message_start` once.
    fn begin(&mut self, out: &mut BytesMut) {
        if std::mem::replace(&mut self.began, true) {
            return;
        }
        let (id, model) = self
            .start
            .as_ref()
            .map(|s| (s.id.as_str(), s.model.as_str()))
            .unwrap_or_default();
        let mut message = Map::new();
        message.insert("id".into(), id.into());
        message.insert("type".into(), "message".into());
        message.insert("role".into(), "assistant".into());
        message.insert("model".into(), model.into());
        message.extend(own_fields(self.start.as_ref()));
        message.insert("content".into(), json!([]));
        message.insert("stop_reason".into(), Value::Null);
        message.insert("stop_sequence".into(), Value::Null);
        // What is known so far.
        message.insert("usage".into(), usage_body(&self.usage, &Map::new()));
        emit(out, json!({ "type": "message_start", "message": message }));
    }

    fn find(&mut self, block: u32) -> Result<&mut Slot, Fault> {
        let found = self.blocks.iter_mut().find(|b| b.block == block);
        found.ok_or_else(unwritable_content)
    }

    /// Opens a block on the wire, with what it held while it waited.
    fn open_wire(&mut self, block: u32, out: &mut BytesMut) -> Result<(), Fault> {
        let index = self.next;
        self.next += 1;
        let wb = self.find(block)?;
        wb.index = Some(index);
        let content = match &wb.kind {
            Block::Text => json!({ "type": "text", "text": "" }),
            Block::ToolCall { id, name } => {
                json!({ "type": "tool_use", "id": id, "name": name, "input": {} })
            }
            Block::Reasoning => json!({ "type": "thinking", "thinking": "" }),
            _ => return Err(unwritable_content()),
        };
        let held = wb.held.take();
        let mut content = content;
        if let Some(at) = self.fields.iter().position(|(b, _)| *b == block) {
            merge_fields(&mut content, &self.fields.remove(at).1);
        }
        let start =
            json!({ "type": "content_block_start", "index": index, "content_block": content });
        emit(out, start);
        if let Some(held) = held {
            delta(index, &held, out);
        }
        Ok(())
    }

    /// Opens the waiting blocks, one after another, as far as each has ended.
    fn pump(&mut self, out: &mut BytesMut) -> Result<(), Fault> {
        while self.current.is_none() && !self.waiting.is_empty() {
            let block = self.waiting.remove(0);
            self.open_wire(block, out)?;
            let wb = self.find(block)?;
            match (wb.ended, wb.index) {
                (true, Some(index)) => {
                    emit(out, json!({ "type": "content_block_stop", "index": index }))
                }
                _ => self.current = Some(block),
            }
        }
        Ok(())
    }

    fn open(&mut self, block: u32, kind: &Block, out: &mut BytesMut) -> Result<(), Fault> {
        if let Some(had) = self.blocks.iter().find(|b| b.block == block) {
            // A start repeated for a block that is open opens nothing more.
            return match had.kind == *kind && !had.ended {
                true => Ok(()),
                false => Err(unwritable_content()),
            };
        }
        self.begin(out);
        self.blocks.push(Slot {
            block,
            kind: kind.clone(),
            index: None,
            held: None,
            ended: false,
        });
        self.waiting.push(block);
        self.pump(out)
    }

    fn say(&mut self, block: u32, part: &Delta, out: &mut BytesMut) -> Result<(), Fault> {
        let wb = self.find(block)?;
        if wb.ended || !fits(part, &wb.kind) {
            return Err(unwritable_content());
        }
        match (wb.index, part.as_str().is_empty()) {
            (_, true) => {}
            (Some(index), false) => delta(index, part, out),
            // The block is not open on the wire yet: keep what it says.
            (None, false) => match &mut wb.held {
                Some(held) => held.as_mut_string().push_str(part.as_str()),
                None => wb.held = Some(part.clone()),
            },
        }
        Ok(())
    }

    fn close(&mut self, block: u32, out: &mut BytesMut) -> Result<(), Fault> {
        let wb = self.find(block)?;
        if std::mem::replace(&mut wb.ended, true) {
            return Ok(());
        }
        if let Some(index) = wb.index {
            emit(out, json!({ "type": "content_block_stop", "index": index }));
            self.current = None;
            self.pump(out)?;
        }
        Ok(())
    }

    /// Ends every block, then writes the terminal `message_delta`.
    fn finish(&mut self, out: &mut BytesMut) -> Result<(), Fault> {
        self.begin(out);
        let open: Vec<u32> = self.blocks.iter().map(|b| b.block).collect();
        for block in open {
            self.close(block, out)?;
        }
        let finish = self.finish.as_ref().unwrap_or(&Finish::Stop);
        let mut delta = Map::new();
        delta.insert("stop_reason".into(), self.stop.reason(finish)?.into());
        delta.insert("stop_sequence".into(), Value::Null);
        delta.extend(self.stop.delta.clone());
        let mut v = Map::new();
        v.insert("type".into(), "message_delta".into());
        v.insert("delta".into(), delta.into());
        v.insert("usage".into(), usage_body(&self.usage, &self.stop.usage));
        v.extend(self.stop.top.clone());
        emit(out, v.into());
        Ok(())
    }
}

fn delta(index: u32, part: &Delta, out: &mut BytesMut) {
    let text = part.as_str();
    let delta = match part {
        Delta::Text(_) => json!({ "type": "text_delta", "text": text }),
        Delta::Arguments(_) => json!({ "type": "input_json_delta", "partial_json": text }),
        Delta::Reasoning(_) => json!({ "type": "thinking_delta", "thinking": text }),
        _ => return,
    };
    if !text.is_empty() {
        emit(
            out,
            json!({ "type": "content_block_delta", "index": index, "delta": delta }),
        );
    }
}

/// Write the event `v`, named by its type.
fn emit(out: &mut BytesMut, v: Value) {
    out.extend_from_slice(event(v["type"].as_str(), &v).as_bytes());
}

impl Encoder<Output> for EventStream {
    fn encode(&mut self, reply: &Output, out: &mut BytesMut) -> Result<(), Fault> {
        let mut made = BytesMut::new();
        self.write(reply, &mut made)?;
        self.gate.pass(reply, &made, out);
        Ok(())
    }

    fn end(&mut self, end: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault> {
        if end.is_ok() {
            self.gate.release(out);
        }
        self.write_end(end, out)
    }
}

impl EventStream {
    fn write(&mut self, reply: &Output, out: &mut BytesMut) -> Result<(), Fault> {
        match reply {
            Output::Start(start) => _ = self.start.get_or_insert_with(|| start.clone()),
            Output::BlockStart { block, kind } => self.open(*block, kind, out)?,
            Output::Delta { block, delta } => self.say(*block, delta, out)?,
            Output::BlockEnd { block } => self.close(*block, out)?,
            Output::Usage(usage) => {
                self.usage += usage;
                check_usage(&self.usage)?;
            }
            Output::Finish(finish) => {
                FINISH.reason(finish)?;
                self.finish = Some(finish.clone());
                self.begin(out);
            }
            Output::Native(NativeValue { format, value: raw })
                if **format == *ANTHROPIC && fields_of(raw).is_some() =>
            {
                if let Some((block, fields)) = fields_of(raw) {
                    self.fields.push((block, fields.clone()));
                }
            }
            Output::Native(NativeValue { format, value: raw }) if **format == *ANTHROPIC => {
                self.begin(out);
                match raw["type"].as_str() {
                    // Beside the terminal `message_delta`, which carries them.
                    Some("message_delta") => self.stop.add(raw),
                    kind => {
                        // A block this format keeps takes its index.
                        if kind == Some("content_block_start")
                            && let Some(i) =
                                raw["index"].as_u64().and_then(|i| u32::try_from(i).ok())
                        {
                            self.next = self.next.max(i.saturating_add(1));
                        }
                        emit(out, Value::clone(raw));
                    }
                }
            }
            Output::Native(NativeValue { format, value: raw }) if is_inert(format, raw) => {}
            _ => return Err(unwritable_content()),
        }
        Ok(())
    }

    fn write_end(&mut self, end: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault> {
        if let Err(f) = end {
            // What the provider finished before it broke off is kept.
            if self.finish.is_some() {
                self.finish(out)?;
            }
            let body = match f.get::<respan_gateway_http::RawReply>().map(|r| r.0.body()) {
                Some(raw) => String::from_utf8_lossy(raw).into_owned(),
                None => AnthropicMessages::error_body(f).to_string(),
            };
            out.extend_from_slice(event(Some("error"), &body).as_bytes());
            return Ok(());
        }
        self.finish(out)?;
        emit(out, json!({ "type": "message_stop" }));
        Ok(())
    }
}
