//! Anthropic Messages: `POST /v1/messages`, JSON or SSE, of the
//! conversation kind.
//!
//! # Responses as events
//!
//! A stream and a whole body decode into the same events, by these rules:
//!
//! - [`Reply::Start`] comes from `message_start`, or the body's `id` and
//!   `model`; its `extra` holds the message's other fields.
//! - Blocks are numbered by their `index`. A `text`, `tool_use` or
//!   `thinking` block opens a [`Block::Text`], [`Block::ToolCall`] or
//!   [`Block::Reasoning`]; `text_delta`, `input_json_delta` and
//!   `thinking_delta` are its deltas, and `content_block_stop` its end. A
//!   whole body's blocks are the same events, one delta each.
//! - `message_delta`'s `stop_reason` is the [`Reply::Finish`]; `usage` of
//!   `message_start` and `message_delta` are [`Reply::Usage`] increments.
//! - What the kind has no form for is a [`Reply::Other`] of this format, as
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

use bytes::Bytes;
use http::HeaderValue;
use respan_gateway_http::bytes;
use respan_gateway_http::http;
use respan_gateway_http::{
    Body, Incoming, Outgoing, http::request::Parts as RequestHead,
    http::response::Parts as ResponseHead,
};
use respan_gateway_sdk::EventStream as TypedStream;
use respan_gateway_sdk::futures::stream::BoxStream;
use respan_gateway_sdk::{BadRequest, Connection, EventSink, Fault, Format};
use serde_json::{Map, Value, json};
use std::sync::Arc;

use crate::Usage;
use crate::common::{
    Gate, Item, Names, Reader, ReplyParser, Totals, UnaryEncoder, Whole, Wire, ended, error_event,
    extra_of, fits, is_inert, merge_fields, post_json, read_content, read_message, refusal, says,
    set, stop_json, supported_usage, tag, unreadable_event, unsupported_fields, unwritable_content,
    unwritable_usage, usage_extra, write_content, write_message,
};
use crate::conversation::{
    Block, Content, Conversation, Delta, Finish, Message, Reply, Role, Start, units,
};
use crate::formats::errors::{merge_details, says_error};
use crate::formats::sse::{event, event_json, is_stream};
use crate::tools::{arguments_json, read_anthropic_tools, write_anthropic_tools};
use respan_gateway_http::Encoder;
use respan_gateway_http::bytes::BytesMut;

/// The API version sent when the client names none.
const VERSION: &str = "2023-06-01";
const VERSION_HEADER: &str = "anthropic-version";

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

pub(crate) const ANTHROPIC: &str = "anthropic-messages";

/// Anthropic Messages, named `anthropic-messages`: a `POST` to any path
/// ending in `/v1/messages`. The provider's base URL leaves out the
/// version (`https://api.anthropic.com`); `anthropic-version: 2023-06-01`
/// is sent if none is set.
#[derive(Debug, Clone, Copy, Default)]
pub struct AnthropicMessages;

impl AnthropicMessages {
    /// This wire format's name.
    pub fn name(&self) -> &str {
        ANTHROPIC
    }

    /// Whether the HTTP request targets this API.
    pub fn accepts(&self, head: &RequestHead) -> bool {
        crate::formats::posts_to(head, "/v1/messages")
    }

    /// Read a request lazily.
    pub fn read_request(
        &self,
        request: http::Request<Body>,
    ) -> Result<TypedStream<Conversation>, Fault> {
        respan_gateway_http::messages(request, |_, raw| Self.decode_request(raw))
    }

    /// Decode a finite request synchronously.
    pub fn decode_request(&self, bytes: Bytes) -> Result<Conversation, Fault> {
        crate::common::read_request::<Self>(bytes)
    }

    /// Prepare a finite provider request before any transport I/O.
    pub fn write_request(
        &self,
        source: &mut TypedStream<Conversation>,
    ) -> Result<(http::Request<Body>, EventSink<Conversation>), Fault> {
        let (mut request, sink) = post_json::<Self>(source, "/v1/messages")?;
        request
            .headers_mut()
            .entry(VERSION_HEADER)
            .or_insert(HeaderValue::from_static(VERSION));
        Ok((request, sink))
    }

    fn parser(&self, hold_start: bool) -> ReplyParser<AnthropicReader> {
        ReplyParser::new(hold_start)
    }

    /// Read provider response events with fresh per-body parser state.
    pub fn read_response(
        &self,
        response: http::Response<Body>,
        call: http::Request<Bytes>,
    ) -> BoxStream<'static, Result<Reply, Fault>> {
        let parser = self.parser(true);
        crate::common::read_response::<Self>(response, &call.into_parts().0, |body| {
            crate::formats::read(
                body,
                parser,
                |parser, bytes, out| parser.push(bytes, out),
                |parser, wire, out| {
                    wire?;
                    parser.finish(out)
                },
            )
        })
    }

    /// Select response framing and its encoder; bodyless responses have none.
    pub fn response_encoder(
        &self,
        head: &mut ResponseHead,
        request: &http::Request<Bytes>,
    ) -> Option<Box<dyn Encoder<Reply>>> {
        crate::common::response_encoder(head, request, |head, _asked| match is_stream(head) {
            false => Box::new(UnaryEncoder::<Self>::new()),
            true => Box::new(EventStream::default()),
        })
    }

    /// Bind response delivery, preserving delayed heads and bodyless handling.
    pub fn write_response(
        &self,
        reply: respan_gateway_http::Reply,
        source: &mut TypedStream<Reply>,
    ) -> Result<EventSink<Reply>, Fault> {
        let mut head = reply.head();
        Ok(match self.response_encoder(&mut head, reply.request()) {
            Some(encoder) => reply.write(head, encoder),
            None => reply.empty(head, source),
        })
    }

    /// Render an HTTP refusal in this provider's error schema.
    pub fn refusal(
        &self,
        fault: &Fault,
        _request: &http::Request<Bytes>,
        head: &mut ResponseHead,
    ) -> Bytes {
        refusal::<Self>(fault, head)
    }

    /// Encode a finite answer synchronously, including finalization errors.
    pub fn encode_response(
        &self,
        request: &http::Request<Bytes>,
        replies: &[Reply],
    ) -> Result<http::Response<Bytes>, Fault> {
        let mut head = http::Response::new(()).into_parts().0;
        let encoder = self.response_encoder(&mut head, request);
        crate::common::encode_response(head, encoder, replies)
    }

    /// Decode a finite answer synchronously, without polling an async stream.
    pub fn decode_response(
        &self,
        response: http::Response<Bytes>,
        call: &RequestHead,
    ) -> Result<Vec<Reply>, Fault> {
        let (mut head, bytes) = response.into_parts();
        if head.status.is_success() && respan_gateway_http::bodiless(&call.method, head.status) {
            return Ok(Vec::new());
        }
        if is_stream(&head) {
            let mut parser = self.parser(true);
            let mut out = Vec::new();
            parser.push(bytes, &mut out)?;
            parser.finish(&mut out)?;
            return Ok(out);
        }
        crate::formats::default_content_type(&mut head, "application/json");
        if !head.status.is_success() {
            return Err(crate::formats::whole::answer(ANTHROPIC, &head, bytes));
        }
        crate::common::unary_reply::<Self>(head.status, bytes)
    }
}

impl Format<Incoming, Conversation, Reply> for AnthropicMessages {
    fn name(&self) -> &str {
        ANTHROPIC
    }
    fn matches(&self, io: &Incoming) -> bool {
        let mut head = respan_gateway_http::http::Request::new(()).into_parts().0;
        head.method = io.request().method().clone();
        head.uri = io.request().uri().clone();
        self.accepts(&head)
    }
    fn bind(&self, io: Incoming) -> Result<Connection<Conversation, Reply>, Fault> {
        Ok(io.bind(
            |request| Self.read_request(request),
            |reply, source| Self.write_response(reply, source),
            |fault, request, head| Self.refusal(fault, request, head),
        ))
    }
}

impl Format<Outgoing, Reply, Conversation> for AnthropicMessages {
    fn name(&self) -> &str {
        ANTHROPIC
    }
    fn bind(&self, io: Outgoing) -> Result<Connection<Reply, Conversation>, Fault> {
        Ok(io.bind(
            |source| Self.write_request(source),
            |response, call| Self.read_response(response, call),
            respan_gateway_http::success,
        ))
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
fn split_system(chat: &Conversation) -> Result<(Vec<Content>, &[Message]), Fault> {
    let lead = chat.messages.iter().take_while(|m| is_system(m)).count();
    let (system, rest) = chat.messages.split_at(lead);
    if rest.iter().any(is_system) {
        return Err(BadRequest::fault(
            "Anthropic has no place for a system message after the conversation starts",
        ));
    }
    if system.iter().any(|m| !m.extra.is_empty()) {
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
    m.extra.is_empty() && m.content.iter().all(empty)
}

impl Wire for AnthropicMessages {
    const NAME: &'static str = ANTHROPIC;
    const OWN: &'static [&'static str] = &["metadata"];
    type Reading = AnthropicReader;

    /// The system prompt becomes a leading system message.
    fn read(v: &Value) -> Conversation {
        let mut chat = Conversation::default();
        chat.stream = v["stream"] == true;
        chat.max_tokens = v["max_tokens"].as_u64();
        chat.temperature = v["temperature"].as_f64();
        chat.top_p = v["top_p"].as_f64();
        chat.stop = v["stop_sequences"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|s| s.as_str().map(Into::into))
            .collect();
        chat.user = v["metadata"]["user_id"].as_str().map(Into::into);
        chat.extra.fields = unsupported_fields(v, KNOWN, &[]);
        // Metadata beside the user id is kept for this format to write again.
        let others = v["metadata"].as_object().into_iter().flatten();
        let others: Map<String, Value> = others
            .filter(|(k, x)| *k != "user_id" && says(x))
            .map(|(k, x)| (k.clone(), x.clone()))
            .collect();
        if !others.is_empty() {
            chat.extra.fields.insert("metadata".into(), others.into());
        }
        if says(&v["system"]) {
            let mut system = Message::new(Role::System, "");
            system.content = read_content(ANTHROPIC, &v["system"]);
            chat.messages.push(system);
        }
        for m in v["messages"].as_array().into_iter().flatten() {
            let mut message = read_message(ANTHROPIC, m, &ROLES);
            tag(&mut message, ANTHROPIC);
            chat.messages.push(message);
        }
        read_anthropic_tools(v, &mut chat);
        chat.extra = extra_of(ANTHROPIC, std::mem::take(&mut chat.extra.fields));
        chat
    }

    fn write(chat: &Conversation) -> Result<Map<String, Value>, Fault> {
        let (system, rest) = split_system(chat)?;
        if rest.is_empty() {
            return Err(BadRequest::fault(
                "Anthropic needs at least one user or assistant message",
            ));
        }
        if chat.temperature.is_some_and(|t| t > 1.0) {
            return Err(BadRequest::fault(
                "Anthropic's temperature is at most 1, so a higher one cannot be translated",
            ));
        }
        let mut v = Map::new();
        let max_tokens = chat.max_tokens.unwrap_or(DEFAULT_MAX_TOKENS);
        v.insert("max_tokens".into(), max_tokens.into());
        set(&mut v, "stream", chat.stream.then_some(true.into()));
        set(
            &mut v,
            "system",
            (!system.is_empty())
                .then(|| write_content(ANTHROPIC, &system))
                .transpose()?,
        );
        // Anthropic refuses an empty turn but the last; dropping one leaves
        // consecutive turns of a role, which it combines.
        let last = rest.len().saturating_sub(1);
        let kept = rest
            .iter()
            .enumerate()
            .filter(|&(i, m)| i == last || !is_empty(m));
        let messages = kept.map(|(_, m)| write_message(ANTHROPIC, &without_empty_text(m), &ROLES));
        let messages: Vec<Value> = messages.collect::<Result<_, _>>()?;
        v.insert("messages".into(), messages.into());
        set(&mut v, "temperature", chat.temperature.map(Into::into));
        set(&mut v, "top_p", chat.top_p.map(Into::into));
        set(&mut v, "stop_sequences", stop_json(&chat.stop));
        let mut metadata = Value::Object(Map::new());
        if let Some(user) = &chat.user {
            metadata["user_id"] = user.as_str().into();
        }
        let own = chat.extra.format.as_deref() == Some(ANTHROPIC);
        if let Some(more) = chat.extra.fields.get("metadata").and_then(Value::as_object) {
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
        write_anthropic_tools(chat, &mut v)?;
        Ok(v)
    }

    fn is_response(v: &Value) -> bool {
        v["type"] == "message"
    }

    /// The blocks in the order they came, empty text left out.
    fn render(whole: &Whole) -> Result<Value, Fault> {
        check_usage(&whole.usage)?;
        let mut content: Vec<Value> = Vec::new();
        // Where each block, by number, is in `content`, and the JSON text of
        // the input of an event-made block, which is parsed at the end.
        let mut at: Vec<(u32, usize)> = Vec::new();
        let mut input: Vec<(usize, String)> = Vec::new();
        // The fields of blocks that come next.
        let mut fields: Vec<(u32, Map<String, Value>)> = Vec::new();
        let mut stop = StopFields::default();
        for item in &whole.items {
            match item {
                Item::Block(part) => {
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
                Item::Other(format, raw) if **format == *ANTHROPIC && fields_of(raw).is_some() => {
                    fields.extend(fields_of(raw).map(|(b, f)| (b, f.clone())));
                }
                Item::Other(format, raw) if **format == *ANTHROPIC => {
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
                Item::Other(format, raw) if is_inert(format, raw) => {}
                Item::Other(..) => return Err(unwritable_content()),
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
    let start = start.filter(|s| s.extra.format.as_deref() == Some(ANTHROPIC));
    start.map(|s| s.extra.fields.clone()).unwrap_or_default()
}

/// What a `message_delta` [`Reply::Other`] says beyond the finish and usage:
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
fn usage_body(u: &Usage, extra: &Map<String, Value>) -> Value {
    let get = |unit| u.get(unit).unwrap_or_default();
    let cached = get(units::CACHED_INPUT_TOKENS);
    let written = get(units::CACHE_WRITE_INPUT_TOKENS);
    let mut body = json!({
        "input_tokens": get(units::INPUT_TOKENS).saturating_sub(cached).saturating_sub(written),
        "output_tokens": get(units::OUTPUT_TOKENS),
    });
    if cached > 0 {
        body["cache_read_input_tokens"] = json!(cached);
    }
    if written > 0 {
        body["cache_creation_input_tokens"] = json!(written);
    }
    let reasoning = get(units::REASONING_TOKENS);
    if reasoning > 0 {
        body["output_tokens_details"] = json!({ "thinking_tokens": reasoning });
    }
    merge_fields(&mut body, extra);
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
    supported_usage(
        usage,
        &[
            units::INPUT_TOKENS,
            units::OUTPUT_TOKENS,
            units::CACHED_INPUT_TOKENS,
            units::CACHE_WRITE_INPUT_TOKENS,
            units::REASONING_TOKENS,
        ],
    )?;
    let input = usage.get(units::INPUT_TOKENS).unwrap_or_default();
    let cached = usage.get(units::CACHED_INPUT_TOKENS).unwrap_or_default();
    let written = usage
        .get(units::CACHE_WRITE_INPUT_TOKENS)
        .unwrap_or_default();
    if cached.checked_add(written).is_none_or(|both| both > input) {
        return Err(unwritable_usage());
    }
    Ok(())
}

/// Usage totals; [`units::INPUT_TOKENS`] includes the cache reads and
/// writes Anthropic counts apart.
fn usage_of(u: &Value) -> Result<Usage, Fault> {
    let n = |key: &str| crate::common::count(u.get(key));
    let mut usage = Usage::default();
    if let Some(input) = n("input_tokens")? {
        let cached = n("cache_read_input_tokens")?.unwrap_or(0);
        let written = n("cache_creation_input_tokens")?.unwrap_or(0);
        usage.add(units::INPUT_TOKENS, input);
        usage.add(units::INPUT_TOKENS, cached);
        usage.add(units::INPUT_TOKENS, written);
        if cached > 0 {
            usage.add(units::CACHED_INPUT_TOKENS, cached);
        }
        if written > 0 {
            usage.add(units::CACHE_WRITE_INPUT_TOKENS, written);
        }
    }
    if let Some(output) = n("output_tokens")? {
        usage.add(units::OUTPUT_TOKENS, output);
    }
    let thinking = u.pointer("/output_tokens_details/thinking_tokens");
    if let Some(thinking) = crate::common::count(thinking)? {
        usage.add(units::REASONING_TOKENS, thinking);
    }
    Ok(usage)
}

/// Decodes events and whole bodies of Anthropic Messages into events, as
/// the module says.
#[derive(Debug)]
pub(crate) struct AnthropicReader {
    name: Arc<str>,
    totals: Totals,
    done: bool,
    /// The usage fields with no unit that were last said, to say them once.
    usage_extra: Map<String, Value>,
    /// The indexes of blocks the kind has no form for: all their events are
    /// [`Reply::Other`].
    others: Vec<u32>,
}

impl Default for AnthropicReader {
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

/// The type of an [`Reply::Other`] that carries the fields of a block's
/// start: not an event of the wire, but of this format's words.
pub(crate) const BLOCK_FIELDS: &str = "content_block_fields";

/// The fields an `Other` of [`BLOCK_FIELDS`] carries for block `block`.
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

impl AnthropicReader {
    fn other(&self, v: Value, out: &mut Vec<Reply>) {
        out.push(Reply::other(self.name.clone(), v));
    }

    /// The events of a message: its start and usage so far.
    fn message(&mut self, m: &Value, out: &mut Vec<Reply>) -> Result<(), Fault> {
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
        start.extra = extra_of(ANTHROPIC, unsupported_fields(m, &known, &[]));
        out.push(Reply::Start(start));
        let usage = self.totals.increment(&usage_of(&m["usage"])?);
        if !usage.is_empty() {
            out.push(Reply::Usage(usage));
        }
        self.usage_other(&m["usage"], out);
        Ok(())
    }

    /// The fields of the usage object `u` that have no unit, as one `Other`
    /// that carries them in a `message_delta`'s usage.
    fn usage_other(&mut self, u: &Value, out: &mut Vec<Reply>) {
        let extra = extra_usage(u);
        if !extra.is_empty() && extra != self.usage_extra {
            let event = json!({ "type": "message_delta", "delta": {}, "usage": extra });
            self.other(event, out);
            self.usage_extra = extra;
        }
    }

    /// The events that open block `block`, `b`, with its first content; a
    /// block of a kind the kind has no form for is one `Other` of `event`.
    fn open(&mut self, block: u32, b: &Value, event: Value, out: &mut Vec<Reply>) {
        let text = || b["text"].as_str().filter(|t| !t.is_empty());
        match b["type"].as_str() {
            Some("text") => {
                self.fields(block, b, &["type", "text"], out);
                out.push(Reply::block_start(block, Block::Text));
                out.extend(text().map(|t| Reply::text(block, t)));
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
                out.push(Reply::block_start(block, kind));
                if !input.is_empty() {
                    out.push(Reply::arguments(block, b["input"].to_string()));
                }
            }
            Some("thinking") => {
                self.fields(block, b, &["type", "thinking", "signature"], out);
                out.push(Reply::block_start(block, Block::Reasoning));
                let thinking = b["thinking"].as_str().filter(|t| !t.is_empty());
                out.extend(thinking.map(|t| Reply::reasoning(block, t)));
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
    /// ones, as one `Other` before the block opens, which the encoder merges
    /// into the block's start.
    fn fields(&self, block: u32, b: &Value, known: &[&str], out: &mut Vec<Reply>) {
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

    fn opaque(&mut self, block: u32, event: Value, out: &mut Vec<Reply>) {
        self.others.push(block);
        self.other(event, out);
    }

    /// What a `message_delta` says beyond the finish and usage, as one
    /// `Other`, if it says anything: `delta` is its `delta` and `top` its own
    /// fields.
    fn stop_fields(&self, delta: &Value, top: &Value, out: &mut Vec<Reply>) {
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

    fn finish(&self, reason: &Value, out: &mut Vec<Reply>) {
        let finish = reason.as_str().map(|r| FINISH.finish(r));
        out.extend(finish.map(Reply::finished));
    }
}

impl Reader for AnthropicReader {
    fn event(&mut self, raw: Bytes, out: &mut Vec<Reply>) -> Result<(), Fault> {
        if self.done {
            return Ok(());
        }
        let Some(v) = event_json(&raw) else {
            unreadable_event(&self.name, &raw, out);
            return Ok(());
        };
        if says_error(&v) || v["type"] == "error" {
            return Err(error_event(&self.name, &raw, &v));
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
                    Some("text_delta") => out.push(Reply::text(block, text("text"))),
                    Some("input_json_delta") => {
                        out.push(Reply::arguments(block, text("partial_json")))
                    }
                    Some("thinking_delta") => out.push(Reply::reasoning(block, text("thinking"))),
                    _ => self.other(v, out),
                }
            }
            Some("content_block_stop") if !opaque => out.push(Reply::block_end(block)),
            Some("message_delta") => {
                let usage = self.totals.increment(&usage_of(&v["usage"])?);
                if !usage.is_empty() {
                    out.push(Reply::Usage(usage));
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

    fn end(&mut self, _out: &mut Vec<Reply>) -> Result<(), Fault> {
        ended(self.done)
    }

    fn body(&mut self, v: &Value, out: &mut Vec<Reply>) -> Result<(), Fault> {
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
                    out.push(Reply::block_end(block));
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

impl Encoder<Reply> for EventStream {
    fn encode(&mut self, reply: &Reply, out: &mut BytesMut) -> Result<(), Fault> {
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
    fn write(&mut self, reply: &Reply, out: &mut BytesMut) -> Result<(), Fault> {
        match reply {
            Reply::Start(start) => _ = self.start.get_or_insert_with(|| start.clone()),
            Reply::BlockStart { block, kind } => self.open(*block, kind, out)?,
            Reply::Delta { block, delta } => self.say(*block, delta, out)?,
            Reply::BlockEnd { block } => self.close(*block, out)?,
            Reply::Usage(usage) => {
                self.usage += usage;
                check_usage(&self.usage)?;
            }
            Reply::Finish(finish) => {
                FINISH.reason(finish)?;
                self.finish = Some(finish.clone());
                self.begin(out);
            }
            Reply::Other { format, raw } if **format == *ANTHROPIC && fields_of(raw).is_some() => {
                if let Some((block, fields)) = fields_of(raw) {
                    self.fields.push((block, fields.clone()));
                }
            }
            Reply::Other { format, raw } if **format == *ANTHROPIC => {
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
            Reply::Other { format, raw } if is_inert(format, raw) => {}
            Reply::Other { .. } => return Err(unwritable_content()),
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

#[cfg(test)]
mod tests {
    use super::*;

    use crate::common::write_request;
    use crate::conversation::{Extra, ToolChoice};

    /// The events of `events`, each decoded in turn.
    fn decode(events: &[&str]) -> Vec<Reply> {
        let mut reader = AnthropicReader::default();
        let mut out = Vec::new();
        for e in events {
            let _ = reader.event(Bytes::from(e.to_string()), &mut out);
        }
        out
    }

    /// The events of a whole body.
    fn decode_body(v: &Value) -> Vec<Reply> {
        let mut out = Vec::new();
        AnthropicReader::default()
            .body(v, &mut out)
            .expect("a whole response");
        out
    }

    /// `replies` as a stream, whole.
    fn stream_of(replies: &[Reply]) -> Result<String, Fault> {
        let mut enc = EventStream::default();
        let mut out = BytesMut::new();
        for reply in replies {
            enc.encode(reply, &mut out)?;
        }
        enc.end(&Ok(()), &mut out)?;
        Ok(String::from_utf8_lossy(&out).into_owned())
    }

    /// The JSON of each event of a stream.
    fn stream_events(stream: &str) -> Vec<Value> {
        let data = stream
            .split("\n\n")
            .filter_map(|e| e.lines().find_map(|l| l.strip_prefix("data: ")));
        data.filter_map(|d| serde_json::from_str(d).ok()).collect()
    }

    /// `replies` as a whole body.
    fn body_of(replies: &[Reply]) -> Result<Value, Fault> {
        let mut whole = Whole::default();
        replies.iter().try_for_each(|r| whole.add(r))?;
        AnthropicMessages::render(&whole)
    }

    fn call(id: &str, name: &str) -> Block {
        Block::ToolCall {
            id: id.into(),
            name: name.into(),
        }
    }

    fn usage(unit: &'static str, n: u64) -> Reply {
        let mut u = Usage::default();
        u.add(unit, n);
        Reply::usage(u)
    }

    fn text_events(block: u32, text: &str) -> Vec<Reply> {
        vec![
            Reply::block_start(block, Block::Text),
            Reply::text(block, text),
            Reply::block_end(block),
        ]
    }

    /// The client's `body` as the conversation it says, with `edit` made,
    /// written as a request.
    fn written(body: Value, edit: impl FnOnce(&mut Conversation)) -> Result<Value, Fault> {
        let mut chat = AnthropicMessages.decode_request(Bytes::from(body.to_string()))?;
        edit(&mut chat);
        Ok(write_request::<AnthropicMessages>(&chat)?.into())
    }

    #[test]
    fn an_edited_tool_call_and_result_keep_their_blocks() {
        let marker = json!({ "type": "ephemeral" });
        let body = json!({ "model": "c", "max_tokens": 5,
        "tools": [{ "name": "lookup", "input_schema": { "type": "object" }, "cache_control": marker }],
        "tool_choice": { "type": "tool", "name": "lookup", "disable_parallel_tool_use": true },
        "messages": [
            { "role": "user", "content": "find bob" },
            { "role": "assistant", "content": [
                { "type": "tool_use", "id": "t1", "name": "lookup", "input": { "email": "bob@x.io" } },
            ] },
            { "role": "user", "content": [
                { "type": "tool_result", "tool_use_id": "t1", "content": "bob@x.io is Bob" },
            ] },
        ]});
        let chat = AnthropicMessages::read(&body);
        assert_eq!(
            chat.tools[0].extra.fields.get("cache_control"),
            Some(&marker)
        );
        assert_eq!(chat.tools[0].extra.format.as_deref(), Some(ANTHROPIC));
        assert_eq!(chat.tool_choice, Some(ToolChoice::Tool("lookup".into())));
        assert_eq!(chat.parallel_tool_calls, Some(false));
        assert_eq!(
            chat.messages[2].content,
            [Content::tool_result(
                "t1",
                vec!["bob@x.io is Bob".into()],
                false
            )]
        );
        assert_eq!(written(body.clone(), |_| {}).ok(), Some(body.clone()));
        let edited = written(body.clone(), |c| {
            for part in c.messages.iter_mut().flat_map(|m| &mut m.content) {
                match part {
                    Content::ToolCall { arguments, .. } => {
                        *arguments = arguments.replace("bob@x.io", "<EMAIL_1>");
                    }
                    Content::ToolResult { content, .. } => {
                        *content = vec!["<EMAIL_1> is Bob".into()];
                    }
                    _ => {}
                }
            }
        });
        let mut expected = body;
        expected["messages"][1]["content"][0]["input"] = json!({ "email": "<EMAIL_1>" });
        expected["messages"][2]["content"][0]["content"] = "<EMAIL_1> is Bob".into();
        assert_eq!(edited.ok(), Some(expected));
    }

    #[test]
    fn a_request_round_trips_through_a_conversation() {
        let body = json!({
            "model": "claude",
            "max_tokens": 10,
            "system": [{ "type": "text", "text": "be brief" }],
            "messages": [{ "role": "user", "content": "hi" }],
            "stop_sequences": ["x"],
        });
        assert_eq!(
            written(body, |_| {}).ok(),
            Some(json!({
                "model": "claude",
                "max_tokens": 10,
                "system": "be brief",
                "messages": [{ "role": "user", "content": "hi" }],
                "stop_sequences": ["x"],
            }))
        );
    }

    #[test]
    fn the_metadata_user_id_is_the_user() {
        let body = json!({ "model": "c", "max_tokens": 5,
            "metadata": { "user_id": "bob@x.io" },
            "messages": [{ "role": "user", "content": "hi" }] });
        let chat = AnthropicMessages::read(&body);
        assert_eq!(chat.user.as_deref(), Some("bob@x.io"));
        assert_eq!(written(body.clone(), |_| {}).ok(), Some(body.clone()));
        let redacted = written(body.clone(), |c| c.user = Some("[x]".into()));
        let metadata = redacted.ok().map(|v| v["metadata"].clone());
        assert_eq!(metadata, Some(json!({ "user_id": "[x]" })));
        let removed = written(body, |c| c.user = None);
        assert_eq!(removed.ok().map(|v| v.get("metadata").cloned()), Some(None));
    }

    #[test]
    fn a_count_written_as_a_whole_float_counts_and_any_other_is_refused() {
        let floats = usage_of(&json!({ "input_tokens": 12.0, "output_tokens": 3 }));
        let floats = floats.expect("a whole float is a count");
        assert_eq!(floats.get(units::INPUT_TOKENS), Some(12));
        assert_eq!(floats.get(units::OUTPUT_TOKENS), Some(3));
        for bad in [json!(1.5), json!(-1), json!("7")] {
            let refused = usage_of(&json!({ "input_tokens": 1, "output_tokens": bad }));
            assert!(
                refused.is_err_and(|f| f.is::<respan_gateway_sdk::Upstream>()),
                "{bad}"
            );
        }
        let none = usage_of(&json!({ "input_tokens": null })).expect("null says none");
        assert!(none.is_empty());
    }

    #[test]
    fn input_usage_counts_cache_reads_and_writes() {
        let u = usage_of(&json!({
            "input_tokens": 3,
            "cache_read_input_tokens": 10,
            "cache_creation_input_tokens": 2,
            "output_tokens": 1,
        }))
        .expect("whole counts");
        assert_eq!(u.get(units::INPUT_TOKENS), Some(15));
        assert_eq!(u.get(units::CACHED_INPUT_TOKENS), Some(10));
        assert_eq!(u.get(units::OUTPUT_TOKENS), Some(1));
    }

    #[test]
    fn stream_usage_is_counted_once_and_thinking_is_reasoning() {
        let out = decode(&[
            "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m\",\"model\":\"c\",\"usage\":{\"input_tokens\":5,\"output_tokens\":1}}}\n\n",
            "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"hm\"}}\n\n",
            "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"input_tokens\":5,\"output_tokens\":7}}\n\n",
        ]);
        let mut total = Usage::default();
        for r in &out {
            if let Reply::Usage(u) = r {
                total += u;
            }
        }
        assert_eq!(total.get(units::INPUT_TOKENS), Some(5));
        assert_eq!(total.get(units::OUTPUT_TOKENS), Some(7));
        assert!(out.contains(&Reply::reasoning(0, "hm")));
    }

    #[test]
    fn an_event_it_cannot_read_is_content_filters_see() {
        let out = decode(&[
            "event: future\ndata: {\"type\":\"future\",\"text\":\"secret\"}\n\n",
            "data: not json\n\n",
            "event: ping\ndata: {\"type\":\"ping\"}\n\n",
            ": a comment\n\n",
        ]);
        // A comment says nothing; the others are events of this format.
        assert_eq!(
            out,
            [
                Reply::other(ANTHROPIC, json!({ "type": "future", "text": "secret" })),
                Reply::other(ANTHROPIC, "not json"),
                Reply::other(ANTHROPIC, json!({ "type": "ping" })),
            ]
        );
        assert!(out.iter().all(|r| !r.is_content()));
    }

    #[test]
    fn text_a_block_opens_with_is_text() {
        let out = decode(&[
            "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":2,\"content_block\":{\"type\":\"text\",\"text\":\"Well, \"}}\n\n",
            "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":3,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
        ]);
        assert_eq!(
            out,
            [
                Reply::block_start(2, Block::Text),
                Reply::text(2, "Well, "),
                // An empty text opens a block that says nothing yet.
                Reply::block_start(3, Block::Text),
            ]
        );
    }

    #[test]
    fn a_system_message_goes_in_the_system_prompt_only_before_the_conversation() {
        let mut chat = Conversation::new([
            Message::new(Role::System, "be brief"),
            Message::new(Role::User, "hi"),
        ]);
        let v = write_request::<AnthropicMessages>(&chat).map(Value::from);
        let v = v.unwrap_or_default();
        assert_eq!(v["system"], "be brief");
        assert_eq!(v["messages"], json!([{ "role": "user", "content": "hi" }]));
        chat.messages.push(Message::new(Role::System, "later"));
        assert!(write_request::<AnthropicMessages>(&chat).is_err());
    }

    #[test]
    fn a_temperature_above_1_is_refused() {
        let mut chat = Conversation::new([Message::new(Role::User, "hi")]);
        chat.temperature = Some(1.5);
        assert!(write_request::<AnthropicMessages>(&chat).is_err());
        chat.temperature = Some(1.0);
        assert!(write_request::<AnthropicMessages>(&chat).is_ok());
    }

    #[test]
    fn an_empty_turn_but_the_last_is_dropped() {
        let chat = Conversation::new([
            Message::new(Role::User, "hi"),
            Message::new(Role::Assistant, ""),
            Message::new(Role::User, "again"),
            Message::new(Role::Assistant, ""),
        ]);
        let v = write_request::<AnthropicMessages>(&chat).map(Value::from);
        assert_eq!(
            v.map(|v| v["messages"].clone()).ok(),
            Some(json!([
                { "role": "user", "content": "hi" },
                { "role": "user", "content": "again" },
                { "role": "assistant", "content": "" },
            ]))
        );
    }

    #[test]
    fn a_request_of_only_system_messages_is_refused() {
        let chat = Conversation::new([Message::new(Role::System, "be brief")]);
        assert!(write_request::<AnthropicMessages>(&chat).is_err());
    }

    #[test]
    fn a_system_prompt_with_cache_markers_is_written_as_it_is() {
        let system = json!([
            { "type": "text", "text": "the repo", "cache_control": { "type": "ephemeral" } },
            { "type": "text", "text": "be brief" },
        ]);
        let body = json!({ "model": "m", "max_tokens": 5, "system": system,
            "messages": [{ "role": "user", "content": "hi" }] });
        // Edited elsewhere, it is as it was.
        let edited = written(body.clone(), |c| c.messages[1].content = vec!["HI".into()]);
        assert_eq!(edited.map(|v| v["system"].clone()).ok(), Some(system));
        // A part with a marker is text, with the marker in its own extra.
        let chat = AnthropicMessages::read(&body);
        assert!(chat.messages[0].is_plain());
        let Content::Text { text, extra, .. } = &chat.messages[0].content[0] else {
            panic!("{:?}", chat.messages[0].content);
        };
        assert_eq!(text, "the repo");
        assert_eq!(
            extra.fields.get("cache_control"),
            Some(&json!({ "type": "ephemeral" }))
        );
        assert_eq!(chat.messages[0].content[1], Content::from("be brief"));
    }

    #[test]
    fn a_system_prompt_redacted_beside_an_inserted_one_keeps_its_cache_marker() {
        let marked =
            json!({ "type": "text", "text": "key sk-1", "cache_control": { "type": "ephemeral" } });
        let body = json!({ "model": "m", "max_tokens": 5, "system": [marked],
            "messages": [{ "role": "user", "content": "hi" }] });
        let edited = written(body, |c| {
            c.messages.insert(0, Message::new(Role::System, "first"));
        });
        assert_eq!(
            edited.map(|v| v["system"].clone()).ok(),
            Some(json!([{ "type": "text", "text": "first" }, marked]))
        );
    }

    #[test]
    fn an_unchanged_part_keeps_its_marker_beside_an_edit_and_an_append() {
        let marked =
            json!({ "type": "text", "text": "A", "cache_control": { "type": "ephemeral" } });
        let body = json!({ "model": "m", "max_tokens": 5, "messages": [
            { "role": "user", "content": [marked, { "type": "text", "text": "B" }] }] });
        let edited = written(body, |c| {
            c.messages[0].content[1] = "B2".into();
            c.messages[0].content.push("C".into());
        });
        assert_eq!(
            edited.map(|v| v["messages"][0]["content"].clone()).ok(),
            Some(json!([
                marked,
                { "type": "text", "text": "B2" },
                { "type": "text", "text": "C" },
            ]))
        );
    }

    #[test]
    fn a_whole_response_decodes_into_events_per_block() {
        let v = json!({
            "id": "m", "type": "message", "role": "assistant", "model": "c",
            "content": [
                { "type": "thinking", "thinking": "hm", "signature": "sig" },
                { "type": "text", "text": "ok" },
                { "type": "tool_use", "id": "t", "name": "f", "input": { "a": 1 } },
                { "type": "text", "text": "cited", "citations": [{ "type": "char_location" }] },
            ],
            "stop_reason": "tool_use", "stop_sequence": null,
            "usage": { "input_tokens": 2, "output_tokens": 1 },
        });
        let events = decode_body(&v);
        let signature = json!({ "type": "content_block_delta", "index": 0,
            "delta": { "type": "signature_delta", "signature": "sig" } });
        let cited = json!({ "type": "content_block_fields", "index": 3,
            "fields": { "citations": [{ "type": "char_location" }] } });
        let mut totals = usage(units::INPUT_TOKENS, 2);
        if let Reply::Usage(u) = &mut totals {
            u.add(units::OUTPUT_TOKENS, 1);
        }
        assert_eq!(
            events,
            [
                Reply::start("m", "c"),
                totals,
                Reply::block_start(0, Block::Reasoning),
                Reply::reasoning(0, "hm"),
                Reply::other(ANTHROPIC, signature),
                Reply::block_end(0),
                Reply::block_start(1, Block::Text),
                Reply::text(1, "ok"),
                Reply::block_end(1),
                Reply::block_start(2, call("t", "f")),
                Reply::arguments(2, "{\"a\":1}"),
                Reply::block_end(2),
                // What the kind has no place for comes before the block opens.
                Reply::other(ANTHROPIC, cited),
                Reply::block_start(3, Block::Text),
                Reply::text(3, "cited"),
                Reply::block_end(3),
                Reply::finished(Finish::ToolUse),
            ]
        );
        // Written again, the whole body is what it was.
        let again = body_of(&events).expect("a body");
        assert_eq!(again["content"], v["content"]);
        assert_eq!(again["stop_reason"], "tool_use");
        assert_eq!(
            again["usage"],
            json!({ "input_tokens": 2, "output_tokens": 1 })
        );
    }

    #[test]
    fn a_cache_marked_part_is_kept_as_the_format_has_it_beside_edited_text() {
        let marked =
            json!({ "type": "text", "text": "the repo", "cache_control": { "type": "ephemeral" } });
        let body = json!({ "model": "m", "max_tokens": 5, "messages": [
            { "role": "user", "content": [marked, { "type": "text", "text": "mine" }] }] });
        let edited = written(body, |c| c.messages[0].content[1] = "MINE".into());
        assert_eq!(
            edited.map(|v| v["messages"][0]["content"].clone()).ok(),
            Some(json!([marked, { "type": "text", "text": "MINE" }]))
        );
    }

    #[test]
    fn an_sse_envelope_is_no_part_of_a_response() {
        let plain = "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"secret\"}}\n\n";
        let wrapped = "id: 7\nevent: content_block_delta\nretry: 1500\n: kept\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"secret\"}}\n\n";
        assert_eq!(decode(&[plain]), decode(&[wrapped]));
    }

    #[test]
    fn usage_so_far_is_in_message_start_and_the_totals_in_message_delta() {
        let mut replies = vec![Reply::start("m", "c"), usage(units::INPUT_TOKENS, 4)];
        replies.extend(text_events(0, "hi"));
        replies.push(usage(units::OUTPUT_TOKENS, 2));
        replies.push(Reply::finished(Finish::Stop));
        let stream = stream_of(&replies).expect("a stream");
        let events = stream_events(&stream);
        assert_eq!(
            events[0]["message"]["usage"],
            json!({ "input_tokens": 4, "output_tokens": 0 })
        );
        let delta = events.iter().find(|e| e["type"] == "message_delta");
        assert_eq!(
            delta.map(|d| d["usage"].clone()),
            Some(json!({ "input_tokens": 4, "output_tokens": 2 }))
        );
    }

    #[test]
    fn a_finish_released_after_usage_keeps_the_final_totals() {
        let mut replies = vec![Reply::start("m", "c")];
        replies.extend(text_events(0, "hi"));
        replies.push(usage(units::OUTPUT_TOKENS, 2));
        replies.push(usage(units::OUTPUT_TOKENS, 3));
        replies.push(Reply::finished(Finish::Length));
        let stream = stream_of(&replies).expect("a stream");
        let events = stream_events(&stream);
        let delta = events.iter().find(|e| e["type"] == "message_delta");
        let delta = delta.cloned().unwrap_or_default();
        assert_eq!(delta["usage"]["output_tokens"], 5);
        assert_eq!(delta["delta"]["stop_reason"], "max_tokens");
    }

    #[test]
    fn dropped_usage_does_not_reappear_in_later_totals() {
        let events = [
            "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{},\"usage\":{\"output_tokens\":1}}\n\n",
            "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{},\"usage\":{\"output_tokens\":7}}\n\n",
            "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{},\"usage\":{\"output_tokens\":7}}\n\n",
        ];
        let mut replies = decode(&events);
        // The second report, which adds 6, is dropped.
        replies.retain(|r| !matches!(r, Reply::Usage(u) if u.get(units::OUTPUT_TOKENS) == Some(6)));
        let stream = stream_of(&replies).expect("a stream");
        let events = stream_events(&stream);
        let delta = events.iter().find(|e| e["type"] == "message_delta");
        assert_eq!(
            delta.map(|d| d["usage"]["output_tokens"].clone()),
            Some(json!(1))
        );
    }

    #[test]
    fn usage_never_decreases_and_edits_upward_are_summed() {
        for (totals, edited, expected) in [([7, 1, 0], None, 7), ([1, 7, 7], Some(3), 9)] {
            let events = totals.map(|n| {
                format!("event: message_delta\ndata: {{\"type\":\"message_delta\",\"delta\":{{}},\"usage\":{{\"output_tokens\":{n}}}}}\n\n")
            });
            let mut replies = decode(&[&events[0], &events[1], &events[2]]);
            if let Some(amount) = edited {
                replies[0] = usage(units::OUTPUT_TOKENS, amount);
            }
            let stream = stream_of(&replies).expect("a stream");
            let events = stream_events(&stream);
            let delta = events.iter().find(|e| e["type"] == "message_delta");
            assert_eq!(
                delta.map(|d| d["usage"]["output_tokens"].clone()),
                Some(json!(expected)),
                "{stream}"
            );
        }
    }

    #[test]
    fn unsupported_usage_edits_fail_instead_of_disappearing() {
        let mut replies = vec![Reply::start("m", "c")];
        replies.extend(text_events(0, "ok"));
        replies.push(usage("credits", 3));
        assert!(stream_of(&replies).is_err());
        assert!(body_of(&replies).is_err());
        // Cache reads are part of the input: more than it is no usage.
        let mut more = usage(units::CACHED_INPUT_TOKENS, 3);
        if let Reply::Usage(u) = &mut more {
            u.add(units::INPUT_TOKENS, 1);
        }
        assert!(body_of(&[more]).is_err());
    }

    #[test]
    fn cache_usage_is_written_as_the_units_say() {
        let mut u = Usage::default();
        for (unit, n) in [
            (units::INPUT_TOKENS, 15),
            (units::CACHED_INPUT_TOKENS, 10),
            (units::CACHE_WRITE_INPUT_TOKENS, 2),
            (units::OUTPUT_TOKENS, 1),
        ] {
            u.add(unit, n);
        }
        assert_eq!(
            usage_body(&u, &Map::new()),
            json!({ "input_tokens": 3, "output_tokens": 1,
                "cache_read_input_tokens": 10, "cache_creation_input_tokens": 2 })
        );
        // A cache that was emptied is no field at all.
        let mut none = Usage::default();
        none.add(units::INPUT_TOKENS, 15);
        none.add(units::OUTPUT_TOKENS, 1);
        assert_eq!(
            usage_body(&none, &Map::new()),
            json!({ "input_tokens": 15, "output_tokens": 1 })
        );
    }

    #[test]
    fn usage_is_written_as_it_was_read() {
        for u in [
            json!({ "input_tokens": 3, "output_tokens": 1 }),
            json!({ "input_tokens": 3, "output_tokens": 1,
                "cache_read_input_tokens": 10, "cache_creation_input_tokens": 2 }),
            json!({ "input_tokens": 3, "output_tokens": 4,
                "output_tokens_details": { "thinking_tokens": 2 } }),
        ] {
            assert_eq!(
                usage_body(&usage_of(&u).expect("whole counts"), &Map::new()),
                u
            );
        }
    }

    #[test]
    fn a_stop_reason_is_written_as_it_was_read() {
        for (reason, sequence) in [
            ("end_turn", Value::Null),
            ("stop_sequence", json!("END")),
            ("max_tokens", Value::Null),
            ("model_context_window_exceeded", Value::Null),
        ] {
            let v = json!({ "id": "m", "type": "message", "role": "assistant", "model": "c",
                "content": [], "stop_reason": reason, "stop_sequence": sequence,
                "usage": { "input_tokens": 1, "output_tokens": 1 } });
            let again = body_of(&decode_body(&v)).expect("a body");
            assert_eq!(again["stop_reason"], v["stop_reason"]);
            assert_eq!(again["stop_sequence"], v["stop_sequence"]);
            let stream = stream_of(&decode_body(&v)).expect("a stream");
            let delta = stream_events(&stream)
                .into_iter()
                .find(|e| e["type"] == "message_delta");
            let delta = delta.unwrap_or_default();
            assert_eq!(delta["delta"]["stop_reason"], v["stop_reason"]);
            assert_eq!(delta["delta"]["stop_sequence"], v["stop_sequence"]);
        }
    }

    #[test]
    fn blocks_are_numbered_by_their_index() {
        let v = json!({ "id": "m", "type": "message", "role": "assistant", "model": "c",
            "content": [
                { "type": "text", "text": "a" },
                { "type": "tool_use", "id": "t", "name": "f", "input": {} },
                { "type": "text", "text": "b" },
            ], "stop_reason": "tool_use", "usage": { "input_tokens": 1, "output_tokens": 1 } });
        let blocks: Vec<_> = decode_body(&v)
            .into_iter()
            .filter_map(|r| match r {
                Reply::BlockStart { block, .. } => Some(block),
                _ => None,
            })
            .collect();
        assert_eq!(blocks, [0, 1, 2]);
    }

    #[test]
    fn a_tool_call_a_filter_turns_to_text_is_gone() {
        let mut replies = vec![Reply::start("m", "c")];
        replies.extend(text_events(0, "[removed]"));
        let stream = stream_of(&replies).expect("a stream");
        assert!(
            stream.contains("[removed]") && !stream.contains("tool_use"),
            "{stream}"
        );
    }

    #[test]
    fn a_text_block_with_citations_is_text_with_its_fields_and_events() {
        let out = decode(&[
            "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"x\",\"citations\":[{\"type\":\"c\"}]}}\n\n",
            "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"citations_delta\",\"citation\":{}}}\n\n",
            "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
        ]);
        assert_eq!(out.len(), 5);
        assert!(matches!(&out[0], Reply::Other { .. }));
        assert_eq!(out[1], Reply::block_start(0, Block::Text));
        assert_eq!(out[2], Reply::text(0, "x"));
        // And written as it is, in this format.
        let stream = stream_of(&out).expect("a stream");
        assert!(stream.contains("citations_delta"), "{stream}");
    }

    #[test]
    fn an_event_the_format_cannot_write_fails() {
        // An event of another format that carries content.
        let foreign = Reply::other(
            "openai-chat",
            json!({ "choices": [{ "index": 1, "delta": { "content": "B" } }] }),
        );
        assert!(stream_of(std::slice::from_ref(&foreign)).is_err());
        assert!(body_of(&[foreign]).is_err());
        // Text for a block that never opened.
        assert!(stream_of(&[Reply::text(0, "x")]).is_err());
        // A block that opens as another kind.
        let events = [
            Reply::block_start(0, Block::Text),
            Reply::block_start(0, call("t", "f")),
        ];
        assert!(stream_of(&events).is_err());
    }

    #[test]
    fn a_text_part_that_says_more_keeps_it_in_its_extra_and_is_still_plain() {
        let part = json!({ "type": "text", "text": "hi", "citations": [{ "type": "c" }] });
        let chat = AnthropicMessages::read(&json!({ "model": "m", "max_tokens": 1,
            "messages": [{ "role": "user", "content": [part] }] }));
        assert!(chat.messages[0].is_plain());
        assert_eq!(chat.messages[0].text(), "hi");
        let again = write_request::<AnthropicMessages>(&chat).map(Value::from);
        assert_eq!(
            again.map(|v| v["messages"][0]["content"][0].clone()).ok(),
            Some(part)
        );
    }

    #[test]
    fn a_system_prompt_of_several_blocks_is_edited_block_by_block() {
        let body = json!({ "model": "m", "max_tokens": 5, "system": [
            { "type": "text", "text": "one" }, { "type": "text", "text": "two sk-1" }],
            "messages": [{ "role": "user", "content": "hi" }] });
        let edited = written(body, |c| c.messages[0].content[1] = "two [x]".into());
        assert_eq!(
            edited.map(|v| v["system"].clone()).ok(),
            Some(json!([{ "type": "text", "text": "one" }, { "type": "text", "text": "two [x]" }]))
        );
    }

    #[test]
    fn a_removed_message_takes_its_cache_marker_with_it() {
        let marked = json!({ "role": "user", "content": [
            { "type": "text", "text": "the repo", "cache_control": { "type": "ephemeral" } }] });
        let body = json!({ "model": "m", "max_tokens": 5, "messages": [
            marked, { "role": "assistant", "content": "ok" }] });
        let edited = written(body, |c| drop(c.messages.remove(0)));
        assert_eq!(
            edited.map(|v| v["messages"].clone()).ok(),
            Some(json!([{ "role": "assistant", "content": "ok" }]))
        );
    }

    #[test]
    fn a_redacted_message_keeps_its_marker_beside_an_inserted_one() {
        let marked =
            json!({ "type": "text", "text": "the repo", "cache_control": { "type": "ephemeral" } });
        let body = json!({ "model": "m", "max_tokens": 5, "messages": [
            { "role": "user", "content": [marked] }] });
        let edited = written(body, |c| {
            c.messages.insert(0, Message::new(Role::User, "first"));
        });
        assert_eq!(
            edited.map(|v| v["messages"].clone()).ok(),
            Some(json!([
                { "role": "user", "content": "first" },
                { "role": "user", "content": [marked] },
            ]))
        );
    }

    #[test]
    fn an_empty_answer_has_no_empty_text_block() {
        let replies = [
            Reply::start("m", "c"),
            Reply::block_start(0, Block::Text),
            Reply::block_end(0),
        ];
        let body = body_of(&replies).expect("a body");
        assert_eq!(body["content"], json!([]));
        let stream = stream_of(&replies).expect("a stream");
        assert!(stream.ends_with("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"));
    }

    #[test]
    fn text_released_late_goes_into_its_block_before_it_stops() {
        // A filter held the end of block 0 and the next text, and released
        // them at the end of the response, in order.
        let replies = [
            Reply::start("m", "c"),
            Reply::block_start(0, Block::Text),
            Reply::text(0, "a"),
            Reply::text(0, "b"),
            Reply::block_end(0),
        ];
        let stream = stream_of(&replies).expect("a stream");
        let events = stream_events(&stream);
        let kinds: Vec<_> = events.iter().filter_map(|e| e["type"].as_str()).collect();
        assert_eq!(
            kinds,
            [
                "message_start",
                "content_block_start",
                "content_block_delta",
                "content_block_delta",
                "content_block_stop",
                "message_delta",
                "message_stop",
            ]
        );
    }

    #[test]
    fn blocks_that_open_together_are_written_one_at_a_time() {
        // OpenAI streams parallel calls interleaved.
        let replies = [
            Reply::block_start(0, call("a", "f")),
            Reply::block_start(1, call("b", "g")),
            Reply::arguments(1, "{\"b\":"),
            Reply::arguments(0, "{}"),
            Reply::arguments(1, "1}"),
            Reply::block_end(0),
            Reply::block_end(1),
        ];
        let stream = stream_of(&replies).expect("a stream");
        let events = stream_events(&stream);
        let seen: Vec<String> = events
            .iter()
            .filter(|e| {
                e["type"]
                    .as_str()
                    .is_some_and(|t| t.starts_with("content_block"))
            })
            .map(|e| {
                let at = &e["index"];
                match e["type"].as_str().unwrap_or_default() {
                    "content_block_delta" => format!("delta{at}:{}", e["delta"]["partial_json"]),
                    kind => format!("{kind}{at}"),
                }
            })
            .collect();
        assert_eq!(
            seen,
            [
                "content_block_start0",
                "delta0:\"{}\"",
                "content_block_stop0",
                "content_block_start1",
                "delta1:\"{\\\"b\\\":1}\"",
                "content_block_stop1",
            ]
        );
    }

    #[test]
    fn a_stream_error_after_the_finish_keeps_the_finish_and_ends_last() {
        let mut enc = EventStream::default();
        let mut out = BytesMut::new();
        let mut replies = vec![Reply::start("m", "c")];
        replies.extend(text_events(0, "hi"));
        replies.push(Reply::finished(Finish::Stop));
        for reply in &replies {
            assert!(enc.encode(reply, &mut out).is_ok());
        }
        let _ = enc.end(&Err(respan_gateway_sdk::Upstream::fault("boom")), &mut out);
        let text = String::from_utf8_lossy(&out);
        let delta = text.find("message_delta").unwrap_or(usize::MAX);
        let error = text.find("event: error").unwrap_or(0);
        assert!(delta < error, "{text}");
        assert!(!text.contains("message_stop"), "{text}");
    }

    #[test]
    fn extra_fields_of_the_message_survive_in_the_same_format() {
        let body = json!({ "id": "m", "type": "message", "role": "assistant", "model": "c",
            "content": [{ "type": "text", "text": "hi" }], "stop_reason": "end_turn",
            "stop_sequence": null, "container": { "id": "k" },
            "usage": { "input_tokens": 1, "output_tokens": 1 } });
        let events = decode_body(&body);
        let Some(Reply::Start(start)) = events.first() else {
            panic!("{events:?}");
        };
        assert_eq!(
            start.extra,
            Extra::new(
                ANTHROPIC,
                json!({ "container": { "id": "k" } })
                    .as_object()
                    .cloned()
                    .unwrap_or_default()
            )
        );
        assert_eq!(
            body_of(&events).expect("a body")["container"],
            json!({ "id": "k" })
        );
        let stream = stream_of(&events).expect("a stream");
        assert_eq!(
            stream_events(&stream)[0]["message"]["container"],
            json!({ "id": "k" })
        );
        // Another format drops it: it cannot change the answer.
        let openai = crate::OpenAiChat.encode_response(
            &respan_gateway_http::http::Request::new(Bytes::from_static(b"{}")),
            &events,
        );
        assert!(openai.is_ok());
    }
}
