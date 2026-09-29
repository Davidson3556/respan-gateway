//! Anthropic Messages: `POST /v1/messages`, JSON or SSE, of the
//! conversation kind.

use bytes::{Bytes, BytesMut};
use http::HeaderValue;
use respan_gateway_core::{Decoder, Encoder, Fault, Format};
use respan_gateway_core::{bytes, serde_json};
use respan_gateway_http::http;
use respan_gateway_http::{Answer, Ask};
use serde_json::{Map, Value, json};

use crate::Usage;
use crate::common::{
    Names, Reading, SoFar, UnaryEncoder, Wire, error_event, holding_start, merge_details,
    patch_content, patch_messages, patch_sampling, post_json, read_content, read_message,
    reads_back, replace, request_decoder, response_decoder, response_encoder, says, says_error,
    set, split_around, stop_json, unpatchable, unreadable_event, unsupported_fields,
    unwritable_content, write_content, write_message,
};
use crate::conversation::{
    Content, Conversation, Finish, Message, Piece, Reply, Role, Start, units,
};
use crate::sse::{Events, SseDecoder, event, event_json, is_stream};

/// The API version sent when the client names none.
const VERSION: &str = "2023-06-01";
const VERSION_HEADER: &str = "anthropic-version";

/// The `max_tokens` of a translated request without one, as it is required.
const DEFAULT_MAX_TOKENS: u64 = 4096;

const FINISH: Names<Finish> = Names(&[
    ("end_turn", Finish::Stop),
    ("stop_sequence", Finish::Stop),
    ("max_tokens", Finish::Length),
    ("tool_use", Finish::ToolUse),
    ("refusal", Finish::Filtered),
]);

const ANTHROPIC: &str = "anthropic-messages";

/// Anthropic Messages, named `anthropic-messages`: a `POST` to any path
/// ending in `/v1/messages`. The provider's base URL leaves out the
/// version (`https://api.anthropic.com`); `anthropic-version: 2023-06-01`
/// is sent if none is set.
#[derive(Debug, Clone, Copy, Default)]
pub struct AnthropicMessages;

impl Format for AnthropicMessages {
    type Ask = Ask;
    type Answer = Answer;
    type Input = Conversation;
    type Output = Reply;

    fn name(&self) -> &str {
        ANTHROPIC
    }

    fn accepts(&self, head: &Ask) -> bool {
        crate::posts_to(head, "/v1/messages")
    }

    fn request_decoder(&self, _head: &Ask) -> Box<dyn Decoder<Conversation>> {
        request_decoder::<Self>()
    }

    /// The `anthropic-version` a relay passed, or the default.
    fn request_encoder(
        &self,
        client: Option<&Ask>,
        body: &mut Bytes,
        streams: bool,
    ) -> Box<dyn Encoder<Conversation, Ask>> {
        let mut head = crate::call_head(client);
        let version = head.headers.entry(VERSION_HEADER);
        version.or_insert(HeaderValue::from_static(VERSION));
        post_json::<Self>(head, body, "/v1/messages", streams)
    }

    fn response_decoder(&self, head: &Answer, call: &Ask, _: &Bytes) -> Box<dyn Decoder<Reply>> {
        let stream = || holding_start(SseDecoder::new(Reading::<Self>::new()));
        response_decoder::<Self>(head, call, stream)
    }

    fn response_encoder(
        &self,
        answer: Option<&Answer>,
        ask: &Ask,
        body: &Bytes,
        refusal: Option<&Fault>,
    ) -> Box<dyn Encoder<Reply, Answer>> {
        response_encoder::<Self>(
            answer,
            ask,
            body,
            refusal,
            |head, relayed, _| match is_stream(head) {
                true => Box::new(EventStream::new(relayed)),
                false => Box::new(UnaryEncoder::<Self>::new(relayed)),
            },
        )
    }

    fn is_content(&self, reply: &Reply) -> bool {
        reply.is_content()
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
const ROLES: Names<Role> = Names(&[("user", Role::User), ("assistant", Role::Assistant)]);

/// The parts of the leading system messages, as the system prompt, and
/// the rest; a later system message is refused.
fn split_system(chat: &Conversation) -> Result<(Vec<Content>, &[Message]), Fault> {
    let lead = chat
        .messages
        .iter()
        .take_while(|m| m.role == Role::System)
        .count();
    let (system, rest) = chat.messages.split_at(lead);
    if rest.iter().any(|m| m.role == Role::System) {
        return Err(Fault::bad_request(
            "Anthropic has no place for a system message after the conversation starts",
        ));
    }
    if system.iter().any(|m| !m.unsupported.is_empty()) {
        return Err(Fault::bad_request(
            "Anthropic's system prompt has no place for a message's other fields",
        ));
    }
    let parts = system.iter().flat_map(|m| m.content.iter().cloned());
    Ok((parts.collect(), rest))
}

/// The system prompt of `after`'s leading system messages: the client's
/// edited in its own form, others as new parts beside it.
fn patch_system(
    system: Option<&Value>,
    before: &Conversation,
    after: &Conversation,
) -> Result<Value, Fault> {
    let own = before.messages.first().filter(|m| m.role == Role::System);
    let lead: Vec<&Message> = after
        .messages
        .iter()
        .take_while(|m| m.role == Role::System)
        .collect();
    let patch = |m: &Message| match own.filter(|o| o.same_source(m)) {
        Some(o) => patch_content(system, &o.content, &m.content),
        None => Ok(write_content(&m.content)),
    };
    // One message keeps the prompt's own form, a string or parts.
    if let ([only], false) = (&lead[..], matches!(system, Some(Value::Array(_)))) {
        return patch(only);
    }
    let mut parts = Vec::new();
    for m in lead {
        match patch(m)? {
            Value::Array(more) => parts.extend(more),
            Value::String(text) => parts.push(json!({ "type": "text", "text": text })),
            other => parts.push(other),
        }
    }
    Ok(Value::Array(parts))
}

/// Whether a message says nothing: no content, or only empty text.
fn is_empty(m: &Message) -> bool {
    let empty = |c: &Content| matches!(c, Content::Text { text } if text.is_empty());
    m.unsupported.is_empty() && m.content.iter().all(empty)
}

/// Set the metadata's user id, or remove it and the metadata if left empty.
fn patch_user(v: &mut Map<String, Value>, user: Option<&str>) {
    let metadata = v.entry("metadata").or_insert(Value::Null);
    if !metadata.is_object() {
        *metadata = json!({});
    }
    if let Some(fields) = metadata.as_object_mut() {
        match user {
            Some(user) => _ = fields.insert("user_id".into(), user.into()),
            None => _ = fields.shift_remove("user_id"),
        }
        if fields.is_empty() {
            v.shift_remove("metadata");
        }
    }
}

impl Wire for AnthropicMessages {
    const NAME: &'static str = ANTHROPIC;

    /// The system prompt becomes a leading system message.
    fn read(v: &Value) -> Conversation {
        let mut chat = Conversation {
            stream: v["stream"] == true,
            max_tokens: v["max_tokens"].as_u64(),
            temperature: v["temperature"].as_f64(),
            top_p: v["top_p"].as_f64(),
            stop: v["stop_sequences"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|s| s.as_str().map(Into::into))
                .collect(),
            user: v["metadata"]["user_id"].as_str().map(Into::into),
            unsupported: unsupported_fields(v, KNOWN, &[]),
            ..Conversation::default()
        };
        if says(&v["system"]) {
            let mut system = Message::new(Role::System, "").with_source(v["system"].clone());
            system.content = read_content(&v["system"]);
            chat.messages.push(system);
        }
        for m in v["messages"].as_array().into_iter().flatten() {
            chat.messages.push(read_message(m, &ROLES));
        }
        chat
    }

    fn write(chat: &Conversation) -> Result<Value, Fault> {
        let (system, rest) = split_system(chat)?;
        if rest.is_empty() {
            return Err(Fault::bad_request(
                "Anthropic needs at least one user or assistant message",
            ));
        }
        if chat.temperature.is_some_and(|t| t > 1.0) {
            return Err(Fault::bad_request(
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
            (!system.is_empty()).then(|| write_content(&system)),
        );
        // Anthropic refuses an empty turn but the last; dropping one leaves
        // consecutive turns of a role, which it combines.
        let last = rest.len().saturating_sub(1);
        let kept = rest
            .iter()
            .enumerate()
            .filter(|&(i, m)| i == last || !is_empty(m));
        let messages = kept.map(|(_, m)| write_message(m, &ROLES));
        let messages: Vec<Value> = messages.collect::<Result<_, _>>()?;
        v.insert("messages".into(), messages.into());
        set(&mut v, "temperature", chat.temperature.map(Into::into));
        set(&mut v, "top_p", chat.top_p.map(Into::into));
        set(&mut v, "stop_sequences", stop_json(&chat.stop));
        let user = chat.user.as_ref().map(|u| json!({ "user_id": u }));
        set(&mut v, "metadata", user);
        Ok(v.into())
    }

    /// Cache markers and anything else unchanged keep their place.
    fn patch(
        v: &mut Map<String, Value>,
        before: &Conversation,
        after: &Conversation,
    ) -> Result<(), Fault> {
        let (was, _) = split_system(before)?;
        let (system, rest) = split_system(after).map_err(|f| unpatchable(f.message()))?;
        if was != system {
            let patched = patch_system(v.get("system"), before, after)?;
            set(v, "system", (!system.is_empty()).then_some(patched));
        }
        if before.max_tokens != after.max_tokens {
            let max_tokens = after.max_tokens.unwrap_or(DEFAULT_MAX_TOKENS);
            v.insert("max_tokens".into(), max_tokens.into());
        }
        patch_sampling(v, before, after, "stop_sequences");
        if before.user != after.user {
            patch_user(v, after.user.as_deref());
        }
        if before.messages != after.messages {
            let messages = patch_messages(&before.messages, rest, &ROLES)?;
            v.insert("messages".into(), messages.into());
        }
        Ok(())
    }

    fn is_response(v: &Value) -> bool {
        v["type"] == "message"
    }

    /// One piece per content block.
    fn reply(v: &Value) -> Reply {
        let mut reply = Reply::start(
            v["id"].as_str().unwrap_or_default(),
            v["model"].as_str().unwrap_or_default(),
        );
        let blocks = v["content"].as_array().into_iter().flatten();
        for (i, b) in blocks.enumerate() {
            // A text block with more, such as citations, is other content.
            let more = |(k, x): (&String, &Value)| k != "type" && k != "text" && says(x);
            let plain = !b.as_object().into_iter().flatten().any(more);
            reply
                .content
                .push(match (b["type"].as_str(), b["text"].as_str()) {
                    (Some("text"), Some(text)) if plain => {
                        Piece::text(u32::try_from(i).unwrap_or(u32::MAX), text)
                    }
                    _ => Piece::other(ANTHROPIC, b.clone()),
                });
        }
        reply.finish = v["stop_reason"].as_str().map(|r| FINISH.finish(r));
        reply
    }

    fn usage(v: &Value) -> Usage {
        usage_of(&v["usage"])
    }

    /// The inverse of [`usage_of`].
    fn usage_body(u: &Usage) -> Value {
        let get = |unit| u.get(unit).unwrap_or_default();
        let cached = get(units::CACHED_INPUT_TOKENS);
        let written = get(units::CACHE_WRITE_INPUT_TOKENS);
        let mut body = json!({
            "input_tokens": (get(units::INPUT_TOKENS) - cached - written) as u64,
            "output_tokens": get(units::OUTPUT_TOKENS) as u64,
        });
        if cached > 0.0 {
            body["cache_read_input_tokens"] = json!(cached as u64);
        }
        if written > 0.0 {
            body["cache_creation_input_tokens"] = json!(written as u64);
        }
        body
    }

    fn render(so_far: &SoFar) -> Value {
        let content: Vec<Value> = so_far
            .text
            .values()
            .map(|t| json!({ "type": "text", "text": t }))
            .collect();
        let finish = so_far.finish.as_ref().unwrap_or(&Finish::Stop);
        let (id, model) = so_far.start.clone().unwrap_or_default();
        json!({
            "id": id,
            "type": "message",
            "role": "assistant",
            "model": model,
            "content": content,
            "stop_reason": FINISH.reason(finish),
            "stop_sequence": null,
            "usage": Self::usage_body(&so_far.usage),
        })
    }

    /// Rebuilds the content, one block per piece: the provider's text block
    /// of that number with the text put in, else a new one; other content
    /// as it came.
    fn put(v: &mut Value, reply: &Reply) {
        if let Some(Start { id, model }) = &reply.start {
            replace(v, "/id", id.as_str());
            replace(v, "/model", model.as_str());
        }
        if let Some(f) = &reply.finish {
            FINISH.rewrite(v, "/stop_reason", f);
        }
        let old = v["content"].take();
        let mut content = Vec::new();
        for piece in &reply.content {
            match piece {
                Piece::Text { block, text } => {
                    let mut b = old
                        .get(*block as usize)
                        .cloned()
                        .filter(|b| b["type"] == "text");
                    let b = b.get_or_insert_with(|| json!({ "type": "text" }));
                    b["text"] = text.as_str().into();
                    content.push(b.clone());
                }
                Piece::Other { raw, .. } => content.push((**raw).clone()),
            }
        }
        v["content"] = content.into();
    }

    fn error_body(fault: &Fault) -> Value {
        let error = json!({ "type": fault.code(), "message": fault.message() });
        json!({ "type": "error", "error": merge_details(error, fault) })
    }
}

/// Puts an edited reply back into a stream event.
fn put_event(v: &mut Value, reply: &Reply) {
    // A block that opened with text a filter took opens empty.
    let text = reply
        .content
        .iter()
        .any(|p| matches!(p, Piece::Text { .. }));
    if v["type"] == "content_block_start" && !text {
        replace(v, "/content_block/text", "");
    }
    if let Some(Start { id, model }) = &reply.start {
        replace(v, "/message/id", id.as_str());
        replace(v, "/message/model", model.as_str());
    }
    if let Some(f) = &reply.finish {
        FINISH.rewrite(v, "/delta/stop_reason", f);
    }
    for piece in &reply.content {
        if let Piece::Text { text, .. } = piece {
            let at = match v["type"].as_str() {
                Some("content_block_start") => "/content_block/text",
                _ => "/delta/text",
            };
            replace(v, at, text.as_str());
        }
    }
}

/// Usage totals; [`units::INPUT_TOKENS`] includes the cache reads and
/// writes Anthropic counts apart.
fn usage_of(u: &Value) -> Usage {
    let n = |key: &str| u[key].as_f64();
    let mut usage = Usage::default();
    if let Some(input) = n("input_tokens") {
        let cached = n("cache_read_input_tokens").unwrap_or(0.0);
        let written = n("cache_creation_input_tokens").unwrap_or(0.0);
        usage.add(units::INPUT_TOKENS, input + cached + written);
        if cached > 0.0 {
            usage.add(units::CACHED_INPUT_TOKENS, cached);
        }
        if written > 0.0 {
            usage.add(units::CACHE_WRITE_INPUT_TOKENS, written);
        }
    }
    if let Some(output) = n("output_tokens") {
        usage.add(units::OUTPUT_TOKENS, output);
    }
    usage
}

impl Events<Reply> for Reading<AnthropicMessages> {
    fn event(&mut self, raw: Bytes, out: &mut Vec<Reply>) -> Result<(), Fault> {
        if self.done {
            return Ok(());
        }
        let Some(v) = event_json(&raw) else {
            out.push(unreadable_event(&self.name, raw));
            return Ok(());
        };
        if says_error(&v) || v["type"] == "error" {
            return Err(error_event(&self.name, &raw, &v));
        }
        let usage = match v["type"].as_str() {
            Some("message_start") => &v["message"]["usage"],
            Some("message_delta") => &v["usage"],
            // Its end, which the encoder writes.
            Some("message_stop") => {
                self.done = true;
                return Ok(());
            }
            _ => &Value::Null,
        };
        let mut reply = event_reply(&v);
        reply.usage = self.totals.increment(&usage_of(usage));
        out.push(reply.came(&self.name, raw));
        Ok(())
    }

    fn end(&mut self, _out: &mut Vec<Reply>) -> Result<(), Fault> {
        self.ended()
    }
}

/// What a stream event says: nothing for a ping, a block's end or an empty
/// text block's start.
fn event_reply(v: &Value) -> Reply {
    let block = v["index"]
        .as_u64()
        .and_then(|i| u32::try_from(i).ok())
        .unwrap_or(0);
    match v["type"].as_str() {
        Some("message_start") => {
            let message = &v["message"];
            let id = message["id"].as_str().unwrap_or_default();
            Reply::start(id, message["model"].as_str().unwrap_or_default())
        }
        Some("content_block_start") => {
            let content = &v["content_block"];
            match (content["type"].as_str(), content["text"].as_str()) {
                (Some("text"), None | Some("")) => Reply::default(),
                (Some("text"), Some(text)) => Reply::text(block, text),
                _ => Reply::other(ANTHROPIC, content.clone()),
            }
        }
        Some("content_block_delta") => {
            let delta = &v["delta"];
            match delta["type"].as_str() {
                Some("text_delta") => {
                    Reply::text(block, delta["text"].as_str().unwrap_or_default())
                }
                _ => Reply::other(ANTHROPIC, delta.clone()),
            }
        }
        Some("message_delta") => {
            let finish = v.pointer("/delta/stop_reason").and_then(Value::as_str);
            let finish = finish.map(|f| FINISH.finish(f));
            finish.map_or_else(Reply::default, Reply::finished)
        }
        Some("ping" | "content_block_stop") | None => Reply::default(),
        // An event type this format does not know may carry content.
        Some(_) => Reply::other(ANTHROPIC, v.clone()),
    }
}

/// Writes Anthropic events: the provider's as they came, with edits
/// written in, and others anew.
#[derive(Debug, Default)]
struct EventStream {
    so_far: SoFar,
    /// Whether `message_start` is written, at the first content.
    started: bool,
    /// The open block and its index, numbered in the order blocks open.
    open: Option<(u32, u32)>,
    opened: u32,
    /// The provider's open text block and its stop, kept back until the
    /// next event so released text can still go in.
    provider_open: Option<(u32, Option<Bytes>)>,
    /// Text for a block the provider has not opened: it goes in when it
    /// opens, or else into a new block at the end.
    waiting: Vec<(u32, String)>,
    /// The provider's events from `message_delta` on.
    held: Option<BytesMut>,
    /// The provider's events before its `message_start`, or that say
    /// nothing after a stop kept back: relayed once that goes out.
    later: Vec<Bytes>,
    /// Stops of the provider's blocks not yet opened, waiting for them.
    parked: Vec<(u32, Bytes)>,
}

impl EventStream {
    fn new(relayed: bool) -> Self {
        let so_far = SoFar {
            relayed,
            ..SoFar::default()
        };
        Self {
            so_far,
            ..Self::default()
        }
    }

    /// Writes `message_start` once.
    fn open(&mut self, out: &mut BytesMut) {
        if std::mem::replace(&mut self.started, true) {
            return;
        }
        let (id, model) = self.so_far.start.clone().unwrap_or_default();
        let message = json!({
            "id": id,
            "type": "message",
            "role": "assistant",
            "model": model,
            "content": [],
            "stop_reason": null,
            "stop_sequence": null,
            // What is known so far.
            "usage": AnthropicMessages::usage_body(&self.so_far.usage),
        });
        emit(out, json!({ "type": "message_start", "message": message }));
        self.relay_later(out);
    }

    fn relay_later(&mut self, out: &mut BytesMut) {
        for raw in std::mem::take(&mut self.later) {
            self.relay(&raw, out);
        }
    }

    /// Closes the provider's block, if its stop was kept back.
    fn close_provider_block(&mut self, out: &mut BytesMut) {
        if let Some((_, Some(stop))) = self.provider_open.take_if(|(_, stop)| stop.is_some()) {
            out.extend_from_slice(&stop);
            self.relay_later(out);
        }
    }

    fn close_block(&mut self, out: &mut BytesMut) {
        if let Some((_, index)) = self.open.take() {
            emit(out, json!({ "type": "content_block_stop", "index": index }));
        }
    }

    /// Text of `block`, opening a new block at the next index if needed.
    fn text(&mut self, block: u32, text: &str, out: &mut BytesMut) {
        let index = match self.open {
            Some((open, index)) if open == block => index,
            _ => {
                self.close_block(out);
                let index = self.opened;
                self.opened += 1;
                self.open = Some((block, index));
                let start = json!({
                    "type": "content_block_start",
                    "index": index,
                    "content_block": { "type": "text", "text": "" },
                });
                emit(out, start);
                index
            }
        };
        delta(index, text, out);
    }

    /// Relay the provider's event `raw`.
    fn relay(&mut self, raw: &Bytes, out: &mut BytesMut) {
        let v = event_json(raw).unwrap_or_default();
        let name = v["type"].as_str();
        match name {
            Some("message_start") => self.started = true,
            Some("message_delta") => _ = self.held.get_or_insert_default(),
            _ => {}
        }
        if let Some(held) = &mut self.held {
            return held.extend_from_slice(raw);
        }
        // What is no content goes where it came.
        let quiet = matches!(name, None | Some("ping"));
        if !self.started && !quiet {
            return self.later.push(raw.clone());
        }
        let index = v["index"].as_u64().and_then(|i| u32::try_from(i).ok());
        // A released delta goes into its block, before the stop kept back.
        if let (Some("content_block_delta"), Some((open, Some(_)))) = (name, &self.provider_open)
            && index == Some(*open)
        {
            return out.extend_from_slice(raw);
        }
        let no_content = quiet
            || (name == Some("content_block_start")
                && v["content_block"]["type"] == "text"
                && v["content_block"]["text"]
                    .as_str()
                    .is_none_or(str::is_empty));
        if no_content && matches!(self.provider_open, Some((_, Some(_)))) {
            return self.later.push(raw.clone());
        }
        self.close_provider_block(out);
        match (name, index) {
            // Its stop is kept back until its next event.
            (Some("content_block_stop"), Some(i)) if self.provider_open == Some((i, None)) => {
                self.provider_open = Some((i, Some(raw.clone())));
            }
            (Some("content_block_stop"), Some(i)) if i >= self.opened => {
                self.parked.push((i, raw.clone()));
            }
            (Some("content_block_start"), Some(i)) => {
                out.extend_from_slice(raw);
                self.opened = self.opened.max(i.saturating_add(1));
                let stop = self.parked.iter().position(|(block, _)| *block == i);
                let stop = stop.map(|at| self.parked.remove(at).1);
                // Written text waiting for this text block goes in first.
                if v["content_block"]["type"] == "text" {
                    self.provider_open = Some((i, stop));
                    let now = self
                        .waiting
                        .iter()
                        .take_while(|(block, _)| *block == i)
                        .count();
                    for (_, text) in self.waiting.drain(..now) {
                        delta(i, &text, out);
                    }
                } else if let Some(stop) = stop {
                    out.extend_from_slice(&stop);
                }
            }
            (Some("message_start"), _) => {
                out.extend_from_slice(raw);
                self.relay_later(out);
            }
            _ => out.extend_from_slice(raw),
        }
    }

    /// Write text the provider did not send, in a new stream or among its
    /// events; other content is an `Err`.
    fn write(&mut self, reply: &Reply, out: &mut BytesMut) -> Result<(), Fault> {
        for piece in &reply.content {
            match piece {
                Piece::Text { text, .. } if text.is_empty() => {}
                Piece::Text { block, text } => match &self.provider_open {
                    // Text waits behind waiting text, to keep order.
                    Some((open, _)) if open == block && self.waiting.is_empty() => {
                        delta(*block, text, out)
                    }
                    _ if self.so_far.relayed => self.waiting.push((*block, text.clone())),
                    _ => {
                        self.open(out);
                        self.text(*block, text, out);
                    }
                },
                // A provider block released has lost its position.
                Piece::Other { .. } if self.so_far.relayed => {
                    return Err(unpatchable(
                        "Anthropic's own blocks other than text cannot be held",
                    ));
                }
                Piece::Other { .. } => return Err(unwritable_content()),
            }
        }
        Ok(())
    }
}

fn delta(index: u32, text: &str, out: &mut BytesMut) {
    let delta = json!({ "type": "text_delta", "text": text });
    let v = json!({ "type": "content_block_delta", "index": index, "delta": delta });
    emit(out, v);
}

/// Write the event `v`, named by its type.
fn emit(out: &mut BytesMut, v: Value) {
    out.extend_from_slice(event(v["type"].as_str(), &v).as_bytes());
}

/// Whether the event `v` carries `piece`: text in a text block's start or
/// delta of its index, other content in another block's.
fn holds(v: &Value, piece: &Piece) -> bool {
    let block = matches!(
        v["type"].as_str(),
        Some("content_block_start" | "content_block_delta")
    );
    let text = v["content_block"]["type"] == "text" || v["delta"]["type"] == "text_delta";
    match piece {
        Piece::Text { block: b, .. } => block && text && v["index"].as_u64() == Some(u64::from(*b)),
        Piece::Other { .. } => block && !text,
    }
}

/// An event `raw`, which said `said`, with its edited reply put back;
/// `None` if nothing is left, unless it opens a message or a block.
fn rebuild(raw: &Bytes, said: &Reply, reply: &Reply) -> Result<Option<Bytes>, Fault> {
    let mut v = event_json(raw).unwrap_or_default();
    let opens = matches!(
        v["type"].as_str(),
        Some("message_start" | "content_block_start")
    );
    if !opens && reply.content.is_empty() && reply.finish.is_none() {
        return Ok(None);
    }
    put_event(&mut v, reply);
    if reply.usage != said.usage && v["usage"].is_object() {
        v["usage"] = AnthropicMessages::usage_body(&reply.usage);
    }
    reads_back::<AnthropicMessages>(reply, &event_reply(&v))?;
    // It read back as an event, so it has its type.
    let name = v["type"].as_str().unwrap_or_default().to_owned();
    Ok(Some(event(Some(&name), &v).into()))
}

impl Encoder<Reply, ()> for EventStream {
    fn head(&mut self, _first: Option<&Reply>) {}

    fn encode(&mut self, reply: &Reply, out: &mut BytesMut) -> Result<(), Fault> {
        self.so_far.note(reply);
        let own = reply.origin(ANTHROPIC).filter(|_| self.so_far.relayed);
        let Some(own) = own else {
            return self.write(reply, out);
        };
        if own.said == *reply {
            self.relay(&own.bytes, out);
            return Ok(());
        }
        // What the event has no place for is written anew around it; only
        // `message_start` carries a start.
        let v = event_json(&own.bytes).unwrap_or_default();
        let (ahead, mut inside, after) =
            split_around(reply, |p| holds(&v, p), v["type"] == "message_delta");
        if v["type"] != "message_start" {
            inside.start = None;
        }
        let mut all = BytesMut::new();
        self.write(&ahead, &mut all)?;
        if let Some(bytes) = rebuild(&own.bytes, &own.said, &inside)? {
            self.relay(&bytes, &mut all);
        }
        self.write(&after, &mut all)?;
        out.extend_from_slice(&all);
        Ok(())
    }

    fn end(&mut self, end: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault> {
        self.close_provider_block(out);
        let held = self.held.take();
        if let Err(f) = end {
            out.extend_from_slice(&held.unwrap_or_default());
            let body = match f.answer::<Bytes>(ANTHROPIC) {
                Some(raw) => String::from_utf8_lossy(raw).into_owned(),
                None => AnthropicMessages::error_body(f).to_string(),
            };
            out.extend_from_slice(event(Some("error"), &body).as_bytes());
            return Ok(());
        }
        self.open(out);
        for (block, text) in std::mem::take(&mut self.waiting) {
            self.text(block, &text, out);
        }
        self.close_block(out);
        // The provider's message_delta, unless a filter took its finish.
        match held {
            Some(held) if !held.is_empty() => out.extend_from_slice(&held),
            _ => {
                let finish = self.so_far.finish.as_ref().unwrap_or(&Finish::Stop);
                let reason = FINISH.reason(finish);
                let delta = json!({ "stop_reason": reason, "stop_sequence": null });
                let usage = AnthropicMessages::usage_body(&self.so_far.usage);
                let v = json!({ "type": "message_delta", "delta": delta, "usage": usage });
                emit(out, v);
            }
        }
        emit(out, json!({ "type": "message_stop" }));
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    /// The rebuilt event, or empty bytes.
    fn rebuilt(raw: &Bytes, reply: &Reply) -> Result<Bytes, Fault> {
        let said = event_reply(&event_json(raw).unwrap_or_default());
        rebuild(raw, &said, reply).map(Option::unwrap_or_default)
    }
    use crate::common::patch_request;

    /// The replies of `events`, each decoded apart.
    fn decode(events: &[&str]) -> Vec<Reply> {
        let mut reader = Reading::<AnthropicMessages>::new();
        let mut out = Vec::new();
        for e in events {
            let _ = reader.event(Bytes::from(e.to_string()), &mut out);
        }
        out
    }

    fn patched(body: Value, edit: impl FnOnce(&mut Conversation)) -> Result<Value, Fault> {
        let before = AnthropicMessages::read(&body);
        let mut chat = before.clone();
        edit(&mut chat);
        let raw = Bytes::from(body.to_string());
        Ok(patch_request::<AnthropicMessages>(&raw, &before, &chat)?.into())
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
        let chat = AnthropicMessages::read(&body);
        assert_eq!(
            AnthropicMessages::write(&chat).ok(),
            Some(json!({
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
        assert_eq!(patched(body.clone(), |_| {}).ok(), Some(body.clone()));
        let redacted = patched(body.clone(), |c| c.user = Some("[x]".into()));
        let metadata = redacted.ok().map(|v| v["metadata"].clone());
        assert_eq!(metadata, Some(json!({ "user_id": "[x]" })));
        let removed = patched(body, |c| c.user = None);
        assert_eq!(removed.ok().map(|v| v.get("metadata").cloned()), Some(None));
    }

    #[test]
    fn input_usage_counts_cache_reads_and_writes() {
        let u = usage_of(&json!({
            "input_tokens": 3,
            "cache_read_input_tokens": 10,
            "cache_creation_input_tokens": 2,
            "output_tokens": 1,
        }));
        assert_eq!(u.get(units::INPUT_TOKENS), Some(15.0));
        assert_eq!(u.get(units::CACHED_INPUT_TOKENS), Some(10.0));
        assert_eq!(u.get(units::OUTPUT_TOKENS), Some(1.0));
    }

    #[test]
    fn stream_usage_is_counted_once_and_thinking_is_other() {
        let out = decode(&[
            "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m\",\"model\":\"c\",\"usage\":{\"input_tokens\":5,\"output_tokens\":1}}}\n\n",
            "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"hm\"}}\n\n",
            "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"input_tokens\":5,\"output_tokens\":7}}\n\n",
        ]);
        let mut total = Usage::default();
        for r in &out {
            total += &r.usage;
        }
        assert_eq!(total.get(units::INPUT_TOKENS), Some(5.0));
        assert_eq!(total.get(units::OUTPUT_TOKENS), Some(7.0));
        assert!(matches!(&out[1].content[..], [Piece::Other { .. }]));
    }

    #[test]
    fn an_event_it_cannot_read_is_content_filters_see() {
        let out = decode(&[
            "event: future\ndata: {\"type\":\"future\",\"text\":\"secret\"}\n\n",
            "data: not json\n\n",
            "event: ping\ndata: {\"type\":\"ping\"}\n\n",
            ": a comment\n\n",
        ]);
        let other = |r: &Reply| matches!(&r.content[..], [Piece::Other { .. }]);
        assert!(other(&out[0]) && other(&out[1]));
        // What is no content is still a reply, which keeps its bytes (N4).
        assert!(
            out[2..]
                .iter()
                .all(|r| !r.is_content() && r.origin(ANTHROPIC).is_some())
        );
    }

    #[test]
    fn text_a_block_opens_with_is_text() {
        let e = "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"Hi \"}}\n\n";
        assert_eq!(decode(&[e]), [Reply::text(0, "Hi ")]);
        let raw = Bytes::from_static(e.as_bytes());
        let edited = rebuilt(&raw, &Reply::text(0, "HI "));
        assert!(edited.is_ok_and(|b| String::from_utf8_lossy(&b).contains("\"text\":\"HI \"")));
    }

    #[test]
    fn a_system_message_goes_in_the_system_prompt_only_before_the_conversation() {
        let mut chat = Conversation {
            messages: vec![
                Message::new(Role::System, "be brief"),
                Message::new(Role::User, "hi"),
            ],
            ..Conversation::default()
        };
        let body = AnthropicMessages::write(&chat).ok();
        assert_eq!(body.map(|v| v["system"].clone()), Some(json!("be brief")));
        chat.messages
            .push(Message::new(Role::System, "now be long"));
        assert!(AnthropicMessages::write(&chat).is_err());
    }

    #[test]
    fn a_temperature_above_1_is_refused() {
        let mut chat = Conversation {
            messages: vec![Message::new(Role::User, "hi")],
            temperature: Some(1.5),
            ..Conversation::default()
        };
        let refused = AnthropicMessages::write(&chat);
        assert!(refused.is_err_and(|f| f.status() == 400));
        chat.temperature = Some(1.0);
        let body = AnthropicMessages::write(&chat).ok();
        assert_eq!(body.map(|v| v["temperature"].clone()), Some(json!(1.0)));
    }

    #[test]
    fn an_empty_turn_but_the_last_is_dropped() {
        let chat = Conversation {
            messages: vec![
                Message::new(Role::User, "hi"),
                Message::new(Role::Assistant, ""),
                Message::new(Role::User, ""),
                Message::new(Role::User, "again"),
                Message::new(Role::Assistant, ""),
            ],
            ..Conversation::default()
        };
        let body = AnthropicMessages::write(&chat).ok();
        assert_eq!(
            body.map(|v| v["messages"].clone()),
            Some(json!([
                { "role": "user", "content": "hi" },
                { "role": "user", "content": "again" },
                { "role": "assistant", "content": "" },
            ]))
        );
    }

    #[test]
    fn a_request_of_only_system_messages_is_refused() {
        let chat = Conversation {
            messages: vec![Message::new(Role::System, "be brief")],
            ..Conversation::default()
        };
        let refused = AnthropicMessages::write(&chat);
        assert!(refused.is_err_and(|f| f.message().contains("at least one user or assistant")));
    }

    #[test]
    fn an_edit_keeps_a_system_prompt_with_cache_markers() {
        let system = json!([{ "type": "text", "text": "be brief", "cache_control": { "type": "ephemeral" } }]);
        let body = json!({
            "model": "c", "system": system.clone(),
            "messages": [{ "role": "user", "content": "hi" }],
        });
        // Message 0 is the system prompt.
        let edited = patched(body.clone(), |c| {
            c.messages[1] = Message::new(Role::User, "HI")
        });
        let mut expected = body;
        expected["messages"][0]["content"] = "HI".into();
        // An edited body gains no `max_tokens`: only what was edited changes.
        assert_eq!(edited.ok(), Some(expected));
    }

    #[test]
    fn a_system_prompt_redacted_beside_an_inserted_one_keeps_its_cache_marker() {
        let cached =
            json!({ "type": "text", "text": "SECRET", "cache_control": { "type": "ephemeral" } });
        let body = json!({
            "model": "c",
            "system": [{ "type": "text", "text": "You are Claude Code" }, cached],
            "messages": [{ "role": "user", "content": "hi" }],
        });
        let edited = patched(body, |c| {
            if let Some(Content::Text { text }) = c.messages[0].content.get_mut(1) {
                *text = "[x]".into();
            }
            c.messages.insert(0, Message::new(Role::System, "gw"));
        });
        let system = edited.ok().map(|v| v["system"].clone());
        assert_eq!(
            system,
            Some(json!([
                { "type": "text", "text": "gw" },
                { "type": "text", "text": "You are Claude Code" },
                { "type": "text", "text": "[x]", "cache_control": { "type": "ephemeral" } },
            ]))
        );
    }

    #[test]
    fn an_unchanged_part_keeps_its_marker_beside_an_edit_and_an_append() {
        let marker = json!({ "type": "ephemeral" });
        let body = json!({ "model": "c", "messages": [], "system": [
            { "type": "text", "text": "SECRET a" },
            { "type": "text", "text": "b", "cache_control": marker },
        ]});
        let edited = patched(body, |c| {
            c.messages[0].content[0] = "[x] a".into();
            c.messages[0].content.push("gateway note".into());
        });
        assert_eq!(
            edited.map(|v| v["system"].clone()).ok(),
            Some(json!([
                { "type": "text", "text": "[x] a" },
                { "type": "text", "text": "b", "cache_control": marker },
                { "type": "text", "text": "gateway note" },
            ]))
        );
    }

    #[test]
    fn a_whole_response_decodes_into_a_piece_per_block() {
        let v = json!({
            "id": "m", "model": "c", "stop_reason": "tool_use",
            "content": [
                { "type": "text", "text": "ok" },
                { "type": "tool_use", "id": "t", "name": "f", "input": {} },
            ],
        });
        let mut whole = Reply::start("m", "c");
        whole.content = vec![
            Piece::text(0, "ok"),
            Piece::other(
                ANTHROPIC,
                json!({ "type": "tool_use", "id": "t", "name": "f", "input": {} }),
            ),
        ];
        whole.finish = Some(Finish::ToolUse);
        assert_eq!(AnthropicMessages::reply(&v), whole);
        let mut patched = v.clone();
        let mut edited = AnthropicMessages::reply(&v);
        edited.content[0] = Piece::text(0, "OK");
        AnthropicMessages::put(&mut patched, &edited);
        assert_eq!(patched["content"][0]["text"], "OK");
        assert_eq!(patched["content"][1]["type"], "tool_use");
        // Text of a block that is not there is a block of its own.
        edited.content.push(Piece::text(9, "x"));
        AnthropicMessages::put(&mut patched, &edited);
        assert_eq!(
            patched["content"][2],
            json!({ "type": "text", "text": "x" })
        );
    }

    #[test]
    fn an_edited_message_keeps_its_cache_marker() {
        let cached = json!([{ "type": "text", "text": "key sk-1", "cache_control": { "type": "ephemeral" } }]);
        let body = json!({ "model": "c", "max_tokens": 5, "messages": [
            { "role": "user", "content": cached },
        ]});
        let edited = patched(body, |c| c.messages[0].content = vec!["key [x]".into()]);
        assert_eq!(
            edited.map(|v| v["messages"][0]["content"].clone()).ok(),
            Some(
                json!([{ "type": "text", "text": "key [x]", "cache_control": { "type": "ephemeral" } }])
            )
        );
    }

    #[test]
    fn usage_is_written_as_it_was_read() {
        let reported = json!({
            "input_tokens": 3,
            "cache_read_input_tokens": 10,
            "cache_creation_input_tokens": 2,
            "output_tokens": 1,
        });
        assert_eq!(
            AnthropicMessages::usage_body(&usage_of(&reported)),
            reported
        );
    }

    #[test]
    fn an_unchanged_finish_keeps_the_providers_reason() {
        let mut v = json!({ "stop_reason": "stop_sequence", "stop_sequence": "X" });
        AnthropicMessages::put(&mut v, &Reply::finished(Finish::Stop));
        assert_eq!(v["stop_reason"], "stop_sequence");
        AnthropicMessages::put(&mut v, &Reply::finished(Finish::Length));
        assert_eq!(v["stop_reason"], "max_tokens");
    }

    #[test]
    fn blocks_are_numbered_in_the_order_they_open() {
        let mut enc = EventStream::new(false);
        let mut out = BytesMut::new();
        assert!(enc.encode(&Reply::text(5, "hi"), &mut out).is_ok());
        assert!(enc.end(&Ok(()), &mut out).is_ok());
        let text = String::from_utf8_lossy(&out);
        assert!(text.contains("\"index\":0"), "{text}");
        assert!(!text.contains("\"index\":5"), "{text}");
    }

    #[test]
    fn a_tool_call_a_filter_turns_to_text_is_gone() {
        let raw = Bytes::from(
            json!({ "id": "m", "model": "c", "stop_reason": "tool_use", "content": [
                { "type": "tool_use", "id": "a", "name": "rm_rf", "input": {} },
            ]})
            .to_string(),
        );
        let v: Value = serde_json::from_slice(&raw).unwrap_or_default();
        let said = AnthropicMessages::reply(&v);
        let mut edited = said.clone();
        edited.content = vec![Piece::text(0, "[removed]")];
        let rebuilt = crate::common::rebuild_whole::<AnthropicMessages>(&raw, &said, &edited);
        let rebuilt: Value = rebuilt.map_or(Value::Null, |b| {
            serde_json::from_slice(&b).unwrap_or_default()
        });
        assert_eq!(
            rebuilt["content"],
            json!([{ "type": "text", "text": "[removed]" }])
        );
    }

    #[test]
    fn a_text_block_with_citations_is_other() {
        let cited = json!({ "type": "text", "text": "Paris", "citations": [{ "type": "web" }] });
        let v = json!({ "id": "m", "model": "c", "content": [cited, { "type": "text", "text": "!", "citations": null }] });
        assert_eq!(
            AnthropicMessages::reply(&v).content,
            [Piece::other(ANTHROPIC, cited), Piece::text(1, "!")]
        );
    }

    #[test]
    fn a_stream_event_edited_into_what_it_has_no_place_for_fails() {
        let raw = Bytes::from_static(
            b"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"t\",\"name\":\"rm_rf\",\"input\":{}}}\n\n",
        );
        assert!(rebuilt(&raw, &Reply::text(0, "[removed]")).is_err());
        let text = Bytes::from_static(
            b"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\n",
        );
        let out = rebuilt(&text, &Reply::text(0, "HI")).unwrap_or_default();
        assert!(String::from_utf8_lossy(&out).contains("\"text\":\"HI\""));
    }

    #[test]
    fn a_text_part_that_says_more_is_not_plain_text() {
        let cached =
            json!([{ "type": "text", "text": "hi", "cache_control": { "type": "ephemeral" } }]);
        assert_eq!(read_content(&cached), [Content::from("hi")]);
        let cited = json!({ "type": "text", "text": "hi", "citations": [{ "type": "x" }] });
        assert_eq!(
            read_content(&json!([cited.clone()])),
            [Content::other(cited)]
        );
    }

    #[test]
    fn a_system_prompt_of_several_blocks_is_edited_block_by_block() {
        let marker = json!({ "type": "ephemeral" });
        let body = json!({ "model": "c", "max_tokens": 5, "messages": [], "system": [
            { "type": "text", "text": "You are Claude Code." },
            { "type": "text", "text": "SECRET rules", "cache_control": marker },
        ]});
        let edited = patched(body, |c| {
            c.messages[0].content[1] = "[redacted] rules".into();
        });
        assert_eq!(
            edited.map(|v| v["system"].clone()).ok(),
            Some(json!([
                { "type": "text", "text": "You are Claude Code." },
                { "type": "text", "text": "[redacted] rules", "cache_control": marker },
            ]))
        );
        // An empty prompt can be filled.
        let empty = json!({ "model": "c", "max_tokens": 5, "messages": [], "system": [] });
        let filled = patched(empty, |c| {
            c.messages.insert(0, Message::new(Role::System, "be kind"))
        });
        assert_eq!(
            filled.map(|v| v["system"].clone()).ok(),
            Some(json!([{ "type": "text", "text": "be kind" }]))
        );
    }

    #[test]
    fn a_removed_message_takes_its_cache_marker_with_it() {
        let marker = json!({ "type": "ephemeral" });
        let body = json!({ "model": "c", "max_tokens": 5, "messages": [
            { "role": "user", "content": "A" },
            { "role": "assistant", "content": [{ "type": "text", "text": "B", "cache_control": marker }] },
            { "role": "user", "content": "C" },
        ]});
        let edited = patched(body, |c| {
            c.messages.remove(1);
            c.messages[1].content = vec!["C-redacted".into()];
        });
        assert_eq!(
            edited.map(|v| v["messages"].clone()).ok(),
            Some(json!([
                { "role": "user", "content": "A" },
                { "role": "user", "content": "C-redacted" },
            ]))
        );
    }

    #[test]
    fn a_redacted_message_keeps_its_marker_beside_an_inserted_one() {
        let marker = json!({ "type": "ephemeral" });
        let body = json!({ "model": "c", "max_tokens": 5, "messages": [
            { "role": "user", "content": [{ "type": "text", "text": "SECRET", "cache_control": marker }] },
            { "role": "assistant", "content": "B" },
        ]});
        let edited = patched(body, |c| {
            c.messages[0].content = vec!["[redacted]".into()];
            c.messages.insert(1, Message::new(Role::User, "and"));
        });
        assert_eq!(
            edited.map(|v| v["messages"].clone()).ok(),
            Some(json!([
                { "role": "user", "content": [{ "type": "text", "text": "[redacted]", "cache_control": marker }] },
                { "role": "user", "content": "and" },
                { "role": "assistant", "content": "B" },
            ]))
        );
    }

    #[test]
    fn an_empty_answer_has_no_empty_text_block() {
        let mut whole = crate::common::SoFar::default();
        let mut empty = Reply::start("m", "c");
        empty.content = vec![Piece::text(0, "")];
        empty.finish = Some(Finish::Length);
        assert!(whole.add(&empty).is_ok());
        let v = AnthropicMessages::render(&whole);
        assert_eq!(v["content"], json!([]));
    }

    // D4: a delta a filter held and released goes into its block, before
    // the block's stop, which is kept back.
    #[test]
    fn a_released_delta_goes_before_its_blocks_stop() {
        let mut enc = EventStream::new(true);
        let mut out = BytesMut::new();
        let events = [
            "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m\",\"model\":\"c\"}}\n\n",
            "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
            "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
            "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"late\"}}\n\n",
        ];
        for reply in decode(&events) {
            assert!(enc.encode(&reply, &mut out).is_ok());
        }
        let out = String::from_utf8_lossy(&out).into_owned();
        let (late, stop) = (out.find("late"), out.find("content_block_stop"));
        assert!(late.is_some() && stop.is_none(), "{out}");
    }
}
