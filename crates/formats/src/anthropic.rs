//! Anthropic Messages: `POST /v1/messages`, JSON or SSE, of the
//! conversation kind.

use bytes::{Bytes, BytesMut};
use http::{HeaderValue, StatusCode};
use respan_gateway_core::{ClientView, Decoder, Doc, Encoder, Fault, Format, Item, Part, Usage};
use respan_gateway_core::{bytes, http, serde_json};
use serde_json::{Map, Value, json};

use crate::common::{
    Names, SoFar, StreamEncoder, StreamWriter, Totals, UnaryEncoder, Wire, decode_request,
    decoder_for, encode_request, json_model, made_head, merge_details, pass_headers, patch_content,
    patch_messages, patch_sampling, post_json, provider_error, read_content, read_message,
    reads_back, replace, says, says_error, set, stop_json, unary_decoder, unless_taken,
    unpatchable, unreadable_event, unsupported_fields, unwritable_content, write_content,
    write_message, write_model,
};
use crate::conversation::{Content, Conversation, Finish, Message, Reply, Role, units};
use crate::sse::{SseDecoder, event, event_json, is_stream};

/// The API version sent when the client names none. Headers a provider's
/// transport adds override it.
const VERSION: &str = "2023-06-01";
const VERSION_HEADER: &str = "anthropic-version";

/// The `max_tokens` of a translated request without one, as Anthropic
/// requires it.
const DEFAULT_MAX_TOKENS: u64 = 4096;

const FINISH: Names<Finish> = Names(&[
    ("end_turn", Finish::Stop),
    ("stop_sequence", Finish::Stop),
    ("max_tokens", Finish::Length),
    ("tool_use", Finish::ToolUse),
    ("refusal", Finish::Filtered),
]);

const ANTHROPIC: &str = "anthropic-messages";

/// Anthropic Messages, `POST /v1/messages`, named `anthropic-messages`.
/// It accepts a client's `POST` to any path ending in `/v1/messages`.
/// As with Anthropic's SDKs, the provider's base URL leaves out the
/// version, such as `https://api.anthropic.com`. It sends
/// `anthropic-version: 2023-06-01` if none is set.
#[derive(Debug, Clone, Copy, Default)]
pub struct AnthropicMessages;

impl Format for AnthropicMessages {
    type Input = Conversation;
    type Output = Reply;

    fn name(&self) -> &str {
        ANTHROPIC
    }

    fn accepts(&self, head: &http::request::Parts) -> bool {
        crate::posts_to(head, "/v1/messages")
    }

    fn model(&self, _head: &http::request::Parts, body: &Doc) -> Option<String> {
        json_model(body)
    }

    fn decode(&self, _head: &http::request::Parts, body: &Doc) -> Result<Conversation, Fault> {
        decode_request::<Self>(body)
    }

    fn encode(
        &self,
        chat: &Conversation,
        original: Option<(&Doc, &Conversation)>,
    ) -> Result<Doc, Fault> {
        encode_request::<Self>(chat, original)
    }

    fn prepare(&self, body: Doc, client: ClientView<'_>) -> Result<http::Request<Doc>, Fault> {
        let mut req = post_json(body, "/v1/messages", client)?;
        // `anthropic-` headers carry the API version and beta features.
        pass_headers(&mut req, client);
        let version = HeaderValue::from_static(VERSION);
        req.headers_mut().entry(VERSION_HEADER).or_insert(version);
        write_model(req.body_mut(), client.model())?;
        Ok(req)
    }

    fn decoder(&self, head: &http::response::Parts, client: ClientView<'_>) -> Box<dyn Decoder> {
        if !is_stream(head) {
            return decoder_for(head, client, unary_decoder::<Self>(head.status));
        }
        let mut totals = Totals::default();
        let events = SseDecoder::new(move |raw, out: &mut Vec<Part>| {
            decode_event(&mut totals, raw, out);
        });
        decoder_for(head, client, Box::new(events))
    }

    fn encoder(
        &self,
        head: &mut http::response::Parts,
        client: ClientView<'_>,
        relayed: bool,
    ) -> Box<dyn Encoder> {
        let relayed = made_head(head, client, relayed);
        if is_stream(head) {
            Box::new(StreamEncoder::new(relayed, EventStream::default()))
        } else {
            Box::new(UnaryEncoder::<Self>::new(head))
        }
    }
}

/// Keys a conversation has a place for. `metadata` holds only a user id,
/// which does not change the output.
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

/// The system prompt, as the parts of the leading system messages, and the
/// rest. Anthropic has no place for a later system message.
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

/// The system prompt of `after`'s leading system messages. The client's,
/// decoded from `system`, is patched in its form so its cache markers stay;
/// others, such as one a plugin added, become new parts beside it.
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

impl Wire for AnthropicMessages {
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
        let mut v = Map::new();
        let max_tokens = chat.max_tokens.unwrap_or(DEFAULT_MAX_TOKENS);
        v.insert("max_tokens".into(), max_tokens.into());
        set(&mut v, "stream", chat.stream.then_some(true.into()));
        set(
            &mut v,
            "system",
            (!system.is_empty()).then(|| write_content(&system)),
        );
        let messages = rest.iter().map(|m| write_message(m, &ROLES));
        let messages: Vec<Value> = messages.collect::<Result<_, _>>()?;
        v.insert("messages".into(), messages.into());
        set(&mut v, "temperature", chat.temperature.map(Into::into));
        set(&mut v, "top_p", chat.top_p.map(Into::into));
        set(&mut v, "stop_sequences", stop_json(&chat.stop));
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
        if before.messages != after.messages {
            let messages = patch_messages(&before.messages, rest, &ROLES)?;
            v.insert("messages".into(), messages.into());
        }
        Ok(())
    }

    fn is_response(v: &Value) -> bool {
        v["type"] == "message"
    }

    /// One event per content block.
    fn replies(v: &Value) -> Vec<Reply> {
        let mut events = vec![Reply::start(
            v["id"].as_str().unwrap_or_default(),
            v["model"].as_str().unwrap_or_default(),
        )];
        let blocks = v["content"].as_array().into_iter().flatten();
        for (i, b) in blocks.enumerate() {
            // A text block with more, such as citations, is other content,
            // so translating it is refused rather than lossy.
            let more = |(k, x): (&String, &Value)| k != "type" && k != "text" && says(x);
            let plain = !b.as_object().into_iter().flatten().any(more);
            events.push(match (b["type"].as_str(), b["text"].as_str()) {
                (Some("text"), Some(text)) if plain => {
                    Reply::text(u32::try_from(i).unwrap_or(u32::MAX), text)
                }
                _ => Reply::other(ANTHROPIC, b.clone()),
            });
        }
        let finish = v["stop_reason"].as_str();
        events.extend(finish.map(|r| Reply::Finish(FINISH.finish(r))));
        events
    }

    fn usage(v: &Value) -> Usage {
        usage_of(&v["usage"])
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
            "usage": usage_body(&so_far.usage),
        })
    }

    /// Rebuild the content from the events, one block each, in order: the
    /// provider's text block of that number with the text put in, new text
    /// as a new block, and other content as it came.
    fn put(v: &mut Value, events: &[Reply]) {
        let old = v["content"].take();
        let mut content = Vec::new();
        for e in events {
            match e {
                Reply::Start { id, model } => {
                    replace(v, "/id", id.as_str());
                    replace(v, "/model", model.as_str());
                }
                Reply::Finish(f) => FINISH.rewrite(v, "/stop_reason", f),
                Reply::Text { block, text } => {
                    let mut b = old
                        .get(*block as usize)
                        .cloned()
                        .filter(|b| b["type"] == "text");
                    let b = b.get_or_insert_with(|| json!({ "type": "text" }));
                    b["text"] = text.as_str().into();
                    content.push(b.clone());
                }
                Reply::Other { raw, .. } => content.push((**raw).clone()),
            }
        }
        v["content"] = content.into();
    }

    fn error_body(fault: &Fault) -> Value {
        let error = json!({ "type": fault.code(), "message": fault.message() });
        json!({ "type": "error", "error": merge_details(error, fault) })
    }
}

/// Put edited events back into a stream event.
fn put_event(v: &mut Value, events: &[Reply]) {
    // A block that opened with text a filter took opens empty.
    if v["type"] == "content_block_start" && !events.iter().any(|e| matches!(e, Reply::Text { .. }))
    {
        replace(v, "/content_block/text", "");
    }
    for e in events {
        match e {
            Reply::Start { id, model } => {
                replace(v, "/message/id", id.as_str());
                replace(v, "/message/model", model.as_str());
            }
            Reply::Finish(f) => FINISH.rewrite(v, "/delta/stop_reason", f),
            Reply::Text { text, .. } => {
                let at = match v["type"].as_str() {
                    Some("content_block_start") => "/content_block/text",
                    _ => "/delta/text",
                };
                replace(v, at, text.as_str());
            }
            Reply::Other { .. } => {}
        }
    }
}

/// Usage totals. Anthropic counts cache reads and writes apart from
/// `input_tokens`; [`units::INPUT_TOKENS`] includes them.
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

/// Usage as Anthropic reports it: the inverse of [`usage_of`].
fn usage_body(u: &Usage) -> Value {
    let cached = u.get(units::CACHED_INPUT_TOKENS);
    let written = u.get(units::CACHE_WRITE_INPUT_TOKENS);
    let mut body = json!({
        "input_tokens": (u.get(units::INPUT_TOKENS) - cached - written) as u64,
        "output_tokens": u.get(units::OUTPUT_TOKENS) as u64,
    });
    if cached > 0.0 {
        body["cache_read_input_tokens"] = json!(cached as u64);
    }
    if written > 0.0 {
        body["cache_creation_input_tokens"] = json!(written as u64);
    }
    body
}

fn decode_event(totals: &mut Totals, raw: Bytes, out: &mut Vec<Part>) {
    let Some(v) = event_json(&raw) else {
        return out.push(unreadable_event(ANTHROPIC, raw));
    };
    if says_error(&v) || v["type"] == "error" {
        return out.push(Part::End(Err(provider_error(StatusCode::OK, &v))));
    }
    let usage = match v["type"].as_str() {
        Some("message_start") => &v["message"]["usage"],
        Some("message_delta") => &v["usage"],
        Some("message_stop") => return out.push(Part::End(Ok(()))),
        _ => &Value::Null,
    };
    let usage = totals.increment(&usage_of(usage));
    match reply_of(&v) {
        Some(e) => out.push(Item::decoded(vec![e], raw).into()),
        None => out.push(Part::Frame(raw)),
    }
    if !usage.is_empty() {
        out.push(usage.into());
    }
}

/// The conversation event a stream event holds, if any.
fn reply_of(v: &Value) -> Option<Reply> {
    let block = v["index"]
        .as_u64()
        .and_then(|i| u32::try_from(i).ok())
        .unwrap_or(0);
    match v["type"].as_str()? {
        "message_start" => {
            let message = &v["message"];
            let id = message["id"].as_str().unwrap_or_default();
            Some(Reply::start(
                id,
                message["model"].as_str().unwrap_or_default(),
            ))
        }
        "content_block_start" => {
            let content = &v["content_block"];
            match (content["type"].as_str(), content["text"].as_str()) {
                // A text block opens empty and its text comes in deltas, but
                // one that opens with text holds that text.
                (Some("text"), None | Some("")) => None,
                (Some("text"), Some(text)) => Some(Reply::text(block, text)),
                _ => Some(Reply::other(ANTHROPIC, content.clone())),
            }
        }
        "content_block_delta" => {
            let delta = &v["delta"];
            Some(match delta["type"].as_str() {
                Some("text_delta") => {
                    Reply::text(block, delta["text"].as_str().unwrap_or_default())
                }
                _ => Reply::other(ANTHROPIC, delta.clone()),
            })
        }
        "message_delta" => {
            let finish = v.pointer("/delta/stop_reason").and_then(Value::as_str)?;
            Some(Reply::Finish(FINISH.finish(finish)))
        }
        "ping" | "content_block_stop" => None,
        // An event type this format does not know may hold content.
        _ => Some(Reply::other(ANTHROPIC, v.clone())),
    }
}

/// Writes Anthropic events from conversation events.
#[derive(Debug, Default)]
struct EventStream {
    /// Whether `message_start` is written. It waits for content, so a
    /// response that fails first can still be refused.
    started: bool,
    /// The open block and the index it is written at. Indexes follow the
    /// order blocks open, as Anthropic's do.
    open: Option<(u32, u32)>,
    opened: u32,
    /// The provider's open text block and its stop, held until the next
    /// event so text a filter releases can still go in.
    provider_open: Option<(u32, Option<Bytes>)>,
    /// Written text for a block not open among the provider's events. It
    /// goes into that block when the provider opens it, or a new block at
    /// the end: a block cannot open among the provider's, which Anthropic
    /// numbers by position.
    waiting: Vec<(u32, String)>,
    /// The provider's events from `message_delta` on.
    held: Option<BytesMut>,
    /// Events that say nothing, such as a ping or the next block's empty
    /// start, after a held stop; they wait with it.
    after_stop: Vec<Bytes>,
}

impl EventStream {
    fn begin(&mut self, so_far: &SoFar, out: &mut BytesMut) {
        // The provider's events open the stream themselves.
        if !so_far.relayed {
            self.open(so_far, out);
        }
    }

    /// Write `message_start`, unless it is written.
    fn open(&mut self, so_far: &SoFar, out: &mut BytesMut) {
        if std::mem::replace(&mut self.started, true) {
            return;
        }
        let (id, model) = so_far.start.clone().unwrap_or_default();
        let message = json!({
            "id": id,
            "type": "message",
            "role": "assistant",
            "model": model,
            "content": [],
            "stop_reason": null,
            "stop_sequence": null,
            // What is known so far, such as a plugin's usage given first.
            "usage": usage_body(&so_far.usage),
        });
        let start = json!({ "type": "message_start", "message": message });
        out.extend_from_slice(event(Some("message_start"), &start).as_bytes());
    }

    /// Close the provider's block, if its stop was held.
    fn close_provider_block(&mut self, out: &mut BytesMut) {
        if let Some((_, Some(stop))) = self.provider_open.take_if(|(_, stop)| stop.is_some()) {
            out.extend_from_slice(&stop);
            for raw in std::mem::take(&mut self.after_stop) {
                self.relay(&raw, out);
            }
        }
    }

    fn close_block(&mut self, out: &mut BytesMut) {
        if let Some((_, index)) = self.open.take() {
            let stop = json!({ "type": "content_block_stop", "index": index });
            out.extend_from_slice(event(Some("content_block_stop"), &stop).as_bytes());
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
                out.extend_from_slice(event(Some("content_block_start"), &start).as_bytes());
                index
            }
        };
        delta(index, text, out);
    }
}

fn delta(index: u32, text: &str, out: &mut BytesMut) {
    let delta = json!({
        "type": "content_block_delta",
        "index": index,
        "delta": { "type": "text_delta", "text": text },
    });
    out.extend_from_slice(event(Some("content_block_delta"), &delta).as_bytes());
}

impl StreamWriter for EventStream {
    /// An event holds only its own kind: a start in `message_start`, a
    /// finish in `message_delta`, text in a text block's start or delta of
    /// that index, other content in another block's.
    fn holds(raw: &Bytes, event: &Reply) -> bool {
        let v = event_json(raw).unwrap_or_default();
        let text = v["content_block"]["type"] == "text" || v["delta"]["type"] == "text_delta";
        match (v["type"].as_str(), event) {
            (Some("message_start"), Reply::Start { .. }) => true,
            (Some("message_delta"), Reply::Finish(_)) => true,
            (Some("content_block_start" | "content_block_delta"), Reply::Text { block, .. }) => {
                text && v["index"].as_u64() == Some(u64::from(*block))
            }
            (Some("content_block_start" | "content_block_delta"), Reply::Other { .. }) => !text,
            _ => false,
        }
    }

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
        // An event of no type, such as a comment, a ping or an empty text
        // block's start, says nothing.
        let says_nothing = matches!(name, None | Some("ping"))
            || (name == Some("content_block_start")
                && v["content_block"]["type"] == "text"
                && v["content_block"]["text"]
                    .as_str()
                    .is_none_or(str::is_empty));
        if says_nothing && matches!(self.provider_open, Some((_, Some(_)))) {
            return self.after_stop.push(raw.clone());
        }
        self.close_provider_block(out);
        let index = v["index"].as_u64().and_then(|i| u32::try_from(i).ok());
        match (name, index) {
            // Its stop is held until its next event.
            (Some("content_block_stop"), Some(i)) if self.provider_open == Some((i, None)) => {
                self.provider_open = Some((i, Some(raw.clone())));
            }
            (Some("content_block_start"), Some(i)) => {
                out.extend_from_slice(raw);
                self.opened = self.opened.max(i.saturating_add(1));
                // Written text goes only into a text block, waiting text
                // for it first.
                if v["content_block"]["type"] == "text" {
                    self.provider_open = Some((i, None));
                    let now = self
                        .waiting
                        .iter()
                        .take_while(|(block, _)| *block == i)
                        .count();
                    for (_, text) in self.waiting.drain(..now) {
                        delta(i, &text, out);
                    }
                }
            }
            _ => out.extend_from_slice(raw),
        }
    }

    /// An event with its edits put back; `None` if a filter took all it
    /// said, unless it opens a message or a block, which is kept emptied.
    fn rebuild(raw: &Bytes, events: &[Reply]) -> Result<Option<Bytes>, Fault> {
        let mut v = event_json(raw).unwrap_or_default();
        let opens = matches!(
            v["type"].as_str(),
            Some("message_start" | "content_block_start")
        );
        if !opens && events.iter().all(|e| matches!(e, Reply::Start { .. })) {
            return Ok(None);
        }
        put_event(&mut v, events);
        reads_back(events, &unless_taken(events, Vec::from_iter(reply_of(&v))))?;
        // It read back as an event, so it has its type.
        let name = v["type"].as_str().unwrap_or_default().to_owned();
        Ok(Some(event(Some(&name), &v).into()))
    }

    fn write(&mut self, e: &Reply, so_far: &SoFar, out: &mut BytesMut) -> Result<(), Fault> {
        match e {
            Reply::Text { block, text } => {
                self.begin(so_far, out);
                match &self.provider_open {
                    // Text waits behind waiting text, to keep order.
                    Some((open, _)) if open == block && self.waiting.is_empty() => {
                        delta(*block, text, out)
                    }
                    _ if so_far.relayed => self.waiting.push((*block, text.clone())),
                    _ => self.text(*block, text, out),
                }
            }
            Reply::Finish(_) => {}
            // A provider block held and released has lost its position.
            Reply::Other { .. } if so_far.relayed => {
                return Err(unpatchable(
                    "Anthropic's own blocks other than text cannot be held",
                ));
            }
            _ => return Err(unwritable_content()),
        }
        Ok(())
    }

    fn end(
        &mut self,
        end: &Result<(), Fault>,
        so_far: &SoFar,
        out: &mut BytesMut,
    ) -> Result<(), Fault> {
        self.close_provider_block(out);
        let held = self.held.take();
        if let Err(f) = end {
            out.extend_from_slice(&held.unwrap_or_default());
            let body = AnthropicMessages::error_body(f);
            out.extend_from_slice(event(Some("error"), &body).as_bytes());
            return Ok(());
        }
        self.open(so_far, out);
        for (block, text) in std::mem::take(&mut self.waiting) {
            self.text(block, &text, out);
        }
        self.close_block(out);
        // The provider's message_delta, unless a filter took its finish.
        match held {
            Some(held) if !held.is_empty() => out.extend_from_slice(&held),
            _ => {
                let reason = FINISH.reason(so_far.finish.as_ref().unwrap_or(&Finish::Stop));
                let delta = json!({
                    "type": "message_delta",
                    "delta": { "stop_reason": reason, "stop_sequence": null },
                    "usage": usage_body(&so_far.usage),
                });
                out.extend_from_slice(event(Some("message_delta"), &delta).as_bytes());
            }
        }
        let stop = json!({ "type": "message_stop" });
        out.extend_from_slice(event(Some("message_stop"), &stop).as_bytes());
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    /// The rebuilt event, or none.
    fn rebuild(raw: &Bytes, events: &[Reply]) -> Result<Bytes, Fault> {
        EventStream::rebuild(raw, events).map(Option::unwrap_or_default)
    }
    use crate::common::encode_request;

    fn patched(body: Value, edit: impl FnOnce(&mut Conversation)) -> Result<Value, Fault> {
        let before = AnthropicMessages::read(&body);
        let mut chat = before.clone();
        edit(&mut chat);
        let body = Doc::from(body);
        let original = Some(&body);
        let doc = encode_request::<AnthropicMessages>(&chat, original.map(|o| (o, &before)))?;
        Ok(doc.json().cloned().unwrap_or_default())
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
    fn input_usage_counts_cache_reads_and_writes() {
        let u = usage_of(&json!({
            "input_tokens": 3,
            "cache_read_input_tokens": 10,
            "cache_creation_input_tokens": 2,
            "output_tokens": 1,
        }));
        assert_eq!(u.get(units::INPUT_TOKENS), 15.0);
        assert_eq!(u.get(units::CACHED_INPUT_TOKENS), 10.0);
        assert_eq!(u.get(units::OUTPUT_TOKENS), 1.0);
    }

    #[test]
    fn stream_usage_is_counted_once_and_thinking_is_other() {
        let mut totals = Totals::default();
        let mut out = Vec::new();
        for e in [
            "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m\",\"model\":\"c\",\"usage\":{\"input_tokens\":5,\"output_tokens\":1}}}\n\n",
            "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"hm\"}}\n\n",
            "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"input_tokens\":5,\"output_tokens\":7}}\n\n",
        ] {
            decode_event(&mut totals, Bytes::from_static(e.as_bytes()), &mut out);
        }
        let mut total = Usage::default();
        for p in &out {
            if let Part::Usage(u) = p {
                total += u;
            }
        }
        assert_eq!(total.get(units::INPUT_TOKENS), 5.0);
        assert_eq!(total.get(units::OUTPUT_TOKENS), 7.0);
        let other = out.iter().any(
            |p| matches!(p, Part::Item(e) if matches!(e.events::<Reply>().ok(), Some([Reply::Other { .. }]))),
        );
        assert!(other);
    }

    #[test]
    fn an_event_it_cannot_read_is_content_filters_see() {
        let mut out = Vec::new();
        for e in [
            "event: future\ndata: {\"type\":\"future\",\"text\":\"secret\"}\n\n",
            "data: not json\n\n",
            "event: ping\ndata: {\"type\":\"ping\"}\n\n",
            ": a comment\n\n",
        ] {
            decode_event(
                &mut Totals::default(),
                Bytes::from_static(e.as_bytes()),
                &mut out,
            );
        }
        let other = |p: &Part| matches!(p, Part::Item(i) if matches!(i.events::<Reply>(), Ok([Reply::Other { .. }])));
        assert!(other(&out[0]) && other(&out[1]));
        assert!(matches!(&out[2..], [Part::Frame(_), Part::Frame(_)]));
    }

    #[test]
    fn text_a_block_opens_with_is_text() {
        let mut out = Vec::new();
        let e = "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"Hi \"}}\n\n";
        decode_event(
            &mut Totals::default(),
            Bytes::from_static(e.as_bytes()),
            &mut out,
        );
        let text = [Reply::text(0, "Hi ")];
        assert!(matches!(&out[..], [Part::Item(i)] if i.events::<Reply>().ok() == Some(&text[..])));
        let raw = Bytes::from_static(e.as_bytes());
        let edited = rebuild(&raw, &[Reply::text(0, "HI ")]);
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
        // No max_tokens is added: the client's body is theirs.
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
    fn a_whole_response_decodes_into_an_event_per_block() {
        let v = json!({
            "id": "m", "model": "c", "stop_reason": "tool_use",
            "content": [
                { "type": "text", "text": "ok" },
                { "type": "tool_use", "id": "t", "name": "f", "input": {} },
            ],
        });
        assert_eq!(
            AnthropicMessages::replies(&v),
            [
                Reply::start("m", "c"),
                Reply::text(0, "ok"),
                Reply::other(
                    ANTHROPIC,
                    json!({ "type": "tool_use", "id": "t", "name": "f", "input": {} })
                ),
                Reply::Finish(Finish::ToolUse),
            ]
        );
        let mut patched = v.clone();
        let mut events = AnthropicMessages::replies(&v);
        events[1] = Reply::text(0, "OK");
        AnthropicMessages::put(&mut patched, &events);
        assert_eq!(patched["content"][0]["text"], "OK");
        assert_eq!(patched["content"][1]["type"], "tool_use");
        // Text of a block that is not there is a block of its own.
        events.insert(3, Reply::text(9, "x"));
        AnthropicMessages::put(&mut patched, &events);
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
        assert_eq!(usage_body(&usage_of(&reported)), reported);
    }

    #[test]
    fn an_unchanged_finish_keeps_the_providers_reason() {
        let mut v = json!({ "stop_reason": "stop_sequence", "stop_sequence": "X" });
        AnthropicMessages::put(&mut v, &[Reply::Finish(Finish::Stop)]);
        assert_eq!(v["stop_reason"], "stop_sequence");
        AnthropicMessages::put(&mut v, &[Reply::Finish(Finish::Length)]);
        assert_eq!(v["stop_reason"], "max_tokens");
    }

    #[test]
    fn blocks_are_numbered_in_the_order_they_open() {
        let mut enc = StreamEncoder::<EventStream>::default();
        let mut out = BytesMut::new();
        for part in [Item::new([Reply::text(5, "hi")]).into(), Part::End(Ok(()))] {
            enc.encode(&part, &mut out).ok();
        }
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
        let events = [
            Reply::start("m", "c"),
            Reply::text(0, "[removed]"),
            Reply::Finish(Finish::ToolUse),
        ];
        let rebuilt = crate::common::rebuild_whole::<AnthropicMessages>(&raw, &events);
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
            AnthropicMessages::replies(&v)[1..],
            [Reply::other(ANTHROPIC, cited), Reply::text(1, "!")]
        );
    }

    #[test]
    fn a_stream_event_edited_into_what_it_has_no_place_for_fails() {
        let raw = Bytes::from_static(
            b"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"t\",\"name\":\"rm_rf\",\"input\":{}}}\n\n",
        );
        assert!(rebuild(&raw, &[Reply::text(0, "[removed]")]).is_err());
        let text = Bytes::from_static(
            b"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\n",
        );
        let out = rebuild(&text, &[Reply::text(0, "HI")]).unwrap_or_default();
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
        let added = [
            Reply::start("m", "c"),
            Reply::text(0, ""),
            Reply::Finish(Finish::Length),
        ]
        .iter()
        .try_for_each(|e| whole.add(e));
        assert!(added.is_ok());
        let v = AnthropicMessages::render(&whole);
        assert_eq!(v["content"], json!([]));
    }
}
