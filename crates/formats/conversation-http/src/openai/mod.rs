//! OpenAI Chat Completions: `POST /chat/completions`, JSON or SSE, of the
//! conversation kind, as a [`Schema`] ([`OpenAiChat`]).
//!
//! # Responses as events
//!
//! A chunk of a stream and a whole body decode into the same events, by
//! these rules, which are deterministic:
//!
//! - [`Output::Start`] comes once, with the first chunk that names an `id`. Its
//!   `native` holds the response's other fields (`created`, `system_fingerprint`,
//!   `service_tier`, a vendor's own object, ...) as the chunk has them.
//! - Blocks are numbered in the order they open. The first choice's
//!   `content` and each `tool_calls` entry (by its `index`, else by its `id`)
//!   open a [`Block::Text`] and a [`Block::ToolCall`] on their first
//!   non-empty delta, or, for a call, on the entry that names its `id` and
//!   `name`. Opening any block ends the text block that is open; tool calls
//!   stay open, as they interleave.
//! - A `finish_reason`, or the end of the stream, ends every block still
//!   open, in the order they opened, and a finish gives a [`Output::Finish`].
//! - `usage` gives a [`Output::Usage`] of what it adds to the totals
//!   reported so far.
//! - A data line that is no chunk is a `Native` of its text. `[DONE]` ends
//!   the stream.
//!
//! # What the kind has no form for
//!
//! What a response says beyond the spec, or that the kind has no form for,
//! is kept as this format's own (`openai-chat`), where it appeared, so a
//! plugin can read it and a client of this format gets it back:
//!
//! - The response's own fields: the first chunk's in the start's `native`;
//!   those a later chunk changes, after its usage, as one
//!   [`Output::Native`] `{"response": {<field>: <value>, ..}}`, which another
//!   format drops, as it drops the start's.
//! - A choice's: one [`Output::Native`] `{"choices": [..]}` before the
//!   chunk's blocks, each choice as it came but for what the kind took, the
//!   first one's message or delta under `delta`: the fields of the first
//!   choice (`logprobs`, a vendor's own) and of its body (`refusal`, content
//!   in parts, a vendor's own), and every other choice whole.
//! - Usage's fields beyond its counts: `{"usage": {..}}`, which another
//!   format drops.
//!
//! Encoders do the reverse. A stream is written as chunks: the opening
//! `role` chunk, a chunk per delta, and, at the end, the finish, the usage
//! chunk if the client asked for it, and `[DONE]`. A whole body is written
//! from the events collected: all text joined into the message `content`.
//! A block is any number unique in the response: chunks number tool calls
//! in the order they open. Reasoning has no field, and is refused.

mod request;
#[cfg(test)]
mod tests;

use crate::is_inert;
use crate::schema::{
    Asked, Decoder, Gate, Names, NativeFieldsExt, Schema, Totals, count, decode_message, ended,
    error_event, fits, merge_details, merge_fields, native_fields, parse_effort, remainder, says,
    says_error, set, stop_json, take_native, tidy, unix_now, unreadable_event, unsupported_fields,
    untranslatable, unwritable_content, unwritable_usage, usage_extra,
};
use respan_gateway_conversation::{
    Assembled, AssembledBlock, AssembledItem, Block, Content, Delta, Finish, Input, Output,
    Reasoning, Role, Start, Usage,
};
use respan_gateway_core::{BadRequest, Connection, Fault, Format, NativeValue, Upstream};
use respan_gateway_http::bytes::{Bytes, BytesMut};
use respan_gateway_http::framing::sse::{event, event_data};
use respan_gateway_http::{ClientIo, Encoder, ProviderIo};
use serde_json::{Map, Value, json};
use std::sync::Arc;

use request::{
    read_openai_message, read_openai_tools, read_response_format, write_openai_message,
    write_openai_tools, write_reasoning, write_response_format,
};

/// The wire format's name ([`Schema::NAME`]).
const OPENAI: &str = "openai-chat";

/// OpenAI Chat Completions, `openai-chat`: a `POST` to any path ending in
/// `/chat/completions`. The provider's base URL includes the version, such
/// as `https://api.openai.com/v1`. It reads OpenAI's fields; what else a
/// vendor sends stays native, as the module says.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OpenAiChat;

/// The counts of a usage object `u` of this format, and its fields beside
/// them; `Err` for a count that is no count.
pub fn read_usage(u: &Value) -> Result<(Usage, Map<String, Value>), Fault> {
    Ok((usage_of(u)?, extra_usage(u)))
}

const FINISH: Names<Finish> = Names(
    &[
        ("stop", Finish::Stop),
        ("length", Finish::Length),
        ("tool_calls", Finish::ToolUse),
        ("function_call", Finish::ToolUse),
        ("content_filter", Finish::Filtered),
    ],
    OPENAI,
);

impl Format<ClientIo, Input, Output> for OpenAiChat {
    fn name(&self) -> &str {
        OPENAI
    }
    fn matches(&self, io: &ClientIo) -> bool {
        self.accepts(io.request())
    }
    fn bind(&self, io: ClientIo) -> Result<Connection<Input, Output>, Fault> {
        self.bind_client(io)
    }
}

impl Format<ProviderIo, Output, Input> for OpenAiChat {
    fn name(&self) -> &str {
        OPENAI
    }
    fn bind(&self, io: ProviderIo) -> Result<Connection<Output, Input>, Fault> {
        self.bind_provider(io)
    }
}

/// Keys a conversation has a place for, or that change no output; `user`
/// too, when it is a string.
const KNOWN: &[&str] = &[
    "model",
    "messages",
    "stream",
    "stream_options",
    "max_tokens",
    "max_completion_tokens",
    "temperature",
    "top_p",
    "stop",
];

/// Keys that change nothing at these values, as SDKs send by default.
const INERT: &[(&str, f64)] = &[
    ("n", 1.0),
    ("presence_penalty", 0.0),
    ("frequency_penalty", 0.0),
    ("logprobs", 0.0),
];

const ROLES: Names<Role> = Names(
    &[
        ("system", Role::System),
        ("developer", Role::Developer),
        ("user", Role::User),
        ("assistant", Role::Assistant),
    ],
    OPENAI,
);

impl Schema for OpenAiChat {
    const NAME: &'static str = OPENAI;
    const PATH: &'static str = "/chat/completions";
    /// The two spellings of a response's token limit, and the structured
    /// output requirement.
    const OWN: &'static [&'static str] = &[
        "max_tokens",
        "max_completion_tokens",
        "response_format",
        "reasoning_effort",
    ];
    type Decoding = OpenAiDecoder;

    fn decoder(&self) -> OpenAiDecoder {
        OpenAiDecoder::new()
    }

    fn decode_json(&self, v: &Value) -> Input {
        let mut chat = Input::default();
        // The larger of both spellings, so the limit read bounds what any
        // provider enforces.
        chat.options.max_output_tokens = v["max_completion_tokens"]
            .as_u64()
            .max(v["max_tokens"].as_u64());
        chat.options.temperature = v["temperature"].as_f64();
        chat.options.top_p = v["top_p"].as_f64();
        chat.options.stop = match &v["stop"] {
            Value::String(s) => vec![s.clone()],
            Value::Array(a) => a
                .iter()
                .filter_map(|s| s.as_str().map(Into::into))
                .collect(),
            _ => Vec::new(),
        };
        chat.user = v["user"].as_str().map(Into::into);
        chat.native = native_fields(OPENAI, unsupported_fields(v, KNOWN, INERT));
        if chat.user.is_some() {
            chat.native.fields_mut(OPENAI).shift_remove("user");
        }
        for m in v["messages"].as_array().into_iter().flatten() {
            let mut message = decode_message::<Self>(m, &ROLES);
            read_openai_message(m, &mut message);
            tidy(&mut message);
            chat.messages.push(message);
        }
        read_openai_tools(v, &mut chat);
        chat.native = native_fields(OPENAI, std::mem::take(chat.native.fields_mut(OPENAI)));
        read_response_format(v, &mut chat);
        chat.options.reasoning =
            take_native(&mut chat, "reasoning_effort", |e| parse_effort(e.as_str()?))
                .map(Reasoning::effort);
        chat
    }

    fn encode_json(&self, chat: &Input) -> Result<Map<String, Value>, Fault> {
        // Several choices cannot be represented as events.
        let several = |n: &Value| n.as_u64().is_some_and(|n| n > 1);
        if chat.native.format() == Some(OPENAI)
            && chat.native.fields().get("n").is_some_and(several)
        {
            return Err(BadRequest::fault(
                "several choices (n > 1) cannot be represented; a raw relay can send them",
            ));
        }
        let mut v = Map::new();
        let mut messages = Vec::new();
        for m in &chat.messages {
            messages.extend(write_openai_message(m, &ROLES)?);
        }
        v.insert("messages".into(), messages.into());
        write_openai_tools(chat, &mut v)?;
        set(
            &mut v,
            "max_completion_tokens",
            chat.options.max_output_tokens.map(Into::into),
        );
        set(
            &mut v,
            "temperature",
            chat.options.temperature.map(Into::into),
        );
        set(&mut v, "top_p", chat.options.top_p.map(Into::into));
        set(&mut v, "stop", stop_json(&chat.options.stop));
        set(&mut v, "user", chat.user.clone().map(Into::into));
        set(&mut v, "response_format", write_response_format(chat));
        set(&mut v, "reasoning_effort", write_reasoning(chat)?);
        if chat.options.top_k.is_some() {
            return Err(untranslatable("top_k in OpenAI Chat"));
        }
        Ok(v)
    }

    fn decode_part(p: &Value) -> Option<Content> {
        request::decode_part(p)
    }

    fn encode_part(c: &Content) -> Result<Value, Fault> {
        request::encode_part(c)
    }

    fn is_response(v: &Value) -> bool {
        v["choices"].is_array()
    }

    /// One text of every block, then the tool calls, as one choice; other
    /// events as the choices they came in.
    fn encode_whole(&self, whole: &Assembled) -> Result<Value, Fault> {
        check_usage(&whole.usage)?;
        let join = |kind: Block| -> String {
            let same = |p: &&AssembledBlock| p.kind == kind;
            whole
                .blocks()
                .filter(same)
                .map(|p| p.text.as_str())
                .collect()
        };
        let (text, reasoning) = (join(Block::Text), join(Block::Reasoning));
        let fields: Vec<_> = whole
            .items()
            .iter()
            .filter_map(|item| match item {
                AssembledItem::Native(NativeValue {
                    format: f,
                    value: raw,
                }) if **f == *OPENAI => call_fields_of(raw),
                _ => None,
            })
            .collect();
        let calls: Vec<Value> = whole
            .blocks()
            .filter_map(|p| match &p.kind {
                Block::ToolCall { id, name } => {
                    let arguments = if p.text.is_empty() { "{}" } else { &p.text };
                    let mut call = call_json(None, Some(id), Some(name), arguments);
                    for (_, entry, function) in fields.iter().filter(|(b, ..)| *b == p.block) {
                        merge_call(&mut call, entry, function);
                    }
                    Some(call)
                }
                _ => None,
            })
            .collect();
        if !reasoning.is_empty() {
            return Err(unwritable_content());
        }
        let mut message = json!({ "role": "assistant", "content": text });
        if !calls.is_empty() {
            if text.is_empty() {
                message["content"] = Value::Null;
            }
            message["tool_calls"] = calls.into();
        }
        let finish = whole.finish.as_ref().unwrap_or(&Finish::Stop);
        let mut usage_more = Map::new();
        let mut response = Map::new();
        let mut choices = vec![json!({
            "index": 0,
            "message": message,
            "finish_reason": FINISH.reason(finish)?,
        })];
        for item in whole.items() {
            let AssembledItem::Native(NativeValue { format, value: raw }) = item else {
                continue;
            };
            if &**format == OPENAI && call_fields_of(raw).is_some() {
                continue;
            }
            if let (true, Some(fields)) = (&**format == OPENAI, usage_fields(raw)) {
                usage_more.extend(fields.clone());
                continue;
            }
            if let (true, Some(fields)) = (&**format == OPENAI, response_of(raw)) {
                merge_into(&mut response, fields);
                continue;
            }
            match (&**format == OPENAI, raw["choices"].as_array()) {
                (true, Some(more)) => more.iter().for_each(|c| merge_choice(&mut choices, c)),
                _ if !is_inert(format, raw) => return Err(unwritable_content()),
                _ => {}
            }
        }
        let mut body = envelope(whole.start.as_ref(), "chat.completion", unix_now());
        merge_into(&mut body, &response);
        body.insert("choices".into(), choices.into());
        body.insert("usage".into(), usage_body(&whole.usage, &usage_more));
        Ok(body.into())
    }

    fn stream_encoder(&self, asked: Asked) -> Box<dyn Encoder<Output>> {
        Box::new(Chunks::new(asked.usage))
    }

    fn error_body(fault: &Fault) -> Value {
        let code = respan_gateway_http::error_code(fault);
        let error =
            json!({ "message": fault.message(), "type": code, "param": null, "code": code });
        json!({ "error": merge_details(error, fault) })
    }

    /// Asks a provider stream for usage, which the encoder hides from a client that
    /// did not ask.
    fn prepare_call(v: &mut Map<String, Value>) {
        let usage = v.get("stream_options").and_then(|o| o.get("include_usage"));
        if v.get("stream") != Some(&Value::Bool(true)) || usage == Some(&Value::Bool(true)) {
            return;
        }
        let options = v.entry("stream_options").or_insert(Value::Null);
        match options.as_object_mut() {
            Some(o) => drop(o.insert("include_usage".into(), true.into())),
            None => *options = json!({ "include_usage": true }),
        }
    }

    /// The client's `max_tokens`, if it spelled its limit so, for the
    /// `max_completion_tokens` written: a compatible server may read only
    /// the spelling its clients send.
    fn respell(original: &Value, v: &mut Map<String, Value>) {
        if original.get("max_tokens").is_some()
            && let Some(limit) = v.get("max_completion_tokens").cloned()
        {
            v.insert("max_tokens".into(), limit);
            if original.get("max_completion_tokens").is_none() {
                v.remove("max_completion_tokens");
            }
        }
    }

    /// Usage, a tool call's fields, or the response's, kept beside it.
    fn is_inert(raw: &Value) -> bool {
        raw.as_object().is_some_and(|o| {
            o.len() == 1
                && ["usage", "call_fields", "response"]
                    .iter()
                    .any(|k| o.contains_key(*k))
        })
    }
}

/// A response's usage, as the kind counts it, from its usage object `u`.
fn usage_of(u: &Value) -> Result<Usage, Fault> {
    let n = |ptr| Ok::<_, Fault>(count(u.pointer(ptr))?.unwrap_or(0));
    let mut usage = Usage::new(n("/prompt_tokens")?, n("/completion_tokens")?);
    usage.cached_input_tokens = n("/prompt_tokens_details/cached_tokens")?;
    usage.reasoning_tokens = n("/completion_tokens_details/reasoning_tokens")?;
    Ok(usage)
}

/// What a usage object `u` says that has no unit: vendor fields, and
/// details beside the cached and reasoning counts.
fn extra_usage(u: &Value) -> Map<String, Value> {
    let known = ["prompt_tokens", "completion_tokens", "total_tokens"];
    let details: [(&str, &[&str]); 2] = [
        ("prompt_tokens_details", &["cached_tokens"]),
        ("completion_tokens_details", &["reasoning_tokens"]),
    ];
    usage_extra(u, &known, &details)
}

/// Usage as this format reports it.
fn usage_body(u: &Usage, native: &Map<String, Value>) -> Value {
    let (input, output) = (u.input_tokens, u.output_tokens);
    let mut body = json!({
        "prompt_tokens": input,
        "completion_tokens": output,
        "total_tokens": input.saturating_add(output),
    });
    if u.cached_input_tokens > 0 {
        body["prompt_tokens_details"] = json!({ "cached_tokens": u.cached_input_tokens });
    }
    if u.reasoning_tokens > 0 {
        body["completion_tokens_details"] = json!({ "reasoning_tokens": u.reasoning_tokens });
    }
    merge_fields(&mut body, native);
    body
}

/// The usage fields an [`Output::Native`] says, if it says only those.
fn usage_fields(raw: &Value) -> Option<&Map<String, Value>> {
    let only = raw.as_object().filter(|o| o.len() == 1)?;
    only.get("usage")?.as_object()
}

/// Merge `fields` into `into`, objects among them field by field.
fn merge_into(into: &mut Map<String, Value>, fields: &Map<String, Value>) {
    let mut merged = Value::Object(std::mem::take(into));
    merge_fields(&mut merged, fields);
    if let Value::Object(merged) = merged {
        *into = merged;
    }
}

/// The response's fields an [`Output::Native`] says, if it says only those.
fn response_of(raw: &Value) -> Option<&Map<String, Value>> {
    let only = raw.as_object().filter(|o| o.len() == 1)?;
    only.get("response")?.as_object()
}

/// OpenAI Chat has no count of cache writes.
fn check_usage(usage: &Usage) -> Result<(), Fault> {
    if usage.cache_write_input_tokens > 0
        || usage.cached_input_tokens > usage.input_tokens
        || usage.reasoning_tokens > usage.output_tokens
        || usage
            .input_tokens
            .checked_add(usage.output_tokens)
            .is_none()
    {
        return Err(unwritable_usage());
    }
    Ok(())
}

/// The fields every chunk or body of a response has: id, `object`, `created`
/// (the response's, if it kept one), model and the fields of the start's own
/// `native` if it is in this format.
fn envelope(start: Option<&Start>, object: &str, now: u64) -> Map<String, Value> {
    let (id, model) = start
        .map(|s| (s.id.as_str(), s.model.as_str()))
        .unwrap_or_default();
    let fields = start.filter(|s| s.native.format() == Some(OPENAI));
    let fields = fields.map(|s| s.native.fields());
    let created = fields.and_then(|f| f.get("created")).cloned();
    let mut v = Map::new();
    v.insert("id".into(), id.into());
    v.insert("object".into(), object.into());
    v.insert("created".into(), created.unwrap_or_else(|| now.into()));
    v.insert("model".into(), model.into());
    for (key, value) in fields.into_iter().flatten() {
        if key != "created" {
            v.insert(key.clone(), value.clone());
        }
    }
    v
}

/// A tool call as OpenAI writes it: in a chunk, numbered `index`, with its
/// id and name if it starts there; in a whole message, unnumbered.
fn call_json(index: Option<u32>, id: Option<&str>, name: Option<&str>, arguments: &str) -> Value {
    let function = match name {
        Some(name) => json!({ "name": name, "arguments": arguments }),
        None => json!({ "arguments": arguments }),
    };
    let mut call = Map::new();
    if let Some(index) = index {
        call.insert("index".into(), index.into());
    }
    if let Some(id) = id {
        call.insert("id".into(), id.into());
        call.insert("type".into(), "function".into());
    }
    call.insert("function".into(), function);
    call.into()
}

/// `choice` with its body under `key`, as `delta` or `message`.
fn rekey(choice: &Value, key: &str) -> Value {
    let mut choice = choice.clone();
    let other = if key == "delta" { "message" } else { "delta" };
    if let Some(fields) = choice.as_object_mut()
        && let Some(body) = fields.shift_remove(other)
    {
        fields.entry(key).or_insert(body);
    }
    choice
}

/// Put `more`, a choice of an [`Output::Native`], among a whole body's
/// `choices`: the first choice's fields join it, another is added. What
/// streamed in pieces is joined: strings are concatenated, and the arrays
/// of a message and of `logprobs` are appended.
fn merge_choice(choices: &mut Vec<Value>, more: &Value) {
    let more = rekey(more, "message");
    let index = more["index"].as_u64().unwrap_or(0);
    let at = choices
        .iter()
        .position(|c| c["index"].as_u64() == Some(index));
    let (Some(at), Some(fields)) = (at, more.as_object()) else {
        return choices.push(more);
    };
    let choice = &mut choices[at];
    for (key, value) in fields {
        match (key.as_str(), value.as_object()) {
            ("index", _) => {}
            ("message", Some(body)) => {
                for (k, x) in body {
                    append_piece(&mut choice["message"][k], x);
                }
                let refused = choice["message"]["refusal"]
                    .as_str()
                    .is_some_and(|r| !r.is_empty());
                if refused && choice["message"]["content"] == "" {
                    choice["message"]["content"] = Value::Null;
                }
            }
            ("logprobs", Some(body)) => {
                for (k, x) in body {
                    append_piece(&mut choice["logprobs"][k], x);
                }
            }
            _ => choice[key] = value.clone(),
        }
    }
}

/// Join `piece` to what `into` has so far: a string continues, an array
/// grows, and anything else is replaced.
fn append_piece(into: &mut Value, piece: &Value) {
    match (into, piece) {
        (Value::String(so_far), Value::String(more)) => so_far.push_str(more),
        (Value::Array(so_far), Value::Array(more)) => so_far.extend(more.iter().cloned()),
        (into, piece) => *into = piece.clone(),
    }
}

/// Decodes chunks and whole bodies of OpenAI Chat into events, as the
/// module says.
#[derive(Debug)]
pub struct OpenAiDecoder {
    /// The format of what the kind has no form for: [`OPENAI`].
    name: Arc<str>,
    totals: Totals,
    started: bool,
    /// The vendor fields of the usage last said, to say them once.
    usage_extra: Map<String, Value>,
    /// The response's fields that came before its start, as Azure's first
    /// chunk, which names no `id`, has.
    early: Map<String, Value>,
    /// The response's fields as last said, from its start on.
    said: Map<String, Value>,
    done: bool,
    /// Every block opened so far; a block's number is its place.
    blocks: Vec<Opened>,
    /// Whether the response made a tool call.
    called: bool,
}

#[derive(Debug)]
struct Opened {
    key: Key,
    open: bool,
    /// The id of the tool call this block is.
    call_id: Option<String>,
}

/// What a block of a response is made of, in OpenAI's terms.
#[derive(Debug, PartialEq)]
enum Key {
    Text,
    /// The tool call numbered so by `index`.
    Call(u64),
}

impl OpenAiDecoder {
    fn new() -> Self {
        Self {
            name: Arc::from(OPENAI),
            totals: Totals::default(),
            started: false,
            usage_extra: Map::new(),
            early: Map::new(),
            said: Map::new(),
            done: false,
            blocks: Vec::new(),
            called: false,
        }
    }
}

/// JSON fields.
type Fields = Map<String, Value>;

/// A tool call's part of a chunk or body.
struct Call {
    /// The entry's `index`, if it names one.
    index: Option<u64>,
    start: Option<(String, String)>,
    arguments: String,
    /// The entry's fields, and its function's, that the kind has no place
    /// for, even null or empty ones.
    entry: Map<String, Value>,
    function: Map<String, Value>,
}

/// The fields of tool call `block` the kind has no place for, as the
/// [`Output::Native`] that carries them: before the call's block opens.
fn call_fields(block: u32, entry: &Fields, function: &Fields) -> Option<Value> {
    if entry.is_empty() && function.is_empty() {
        return None;
    }
    let fields = json!({ "block": block, "entry": entry, "function": function });
    Some(json!({ "call_fields": fields }))
}

/// What a [`call_fields`] event says: the block, the entry's fields and the
/// function's.
fn call_fields_of(raw: &Value) -> Option<(u32, &Fields, &Fields)> {
    let only = raw.as_object().filter(|o| o.len() == 1)?;
    let fields = only.get("call_fields")?;
    let block = u32::try_from(fields["block"].as_u64()?).ok()?;
    Some((
        block,
        fields["entry"].as_object()?,
        fields["function"].as_object()?,
    ))
}

/// Put the fields of a `call_fields` event into the tool call `call`.
fn merge_call(call: &mut Value, entry: &Map<String, Value>, function: &Map<String, Value>) {
    if let Some(call) = call.as_object_mut() {
        call.extend(entry.clone());
    }
    if let Some(f) = call["function"].as_object_mut() {
        f.extend(function.clone());
    }
}

impl OpenAiDecoder {
    /// The block of `key`, if it is open.
    fn open_block(&self, key: &Key) -> Option<u32> {
        let at = self.blocks.iter().position(|b| b.open && b.key == *key);
        at.and_then(|at| u32::try_from(at).ok())
    }

    /// The number of the tool call `call` is of: its `index`, else, for a
    /// vendor that sends none, the open call it continues (it names no id,
    /// or the open call's own) or a new one.
    fn call_index(&self, call: &Call) -> u64 {
        if let Some(index) = call.index {
            return index;
        }
        let calls = self.blocks.iter().filter(|b| matches!(b.key, Key::Call(_)));
        let named = call.start.as_ref().map(|(id, _)| id);
        match calls.clone().rfind(|b| b.open) {
            Some(Opened {
                key: Key::Call(index),
                call_id,
                ..
            }) if named.is_none() || named == call_id.as_ref() => *index,
            _ => u64::try_from(calls.count()).unwrap_or(u64::MAX),
        }
    }

    /// Open a block of `key` as `kind`, ending the open text.
    fn open(&mut self, key: Key, kind: Block, out: &mut Vec<Output>) -> u32 {
        for (n, block) in self.blocks.iter_mut().enumerate() {
            if block.open && block.key == Key::Text {
                block.open = false;
                out.push(Output::block_end(u32::try_from(n).unwrap_or(u32::MAX)));
            }
        }
        let block = u32::try_from(self.blocks.len()).unwrap_or(u32::MAX);
        self.called |= matches!(kind, Block::ToolCall { .. });
        let call_id = match &kind {
            Block::ToolCall { id, .. } => Some(id.clone()),
            _ => None,
        };
        self.blocks.push(Opened {
            key,
            open: true,
            call_id,
        });
        out.push(Output::block_start(block, kind));
        block
    }

    /// Delta `delta` of the block of `key`, opened as `kind` if it is not.
    fn say(&mut self, key: Key, kind: Block, delta: Delta, out: &mut Vec<Output>) {
        let block = match self.open_block(&key) {
            Some(block) => block,
            None => self.open(key, kind, out),
        };
        out.push(Output::Delta { block, delta });
    }

    /// End every open block, in the order they opened.
    fn close(&mut self, out: &mut Vec<Output>) {
        for (n, block) in self.blocks.iter_mut().enumerate() {
            if std::mem::take(&mut block.open) {
                out.push(Output::block_end(u32::try_from(n).unwrap_or(u32::MAX)));
            }
        }
    }

    /// The events of one chunk, whose first choice's body is under `key`
    /// (`delta`, or `message` in a whole body).
    fn chunk(&mut self, v: &Value, key: &str, out: &mut Vec<Output>) -> Result<(), Fault> {
        let id = v["id"].as_str().filter(|id| !id.is_empty());
        let fields = response_fields(v);
        let mut changed = Map::new();
        match self.started {
            false => self.early.extend(fields),
            true => {
                for (key, value) in fields {
                    if self.said.get(&key) != Some(&value) {
                        self.said.insert(key.clone(), value.clone());
                        changed.insert(key, value);
                    }
                }
            }
        }
        if let (Some(id), false) = (id, self.started) {
            self.started = true;
            let model = v["model"].as_str().unwrap_or_default();
            let mut start = Start::new(id, model);
            self.said = std::mem::take(&mut self.early);
            start.native = native_fields(OPENAI, self.said.clone());
            out.push(Output::Start(start));
        }
        let u = &v["usage"];
        let usage = self.totals.increment(&usage_of(u)?);
        if !usage.is_empty() {
            out.push(Output::Usage(usage));
        }
        let native = extra_usage(u);
        if !native.is_empty() && native != self.usage_extra {
            out.push(Output::native(
                self.name.clone(),
                json!({ "usage": native }),
            ));
            self.usage_extra = native;
        }
        if !changed.is_empty() {
            let response = json!({ "response": changed });
            out.push(Output::native(self.name.clone(), response));
        }
        // What the kind has no form for goes first, so a chunk that cannot be
        // translated fails before any of it is written.
        for choice in v["choices"].as_array().into_iter().flatten() {
            if choice_index(choice) != 0 {
                out.push(Output::native(
                    self.name.clone(),
                    json!({ "choices": [rekey(choice, "delta")] }),
                ));
            }
        }
        let Some(first) = first_choice(v) else {
            return Ok(());
        };
        let body = &first[key];
        let calls = tool_calls(body);
        if let Some(rest) = choice_leftovers(first, key, calls.is_some()) {
            out.push(Output::native(
                self.name.clone(),
                json!({ "choices": [rest] }),
            ));
        }
        if let Some(text) = body["content"].as_str().filter(|t| !t.is_empty()) {
            self.say(Key::Text, Block::Text, Delta::Text(text.into()), out);
        }
        for call in calls.into_iter().flatten() {
            let key = Key::Call(self.call_index(&call));
            let block = match (call.start, self.open_block(&key)) {
                (Some((id, name)), None) => {
                    let next = u32::try_from(self.blocks.len()).unwrap_or(u32::MAX);
                    if let Some(fields) = call_fields(next, &call.entry, &call.function) {
                        out.push(Output::native(self.name.clone(), fields));
                    }
                    self.open(key, Block::ToolCall { id, name }, out)
                }
                (_, Some(block)) => block,
                (None, None) => {
                    return Err(Upstream::fault(
                        "a tool call's arguments come before the call starts",
                    ));
                }
            };
            if !call.arguments.is_empty() {
                out.push(Output::arguments(block, call.arguments));
            }
        }
        if let Some(reason) = first["finish_reason"].as_str() {
            self.close(out);
            // A call the request forced finishes as "stop" at OpenAI; a
            // response that made calls ends in tool use all the same.
            let finish = match (FINISH.finish(reason), self.called) {
                (Finish::Stop, true) => Finish::ToolUse,
                (finish, _) => finish,
            };
            out.push(Output::finished(finish));
        }
        Ok(())
    }
}

impl Decoder for OpenAiDecoder {
    fn event(&mut self, raw: Bytes, out: &mut Vec<Output>) -> Result<(), Fault> {
        if self.done {
            return Ok(());
        }
        let data = event_data(&raw);
        if data.as_deref().map(str::trim) == Some("[DONE]") {
            self.close(out);
            self.done = true;
            return Ok(());
        }
        let v = data
            .as_deref()
            .and_then(|d| serde_json::from_str::<Value>(d).ok());
        let chunk = |v: &Value| v["choices"].is_array() || !v["usage"].is_null() || says_error(v);
        let Some(v) = v.filter(chunk) else {
            unreadable_event(&self.name, &raw, out);
            return Ok(());
        };
        if says_error(&v) {
            return Err(error_event(OPENAI, &raw, &v));
        }
        self.chunk(&v, "delta", out)
    }

    fn end(&mut self, _out: &mut Vec<Output>) -> Result<(), Fault> {
        ended(self.done)
    }

    fn body(&mut self, v: &Value, out: &mut Vec<Output>) -> Result<(), Fault> {
        self.chunk(v, "message", out)?;
        self.close(out);
        self.done = true;
        Ok(())
    }
}

/// The fields of a chunk or body that are the response's, not an event's:
/// what is neither its id, model, choices nor usage.
fn response_fields(v: &Value) -> Map<String, Value> {
    let known = ["id", "model", "object", "choices", "usage", "obfuscation"];
    unsupported_fields(v, &known, &[])
}

/// A choice's `index`; a choice with none is the first.
fn choice_index(choice: &Value) -> u64 {
    choice["index"].as_u64().unwrap_or(0)
}

/// The first choice, if it is index 0: the conversation's.
fn first_choice(v: &Value) -> Option<&Value> {
    let choice = v["choices"].get(0)?;
    (choice_index(choice) == 0).then_some(choice)
}

/// The first choice without what the kind has: its text, tool calls
/// (`typed`, if the kind has all of them) and finish; its body is under
/// `delta`. `None` if nothing is left.
fn choice_leftovers(choice: &Value, key: &str, typed: bool) -> Option<Value> {
    let mut rest = Map::new();
    rest.insert("index".into(), 0.into());
    let fields = choice.as_object().into_iter().flatten();
    for (k, x) in fields {
        if k == "index" || k == "finish_reason" {
            continue;
        }
        if k != key {
            if says(x) {
                rest.insert(k.clone(), x.clone());
            }
            continue;
        }
        let mut body = Map::new();
        for (field, y) in x.as_object().into_iter().flatten() {
            let text = y.is_string() || y.is_null();
            let ours = match field.as_str() {
                "role" => true,
                "content" => text,
                "tool_calls" => typed,
                _ => false,
            };
            if !ours && says(y) {
                body.insert(field.clone(), y.clone());
            }
        }
        if !body.is_empty() {
            rest.insert("delta".into(), body.into());
        }
    }
    (rest.len() > 1).then(|| rest.into())
}

/// The tool calls of a choice's `body`, if each is a function call the
/// kind can say, with the `index` each names. `None` leaves
/// them other content.
fn tool_calls(body: &Value) -> Option<Vec<Call>> {
    let calls = body.get("tool_calls")?.as_array()?;
    if calls.is_empty() {
        return None;
    }
    let call = |c: &Value| {
        let function = &c["function"];
        let function_call = c["type"].is_null() || c["type"] == "function";
        if !function_call {
            return None;
        }
        let arguments = match &function["arguments"] {
            Value::Null => "",
            arguments => arguments.as_str()?,
        };
        let index = match c.get("index") {
            Some(index) => Some(index.as_u64()?),
            None => None,
        };
        let start = c["id"].as_str().zip(function["name"].as_str());
        Some(Call {
            index,
            start: start.map(|(id, name)| (id.to_owned(), name.to_owned())),
            arguments: arguments.to_owned(),
            entry: remainder(c, &["index", "id", "type", "function"]),
            function: remainder(function, &["name", "arguments"]),
        })
    };
    calls.iter().map(call).collect()
}

/// Writes events as OpenAI chunks: the opening chunk, one per delta, and
/// at the end the finish, usage if the client asked for it, and `[DONE]`.
#[derive(Debug)]
struct Chunks {
    /// Whether the client asked for the usage chunk; else it is hidden.
    show_usage: bool,
    start: Option<Start>,
    /// When the opening chunk was written.
    created: u64,
    began: bool,
    /// The blocks seen, by number.
    blocks: Vec<Slot>,
    /// How many tool calls have started: the next one's `index`.
    calls: u32,
    usage: Usage,
    /// The finish, written at the end, after the content released before it.
    finish: Option<Finish>,
    gate: Gate,
    /// The vendor fields of usage, written in the usage chunk.
    usage_extra: Map<String, Value>,
    /// The fields of tool calls that have not started, by block.
    call_fields: Vec<(u32, Fields, Fields)>,
    /// The response's fields a later chunk changed, for the next chunk.
    response: Fields,
}

#[derive(Debug)]
struct Slot {
    block: u32,
    kind: Block,
    /// A tool call's `index`.
    call: Option<u32>,
    /// Whether it had arguments: else it ends with `{}`.
    arguments: bool,
    ended: bool,
}

impl Chunks {
    fn new(show_usage: bool) -> Self {
        Self {
            show_usage,
            start: None,
            created: 0,
            began: false,
            blocks: Vec::new(),
            calls: 0,
            usage: Usage::default(),
            finish: None,
            gate: Gate::default(),
            usage_extra: Map::new(),
            call_fields: Vec::new(),
            response: Map::new(),
        }
    }

    fn begin(&mut self, out: &mut BytesMut) {
        if std::mem::replace(&mut self.began, true) {
            return;
        }
        self.created = unix_now();
        let delta = json!({ "role": "assistant", "content": "" });
        self.chunk(
            out,
            json!([{ "index": 0, "delta": delta, "finish_reason": null }]),
            None,
        );
    }

    fn delta(&mut self, out: &mut BytesMut, delta: Value) {
        self.chunk(
            out,
            json!([{ "index": 0, "delta": delta, "finish_reason": null }]),
            None,
        );
    }

    fn chunk(&mut self, out: &mut BytesMut, choices: Value, usage: Option<Value>) {
        let mut chunk = envelope(self.start.as_ref(), "chat.completion.chunk", self.created);
        merge_into(&mut chunk, &std::mem::take(&mut self.response));
        chunk.insert("choices".into(), choices);
        if let Some(usage) = usage {
            chunk.insert("usage".into(), usage);
        }
        out.extend_from_slice(event(None, Value::Object(chunk)).as_bytes());
    }

    fn slot(&mut self, block: u32) -> Result<&mut Slot, Fault> {
        let slot = self.blocks.iter_mut().find(|s| s.block == block);
        slot.filter(|s| !s.ended).ok_or_else(unwritable_content)
    }

    fn open(&mut self, block: u32, kind: &Block, out: &mut BytesMut) -> Result<(), Fault> {
        if let Some(had) = self.blocks.iter().find(|s| s.block == block) {
            // A start repeated for an open block opens nothing more.
            return match had.kind == *kind && !had.ended {
                true => Ok(()),
                false => Err(unwritable_content()),
            };
        }
        self.begin(out);
        let call = match kind {
            Block::ToolCall { id, name } => {
                let index = self.calls;
                self.calls += 1;
                let mut call = call_json(Some(index), Some(id), Some(name), "");
                if let Some(at) = self.call_fields.iter().position(|(b, ..)| *b == block) {
                    let (_, entry, function) = self.call_fields.remove(at);
                    merge_call(&mut call, &entry, &function);
                }
                self.delta(out, json!({ "tool_calls": [call] }));
                Some(index)
            }
            _ => None,
        };
        self.blocks.push(Slot {
            block,
            kind: kind.clone(),
            call,
            arguments: false,
            ended: false,
        });
        Ok(())
    }

    fn say(&mut self, block: u32, part: &Delta, out: &mut BytesMut) -> Result<(), Fault> {
        let slot = self.slot(block)?;
        if !fits(part, &slot.kind) {
            return Err(unwritable_content());
        }
        let text = part.as_str();
        if text.is_empty() {
            return Ok(());
        }
        let delta = match (part, slot.call) {
            (Delta::Text(_), _) => json!({ "content": text }),
            (Delta::Arguments(_), Some(index)) => {
                slot.arguments = true;
                json!({ "tool_calls": [call_json(Some(index), None, None, text)] })
            }
            _ => return Err(unwritable_content()),
        };
        self.delta(out, delta);
        Ok(())
    }

    /// End a block: a call that had no arguments has none, as JSON.
    fn close(&mut self, block: u32, out: &mut BytesMut) -> Result<(), Fault> {
        let slot = self.blocks.iter_mut().find(|s| s.block == block);
        let slot = slot.ok_or_else(unwritable_content)?;
        let bare = slot.call.filter(|_| !slot.arguments && !slot.ended);
        (slot.arguments, slot.ended) = (true, true);
        if let Some(index) = bare {
            let call = call_json(Some(index), None, None, "{}");
            self.delta(out, json!({ "tool_calls": [call] }));
        }
        Ok(())
    }
}

impl Encoder<Output> for Chunks {
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

impl Chunks {
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
            Output::Native(NativeValue { format, value: raw }) if **format == *OPENAI => {
                self.write_native(raw, out);
            }
            Output::Native(NativeValue { format, value: raw }) if is_inert(format, raw) => {}
            _ => return Err(unwritable_content()),
        }
        Ok(())
    }

    /// An event of this format's own: usage, call and response fields are
    /// kept for their place, a chunk is written as it came.
    fn write_native(&mut self, raw: &Value, out: &mut BytesMut) {
        if let Some(fields) = usage_fields(raw) {
            self.usage_extra.extend(fields.clone());
        } else if let Some(fields) = response_of(raw) {
            merge_into(&mut self.response, fields);
        } else if let Some((block, entry, function)) = call_fields_of(raw) {
            self.call_fields
                .push((block, entry.clone(), function.clone()));
        } else {
            match raw {
                // An event that could not be read, as it came.
                Value::String(data) => out.extend_from_slice(event(None, data).as_bytes()),
                v if v["choices"].is_array() => {
                    self.begin(out);
                    let choices = v["choices"].as_array().into_iter().flatten();
                    let choices: Vec<Value> = choices.map(|c| rekey(c, "delta")).collect();
                    self.chunk(out, choices.into(), None);
                }
                other => out.extend_from_slice(event(None, other).as_bytes()),
            }
        }
    }

    fn write_end(&mut self, end: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault> {
        if let Err(f) = end {
            // What the provider finished before it broke off is kept.
            if let Some(finish) = self.finish.take() {
                self.begin(out);
                let reason = FINISH.reason(&finish)?;
                self.chunk(
                    out,
                    json!([{ "index": 0, "delta": {}, "finish_reason": reason }]),
                    None,
                );
            }
            let body = match f.get::<respan_gateway_http::RawReply>().map(|r| r.0.body()) {
                Some(raw) => String::from_utf8_lossy(raw).into_owned(),
                None => OpenAiChat::error_body(f).to_string(),
            };
            out.extend_from_slice(event(None, body).as_bytes());
            return Ok(());
        }
        self.begin(out);
        let open: Vec<u32> = self
            .blocks
            .iter()
            .filter(|s| !s.ended)
            .map(|s| s.block)
            .collect();
        for block in open {
            self.close(block, out)?;
        }
        let finish = self.finish.take().unwrap_or(Finish::Stop);
        let reason = FINISH.reason(&finish)?;
        self.chunk(
            out,
            json!([{ "index": 0, "delta": {}, "finish_reason": reason }]),
            None,
        );
        if self.show_usage && !self.usage.is_empty() {
            self.chunk(
                out,
                json!([]),
                Some(usage_body(&self.usage, &self.usage_extra)),
            );
        }
        out.extend_from_slice(b"data: [DONE]\n\n");
        Ok(())
    }
}
