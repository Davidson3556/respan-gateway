#![warn(missing_docs)]
//! OpenAI Chat Completions: `POST /chat/completions`, JSON or SSE, of the
//! conversation kind, as a [`Codec`], in OpenAI's own terms or a vendor's
//! dialect ([`OpenAiChat`]).
//!
//! # Responses as events
//!
//! A chunk of a stream and a whole body decode into the same events, by
//! these rules, which are deterministic:
//!
//! - [`Output::Start`] comes once, with the first chunk that names an `id`. Its
//!   `native` holds the response's other fields (`created`, `system_fingerprint`,
//!   `service_tier`, ...) in this format's terms.
//! - Blocks are numbered in the order they open. The first choice's
//!   `reasoning_content` (in a dialect that has it), `content` and each
//!   `tool_calls` entry (by its `index`, in a whole body by its place) open
//!   a [`Block::Reasoning`], a [`Block::Text`] and a [`Block::ToolCall`] on
//!   their first non-empty delta, or, for a call, on the entry that names
//!   its `id` and `name`.
//!   Opening any block ends the text and reasoning blocks that are open;
//!   tool calls stay open, as they interleave.
//! - A `finish_reason`, or the end of the stream, ends every block still
//!   open, in the order they opened, and a finish gives a [`Output::Finish`].
//! - `usage` (or, in a dialect that reports it elsewhere, that object's)
//!   gives a [`Output::Usage`] of what it adds to the totals reported so far.
//! - What a chunk says that the kind has no form for (another choice,
//!   `logprobs`, `refusal`, content in parts, a field of another vendor's
//!   dialect) is one [`Output::Native`] of this format, `{"choices": [..]}`
//!   with the first choice's body under `delta`, which comes before the
//!   chunk's blocks. A data line that is no chunk is
//!   a `Native` of its text. `[DONE]` ends the stream.
//!
//! Encoders do the reverse. A stream is written as chunks: the opening
//! `role` chunk, a chunk per delta, and, at the end, the finish, the usage
//! chunk if the client asked for it, and `[DONE]`. A whole body is written
//! from the events collected: all text joined into the message `content`.
//! Reasoning is written as `reasoning_content` in a dialect that has it, and
//! refused in one that does not.

mod request;

use respan_gateway_codec::{
    Asked, Codec, Gate, Names, NativeFieldsExt, Reader, Totals, count, ended, error_event, fits,
    is_inert, merge_details, merge_fields, native_fields, read_message, remainder, says,
    says_error, set, stop_json, supported_usage, tidy, unix_now, unreadable_event,
    unsupported_fields, unwritable_content, unwritable_usage, usage_extra,
};
use respan_gateway_conversation::{
    Assembled, AssembledBlock, AssembledItem, Block, Content, Delta, Finish, Input, Output, Role,
    Start, units,
};
use respan_gateway_http::bytes::{Bytes, BytesMut};
use respan_gateway_http::formats::sse::{event, event_data};
use respan_gateway_http::{Encoder, Incoming, Outgoing};
use respan_gateway_sdk::{BadRequest, Connection, Fault, Format, NativeValue, Upstream, Usage};
use serde_json::{Map, Value, json};
use std::sync::Arc;

use request::{
    read_openai_message, read_openai_tools, read_response_format, write_openai_message,
    write_openai_tools, write_response_format,
};

/// The wire format's name ([`Codec::NAME`]), which its dialects share.
const OPENAI: &str = "openai-chat";

/// OpenAI Chat Completions, as OpenAI or a vendor serves it: a `POST` to any
/// path ending in `/chat/completions`. The provider's base URL includes the
/// version, such as `https://api.openai.com/v1`.
///
/// [`OpenAiChat::OPENAI`], named `openai-chat`, reads only OpenAI's own
/// fields; another vendor's stays native, as what the kind has no form for
/// does, so it survives a relay in this format and is refused in
/// translation. A vendor's dialect, named for the vendor, reads its fields
/// too, as its constant says, and writes them back. The values of every
/// dialect are this wire format's (`openai-chat` is the [`Codec::NAME`] of
/// each), so what one keeps native another writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenAiChat {
    name: &'static str,
    /// Whether reasoning is `reasoning_content`: in a request's messages and
    /// a response's message or delta.
    reasoning: bool,
    /// The object beside a response that holds its usage, when `usage`
    /// does not.
    usage_under: Option<&'static str>,
    /// The fields of a choice that describe it and carry no content.
    inert_choice: &'static [&'static str],
}

impl OpenAiChat {
    /// OpenAI's own, `openai-chat`.
    pub const OPENAI: Self = Self::dialect(OPENAI);
    /// Groq's, `groq-chat`: a stream reports usage under `x_groq`, beside
    /// the response; the object's other fields stay native.
    pub const GROQ: Self = Self {
        usage_under: Some("x_groq"),
        ..Self::dialect("groq-chat")
    };
    /// DeepSeek's, `deepseek-chat`: reasoning is `reasoning_content`.
    pub const DEEPSEEK: Self = Self {
        reasoning: true,
        ..Self::dialect("deepseek-chat")
    };
    /// vLLM's, `vllm-chat`: reasoning is `reasoning_content`, and a choice's
    /// `stop_reason`, the stop token or string that ended it, describes its
    /// finish.
    pub const VLLM: Self = Self {
        reasoning: true,
        inert_choice: &["stop_reason"],
        ..Self::dialect("vllm-chat")
    };
    /// OpenRouter's, `openrouter-chat`: a choice's `native_finish_reason`,
    /// the upstream provider's own reason, describes its finish.
    pub const OPENROUTER: Self = Self {
        inert_choice: &["native_finish_reason"],
        ..Self::dialect("openrouter-chat")
    };
    /// Azure OpenAI's, `azure-chat`: a choice's `content_filter_results`
    /// and `content_filter_offsets` describe the moderation it passed.
    pub const AZURE: Self = Self {
        inert_choice: &["content_filter_results", "content_filter_offsets"],
        ..Self::dialect("azure-chat")
    };

    /// This format's name: `openai-chat`, or its dialect's own.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// The dialect `name`, which reads no field but OpenAI's.
    const fn dialect(name: &'static str) -> Self {
        Self {
            name,
            reasoning: false,
            usage_under: None,
            inert_choice: &[],
        }
    }
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

impl Format<Incoming, Input, Output> for OpenAiChat {
    fn name(&self) -> &str {
        self.name
    }
    fn matches(&self, io: &Incoming) -> bool {
        self.accepts(io.request())
    }
    fn bind(&self, io: Incoming) -> Result<Connection<Input, Output>, Fault> {
        self.bind_client(io)
    }
}

impl Format<Outgoing, Output, Input> for OpenAiChat {
    fn name(&self) -> &str {
        self.name
    }
    fn bind(&self, io: Outgoing) -> Result<Connection<Output, Input>, Fault> {
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

impl Codec for OpenAiChat {
    const NAME: &'static str = OPENAI;
    const PATH: &'static str = "/chat/completions";
    /// The two spellings of a response's token limit, and the structured
    /// output requirement.
    const OWN: &'static [&'static str] =
        &["max_tokens", "max_completion_tokens", "response_format"];
    type Reading = OpenAiReader;

    fn name(&self) -> &'static str {
        self.name
    }

    fn reader(&self) -> OpenAiReader {
        OpenAiReader::new(*self)
    }

    fn read(&self, v: &Value) -> Input {
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
            let mut message = read_message::<Self>(m, &ROLES);
            read_openai_message(*self, m, &mut message);
            tidy(&mut message);
            chat.messages.push(message);
        }
        read_openai_tools(v, &mut chat);
        chat.native = native_fields(OPENAI, std::mem::take(chat.native.fields_mut(OPENAI)));
        read_response_format(v, &mut chat);
        chat
    }

    fn write(&self, chat: &Input) -> Result<Map<String, Value>, Fault> {
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
            messages.extend(write_openai_message(*self, m, &ROLES)?);
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
        Ok(v)
    }

    fn read_part(p: &Value) -> Option<Content> {
        request::read_part(p)
    }

    fn write_part(c: &Content) -> Result<Value, Fault> {
        request::write_part(c)
    }

    fn is_response(v: &Value) -> bool {
        v["choices"].is_array()
    }

    /// One text of every block, then the tool calls, as one choice; other
    /// events as the choices they came in.
    fn render(&self, whole: &Assembled) -> Result<Value, Fault> {
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
        let mut message = json!({ "role": "assistant", "content": text });
        if !reasoning.is_empty() {
            if !self.reasoning {
                return Err(unwritable_content());
            }
            message["reasoning_content"] = reasoning.into();
        }
        if !calls.is_empty() {
            if text.is_empty() {
                message["content"] = Value::Null;
            }
            message["tool_calls"] = calls.into();
        }
        let finish = whole.finish.as_ref().unwrap_or(&Finish::Stop);
        let mut usage_more = Map::new();
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
            match (&**format == OPENAI, raw["choices"].as_array()) {
                (true, Some(more)) => more.iter().for_each(|c| merge_choice(&mut choices, c)),
                _ if !is_inert(format, raw) => return Err(unwritable_content()),
                _ => {}
            }
        }
        let mut body = envelope(whole.start.as_ref(), "chat.completion", unix_now());
        body.insert("choices".into(), choices.into());
        body.insert("usage".into(), usage_body(&whole.usage, &usage_more));
        Ok(body.into())
    }

    fn stream_encoder(&self, asked: Asked) -> Box<dyn Encoder<Output>> {
        Box::new(Chunks::new(*self, asked.usage))
    }

    fn error_body(fault: &Fault) -> Value {
        let code = respan_gateway_http::error_code(fault);
        let error =
            json!({ "message": fault.message(), "type": code, "param": null, "code": code });
        json!({ "error": merge_details(error, fault) })
    }

    /// Asks a stream for usage, which the encoder hides from a client that
    /// did not ask.
    fn finish(v: &mut Map<String, Value>) {
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
}

/// A response's usage, as the kind counts it, from its usage object `u`.
fn usage_of(u: &Value) -> Result<Usage, Fault> {
    let mut usage = Usage::default();
    for (unit, ptr) in [
        (units::INPUT_TOKENS, "/prompt_tokens"),
        (units::OUTPUT_TOKENS, "/completion_tokens"),
        (
            units::CACHED_INPUT_TOKENS,
            "/prompt_tokens_details/cached_tokens",
        ),
        (
            units::REASONING_TOKENS,
            "/completion_tokens_details/reasoning_tokens",
        ),
    ] {
        if let Some(n) = count(u.pointer(ptr))? {
            usage.add(unit, n);
        }
    }
    Ok(usage)
}

/// The usage object of a chunk or body: `usage`, else, in a dialect that
/// reports it beside the response, that object's.
fn usage_object(chat: OpenAiChat, v: &Value) -> &Value {
    match (&v["usage"], chat.usage_under) {
        (u, _) if u.is_object() => u,
        (u, None) => u,
        (_, Some(under)) => &v[under]["usage"],
    }
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
    let get = |unit| u.get(unit).unwrap_or_default();
    let (input, output) = (get(units::INPUT_TOKENS), get(units::OUTPUT_TOKENS));
    let mut body = json!({
        "prompt_tokens": input,
        "completion_tokens": output,
        "total_tokens": input.saturating_add(output),
    });
    let cached = get(units::CACHED_INPUT_TOKENS);
    if cached > 0 {
        body["prompt_tokens_details"] = json!({ "cached_tokens": cached });
    }
    let reasoning = get(units::REASONING_TOKENS);
    if reasoning > 0 {
        body["completion_tokens_details"] = json!({ "reasoning_tokens": reasoning });
    }
    merge_fields(&mut body, native);
    body
}

/// The usage fields an [`Output::Native`] says, if it says only those.
fn usage_fields(raw: &Value) -> Option<&Map<String, Value>> {
    let only = raw.as_object().filter(|o| o.len() == 1)?;
    only.get("usage")?.as_object()
}

fn check_usage(usage: &Usage) -> Result<(), Fault> {
    supported_usage(
        usage,
        &[
            units::INPUT_TOKENS,
            units::OUTPUT_TOKENS,
            units::CACHED_INPUT_TOKENS,
            units::REASONING_TOKENS,
        ],
    )?;
    let input = usage.get(units::INPUT_TOKENS).unwrap_or_default();
    let output = usage.get(units::OUTPUT_TOKENS).unwrap_or_default();
    if usage.get(units::CACHED_INPUT_TOKENS).unwrap_or_default() > input
        || usage.get(units::REASONING_TOKENS).unwrap_or_default() > output
        || input.checked_add(output).is_none()
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
/// `choices`: the first choice's fields join it, another is added.
fn merge_choice(choices: &mut Vec<Value>, more: &Value) {
    let more = rekey(more, "message");
    let index = more["index"].as_u64().unwrap_or(0);
    let at = choices
        .iter()
        .position(|c| c["index"].as_u64() == Some(index));
    let (Some(at), Some(fields)) = (at, more.as_object()) else {
        return choices.push(more);
    };
    for (key, value) in fields {
        match (key.as_str(), value.as_object()) {
            ("index", _) => {}
            ("message", Some(body)) => {
                for (k, x) in body {
                    choices[at]["message"][k] = x.clone();
                }
            }
            _ => choices[at][key] = value.clone(),
        }
    }
}

/// Decodes chunks and whole bodies of OpenAI Chat into events, as the
/// module says.
#[derive(Debug)]
pub struct OpenAiReader {
    /// The dialect read.
    chat: OpenAiChat,
    /// The format of what the kind has no form for: [`OPENAI`].
    name: Arc<str>,
    totals: Totals,
    started: bool,
    /// The vendor fields of the usage last said, to say them once.
    usage_extra: Map<String, Value>,
    /// The response's fields that came before its start, as Azure's first
    /// chunk, which names no `id`, has.
    early: Map<String, Value>,
    done: bool,
    /// Every block opened so far; a block's number is its place.
    blocks: Vec<Opened>,
}

#[derive(Debug)]
struct Opened {
    key: Key,
    open: bool,
}

/// What a block of a response is made of, in OpenAI's terms.
#[derive(Debug, PartialEq)]
enum Key {
    Text,
    Reasoning,
    /// The tool call numbered so by `index`.
    Call(u64),
}

impl OpenAiReader {
    fn new(chat: OpenAiChat) -> Self {
        Self {
            chat,
            name: Arc::from(OPENAI),
            totals: Totals::default(),
            started: false,
            usage_extra: Map::new(),
            early: Map::new(),
            done: false,
            blocks: Vec::new(),
        }
    }
}

/// JSON fields.
type Fields = Map<String, Value>;

/// A tool call's part of a chunk or body.
struct Call {
    index: u64,
    start: Option<(String, String)>,
    arguments: String,
    /// The entry's fields, and its function's, that the kind has no place
    /// for, even null or empty ones.
    entry: Map<String, Value>,
    function: Map<String, Value>,
}

/// The fields of tool call `block` the kind has no place for, as the
/// [`Output::Native`] that carries them: before the call's block opens.
fn call_fields(block: u32, call: &Call) -> Option<Value> {
    if call.entry.is_empty() && call.function.is_empty() {
        return None;
    }
    let fields = json!({ "block": block, "entry": call.entry, "function": call.function });
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

impl OpenAiReader {
    /// The block of `key`, if it is open.
    fn open_block(&self, key: &Key) -> Option<u32> {
        let at = self.blocks.iter().position(|b| b.open && b.key == *key);
        at.and_then(|at| u32::try_from(at).ok())
    }

    /// Open a block of `key` as `kind`, ending the open text and reasoning.
    fn open(&mut self, key: Key, kind: Block, out: &mut Vec<Output>) -> u32 {
        for (n, block) in self.blocks.iter_mut().enumerate() {
            if block.open && matches!(block.key, Key::Text | Key::Reasoning) {
                block.open = false;
                out.push(Output::block_end(u32::try_from(n).unwrap_or(u32::MAX)));
            }
        }
        let block = u32::try_from(self.blocks.len()).unwrap_or(u32::MAX);
        self.blocks.push(Opened { key, open: true });
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
        if !self.started {
            self.early.extend(response_fields(self.chat, v));
        }
        if let (Some(id), false) = (id, self.started) {
            self.started = true;
            let model = v["model"].as_str().unwrap_or_default();
            let mut start = Start::new(id, model);
            start.native = native_fields(OPENAI, std::mem::take(&mut self.early));
            out.push(Output::Start(start));
        }
        let u = usage_object(self.chat, v);
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
        if let Some(rest) = rest_of(self.chat, first, key, calls.is_some()) {
            out.push(Output::native(
                self.name.clone(),
                json!({ "choices": [rest] }),
            ));
        }
        let reasoning = body["reasoning_content"]
            .as_str()
            .filter(|_| self.chat.reasoning);
        if let Some(text) = reasoning.filter(|t| !t.is_empty()) {
            self.say(
                Key::Reasoning,
                Block::Reasoning,
                Delta::Reasoning(text.into()),
                out,
            );
        }
        if let Some(text) = body["content"].as_str().filter(|t| !t.is_empty()) {
            self.say(Key::Text, Block::Text, Delta::Text(text.into()), out);
        }
        for call in calls.into_iter().flatten() {
            let key = Key::Call(call.index);
            let call_fields_source = Call {
                index: call.index,
                start: None,
                arguments: String::new(),
                entry: call.entry.clone(),
                function: call.function.clone(),
            };
            let block = match (call.start, self.open_block(&key)) {
                (Some((id, name)), None) => {
                    let next = u32::try_from(self.blocks.len()).unwrap_or(u32::MAX);
                    if let Some(fields) = call_fields(next, &call_fields_source) {
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
            out.push(Output::finished(FINISH.finish(reason)));
        }
        Ok(())
    }
}

impl Reader for OpenAiReader {
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
            return Err(error_event(self.chat.name, &raw, &v));
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
/// what is neither its id, model, choices nor usage, nor the usage in the
/// object a dialect reports it under.
fn response_fields(chat: OpenAiChat, v: &Value) -> Map<String, Value> {
    let known = ["id", "model", "object", "choices", "usage", "obfuscation"];
    let mut fields = unsupported_fields(v, &known, &[]);
    if let Some(under) = chat.usage_under {
        let beside = v[under].as_object().into_iter().flatten();
        let rest: Map<String, Value> = beside
            .filter(|(k, x)| *k != "usage" && says(x))
            .map(|(k, x)| (k.clone(), x.clone()))
            .collect();
        match rest.is_empty() {
            true => _ = fields.shift_remove(under),
            false => _ = fields.insert(under.into(), rest.into()),
        }
    }
    fields
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

/// The first choice without what the kind has: its text, reasoning in a
/// dialect that has it, tool calls (`typed`, if the kind has all of them),
/// finish and what the dialect adds to describe it; its body is under
/// `delta`. `None` if nothing is left.
fn rest_of(chat: OpenAiChat, choice: &Value, key: &str, typed: bool) -> Option<Value> {
    let mut rest = Map::new();
    rest.insert("index".into(), 0.into());
    let fields = choice.as_object().into_iter().flatten();
    for (k, x) in fields {
        if k == "index" || k == "finish_reason" || chat.inert_choice.contains(&k.as_str()) {
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
                "reasoning_content" => text && chat.reasoning,
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
/// kind can say: numbered by their `index`, else by place. `None` leaves
/// them other content.
fn tool_calls(body: &Value) -> Option<Vec<Call>> {
    let calls = body.get("tool_calls")?.as_array()?;
    if calls.is_empty() {
        return None;
    }
    let call = |(at, c): (usize, &Value)| {
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
            Some(index) => index.as_u64()?,
            None => u64::try_from(at).ok()?,
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
    calls.iter().enumerate().map(call).collect()
}

/// Writes events as OpenAI chunks: the opening chunk, one per delta, and
/// at the end the finish, usage if the client asked for it, and `[DONE]`.
#[derive(Debug)]
struct Chunks {
    /// The dialect written.
    chat: OpenAiChat,
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
    fn new(chat: OpenAiChat, show_usage: bool) -> Self {
        Self {
            chat,
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

    fn delta(&self, out: &mut BytesMut, delta: Value) {
        self.chunk(
            out,
            json!([{ "index": 0, "delta": delta, "finish_reason": null }]),
            None,
        );
    }

    fn chunk(&self, out: &mut BytesMut, choices: Value, usage: Option<Value>) {
        let mut chunk = envelope(self.start.as_ref(), "chat.completion.chunk", self.created);
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
        let reasoning = self.chat.reasoning;
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
            (Delta::Reasoning(_), _) if reasoning => {
                json!({ "reasoning_content": text })
            }
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
            Output::Native(NativeValue { format, value: raw })
                if **format == *OPENAI && usage_fields(raw).is_some() =>
            {
                if let Some(fields) = usage_fields(raw) {
                    self.usage_extra.extend(fields.clone());
                }
            }
            Output::Native(NativeValue { format, value: raw })
                if **format == *OPENAI && call_fields_of(raw).is_some() =>
            {
                if let Some((block, entry, function)) = call_fields_of(raw) {
                    self.call_fields
                        .push((block, entry.clone(), function.clone()));
                }
            }
            Output::Native(NativeValue { format, value: raw }) if **format == *OPENAI => {
                match &**raw {
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
            Output::Native(NativeValue { format, value: raw }) if is_inert(format, raw) => {}
            Output::Native(NativeValue { .. }) => {
                return Err(unwritable_content());
            }
            _ => return Err(unwritable_content()),
        }
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    use respan_gateway_anthropic::AnthropicMessages;
    use respan_gateway_codec::OutputParser;
    use respan_gateway_conversation::{Content, Message, NativeFields, Tool, ToolChoice};
    use respan_gateway_http::{Status, http::StatusCode};
    use respan_gateway_sdk::{Details, ErrorCode};

    /// The events of `chunks`, and how the stream ended.
    fn decode_all(chunks: &[&str]) -> (Vec<Output>, Result<(), Fault>) {
        decode_all_in(OpenAiChat::OPENAI, chunks)
    }

    /// [`decode_all`] in the dialect `chat`.
    fn decode_all_in(chat: OpenAiChat, chunks: &[&str]) -> (Vec<Output>, Result<(), Fault>) {
        let mut d = OutputParser::new(chat.reader(), false, None);
        let mut out = Vec::new();
        for c in chunks {
            if let Err(f) = d.push(Bytes::from(c.to_string()), &mut out) {
                return (out, Err(f));
            }
        }
        let end = d.finish(&mut out);
        (out, end)
    }

    /// The events of one whole body, in the dialect `chat`.
    fn decode_body(chat: OpenAiChat, v: &Value) -> Vec<Output> {
        let mut out = Vec::new();
        chat.reader().body(v, &mut out).expect("a whole response");
        out
    }

    /// `replies` as a stream a client that asked for usage as `usage` gets,
    /// whole, and how it ended.
    fn stream_of(replies: &[Output], usage: bool) -> Result<String, Fault> {
        let mut enc = Chunks::new(OpenAiChat::OPENAI, usage);
        let mut out = BytesMut::new();
        for reply in replies {
            enc.encode(reply, &mut out)?;
        }
        enc.end(&Ok(()), &mut out)?;
        Ok(String::from_utf8_lossy(&out).into_owned())
    }

    /// `replies` as a whole body.
    fn body_of(replies: &[Output]) -> Result<Value, Fault> {
        body_in(OpenAiChat::OPENAI, replies)
    }

    /// [`body_of`] in the dialect `chat`.
    fn body_in(chat: OpenAiChat, replies: &[Output]) -> Result<Value, Fault> {
        let mut whole = Assembled::default();
        replies.iter().try_for_each(|r| whole.push(r))?;
        chat.render(&whole)
    }

    /// The JSON of each data line, but `[DONE]`.
    fn chunks_of(stream: &str) -> Vec<Value> {
        let data = stream
            .split("\n\n")
            .filter_map(|e| e.strip_prefix("data: "));
        data.filter_map(|d| serde_json::from_str(d).ok()).collect()
    }

    fn text_events(block: u32, text: &str) -> Vec<Output> {
        vec![
            Output::block_start(block, Block::Text),
            Output::text(block, text),
            Output::block_end(block),
        ]
    }

    fn call(id: &str, name: &str) -> Block {
        Block::ToolCall {
            id: id.into(),
            name: name.into(),
        }
    }

    fn usage(unit: &'static str, n: u64) -> Output {
        let mut u = Usage::default();
        u.add(unit, n);
        Output::usage(u)
    }

    #[test]
    fn a_start_is_pushed_only_with_the_first_content_and_dropped_at_a_failure() {
        let role = "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"}}],\"usage\":{\"prompt_tokens\":2}}\n\n";
        let text =
            "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hi\"}}]}\n\n";
        let error = "data: {\"error\":{\"message\":\"Overloaded\"}}\n\n";
        let batches = |events: &[&str]| {
            let mut d = OutputParser::new(OpenAiChat::OPENAI.reader(), true, None);
            let batches: Vec<(Vec<Output>, bool)> = events
                .iter()
                .map(|e| {
                    let mut out = Vec::new();
                    let failed = d.push(Bytes::from(e.to_string()), &mut out).is_err();
                    (out, failed)
                })
                .collect();
            batches
        };
        let content = batches(&[role, ": ping\n\n", text]);
        assert!(content[0].0.is_empty() && content[1].0.is_empty());
        // The start, the usage held behind it, and the text, in order.
        let kinds: Vec<bool> = content[2].0.iter().map(Output::is_content).collect();
        assert_eq!(kinds, [true, false, true, true]);
        assert!(matches!(&content[2].0[0], Output::Start(s) if s.id == "c1"));
        // A failure first leaves only the usage held, saying nothing.
        let failed = batches(&[role, error]);
        assert!(failed[1].1);
        assert_eq!(failed[1].0.len(), 1);
        let held = &failed[1].0[0];
        assert!(
            matches!(held, Output::Usage(u) if u.get(units::INPUT_TOKENS) == Some(2)),
            "{held:?}"
        );
    }

    #[test]
    fn a_stream_decodes_into_events() {
        let (out, end) = decode_all(&[
            "data: {\"id\":\"x\",\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"He",
            "llo\"}}]}\n\ndata: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":1}}\n\n",
            "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":2}}\n\n",
            "data: [DONE]\n\n",
        ]);
        assert!(end.is_ok());
        let mut first = usage(units::INPUT_TOKENS, 3);
        if let Output::Usage(u) = &mut first {
            u.add(units::OUTPUT_TOKENS, 1);
        }
        assert_eq!(
            out,
            [
                Output::start("x", "m"),
                Output::block_start(0, Block::Text),
                Output::text(0, "Hello"),
                // Totals repeated in a later chunk add only what changed.
                first,
                Output::block_end(0),
                Output::finished(Finish::Stop),
                usage(units::OUTPUT_TOKENS, 1),
            ]
        );
    }

    /// Chunks as OpenAI sends them, with the fields it sets to null.
    #[test]
    fn real_openai_chunks_decode_to_text_and_tool_calls() {
        let (out, _) = decode_all(&[
            "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt\",\"system_fingerprint\":\"fp\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\",\"refusal\":null},\"logprobs\":null,\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hi\"},\"logprobs\":null,\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"f\",\"arguments\":\"\"}}]},\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{},\"logprobs\":null,\"finish_reason\":\"tool_calls\"}]}\n\n",
            "data: [DONE]\n\n",
        ]);
        let mut start = Output::start("c1", "gpt");
        if let Output::Start(s) = &mut start {
            let fields = json!({ "created": 1, "system_fingerprint": "fp" });
            let fields = fields.as_object().cloned().unwrap_or_default();
            s.native = Some(NativeFields::new(OPENAI, fields));
        }
        assert_eq!(
            out,
            [
                start,
                // An empty text opens no block.
                Output::block_start(0, Block::Text),
                Output::text(0, "Hi"),
                // A tool call ends the text before it, and starts its own.
                Output::block_end(0),
                Output::block_start(1, call("call_1", "f")),
                Output::block_end(1),
                Output::finished(Finish::ToolUse),
            ]
        );
    }

    #[test]
    fn blocks_are_numbered_in_the_order_they_open() {
        let (out, _) = decode_all_in(
            OpenAiChat::DEEPSEEK,
            &[
                "data: {\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{\"reasoning_content\":\"hm\"}}]}\n\n",
                "data: {\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hi\"}}]}\n\n",
                "data: {\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":3,\"id\":\"a\",\"function\":{\"name\":\"f\",\"arguments\":\"{\"}},{\"index\":1,\"id\":\"b\",\"function\":{\"name\":\"g\",\"arguments\":\"\"}}]}}]}\n\n",
                "data: {\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":3,\"function\":{\"arguments\":\"}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\n",
                "data: [DONE]\n\n",
            ],
        );
        assert_eq!(
            out[1..],
            [
                Output::block_start(0, Block::Reasoning),
                Output::reasoning(0, "hm"),
                Output::block_end(0),
                Output::block_start(1, Block::Text),
                Output::text(1, "Hi"),
                Output::block_end(1),
                // Each call, by its `index`, is a block, as it opens.
                Output::block_start(2, call("a", "f")),
                Output::arguments(2, "{"),
                Output::block_start(3, call("b", "g")),
                Output::arguments(2, "}"),
                Output::block_end(2),
                Output::block_end(3),
                Output::finished(Finish::ToolUse),
            ]
        );
    }

    #[test]
    fn a_choice_with_logprobs_is_other_content() {
        let (out, _) = decode_all(&[
            "data: {\"id\":\"x\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hi\"},\"logprobs\":{\"content\":[]}}]}\n\n",
        ]);
        // Its text is text, as in a whole response, so a filter sees it; the
        // rest is other content, which comes first.
        let rest = json!({ "choices": [{ "index": 0, "logprobs": { "content": [] } }] });
        assert_eq!(
            out[1..],
            [
                Output::native(OPENAI, rest),
                Output::block_start(0, Block::Text),
                Output::text(0, "Hi"),
            ]
        );
    }

    #[test]
    fn what_a_dialect_adds_to_describe_a_choice_is_no_content() {
        let chunk = "data: {\"id\":\"x\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\",\"native_finish_reason\":\"stop\",\"stop_reason\":null}]}\n\n";
        let (out, _) = decode_all_in(OpenAiChat::OPENROUTER, &[chunk]);
        assert_eq!(out[1..], [Output::finished(Finish::Stop)]);
        // OpenAI's own format reads no other vendor's field: it stays native.
        let (out, _) = decode_all(&[chunk]);
        let rest = json!({ "choices": [{ "index": 0, "native_finish_reason": "stop" }] });
        assert_eq!(
            out[1..],
            [Output::native(OPENAI, rest), Output::finished(Finish::Stop)]
        );
    }

    #[test]
    fn openai_chat_reads_no_vendors_field_but_relays_it() {
        let chunks = [
            "data: {\"id\":\"x\",\"x_groq\":{\"usage\":{\"prompt_tokens\":3}},\"choices\":[{\"index\":0,\"delta\":{\"reasoning_content\":\"hm\"}}]}\n\n",
            "data: [DONE]\n\n",
        ];
        let (out, end) = decode_all(&chunks);
        assert!(end.is_ok());
        // No usage and no reasoning: Groq's usage stays in the start's
        // fields, DeepSeek's reasoning in the choice's.
        let Some(Output::Start(start)) = out.first() else {
            panic!("{out:?}");
        };
        assert_eq!(
            start.native.fields()["x_groq"],
            json!({ "usage": { "prompt_tokens": 3 } })
        );
        let rest = json!({ "choices": [{ "index": 0, "delta": { "reasoning_content": "hm" } }] });
        assert_eq!(out[1..], [Output::native(OPENAI, rest)]);
        // Both go back to a client of the format as they came.
        let stream = stream_of(&out, false).expect("a stream");
        assert!(stream.contains("\"x_groq\":{\"usage\""), "{stream}");
        assert!(stream.contains("\"reasoning_content\":\"hm\""), "{stream}");
        // A request's reasoning is a message field like any other.
        let body = json!({ "model": "m", "messages": [
            { "role": "assistant", "content": "ok", "reasoning_content": "hm" },
        ]});
        let chat = OpenAiChat::OPENAI.read(&body);
        assert_eq!(chat.messages[0].content, [Content::from("ok")]);
        assert_eq!(
            chat.messages[0].native.fields().get("reasoning_content"),
            Some(&json!("hm"))
        );
        assert_eq!(
            written(body, |_| {})
                .ok()
                .map(|v| v["messages"][0]["reasoning_content"].clone()),
            Some(json!("hm"))
        );
    }

    #[test]
    fn only_the_first_choice_finishes_the_response() {
        let (out, _) = decode_all(&[
            "data: {\"id\":\"x\",\"choices\":[{\"index\":1,\"delta\":{},\"finish_reason\":\"length\"}]}\n\n",
        ]);
        assert!(!out.iter().any(|r| matches!(r, Output::Finish(_))));
        // The other choice is kept, in this format's words.
        assert!(
            matches!(&out[1..], [Output::Native(NativeValue { format, .. })] if &**format == OPENAI)
        );
    }

    #[test]
    fn content_the_kind_cannot_hold_is_other() {
        let (out, _) = decode_all(&[
            "data: {\"id\":\"x\",\"choices\":[{\"index\":0,\"delta\":{\"refusal\":\"no\"}}]}\n\n",
        ]);
        let refusal = json!({ "choices": [{ "index": 0, "delta": { "refusal": "no" } }] });
        assert_eq!(out[1..], [Output::native(OPENAI, refusal)]);
    }

    #[test]
    fn a_chunk_is_all_it_says_as_events_in_order() {
        let (out, _) = decode_all(&[
            "data: {\"id\":\"x\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":\"stop\"}],\"usage\":{\"completion_tokens\":1}}\n\n",
        ]);
        assert_eq!(
            out,
            [
                Output::start("x", ""),
                usage(units::OUTPUT_TOKENS, 1),
                Output::block_start(0, Block::Text),
                Output::text(0, "hi"),
                Output::block_end(0),
                Output::finished(Finish::Stop),
            ]
        );
    }

    // A decoder ends only at the end of its body, so what follows the
    // terminator is read and ignored; a body that ends before it is cut
    // short.
    #[test]
    fn the_terminator_completes_the_response_and_nothing_after_it_is_read() {
        let (out, end) = decode_all(&["data: [DONE]\n\ndata: {\"choices\":[]}\n\n"]);
        assert!(out.is_empty() && end.is_ok());
        let (_, end) = decode_all(&["data: {\"choices\":[]}\n\n"]);
        assert_eq!(
            end.err()
                .map(|f| respan_gateway_http::error_code(&f).to_owned())
                .as_deref(),
            Some("truncated")
        );
    }

    #[test]
    fn a_provider_error_event_ends_the_stream_with_its_code() {
        let (_, end) = decode_all(&[
            "data: {\"error\":{\"type\":\"server_error\",\"message\":\"overloaded\"}}\n\n",
        ]);
        assert!(matches!(
            end,
            Err(f) if respan_gateway_http::error_code(&f) == "server_error" && f.message() == "overloaded"
        ));
    }

    #[test]
    fn a_provider_error_event_goes_out_as_it_came_in_the_same_format() {
        let error = "data: {\"error\":{\"message\":\"boom\",\"type\":\"server_error\",\"param\":null,\"code\":null}}\n\n";
        let (_, end) = decode_all(&[error]);
        let mut bytes = BytesMut::new();
        let mut enc = Chunks::new(OpenAiChat::OPENAI, false);
        assert!(enc.end(&end, &mut bytes).is_ok());
        // Its fields are the format's, so it is written as it came.
        assert_eq!(&bytes[..], error.as_bytes());
    }

    #[test]
    fn an_edited_start_is_written_in_every_chunk() {
        let mut replies = vec![Output::start("y", "alias")];
        replies.extend(text_events(0, "HI"));
        let stream = stream_of(&replies, false).expect("a stream");
        let chunks = chunks_of(&stream);
        assert!(
            chunks
                .iter()
                .all(|c| c["id"] == "y" && c["model"] == "alias")
        );
        let text: String = chunks
            .iter()
            .filter_map(|c| c["choices"][0]["delta"]["content"].as_str())
            .collect();
        assert_eq!(text, "HI");
        // Text a filter replaced with a finish is the finish.
        let length = [Output::start("y", "m"), Output::finished(Finish::Length)];
        let stream = stream_of(&length, false).expect("a stream");
        assert!(stream.contains("\"finish_reason\":\"length\""), "{stream}");
    }

    #[test]
    fn a_finish_edited_beside_a_tool_call_is_written() {
        let replies = [
            Output::block_start(0, call("t", "f")),
            Output::block_end(0),
            Output::finished(Finish::Stop),
        ];
        let stream = stream_of(&replies, false).expect("a stream");
        assert!(stream.contains("\"finish_reason\":\"stop\""), "{stream}");
        // A call that had no arguments has none, as JSON.
        assert!(stream.contains("\"arguments\":\"{}\""), "{stream}");
    }

    #[test]
    fn a_tool_call_emptied_in_a_stream_fails_rather_than_leave_a_shell() {
        // Arguments for a call that never started.
        let replies = [Output::arguments(0, "{}")];
        assert!(stream_of(&replies, false).is_err());
        assert!(body_of(&replies).is_err());
        // A call that is a shell, with no id or name, cannot be decoded.
        let (_, end) = decode_all(&[
            "data: {\"id\":\"x\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0}]}}]}\n\n",
        ]);
        assert!(end.is_err());
    }

    #[test]
    fn the_text_of_the_opening_chunk_can_be_cleared() {
        let (out, _) = decode_all(&[
            "data: {\"id\":\"c1\",\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"SECRET\"}}]}\n\n",
        ]);
        let kept: Vec<Output> = out
            .into_iter()
            .filter(|r| !matches!(r, Output::Delta { .. }))
            .collect();
        let stream = stream_of(&kept, false).expect("a stream");
        assert!(!stream.contains("SECRET"), "{stream}");
    }

    #[test]
    fn a_tool_call_a_filter_replaces_with_text_is_gone() {
        let mut replies = vec![Output::start("x", "m")];
        replies.extend(text_events(0, "[removed]"));
        let stream = stream_of(&replies, false).expect("a stream");
        assert!(stream.contains("\"content\":\"[removed]\""), "{stream}");
        assert!(!stream.contains("tool_calls"), "{stream}");
        // What it cannot write fails the response.
        let foreign = [Output::native("anthropic-messages", json!({ "type": "x" }))];
        assert!(body_of(&foreign).is_err());
    }

    #[test]
    fn a_whole_response_decodes_into_events() {
        let v = json!({
            "id": "c1", "model": "gpt", "created": 7,
            "choices": [{ "index": 0, "finish_reason": "tool_calls", "message": {
                "role": "assistant", "content": "ok", "refusal": null,
                "tool_calls": [{ "id": "t", "type": "function", "function": { "name": "f", "arguments": "{}" } }],
            }}],
            "usage": { "prompt_tokens": 2, "completion_tokens": 1 },
        });
        let mut start = Output::start("c1", "gpt");
        if let Output::Start(s) = &mut start {
            let fields = json!({ "created": 7 });
            s.native = Some(NativeFields::new(
                OPENAI,
                fields.as_object().cloned().unwrap_or_default(),
            ));
        }
        let mut totals = usage(units::INPUT_TOKENS, 2);
        if let Output::Usage(u) = &mut totals {
            u.add(units::OUTPUT_TOKENS, 1);
        }
        assert_eq!(
            decode_body(OpenAiChat::OPENAI, &v),
            [
                start,
                totals,
                Output::block_start(0, Block::Text),
                Output::text(0, "ok"),
                Output::block_end(0),
                Output::block_start(1, call("t", "f")),
                Output::arguments(1, "{}"),
                Output::block_end(1),
                Output::finished(Finish::ToolUse),
            ]
        );
        // Logprobs and content that is not text are other content too.
        let logprobs = json!({ "choices": [{ "index": 0, "logprobs": { "content": [] },
            "message": { "role": "assistant", "content": "ok" } }] });
        assert!(matches!(
            decode_body(OpenAiChat::OPENAI, &logprobs).first(),
            Some(Output::Native(NativeValue { .. }))
        ));
        let parts = json!({ "choices": [{ "index": 0,
            "message": { "role": "assistant", "content": [{ "type": "text", "text": "ok" }] } }] });
        assert!(matches!(
            decode_body(OpenAiChat::OPENAI, &parts).first(),
            Some(Output::Native(NativeValue { .. }))
        ));
        // A whole body collects what the events say, and keeps its other fields.
        let again = body_of(&decode_body(OpenAiChat::OPENAI, &v)).expect("a body");
        assert_eq!(again["created"], 7);
        let (said, sent) = (&again["choices"][0]["message"], &v["choices"][0]["message"]);
        assert_eq!(said["content"], "ok");
        assert_eq!(said["tool_calls"], sent["tool_calls"]);
        assert_eq!(again["choices"][0]["finish_reason"], "tool_calls");
        assert_eq!(again["usage"]["prompt_tokens"], 2);
    }

    #[test]
    fn unsupported_usage_edits_fail_instead_of_disappearing() {
        let mut synthetic = vec![Output::start("x", "m")];
        synthetic.extend(text_events(0, "ok"));
        synthetic.push(usage("credits", 3));
        assert!(stream_of(&synthetic, true).is_err());
        assert!(body_of(&synthetic).is_err());
        // Cached tokens are part of the input: more than it is no usage.
        let more = [usage(units::CACHED_INPUT_TOKENS, 3)];
        assert!(body_of(&more).is_err());
    }

    #[test]
    fn groq_usage_is_read_from_beside_the_response_and_its_other_fields_are_kept() {
        let events = [
            "data: {\"id\":\"g\",\"choices\":[],\"x_groq\":{\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"vendor\":\"a\"},\"queue_time\":0.2}}\n\n",
            "id: groq\nevent: completion.chunk\ndata: {\"choices\":[],\"x_groq\":{\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":7,\"vendor\":\"b\"},\"queue_time\":0.3}}\n\n",
            "data: [DONE]\n\n",
        ];
        let (out, _) = decode_all_in(OpenAiChat::GROQ, &events);
        let sums: Vec<_> = out
            .iter()
            .filter_map(|r| match r {
                Output::Usage(u) => u.get(units::OUTPUT_TOKENS),
                _ => None,
            })
            .collect();
        assert_eq!(sums, [1, 6]);
        let Some(Output::Start(start)) = out.first() else {
            panic!("{out:?}");
        };
        assert_eq!(
            start.native.fields()["x_groq"],
            json!({ "queue_time": 0.2 })
        );
        let stream = stream_of(&out, true).expect("a stream");
        let last = chunks_of(&stream).pop().unwrap_or_default();
        assert_eq!(last["usage"]["completion_tokens"], 7);
        assert_eq!(last["x_groq"], json!({ "queue_time": 0.2 }));
    }

    #[test]
    fn whole_usage_keeps_its_details_and_groq_usage_is_counted() {
        for groq in [false, true] {
            let u = json!({
                "prompt_tokens": 4, "completion_tokens": 7, "total_tokens": 11,
                "prompt_tokens_details": {"cached_tokens": 2, "audio_tokens": 1},
                "completion_tokens_details": {"reasoning_tokens": 3},
            });
            let mut v = json!({"id": "c", "model": "m", "choices": [{"index": 0,
                "message": {"role": "assistant", "content": "hi"}, "finish_reason": "stop"}]});
            match groq {
                false => v["usage"] = u,
                true => v["x_groq"] = json!({ "usage": u, "elapsed": 12 }),
            }
            let events = decode_body(OpenAiChat::GROQ, &v);
            let total = events.iter().fold(Usage::default(), |mut all, r| {
                if let Output::Usage(u) = r {
                    all += u;
                }
                all
            });
            for (unit, n) in [
                (units::INPUT_TOKENS, 4),
                (units::OUTPUT_TOKENS, 7),
                (units::CACHED_INPUT_TOKENS, 2),
                (units::REASONING_TOKENS, 3),
            ] {
                assert_eq!(total.get(unit), Some(n), "groq {groq}");
            }
            let again = body_of(&events).expect("a body");
            let usage = &again["usage"];
            assert_eq!(usage["total_tokens"], 11);
            // What has no unit travels with the response.
            assert_eq!(
                usage["prompt_tokens_details"],
                json!({"cached_tokens": 2, "audio_tokens": 1})
            );
            assert_eq!(
                usage["completion_tokens_details"],
                json!({"reasoning_tokens": 3})
            );
            if groq {
                assert_eq!(again["x_groq"], json!({ "elapsed": 12 }));
            }
        }
    }

    #[test]
    fn a_usage_chunk_the_client_did_not_ask_for_is_not_written() {
        let first = "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":3}}\n\n";
        // OpenAI repeats the totals in a usage chunk the client did not ask
        // for; its own fields, such as `obfuscation`, do not keep it.
        let again =
            "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":3},\"obfuscation\":\"xyz\"}\n\n";
        let (replies, end) = decode_all(&[first, again, "data: [DONE]\n\n"]);
        assert!(end.is_ok());
        let counted: Vec<_> = replies
            .iter()
            .filter_map(|r| match r {
                Output::Usage(u) => u.get(units::INPUT_TOKENS),
                _ => None,
            })
            .collect();
        assert_eq!(counted, [3]);
        // It counts, but neither it nor the usage on a chunk with choices is
        // written.
        let out = stream_of(&replies, false).expect("a stream");
        assert!(!out.contains("usage") && out.contains("\"stop\""), "{out}");
        assert!(!out.contains("obfuscation"), "{out}");
        assert!(out.ends_with("data: [DONE]\n\n"));
    }

    #[test]
    fn nested_groq_usage_is_hidden_when_not_requested() {
        let (replies, _) = decode_all_in(
            OpenAiChat::GROQ,
            &[
                "id: groq\ndata: {\"id\":\"g\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}],\"x_groq\":{\"usage\":{\"prompt_tokens\":1},\"trace\":\"x\"}}\n\n",
                "data: [DONE]\n\n",
            ],
        );
        let out = stream_of(&replies, false).expect("a stream");
        assert!(out.contains("\"trace\":\"x\""), "{out}");
        assert!(!out.contains("\"usage\""), "{out}");
    }

    #[test]
    fn an_sse_envelope_is_no_part_of_a_response() {
        let plain = "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"secret\"}}]}\n\n";
        let wrapped = "id: 7\nevent: completion.chunk\nretry: 1500\n: kept\ndata: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"secret\"}}]}\n\n";
        assert_eq!(decode_all(&[plain]).0, decode_all(&[wrapped]).0);
    }

    #[test]
    fn stream_usage_is_cumulative_on_the_wire_and_written_as_one_total() {
        let events = [
            "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2,\"vendor\":9}}\n\n",
            "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":7,\"total_tokens\":8,\"vendor\":10}}\n\n",
            "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":7,\"total_tokens\":8,\"vendor\":11}}\n\n",
            "data: [DONE]\n\n",
        ];
        let (mut replies, _) = decode_all(&events);
        let outputs: Vec<_> = replies
            .iter()
            .filter_map(|r| match r {
                Output::Usage(u) => u.get(units::OUTPUT_TOKENS),
                _ => None,
            })
            .collect();
        assert_eq!(outputs, [1, 6]);
        replies.retain(|r| matches!(r, Output::Usage(_)));
        // A plugin lowers the second increment: the total follows it.
        replies[1] = usage(units::OUTPUT_TOKENS, 2);
        let stream = stream_of(&replies, true).expect("a stream");
        let last = chunks_of(&stream).pop().unwrap_or_default();
        assert_eq!(last["usage"]["completion_tokens"], 3, "{stream}");
    }

    #[test]
    fn dropped_usage_does_not_reappear_in_later_totals() {
        for groq in [false, true] {
            let events: Vec<_> = [1, 7, 7]
                .map(|n| {
                    let usage = json!({"completion_tokens": n, "vendor": 9});
                    let event = if groq {
                        json!({"choices": [], "x_groq": {"usage": usage}})
                    } else {
                        json!({"choices": [], "usage": usage})
                    };
                    format!("data: {event}\n\n")
                })
                .into();
            let chat = if groq {
                OpenAiChat::GROQ
            } else {
                OpenAiChat::OPENAI
            };
            let (mut replies, _) = decode_all_in(
                chat,
                &[&events[0], &events[1], &events[2], "data: [DONE]\n\n"],
            );
            // The second report, which adds 6, is dropped.
            replies.retain(
                |r| !matches!(r, Output::Usage(u) if u.get(units::OUTPUT_TOKENS) == Some(6)),
            );
            let stream = stream_of(&replies, true).expect("a stream");
            let again = decode_all(&[&stream]).0;
            let total: u64 = again
                .iter()
                .filter_map(|r| match r {
                    Output::Usage(u) => u.get(units::OUTPUT_TOKENS),
                    _ => None,
                })
                .sum();
            assert_eq!(total, 1, "groq={groq}: {stream}");
        }
    }

    #[test]
    fn usage_never_decreases_and_edits_upward_are_summed() {
        for (totals, edited, expected) in [([7, 1, 0], None, 7), ([1, 7, 7], Some(3), 9)] {
            let events = totals.map(|n| {
                format!("data: {{\"choices\":[],\"usage\":{{\"completion_tokens\":{n}}}}}\n\n")
            });
            let (mut replies, _) =
                decode_all(&[&events[0], &events[1], &events[2], "data: [DONE]\n\n"]);
            if let Some(amount) = edited {
                replies[0] = usage(units::OUTPUT_TOKENS, amount);
            }
            let stream = stream_of(&replies, true).expect("a stream");
            let last = chunks_of(&stream).pop().unwrap_or_default();
            assert_eq!(last["usage"]["completion_tokens"], expected, "{stream}");
        }
    }

    /// The request `body` with what the format adds ([`Codec::finish`]).
    fn with_usage(body: &str) -> Value {
        let mut v: Map<String, Value> = serde_json::from_str(body).unwrap_or_default();
        OpenAiChat::finish(&mut v);
        v.into()
    }

    #[test]
    fn a_request_that_needs_nothing_goes_as_it_came() {
        let body = r#"{"model":"a","stream":false}"#;
        assert_eq!(
            with_usage(body),
            serde_json::from_str::<Value>(body).unwrap_or_default()
        );
    }

    #[test]
    fn streaming_requests_ask_for_usage() {
        assert_eq!(
            with_usage(r#"{"model":"a","stream":true}"#),
            json!({"model":"a","stream":true,"stream_options":{"include_usage":true}})
        );
    }

    #[test]
    fn null_stream_options_still_ask_for_usage() {
        let body = r#"{"model":"a","stream":true,"stream_options":null}"#;
        assert_eq!(
            with_usage(body),
            json!({"model":"a","stream":true,"stream_options":{"include_usage":true}})
        );
    }

    #[test]
    fn asking_for_usage_keeps_the_clients_other_stream_options() {
        let body = r#"{"model":"a","stream":true,"stream_options":{"include_obfuscation":false}}"#;
        assert_eq!(
            with_usage(body),
            json!({"model":"a","stream":true,"stream_options":{"include_obfuscation":false,"include_usage":true}})
        );
    }

    /// The client's `body` as the conversation it says, with `edit` made,
    /// written as a request.
    fn written(body: Value, edit: impl FnOnce(&mut Input)) -> Result<Value, Fault> {
        let mut chat = OpenAiChat::OPENAI.decode_request(Bytes::from(body.to_string()))?;
        edit(&mut chat);
        Ok(OpenAiChat::OPENAI.encode_request(&chat)?.into())
    }

    #[test]
    fn a_request_round_trips_through_a_conversation() {
        let body = json!({
            "model": "m",
            "messages": [
                { "role": "system", "content": "be brief" },
                { "role": "user", "content": [{ "type": "text", "text": "hi" }] },
                { "role": "developer", "content": "and kind" },
            ],
            "max_tokens": 5,
            "stop": "x",
            "n": 1,
        });
        assert_eq!(
            written(body, |_| {}).ok(),
            Some(json!({
                "model": "m",
                "messages": [
                    { "role": "system", "content": "be brief" },
                    { "role": "user", "content": "hi" },
                    { "role": "developer", "content": "and kind" },
                ],
                "max_completion_tokens": 5,
                "stop": ["x"],
            }))
        );
    }

    /// Hides an email in tool calls and results, as a PII plugin does.
    fn hide_email(chat: &mut Input) {
        for part in chat.messages.iter_mut().flat_map(|m| &mut m.content) {
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
    }

    #[test]
    fn an_edited_tool_call_and_result_are_written_into_the_request() {
        let call = json!({ "id": "c1", "type": "function",
            "function": { "name": "lookup", "arguments": "{\"email\":\"bob@x.io\"}" } });
        let tool = json!({ "type": "function", "function": { "name": "lookup", "strict": true,
            "parameters": { "type": "object" } } });
        let body = json!({ "model": "m", "tools": [tool], "tool_choice": "required",
        "messages": [
            { "role": "user", "content": "find bob" },
            { "role": "assistant", "content": null, "tool_calls": [call] },
            { "role": "tool", "tool_call_id": "c1", "content": "bob@x.io is Bob" },
        ]});
        let chat = OpenAiChat::OPENAI.read(&body);
        assert_eq!(
            chat.messages[1].content,
            [Content::tool_call(
                "c1",
                "lookup",
                "{\"email\":\"bob@x.io\"}"
            )]
        );
        assert_eq!(chat.options.tool_choice, Some(ToolChoice::Required));
        assert_eq!(
            chat.tools[0].native.fields().get("strict"),
            Some(&json!(true))
        );
        assert_eq!(chat.tools[0].native.format(), Some(OPENAI));
        assert!(chat.native.is_empty());
        assert_eq!(written(body.clone(), |_| {}).ok(), Some(body.clone()));
        let mut expected = body.clone();
        expected["messages"][1]["tool_calls"][0]["function"]["arguments"] =
            "{\"email\":\"<EMAIL_1>\"}".into();
        expected["messages"][2]["content"] = "<EMAIL_1> is Bob".into();
        assert_eq!(written(body, hide_email).ok(), Some(expected));
    }

    #[test]
    fn tool_results_and_calls_are_written_as_openai_messages() {
        let mut both = Message::new(Role::User, "and then?");
        both.content
            .insert(0, Content::tool_result("c1", vec!["42".into()], false));
        let mut asked = Message::new(Role::Assistant, "");
        asked.content = vec![Content::tool_call("c1", "f", "{}")];
        let chat = Input::new([asked, both]);
        let v = OpenAiChat::OPENAI.encode_request(&chat).map(Value::Object);
        assert_eq!(
            v.ok().map(|v| v["messages"].clone()),
            Some(json!([
                { "role": "assistant", "content": null, "tool_calls": [
                    { "id": "c1", "type": "function", "function": { "name": "f", "arguments": "{}" } },
                ] },
                { "role": "tool", "tool_call_id": "c1", "content": "42" },
                { "role": "user", "content": "and then?" },
            ]))
        );
        // OpenAI cannot say a call failed.
        let mut failed = Message::new(Role::User, "");
        failed.content = vec![Content::tool_result("c1", vec!["no".into()], true)];
        assert!(
            OpenAiChat::OPENAI
                .encode_request(&Input::new([failed]))
                .is_err()
        );
    }

    #[test]
    fn what_a_conversation_cannot_hold_is_kept_or_refused() {
        let body = json!({ "model": "m", "messages": [
            { "role": "user", "name": "alice", "content": "hi" },
        ], "tools": [{ "type": "function", "function": { "name": "f" } }], "seed": 7 });
        let chat = OpenAiChat::OPENAI.read(&body);
        assert_eq!(chat.native.fields().get("seed"), Some(&json!(7)));
        assert_eq!(chat.native.format(), Some(OPENAI));
        // A function tool is the kind's.
        assert_eq!(chat.tools, [Tool::new("f", Value::Null)]);
        assert_eq!(chat.messages[0].text(), "hi");
        assert_eq!(
            chat.messages[0].native.fields().get("name"),
            Some(&json!("alice"))
        );
        // To another format it is refused; to OpenAI it is kept.
        let err = AnthropicMessages.encode_request(&chat).err();
        assert_eq!(
            err.map(|e| respan_gateway_http::status(&e).as_u16()),
            Some(400)
        );
        assert_eq!(written(body.clone(), |_| {}).ok(), Some(body));
        // With the native fields cleared and only plain messages, it is sent.
        let mut chat = chat;
        chat.native = None;
        chat.messages = vec![Message::new(Role::User, "hi")];
        assert!(AnthropicMessages.encode_request(&chat).is_ok());
    }

    #[test]
    fn several_choices_are_refused_as_a_bad_request() {
        let body = json!({ "model": "m", "messages": [], "n": 2 });
        let chat = OpenAiChat::OPENAI.read(&body);
        assert_eq!(chat.native.fields().get("n"), Some(&json!(2)));
        let refused = OpenAiChat::OPENAI.encode_request(&chat).err();
        assert!(refused.is_some_and(|f| f.is::<respan_gateway_sdk::BadRequest>()));
        // One choice is the default, and says nothing.
        let one = OpenAiChat::OPENAI.read(&json!({ "model": "m", "messages": [], "n": 1 }));
        assert!(one.native.is_empty());
    }

    #[test]
    fn the_user_field_is_read_redacted_and_translated() {
        let body = json!({ "model": "m", "user": "bob@x.io",
            "messages": [{ "role": "user", "content": "hi" }] });
        let chat = OpenAiChat::OPENAI.read(&body);
        assert_eq!(chat.user.as_deref(), Some("bob@x.io"));
        assert!(chat.native.is_empty());
        // Kept as it came, or changed or removed by a plugin.
        assert_eq!(written(body.clone(), |_| {}).ok(), Some(body.clone()));
        let redacted = written(body.clone(), |c| c.user = Some("[x]".into()));
        assert_eq!(redacted.ok().map(|v| v["user"].clone()), Some(json!("[x]")));
        let removed = written(body.clone(), |c| c.user = None);
        assert_eq!(
            removed.ok(),
            Some(json!({ "model": "m", "messages": [{ "role": "user", "content": "hi" }] }))
        );
        // To Anthropic it is the metadata's user id, and back.
        let sent = AnthropicMessages.encode_request(&chat).map(Value::from);
        let sent = sent.unwrap_or_default();
        assert_eq!(sent["metadata"], json!({ "user_id": "bob@x.io" }));
        let back = OpenAiChat::OPENAI.encode_request(&AnthropicMessages.read(&sent));
        assert_eq!(
            back.ok().map(|v| v["user"].clone()),
            Some(json!("bob@x.io"))
        );
        // A user that is not a string is not the kind's: it stays in `native`.
        let odd = OpenAiChat::OPENAI.read(&json!({ "user": 7, "messages": [] }));
        assert_eq!(
            (odd.user, odd.native.fields().get("user")),
            (None, Some(&json!(7)))
        );
    }

    #[test]
    fn a_plugin_that_removes_tools_sends_the_request_without_them() {
        let body = json!({ "model": "m", "messages": [], "tools": [{ "type": "function" }] });
        let edited = written(body, |c| {
            c.native.fields_mut(OPENAI).shift_remove("tools");
            c.native.fields_mut(OPENAI).insert("seed".into(), 7.into());
        });
        assert_eq!(
            edited.ok(),
            Some(json!({ "model": "m", "messages": [], "seed": 7 }))
        );
    }

    #[test]
    fn the_limit_is_read_as_the_larger_spelling_and_written_canonically() {
        let v = json!({ "max_tokens": 100_000, "max_completion_tokens": 10, "messages": [] });
        assert_eq!(
            OpenAiChat::OPENAI.read(&v).options.max_output_tokens,
            Some(100_000)
        );
        for (limits, want) in [
            (
                json!({ "max_tokens": 4000 }),
                json!({ "max_completion_tokens": 10 }),
            ),
            (
                json!({ "max_completion_tokens": 4000 }),
                json!({ "max_completion_tokens": 10 }),
            ),
            (
                json!({ "max_tokens": 4000, "max_completion_tokens": 4000 }),
                json!({ "max_completion_tokens": 10 }),
            ),
            (json!({}), json!({ "max_completion_tokens": 10 })),
        ] {
            let mut body = json!({ "model": "m", "messages": [] });
            if let Some(fields) = body.as_object_mut() {
                fields.extend(limits.as_object().cloned().unwrap_or_default());
            }
            let edited = written(body.clone(), |c| c.options.max_output_tokens = Some(10))
                .unwrap_or_default();
            for key in ["max_tokens", "max_completion_tokens"] {
                assert_eq!(edited.get(key), want.get(key), "{body}: {edited}");
            }
            // Removed, it is gone under every spelling.
            let none = written(body, |c| c.options.max_output_tokens = None).unwrap_or_default();
            assert!(
                none.get("max_tokens").is_none() && none.get("max_completion_tokens").is_none()
            );
        }
        // Another format writes its own spelling.
        let chat = OpenAiChat::OPENAI
            .read(&json!({ "max_tokens": 7, "messages": [{"role":"user","content":"hi"}] }));
        let sent = AnthropicMessages.encode_request(&chat).map(Value::from);
        assert_eq!(sent.map(|v| v["max_tokens"].clone()).ok(), Some(json!(7)));
    }

    #[test]
    fn whether_it_streams_is_written_with_its_options() {
        let body = json!({ "model": "m", "stream": true, "stream_options": { "include_usage": true },
            "messages": [{ "role": "user", "content": "hi" }] });
        let out = written(body, |_| {}).unwrap_or_default();
        assert!(out.get("stream").is_none());
        assert!(out.get("stream_options").is_none());
    }

    #[test]
    fn a_removed_field_is_not_written_and_the_others_are() {
        let body =
            json!({ "model": "m", "temperature": 0.5, "messages": [], "top_p": 0.9, "seed": 1 });
        let out = written(body, |c| c.options.temperature = None).unwrap_or_default();
        assert_eq!(
            out,
            json!({ "model": "m", "messages": [], "top_p": 0.9, "seed": 1 })
        );
    }

    #[test]
    fn keys_set_to_null_say_nothing() {
        let body = json!({ "model": "m", "tools": null, "n": null, "messages": [
            { "role": "user", "name": null, "content": "hi" },
            { "role": "assistant", "content": "yo", "refusal": null },
        ]});
        let chat = OpenAiChat::OPENAI.read(&body);
        assert!(chat.native.is_empty(), "{:?}", chat.native);
        assert_eq!(chat.messages.len(), 2);
        assert!(chat.messages.iter().all(Message::is_plain));
    }

    #[test]
    fn an_edited_message_is_written_with_its_role_and_its_other_fields() {
        let body = json!({ "model": "m", "messages": [
            { "role": "developer", "content": "key sk-1", "name": "ops" },
        ]});
        let edited = written(body, |c| c.messages[0].content = vec!["key [x]".into()]);
        // The message keeps its spelling of the role, and its name.
        assert_eq!(
            edited.map(|v| v["messages"].clone()).ok(),
            Some(json!([{ "role": "developer", "content": "key [x]", "name": "ops" }]))
        );
    }

    #[test]
    fn a_system_prompt_can_be_added_to_a_conversation_with_tool_calls() {
        let call = json!({ "role": "assistant", "content": null, "tool_calls": [{ "id": "t", "type": "function", "function": { "name": "f", "arguments": "{}" } }] });
        let result = json!({ "role": "tool", "tool_call_id": "t", "content": "42" });
        let body = json!({ "model": "m", "messages": [
            { "role": "user", "content": "hi" }, call.clone(), result.clone(),
            { "role": "user", "content": "thanks" },
        ]});
        let edited = written(body, |c| {
            c.messages.insert(0, Message::new(Role::System, "be kind"))
        });
        assert_eq!(
            edited.map(|v| v["messages"].clone()).ok(),
            Some(json!([
                { "role": "system", "content": "be kind" },
                { "role": "user", "content": "hi" }, call, result,
                { "role": "user", "content": "thanks" },
            ]))
        );
    }

    #[test]
    fn what_a_message_cannot_hold_is_kept_where_it_is() {
        let image = json!({ "type": "image_url", "image_url": { "url": "u" } });
        let tool = json!({ "role": "tool", "tool_call_id": "t", "content": "42" });
        let body = json!({ "model": "m", "messages": [
            { "role": "user", "content": [{ "type": "text", "text": "what is this?" }, image] },
            tool,
        ]});
        let chat = OpenAiChat::OPENAI.read(&body);
        assert_eq!(chat.messages[0].text(), "what is this?");
        // A tool message is a user message of one result.
        assert_eq!(chat.messages[1].role, Role::User);
        assert_eq!(
            chat.messages[1].content,
            [Content::tool_result("t", vec!["42".into()], false)]
        );
        assert!(chat.messages[1].native.is_empty());
        // The text is edited in its part, beside the image and tool result.
        let edited = written(body, |c| {
            c.messages[0].content[0] = "WHAT IS THIS?".into();
        });
        assert_eq!(
            edited.map(|v| v["messages"].clone()).ok(),
            Some(json!([
                { "role": "user", "content": [{ "type": "text", "text": "WHAT IS THIS?" }, image] },
                tool,
            ]))
        );
    }

    #[test]
    fn a_long_conversation_changed_throughout_is_written_message_by_message() {
        let messages: Vec<Value> = (0..5000)
            .map(|i| json!({ "role": "user", "content": format!("m{i}") }))
            .collect();
        let body = json!({ "model": "m", "messages": messages });
        let edited = written(body, |c| {
            c.messages.insert(0, Message::new(Role::System, "be kind"));
            for m in &mut c.messages[1..] {
                m.content = vec![m.text().to_uppercase().into()];
            }
        });
        let messages = edited.map(|v| v["messages"].clone()).unwrap_or_default();
        assert_eq!(
            messages[0],
            json!({ "role": "system", "content": "be kind" })
        );
        assert_eq!(
            messages[5000],
            json!({ "role": "user", "content": "M4999" })
        );
    }

    #[test]
    fn an_insert_and_an_edit_elsewhere_keep_the_messages_between() {
        // Claude Code marks cache breakpoints on earlier messages: a part
        // with a marker is the format's own, and is written as it is.
        let cached = json!({ "role": "user", "content": [
            { "type": "text", "text": "the repo", "cache_control": { "type": "ephemeral" } },
        ]});
        let body = json!({ "model": "m", "messages": [
            cached,
            { "role": "assistant", "content": "ok" },
            { "role": "user", "content": "my SSN is 123" },
        ]});
        let edited = written(body, |c| {
            c.messages.insert(0, Message::new(Role::System, "be kind"));
            c.messages[3].content = vec!["my SSN is [redacted]".into()];
        });
        assert_eq!(
            edited.map(|v| v["messages"].clone()).ok(),
            Some(json!([
                { "role": "system", "content": "be kind" },
                cached,
                { "role": "assistant", "content": "ok" },
                { "role": "user", "content": "my SSN is [redacted]" },
            ]))
        );
    }

    #[test]
    fn messages_are_added_where_the_plugin_puts_them_among_images() {
        let image = json!({ "role": "user", "content": [{ "type": "image_url", "image_url": { "url": "u" } }] });
        let body = json!({ "model": "m", "messages": [image] });
        let added = written(body, |c| {
            c.messages.insert(0, Message::new(Role::System, "be kind"));
            c.messages.push(Message::new(Role::User, "and"));
        });
        assert_eq!(
            added.map(|v| v["messages"].clone()).ok(),
            Some(json!([
                { "role": "system", "content": "be kind" },
                image,
                { "role": "user", "content": "and" },
            ]))
        );
    }

    #[test]
    fn a_message_added_to_a_request_with_none_does_not_panic() {
        let edited = written(json!({ "model": "m" }), |c| {
            c.messages.insert(0, Message::new(Role::System, "be kind"));
        });
        assert_eq!(
            edited.map(|v| v["messages"].clone()).ok(),
            Some(json!([{ "role": "system", "content": "be kind" }]))
        );
    }

    #[test]
    fn an_edit_changes_only_what_the_edit_changed() {
        let body = json!({
            "model": "m", "temperature": 0.5, "top_p": 0.25, "stop": ["x"],
            "messages": [{ "role": "user", "content": "hi" }],
        });
        let edited = written(body.clone(), |c| {
            c.messages[0] = Message::new(Role::User, "HI")
        });
        let mut expected = body;
        expected["messages"][0]["content"] = "HI".into();
        assert_eq!(edited.ok(), Some(expected));
    }

    #[test]
    fn an_edit_keeps_messages_a_conversation_cannot_hold() {
        let image = json!({ "role": "user", "content": [{ "type": "image_url", "image_url": { "url": "u", "detail": "high" } }] });
        let body = json!({ "model": "m", "messages": [
            { "role": "user", "content": "hi" }, image.clone(),
        ]});
        let edited = written(body.clone(), |c| {
            c.messages[0] = Message::new(Role::User, "HI")
        });
        assert_eq!(
            edited.map(|v| v["messages"].clone()).ok(),
            Some(json!([{ "role": "user", "content": "HI" }, image]))
        );
        // Removing it removes it; sending it in another format is refused.
        let removed = written(body.clone(), |c| drop(c.messages.pop()));
        assert_eq!(
            removed.map(|v| v["messages"].clone()).ok(),
            Some(json!([{ "role": "user", "content": "hi" }]))
        );
        let chat = OpenAiChat::OPENAI.read(&body);
        assert!(AnthropicMessages.encode_request(&chat).is_err());
        assert!(OpenAiChat::OPENAI.encode_request(&chat).is_ok());
    }

    #[test]
    fn what_is_already_in_the_targets_format_crosses_and_the_rest_is_refused() {
        let pdf = json!({ "type": "document", "source": { "type": "base64",
            "media_type": "application/pdf", "data": "JVBERi0=" } });
        let mut chat = OpenAiChat::OPENAI.read(&json!({ "model": "m", "messages": [
            { "role": "user", "content": [{ "type": "text", "text": "read" },
                { "type": "file", "file": { "file_data": "x" } }] }] }));
        // A part in OpenAI's shape does not reach Anthropic.
        assert!(AnthropicMessages.encode_request(&chat).is_err());
        // One rewritten into Anthropic's does, as it is.
        chat.messages[0].content[1] = Content::native("anthropic-messages", pdf.clone());
        let sent = AnthropicMessages.encode_request(&chat).map(Value::from);
        assert_eq!(
            sent.map(|v| v["messages"][0]["content"][1].clone()).ok(),
            Some(pdf)
        );
        // The same holds for the fields of a request.
        let thinking = json!({ "thinking": { "type": "enabled" } });
        let fields = thinking.as_object().cloned().unwrap_or_default();
        chat.native = Some(NativeFields::new(OPENAI, fields.clone()));
        assert!(AnthropicMessages.encode_request(&chat).is_err());
        chat.native = Some(NativeFields::new("anthropic-messages", fields));
        let sent = AnthropicMessages.encode_request(&chat).map(Value::from);
        assert_eq!(
            sent.map(|v| v["thinking"].clone()).ok(),
            Some(json!({ "type": "enabled" }))
        );
    }

    /// Fields tagged as another format's are refused in a request, as a
    /// message's and a tool's are.
    #[test]
    fn fields_in_another_format_are_not_written_into_a_request() {
        let body = json!({ "model": "m", "messages": [{ "role": "user", "content": "hi" }],
            "tools": [{ "type": "function", "function": { "name": "f" } }] });
        let chat = OpenAiChat::OPENAI.read(&body);
        let thinking = json!({ "thinking": { "type": "enabled" } });
        let fields = thinking.as_object().cloned().unwrap_or_default();
        let mut foreign = chat.clone();
        foreign.native = Some(NativeFields::new("anthropic-messages", fields.clone()));
        let mut message = chat.clone();
        message.messages[0].native = Some(NativeFields::new("anthropic-messages", fields.clone()));
        let mut tool = chat.clone();
        tool.tools[0].native = Some(NativeFields::new("anthropic-messages", fields.clone()));
        let mut role = chat.clone();
        role.messages[0].role = Role::native("anthropic-messages", "critic");
        for chat in [foreign, message, tool, role] {
            let sent = OpenAiChat::OPENAI.encode_request(&chat);
            assert!(sent.is_err(), "sent: {:?}", sent.map(Value::from));
        }
        // Tagged for this format, the same is written.
        let mut own = chat;
        own.native = Some(NativeFields::new(OPENAI, fields));
        let sent = OpenAiChat::OPENAI.encode_request(&own).map(Value::from);
        assert_eq!(
            sent.map(|v| v["thinking"].clone()).ok(),
            Some(json!({ "type": "enabled" }))
        );
    }

    #[test]
    fn a_stream_made_from_events_always_finishes() {
        let mut replies = vec![Output::start("x", "m")];
        replies.extend(text_events(0, "hi"));
        let out = stream_of(&replies, false).expect("a stream");
        assert!(out.contains("\"finish_reason\":\"stop\""), "{out}");
        assert!(out.ends_with("data: [DONE]\n\n"), "{out}");
    }

    #[test]
    fn a_translated_tool_call_is_refused() {
        // An event another format has no form in this one for is refused...
        let other = Output::native(
            "anthropic-messages",
            json!({ "type": "content_block_start" }),
        );
        assert!(stream_of(&[other], false).is_err());
        // ... but a keep-alive changes no answer, and is dropped.
        let ping = Output::native("anthropic-messages", json!({ "type": "ping" }));
        assert!(stream_of(&[ping], false).is_ok());
    }

    // What a chunk has no place for is written beside the text.
    #[test]
    fn text_a_chunk_has_no_place_for_is_written_beside_it() {
        let (replies, _) = decode_all(&[
            "data: {\"id\":\"x\",\"choices\":[{\"index\":1,\"delta\":{\"content\":\"B\"}}]}\n\n",
        ]);
        let mut edited = replies;
        edited.extend(text_events(0, "note "));
        let out = stream_of(&edited, true).expect("a stream");
        assert!(
            out.contains("\"content\":\"note \"") && out.contains("\"index\":1"),
            "{out}"
        );
    }

    #[test]
    fn a_whole_body_joins_every_text_and_keeps_reasoning_and_calls() {
        let replies = [
            Output::start("x", "m"),
            Output::block_start(0, Block::Reasoning),
            Output::reasoning(0, "hm"),
            Output::block_end(0),
            Output::block_start(1, Block::Text),
            Output::text(1, "one "),
            Output::block_end(1),
            Output::block_start(2, Block::Text),
            Output::text(2, "two"),
            Output::block_end(2),
            Output::block_start(3, call("t", "f")),
            Output::block_end(3),
        ];
        let body = body_in(OpenAiChat::DEEPSEEK, &replies).expect("a body");
        let message = &body["choices"][0]["message"];
        assert_eq!(message["content"], "one two");
        assert_eq!(message["reasoning_content"], "hm");
        // A call with no arguments has none, as JSON.
        assert_eq!(message["tool_calls"][0]["function"]["arguments"], "{}");
        assert_eq!(body["choices"][0]["finish_reason"], "stop");
        // OpenAI's own format has no field for reasoning.
        assert!(body_of(&replies).is_err());
        assert!(stream_of(&replies, false).is_err());
    }

    #[test]
    fn a_dialect_with_reasoning_reads_and_writes_it_in_requests() {
        let body = json!({ "model": "m", "messages": [
            { "role": "user", "content": "hi" },
            { "role": "assistant", "content": "ok", "reasoning_content": "hm" },
        ]});
        let chat = OpenAiChat::DEEPSEEK.decode_request(Bytes::from(body.to_string()));
        let chat = chat.expect("a request");
        assert_eq!(
            chat.messages[1].content,
            [
                Content::Reasoning {
                    text: "hm".into(),
                    native: None
                },
                Content::from("ok")
            ]
        );
        let sent = OpenAiChat::DEEPSEEK.encode_request(&chat).map(Value::from);
        assert_eq!(sent.ok(), Some(body));
        // OpenAI's own format has no field for it; Anthropic's thinking is.
        assert!(OpenAiChat::OPENAI.encode_request(&chat).is_err());
        let sent = AnthropicMessages.encode_request(&chat).map(Value::from);
        let thinking = json!({ "type": "thinking", "thinking": "hm" });
        assert_eq!(
            sent.ok().map(|v| v["messages"][1]["content"][0].clone()),
            Some(thinking)
        );
    }

    #[test]
    fn the_fault_says_message_and_type_the_provider_the_rest() {
        let details = json!({"message": "", "type": "https://x/p", "code": null, "native": 1});
        let fault = BadRequest::fault("m")
            .with(Details(details.as_object().expect("error fields").clone()));
        let body = OpenAiChat::error_body(&fault);
        // The fault's words are the format's; the provider's `code` stays.
        let kept = json!({"message": "m", "type": "bad_request", "param": null, "code": null, "native": 1});
        assert_eq!(body["error"], kept);
    }

    #[test]
    fn a_plugins_fault_takes_one_shape_on_every_path() {
        let details = json!({"message": "x", "type": 5, "code": 7, "param": 3, "category": "pii"});
        let fault = Fault::new("pii")
            .with(ErrorCode("blocked".into()))
            .with(Status(StatusCode::FORBIDDEN))
            .with(Details(details.as_object().expect("error fields").clone()));
        let body = OpenAiChat::error_body(&fault);
        // OpenAI's `code` and `param` are strings: a number is not kept.
        let kept = json!({"message": "pii", "type": "blocked", "param": null, "code": "blocked", "category": "pii"});
        assert_eq!(body["error"], kept);
        // A string, as OpenAI's own errors give, is kept.
        let details = json!({"param": "messages", "code": "context_length_exceeded"});
        let fault = BadRequest::fault("too long")
            .with(Details(details.as_object().expect("error fields").clone()));
        let error = &OpenAiChat::error_body(&fault)["error"];
        assert_eq!(
            (&error["param"], &error["code"]),
            (&json!("messages"), &json!("context_length_exceeded"))
        );
    }
}
