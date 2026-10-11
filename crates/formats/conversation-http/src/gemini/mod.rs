//! Google Gemini: `POST /v1beta/models/{model}:generateContent` and
//! `:streamGenerateContent?alt=sse`, JSON or SSE, of the conversation kind,
//! as a [`Schema`] ([`GeminiGenerate`]).
//!
//! The model is in the path, not the body: a client's request names it there
//! (`models/` is left off), and a call to a provider writes it there from the
//! input's model. A stream is asked for by the method (`streamGenerateContent`),
//! not by a field of the body, and only as SSE (`alt=sse`): the JSON array
//! stream of a request without it is refused.
//!
//! # Requests
//!
//! `contents` are messages (`user`; `model` is the assistant), their parts
//! the kind's: `text`, a `thought` part as reasoning, `inlineData` and
//! `fileData` as an image, audio or document by their MIME type,
//! `functionCall` as a tool call and `functionResponse` as its result. Gemini
//! names a call by tool and position unless it sends an `id`: a call without
//! one is given an id of this format's making (`gemini_call_..`), which its
//! answer finds by name, and which is never sent back to Gemini. A
//! `functionResponse`'s `response` object is the result's text, as JSON;
//! writing it, an object stays as it is, other text goes under `output`
//! (`error` for a failed call). `systemInstruction` is a leading system
//! message; `tools` are the function declarations, their `parameters` (or
//! `parametersJsonSchema`) JSON Schema; `toolConfig` gives the tool choice;
//! `generationConfig` the output limit, `temperature`, `topP`, stop
//! sequences and structured output (`responseMimeType` with a schema).
//!
//! What the kind has no form for is this format's `native`, where it
//! appeared, and refused in another format: `safetySettings`,
//! `cachedContent`, `topK`, `thinkingConfig`, other `generationConfig`
//! fields, tools Google runs, a `toolConfig` the kind has no choice for,
//! video parts, and a part's `thoughtSignature` (which another format drops).
//! A request for several candidates, one tool call at a time, or an end
//! user's id is refused when written.
//!
//! # Responses as events
//!
//! A stream and a whole body decode into the same events, by these rules:
//!
//! - [`Output::Start`] comes from the first response's `responseId` and
//!   `modelVersion`; its `native` holds its other fields (`promptFeedback`).
//! - The first candidate's parts are blocks, numbered as they open:
//!   consecutive `text` parts are one [`Block::Text`] (a `thought` part a
//!   [`Block::Reasoning`]), a `functionCall` is a [`Block::ToolCall`] whose
//!   arguments are one delta. A part with fields beyond its content, such as
//!   a `thoughtSignature`, is a block of its own, and the fields come before
//!   it in an inert [`Output::Native`] `{"type": "part_fields", ..}`.
//! - `finishReason` is the [`Output::Finish`]: `STOP` is a stop, or a
//!   tool use when the response made calls, `MAX_TOKENS` the token limit, and
//!   `SAFETY`, `RECITATION`, `BLOCKLIST` and the like are filtered; any other
//!   is native to this format. A prompt Gemini blocks (`promptFeedback`
//!   with no candidate) is filtered.
//! - `usageMetadata` gives [`Output::Usage`]: `promptTokenCount` is the
//!   input, `candidatesTokenCount` and `thoughtsTokenCount` the output,
//!   `cachedContentTokenCount` the cached input.
//! - What else a response says is an inert [`Output::Native`] of this format
//!   (`candidate_fields`, `response_fields`, `usage_fields`), which another
//!   format drops, and what the kind has no form for is not inert:
//!   a part of another kind (`executableCode`, a response image), and a
//!   second candidate.
//!
//! A stream ends with its last response, which has the `finishReason`; one
//! that ends without it was cut. Encoders do the reverse: a stream is a
//! response per delta, a tool call whole once it ends, and the last with
//! the finish and usage; a body is written from the events collected.

mod request;
#[cfg(test)]
mod tests;

use crate::is_inert;
use crate::schema::{
    Asked, Decoder, Gate, Keep, Names, NativeFieldsExt, Schema, Totals, count, error_event,
    kept_fields, merge_details, merge_fields, native_fields, remainder, says, says_error, set,
    unreadable_event, unwritable_content, unwritable_usage, usage_extra,
};
use respan_gateway_conversation::{
    Assembled, AssembledItem, Block, Delta, Finish, Input, Output, Start, Usage,
};
use respan_gateway_core::{BadRequest, Connection, Fault, Format, NativeValue};
use respan_gateway_http::bytes::{Bytes, BytesMut};
use respan_gateway_http::framing::sse::{event, event_json};
use respan_gateway_http::http::{self, StatusCode, request::Parts as RequestHead};
use respan_gateway_http::{ClientIo, Encoder, ProviderIo};
use serde_json::{Map, Value, json};
use std::sync::Arc;

use request::{
    is_synthetic, read_config, read_messages, read_tools, split_system, write_config,
    write_messages, write_tools,
};

/// This format's name.
const GEMINI: &str = "gemini-generate";

/// What starts the id of a tool call Gemini named none for.
const SYNTHETIC: &str = "gemini_call_";

const ROLES: Names<respan_gateway_conversation::Role> = Names(
    &[
        ("user", respan_gateway_conversation::Role::User),
        ("model", respan_gateway_conversation::Role::Assistant),
    ],
    GEMINI,
);

/// The reasons the kind has; `STOP` is also a tool use, which a response
/// that made calls says so.
const FINISH: Names<Finish> = Names(
    &[
        ("STOP", Finish::Stop),
        ("MAX_TOKENS", Finish::Length),
        ("STOP", Finish::ToolUse),
        ("SAFETY", Finish::Filtered),
    ],
    GEMINI,
);

/// The finish reason of a prompt Gemini blocked, which has no candidate.
const BLOCKED: &str = "PROMPT_BLOCKED";

/// A reason as a [`Finish`]: those that filter the answer, as the kind has
/// it, stay Gemini's own for a client of this format.
fn finish_of(reason: &str) -> Finish {
    match reason {
        "RECITATION"
        | "BLOCKLIST"
        | "PROHIBITED_CONTENT"
        | "SPII"
        | "IMAGE_SAFETY"
        | "IMAGE_PROHIBITED_CONTENT"
        | "IMAGE_RECITATION"
        | "LANGUAGE" => Finish::native_or(GEMINI, reason, Finish::Filtered),
        _ => FINISH.finish(reason),
    }
}

/// The model and the method a Gemini path names: `.../models/{model}:{method}`.
fn call_of(uri: &http::Uri) -> Option<(&str, &str)> {
    let path = uri.path();
    let (rest, last) = path.rsplit_once('/')?;
    let (model, method) = last.rsplit_once(':')?;
    let named = rest.rsplit('/').next() == Some("models") && !model.is_empty();
    let known = matches!(method, "generateContent" | "streamGenerateContent");
    (named && known).then_some((model, method))
}

/// Whether the call at `uri` is a streaming method.
pub(crate) fn streams(uri: &http::Uri) -> bool {
    call_of(uri).is_some_and(|(_, method)| method == "streamGenerateContent")
}

/// Google Gemini's `generateContent`, named `gemini-generate`: a `POST` to a
/// path `.../models/{model}:generateContent` or `:streamGenerateContent`.
/// The provider's base URL includes the version, such as
/// `https://generativelanguage.googleapis.com/v1beta`, and its transport the
/// key (`x-goog-api-key`).
#[derive(Debug, Clone, Copy, Default)]
pub struct GeminiGenerate;

impl GeminiGenerate {
    /// This format's name, `gemini-generate`.
    pub fn name(&self) -> &'static str {
        GEMINI
    }
}

impl Format<ClientIo, Input, Output> for GeminiGenerate {
    fn name(&self) -> &str {
        GEMINI
    }
    fn matches(&self, io: &ClientIo) -> bool {
        self.accepts(io.request())
    }
    fn bind(&self, io: ClientIo) -> Result<Connection<Input, Output>, Fault> {
        self.bind_client(io)
    }
}

impl Format<ProviderIo, Output, Input> for GeminiGenerate {
    fn name(&self) -> &str {
        GEMINI
    }
    fn bind(&self, io: ProviderIo) -> Result<Connection<Output, Input>, Fault> {
        self.bind_provider(io)
    }
}

/// Keys the kind has a place for, or that change no output.
const KNOWN: &[&str] = &[
    "contents",
    "systemInstruction",
    "tools",
    "toolConfig",
    "generationConfig",
    "model",
];

/// Whether a model name is one a path can hold as it is.
fn is_segment(model: &str) -> bool {
    let plain = |c: char| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.');
    !model.is_empty() && model != "." && model != ".." && model.chars().all(plain)
}

impl GeminiGenerate {
    /// A call's request: its body, and the model its path names.
    fn decode_call(&self, head: &RequestHead, raw: Bytes) -> Result<Input, Fault> {
        let v = match serde_json::from_slice::<Value>(&raw) {
            Ok(v) if v.is_object() => v,
            _ => return Err(BadRequest::fault("request body is not a JSON object")),
        };
        if streams(&head.uri)
            && !head
                .uri
                .query()
                .is_some_and(|q| q.split('&').any(|p| p == "alt=sse"))
        {
            return Err(BadRequest::fault(
                "streamGenerateContent is served as SSE only: add ?alt=sse",
            ));
        }
        let mut chat = self.decode_json(&v);
        chat.model = call_of(&head.uri).map(|(model, _)| model.to_owned());
        Ok(chat)
    }
}

impl Schema for GeminiGenerate {
    const NAME: &'static str = GEMINI;
    const PATH: &'static str = "/models/{model}:generateContent";
    const STREAM_FLAG: bool = false;
    const OWN: &'static [&'static str] = &["generationConfig", "tools", "toolConfig"];
    type Decoding = GeminiDecoder;

    fn decoder(&self) -> GeminiDecoder {
        GeminiDecoder::default()
    }

    fn accepts<B>(&self, request: &http::Request<B>) -> bool {
        request.method() == http::Method::POST && call_of(request.uri()).is_some()
    }

    fn read_request(
        &self,
        request: http::Request<respan_gateway_http::Body>,
    ) -> respan_gateway_core::EventStream<Input> {
        let schema = *self;
        respan_gateway_http::messages(request, move |head, raw| schema.decode_call(head, raw))
    }

    /// The model is in the path, not in the body.
    fn encode_request(&self, chat: &Input) -> Result<Map<String, Value>, Fault> {
        let keep = Keep {
            own: Self::OWN,
            droppable: &[],
        };
        let in_the_request = |key: &str| format!("'{key}' in the request");
        let native = kept_fields(GEMINI, &chat.native, in_the_request, keep)?;
        let mut v = self.encode_json(chat)?;
        v.extend(native);
        Ok(v)
    }

    fn target(&self, input: &Input, stream: bool) -> Result<Option<String>, Fault> {
        let model = input.model.as_deref().unwrap_or_default();
        let model = model.strip_prefix("models/").unwrap_or(model);
        if !is_segment(model) {
            return Err(BadRequest::fault(
                "a Gemini call needs a model, such as gemini-2.5-flash",
            ));
        }
        Ok(Some(match stream {
            true => format!("/models/{model}:streamGenerateContent?alt=sse"),
            false => format!("/models/{model}:generateContent"),
        }))
    }

    fn decode_part(_: &Value) -> Option<respan_gateway_conversation::Content> {
        None
    }

    fn encode_part(_: &respan_gateway_conversation::Content) -> Result<Value, Fault> {
        Err(unwritable_content())
    }

    fn stream_encoder(&self, _asked: Asked) -> Box<dyn Encoder<Output>> {
        Box::new(EventStream::default())
    }

    /// What is kept beside a response: its fields, a candidate's, usage's.
    fn is_inert(raw: &Value) -> bool {
        matches!(
            raw["type"].as_str(),
            Some("part_fields" | "candidate_fields" | "response_fields" | "usage_fields")
        )
    }

    fn decode_json(&self, v: &Value) -> Input {
        let mut chat = Input::default();
        let rest = crate::schema::unsupported_fields(v, KNOWN, &[]);
        chat.native = native_fields(GEMINI, rest);
        read_messages(v, &mut chat);
        read_tools(v, &mut chat);
        read_config(v, &mut chat);
        chat.native = native_fields(GEMINI, std::mem::take(chat.native.fields_mut(GEMINI)));
        chat
    }

    fn encode_json(&self, chat: &Input) -> Result<Map<String, Value>, Fault> {
        let (system, rest) = split_system(chat)?;
        if rest.is_empty() {
            return Err(BadRequest::fault(
                "Gemini needs at least one user or model message",
            ));
        }
        let mut v = Map::new();
        set(&mut v, "systemInstruction", system);
        v.insert("contents".into(), write_messages(rest)?.into());
        write_tools(chat, &mut v)?;
        write_config(chat, &mut v)?;
        Ok(v)
    }

    fn is_response(v: &Value) -> bool {
        ["candidates", "promptFeedback", "usageMetadata"]
            .iter()
            .any(|k| v.get(*k).is_some())
    }

    fn encode_whole(&self, whole: &Assembled) -> Result<Value, Fault> {
        check_usage(&whole.usage)?;
        let mut body = ResponseFields::default();
        let mut parts: Vec<Value> = Vec::new();
        let mut fields: Vec<(u32, Map<String, Value>)> = Vec::new();
        for item in whole.items() {
            match item {
                AssembledItem::Block(part) => {
                    let mut written = match &part.kind {
                        Block::Text | Block::Reasoning if part.text.is_empty() => {
                            if !fields.iter().any(|(b, _)| *b == part.block) {
                                continue;
                            }
                            part_of(&part.kind, "")?
                        }
                        kind => part_of(kind, &part.text)?,
                    };
                    if let Some(i) = fields.iter().position(|(b, _)| *b == part.block) {
                        merge_fields(&mut written, &fields.remove(i).1);
                    }
                    parts.push(written);
                }
                AssembledItem::Native(NativeValue { format, value: raw })
                    if **format == *GEMINI =>
                {
                    match (raw["type"].as_str(), fields_of(raw)) {
                        (Some("part_fields"), Some((block, f))) => fields.push((block, f.clone())),
                        (Some("part"), _) => parts.push(raw["part"].clone()),
                        _ if body.add(raw) => {}
                        _ => return Err(unwritable_content()),
                    }
                }
                AssembledItem::Native(NativeValue { format, value: raw })
                    if is_inert(format, raw) => {}
                AssembledItem::Native(..) => return Err(unwritable_content()),
            }
        }
        let finish = whole.finish.as_ref().unwrap_or(&Finish::Stop);
        body.finish(whole.start.as_ref(), parts, finish, &whole.usage)
    }

    fn error_body(fault: &Fault) -> Value {
        let status = respan_gateway_http::status(fault);
        let error = json!({
            "code": status.as_u16(),
            "message": fault.message(),
            "status": google_status(status),
        });
        let mut error = merge_details(error, fault);
        // A provider of another format sends a code of its own, as a string.
        error["code"] = status.as_u16().into();
        if !error["status"].as_str().is_some_and(is_status_name) {
            error["status"] = google_status(status).into();
        }
        json!({ "error": error })
    }
}

/// Whether `name` is a `google.rpc.Code` name, such as `NOT_FOUND`.
fn is_status_name(name: &str) -> bool {
    !name.is_empty() && name.chars().all(|c| c.is_ascii_uppercase() || c == '_')
}

/// The `google.rpc.Code` name Google gives an HTTP `status`.
fn google_status(status: StatusCode) -> &'static str {
    match status.as_u16() {
        400 => "INVALID_ARGUMENT",
        401 => "UNAUTHENTICATED",
        403 => "PERMISSION_DENIED",
        404 => "NOT_FOUND",
        409 => "ABORTED",
        429 => "RESOURCE_EXHAUSTED",
        499 => "CANCELLED",
        501 => "UNIMPLEMENTED",
        502 | 503 => "UNAVAILABLE",
        504 => "DEADLINE_EXCEEDED",
        s if s >= 500 => "INTERNAL",
        _ => "FAILED_PRECONDITION",
    }
}

/// The part of a block of `kind` holding `text`, with the call's `id` set
/// unless it is one made here.
fn part_of(kind: &Block, text: &str) -> Result<Value, Fault> {
    Ok(match kind {
        Block::Text => json!({ "text": text }),
        Block::Reasoning => json!({ "text": text, "thought": true }),
        Block::ToolCall { id, name } => {
            let args = match text.trim() {
                "" => json!({}),
                text => serde_json::from_str::<Value>(text)
                    .ok()
                    .filter(Value::is_object)
                    .ok_or_else(unwritable_content)?,
            };
            let mut call = json!({ "name": name, "args": args });
            if !is_synthetic(id) {
                call["id"] = id.as_str().into();
            }
            json!({ "functionCall": call })
        }
        _ => return Err(unwritable_content()),
    })
}

/// The `fields` a `part_fields` event holds, and the block they are for.
fn fields_of(raw: &Value) -> Option<(u32, &Map<String, Value>)> {
    let block = u32::try_from(raw["block"].as_u64()?).ok()?;
    Some((block, raw["fields"].as_object()?))
}

/// The fields a response keeps beside its parts, from the events that carry
/// them.
#[derive(Debug, Default)]
struct ResponseFields {
    /// The first candidate's fields beside its content.
    candidate: Map<String, Value>,
    /// The response's own, beside its candidates and usage.
    top: Map<String, Value>,
    /// The usage fields with no unit.
    usage: Map<String, Value>,
    /// The other candidates, whole.
    more: Vec<Value>,
}

impl ResponseFields {
    /// Takes the fields `raw` carries, if it carries any.
    fn add(&mut self, raw: &Value) -> bool {
        let extend = |into: &mut Map<String, Value>| {
            if let Some(fields) = raw["fields"].as_object() {
                let mut all = Value::Object(std::mem::take(into));
                merge_fields(&mut all, fields);
                if let Value::Object(all) = all {
                    *into = all;
                }
            }
        };
        match raw["type"].as_str() {
            Some("candidate_fields") => extend(&mut self.candidate),
            Some("response_fields") => extend(&mut self.top),
            Some("usage_fields") => extend(&mut self.usage),
            Some("candidate") => self.more.push(raw["candidate"].clone()),
            _ => return false,
        }
        true
    }

    /// The response of `parts` and the rest.
    fn finish(
        self,
        start: Option<&Start>,
        parts: Vec<Value>,
        finish: &Finish,
        usage: &Usage,
    ) -> Result<Value, Fault> {
        let reason = reason_of(finish)?;
        let mut body = Map::new();
        body.extend(own_fields(start));
        let blocked = reason == BLOCKED;
        let mut candidates = Vec::new();
        if !blocked {
            let mut candidate = Map::new();
            if !parts.is_empty() {
                candidate.insert("content".into(), json!({ "role": "model", "parts": parts }));
            }
            candidate.insert("finishReason".into(), reason.into());
            candidate.insert("index".into(), 0.into());
            candidate.extend(self.candidate);
            candidates.push(Value::Object(candidate));
            candidates.extend(self.more);
            body.insert("candidates".into(), candidates.into());
        }
        if let Some(usage) = usage_body(usage, &self.usage) {
            body.insert("usageMetadata".into(), usage);
        }
        let (id, model) = start
            .map(|s| (s.id.as_str(), s.model.as_str()))
            .unwrap_or_default();
        if !model.is_empty() {
            body.insert("modelVersion".into(), model.into());
        }
        if !id.is_empty() {
            body.insert("responseId".into(), id.into());
        }
        body.extend(self.top);
        Ok(body.into())
    }
}

/// The reason to write for `finish`.
fn reason_of(finish: &Finish) -> Result<&str, Fault> {
    match finish {
        Finish::Native { format, reason, .. } if **format == *GEMINI && reason == BLOCKED => {
            Ok(BLOCKED)
        }
        finish => FINISH.reason(finish),
    }
}

/// The start's own fields, if they are in this format.
fn own_fields(start: Option<&Start>) -> Map<String, Value> {
    let start = start.filter(|s| s.native.format() == Some(GEMINI));
    start.map(|s| s.native.fields().clone()).unwrap_or_default()
}

fn check_usage(usage: &Usage) -> Result<(), Fault> {
    let fits = usage.cache_write_input_tokens == 0
        && usage.cached_input_tokens <= usage.input_tokens
        && usage.reasoning_tokens <= usage.output_tokens;
    match fits {
        true => Ok(()),
        false => Err(unwritable_usage()),
    }
}

/// Usage as `usageMetadata` has it, if there is any: the output without the
/// thoughts as `candidatesTokenCount`, and their sum as the total.
fn usage_body(u: &Usage, native: &Map<String, Value>) -> Option<Value> {
    if u.is_empty() && native.is_empty() {
        return None;
    }
    let tool_use = native
        .get("toolUsePromptTokenCount")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let mut body = json!({
        "promptTokenCount": u.input_tokens,
        "totalTokenCount": u.input_tokens.saturating_add(u.output_tokens).saturating_add(tool_use),
    });
    let candidates = u.output_tokens.saturating_sub(u.reasoning_tokens);
    if candidates > 0 {
        body["candidatesTokenCount"] = candidates.into();
    }
    if u.cached_input_tokens > 0 {
        body["cachedContentTokenCount"] = u.cached_input_tokens.into();
    }
    if u.reasoning_tokens > 0 {
        body["thoughtsTokenCount"] = u.reasoning_tokens.into();
    }
    merge_fields(&mut body, native);
    Some(body)
}

/// Usage totals; the thoughts are part of the output.
fn usage_of(u: &Value) -> Result<Usage, Fault> {
    let n = |key: &str| count(u.get(key));
    let thoughts = n("thoughtsTokenCount")?.unwrap_or(0);
    Ok(Usage {
        input_tokens: n("promptTokenCount")?.unwrap_or(0),
        output_tokens: n("candidatesTokenCount")?
            .unwrap_or(0)
            .saturating_add(thoughts),
        cached_input_tokens: n("cachedContentTokenCount")?.unwrap_or(0),
        reasoning_tokens: thoughts,
        ..Usage::default()
    })
}

/// What a usage object says that has no unit.
fn extra_usage(u: &Value) -> Map<String, Value> {
    let known = [
        "promptTokenCount",
        "candidatesTokenCount",
        "thoughtsTokenCount",
        "cachedContentTokenCount",
        "totalTokenCount",
    ];
    usage_extra(u, &known, &[])
}

/// A block of text or reasoning that is open, and may take more.
#[derive(Debug, Clone, Copy)]
struct Open {
    block: u32,
    reasoning: bool,
}

/// Decodes responses of Gemini, streamed or whole, into events, as the
/// module says.
#[derive(Debug)]
pub struct GeminiDecoder {
    name: Arc<str>,
    totals: Totals,
    started: bool,
    done: bool,
    /// The id of the response, which the ids of its calls are made from.
    id: String,
    next: u32,
    open: Option<Open>,
    /// Whether the response has made a tool call.
    called: bool,
    /// The response, candidate and usage fields last said, to say changes only.
    response: Map<String, Value>,
    candidate: Map<String, Value>,
    usage: Map<String, Value>,
}

impl Default for GeminiDecoder {
    fn default() -> Self {
        Self {
            name: Arc::from(GEMINI),
            totals: Totals::default(),
            started: false,
            done: false,
            id: String::new(),
            next: 0,
            open: None,
            called: false,
            response: Map::new(),
            candidate: Map::new(),
            usage: Map::new(),
        }
    }
}

impl GeminiDecoder {
    fn other(&self, v: Value, out: &mut Vec<Output>) {
        out.push(Output::native(self.name.clone(), v));
    }

    /// Says `fields` as a `kind` event, if they changed.
    fn changed(
        &self,
        kind: &str,
        fields: Map<String, Value>,
        last: &mut Map<String, Value>,
        out: &mut Vec<Output>,
    ) {
        if !fields.is_empty() && fields != *last {
            self.other(json!({ "type": kind, "fields": fields }), out);
            *last = fields;
        }
    }

    fn close(&mut self, out: &mut Vec<Output>) {
        out.extend(self.open.take().map(|o| Output::block_end(o.block)));
    }

    /// A new block: its number, with the fields beside its content said first.
    fn block(&mut self, fields: Map<String, Value>, out: &mut Vec<Output>) -> u32 {
        self.close(out);
        let block = self.next;
        self.next += 1;
        if !fields.is_empty() {
            let event = json!({ "type": "part_fields", "block": block, "fields": fields });
            self.other(event, out);
        }
        block
    }

    /// The events of one part.
    fn part(&mut self, p: &Value, out: &mut Vec<Output>) {
        let payload = |k: &str| p.get(k).is_some();
        if let Some(text) = p["text"].as_str().filter(|_| payload("text")) {
            let reasoning = p["thought"] == true;
            let fields = remainder(p, &["text", "thought"]);
            let fields: Map<String, Value> =
                fields.into_iter().filter(|(_, v)| !v.is_null()).collect();
            let kind = || match reasoning {
                true => Block::Reasoning,
                false => Block::Text,
            };
            if !fields.is_empty() {
                let block = self.block(fields, out);
                out.push(Output::block_start(block, kind()));
                out.extend(self.delta(block, text, reasoning));
                out.push(Output::block_end(block));
            } else if !text.is_empty() {
                let block = match self.open.filter(|o| o.reasoning == reasoning) {
                    Some(open) => open.block,
                    None => {
                        let block = self.block(Map::new(), out);
                        out.push(Output::block_start(block, kind()));
                        self.open = Some(Open { block, reasoning });
                        block
                    }
                };
                out.extend(self.delta(block, text, reasoning));
            }
        } else if let Some(call) = p["functionCall"]
            .as_object()
            .filter(|_| payload("functionCall"))
        {
            let name = call.get("name").and_then(Value::as_str);
            let args = call.get("args").filter(|a| a.is_object() || a.is_null());
            let clean = call
                .keys()
                .all(|k| matches!(k.as_str(), "name" | "args" | "id"));
            let (Some(name), Some(args), true) = (name, args, clean) else {
                return self.opaque(p, out);
            };
            let fields = remainder(p, &["functionCall"]);
            let block = self.block(fields, out);
            self.called = true;
            let id = match call.get("id").and_then(Value::as_str) {
                Some(id) => id.to_owned(),
                None if self.id.is_empty() => format!("{SYNTHETIC}{block}"),
                None => format!("{SYNTHETIC}{}_{block}", self.id),
            };
            out.push(Output::block_start(
                block,
                Block::ToolCall {
                    id,
                    name: name.into(),
                },
            ));
            let args = if args.is_null() {
                json!({})
            } else {
                args.clone()
            };
            out.push(Output::arguments(block, args.to_string()));
            out.push(Output::block_end(block));
        } else {
            self.opaque(p, out);
        }
    }

    /// A part the kind has no form for.
    fn opaque(&mut self, p: &Value, out: &mut Vec<Output>) {
        self.close(out);
        self.other(json!({ "type": "part", "part": p }), out);
    }

    fn delta(&self, block: u32, text: &str, reasoning: bool) -> Option<Output> {
        match (reasoning, text.is_empty()) {
            (_, true) => None,
            (true, _) => Some(Output::reasoning(block, text)),
            (false, _) => Some(Output::text(block, text)),
        }
    }

    /// The events of one response, whole or a stream's.
    fn response(&mut self, v: &Value, out: &mut Vec<Output>) -> Result<(), Fault> {
        let known = ["candidates", "usageMetadata", "modelVersion", "responseId"];
        let mut top = remainder(v, &known);
        top.retain(|_, v| says(v));
        if !self.started {
            self.started = true;
            self.id = v["responseId"].as_str().unwrap_or_default().to_owned();
            let model = v["modelVersion"].as_str().unwrap_or_default();
            let mut start = Start::new(self.id.clone(), model);
            start.native = native_fields(GEMINI, top.clone());
            self.response = top;
            out.push(Output::Start(start));
        } else {
            let mut last = std::mem::take(&mut self.response);
            self.changed("response_fields", top, &mut last, out);
            self.response = last;
        }
        let candidates = v["candidates"].as_array().map(Vec::as_slice).unwrap_or(&[]);
        for (at, c) in candidates.iter().enumerate() {
            if at > 0 {
                self.other(json!({ "type": "candidate", "candidate": c }), out);
                continue;
            }
            for p in c["content"]["parts"].as_array().into_iter().flatten() {
                self.part(p, out);
            }
            let mut fields = remainder(c, &["content", "finishReason", "index"]);
            fields.retain(|_, v| says(v));
            let mut last = std::mem::take(&mut self.candidate);
            self.changed("candidate_fields", fields, &mut last, out);
            self.candidate = last;
            if let Some(reason) = c["finishReason"].as_str() {
                self.close(out);
                let finish = match (finish_of(reason), self.called) {
                    (Finish::Stop, true) => Finish::ToolUse,
                    (finish, _) => finish,
                };
                out.push(Output::finished(finish));
                self.done = true;
            }
        }
        if candidates.is_empty() && says(&v["promptFeedback"]["blockReason"]) {
            let filtered = Finish::native_or(GEMINI, BLOCKED, Finish::Filtered);
            out.push(Output::finished(filtered));
            self.done = true;
        }
        let usage = &v["usageMetadata"];
        if usage.is_object() {
            let step = self.totals.increment(&usage_of(usage)?);
            if !step.is_empty() {
                out.push(Output::Usage(step));
            }
            let mut last = std::mem::take(&mut self.usage);
            self.changed("usage_fields", extra_usage(usage), &mut last, out);
            self.usage = last;
        }
        Ok(())
    }
}

impl Decoder for GeminiDecoder {
    fn event(&mut self, raw: Bytes, out: &mut Vec<Output>) -> Result<(), Fault> {
        let Some(v) = event_json(&raw) else {
            unreadable_event(&self.name, &raw, out);
            return Ok(());
        };
        if says_error(&v) {
            return Err(error_event(GEMINI, &raw, &v));
        }
        self.response(&v, out)
    }

    fn end(&mut self, out: &mut Vec<Output>) -> Result<(), Fault> {
        self.close(out);
        crate::schema::ended(self.done)
    }

    fn body(&mut self, v: &Value, out: &mut Vec<Output>) -> Result<(), Fault> {
        self.response(v, out)?;
        self.close(out);
        self.done = true;
        Ok(())
    }
}

/// A block, by number, as the stream has it so far.
#[derive(Debug)]
struct Slot {
    block: u32,
    kind: Block,
    /// A tool call's arguments, until it ends.
    arguments: String,
    /// The fields to write with its first part.
    fields: Option<Map<String, Value>>,
    ended: bool,
}

/// Writes events as Gemini's stream: a response for each delta of text, one
/// for each tool call once it ends, and the last with the finish and usage.
#[derive(Debug, Default)]
struct EventStream {
    start: Option<Start>,
    began: bool,
    usage: Usage,
    finish: Option<Finish>,
    fields: ResponseFields,
    blocks: Vec<Slot>,
    /// The fields of blocks that have not opened, by block.
    pending: Vec<(u32, Map<String, Value>)>,
    gate: Gate,
}

impl EventStream {
    fn find(&mut self, block: u32) -> Result<&mut Slot, Fault> {
        let found = self.blocks.iter_mut().find(|b| b.block == block);
        found.ok_or_else(unwritable_content)
    }

    /// A response of these `parts`, with the finish and usage if `last`.
    fn chunk(&mut self, parts: Vec<Value>, last: Option<&Finish>) -> Result<Value, Fault> {
        let first = !std::mem::replace(&mut self.began, true);
        let mut v = Map::new();
        if first {
            v.extend(own_fields(self.start.as_ref()));
        }
        let mut candidate = Map::new();
        if !parts.is_empty() || last.is_none() {
            candidate.insert("content".into(), json!({ "role": "model", "parts": parts }));
        } else {
            candidate.insert("content".into(), json!({ "role": "model", "parts": [] }));
        }
        candidate.insert("index".into(), 0.into());
        let mut blocked = false;
        if let Some(finish) = last {
            let reason = reason_of(finish)?;
            blocked = reason == BLOCKED;
            candidate.insert("finishReason".into(), reason.into());
            candidate.extend(std::mem::take(&mut self.fields.candidate));
        }
        if !blocked {
            let mut candidates = vec![Value::Object(candidate)];
            if last.is_some() {
                candidates.append(&mut self.fields.more);
            }
            v.insert("candidates".into(), candidates.into());
        }
        if last.is_some() {
            if let Some(usage) = usage_body(&self.usage, &self.fields.usage) {
                v.insert("usageMetadata".into(), usage);
            }
            v.extend(std::mem::take(&mut self.fields.top));
        }
        if let Some(start) = &self.start {
            if !start.model.is_empty() {
                v.insert("modelVersion".into(), start.model.as_str().into());
            }
            if !start.id.is_empty() {
                v.insert("responseId".into(), start.id.as_str().into());
            }
        }
        Ok(Value::Object(v))
    }

    fn emit(&mut self, parts: Vec<Value>, out: &mut BytesMut) -> Result<(), Fault> {
        let v = self.chunk(parts, None)?;
        out.extend_from_slice(event(None, &v).as_bytes());
        Ok(())
    }

    fn open(&mut self, block: u32, kind: &Block) -> Result<(), Fault> {
        if let Some(had) = self.blocks.iter().find(|b| b.block == block) {
            // A start repeated for a block that is open opens nothing more.
            return match had.kind == *kind && !had.ended {
                true => Ok(()),
                false => Err(unwritable_content()),
            };
        }
        if !matches!(
            kind,
            Block::Text | Block::Reasoning | Block::ToolCall { .. }
        ) {
            return Err(unwritable_content());
        }
        let at = self.pending.iter().position(|(b, _)| *b == block);
        let fields = at.map(|i| self.pending.remove(i).1);
        self.blocks.push(Slot {
            block,
            kind: kind.clone(),
            arguments: String::new(),
            fields,
            ended: false,
        });
        Ok(())
    }

    fn say(&mut self, block: u32, delta: &Delta, out: &mut BytesMut) -> Result<(), Fault> {
        let slot = self.find(block)?;
        if slot.ended || !crate::schema::fits(delta, &slot.kind) {
            return Err(unwritable_content());
        }
        if delta.as_str().is_empty() {
            return Ok(());
        }
        if let Delta::Arguments(text) = delta {
            slot.arguments.push_str(text);
            return Ok(());
        }
        let mut part = part_of(&slot.kind, delta.as_str())?;
        if let Some(fields) = slot.fields.take() {
            merge_fields(&mut part, &fields);
        }
        self.emit(vec![part], out)
    }

    fn close(&mut self, block: u32, out: &mut BytesMut) -> Result<(), Fault> {
        let slot = self.find(block)?;
        if std::mem::replace(&mut slot.ended, true) {
            return Ok(());
        }
        let text = match slot.kind {
            Block::ToolCall { .. } => slot.arguments.as_str(),
            // Fields that no text carried go in a part of their own.
            _ => "",
        };
        let mut part = part_of(&slot.kind, text)?;
        let tool = matches!(slot.kind, Block::ToolCall { .. });
        match slot.fields.take() {
            Some(fields) => merge_fields(&mut part, &fields),
            None if !tool => return Ok(()),
            None => {}
        }
        self.emit(vec![part], out)
    }

    /// Ends every block, then writes the last response.
    fn finish(&mut self, out: &mut BytesMut) -> Result<(), Fault> {
        let open: Vec<u32> = self.blocks.iter().map(|b| b.block).collect();
        for block in open {
            self.close(block, out)?;
        }
        let finish = self.finish.clone().unwrap_or(Finish::Stop);
        let v = self.chunk(Vec::new(), Some(&finish))?;
        out.extend_from_slice(event(None, &v).as_bytes());
        Ok(())
    }
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
            Output::BlockStart { block, kind } => self.open(*block, kind)?,
            Output::Delta { block, delta } => self.say(*block, delta, out)?,
            Output::BlockEnd { block } => self.close(*block, out)?,
            Output::Usage(usage) => {
                self.usage += usage;
                check_usage(&self.usage)?;
            }
            Output::Finish(finish) => {
                reason_of(finish)?;
                self.finish = Some(finish.clone());
            }
            Output::Native(NativeValue { format, value: raw }) if **format == *GEMINI => {
                match (raw["type"].as_str(), fields_of(raw)) {
                    (Some("part_fields"), Some((block, fields))) => {
                        match self.blocks.iter_mut().find(|b| b.block == block) {
                            Some(slot) => slot.fields = Some(fields.clone()),
                            None => self.pending.push((block, fields.clone())),
                        }
                    }
                    (Some("part"), _) => self.emit(vec![raw["part"].clone()], out)?,
                    _ if self.fields.add(raw) => {}
                    _ => return Err(unwritable_content()),
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
                None => GeminiGenerate::error_body(f).to_string(),
            };
            out.extend_from_slice(event(None, &body).as_bytes());
            return Ok(());
        }
        self.finish(out)
    }
}
