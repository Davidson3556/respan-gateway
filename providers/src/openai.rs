//! OpenAI Chat Completions: `POST /chat/completions`, JSON or SSE, of the
//! conversation kind.

use bytes::{Bytes, BytesMut};
use respan_gateway_core::Fault;
use respan_gateway_http::bytes;
use respan_gateway_http::{RequestHead, ResponseHead};
use serde_json::{Map, Value, json};

use crate::Usage;
use crate::common::{
    Names, Reading, SoFar, UnaryEncoder, Wire, error_event, holding_start, patch_messages,
    patch_sampling, post_json, probe, read_message, reads_back, refusal, request_decoder,
    response_decoder, response_encoder, says, set, split_around, stop_json, unix_now,
    unreadable_event, unsupported_fields, unwritable_content, write_message,
};
use crate::conversation::{Conversation, Finish, Piece, Reply, Role, Start, units};
use crate::formats::errors::{merge_details, says_error};
use crate::formats::sse::{Events, SseDecoder, event, event_as, event_data, event_json, is_stream};
use crate::tools::{Fragment, fragments};
use respan_gateway_http::{Codec, Decoder, Encoder, impl_format};

pub(crate) const OPENAI: &str = "openai-chat";

/// OpenAI Chat Completions, named `openai-chat`: a `POST` to any path
/// ending in `/chat/completions`. The provider's base URL includes the
/// version, such as `https://api.openai.com/v1`.
#[derive(Debug, Clone, Copy, Default)]
pub struct OpenAiChat;

const FINISH: Names<Finish> = Names(&[
    ("stop", Finish::Stop),
    ("length", Finish::Length),
    ("tool_calls", Finish::ToolUse),
    ("function_call", Finish::ToolUse),
    ("content_filter", Finish::Filtered),
]);

impl Codec for OpenAiChat {
    type Input = Conversation;
    type Output = Reply;

    fn name(&self) -> &str {
        OPENAI
    }

    fn accepts(&self, head: &RequestHead) -> bool {
        crate::formats::posts_to(head, "/chat/completions")
    }

    fn request_decoder(&self, _head: &RequestHead) -> Box<dyn Decoder<Conversation>> {
        request_decoder::<Self>()
    }

    fn request_encoder(
        &self,
        client: Option<&RequestHead>,
        body: &mut Bytes,
        streams: bool,
    ) -> Box<dyn Encoder<Conversation, RequestHead>> {
        post_json::<Self>(
            crate::formats::call_head(client),
            body,
            "/chat/completions",
            streams,
        )
    }

    fn response_decoder(
        &self,
        head: &ResponseHead,
        call: &RequestHead,
        _: &Bytes,
    ) -> Box<dyn Decoder<Reply>> {
        let stream = || holding_start(SseDecoder::new(Reading::<Self>::new()));
        response_decoder::<Self>(head, call, stream)
    }

    fn response_encoder(
        &self,
        answer: Option<&ResponseHead>,
        ask: &RequestHead,
        body: &Bytes,
    ) -> Box<dyn Encoder<Reply, ResponseHead>> {
        response_encoder::<Self>(answer, ask, body, |head, relayed, asked| {
            match is_stream(head) {
                false => Box::new(UnaryEncoder::<Self>::new(relayed)),
                true => Box::new(Chunks::new(relayed, asked.usage)),
            }
        })
    }

    fn refusal(&self, fault: &Fault, _body: &Bytes, head: &mut ResponseHead) -> Bytes {
        refusal::<Self>(fault, head)
    }
}

impl_format!(OpenAiChat);

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

const ROLES: Names<Role> = Names(&[
    ("system", Role::System),
    ("developer", Role::System),
    ("user", Role::User),
    ("assistant", Role::Assistant),
]);

impl Wire for OpenAiChat {
    const NAME: &'static str = OPENAI;

    fn read(v: &Value) -> Conversation {
        let mut chat = Conversation::default();
        chat.stream = v["stream"] == true;
        // The larger of both spellings, so the limit read bounds what any
        // provider enforces, and a lower one patches every spelling.
        chat.max_tokens = v["max_completion_tokens"]
            .as_u64()
            .max(v["max_tokens"].as_u64());
        chat.temperature = v["temperature"].as_f64();
        chat.top_p = v["top_p"].as_f64();
        chat.stop = match &v["stop"] {
            Value::String(s) => vec![s.clone()],
            Value::Array(a) => a
                .iter()
                .filter_map(|s| s.as_str().map(Into::into))
                .collect(),
            _ => Vec::new(),
        };
        chat.user = v["user"].as_str().map(Into::into);
        chat.unsupported = unsupported_fields(v, KNOWN, INERT);
        chat.unsupported_in = Some(OPENAI.into());
        if chat.user.is_some() {
            chat.unsupported.shift_remove("user");
        }
        for m in v["messages"].as_array().into_iter().flatten() {
            chat.messages.push(read_message(OPENAI, m, &ROLES));
        }
        chat
    }

    fn write(chat: &Conversation) -> Result<Value, Fault> {
        let mut v = Map::new();
        let messages = chat
            .messages
            .iter()
            .map(|m| write_message(OPENAI, m, &ROLES));
        let messages: Vec<Value> = messages.collect::<Result<_, _>>()?;
        v.insert("messages".into(), messages.into());
        set(&mut v, "stream", chat.stream.then_some(true.into()));
        set(
            &mut v,
            "max_completion_tokens",
            chat.max_tokens.map(Into::into),
        );
        set(&mut v, "temperature", chat.temperature.map(Into::into));
        set(&mut v, "top_p", chat.top_p.map(Into::into));
        set(&mut v, "stop", stop_json(&chat.stop));
        set(&mut v, "user", chat.user.clone().map(Into::into));
        Ok(v.into())
    }

    fn patch(
        v: &mut Map<String, Value>,
        before: &Conversation,
        after: &Conversation,
    ) -> Result<(), Fault> {
        if before.max_tokens != after.max_tokens {
            // Under every spelling the client used, or the current one.
            let limit = after.max_tokens.map(Value::from);
            let used = |key: &str| v.get(key).is_some_and(says);
            let (old, new) = (used("max_tokens"), used("max_completion_tokens"));
            if old {
                set(v, "max_tokens", limit.clone());
            }
            if new || !old {
                set(v, "max_completion_tokens", limit);
            }
        }
        patch_sampling(v, before, after, "stop");
        if before.user != after.user {
            set(v, "user", after.user.clone().map(Into::into));
        }
        if before.messages != after.messages {
            let messages = patch_messages(OPENAI, &before.messages, &after.messages, &ROLES)?;
            v.insert("messages".into(), messages.into());
        }
        Ok(())
    }

    fn is_response(v: &Value) -> bool {
        v["choices"].is_array()
    }

    /// Text as block 0.
    fn reply(v: &Value) -> Reply {
        let mut reply = Reply::start(
            v["id"].as_str().unwrap_or_default(),
            v["model"].as_str().unwrap_or_default(),
        );
        reply.content = content(v, "message");
        reply.finish = finish(v);
        reply
    }

    fn usage(v: &Value) -> Usage {
        let mut usage = Usage::default();
        // Groq reports it beside the response, under its own name.
        let u = match &v["usage"] {
            u if u.is_object() => u,
            _ => &v["x_groq"]["usage"],
        };
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
            if let Some(n) = u.pointer(ptr).and_then(Value::as_f64) {
                usage.add(unit, n);
            }
        }
        usage
    }

    fn usage_body(u: &Usage) -> Value {
        let get = |unit| u.get(unit).unwrap_or_default();
        let (input, output) = (get(units::INPUT_TOKENS), get(units::OUTPUT_TOKENS));
        let mut body = json!({
            "prompt_tokens": input as u64,
            "completion_tokens": output as u64,
            "total_tokens": (input + output) as u64,
        });
        let cached = get(units::CACHED_INPUT_TOKENS);
        if cached > 0.0 {
            body["prompt_tokens_details"] = json!({ "cached_tokens": cached as u64 });
        }
        let reasoning = get(units::REASONING_TOKENS);
        if reasoning > 0.0 {
            body["completion_tokens_details"] = json!({ "reasoning_tokens": reasoning as u64 });
        }
        body
    }

    fn render(so_far: &SoFar) -> Result<Value, Fault> {
        let text: String = so_far.text.values().map(String::as_str).collect();
        let finish = so_far.finish.as_ref().unwrap_or(&Finish::Stop);
        let (id, model) = so_far.start.clone().unwrap_or_default();
        let mut message = json!({ "role": "assistant", "content": text });
        if !so_far.calls.is_empty() {
            message["tool_calls"] = so_far.calls.openai().into();
            if text.is_empty() {
                message["content"] = Value::Null;
            }
        }
        Ok(json!({
            "id": id,
            "object": "chat.completion",
            "created": unix_now(),
            "model": model,
            "choices": [{
                "index": 0,
                "message": message,
                "finish_reason": FINISH.reason(finish),
            }],
            "usage": Self::usage_body(&so_far.usage),
        }))
    }

    /// One text of every block, then other content.
    fn in_order(content: &[Piece]) -> Vec<Piece> {
        let text: Vec<&str> = content
            .iter()
            .filter_map(|p| match p {
                Piece::Text { text, .. } => Some(text.as_str()),
                _ => None,
            })
            .collect();
        let texts = (!text.is_empty()).then(|| Piece::text(0, text.concat()));
        let others = content.iter().filter(|p| !matches!(p, Piece::Text { .. }));
        texts.into_iter().chain(others.cloned()).collect()
    }

    fn put(v: &mut Value, reply: &Reply) -> Result<(), Fault> {
        put_choice(v, reply, "message")
    }

    fn error_body(fault: &Fault) -> Value {
        let code = fault.code();
        let error =
            json!({ "message": fault.message(), "type": code, "param": null, "code": code });
        json!({ "error": merge_details(error, fault) })
    }

    fn unfinished(raw: &[u8]) -> bool {
        let asked = probe(raw);
        asked.stream && !asked.usage
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
}

/// Put an edited reply back into a response's choices, under `message`, or
/// a chunk's, under `delta`; other content goes as it came.
fn put_choice(v: &mut Value, reply: &Reply, key: &str) -> Result<(), Fault> {
    if reply
        .content
        .iter()
        .any(|piece| !matches!(piece, Piece::Text { .. } | Piece::Other { .. }))
    {
        return Err(unwritable_content());
    }
    // Every chunk may name them, though some leave the model out.
    if let Some(Start { id, model, .. }) = &reply.start {
        for (field, value) in [("id", id), ("model", model)] {
            if let Some(fields) = v.as_object_mut()
                && (fields.contains_key(field) || !value.is_empty())
            {
                fields.insert(field.into(), value.as_str().into());
            }
        }
    }
    // The choices are the other content, or the first choice bare if a
    // filter took it; its text and finish follow.
    let rest = rest_of(v, key);
    let old_finish = v.pointer("/choices/0/finish_reason").cloned();
    let other = reply.content.iter().find_map(|p| match p {
        Piece::Other { format, raw, .. } if **format == *OPENAI && raw.is_array() => {
            Some((**raw).clone())
        }
        _ => None,
    });
    let more = rest
        .as_array()
        .into_iter()
        .flatten()
        .any(|c| says_more(c, key));
    // Only the first choice is the conversation's.
    let first = first_choice(v).is_some();
    let taken = || {
        let choice = &rest[0];
        let mut bare = json!({ "index": 0 });
        for (field, value) in choice.as_object().into_iter().flatten() {
            if INERT_CHOICE.contains(&field.as_str()) {
                bare[field] = value.clone();
            }
        }
        if let Some(role) = choice[key].get("role") {
            bare[key] = json!({ "role": role });
        }
        json!([bare])
    };
    match other {
        Some(other) => v["choices"] = other,
        None if more && first => v["choices"] = taken(),
        None if more => v["choices"] = json!([]),
        None if first => v["choices"] = rest,
        None => {}
    }
    if !first {
        return Ok(());
    }
    // A choice has one text: every block of it, in block order.
    let mut texts: Vec<(u32, &str)> = reply
        .content
        .iter()
        .filter_map(|p| match p {
            Piece::Text { block, text, .. } => Some((*block, text.as_str())),
            _ => None,
        })
        .collect();
    texts.sort_by_key(|(block, _)| *block);
    let joined: String = texts.iter().map(|(_, t)| *t).collect();
    let text = (!texts.is_empty()).then_some(joined.as_str());
    if let Some(choice) = v.pointer_mut("/choices/0").and_then(Value::as_object_mut) {
        let body = choice.entry(key).or_insert_with(|| json!({}));
        if let Some(body) = body.as_object_mut() {
            // A whole message needs `content`, even if null.
            match (text, key) {
                (Some(text), _) => _ = body.insert("content".into(), text.into()),
                (None, "message") => _ = body.insert("content".into(), Value::Null),
                (None, _) => _ = body.shift_remove("content"),
            }
        }
        // The provider's spelling while unchanged; null if taken.
        let reason = match (&reply.finish, old_finish) {
            (Some(f), Some(Value::String(old))) if FINISH.finish(&old) == *f => Some(old.into()),
            (Some(f), _) => Some(FINISH.reason(f).into()),
            (None, old) => old.map(|_| Value::Null),
        };
        if let Some(reason) = reason {
            choice.insert("finish_reason".into(), reason);
        }
    }
    Ok(())
}

/// The choices without the first one's text and finish.
fn rest_of(v: &Value, key: &str) -> Value {
    let mut rest = v["choices"].clone();
    if first_choice(v).is_some()
        && let Some(choice) = rest.get_mut(0).and_then(Value::as_object_mut)
    {
        choice.shift_remove("finish_reason");
        // Content in parts stays: it is other content.
        if let Some(body) = choice.get_mut(key).and_then(Value::as_object_mut)
            && body
                .get("content")
                .is_none_or(|c| c.is_string() || c.is_null())
        {
            body.shift_remove("content");
        }
    }
    rest
}

impl Events<Reply> for Reading<OpenAiChat> {
    fn event(&mut self, raw: Bytes, out: &mut Vec<Reply>) -> Result<(), Fault> {
        let data = event_data(&raw);
        if self.done || data.as_deref().map(str::trim) == Some("[DONE]") {
            self.done = true;
            return Ok(());
        }
        let v = data
            .as_deref()
            .and_then(|d| serde_json::from_str::<Value>(d).ok());
        let chunk = |v: &Value| v["choices"].is_array() || !v["usage"].is_null() || says_error(v);
        let Some(v) = v.filter(chunk) else {
            out.push(unreadable_event(&self.name, raw));
            return Ok(());
        };
        if says_error(&v) {
            return Err(error_event(&self.name, &raw, &v));
        }
        let mut reply = chunk_reply(&v);
        reply.usage = self.totals.increment(&<OpenAiChat as Wire>::usage(&v));
        out.push(reply.came(&self.name, raw));
        Ok(())
    }

    fn end(&mut self, _out: &mut Vec<Reply>) -> Result<(), Fault> {
        self.ended()
    }
}

/// What a chunk says: a start, unless id-less (as Azure's filter results),
/// its content and a finish.
fn chunk_reply(v: &Value) -> Reply {
    let id = v["id"].as_str().filter(|id| !id.is_empty());
    let mut reply = match id {
        Some(id) => Reply::start(id, v["model"].as_str().unwrap_or_default()),
        None => Reply::default(),
    };
    reply.content = content(v, "delta");
    reply.finish = finish(v);
    reply
}

/// The first choice, if it is index 0: the conversation's.
fn first_choice(v: &Value) -> Option<&Value> {
    let choice = v["choices"].get(0)?;
    (choice["index"].as_u64().unwrap_or(0) == 0).then_some(choice)
}

/// The content of a response's choices, under `message`, or a chunk's,
/// under `delta`: the first choice's text as block 0, then anything else
/// as one `Other` of the choices without that text and finish.
fn content(v: &Value, key: &str) -> Vec<Piece> {
    let text = first_choice(v).and_then(|c| c[key]["content"].as_str());
    let rest = rest_of(v, key);
    let mut content: Vec<Piece> = text.map(|t| Piece::text(0, t)).into_iter().collect();
    let choices = rest.as_array().into_iter().flatten();
    if choices.clone().any(|c| says_more(c, key)) {
        content.push(Piece::other(OPENAI, rest));
    }
    content
}

/// Choice fields that carry no content: OpenRouter's, vLLM's and Azure's.
pub(crate) const INERT_CHOICE: &[&str] = &[
    "native_finish_reason",
    "stop_reason",
    "content_filter_results",
    "content_filter_offsets",
];

/// Whether a choice says anything but its index, its body's role and
/// [`INERT_CHOICE`] fields.
fn says_more(choice: &Value, key: &str) -> bool {
    let fields = choice.as_object().into_iter().flatten();
    let fields = fields.filter(|(k, _)| *k != "index" && !INERT_CHOICE.contains(&k.as_str()));
    fields.into_iter().any(|(k, x)| {
        if k == key {
            let body = x.as_object().into_iter().flatten();
            body.filter(|(k, _)| *k != "role").any(|(_, y)| says(y))
        } else {
            says(x)
        }
    })
}

/// The first choice's finish.
fn finish(v: &Value) -> Option<Finish> {
    let reason = first_choice(v)?["finish_reason"].as_str()?;
    Some(FINISH.finish(reason))
}

/// A relayed chunk without the usage the gateway asked for: no `usage`
/// field, and `None` for the usage chunk.
fn without_usage(raw: &Bytes) -> Option<Bytes> {
    if !raw.windows(7).any(|w| w == b"\"usage\"") {
        return Some(raw.clone());
    }
    let mut v = event_json(raw).unwrap_or_default();
    let Some(field) = v.as_object_mut().and_then(|o| o.shift_remove("usage")) else {
        return Some(raw.clone());
    };
    if field.is_object() && v["choices"].as_array().is_none_or(Vec::is_empty) {
        return None;
    }
    Some(event_as(Some(raw), None, &v).into())
}

/// A chunk `raw` with its edited reply put back; `None` if nothing of it
/// is left.
fn rebuild(raw: &Bytes, said: &Reply, reply: &Reply) -> Result<Option<Bytes>, Fault> {
    let mut chunk = event_json(raw).unwrap_or_default();
    put_choice(&mut chunk, reply, "delta")?;
    if reply.usage != said.usage && chunk["usage"].is_object() {
        chunk["usage"] = OpenAiChat::usage_body(&reply.usage);
    }
    let silent = reply.content.is_empty()
        && reply.finish.is_none()
        && chunk["usage"].is_null()
        && chunk.pointer("/choices/0/delta/role").is_none();
    if silent {
        return Ok(None);
    }
    reads_back::<OpenAiChat>(reply, &chunk_reply(&chunk))?;
    Ok(Some(event(None, &chunk).into()))
}

/// Writes OpenAI chunks: the provider's as they came, with edits written
/// in, and others anew.
#[derive(Debug, Default)]
struct Chunks {
    so_far: SoFar,
    /// Whether the client asked for the usage chunk; else it is hidden.
    show_usage: bool,
    /// When the opening chunk was written, at the first content.
    created: Option<u64>,
    /// The provider's chunks that end the response, kept back to the end.
    held: Option<BytesMut>,
    /// A written finish, kept back to the end and sent before those chunks.
    finish: Option<BytesMut>,
    /// The tool calls written so far, by the number their maker gave them,
    calls: Vec<usize>,
    /// The places of the calls that have no arguments yet.
    bare: Vec<usize>,
}

impl Chunks {
    fn new(relayed: bool, show_usage: bool) -> Self {
        let so_far = SoFar {
            relayed,
            ..SoFar::default()
        };
        Self {
            so_far,
            show_usage,
            ..Self::default()
        }
    }

    fn begin(&mut self, out: &mut BytesMut) {
        if self.created.is_some() {
            return;
        }
        self.created = Some(unix_now());
        // The provider's chunks open the stream themselves.
        if self.so_far.relayed {
            return;
        }
        let delta = json!({ "role": "assistant", "content": "" });
        let choices = json!([{ "index": 0, "delta": delta, "finish_reason": null }]);
        self.chunk(out, choices, None);
    }

    fn choice(&mut self, out: &mut BytesMut, delta: Value) {
        self.begin(out);
        let choices = json!([{ "index": 0, "delta": delta, "finish_reason": null }]);
        self.chunk(out, choices, None);
    }

    /// Write a piece of a tool call another format made: its start opens
    /// the next call, arguments go to the call they continue.
    fn call(&mut self, fragment: Fragment, out: &mut BytesMut) -> Result<(), Fault> {
        let at = fragment.continues(self.calls.iter().copied());
        let arguments = &fragment.arguments;
        let call = match (fragment.start, at) {
            (Some((id, name)), None) => {
                let at = self.calls.len();
                self.calls.push(fragment.index.unwrap_or(at));
                if arguments.is_empty() {
                    self.bare.push(at);
                }
                let function = json!({ "name": name, "arguments": arguments });
                json!({ "index": at, "id": id, "type": "function", "function": function })
            }
            (_, Some(at)) => {
                if !arguments.is_empty() {
                    self.bare.retain(|b| *b != at);
                }
                json!({ "index": at, "function": { "arguments": arguments } })
            }
            (None, None) => return Err(unwritable_content()),
        };
        self.choice(out, json!({ "tool_calls": [call] }));
        Ok(())
    }

    /// Ends the tool calls: one that had no arguments has none, as JSON.
    fn end_calls(&mut self, out: &mut BytesMut) {
        for at in std::mem::take(&mut self.bare) {
            let function = json!({ "arguments": "{}" });
            let call = json!({ "index": at, "function": function });
            self.choice(out, json!({ "tool_calls": [call] }));
        }
    }

    fn chunk(&self, out: &mut BytesMut, choices: Value, usage: Option<Value>) {
        let (id, model) = self.so_far.start.clone().unwrap_or_default();
        let mut chunk = json!({
            "id": id,
            "object": "chat.completion.chunk",
            "created": self.created.unwrap_or_default(),
            "model": model,
            "choices": choices,
        });
        if let Some(u) = usage {
            chunk["usage"] = u;
        }
        out.extend_from_slice(event(None, &chunk).as_bytes());
    }

    /// Relay the provider's chunk `raw`, which says `reply`. Its end (a
    /// finish, or usage alone) is kept back until more content, so replies
    /// a filter releases at the end go before it.
    fn relay(&mut self, raw: &Bytes, reply: &Reply, out: &mut BytesMut) {
        let raw = match self.show_usage {
            true => raw.clone(),
            false => match without_usage(raw) {
                Some(rest) => rest,
                None => return,
            },
        };
        self.created.get_or_insert_with(unix_now);
        let content = !reply.content.is_empty() || reply.finish.is_some();
        let ends = reply.finish.is_some() || (reply.content.is_empty() && !reply.usage.is_empty());
        if ends {
            self.held.get_or_insert_default().extend_from_slice(&raw);
        } else if let (Some(held), false) = (&mut self.held, content) {
            held.extend_from_slice(&raw);
        } else {
            out.extend_from_slice(&self.held.take().unwrap_or_default());
            out.extend_from_slice(&raw);
        }
    }

    /// Write text, this format's other content among its own chunks, and a
    /// finish, kept back for the end; else an `Err`.
    fn write(&mut self, reply: &Reply, out: &mut BytesMut) -> Result<(), Fault> {
        // Buffered, so a reply failing partway writes nothing.
        let mut all = BytesMut::new();
        for piece in &reply.content {
            match piece {
                Piece::Text { text, .. } if text.is_empty() => {}
                Piece::Text { text, .. } => self.choice(&mut all, json!({ "content": text })),
                // Its choices, or an event that could not be read, as it came.
                Piece::Other { format, raw, .. } if self.so_far.relayed && **format == *OPENAI => {
                    self.begin(&mut all);
                    match &**raw {
                        Value::String(data) => all.extend_from_slice(event(None, data).as_bytes()),
                        choices => self.chunk(&mut all, choices.clone(), None),
                    }
                }
                Piece::Other { format, raw, .. } => match fragments(format, raw) {
                    Some(calls) => calls.into_iter().try_for_each(|f| self.call(f, &mut all))?,
                    None => return Err(unwritable_content()),
                },
                _ => return Err(unwritable_content()),
            }
        }
        if let Some(f) = &reply.finish {
            self.begin(&mut all);
            let mut finish = BytesMut::new();
            let choices = json!([{ "index": 0, "delta": {}, "finish_reason": FINISH.reason(f) }]);
            self.chunk(&mut finish, choices, None);
            self.finish = Some(finish);
        }
        out.extend_from_slice(&all);
        Ok(())
    }
}

impl Encoder<Reply, ()> for Chunks {
    fn head(&mut self, _first: Option<&Reply>) {}

    fn encode(&mut self, reply: &Reply, out: &mut BytesMut) -> Result<(), Fault> {
        self.so_far.note(reply);
        let own = reply.came_in(OPENAI).filter(|_| self.so_far.relayed);
        let Some(own) = own else {
            return self.write(reply, out);
        };
        if own.said == *reply {
            self.relay(&own.bytes, reply, out);
            return Ok(());
        }
        // What the chunk has no place for is written anew around it.
        let v = event_json(&own.bytes).unwrap_or_default();
        let first = first_choice(&v).is_some();
        let holds = |p: &Piece| !matches!(p, Piece::Text { .. }) || first;
        let (ahead, inside, after) = split_around(reply, holds, first);
        let mut all = BytesMut::new();
        self.write(&ahead, &mut all)?;
        if let Some(bytes) = rebuild(&own.bytes, &own.said, &inside)? {
            self.relay(&bytes, &inside, &mut all);
        }
        self.write(&after, &mut all)?;
        out.extend_from_slice(&all);
        Ok(())
    }

    fn end(&mut self, end: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault> {
        if let Err(f) = end {
            out.extend_from_slice(&self.held.take().unwrap_or_default());
            let body = match f.answer::<Bytes>(OPENAI) {
                Some(raw) => String::from_utf8_lossy(raw).into_owned(),
                None => OpenAiChat::error_body(f).to_string(),
            };
            out.extend_from_slice(event(None, body).as_bytes());
            return Ok(());
        }
        if !self.so_far.relayed && self.so_far.finish.is_none() {
            self.write(&Reply::finished(Finish::Stop), out)?;
        }
        self.begin(out);
        self.end_calls(out);
        out.extend_from_slice(&self.finish.take().unwrap_or_default());
        out.extend_from_slice(&self.held.take().unwrap_or_default());
        // Relayed chunks carry their own usage chunk.
        if !self.so_far.relayed && self.show_usage && !self.so_far.usage.is_empty() {
            let usage = OpenAiChat::usage_body(&self.so_far.usage);
            self.chunk(out, json!([]), Some(usage));
        }
        out.extend_from_slice(b"data: [DONE]\n\n");
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use respan_gateway_http::http::{self, HeaderValue};

    use crate::common::{patch_request, write_request};
    use crate::conversation::Message;

    /// A chunk's reply, as decoded, for editing.
    fn said(raw: &Bytes) -> Reply {
        chunk_reply(&event_json(raw).unwrap_or_default())
    }

    /// The rebuilt event, or empty bytes.
    fn rebuilt(raw: &Bytes, reply: &Reply) -> Result<Bytes, Fault> {
        rebuild(raw, &said(raw), reply).map(Option::unwrap_or_default)
    }

    /// The replies of `chunks`, and how the stream ended.
    fn decode_all(chunks: &[&str]) -> (Vec<Reply>, Result<(), Fault>) {
        let mut d = SseDecoder::new(Reading::<OpenAiChat>::new());
        let mut out = Vec::new();
        for c in chunks {
            if let Err(f) = d.decode(Bytes::from(c.to_string()), &mut out) {
                return (out, Err(f));
            }
        }
        let end = d.end(&mut out);
        (out, end)
    }

    fn head(content_type: &'static str) -> http::response::Parts {
        let mut head = http::Response::new(()).into_parts().0;
        let value = HeaderValue::from_static(content_type);
        head.headers.insert(http::header::CONTENT_TYPE, value);
        head
    }

    #[test]
    fn a_start_is_pushed_only_with_the_first_content_and_dropped_at_a_failure() {
        let call = respan_gateway_http::fresh_request_head();
        let role = "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"}}],\"usage\":{\"prompt_tokens\":2}}\n\n";
        let text =
            "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hi\"}}]}\n\n";
        let error = "data: {\"error\":{\"message\":\"Overloaded\"}}\n\n";
        let batches = |events: &[&str]| {
            let mut d =
                OpenAiChat.response_decoder(&head("text/event-stream"), &call, &Bytes::new());
            let batches: Vec<(Vec<Reply>, bool)> = events
                .iter()
                .map(|e| {
                    let mut out = Vec::new();
                    let failed = d.decode(Bytes::from(e.to_string()), &mut out).is_err();
                    (out, failed)
                })
                .collect();
            batches
        };
        let content = batches(&[role, ": ping\n\n", text]);
        assert!(content[0].0.is_empty() && content[1].0.is_empty());
        // The start, the ping held behind it, and the text, each as it came.
        let kinds: Vec<bool> = content[2].0.iter().map(Reply::is_content).collect();
        assert_eq!(kinds, [true, false, true]);
        assert!(content[2].0.iter().all(|r| r.came_in(OPENAI).is_some()));
        // A failure first leaves only the usage held, saying nothing.
        let failed = batches(&[role, error]);
        assert!(failed[1].1);
        assert_eq!(failed[1].0.len(), 1);
        let usage = &failed[1].0[0];
        assert!(!usage.is_content() && usage.usage.get(units::INPUT_TOKENS) == Some(2.0));
    }

    #[test]
    fn a_stream_decodes_into_conversation_replies() {
        let (out, end) = decode_all(&[
            "data: {\"id\":\"x\",\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"He",
            "llo\"}}]}\n\ndata: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":1}}\n\n",
            "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":2}}\n\n",
            "data: [DONE]\n\n",
        ]);
        assert!(end.is_ok());
        let mut hello = Reply::start("x", "m");
        hello.content = vec![Piece::text(0, "Hello")];
        assert_eq!(out[0], hello);
        assert_eq!(out[1].finish, Some(Finish::Stop));
        assert_eq!(out[1].usage.get(units::OUTPUT_TOKENS), Some(1.0));
        // Totals repeated in a later chunk add only what changed; usage
        // alone is no content.
        assert_eq!(out[2].usage.get(units::OUTPUT_TOKENS), Some(1.0));
        assert_eq!(out[2].usage.get(units::INPUT_TOKENS), None);
        assert!(!out[2].is_content());
    }

    /// Chunks as OpenAI sends them, with the fields it sets to null.
    #[test]
    fn real_openai_chunks_decode_to_text_and_other() {
        let (out, _) = decode_all(&[
            "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt\",\"system_fingerprint\":\"fp\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\",\"refusal\":null},\"logprobs\":null,\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hi\"},\"logprobs\":null,\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"f\",\"arguments\":\"\"}}]},\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{},\"logprobs\":null,\"finish_reason\":\"tool_calls\"}]}\n\n",
            "data: [DONE]\n\n",
        ]);
        let with = |mut start: Reply, content: Vec<Piece>, finish: Option<Finish>| {
            start.content = content;
            start.finish = finish;
            start
        };
        let call = json!([{ "index": 0, "delta": { "tool_calls": [{ "index": 0, "id": "call_1", "type": "function", "function": { "name": "f", "arguments": "" } }] } }]);
        assert_eq!(
            out,
            [
                with(Reply::start("c1", "gpt"), vec![Piece::text(0, "")], None),
                // Every chunk names its response.
                with(Reply::start("c1", "gpt"), vec![Piece::text(0, "Hi")], None),
                with(
                    Reply::start("c1", ""),
                    vec![Piece::other(OPENAI, call)],
                    None
                ),
                with(Reply::start("c1", ""), vec![], Some(Finish::ToolUse)),
            ]
        );
        // Each chunk is one reply that keeps its bytes.
        assert!(out.iter().all(|r| r.came_in(OPENAI).is_some()));
    }

    #[test]
    fn a_choice_with_logprobs_is_other_content() {
        let raw = Bytes::from_static(
            b"data: {\"id\":\"x\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hi\"},\"logprobs\":{\"content\":[]}}]}\n\n",
        );
        // Its text is text, as in a whole response, so a filter sees it.
        assert_eq!(
            said(&raw).content,
            [
                Piece::text(0, "Hi"),
                Piece::other(
                    OPENAI,
                    json!([{ "index": 0, "delta": {}, "logprobs": { "content": [] } }])
                ),
            ]
        );
        let mut edited = said(&raw);
        edited.content[0] = Piece::text(0, "HI");
        let out = rebuilt(&raw, &edited);
        assert!(out.is_ok_and(|b| String::from_utf8_lossy(&b).contains("\"content\":\"HI\"")));
    }

    #[test]
    fn what_servers_add_to_describe_a_choice_is_no_content() {
        let (out, _) = decode_all(&[
            "data: {\"id\":\"x\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\",\"native_finish_reason\":\"stop\",\"stop_reason\":null}]}\n\n",
        ]);
        assert!(out[0].content.is_empty() && out[0].finish == Some(Finish::Stop));
    }

    #[test]
    fn only_the_first_choice_finishes_the_response() {
        let (out, _) = decode_all(&[
            "data: {\"id\":\"x\",\"choices\":[{\"index\":1,\"delta\":{},\"finish_reason\":\"length\"}]}\n\n",
        ]);
        assert!(out.iter().all(|r| r.finish.is_none()));
    }

    #[test]
    fn content_the_kind_cannot_hold_is_other() {
        let (out, _) = decode_all(&[
            "data: {\"id\":\"x\",\"choices\":[{\"index\":0,\"delta\":{\"reasoning_content\":\"hm\"}}]}\n\n",
        ]);
        let reasoning = json!([{ "index": 0, "delta": { "reasoning_content": "hm" } }]);
        assert_eq!(out[0].content, [Piece::other(OPENAI, reasoning)]);
    }

    #[test]
    fn a_chunk_is_one_reply_of_all_it_says() {
        let (out, _) = decode_all(&[
            "data: {\"id\":\"x\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":\"stop\"}]}\n\n",
        ]);
        let [chunk] = &out[..] else {
            panic!("{out:?}");
        };
        let mut hi = Reply::start("x", "");
        hi.content = vec![Piece::text(0, "hi")];
        hi.finish = Some(Finish::Stop);
        assert_eq!(chunk, &hi);
        assert!(chunk.came_in(OPENAI).is_some());
    }

    // N5: a decoder ends only at the end of its body, so what follows the
    // terminator is read and ignored; a body that ends before it is cut
    // short.
    #[test]
    fn the_terminator_completes_the_response_and_nothing_after_it_is_read() {
        let (out, end) = decode_all(&["data: [DONE]\n\ndata: {\"choices\":[]}\n\n"]);
        assert!(out.is_empty() && end.is_ok());
        let (_, end) = decode_all(&["data: {\"choices\":[]}\n\n"]);
        assert_eq!(
            end.err().map(|f| f.code().to_owned()).as_deref(),
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
            Err(f) if f.code() == "server_error" && f.message() == "overloaded"
        ));
    }

    #[test]
    fn a_provider_error_event_goes_out_as_it_came_in_the_same_format() {
        let error = "data: {\"error\":{\"message\":\"boom\",\"type\":\"server_error\",\"param\":null,\"code\":null}}\n\n";
        let (_, end) = decode_all(&[error]);
        let mut bytes = BytesMut::new();
        let mut enc = Chunks::new(true, false);
        assert!(enc.end(&end, &mut bytes).is_ok());
        // Its fields are the format's, so it is written as it came.
        assert_eq!(&bytes[..], error.as_bytes());
    }

    #[test]
    fn an_edited_event_keeps_its_chunk_shape() {
        let raw = Bytes::from_static(
            b"data: {\"id\":\"x\",\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\n",
        );
        let mut edited = Reply::start("y", "m");
        edited.content = vec![Piece::text(0, "HI")];
        assert_eq!(
            rebuilt(&raw, &edited).ok(),
            Some(Bytes::from_static(
                b"data: {\"id\":\"y\",\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"HI\"}}]}\n\n"
            ))
        );
        // Text a filter replaced with a finish is the finish.
        let finish = rebuilt(&raw, &Reply::finished(Finish::Length)).unwrap_or_default();
        assert!(String::from_utf8_lossy(&finish).contains("\"finish_reason\":\"length\""));
    }

    #[test]
    fn a_finish_edited_beside_a_tool_call_is_written() {
        let raw = Bytes::from_static(
            b"data: {\"id\":\"x\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0}]},\"finish_reason\":\"tool_calls\"}]}\n\n",
        );
        let mut edited = said(&raw);
        edited.finish = Some(Finish::Stop);
        let out = rebuilt(&raw, &edited);
        assert!(
            out.is_ok_and(|b| String::from_utf8_lossy(&b).contains("\"finish_reason\":\"stop\""))
        );
    }

    #[test]
    fn a_tool_call_emptied_in_a_stream_fails_rather_than_leave_a_shell() {
        let raw = Bytes::from_static(
            b"data: {\"id\":\"x\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0}]}}]}\n\n",
        );
        assert!(rebuilt(&raw, &Reply::other(OPENAI, json!({}))).is_err());
    }

    #[test]
    fn a_model_renamed_in_a_chunk_without_one_is_written() {
        let raw = Bytes::from_static(
            b"data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\n",
        );
        let mut edited = said(&raw);
        edited.start = Reply::start("c1", "alias").start;
        let out = rebuilt(&raw, &edited);
        assert!(out.is_ok_and(|b| String::from_utf8_lossy(&b).contains("\"model\":\"alias\"")));
    }

    #[test]
    fn the_text_of_the_opening_chunk_can_be_cleared() {
        let raw = Bytes::from_static(
            b"data: {\"id\":\"c1\",\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"SECRET\"}}]}\n\n",
        );
        let mut edited = said(&raw);
        edited.content = vec![Piece::text(0, "")];
        let out = rebuilt(&raw, &edited);
        assert!(out.is_ok_and(|b| !String::from_utf8_lossy(&b).contains("SECRET")));
    }

    #[test]
    fn a_tool_call_a_filter_replaces_with_text_is_gone() {
        let raw = Bytes::from_static(
            b"data: {\"id\":\"x\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":null,\"tool_calls\":[{\"index\":0}]}}]}\n\n",
        );
        let out = rebuilt(&raw, &Reply::text(0, "[removed]")).unwrap_or_default();
        let out = String::from_utf8_lossy(&out);
        assert!(
            out.contains("\"content\":\"[removed]\"") && !out.contains("tool_calls"),
            "{out}"
        );
        // Changing a tool call, rather than taking it, fails the response.
        let v = json!({ "id": "c1", "model": "m", "choices": [{ "index": 0, "finish_reason": "tool_calls",
            "message": { "role": "assistant", "content": "ok", "tool_calls": [{ "id": "t" }] } }] });
        let whole = Bytes::from(v.to_string());
        let mut edited = OpenAiChat::reply(&v);
        edited.content = vec![
            Piece::text(0, "ok"),
            Piece::other(OPENAI, json!({ "not": "choices" })),
        ];
        let said = OpenAiChat::reply(&v);
        assert!(crate::common::rebuild_whole::<OpenAiChat>(&whole, &said, &edited).is_err());
    }

    #[test]
    fn a_whole_response_decodes_into_one_reply() {
        let v = json!({
            "id": "c1", "model": "gpt",
            "choices": [{ "index": 0, "finish_reason": "tool_calls", "message": {
                "role": "assistant", "content": "ok", "refusal": null,
                "tool_calls": [{ "id": "t", "type": "function", "function": { "name": "f", "arguments": "{}" } }],
            }}],
        });
        let mut whole = Reply::start("c1", "gpt");
        whole.content = vec![
            Piece::text(0, "ok"),
            Piece::other(
                OPENAI,
                json!([{ "index": 0, "message": {
                    "role": "assistant", "refusal": null,
                    "tool_calls": [{ "id": "t", "type": "function", "function": { "name": "f", "arguments": "{}" } }],
                }}]),
            ),
        ];
        whole.finish = Some(Finish::ToolUse);
        assert_eq!(OpenAiChat::reply(&v), whole);
        // Its text can be edited beside the tool call.
        let mut edited = OpenAiChat::reply(&v);
        edited.content[0] = Piece::text(0, "OK");
        let raw = Bytes::from(v.to_string());
        let said = OpenAiChat::reply(&v);
        let out = crate::common::rebuild_whole::<OpenAiChat>(&raw, &said, &edited);
        assert!(String::from_utf8_lossy(&out.unwrap_or_default()).contains("\"content\":\"OK\""));
        // Logprobs and content that is not text are other content too.
        let logprobs = json!({ "choices": [{ "index": 0, "logprobs": { "content": [] },
            "message": { "role": "assistant", "content": "ok" } }] });
        assert!(matches!(
            OpenAiChat::reply(&logprobs).content.last(),
            Some(Piece::Other { .. })
        ));
        let parts = json!({ "choices": [{ "index": 0,
            "message": { "role": "assistant", "content": [{ "type": "text", "text": "ok" }] } }] });
        assert!(matches!(
            OpenAiChat::reply(&parts).content.last(),
            Some(Piece::Other { .. })
        ));
        let mut patched = v.clone();
        assert!(OpenAiChat::put(&mut patched, &Reply::text(0, "OK")).is_ok());
        assert_eq!(patched["choices"][0]["message"]["content"], "OK");
        // An edit that points nowhere changes nothing.
        let mut empty = json!({});
        assert!(OpenAiChat::put(&mut empty, &Reply::text(3, "x")).is_ok());
        assert_eq!(empty, json!({}));
    }

    #[test]
    fn a_usage_chunk_the_client_did_not_ask_for_is_not_relayed() {
        let first = "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":3}}\n\n";
        // OpenAI repeats the totals in a usage chunk the client did not ask
        // for; its own fields, such as `obfuscation`, do not keep it.
        let again =
            "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":3},\"obfuscation\":\"xyz\"}\n\n";
        let (replies, end) = decode_all(&[first, again, "data: [DONE]\n\n"]);
        let counted = replies.iter().map(|r| r.usage.get(units::INPUT_TOKENS));
        assert_eq!(counted.collect::<Vec<_>>(), [Some(3.0), None]);
        // It counts, but neither it nor the usage on a chunk with choices is
        // relayed.
        let mut encoder = Chunks::new(true, false);
        let mut out = BytesMut::new();
        for reply in &replies {
            assert!(encoder.encode(reply, &mut out).is_ok());
        }
        assert!(encoder.end(&end, &mut out).is_ok());
        let out = String::from_utf8_lossy(&out);
        assert!(!out.contains("usage") && out.contains("\"stop\""), "{out}");
        assert!(out.ends_with("data: [DONE]\n\n"));
    }

    /// `body`, relayed unread, as it goes to a provider.
    fn with_usage(body: &'static str) -> Bytes {
        let mut body = Bytes::from_static(body.as_bytes());
        let _ = OpenAiChat.request_encoder(None, &mut body, false);
        body
    }

    #[test]
    fn a_request_that_needs_nothing_goes_as_it_came() {
        for body in [r#"{"model":"a",  "stream":false}"#, "not json"] {
            assert_eq!(&with_usage(body)[..], body.as_bytes());
        }
    }

    #[test]
    fn streaming_requests_ask_for_usage() {
        assert_eq!(
            &with_usage(r#"{"model":"a","stream":true}"#)[..],
            br#"{"model":"a","stream":true,"stream_options":{"include_usage":true}}"#
        );
    }

    #[test]
    fn null_stream_options_still_ask_for_usage() {
        let body = r#"{"model":"a","stream":true,"stream_options":null}"#;
        assert_eq!(
            &with_usage(body)[..],
            br#"{"model":"a","stream":true,"stream_options":{"include_usage":true}}"#
        );
    }

    #[test]
    fn asking_for_usage_keeps_the_clients_other_stream_options() {
        let body = r#"{"model":"a","stream":true,"stream_options":{"include_obfuscation":false}}"#;
        assert_eq!(
            &with_usage(body)[..],
            br#"{"model":"a","stream":true,"stream_options":{"include_obfuscation":false,"include_usage":true}}"#
        );
    }

    /// The client's `body` with `edit` made to its conversation.
    fn patched(body: Value, edit: impl FnOnce(&mut Conversation)) -> Result<Value, Fault> {
        let before = OpenAiChat::read(&body);
        let mut chat = before.clone();
        edit(&mut chat);
        let raw = Bytes::from(body.to_string());
        Ok(patch_request::<OpenAiChat>(&raw, &before, &chat)?.into())
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
        let chat = OpenAiChat::read(&body);
        assert_eq!(
            OpenAiChat::write(&chat).ok(),
            Some(json!({
                "messages": [
                    { "role": "system", "content": "be brief" },
                    { "role": "user", "content": "hi" },
                    { "role": "system", "content": "and kind" },
                ],
                "max_completion_tokens": 5,
                "stop": ["x"],
            }))
        );
    }

    #[test]
    fn what_a_conversation_cannot_hold_is_kept_or_refused() {
        let body = json!({ "model": "m", "messages": [
            { "role": "user", "name": "alice", "content": "hi" },
        ], "tools": [{ "type": "function", "function": { "name": "f" } }], "n": 2 });
        let chat = OpenAiChat::read(&body);
        assert_eq!(
            chat.unsupported,
            *json!({ "tools": [{ "type": "function", "function": { "name": "f" } }], "n": 2 })
                .as_object()
                .unwrap_or(&Map::new())
        );
        assert_eq!(chat.messages[0].text(), "hi");
        assert_eq!(
            chat.messages[0].unsupported.get("name"),
            Some(&json!("alice"))
        );
        // To another format it is refused; to OpenAI it is kept.
        let err = write_request::<crate::anthropic::AnthropicMessages>(&chat).err();
        assert_eq!(
            err.map(|e| respan_gateway_http::status(&e).as_u16()),
            Some(400)
        );
        assert!(write_request::<OpenAiChat>(&chat).is_ok());
        assert_eq!(patched(body.clone(), |_| {}).ok(), Some(body));
        // With the unsupported cleared and only plain messages, it is
        // sent.
        let mut chat = chat;
        chat.unsupported.clear();
        chat.messages = vec![Message::new(Role::User, "hi")];
        assert!(write_request::<OpenAiChat>(&chat).is_ok());
    }

    #[test]
    fn the_user_field_is_read_redacted_and_translated() {
        use crate::anthropic::AnthropicMessages;
        let body = json!({ "model": "m", "user": "bob@x.io",
            "messages": [{ "role": "user", "content": "hi" }] });
        let chat = OpenAiChat::read(&body);
        assert_eq!(chat.user.as_deref(), Some("bob@x.io"));
        assert!(chat.unsupported.is_empty());
        // Kept as it came, or changed or removed by a plugin.
        assert_eq!(patched(body.clone(), |_| {}).ok(), Some(body.clone()));
        let redacted = patched(body.clone(), |c| c.user = Some("[x]".into()));
        assert_eq!(redacted.ok().map(|v| v["user"].clone()), Some(json!("[x]")));
        let removed = patched(body.clone(), |c| c.user = None);
        assert_eq!(
            removed.ok(),
            Some(json!({ "model": "m", "messages": [{ "role": "user", "content": "hi" }] }))
        );
        // To Anthropic it is the metadata's user id, and back.
        let sent = write_request::<AnthropicMessages>(&chat).map(Value::from);
        let sent = sent.unwrap_or_default();
        assert_eq!(sent["metadata"], json!({ "user_id": "bob@x.io" }));
        let back = write_request::<OpenAiChat>(&AnthropicMessages::read(&sent));
        assert_eq!(
            back.ok().map(|v| v["user"].clone()),
            Some(json!("bob@x.io"))
        );
        // A user that is not a string is not the kind's: it stays unsupported.
        let odd = OpenAiChat::read(&json!({ "user": 7, "messages": [] }));
        assert_eq!(
            (odd.user, odd.unsupported.get("user")),
            (None, Some(&json!(7)))
        );
    }

    #[test]
    fn a_plugin_that_removes_tools_sends_the_request_without_them() {
        let body = json!({ "model": "m", "messages": [], "tools": [{ "type": "function" }] });
        let edited = patched(body, |c| {
            c.unsupported.remove("tools");
            c.unsupported.insert("seed".into(), 7.into());
        });
        assert_eq!(
            edited.ok(),
            Some(json!({ "model": "m", "messages": [], "seed": 7 }))
        );
    }

    #[test]
    fn a_new_limit_leaves_no_old_one_behind() {
        let messages = json!([{ "role": "user", "content": "hi" }]);
        for (body, want) in [
            (
                json!({ "max_tokens": 4000, "max_completion_tokens": 4000 }),
                json!({ "max_tokens": 10, "max_completion_tokens": 10 }),
            ),
            (json!({ "max_tokens": 4000 }), json!({ "max_tokens": 10 })),
            (json!({}), json!({ "max_completion_tokens": 10 })),
            // A key set to null is no spelling the client used.
            (
                json!({ "max_tokens": null, "max_completion_tokens": 5 }),
                json!({ "max_tokens": null, "max_completion_tokens": 10 }),
            ),
        ] {
            let mut body = body;
            body["messages"] = messages.clone();
            let mut want = want;
            want["messages"] = messages.clone();
            assert_eq!(patched(body, |c| c.max_tokens = Some(10)).ok(), Some(want));
        }
    }

    #[test]
    fn whether_it_streams_is_written_with_its_options() {
        let body = json!({ "model": "m", "stream": true, "stream_options": { "include_usage": true },
            "messages": [{ "role": "user", "content": "hi" }] });
        let out = patched(body, |c| c.stream = false).unwrap_or_default();
        assert_eq!(out["stream"], false);
        assert!(out.get("stream_options").is_none());
    }

    #[test]
    fn a_removed_field_leaves_the_others_in_their_order() {
        let body =
            json!({ "model": "m", "temperature": 0.5, "messages": [], "top_p": 0.9, "seed": 1 });
        let out = patched(body, |c| c.temperature = None).unwrap_or_default();
        let keys: Vec<&String> = out
            .as_object()
            .map(|o| o.keys().collect())
            .unwrap_or_default();
        assert_eq!(keys, ["model", "messages", "top_p", "seed"]);
    }

    #[test]
    fn keys_set_to_null_say_nothing() {
        let body = json!({ "model": "m", "tools": null, "n": null, "messages": [
            { "role": "user", "name": null, "content": "hi" },
            { "role": "assistant", "content": "yo", "refusal": null },
        ]});
        let chat = OpenAiChat::read(&body);
        assert!(chat.unsupported.is_empty(), "{:?}", chat.unsupported);
        assert_eq!(chat.messages.len(), 2);
    }

    #[test]
    fn an_edited_message_keeps_its_role_and_form() {
        let body = json!({ "model": "m", "messages": [
            { "role": "developer", "content": "key sk-1", "name": null },
        ]});
        // Edited in place, it keeps its form; a message made anew would not.
        let edited = patched(body, |c| c.messages[0].content = vec!["key [x]".into()]);
        assert_eq!(
            edited.map(|v| v["messages"].clone()).ok(),
            Some(json!([{ "role": "developer", "content": "key [x]", "name": null }]))
        );
    }

    #[test]
    fn a_system_prompt_can_be_added_to_a_conversation_with_tool_calls() {
        let call = json!({ "role": "assistant", "tool_calls": [{ "id": "t", "type": "function", "function": { "name": "f", "arguments": "{}" } }] });
        let result = json!({ "role": "tool", "tool_call_id": "t", "content": "42" });
        let body = json!({ "model": "m", "messages": [
            { "role": "user", "content": "hi" }, call.clone(), result.clone(),
            { "role": "user", "content": "thanks" },
        ]});
        let edited = patched(body, |c| {
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
        let chat = OpenAiChat::read(&body);
        assert_eq!(chat.messages[0].text(), "what is this?");
        assert_eq!(chat.messages[1].role, Role::Other("tool".into()));
        assert_eq!(
            chat.messages[1].unsupported.get("tool_call_id"),
            Some(&json!("t"))
        );
        // The text is edited in its part, beside the image and tool result.
        let edited = patched(body, |c| {
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
    fn a_long_conversation_changed_throughout_keeps_each_message_in_its_form() {
        let messages: Vec<Value> = (0..5000)
            .map(|i| json!({ "role": "user", "content": format!("m{i}") }))
            .collect();
        let body = json!({ "model": "m", "messages": messages });
        let edited = patched(body, |c| {
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
        // Claude Code marks cache breakpoints on earlier messages.
        let cached = json!({ "role": "user", "content": [
            { "type": "text", "text": "the repo", "cache_control": { "type": "ephemeral" } },
        ]});
        let body = json!({ "model": "m", "messages": [
            cached,
            { "role": "assistant", "content": "ok" },
            { "role": "user", "content": "my SSN is 123" },
        ]});
        let edited = patched(body, |c| {
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
        let added = patched(body, |c| {
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
        let edited = patched(json!({ "model": "m" }), |c| {
            c.messages.insert(0, Message::new(Role::System, "be kind"));
        });
        assert_eq!(
            edited.map(|v| v["messages"].clone()).ok(),
            Some(json!([{ "role": "system", "content": "be kind" }]))
        );
    }

    #[test]
    fn a_patch_changes_only_what_the_edit_changed() {
        let body = json!({
            "model": "m", "temperature": 1, "top_p": 1, "stop": "x",
            "messages": [{ "role": "user", "content": "hi" }],
        });
        let edited = patched(body.clone(), |c| {
            c.messages[0] = Message::new(Role::User, "HI")
        });
        let mut expected = body;
        expected["messages"][0]["content"] = "HI".into();
        assert_eq!(edited.ok(), Some(expected));
    }

    #[test]
    fn an_edit_keeps_messages_a_conversation_cannot_hold() {
        let image = json!({ "role": "user", "content": [{ "type": "image_url", "image_url": { "url": "u" } }] });
        let body = json!({ "model": "m", "messages": [
            { "role": "user", "content": "hi" }, image.clone(),
        ]});
        let edited = patched(body.clone(), |c| {
            c.messages[0] = Message::new(Role::User, "HI")
        });
        assert_eq!(
            edited.map(|v| v["messages"].clone()).ok(),
            Some(json!([{ "role": "user", "content": "HI" }, image]))
        );
        // Removing it removes it; sending it in another format is refused.
        let removed = patched(body.clone(), |c| drop(c.messages.pop()));
        assert_eq!(
            removed.map(|v| v["messages"].clone()).ok(),
            Some(json!([{ "role": "user", "content": "hi" }]))
        );
        let chat = OpenAiChat::read(&body);
        assert!(write_request::<crate::anthropic::AnthropicMessages>(&chat).is_err());
        assert!(write_request::<OpenAiChat>(&chat).is_ok());
    }

    #[test]
    fn what_is_already_in_the_targets_format_crosses_and_the_rest_is_refused() {
        use crate::anthropic::AnthropicMessages;
        let pdf = json!({ "type": "document", "source": { "type": "base64",
            "media_type": "application/pdf", "data": "JVBERi0=" } });
        let mut chat = OpenAiChat::read(&json!({ "model": "m", "messages": [
            { "role": "user", "content": [{ "type": "text", "text": "read" },
                { "type": "file", "file": { "file_data": "x" } }] }] }));
        // A part in OpenAI's shape does not reach Anthropic.
        assert!(write_request::<AnthropicMessages>(&chat).is_err());
        // One rewritten into Anthropic's does, byte for byte.
        chat.messages[0].content[1] =
            crate::conversation::Content::other("anthropic-messages", pdf.clone());
        let sent = write_request::<AnthropicMessages>(&chat).map(Value::from);
        assert_eq!(
            sent.map(|v| v["messages"][0]["content"][1].clone()).ok(),
            Some(pdf)
        );
        // The same holds for the fields of a request.
        chat.unsupported
            .insert("thinking".into(), json!({ "type": "enabled" }));
        assert!(write_request::<AnthropicMessages>(&chat).is_err());
        chat.unsupported_in = Some("anthropic-messages".into());
        let sent = write_request::<AnthropicMessages>(&chat).map(Value::from);
        assert_eq!(
            sent.map(|v| v["thinking"].clone()).ok(),
            Some(json!({ "type": "enabled" }))
        );
    }

    /// Fields tagged as another format's are refused in a patched request,
    /// as `write_request` refuses them.
    #[test]
    fn fields_in_another_format_are_not_patched_into_the_clients_request() {
        let raw: Bytes = r#"{"model":"m","messages":[{"role":"user","content":"hi"}]}"#.into();
        let v: Value = serde_json::from_slice(&raw).unwrap_or_default();
        let before = OpenAiChat::read(&v);
        assert_eq!(before.unsupported_in.as_deref(), Some(OpenAiChat::NAME));
        let mut after = before.clone();
        after.unsupported_in = Some("anthropic-messages".into());
        after
            .unsupported
            .insert("thinking".into(), json!({ "type": "enabled" }));
        assert!(write_request::<OpenAiChat>(&after).is_err());
        let patched = patch_request::<OpenAiChat>(&raw, &before, &after);
        assert!(patched.is_err(), "sent: {:?}", patched.map(Value::from));
        // Tagged for this format, the same change is written in.
        after.unsupported_in = Some(OpenAiChat::NAME.into());
        let patched = patch_request::<OpenAiChat>(&raw, &before, &after);
        assert_eq!(
            patched.map(|v| v["thinking"].clone()).ok(),
            Some(json!({ "type": "enabled" }))
        );
    }

    /// A message's fields tagged as another format's are refused in a
    /// patched request too, changed message or new one.
    #[test]
    fn a_messages_fields_in_another_format_are_not_patched_in() {
        let raw: Bytes = r#"{"model":"m","messages":[{"role":"user","content":"hi"}]}"#.into();
        let v: Value = serde_json::from_slice(&raw).unwrap_or_default();
        let before = OpenAiChat::read(&v);
        let mut changed = before.clone();
        changed.unsupported_in = Some("anthropic-messages".into());
        changed.messages[0]
            .unsupported
            .insert("cache_control".into(), json!({ "type": "ephemeral" }));
        let mut added = before.clone();
        added.unsupported_in = Some("anthropic-messages".into());
        let mut message = Message::new(Role::User, "more");
        message.unsupported.insert("x".into(), json!(1));
        added.messages.push(message);
        for chat in [changed, added] {
            assert!(write_request::<OpenAiChat>(&chat).is_err());
            let patched = patch_request::<OpenAiChat>(&raw, &before, &chat);
            assert!(patched.is_err(), "sent: {:?}", patched.map(Value::from));
        }
    }

    #[test]
    fn a_stream_made_from_replies_always_finishes() {
        let mut enc = Chunks::new(false, false);
        let mut out = BytesMut::new();
        let mut text = Reply::start("x", "m");
        text.content = vec![Piece::text(0, "hi")];
        assert!(enc.encode(&text, &mut out).is_ok());
        assert!(enc.end(&Ok(()), &mut out).is_ok());
        assert!(String::from_utf8_lossy(&out).contains("\"finish_reason\":\"stop\""));
    }

    #[test]
    fn a_translated_tool_call_is_refused() {
        let mut enc = Chunks::new(false, false);
        let other = Reply::other(OPENAI, json!({}));
        assert!(enc.encode(&other, &mut BytesMut::new()).is_err());
    }

    // An edit that puts text where the chunk has no place for it, as in a
    // chunk of another choice, is written anew beside it.
    #[test]
    fn text_a_chunk_has_no_place_for_is_written_beside_it() {
        let raw = Bytes::from_static(
            b"data: {\"id\":\"x\",\"choices\":[{\"index\":1,\"delta\":{\"content\":\"B\"}}]}\n\n",
        );
        let (replies, _) = decode_all(&[std::str::from_utf8(&raw).unwrap_or_default()]);
        let mut edited = replies[0].clone();
        edited.content.insert(0, Piece::text(0, "note "));
        let mut enc = Chunks::new(true, true);
        let mut out = BytesMut::new();
        assert!(enc.encode(&edited, &mut out).is_ok());
        let out = String::from_utf8_lossy(&out);
        assert!(
            out.contains("\"content\":\"note \"") && out.contains("\"index\":1"),
            "{out}"
        );
    }
    #[test]
    fn the_limit_read_is_the_larger_spelling_and_a_lower_one_patches_both() {
        let v = json!({ "max_tokens": 100_000, "max_completion_tokens": 10, "messages": [] });
        assert_eq!(OpenAiChat::read(&v).max_tokens, Some(100_000));
        let body = json!({ "max_tokens": 100_000, "max_completion_tokens": 10 });
        let want = json!({ "max_tokens": 10, "max_completion_tokens": 10 });
        assert_eq!(patched(body, |c| c.max_tokens = Some(10)).ok(), Some(want));
    }
}
