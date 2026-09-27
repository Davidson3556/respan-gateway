//! OpenAI Chat Completions: `POST /chat/completions`, JSON or SSE, of the
//! conversation kind.

use bytes::{Bytes, BytesMut};
use http::StatusCode;
use respan_gateway_core::{ClientView, Decoder, Doc, Encoder, Fault, Format, Item, Part, Usage};
use respan_gateway_core::{bytes, http, serde_json};
use serde_json::{Map, Value, json};

use crate::common::{
    Names, SoFar, StreamEncoder, StreamWriter, Totals, UnaryEncoder, Wire, decode_request,
    decoder_for, edit, encode_request, json_model, made_head, merge_details, pass_headers,
    patch_messages, patch_sampling, post_json, provider_error, read_message, reads_back, says,
    says_error, set, stop_json, streams, unary_decoder, unix_now, unless_taken, unreadable_event,
    unsupported_fields, unwritable_content, write_message, write_model,
};
use crate::conversation::{Conversation, Finish, Reply, Role, units};
use crate::sse::{SseDecoder, event, event_as, event_data, event_json, is_stream};

const OPENAI: &str = "openai-chat";

/// OpenAI Chat Completions, `POST /chat/completions`, named
/// `openai-chat`. It accepts a client's `POST` to any path ending in
/// `/chat/completions`. As with OpenAI's SDKs, the provider's base URL holds the
/// version, such as `https://api.openai.com/v1` or
/// `https://api.groq.com/openai/v1`.
#[derive(Debug, Clone, Copy, Default)]
pub struct OpenAiChat;

const FINISH: Names<Finish> = Names(&[
    ("stop", Finish::Stop),
    ("length", Finish::Length),
    ("tool_calls", Finish::ToolUse),
    ("function_call", Finish::ToolUse),
    ("content_filter", Finish::Filtered),
]);

impl Format for OpenAiChat {
    type Input = Conversation;
    type Output = Reply;

    fn name(&self) -> &str {
        OPENAI
    }

    fn accepts(&self, head: &http::request::Parts) -> bool {
        crate::posts_to(head, "/chat/completions")
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
        let mut req = post_json(body, "/chat/completions", client)?;
        pass_headers(&mut req, client);
        let body = req.body_mut();
        write_model(body, client.model())?;
        add_usage(body)?;
        Ok(req)
    }

    fn decoder(&self, head: &http::response::Parts, client: ClientView<'_>) -> Box<dyn Decoder> {
        if !is_stream(head) {
            return decoder_for(head, client, unary_decoder::<Self>(head.status));
        }
        let mut state = StreamState {
            // A usage chunk the client did not ask for is the gateway's.
            hide_usage: client.is_own() && !client_asks_for_usage(client),
            ..StreamState::default()
        };
        let events = SseDecoder::new(move |raw, out: &mut Vec<Part>| state.event(raw, out));
        decoder_for(head, client, Box::new(events))
    }

    fn encoder(
        &self,
        head: &mut http::response::Parts,
        client: ClientView<'_>,
        relayed: bool,
    ) -> Box<dyn Encoder> {
        let relayed = made_head(head, client, relayed);
        if !is_stream(head) {
            return Box::new(UnaryEncoder::<Self>::new(head));
        }
        let chunks = Chunks {
            show_usage: client_asks_for_usage(client),
            ..Chunks::default()
        };
        Box::new(StreamEncoder::new(relayed, chunks))
    }
}

/// Ask a streaming request for usage, which OpenAI sends only if asked.
/// The decoder hides it from a client that did not ask.
fn add_usage(body: &mut Doc) -> Result<(), Fault> {
    if !streams(body) || body.json().is_some_and(asks_for_usage) {
        return Ok(());
    }
    edit(body, |v| {
        let options = v.entry("stream_options").or_insert(Value::Null);
        match options.as_object_mut() {
            Some(o) => drop(o.insert("include_usage".into(), true.into())),
            None => *options = json!({ "include_usage": true }),
        }
    })
}

/// Keys a conversation has a place for. `stream_options` is the gateway's
/// to set, and `user` does not change the output.
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
    "user",
];

/// Keys that change nothing at these values. Common SDKs send them by
/// default, so they must not block translation.
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
    fn read(v: &Value) -> Conversation {
        let mut chat = Conversation {
            stream: v["stream"] == true,
            max_tokens: v["max_completion_tokens"]
                .as_u64()
                .or(v["max_tokens"].as_u64()),
            temperature: v["temperature"].as_f64(),
            top_p: v["top_p"].as_f64(),
            stop: match &v["stop"] {
                Value::String(s) => vec![s.clone()],
                Value::Array(a) => a
                    .iter()
                    .filter_map(|s| s.as_str().map(Into::into))
                    .collect(),
                _ => Vec::new(),
            },
            unsupported: unsupported_fields(v, KNOWN, INERT),
            ..Conversation::default()
        };
        for m in v["messages"].as_array().into_iter().flatten() {
            chat.messages.push(read_message(m, &ROLES));
        }
        chat
    }

    fn write(chat: &Conversation) -> Result<Value, Fault> {
        let mut v = Map::new();
        let messages = chat.messages.iter().map(|m| write_message(m, &ROLES));
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
        Ok(v.into())
    }

    fn patch(
        v: &mut Map<String, Value>,
        before: &Conversation,
        after: &Conversation,
    ) -> Result<(), Fault> {
        if before.max_tokens != after.max_tokens {
            // Under every spelling the client used, or the current one, so
            // no stale limit is left.
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
        if before.messages != after.messages {
            let messages = patch_messages(&before.messages, &after.messages, &ROLES)?;
            v.insert("messages".into(), messages.into());
        }
        Ok(())
    }

    fn is_response(v: &Value) -> bool {
        v["choices"].is_array()
    }

    /// Text as block 0.
    fn replies(v: &Value) -> Vec<Reply> {
        let start = Reply::start(
            v["id"].as_str().unwrap_or_default(),
            v["model"].as_str().unwrap_or_default(),
        );
        let mut events = vec![start];
        events.extend(content(v, "message"));
        events.extend(finish(v));
        events
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

    fn render(so_far: &SoFar) -> Value {
        let text: String = so_far.text.values().map(String::as_str).collect();
        let finish = so_far.finish.as_ref().unwrap_or(&Finish::Stop);
        let (id, model) = so_far.start.clone().unwrap_or_default();
        json!({
            "id": id,
            "object": "chat.completion",
            "created": unix_now(),
            "model": model,
            "choices": [{
                "index": 0,
                "message": { "role": "assistant", "content": text },
                "finish_reason": FINISH.reason(finish),
            }],
            "usage": usage_body(&so_far.usage),
        })
    }

    fn in_order(events: &[Reply]) -> Vec<Reply> {
        in_order(events)
    }

    fn put(v: &mut Value, events: &[Reply]) {
        put_choice(v, events, "message");
    }

    fn error_body(fault: &Fault) -> Value {
        let code = fault.code();
        let error =
            json!({ "message": fault.message(), "type": code, "param": null, "code": code });
        json!({ "error": merge_details(error, fault) })
    }
}

/// `events` as a choice reads back: start, one text of every block, other
/// content, then finish, whatever order a filter left them in.
fn in_order(events: &[Reply]) -> Vec<Reply> {
    let rank = |e: &Reply| match e {
        Reply::Start { .. } => 0,
        Reply::Text { .. } => 1,
        Reply::Other { .. } => 2,
        _ => 3,
    };
    let mut events = events.to_vec();
    events.sort_by_key(rank);
    let mut joined: Vec<Reply> = Vec::with_capacity(events.len());
    for e in events {
        match (joined.last_mut(), e) {
            (Some(Reply::Text { text, .. }), Reply::Text { text: more, .. }) => {
                text.push_str(&more)
            }
            (_, Reply::Text { text, .. }) => joined.push(Reply::text(0, text)),
            (_, e) => joined.push(e),
        }
    }
    joined
}

/// Put edited events back into a whole response's choices, under
/// `message`, or a chunk's, under `delta`. Other content is written as it
/// came, so a changed one fails the response.
fn put_choice(v: &mut Value, events: &[Reply], key: &str) {
    for event in events {
        // Every chunk may name them, though some leave the model out.
        if let Reply::Start { id, model } = event {
            for (field, value) in [("id", id), ("model", model)] {
                if let Some(fields) = v.as_object_mut()
                    && (fields.contains_key(field) || !value.is_empty())
                {
                    fields.insert(field.into(), value.as_str().into());
                }
            }
        }
    }
    // Rebuild the choices as `content` reads them: the other content as it
    // came or, if a filter took it, the first choice bare, or none; then
    // its text and finish.
    let rest = rest_of(v, key);
    let old_finish = v.pointer("/choices/0/finish_reason").cloned();
    let other = events.iter().find_map(|e| match e {
        Reply::Other { format, raw } if **format == *OPENAI && raw.is_array() => {
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
    if first_choice(v).is_none() {
        match other {
            Some(other) => v["choices"] = other,
            None if more => v["choices"] = json!([]),
            None => {}
        }
        return;
    }
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
    v["choices"] = match other {
        Some(other) => other,
        None if more => taken(),
        None => rest,
    };
    // A choice has one text: every block of it, in block order.
    let mut texts: Vec<(u32, &str)> = events
        .iter()
        .filter_map(|e| match e {
            Reply::Text { block, text } => Some((*block, text.as_str())),
            _ => None,
        })
        .collect();
    texts.sort_by_key(|(block, _)| *block);
    let joined: String = texts.iter().map(|(_, t)| *t).collect();
    let text = (!texts.is_empty()).then_some(joined.as_str());
    if let Some(choice) = v.pointer_mut("/choices/0").and_then(Value::as_object_mut) {
        let body = choice.entry(key).or_insert_with(|| json!({}));
        if let Some(body) = body.as_object_mut() {
            // A whole message's content is required, if null.
            match (text, key) {
                (Some(text), _) => _ = body.insert("content".into(), text.into()),
                (None, "message") => _ = body.insert("content".into(), Value::Null),
                (None, _) => _ = body.shift_remove("content"),
            }
        }
        // The provider's spelling unless a filter changed the finish; null
        // if a filter took it.
        let finish = events.iter().find_map(|e| match e {
            Reply::Finish(f) => Some(f),
            _ => None,
        });
        let reason = match (finish, old_finish) {
            (Some(f), Some(Value::String(old))) if FINISH.finish(&old) == *f => Some(old.into()),
            (Some(f), _) => Some(FINISH.reason(f).into()),
            (None, old) => old.map(|_| Value::Null),
        };
        if let Some(reason) = reason {
            choice.insert("finish_reason".into(), reason);
        }
    }
}

/// The choices without the first one's text and finish.
fn rest_of(v: &Value, key: &str) -> Value {
    let mut rest = v["choices"].clone();
    if first_choice(v).is_some()
        && let Some(choice) = rest.get_mut(0).and_then(Value::as_object_mut)
    {
        choice.shift_remove("finish_reason");
        // Text, or none; content in parts is other content.
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

/// Whether the client, if it speaks OpenAI, asked for the usage chunk.
fn client_asks_for_usage(client: ClientView<'_>) -> bool {
    client
        .body()
        .and_then(Doc::json)
        .is_some_and(asks_for_usage)
}

fn asks_for_usage(v: &Value) -> bool {
    v.pointer("/stream_options/include_usage")
        .and_then(Value::as_bool)
        == Some(true)
}

fn usage_body(u: &Usage) -> Value {
    let (input, output) = (u.get(units::INPUT_TOKENS), u.get(units::OUTPUT_TOKENS));
    let mut body = json!({
        "prompt_tokens": input as u64,
        "completion_tokens": output as u64,
        "total_tokens": (input + output) as u64,
    });
    let cached = u.get(units::CACHED_INPUT_TOKENS);
    if cached > 0.0 {
        body["prompt_tokens_details"] = json!({ "cached_tokens": cached as u64 });
    }
    let reasoning = u.get(units::REASONING_TOKENS);
    if reasoning > 0.0 {
        body["completion_tokens_details"] = json!({ "reasoning_tokens": reasoning as u64 });
    }
    body
}

#[derive(Debug, Default)]
struct StreamState {
    totals: Totals,
    /// Whether the usage chunk is the gateway's, not asked for by the client.
    hide_usage: bool,
}

impl StreamState {
    fn event(&mut self, raw: Bytes, out: &mut Vec<Part>) {
        let data = event_data(&raw);
        if data.as_deref().map(str::trim) == Some("[DONE]") {
            out.push(Part::End(Ok(())));
            return;
        }
        let Some(mut v) = data.and_then(|d| serde_json::from_str::<Value>(&d).ok()) else {
            return out.push(unreadable_event(OPENAI, raw));
        };
        if says_error(&v) {
            return out.push(Part::End(Err(provider_error(StatusCode::OK, &v))));
        }
        let usage = self.totals.increment(&OpenAiChat::usage(&v));
        let mut raw = raw;
        let usage_field = v
            .as_object_mut()
            .filter(|_| self.hide_usage)
            .and_then(|o| o.shift_remove("usage"));
        if let Some(field) = usage_field {
            // The gateway asked for usage, not the client: the field (null
            // on all but the last chunk) counts but is not sent, and the
            // usage chunk, with no choices, is dropped.
            if field.is_object() && v["choices"].as_array().is_none_or(Vec::is_empty) {
                if !usage.is_empty() {
                    out.push(usage.into());
                }
                return;
            }
            raw = event_as(Some(&raw), None, &v).into();
        }
        if !v["choices"].is_array() && v["usage"].is_null() {
            // Not a chunk: filters see it rather than it passing unseen.
            return out.push(unreadable_event(OPENAI, raw));
        }
        let events = chunk_replies(&v);
        // A chunk with no events, such as an id-less one of filter
        // results, is a Frame.
        if events.is_empty() {
            out.push(Part::Frame(raw));
        } else {
            out.push(Item::decoded(events, raw).into());
        }
        if !usage.is_empty() {
            out.push(usage.into());
        }
    }
}

/// The events of a chunk: a start, its content and a finish. Every chunk
/// names its response, so an edit to the start reaches each; an id-less
/// one of filter results, which some servers open with, has no start.
fn chunk_replies(v: &Value) -> Vec<Reply> {
    let id = v["id"].as_str().filter(|id| !id.is_empty());
    let start = id.map(|id| Reply::start(id, v["model"].as_str().unwrap_or_default()));
    start
        .into_iter()
        .chain(content(v, "delta"))
        .chain(finish(v))
        .collect()
}

/// The first choice, if it is index 0: only its text and finish are the
/// conversation's.
fn first_choice(v: &Value) -> Option<&Value> {
    let choice = v["choices"].get(0)?;
    (choice["index"].as_u64().unwrap_or(0) == 0).then_some(choice)
}

/// The content of a whole response's choices, under `message`, or a
/// chunk's, under `delta`: the first choice's text as block 0. Anything
/// else, such as a tool call, logprobs or another choice, is one `Other`
/// holding all the choices without that text and finish. Null fields say
/// nothing.
fn content(v: &Value, key: &str) -> Vec<Reply> {
    let text = first_choice(v).and_then(|c| c[key]["content"].as_str());
    let rest = rest_of(v, key);
    let mut events: Vec<Reply> = text.map(|t| Reply::text(0, t)).into_iter().collect();
    let choices = rest.as_array().into_iter().flatten();
    if choices.clone().any(|c| says_more(c, key)) {
        events.push(Reply::other(OPENAI, rest));
    }
    events
}

/// Choice fields compatible servers add that describe the choice and hold
/// no content: OpenRouter's provider finish reason, vLLM's matched stop
/// string, Azure's filter results.
const INERT_CHOICE: &[&str] = &[
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
fn finish(v: &Value) -> Option<Reply> {
    let reason = first_choice(v)?["finish_reason"].as_str()?;
    Some(Reply::Finish(FINISH.finish(reason)))
}

/// Writes OpenAI chunks from events.
#[derive(Debug, Default)]
struct Chunks {
    show_usage: bool,
    /// When the opening chunk was written. It waits for content, so a
    /// response that fails first can still be refused.
    created: Option<u64>,
    /// The provider's chunks that end the response, held to the end.
    held: Option<BytesMut>,
    /// A written finish, held to the end and sent before `held`.
    finish: Option<BytesMut>,
}

impl Chunks {
    fn begin(&mut self, so_far: &SoFar, out: &mut BytesMut) {
        if self.created.is_some() {
            return;
        }
        self.created = Some(unix_now());
        // The provider's chunks open the stream themselves.
        if so_far.relayed {
            return;
        }
        let delta = json!({ "role": "assistant", "content": "" });
        let choices = json!([{ "index": 0, "delta": delta, "finish_reason": null }]);
        self.chunk(so_far, out, choices, None);
    }

    fn choice(&mut self, so_far: &SoFar, out: &mut BytesMut, delta: Value, finish: Option<&str>) {
        self.begin(so_far, out);
        let choices = json!([{ "index": 0, "delta": delta, "finish_reason": finish }]);
        self.chunk(so_far, out, choices, None);
    }

    fn chunk(&self, so_far: &SoFar, out: &mut BytesMut, choices: Value, usage: Option<Value>) {
        let (id, model) = so_far.start.clone().unwrap_or_default();
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
}

impl StreamWriter for Chunks {
    /// A chunk holds text and a finish only if it has a first choice.
    fn holds(raw: &Bytes, event: &Reply) -> bool {
        match event {
            Reply::Text { .. } | Reply::Finish(_) => {
                event_json(raw).is_some_and(|v| first_choice(&v).is_some())
            }
            _ => true,
        }
    }

    fn relay(&mut self, raw: &Bytes, out: &mut BytesMut) {
        self.created.get_or_insert_with(unix_now);
        // The end (a first-choice finish, or usage with no choices) is
        // held, so events a filter releases at the end go before it. More
        // content, such as another choice's, releases it.
        let v = event_json(raw).unwrap_or_default();
        let choices = v["choices"].as_array().is_some_and(|c| !c.is_empty());
        let content = chunk_replies(&v)
            .iter()
            .any(|e| !matches!(e, Reply::Start { .. }));
        if finish(&v).is_some() || (!choices && !v["usage"].is_null()) {
            self.held.get_or_insert_default().extend_from_slice(raw);
        } else if let (Some(held), false) = (&mut self.held, content) {
            // What says nothing, such as a filter annotation, waits too.
            held.extend_from_slice(raw);
        } else {
            out.extend_from_slice(&self.held.take().unwrap_or_default());
            out.extend_from_slice(raw);
        }
    }

    /// A chunk with its edited events put back. `None` if a filter took all
    /// but its start and it has no role or usage.
    fn rebuild(raw: &Bytes, events: &[Reply]) -> Result<Option<Bytes>, Fault> {
        let mut chunk = event_json(raw).unwrap_or_default();
        put_choice(&mut chunk, events, "delta");
        let silent = events.iter().all(|e| matches!(e, Reply::Start { .. }))
            && chunk["usage"].is_null()
            && chunk.pointer("/choices/0/delta/role").is_none();
        if silent {
            return Ok(None);
        }
        let again = unless_taken(events, chunk_replies(&chunk));
        reads_back(&in_order(events), &in_order(&again))?;
        Ok(Some(event(None, &chunk).into()))
    }

    fn write(&mut self, e: &Reply, so_far: &SoFar, out: &mut BytesMut) -> Result<(), Fault> {
        match e {
            Reply::Text { text, .. } => self.choice(so_far, out, json!({ "content": text }), None),
            // Held for the end, as the provider's finish is.
            Reply::Finish(f) => {
                self.begin(so_far, out);
                let mut finish = BytesMut::new();
                let choices =
                    json!([{ "index": 0, "delta": {}, "finish_reason": FINISH.reason(f) }]);
                self.chunk(so_far, &mut finish, choices, None);
                self.finish = Some(finish);
            }
            // OpenAI content a filter held, such as a tool call, as it
            // came: its choices, or an event that could not be read.
            Reply::Other { format, raw } if so_far.relayed && **format == *OPENAI => {
                self.begin(so_far, out);
                match &**raw {
                    Value::String(data) => out.extend_from_slice(event(None, data).as_bytes()),
                    choices => self.chunk(so_far, out, choices.clone(), None),
                }
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
        if let Err(f) = end {
            out.extend_from_slice(&self.held.take().unwrap_or_default());
            let body = OpenAiChat::error_body(f);
            out.extend_from_slice(event(None, body).as_bytes());
            return Ok(());
        }
        if !so_far.relayed && so_far.finish.is_none() {
            self.write(&Reply::Finish(Finish::Stop), so_far, out)?;
        }
        self.begin(so_far, out);
        out.extend_from_slice(&self.finish.take().unwrap_or_default());
        out.extend_from_slice(&self.held.take().unwrap_or_default());
        // Written only for chunks made from events; relayed ones carry it.
        if !so_far.relayed && self.show_usage && !so_far.usage.is_empty() {
            self.chunk(so_far, out, json!([]), Some(usage_body(&so_far.usage)));
        }
        out.extend_from_slice(b"data: [DONE]\n\n");
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    /// The rebuilt event, or none.
    fn rebuild(raw: &Bytes, events: &[Reply]) -> Result<Bytes, Fault> {
        Chunks::rebuild(raw, events).map(Option::unwrap_or_default)
    }
    use crate::common::encode_request;
    use crate::conversation::Message;

    fn decode_all(chunks: &[&str]) -> Vec<Part> {
        let mut state = StreamState::default();
        let mut out = Vec::new();
        let mut d = SseDecoder::new(move |raw, out: &mut Vec<Part>| state.event(raw, out));
        for c in chunks {
            d.decode(Bytes::from(c.to_string()), &mut out);
        }
        d.finish(&mut out);
        out
    }

    /// The events of the items in `out`, in order.
    fn replies(out: &[Part]) -> Vec<&Reply> {
        out.iter()
            .filter_map(|p| match p {
                Part::Item(i) => i.events::<Reply>().ok(),
                _ => None,
            })
            .flatten()
            .collect()
    }

    fn usage(out: &[Part]) -> Vec<&Usage> {
        out.iter()
            .filter_map(|p| match p {
                Part::Usage(u) => Some(u),
                _ => None,
            })
            .collect()
    }

    /// The client's `body` with `edit` made to its conversation.
    fn patched(body: Value, edit: impl FnOnce(&mut Conversation)) -> Result<Value, Fault> {
        let before = OpenAiChat::read(&body);
        let mut chat = before.clone();
        edit(&mut chat);
        let doc = encode_request::<OpenAiChat>(&chat, Some((&Doc::from(body), &before)))?;
        Ok(doc.json().cloned().unwrap_or_default())
    }

    #[test]
    fn a_stream_decodes_into_conversation_replies() {
        let out = decode_all(&[
            "data: {\"id\":\"x\",\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"He",
            "llo\"}}]}\n\ndata: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":1}}\n\n",
            "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":2}}\n\n",
            "data: [DONE]\n\n",
        ]);
        assert_eq!(
            replies(&out),
            [
                &Reply::start("x", "m"),
                &Reply::text(0, "Hello"),
                &Reply::Finish(Finish::Stop)
            ]
        );
        let usage = usage(&out);
        assert_eq!(usage[0].get(units::OUTPUT_TOKENS), 1.0);
        // Totals repeated in a later chunk add only what changed.
        assert_eq!(usage[1].get(units::OUTPUT_TOKENS), 1.0);
        assert_eq!(usage[1].get(units::INPUT_TOKENS), 0.0);
        assert!(matches!(out.last(), Some(Part::End(Ok(())))));
    }

    /// Chunks as OpenAI sends them, with the fields it sets to null.
    #[test]
    fn real_openai_chunks_decode_to_text_and_other() {
        let out = decode_all(&[
            "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt\",\"system_fingerprint\":\"fp\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\",\"refusal\":null},\"logprobs\":null,\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hi\"},\"logprobs\":null,\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"f\",\"arguments\":\"\"}}]},\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{},\"logprobs\":null,\"finish_reason\":\"tool_calls\"}]}\n\n",
            "data: [DONE]\n\n",
        ]);
        assert_eq!(
            replies(&out),
            [
                &Reply::start("c1", "gpt"),
                &Reply::text(0, ""),
                // Every chunk names its response.
                &Reply::start("c1", "gpt"),
                &Reply::text(0, "Hi"),
                &Reply::start("c1", ""),
                &Reply::other(
                    OPENAI,
                    json!([{ "index": 0, "delta": { "tool_calls": [{ "index": 0, "id": "call_1", "type": "function", "function": { "name": "f", "arguments": "" } }] } }])
                ),
                &Reply::start("c1", ""),
                &Reply::Finish(Finish::ToolUse),
            ]
        );
        // Each chunk's bytes are carried once.
        let carried = out
            .iter()
            .filter(|p| matches!(p, Part::Item(i) if i.raw().is_some()))
            .count();
        assert_eq!(carried, 4);
    }

    #[test]
    fn a_choice_with_logprobs_is_other_content() {
        let out = decode_all(&[
            "data: {\"id\":\"x\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hi\"},\"logprobs\":{\"content\":[]}}]}\n\n",
        ]);
        // Its text is text, as in a whole response, so a filter sees it.
        assert_eq!(
            replies(&out)[1..],
            [
                &Reply::text(0, "Hi"),
                &Reply::other(
                    OPENAI,
                    json!([{ "index": 0, "delta": {}, "logprobs": { "content": [] } }])
                ),
            ]
        );
        let raw = Bytes::from_static(
            b"data: {\"id\":\"x\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hi\"},\"logprobs\":{\"content\":[]}}]}\n\n",
        );
        let mut edited = chunk_replies(&event_json(&raw).unwrap_or_default());
        edited[1] = Reply::text(0, "HI");
        let out = rebuild(&raw, &edited);
        assert!(out.is_ok_and(|b| String::from_utf8_lossy(&b).contains("\"content\":\"HI\"")));
    }

    #[test]
    fn what_servers_add_to_describe_a_choice_is_no_content() {
        let out = decode_all(&[
            "data: {\"id\":\"x\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\",\"native_finish_reason\":\"stop\",\"stop_reason\":null}]}\n\n",
        ]);
        assert_eq!(replies(&out)[1..], [&Reply::Finish(Finish::Stop)]);
    }

    #[test]
    fn only_the_first_choice_finishes_the_response() {
        let out = decode_all(&[
            "data: {\"id\":\"x\",\"choices\":[{\"index\":1,\"delta\":{},\"finish_reason\":\"length\"}]}\n\n",
        ]);
        assert!(!replies(&out).iter().any(|e| matches!(e, Reply::Finish(_))));
    }

    #[test]
    fn content_the_kind_cannot_hold_is_other() {
        let out = decode_all(&[
            "data: {\"id\":\"x\",\"choices\":[{\"index\":0,\"delta\":{\"reasoning_content\":\"hm\"}}]}\n\n",
        ]);
        let reasoning = json!([{ "index": 0, "delta": { "reasoning_content": "hm" } }]);
        assert_eq!(
            replies(&out),
            [&Reply::start("x", ""), &Reply::other(OPENAI, reasoning)]
        );
    }

    #[test]
    fn a_chunk_is_one_item_of_all_its_events() {
        let out = decode_all(&[
            "data: {\"id\":\"x\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":\"stop\"}]}\n\n",
        ]);
        let [Part::Item(chunk)] = &out[..] else {
            panic!("{out:?}");
        };
        assert_eq!(
            chunk.events::<Reply>().ok(),
            Some(
                &[
                    Reply::start("x", ""),
                    Reply::text(0, "hi"),
                    Reply::Finish(Finish::Stop)
                ][..]
            )
        );
        assert!(chunk.raw().is_some());
    }

    #[test]
    fn the_terminator_ends_the_response() {
        // What comes after it the kernel never reads.
        let out = decode_all(&["data: [DONE]\n\ndata: {\"choices\":[]}\n\n"]);
        assert!(matches!(out.first(), Some(Part::End(Ok(())))));
    }

    #[test]
    fn a_provider_error_event_ends_the_stream_with_its_code() {
        let out = decode_all(&[
            "data: {\"error\":{\"type\":\"server_error\",\"message\":\"overloaded\"}}\n\n",
        ]);
        assert!(matches!(
            &out[..],
            [Part::End(Err(f))]
                if f.code() == "server_error" && f.message() == "overloaded"
        ));
    }

    #[test]
    fn a_provider_error_event_goes_out_as_it_came_in_the_same_format() {
        let error = "data: {\"error\":{\"message\":\"boom\",\"type\":\"server_error\",\"param\":null,\"code\":null}}\n\n";
        let mut bytes = BytesMut::new();
        let mut enc = StreamEncoder::<Chunks>::default();
        for part in decode_all(&[error]) {
            enc.encode(&part, &mut bytes).ok();
        }
        assert_eq!(&bytes[..], error.as_bytes());
    }

    #[test]
    fn an_edited_event_keeps_its_chunk_shape() {
        let raw = Bytes::from_static(
            b"data: {\"id\":\"x\",\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\n",
        );
        assert_eq!(
            rebuild(&raw, &[Reply::start("y", "m"), Reply::text(0, "HI")]).ok(),
            Some(Bytes::from_static(
                b"data: {\"id\":\"y\",\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"HI\"}}]}\n\n"
            ))
        );
        // Text a filter replaced with a finish is the finish.
        let finish = rebuild(&raw, &[Reply::Finish(Finish::Length)]).unwrap_or_default();
        assert!(String::from_utf8_lossy(&finish).contains("\"finish_reason\":\"length\""));
    }

    #[test]
    fn a_finish_edited_beside_a_tool_call_is_written() {
        let raw = Bytes::from_static(
            b"data: {\"id\":\"x\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0}]},\"finish_reason\":\"tool_calls\"}]}\n\n",
        );
        let other = Reply::other(
            OPENAI,
            json!([{ "index": 0, "delta": { "tool_calls": [{ "index": 0 }] } }]),
        );
        let events = [Reply::start("x", ""), other, Reply::Finish(Finish::Stop)];
        let out = rebuild(&raw, &events);
        assert!(
            out.is_ok_and(|b| String::from_utf8_lossy(&b).contains("\"finish_reason\":\"stop\""))
        );
    }

    #[test]
    fn a_tool_call_emptied_in_a_stream_fails_rather_than_leave_a_shell() {
        let raw = Bytes::from_static(
            b"data: {\"id\":\"x\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0}]}}]}\n\n",
        );
        assert!(rebuild(&raw, &[Reply::other(OPENAI, json!({}))]).is_err());
    }

    #[test]
    fn a_model_renamed_in_a_chunk_without_one_is_written() {
        let raw = Bytes::from_static(
            b"data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\n",
        );
        let events = [Reply::start("c1", "alias"), Reply::text(0, "hi")];
        let out = rebuild(&raw, &events);
        assert!(out.is_ok_and(|b| String::from_utf8_lossy(&b).contains("\"model\":\"alias\"")));
    }

    #[test]
    fn the_text_of_the_opening_chunk_can_be_cleared() {
        let raw = Bytes::from_static(
            b"data: {\"id\":\"c1\",\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"SECRET\"}}]}\n\n",
        );
        let out = rebuild(&raw, &[Reply::start("c1", "m"), Reply::text(0, "")]);
        assert!(out.is_ok_and(|b| !String::from_utf8_lossy(&b).contains("SECRET")));
    }

    #[test]
    fn a_tool_call_a_filter_replaces_with_text_is_gone() {
        let raw = Bytes::from_static(
            b"data: {\"id\":\"x\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":null,\"tool_calls\":[{\"index\":0}]}}]}\n\n",
        );
        let out = rebuild(&raw, &[Reply::text(0, "[removed]")]).unwrap_or_default();
        let out = String::from_utf8_lossy(&out);
        assert!(
            out.contains("\"content\":\"[removed]\"") && !out.contains("tool_calls"),
            "{out}"
        );
        // Changing a tool call, rather than taking it, fails the response.
        let whole = Bytes::from(
            json!({ "id": "c1", "model": "m", "choices": [{ "index": 0, "finish_reason": "tool_calls",
                "message": { "role": "assistant", "content": "ok", "tool_calls": [{ "id": "t" }] } }] })
            .to_string(),
        );
        let events = [
            Reply::start("c1", "m"),
            Reply::text(0, "ok"),
            Reply::other(OPENAI, json!({ "not": "choices" })),
            Reply::Finish(Finish::ToolUse),
        ];
        assert!(crate::common::rebuild_whole::<OpenAiChat>(&whole, &events).is_err());
    }

    #[test]
    fn a_whole_response_decodes_into_events() {
        let v = json!({
            "id": "c1", "model": "gpt",
            "choices": [{ "index": 0, "finish_reason": "tool_calls", "message": {
                "role": "assistant", "content": "ok", "refusal": null,
                "tool_calls": [{ "id": "t", "type": "function", "function": { "name": "f", "arguments": "{}" } }],
            }}],
        });
        assert_eq!(
            OpenAiChat::replies(&v),
            [
                Reply::start("c1", "gpt"),
                Reply::text(0, "ok"),
                Reply::other(
                    OPENAI,
                    json!([{ "index": 0, "message": {
                        "role": "assistant", "refusal": null,
                        "tool_calls": [{ "id": "t", "type": "function", "function": { "name": "f", "arguments": "{}" } }],
                    }}])
                ),
                Reply::Finish(Finish::ToolUse),
            ]
        );
        // Its text can be edited beside the tool call.
        let mut events = OpenAiChat::replies(&v);
        events[1] = Reply::text(0, "OK");
        let raw = Bytes::from(v.to_string());
        let out = crate::common::rebuild_whole::<OpenAiChat>(&raw, &events).unwrap_or_default();
        assert!(String::from_utf8_lossy(&out).contains("\"content\":\"OK\""));
        // Logprobs and content that is not text are other content too.
        let logprobs = json!({ "choices": [{ "index": 0, "logprobs": { "content": [] },
            "message": { "role": "assistant", "content": "ok" } }] });
        assert!(matches!(
            OpenAiChat::replies(&logprobs).last(),
            Some(Reply::Other { .. })
        ));
        let parts = json!({ "choices": [{ "index": 0,
            "message": { "role": "assistant", "content": [{ "type": "text", "text": "ok" }] } }] });
        assert!(matches!(
            OpenAiChat::replies(&parts).last(),
            Some(Reply::Other { .. })
        ));
        let mut patched = v.clone();
        OpenAiChat::put(&mut patched, &[Reply::text(0, "OK")]);
        assert_eq!(patched["choices"][0]["message"]["content"], "OK");
        // An edit that points nowhere changes nothing, rather than panicking.
        let mut empty = json!({});
        OpenAiChat::put(&mut empty, &[Reply::text(3, "x")]);
        assert_eq!(empty, json!({}));
    }

    #[test]
    fn a_usage_chunk_the_client_did_not_ask_for_is_not_relayed() {
        let mut state = StreamState {
            hide_usage: true,
            ..StreamState::default()
        };
        let mut out = Vec::new();
        let chunk = |s: &'static str| Bytes::from_static(s.as_bytes());
        let first = "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":3}}\n\n";
        state.event(chunk(first), &mut out);
        // The same totals again, in the chunk the client did not ask for.
        // OpenAI's own fields on it, such as `obfuscation`, do not keep it.
        let again =
            "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":3},\"obfuscation\":\"xyz\"}\n\n";
        state.event(chunk(again), &mut out);
        assert!(!out.iter().any(|p| matches!(p, Part::Frame(_))));
        // Nor is the usage on a chunk with choices.
        let sent = out.iter().filter_map(|p| match p {
            Part::Item(i) => i.raw(),
            _ => None,
        });
        assert!(
            sent.clone().count() == 1 && sent.clone().all(|b| !b.windows(5).any(|w| w == b"usage"))
        );
        let usage: Vec<f64> = out
            .iter()
            .filter_map(|p| match p {
                Part::Usage(u) => Some(u.get(units::INPUT_TOKENS)),
                _ => None,
            })
            .collect();
        assert_eq!(usage, [3.0]);
    }

    /// `body` as it goes to a provider.
    fn with_usage(body: &'static str) -> Bytes {
        let mut doc = Doc::from(Bytes::from_static(body.as_bytes()));
        assert!(add_usage(&mut doc).is_ok());
        doc.to_bytes()
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
        let err = encode_request::<OpenAiChat>(&chat, None).err();
        assert_eq!(err.map(|e| e.status()), Some(http::StatusCode::BAD_REQUEST));
        assert_eq!(patched(body.clone(), |_| {}).ok(), Some(body));
        // A plugin that clears what is unsupported, and keeps only plain
        // messages, sends the rest.
        let mut chat = chat;
        chat.unsupported.clear();
        chat.messages = vec![Message::new(Role::User, "hi")];
        assert!(encode_request::<OpenAiChat>(&chat, None).is_ok());
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
        // The text is edited in its part, beside the image and the tool
        // result.
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
        // Removing it removes it; translating it is refused.
        let removed = patched(body.clone(), |c| drop(c.messages.pop()));
        assert_eq!(
            removed.map(|v| v["messages"].clone()).ok(),
            Some(json!([{ "role": "user", "content": "hi" }]))
        );
        let chat = OpenAiChat::read(&body);
        assert!(encode_request::<OpenAiChat>(&chat, None).is_err());
    }

    #[test]
    fn a_stream_made_from_events_always_finishes() {
        let mut enc = StreamEncoder::<Chunks>::default();
        let mut out = BytesMut::new();
        let text = Item::new([Reply::start("x", "m"), Reply::text(0, "hi")]);
        let written = [Part::Item(text), Part::End(Ok(()))]
            .iter()
            .try_for_each(|p| enc.encode(p, &mut out));
        assert!(written.is_ok());
        assert!(String::from_utf8_lossy(&out).contains("\"finish_reason\":\"stop\""));
    }

    #[test]
    fn a_translated_tool_call_is_refused() {
        let mut enc = StreamEncoder::<Chunks>::default();
        let other = Item::new([Reply::other(OPENAI, json!({}))]);
        assert!(
            enc.encode(&Part::Item(other), &mut BytesMut::new())
                .is_err()
        );
    }
}
