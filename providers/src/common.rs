//! What the conversation formats share: requests (decoding, and encoding
//! from the value alone), the decoding of responses into events with its
//! hold of a start, the collection of events into one whole body, usage
//! totals, and small JSON helpers.
//!
//! Nothing here reuses the bytes a value was decoded from: values are plain
//! data, and an encoder writes what a value says. Only an explicit raw relay
//! sends the client's bytes ([`respan_gateway_http::SendRaw`]).

use std::marker::PhantomData;
use std::sync::Arc;

use respan_gateway_http::bytes::{BufMut, Bytes, BytesMut};
use respan_gateway_http::http::{self, HeaderValue, StatusCode, header::CONTENT_TYPE};
use respan_gateway_http::{
    Status, bodiless, http::request::Parts as RequestHead, http::response::Parts as ResponseHead,
};
use respan_gateway_sdk::{BadRequest, ErrorCode, Fault, Truncated, Upstream};
use serde_json::{self, Map, Value};

use crate::Usage;
use crate::anthropic::ANTHROPIC;
use crate::conversation::{
    Block, Content, Conversation, Delta, Extra, Finish, Message, Reply, Role, Start,
};
use crate::formats::errors::{provider_error, says_error};
use crate::formats::sse::{content_type, event_data};
use crate::tools::{anthropic_block, anthropic_part};
use respan_gateway_http::{Body, Encoder};
use respan_gateway_sdk::{EventSink, EventStream};

/// What a conversation format supplies; the plumbing around it is shared.
pub(crate) trait Wire: Send + Sync + 'static {
    /// The format's name.
    const NAME: &'static str;
    /// The keys of a conversation's `extra` this format writes itself, from
    /// the conversation and the extra together.
    const OWN: &'static [&'static str] = &[];
    /// Decodes a response, whole or streamed, into events.
    type Reading: Reader;
    /// A JSON request as a conversation, noting what it has no place for.
    fn read(v: &Value) -> Conversation;
    /// A conversation as a request, but for its own `extra` fields and the
    /// model, refusing what this format cannot say.
    fn write(chat: &Conversation) -> Result<Map<String, Value>, Fault>;
    /// Whether a successful body is a whole response of this format.
    fn is_response(v: &Value) -> bool;
    /// The events of a response, collected, as one whole body.
    fn render(whole: &Whole) -> Result<Value, Fault>;
    /// The error body for `fault`, its details merged by [`merge_details`].
    ///
    /// [`merge_details`]: crate::formats::errors::merge_details
    fn error_body(fault: &Fault) -> Value;
    /// Whether the client's request `raw` needs [`Wire::finish`].
    fn unfinished(_raw: &[u8]) -> bool {
        false
    }
    /// Add to a request what this format needs, such as asking for usage.
    fn finish(_v: &mut Map<String, Value>) {}
}

/// A format's decoder of a response into events, which holds what it needs
/// between the events of one body.
pub(crate) trait Reader: Default + Send + 'static {
    /// The events of one stream event, `raw`.
    fn event(&mut self, raw: Bytes, out: &mut Vec<Reply>) -> Result<(), Fault>;
    /// The events of a stream that has ended, or an error if it was cut.
    fn end(&mut self, out: &mut Vec<Reply>) -> Result<(), Fault>;
    /// The events of one whole response body `v`.
    fn body(&mut self, v: &Value, out: &mut Vec<Reply>) -> Result<(), Fault>;
}

/// Reads a finite JSON request.
pub(crate) fn read_request<W: Wire>(raw: Bytes) -> Result<Conversation, Fault> {
    match serde_json::from_slice::<Value>(&raw) {
        Ok(v) if v.is_object() => {
            let mut chat = W::read(&v);
            chat.model = v["model"].as_str().map(Into::into);
            Ok(chat)
        }
        _ => Err(not_an_object()),
    }
}

fn not_an_object() -> Fault {
    BadRequest::fault("request body is not a JSON object")
}

/// Make `head` a JSON `POST` to `path`, adding what the format needs
/// without decoding an unread relay into a conversation.
pub(crate) fn post_json<W: Wire>(
    source: &mut EventStream<Conversation>,
    path: &str,
) -> Result<(http::Request<Body>, EventSink<Conversation>), Fault> {
    if !source.is_single() {
        return Err(Fault::config(format!(
            "{} does not accept streaming inputs",
            W::NAME
        )));
    }
    let (request, sink) =
        respan_gateway_http::request(W::NAME, source, Box::new(RequestEncoder::<W>(PhantomData)))?;
    let (mut head, mut body) = request.into_parts();
    head.method = http::Method::POST;
    crate::formats::set_path(&mut head, path);
    head.headers
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    if let Some(raw) = body.full_bytes_mut() {
        let finished = finish_raw::<W>(raw.clone());
        if finished != *raw {
            crate::formats::body_changed(&mut head);
            *raw = finished;
        }
    }
    Ok((http::Request::from_parts(head, body), sink))
}

/// Add protocol requirements while preserving unread malformed relay bytes.
pub(crate) fn finish_raw<W: Wire>(body: Bytes) -> Bytes {
    if W::unfinished(&body)
        && let Ok(mut v) = object(&body)
    {
        W::finish(&mut v);
        return to_bytes(&v).unwrap_or(body);
    }
    body
}

/// Writes a conversation as a request, then what the format adds
/// ([`Wire::finish`]). What cannot be written is the client's fault: without
/// the bytes it came as, the gateway cannot tell an edit from the request.
struct RequestEncoder<W>(PhantomData<W>);

impl<W: Wire> Encoder<Conversation> for RequestEncoder<W> {
    fn encode(&mut self, chat: &Conversation, out: &mut BytesMut) -> Result<(), Fault> {
        let mut v = write_request::<W>(chat).map_err(|f| match f.is::<BadRequest>() {
            true => f,
            false => BadRequest::fault(f.message()).with_source(f),
        })?;
        W::finish(&mut v);
        out.extend_from_slice(&to_bytes(&v)?);
        Ok(())
    }

    fn end(&mut self, _end: &Result<(), Fault>, _out: &mut BytesMut) -> Result<(), Fault> {
        Ok(())
    }
}

/// `v` as JSON bytes.
fn to_bytes(v: &Map<String, Value>) -> Result<Bytes, Fault> {
    let mut out = BytesMut::new().writer();
    let written = serde_json::to_writer(&mut out, v);
    written.map_err(|e| Fault::internal("a request could not be written").with_source(e))?;
    Ok(out.into_inner().freeze())
}

/// The JSON object in `raw`.
fn object(raw: &[u8]) -> Result<Map<String, Value>, Fault> {
    match serde_json::from_slice(raw) {
        Ok(Value::Object(v)) => Ok(v),
        _ => Err(not_an_object()),
    }
}

/// A conversation as a new request, if nothing is lost: its fields
/// the kind has no place for are written only if they are this format's.
pub(crate) fn write_request<W: Wire>(chat: &Conversation) -> Result<Map<String, Value>, Fault> {
    let mut extra = Map::new();
    let in_the_request = |key: &str| format!("'{key}' in the request");
    let keep = Keep {
        own: W::OWN,
        droppable: &["max_tokens", "max_completion_tokens"],
    };
    write_extra(W::NAME, &chat.extra, &mut extra, in_the_request, keep)?;
    let mut v = Map::new();
    set(&mut v, "model", chat.model.clone().map(Into::into));
    v.extend(W::write(chat)?);
    v.extend(extra);
    Ok(v)
}

/// What a format does with the fields of an `extra` it writes: `own` keys it
/// writes itself, which are left out, and `droppable` keys, a field's own
/// spelling or a hint, which another format drops instead of refusing.
#[derive(Clone, Copy, Default)]
pub(crate) struct Keep {
    pub(crate) own: &'static [&'static str],
    pub(crate) droppable: &'static [&'static str],
}

/// The fields of `extra` to write in `format`: all but the `own` keys if they
/// are in `format`; if they are another's, none if each is droppable, else
/// an `Err` naming the first that is not, by `what`.
pub(crate) fn kept_fields(
    format: &str,
    extra: &Extra,
    what: impl FnOnce(&str) -> String,
    keep: Keep,
) -> Result<Map<String, Value>, Fault> {
    let own = extra.format.as_deref() == Some(format);
    let mut fields = Map::new();
    for (key, value) in &extra.fields {
        match (own, keep.droppable.contains(&key.as_str())) {
            (true, _) if keep.own.contains(&key.as_str()) => {}
            (true, _) => _ = fields.insert(key.clone(), value.clone()),
            (false, true) => {}
            (false, false) => return Err(untranslatable(&what(key))),
        }
    }
    Ok(fields)
}

/// Write the fields of `extra` into `v` as [`kept_fields`] says.
pub(crate) fn write_extra(
    format: &str,
    extra: &Extra,
    v: &mut Map<String, Value>,
    what: impl FnOnce(&str) -> String,
    keep: Keep,
) -> Result<(), Fault> {
    v.extend(kept_fields(format, extra, what, keep)?);
    Ok(())
}

/// `fields` as the extra of a value decoded by `format`: none if empty.
pub(crate) fn extra_of(format: &str, fields: Map<String, Value>) -> Extra {
    match fields.is_empty() {
        true => Extra::default(),
        false => Extra::new(format, fields),
    }
}

/// What a JSON request asks of its response: a stream, and usage in it.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct Asked {
    pub(crate) stream: bool,
    pub(crate) usage: bool,
}

/// [`Asked`] of a JSON body, reading only its two fields.
pub(crate) fn probe(raw: &[u8]) -> Asked {
    #[derive(serde::Deserialize)]
    struct Probe {
        #[serde(default)]
        stream: Value,
        #[serde(default)]
        stream_options: Value,
    }
    match serde_json::from_slice::<Probe>(raw) {
        Ok(p) => Asked {
            stream: p.stream == true,
            usage: p.stream_options["include_usage"] == true,
        },
        Err(_) => Asked::default(),
    }
}

/// Finish a success head: its framing is the client's, whatever the
/// provider's was.
fn made_head(head: &mut ResponseHead, asks_stream: bool) {
    if crate::formats::sse::is_stream(head) != asks_stream {
        head.headers.remove(CONTENT_TYPE);
    }
    if content_type(head).is_none() {
        let content_type = match asks_stream {
            true => "text/event-stream",
            false => "application/json",
        };
        head.headers
            .insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    }
}

/// A response of format `W` to the call `call`: nothing if it has no body,
/// else events decoded by `stream` for a stream, else those of one JSON
/// message.
pub(crate) fn read_response<W: Wire>(
    response: http::Response<Body>,
    call: &RequestHead,
    stream: impl FnOnce(
        Body,
    )
        -> respan_gateway_sdk::futures::stream::BoxStream<'static, Result<Reply, Fault>>,
) -> respan_gateway_sdk::futures::stream::BoxStream<'static, Result<Reply, Fault>> {
    let (mut head, body) = response.into_parts();
    if head.status.is_success() && bodiless(&call.method, head.status) {
        return Box::pin(respan_gateway_sdk::futures::stream::empty());
    }
    if crate::formats::sse::is_stream(&head) {
        return stream(body);
    }
    crate::formats::default_content_type(&mut head, "application/json");
    let status = head.status;
    crate::formats::whole::read(
        W::NAME,
        http::Response::from_parts(head, body),
        move |raw, out| {
            out.extend(unary_reply::<W>(status, raw)?);
            Ok(())
        },
    )
}

/// Choose response framing from the explicitly selected head and the client's
/// request. Bodiless responses need no encoder. Otherwise pass the head and
/// what the client [`Asked`].
pub(crate) fn response_encoder(
    head: &mut ResponseHead,
    request: &http::Request<Bytes>,
    encoder: impl FnOnce(&ResponseHead, Asked) -> Box<dyn Encoder<Reply>>,
) -> Option<Box<dyn Encoder<Reply>>> {
    if bodiless(request.method(), head.status) {
        return None;
    }
    let asked = probe(request.body());
    made_head(head, asked.stream);
    Some(encoder(head, asked))
}

/// Finite response encoding uses the exact same encoder as HTTP delivery.
pub(crate) fn encode_response(
    head: ResponseHead,
    encoder: Option<Box<dyn Encoder<Reply>>>,
    replies: &[Reply],
) -> Result<http::Response<Bytes>, Fault> {
    let bytes = match encoder {
        Some(encoder) => respan_gateway_http::encode_all(encoder, replies)?,
        None => Bytes::new(),
    };
    Ok(http::Response::from_parts(head, bytes))
}

/// A refusal in format `W`: its own words as this format's JSON error.
/// The format has no error under a success, so a refusal under a status
/// that says none is a plain 500, as the adapter writes what no format can.
pub(crate) fn refusal<W: Wire>(fault: &Fault, head: &mut ResponseHead) -> Bytes {
    if !(head.status.is_client_error() || head.status.is_server_error()) {
        head.status = StatusCode::INTERNAL_SERVER_ERROR;
        head.headers.clear();
        let text = HeaderValue::from_static("text/plain; charset=utf-8");
        head.headers.insert(CONTENT_TYPE, text);
        return Bytes::from(fault.message().to_owned());
    }
    let json = HeaderValue::from_static("application/json");
    head.headers.insert(CONTENT_TYPE, json);
    match fault
        .get::<respan_gateway_http::RawReply>()
        .map(|r| r.0.body())
    {
        Some(words) => words.clone(),
        None => W::error_body(fault).to_string().into(),
    }
}

/// One JSON message, decoded whole: the events of a success, else the
/// provider's error.
pub(crate) fn unary_reply<W: Wire>(status: StatusCode, raw: Bytes) -> Result<Vec<Reply>, Fault> {
    let v = serde_json::from_slice::<Value>(&raw).ok();
    if let Some(v) = v.as_ref().filter(|v| W::is_response(v)) {
        let mut out = Vec::new();
        W::Reading::default().body(v, &mut out)?;
        return Ok(out);
    }
    Err(
        match crate::formats::sse::failure_json(&raw).filter(says_error) {
            Some(v) => provider_error(status, &v).with(respan_gateway_http::ProviderError::new(
                W::NAME,
                http::Response::new(raw),
            )),
            None => Upstream::fault("the provider's success is not a response of its format"),
        },
    )
}

/// One part of a response collected whole: a block, or an event the kind has
/// no form for, in the order they came.
#[derive(Debug)]
pub(crate) enum Item {
    /// A content block: the text, arguments or reasoning of its deltas.
    Block(Part),
    /// An [`Reply::Other`] event.
    Other(Arc<str>, Arc<Value>),
}

/// A content block collected whole.
#[derive(Debug)]
pub(crate) struct Part {
    pub(crate) block: u32,
    pub(crate) kind: Block,
    pub(crate) text: String,
    ended: bool,
}

/// The events of a response collected into what one whole body says; an
/// encoder of a unary response writes it.
#[derive(Debug, Default)]
pub(crate) struct Whole {
    /// The response's id, model and fields, from its first start.
    pub(crate) start: Option<Start>,
    /// What it says, in order.
    pub(crate) items: Vec<Item>,
    pub(crate) finish: Option<Finish>,
    pub(crate) usage: Usage,
}

impl Whole {
    /// Add an event; a delta or an end of a block that is not open, or a
    /// block opened as another kind, is an `Err`.
    pub(crate) fn add(&mut self, reply: &Reply) -> Result<(), Fault> {
        match reply {
            Reply::Start(start) => _ = self.start.get_or_insert_with(|| start.clone()),
            Reply::BlockStart { block, kind } => match self.part(*block) {
                Some(had) if had.kind == *kind && !had.ended => {}
                Some(_) => return Err(unwritable_content()),
                None => self.items.push(Item::Block(Part {
                    block: *block,
                    kind: kind.clone(),
                    text: String::new(),
                    ended: false,
                })),
            },
            Reply::Delta { block, delta } => match self.part(*block) {
                Some(part) if !part.ended && fits(delta, &part.kind) => {
                    part.text.push_str(delta.as_str())
                }
                _ => return Err(unwritable_content()),
            },
            Reply::BlockEnd { block } => match self.part(*block) {
                Some(part) => part.ended = true,
                None => return Err(unwritable_content()),
            },
            Reply::Usage(usage) => self.usage += usage,
            Reply::Finish(finish) => self.finish = Some(finish.clone()),
            Reply::Other { format, raw } => {
                self.items.push(Item::Other(format.clone(), raw.clone()))
            }
            _ => return Err(unwritable_content()),
        }
        Ok(())
    }

    fn part(&mut self, block: u32) -> Option<&mut Part> {
        self.items.iter_mut().find_map(|item| match item {
            Item::Block(part) if part.block == block => Some(part),
            _ => None,
        })
    }

    /// The parts, in order.
    pub(crate) fn parts(&self) -> impl Iterator<Item = &Part> {
        self.items.iter().filter_map(|item| match item {
            Item::Block(part) => Some(part),
            Item::Other(..) => None,
        })
    }
}

/// What an encoder of a stream has written but keeps back until a reply shows
/// content ([`shows`]), so that a failure before it leaves nothing written
/// and the response is refused, as the decoder holds a start for.
#[derive(Debug, Default)]
pub(crate) struct Gate {
    shown: bool,
    held: BytesMut,
}

impl Gate {
    /// Let through, with what came before, what `reply` made, if it or any
    /// before it shows content.
    pub(crate) fn pass(&mut self, reply: &Reply, made: &[u8], out: &mut BytesMut) {
        self.held.extend_from_slice(made);
        if self.shown || shows(reply) {
            self.release(out);
        }
    }

    /// Let through all that is held: a stream that ended well shows what it
    /// has.
    pub(crate) fn release(&mut self, out: &mut BytesMut) {
        self.shown = true;
        out.extend_from_slice(&self.held.split());
    }
}

/// Writes one message at its end: the events collected into a whole body. A
/// failure, before any byte, is refused.
pub(crate) struct UnaryEncoder<W> {
    whole: Whole,
    wire: PhantomData<W>,
}

impl<W: Wire> UnaryEncoder<W> {
    pub(crate) fn new() -> Self {
        let (whole, wire) = (Whole::default(), PhantomData);
        Self { whole, wire }
    }
}

impl<W: Wire> Encoder<Reply> for UnaryEncoder<W> {
    fn encode(&mut self, reply: &Reply, _out: &mut BytesMut) -> Result<(), Fault> {
        self.whole.add(reply)
    }

    fn end(&mut self, end: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault> {
        end.clone()?;
        out.extend_from_slice(W::render(&self.whole)?.to_string().as_bytes());
        Ok(())
    }
}

/// Whether `raw`, an event of another `format`, changes no answer: a
/// keep-alive, or what the other format keeps beside a response (a
/// signature, a stop sequence). A format drops these when translating, and
/// refuses any other event it has no form for.
pub(crate) fn is_inert(format: &str, raw: &Value) -> bool {
    let usage_only = raw.as_object().is_some_and(|o| {
        o.len() == 1 && (o.contains_key("usage") || o.contains_key("call_fields"))
    });
    (format == crate::openai::OPENAI && usage_only)
        || format == ANTHROPIC
            && (matches!(
                raw["type"].as_str(),
                Some("ping" | "message_delta" | "content_block_fields")
            ) || raw["delta"]["type"] == "signature_delta")
}

/// Merge the fields of `extra` into `target`, an object, objects among them
/// field by field.
pub(crate) fn merge_fields(target: &mut Value, extra: &Map<String, Value>) {
    let Some(target) = target.as_object_mut() else {
        return;
    };
    for (key, value) in extra {
        match (target.get_mut(key), value) {
            (Some(had), Value::Object(more)) if had.is_object() => merge_fields(had, more),
            _ => _ = target.insert(key.clone(), value.clone()),
        }
    }
}

/// The fields of the usage object `u` the kind has no unit for: not the
/// `known` counters, and of the `details` objects (by name, with the known
/// counters of each) only what else they hold.
/// A provider's usage count at `v`: `None` if it says none (absent or
/// null); a whole number even when written as a float (`12.0`); else a
/// failure, so a count that cannot be one is never metered as zero.
pub(crate) fn count(v: Option<&Value>) -> Result<Option<u64>, Fault> {
    let Some(v) = v.filter(|v| !v.is_null()) else {
        return Ok(None);
    };
    let whole = v.as_u64().or_else(|| {
        let f = v.as_f64()?;
        (f >= 0.0 && f.fract() == 0.0 && f <= u64::MAX as f64).then_some(f as u64)
    });
    whole
        .map(Some)
        .ok_or_else(|| Upstream::fault(format!("a usage count is not a whole number: {v}")))
}

pub(crate) fn usage_extra(
    u: &Value,
    known: &[&str],
    details: &[(&str, &[&str])],
) -> Map<String, Value> {
    let mut extra = Map::new();
    for (key, value) in u.as_object().into_iter().flatten() {
        match details.iter().find(|(name, _)| name == key) {
            Some((_, counters)) => {
                let rest = value.as_object().into_iter().flatten();
                let rest: Map<String, Value> = rest
                    .filter(|(k, v)| !counters.contains(&k.as_str()) && !v.is_null())
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                if !rest.is_empty() {
                    extra.insert(key.clone(), rest.into());
                }
            }
            None if known.contains(&key.as_str()) || value.is_null() => {}
            None => _ = extra.insert(key.clone(), value.clone()),
        }
    }
    extra
}

/// Whether `object` says nothing but in the fields `these`.
pub(crate) fn only(object: &Value, these: &[&str]) -> bool {
    let fields = object.as_object();
    fields.is_some_and(|o| {
        o.iter()
            .all(|(k, v)| these.contains(&k.as_str()) || !says(v))
    })
}

/// Whether `delta` is of the kind of `block`.
pub(crate) fn fits(delta: &Delta, block: &Block) -> bool {
    matches!(
        (delta, block),
        (Delta::Text(_), Block::Text)
            | (Delta::Arguments(_), Block::ToolCall { .. })
            | (Delta::Reasoning(_), Block::Reasoning)
    )
}

/// Whether a reply shows the client something: not a start, usage, or the
/// opening or end of a text block, which says nothing yet.
pub(crate) fn shows(reply: &Reply) -> bool {
    match reply {
        Reply::Delta { delta, .. } => !delta.as_str().is_empty(),
        Reply::BlockStart { kind, .. } => *kind != Block::Text,
        Reply::Finish(_) => true,
        _ => false,
    }
}

/// Incremental provider response parsing, shared by streaming and finite reads.
/// Semantic failures retain held usage; wire failures never finalize the parser.
pub(crate) struct ReplyParser<R> {
    frames: crate::formats::sse::Parser,
    reading: R,
    held: Vec<Reply>,
    through: bool,
}

impl<R: Reader> ReplyParser<R> {
    /// A parser that, if `hold_start`, keeps a response's start and what
    /// comes before its first content back until that content comes, so a
    /// failure before it leaves nothing started.
    pub(crate) fn new(hold_start: bool) -> Self {
        Self {
            frames: crate::formats::sse::Parser::new(),
            reading: R::default(),
            held: Vec::new(),
            through: !hold_start,
        }
    }

    pub(crate) fn push(&mut self, bytes: Bytes, out: &mut Vec<Reply>) -> Result<(), Fault> {
        let from = out.len();
        let result = self
            .frames
            .push(bytes)
            .into_iter()
            .try_for_each(|raw| self.reading.event(raw, out));
        self.hold(out, from, result.is_err());
        result
    }

    pub(crate) fn finish(&mut self, out: &mut Vec<Reply>) -> Result<(), Fault> {
        let from = out.len();
        let result = self
            .frames
            .finish()
            .into_iter()
            .try_for_each(|raw| self.reading.event(raw, out))
            .and_then(|()| self.reading.end(out));
        self.hold(out, from, result.is_err());
        out.append(&mut self.held);
        result
    }

    fn hold(&mut self, out: &mut Vec<Reply>, from: usize, failed: bool) {
        for reply in out.split_off(from) {
            match reply {
                _ if self.through => out.push(reply),
                ref r if !r.is_content() && self.held.is_empty() => out.push(reply),
                ref r if !r.is_content() || !shows(r) => self.held.push(reply),
                _ => {
                    out.append(&mut self.held);
                    out.push(reply);
                    self.through = true;
                }
            }
        }
        if failed && !self.through {
            let mut usage = Usage::default();
            for held in self.held.drain(..) {
                if let Reply::Usage(u) = held {
                    usage += &u;
                }
            }
            if !usage.is_empty() {
                out.push(Reply::Usage(usage));
            }
        }
    }
}

/// The events of a stream event that is not its format's JSON, as an
/// [`Reply::Other`] so filters read it; one with no data, such as a comment,
/// says nothing.
pub(crate) fn unreadable_event(name: &Arc<str>, raw: &Bytes, out: &mut Vec<Reply>) {
    if let Some(data) = event_data(raw) {
        out.push(Reply::other(name.clone(), data));
    }
}

/// The fault of a provider's error event `v`, whose data goes as it came
/// to a client of this format.
pub(crate) fn error_event(name: &Arc<str>, raw: &[u8], v: &Value) -> Fault {
    let data = Bytes::from(event_data(raw).unwrap_or_default());
    provider_error(StatusCode::OK, v).with(respan_gateway_http::ProviderError::new(
        name.to_string(),
        http::Response::new(data),
    ))
}

/// A message's content as a string if it is one text with nothing beside
/// it, and parts otherwise; a part in a format other than `format` is refused.
pub(crate) fn write_content(format: &str, content: &[Content]) -> Result<Value, Fault> {
    Ok(match content {
        [] => "".into(),
        [Content::Text { text, extra, .. }] if kept_text_fields(format, extra)?.is_empty() => {
            text.as_str().into()
        }
        parts => parts
            .iter()
            .map(|p| write_part(format, p))
            .collect::<Result<_, _>>()?,
    })
}

/// Content of `format` as parts: a string is one text part.
pub(crate) fn read_content(format: &str, v: &Value) -> Vec<Content> {
    match v {
        Value::String(text) => vec![Content::from(text.as_str())],
        Value::Array(parts) => parts.iter().map(|p| read_part(format, p)).collect(),
        Value::Null => Vec::new(),
        other => vec![Content::other(format, other.clone())],
    }
}

/// The fields of a text part's `extra` to write in `format`: a cache marker
/// is dropped by another format, any other field refused.
fn kept_text_fields(format: &str, extra: &Extra) -> Result<Map<String, Value>, Fault> {
    let keep = Keep {
        own: &[],
        droppable: &["cache_control"],
    };
    kept_fields(format, extra, |key| format!("a text part's '{key}'"), keep)
}

/// A part as `format` has it: text, an Anthropic tool block, or a part in
/// that format only.
fn write_part(format: &str, c: &Content) -> Result<Value, Fault> {
    match c {
        Content::Text { text, extra, .. } => {
            let mut part = Map::new();
            part.insert("type".into(), "text".into());
            part.insert("text".into(), text.as_str().into());
            part.extend(kept_text_fields(format, extra)?);
            Ok(part.into())
        }
        Content::ToolCall { .. } | Content::ToolResult { .. } if format == ANTHROPIC => {
            anthropic_block(c)
        }
        Content::Other { format: f, raw, .. } if **f == *format => Ok(Value::clone(raw)),
        Content::Other { format: f, .. } => Err(untranslatable(&format!("a part in {f}'s shape"))),
        _ => Err(untranslatable("an unsupported kind of part")),
    }
}

/// A text part, whose other fields, such as a cache marker, are its `extra`;
/// an Anthropic tool block; or other content, which only a call in the
/// format it is in can send.
fn read_part(format: &str, p: &Value) -> Content {
    let tool = || anthropic_part(p).filter(|_| format == ANTHROPIC);
    match (p["type"].as_str(), p["text"].as_str()) {
        (Some("text"), Some(text)) => {
            let fields = p.as_object().into_iter().flatten();
            let fields = fields.filter(|(k, v)| *k != "type" && *k != "text" && says(v));
            let fields = fields.map(|(k, v)| (k.clone(), v.clone())).collect();
            let mut part = Content::from(text);
            if let Content::Text { extra, .. } = &mut part {
                *extra = extra_of(format, fields);
            }
            part
        }
        _ => tool().unwrap_or_else(|| Content::other(format, p.clone())),
    }
}

/// A message as a conversation keeps it: its other fields in its `extra`.
pub(crate) fn read_message(format: &str, m: &Value, roles: &Names<Role>) -> Message {
    let name = m["role"].as_str().unwrap_or_default();
    let role = roles
        .value(name)
        .unwrap_or_else(|| Role::other(format, name));
    let fields = m.as_object().into_iter().flatten();
    let fields = fields.filter(|(k, v)| *k != "role" && *k != "content" && says(v));
    let mut message = Message::new(role, "");
    message.content = read_content(format, &m["content"]);
    message.extra.fields = fields.map(|(k, v)| (k.clone(), v.clone())).collect();
    tag(&mut message, format);
    message
}

/// Note that what a message's `extra` says is in `format`, if it says
/// anything the kind has no form for.
pub(crate) fn tag(message: &mut Message, format: &str) {
    message.extra.format = (!message.extra.is_empty()).then(|| Arc::from(format));
}

/// A message written anew: role, content and its other fields.
pub(crate) fn write_message(
    format: &str,
    m: &Message,
    roles: &Names<Role>,
) -> Result<Value, Fault> {
    let mut v = Map::new();
    v.insert("role".into(), role_of(format, m, roles)?);
    v.insert("content".into(), write_content(format, &m.content)?);
    let fields = |_: &str| "messages with fields the kind has no place for".to_owned();
    write_extra(format, &m.extra, &mut v, fields, Keep::default())?;
    Ok(v.into())
}

/// The role to write: another format's roles are refused.
fn role_of(format: &str, m: &Message, roles: &Names<Role>) -> Result<Value, Fault> {
    let role = match &m.role {
        Role::Other {
            format: f, name, ..
        } if **f == *format => Some(name.as_str()),
        Role::Other { .. } => None,
        role => roles.name(role),
    };
    let role = role.map(Value::from);
    role.ok_or_else(|| untranslatable("a role this format has no place for"))
}

/// Turns a provider's running usage totals into increments.
#[derive(Debug, Default)]
pub(crate) struct Totals(Usage);

/// A provider can only express its known token counters.
pub(crate) fn supported_usage(usage: &Usage, units: &[&str]) -> Result<(), Fault> {
    for (unit, amount) in usage.iter() {
        if !units.contains(&unit.as_ref()) && amount != 0 {
            return Err(unwritable_usage());
        }
    }
    Ok(())
}

impl Totals {
    /// What `total` adds to the totals seen so far; usage only grows.
    pub(crate) fn increment(&mut self, total: &Usage) -> Usage {
        let mut step = Usage::default();
        for (unit, n) in total.iter() {
            let seen = self.0.get(&unit).unwrap_or_default();
            let more = n.saturating_sub(seen);
            if more > 0 {
                step.add(unit.clone(), more);
                self.0.add(unit, more);
            }
        }
        step
    }
}

/// Stop sequences, or `None` for none.
pub(crate) fn stop_json(stop: &[String]) -> Option<Value> {
    (!stop.is_empty()).then(|| stop.into())
}

/// A format's names, under its own name, for a conversation type's values;
/// the first of several is written.
pub(crate) struct Names<T: 'static>(pub &'static [(&'static str, T)], pub &'static str);

impl<T: PartialEq + Clone> Names<T> {
    /// The value `name` means, if it is one of these.
    pub(crate) fn value(&self, name: &str) -> Option<T> {
        self.0
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, v)| v.clone())
    }

    /// The name written for `value`, if this format has one.
    pub(crate) fn name(&self, value: &T) -> Option<&'static str> {
        self.0.iter().find(|(_, v)| v == value).map(|(n, _)| *n)
    }
}

impl Names<Finish> {
    /// A provider's reason, as a [`Finish`].
    pub(crate) fn finish(&self, reason: &str) -> Finish {
        self.value(reason)
            .unwrap_or_else(|| Finish::other(self.1, reason))
    }

    /// The reason to write: a native one, only in its own format; others
    /// are refused, not invented as a normal completion.
    pub(crate) fn reason<'a>(&self, finish: &'a Finish) -> Result<&'a str, Fault> {
        match finish {
            Finish::Other { format, reason, .. } if **format == *self.1 => Ok(reason),
            Finish::Other { .. } => Err(unwritable_content()),
            _ => self.name(finish).ok_or_else(unwritable_content),
        }
    }
}

/// An edit of usage this format cannot represent.
pub(crate) fn unwritable_usage() -> Fault {
    Fault::config("a plugin's change cannot be made: this format cannot represent the usage")
}

/// A request carrying what cannot be sent in another format.
pub(crate) fn untranslatable(what: &str) -> Fault {
    BadRequest::fault(format!("{what} cannot be translated to another format yet"))
}

/// A 502 for content the client's format cannot write anew, such as a tool call.
pub(crate) fn unwritable_content() -> Fault {
    let message = "the response holds content that cannot yet be written anew in this format";
    Fault::new(message)
        .with(ErrorCode("untranslatable".into()))
        .with(Status(502))
}

/// A stream that ended before its end event.
pub(crate) fn ended(done: bool) -> Result<(), Fault> {
    match done {
        true => Ok(()),
        false => Err(Truncated::fault()),
    }
}

/// Whether a JSON value says anything: not null or empty.
pub(crate) fn says(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
        _ => true,
    }
}

pub(crate) fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// The fields of `v` not in `known` that say something other than their
/// `inert` value.
pub(crate) fn unsupported_fields(
    v: &Value,
    known: &[&str],
    inert: &[(&str, f64)],
) -> Map<String, Value> {
    let number = |v: &Value| v.as_f64().or(v.as_bool().map(f64::from));
    let fields = v.as_object().into_iter().flatten();
    fields
        .filter(|(key, value)| {
            let harmless = !says(value)
                || inert
                    .iter()
                    .any(|(k, n)| k == key && number(value) == Some(*n));
            !known.contains(&key.as_str()) && !harmless
        })
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// Set `key` to `value`, or remove it for `None`.
pub(crate) fn set(v: &mut Map<String, Value>, key: &str, value: Option<Value>) {
    match value {
        Some(value) => v.insert(key.into(), value),
        None => v.shift_remove(key),
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AnthropicMessages, OpenAiChat};
    use respan_gateway_sdk::Details;
    use serde_json::json;

    fn usage_of(reply: &Reply) -> Option<u64> {
        match reply {
            Reply::Usage(u) => u.get(crate::conversation::units::INPUT_TOKENS),
            _ => None,
        }
    }

    #[tokio::test]
    async fn held_usage_survives_semantic_failure_but_not_a_wire_failure() {
        use respan_gateway_sdk::futures::{StreamExt, stream};
        let fixtures = [
            (
                "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"}}],\"usage\":{\"prompt_tokens\":2}}\n\n",
                "data: {\"error\":{\"message\":\"semantic\"}}\n\n",
            ),
            (
                "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"m\",\"content\":[],\"usage\":{\"input_tokens\":2,\"output_tokens\":0}}}\n\n",
                "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"semantic\"}}\n\n",
            ),
        ];
        for (provider, (start, error)) in fixtures.into_iter().enumerate() {
            for wire in [false, true] {
                let ending = if wire {
                    Err(Upstream::fault("wire"))
                } else {
                    Ok(Bytes::from(error))
                };
                let body = Body::stream(stream::iter([Ok(Bytes::from(start)), ending]));
                let mut response = http::Response::new(body);
                response
                    .headers_mut()
                    .insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
                let request = http::Request::new(Bytes::new());
                let values = if provider == 0 {
                    OpenAiChat.read_response(response, request)
                } else {
                    AnthropicMessages.read_response(response, request)
                }
                .collect::<Vec<_>>()
                .await;
                let error = values
                    .last()
                    .expect("terminal fault")
                    .as_ref()
                    .expect_err("stream failed");
                assert_eq!(error.message(), if wire { "wire" } else { "semantic" });
                if wire {
                    assert_eq!(
                        values.len(),
                        1,
                        "wire failure must not finalize held starts"
                    );
                } else {
                    assert_eq!(
                        values.len(),
                        2,
                        "semantic failure retains one usage-only event"
                    );
                    let usage = values[0].as_ref().expect("salvaged usage");
                    assert!(!usage.is_content());
                    assert_eq!(usage_of(usage), Some(2));
                }
            }
        }
    }

    #[tokio::test]
    async fn finite_and_streamed_response_reads_share_framing_and_errors() {
        use respan_gateway_sdk::futures::{TryStreamExt, stream};

        for anthropic in [false, true] {
            for streamed in [false, true] {
                let request = http::Request::new(Bytes::from(
                    json!({
                        "stream": streamed, "stream_options": {"include_usage": true}
                    })
                    .to_string(),
                ));
                let replies = [
                    Reply::start("r", "m"),
                    Reply::block_start(0, Block::Text),
                    Reply::text(0, "hello"),
                    Reply::block_end(0),
                    Reply::finished(Finish::Stop),
                ];
                let response = if anthropic {
                    AnthropicMessages.encode_response(&request, &replies)
                } else {
                    OpenAiChat.encode_response(&request, &replies)
                }
                .expect("encode finite answer");
                let (head, bytes) = response.into_parts();
                let mut refused = head.clone();
                refused.status = StatusCode::TOO_MANY_REQUESTS;
                let mut empty = head.clone();
                empty.status = StatusCode::NO_CONTENT;
                // Whole values, byte-split SSE, incomplete SSE, malformed JSON,
                // HTTP refusal, and bodyless answers all use the same semantics.
                let cases = [
                    (head.clone(), bytes.clone()),
                    (head.clone(), bytes.slice(..bytes.len() / 2)),
                    (head, Bytes::from_static(b"not-json")),
                    (
                        refused,
                        Bytes::from_static(b"{\"error\":{\"message\":\"busy\"}}"),
                    ),
                    (empty, Bytes::from_static(b"unread bodyless garbage")),
                ];
                for (head, bytes) in cases {
                    let call = request.clone().into_parts().0;
                    let finite = http::Response::from_parts(head.clone(), bytes.clone());
                    let finite = if anthropic {
                        AnthropicMessages.decode_response(finite, &call)
                    } else {
                        OpenAiChat.decode_response(finite, &call)
                    };
                    let chunks: Vec<_> = bytes
                        .iter()
                        .map(|b| Ok(Bytes::copy_from_slice(&[*b])))
                        .collect();
                    let live = http::Response::from_parts(head, Body::stream(stream::iter(chunks)));
                    let live = if anthropic {
                        AnthropicMessages.read_response(live, request.clone())
                    } else {
                        OpenAiChat.read_response(live, request.clone())
                    }
                    .try_collect::<Vec<_>>()
                    .await;
                    match (finite, live) {
                        (Ok(finite), Ok(live)) => assert_eq!(finite, live),
                        (Err(finite), Err(live)) => {
                            assert_eq!(
                                respan_gateway_http::error_code(&finite),
                                respan_gateway_http::error_code(&live)
                            );
                            assert_eq!(finite.message(), live.message());
                        }
                        (finite, live) => panic!("finite/live mismatch: {finite:?} / {live:?}"),
                    }
                }
            }
        }
    }

    #[test]
    fn native_context_window_finish_survives_reframing_but_not_translation() {
        let request =
            |stream| http::Request::new(Bytes::from(json!({"stream": stream}).to_string()));
        let call = http::Request::new(()).into_parts().0;
        let native = "model_context_window_exceeded";
        let unary = json!({
            "id": "m", "type": "message", "role": "assistant", "model": "claude-x",
            "content": [{"type": "text", "text": "partial"}],
            "stop_reason": native,
            "usage": {"input_tokens": 4, "output_tokens": 2}
        });
        let as_unary = http::Response::new(Bytes::from(unary.to_string()));
        let replies = AnthropicMessages
            .decode_response(as_unary, &call)
            .expect("decode unary");
        let framed = AnthropicMessages
            .encode_response(&request(true), &replies)
            .expect("SSE");
        let body = String::from_utf8_lossy(framed.body());
        assert!(
            body.contains(&format!("\"stop_reason\":\"{native}\"")),
            "{body}"
        );
        assert!(
            OpenAiChat
                .encode_response(&request(false), &replies)
                .is_err()
        );

        let sse = format!(
            "event: message_start\ndata: {{\"type\":\"message_start\",\"message\":{{\"id\":\"m\",\"model\":\"claude-x\",\"usage\":{{\"input_tokens\":4,\"output_tokens\":0}}}}}}\n\n\
             event: content_block_start\ndata: {{\"type\":\"content_block_start\",\"index\":0,\"content_block\":{{\"type\":\"text\",\"text\":\"\"}}}}\n\n\
             event: content_block_delta\ndata: {{\"type\":\"content_block_delta\",\"index\":0,\"delta\":{{\"type\":\"text_delta\",\"text\":\"partial\"}}}}\n\n\
             event: content_block_stop\ndata: {{\"type\":\"content_block_stop\",\"index\":0}}\n\n\
             event: message_delta\ndata: {{\"type\":\"message_delta\",\"delta\":{{\"stop_reason\":\"{native}\"}},\"usage\":{{\"output_tokens\":2}}}}\n\n\
             event: message_stop\ndata: {{\"type\":\"message_stop\"}}\n\n"
        );
        let mut head = http::Response::new(()).into_parts().0;
        head.headers
            .insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
        let replies = AnthropicMessages
            .decode_response(http::Response::from_parts(head, sse.into()), &call)
            .expect("decode SSE");
        let framed = AnthropicMessages
            .encode_response(&request(false), &replies)
            .expect("unary");
        let body: Value = serde_json::from_slice(framed.body()).expect("reframed unary JSON");
        assert_eq!(body["stop_reason"], native);
        assert!(
            OpenAiChat
                .encode_response(&request(false), &replies)
                .is_err()
        );

        let unknown = Reply::finished(Finish::other("openai-chat", "unrecognized_native_finish"));
        assert!(
            AnthropicMessages
                .encode_response(&request(false), &[unknown])
                .is_err()
        );
    }

    #[test]
    fn anthrope_sse_to_unary_keeps_text_tool_text_and_tool_text_order() {
        let request = http::Request::new(Bytes::from_static(b"{\"stream\":false}"));
        let call = http::Request::new(()).into_parts().0;
        for (parts, expected) in [
            (
                vec![
                    json!({"type":"text","text":"before"}),
                    json!({"type":"tool_use","id":"t","name":"lookup","input":{"q":"a"}}),
                    json!({"type":"text","text":"after"}),
                ],
                vec!["text", "tool_use", "text"],
            ),
            (
                vec![
                    json!({"type":"tool_use","id":"t","name":"lookup","input":{"q":"a"}}),
                    json!({"type":"text","text":"after"}),
                ],
                vec!["tool_use", "text"],
            ),
        ] {
            let mut events = vec![
                json!({"type":"message_start","message":{"id":"m","model":"claude-x","usage":{"input_tokens":3,"output_tokens":0}}}),
            ];
            for (index, part) in parts.iter().enumerate() {
                events.push(json!({"type":"content_block_start","index":index,"content_block": match part["type"].as_str() {
                    Some("text") => json!({"type":"text","text":""}),
                    _ => json!({"type":"tool_use","id":part["id"],"name":part["name"],"input":{}}),
                }}));
                let delta = match part["type"].as_str() {
                    Some("text") => json!({"type":"text_delta","text":part["text"]}),
                    _ => {
                        json!({"type":"input_json_delta","partial_json":part["input"].to_string()})
                    }
                };
                events.push(json!({"type":"content_block_delta","index":index,"delta":delta}));
                events.push(json!({"type":"content_block_stop","index":index}));
            }
            events.extend([
                json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":2}}),
                json!({"type":"message_stop"}),
            ]);
            let body: String = events
                .iter()
                .map(|event| {
                    format!(
                        "event: {}\ndata: {event}\n\n",
                        event["type"].as_str().unwrap_or_default()
                    )
                })
                .collect();
            let mut head = http::Response::new(()).into_parts().0;
            head.headers
                .insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
            let replies = AnthropicMessages
                .decode_response(http::Response::from_parts(head, body.into()), &call)
                .expect("decode SSE");
            let rendered = AnthropicMessages
                .encode_response(&request, &replies)
                .expect("render unary");
            let rendered: Value = serde_json::from_slice(rendered.body()).expect("JSON answer");
            let content = rendered["content"].as_array().expect("blocks");
            let types: Vec<_> = content.iter().filter_map(|b| b["type"].as_str()).collect();
            assert_eq!(types, expected);
            assert_eq!(
                rendered["usage"],
                json!({"input_tokens":3,"output_tokens":2})
            );
            assert_eq!(rendered["stop_reason"], "tool_use");
            let texts: Vec<_> = content.iter().filter_map(|b| b["text"].as_str()).collect();
            assert_eq!(
                texts,
                if parts.len() == 3 {
                    vec!["before", "after"]
                } else {
                    vec!["after"]
                }
            );
            let tool = content
                .iter()
                .find(|b| b["type"] == "tool_use")
                .expect("tool");
            assert_eq!(tool["input"], json!({"q":"a"}));
        }
    }

    #[tokio::test]
    async fn non_success_sse_is_a_whole_refusal_in_finite_and_live_reads() {
        use respan_gateway_sdk::futures::{TryStreamExt, stream};
        let cases = [
            (
                false,
                "openai-chat",
                "{\"error\":{\"type\":\"requests\",\"message\":\"slow down\"}}",
            ),
            (
                true,
                "anthropic-messages",
                "{\"type\":\"error\",\"error\":{\"type\":\"rate_limit_error\",\"message\":\"slow down\"}}",
            ),
        ];
        let call = http::Request::new(Bytes::new());
        for (anthropic, _name, body) in cases {
            let successful_sse = if anthropic {
                "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m\",\"model\":\"claude-x\",\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
            } else {
                "data: {\"id\":\"c\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"
            };
            for body in [
                body,
                successful_sse,
                "event: error\ndata: {\"error\":{\"type\":\"rate_limit_error\",\"message\":\"slow down\"}}\n\n",
            ] {
                let mut response = http::Response::new(Bytes::from(body.to_owned()));
                *response.status_mut() = StatusCode::TOO_MANY_REQUESTS;
                response
                    .headers_mut()
                    .insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
                response
                    .headers_mut()
                    .insert("retry-after", HeaderValue::from_static("7"));
                let finite = if anthropic {
                    AnthropicMessages
                        .decode_response(response.clone(), &call.clone().into_parts().0)
                } else {
                    OpenAiChat.decode_response(response.clone(), &call.clone().into_parts().0)
                }
                .expect_err("non-success must not decode as successful SSE");
                let (head, bytes) = response.into_parts();
                let live = http::Response::from_parts(
                    head,
                    Body::stream(stream::iter([Ok(bytes.clone())])),
                );
                let live = if anthropic {
                    AnthropicMessages.read_response(live, call.clone())
                } else {
                    OpenAiChat.read_response(live, call.clone())
                }
                .try_collect::<Vec<_>>()
                .await
                .expect_err("non-success must refuse");
                for fault in [finite, live] {
                    assert_eq!(fault.get::<Status>().map(|s| s.0), Some(429));
                    assert_eq!(
                        fault.get::<respan_gateway_sdk::RetryAfter>().map(|r| r.0),
                        Some(std::time::Duration::from_secs(7))
                    );
                    assert_eq!(
                        fault
                            .get::<respan_gateway_http::ProviderError>()
                            .map(|e| &e.response)
                            .map(|a| a.body().clone()),
                        Some(bytes.clone())
                    );
                }
            }
        }
    }

    #[test]
    fn orphan_tool_arguments_fail_in_unary_and_streaming_responses() {
        // Arguments of a call that never started, in either format's words.
        let orphan = [Reply::arguments(0, "{}")];
        let ask = http::Request::new(()).into_parts().0;
        for stream in [false, true] {
            let body: Bytes = json!({"stream": stream, "stream_options": {"include_usage": true}})
                .to_string()
                .into();
            let request = http::Request::from_parts(ask.clone(), body);
            assert!(
                OpenAiChat.encode_response(&request, &orphan).is_err(),
                "OpenAI stream={stream}"
            );
            assert!(
                AnthropicMessages
                    .encode_response(&request, &orphan)
                    .is_err(),
                "Anthropic stream={stream}"
            );
        }
    }

    #[test]
    fn interleaved_calls_and_repeated_starts_remain_writable() {
        // As OpenAI streams parallel calls: interleaved, a start repeated.
        let call = |block: u32, id: &str, name: &str| {
            Reply::block_start(
                block,
                Block::ToolCall {
                    id: id.into(),
                    name: name.into(),
                },
            )
        };
        let replies = [
            call(0, "a", "alpha"),
            Reply::arguments(0, "{\"a\":"),
            call(1, "b", "beta"),
            Reply::arguments(1, "{\"b\":"),
            Reply::arguments(0, "1}"),
            call(1, "b", "beta"),
            Reply::arguments(1, "2}"),
            Reply::block_end(0),
            Reply::block_end(1),
        ];
        let ask = http::Request::new(()).into_parts().0;
        for stream in [false, true] {
            let body: Bytes = json!({"stream":stream,"stream_options":{"include_usage":true}})
                .to_string()
                .into();
            let request = http::Request::from_parts(ask.clone(), body);
            let openai = OpenAiChat.encode_response(&request, &replies);
            assert!(openai.is_ok(), "OpenAI stream={stream}");
            let anthropic = AnthropicMessages.encode_response(&request, &replies);
            let anthropic = anthropic.expect("Anthropic");
            let text = String::from_utf8_lossy(anthropic.body()).into_owned();
            let calls: Vec<Value> = match stream {
                false => serde_json::from_str::<Value>(&text).expect("JSON")["content"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default(),
                true => {
                    let mut calls = vec![json!({"input": ""}), json!({"input": ""})];
                    for part in text.split("\n\n") {
                        let data = part.lines().find_map(|l| l.strip_prefix("data: "));
                        let v: Value =
                            serde_json::from_str(data.unwrap_or("null")).unwrap_or_default();
                        if let (Some(i), Some(json)) =
                            (v["index"].as_u64(), v["delta"]["partial_json"].as_str())
                        {
                            let was = calls[i as usize]["input"].as_str().unwrap_or("").to_owned();
                            calls[i as usize]["input"] = format!("{was}{json}").into();
                        }
                    }
                    calls
                }
            };
            let inputs: Vec<Value> = calls
                .iter()
                .map(|c| match &c["input"] {
                    Value::String(json) => serde_json::from_str(json).unwrap_or_default(),
                    input => input.clone(),
                })
                .collect();
            assert_eq!(
                inputs,
                [json!({"a": 1}), json!({"b": 2})],
                "stream={stream}: {text}"
            );
        }
    }

    #[test]
    fn a_cache_marker_stays_with_its_own_part_among_equal_text() {
        let old = json!([
            { "type": "text", "text": "A", "cache_control": { "type": "ephemeral" } },
            { "type": "text", "text": "A" },
        ]);
        let mut content = read_content("anthropic-messages", &old);
        // A part that says more is text with its own extra, and so equal text
        // beside it is a part of its own.
        assert!(matches!(&content[0], Content::Text { extra, .. } if !extra.is_empty()));
        assert_eq!(content[1], Content::from("A"));
        content.insert(0, Content::from("B"));
        content.push(Content::from("A"));
        let written = write_content("anthropic-messages", &content).expect("content");
        assert_eq!(written[0], json!({"type":"text","text":"B"}));
        assert_eq!(written[1], old[0], "the marker stays with its own part");
        assert_eq!(written[2], json!({"type":"text","text":"A"}));
        assert_eq!(written[3], json!({"type":"text","text":"A"}));
        // Another format drops a cache marker.
        let dropped = write_content("openai-chat", &content).expect("content");
        assert_eq!(dropped[1], json!({"type":"text","text":"A"}));
    }

    #[test]
    fn a_large_content_edit_keeps_every_part_as_it_was_without_matching_them() {
        let old: Value = (0..10_000)
            .map(|n| {
                json!({
                    "type": "text", "text": n.to_string(), "cache_control": {"type": "ephemeral"}
                })
            })
            .collect();
        let mut content = read_content("anthropic-messages", &old);
        content[5_000] = Content::from("edited");
        let written = write_content("anthropic-messages", &content).expect("content");
        assert_eq!(written.as_array().expect("an array").len(), 10_000);
        assert_eq!(written[5_000], json!({"type":"text","text":"edited"}));
        assert_eq!(written[4_999], old[4_999]);
        assert_eq!(written[5_001], old[5_001]);
    }

    #[test]
    fn large_content_in_another_order_is_written_in_that_order() {
        let old: Value = (0..1_000)
            .map(|n| json!({"type": "text", "text": n.to_string()}))
            .collect();
        let was = read_content("anthropic-messages", &old);
        let now: Vec<_> = was.iter().rev().cloned().collect();
        let written = write_content("anthropic-messages", &now).expect("content");
        let reversed: Vec<Value> = old
            .as_array()
            .expect("an array")
            .iter()
            .rev()
            .cloned()
            .collect();
        assert_eq!(written, Value::Array(reversed));
    }

    #[test]
    fn a_translated_error_keeps_each_formats_schema() {
        let shapes = [
            json!({"type": "error", "error": {"type": "overloaded_error", "message": ""}}),
            json!({"type": "https://tools.ietf.org/html/rfc7231#section-6.5.1", "title": "bad", "status": 400}),
            json!({"error": null, "type": "error", "message": "nope"}),
            json!({"object": "error", "message": {"detail": [{"msg": "Field required"}]}}),
            json!({"error": {"code": 429, "status": "RESOURCE_EXHAUSTED", "message": "quota"}}),
        ];
        for shape in shapes {
            let fault = provider_error(StatusCode::BAD_REQUEST, &shape);
            let words = (
                json!(respan_gateway_http::error_code(&fault)),
                json!(fault.message()),
            );
            let openai = OpenAiChat::error_body(&fault);
            let anthropic = AnthropicMessages::error_body(&fault);
            assert!(!fault.message().is_empty(), "{shape}");
            for error in [&openai["error"], &anthropic["error"]] {
                assert_eq!(
                    (error["type"].clone(), error["message"].clone()),
                    words,
                    "{shape}"
                );
            }
            assert!(openai["error"]["code"].is_string(), "{shape}");
            assert!(openai["error"].get("error").is_none(), "{shape}");
        }
    }

    #[test]
    fn details_that_are_not_an_object_are_ignored() {
        let fault = BadRequest::fault("m").with(Details(json!([1])));
        let body = OpenAiChat::error_body(&fault);
        assert_eq!(
            body["error"],
            json!({"message": "m", "type": "bad_request", "param": null, "code": "bad_request"})
        );
    }

    #[test]
    fn the_fault_says_message_and_type_the_provider_the_rest() {
        let details = json!({"message": "", "type": "https://x/p", "code": null, "extra": 1});
        let fault = BadRequest::fault("m").with(Details(details));
        let body = OpenAiChat::error_body(&fault);
        // The fault's words are the format's; the provider's `code` stays.
        let kept =
            json!({"message": "m", "type": "bad_request", "param": null, "code": null, "extra": 1});
        assert_eq!(body["error"], kept);
    }

    #[test]
    fn a_plugins_fault_takes_one_shape_on_every_path() {
        let details = json!({"message": "x", "type": 5, "code": 7, "param": 3, "category": "pii"});
        let fault = Fault::new("pii")
            .with(ErrorCode("blocked".into()))
            .with(Status(403))
            .with(Details(details));
        let body = OpenAiChat::error_body(&fault);
        // OpenAI's `code` and `param` are strings: a number is not kept.
        let kept = json!({"message": "pii", "type": "blocked", "param": null, "code": "blocked", "category": "pii"});
        assert_eq!(body["error"], kept);
        // A string, as OpenAI's own errors give, is kept.
        let details = json!({"param": "messages", "code": "context_length_exceeded"});
        let fault = BadRequest::fault("too long").with(Details(details));
        let error = &OpenAiChat::error_body(&fault)["error"];
        assert_eq!(
            (&error["param"], &error["code"]),
            (&json!("messages"), &json!("context_length_exceeded"))
        );
    }

    #[test]
    fn usage_totals_become_increments() {
        let mut totals = Totals::default();
        let total = |n| {
            let mut u = Usage::default();
            u.add("out", n);
            u
        };
        assert_eq!(totals.increment(&total(1)).get("out"), Some(1));
        assert_eq!(totals.increment(&total(3)).get("out"), Some(2));
        assert!(totals.increment(&total(3)).is_empty());
        assert!(totals.increment(&total(2)).is_empty(), "never negative");
    }

    #[test]
    fn a_whole_body_collects_blocks_in_order_and_refuses_orphans() {
        let mut whole = Whole::default();
        for reply in [
            Reply::start("r", "m"),
            Reply::block_start(3, Block::Text),
            Reply::text(3, "a"),
            Reply::block_start(3, Block::Text),
            Reply::text(3, "b"),
            Reply::block_end(3),
            Reply::other("f", json!(1)),
            Reply::finished(Finish::Stop),
        ] {
            whole.add(&reply).expect("a response");
        }
        let texts: Vec<_> = whole.parts().map(|p| p.text.as_str()).collect();
        assert_eq!(texts, ["ab"]);
        assert!(matches!(whole.items[1], Item::Other(..)));
        assert!(whole.add(&Reply::text(3, "c")).is_err(), "ended");
        assert!(whole.add(&Reply::text(9, "c")).is_err(), "never opened");
        assert!(whole.add(&Reply::block_end(9)).is_err());
        let call = Block::ToolCall {
            id: "t".into(),
            name: "f".into(),
        };
        assert!(whole.add(&Reply::block_start(3, call)).is_err(), "kind");
    }
}
