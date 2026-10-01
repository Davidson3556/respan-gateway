//! What the conversation formats share: unary bodies, request decoding and
//! encoding, usage totals, and small JSON helpers.

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::Arc;

use respan_gateway_http::bytes::{BufMut, Bytes, BytesMut};
use respan_gateway_http::http::{self, HeaderValue, StatusCode, header::CONTENT_TYPE};
use respan_gateway_http::{RequestHead, ResponseHead, Status, bodiless};
use respan_gateway_sdk::Fault;
use serde_json::{self, Map, Value, json};

use crate::Usage;
use crate::conversation::{Content, Conversation, Finish, Message, Piece, Reply, Role};
use crate::formats::errors::{provider_error, says_error};
use crate::formats::sse::{content_type, event_data};
use crate::tools::{Calls, fragments};
use respan_gateway_http::{Body, Encoder};
use respan_gateway_sdk::{EventSink, EventStream};

/// What a conversation format supplies; the plumbing around it is shared.
pub(crate) trait Wire: Send + Sync + 'static {
    /// The format's name.
    const NAME: &'static str;
    /// A JSON request as a conversation, noting what it has no place for.
    fn read(v: &Value) -> Conversation;
    /// A conversation as a request, refusing what this format cannot say.
    fn write(chat: &Conversation) -> Result<Value, Fault>;
    /// Write into the client's request the fields `after` changed from
    /// `before`, its decoding.
    fn patch(
        v: &mut Map<String, Value>,
        before: &Conversation,
        after: &Conversation,
    ) -> Result<(), Fault>;

    /// Whether a successful body is a whole response of this format.
    fn is_response(v: &Value) -> bool;
    /// What a whole response says, but its usage.
    fn reply(v: &Value) -> Reply;
    /// The usage totals of a whole response.
    fn usage(v: &Value) -> Usage;
    /// Usage as this format reports it.
    fn usage_body(u: &Usage) -> Value;
    /// Write edited totals into existing usage, preserving vendor fields.
    fn patch_usage(v: &mut Value, total: &Usage, said: &Usage, edited: &Usage)
    -> Result<(), Fault>;
    /// A whole response from the replies of another format, or a plugin.
    fn render(so_far: &SoFar) -> Result<Value, Fault>;
    /// Put an edited reply back into a whole response of this format.
    fn put(v: &mut Value, reply: &Reply) -> Result<(), Fault>;
    /// `content` in the order this format reads it back.
    fn in_order(content: &[Piece]) -> Vec<Piece> {
        content.to_vec()
    }
    /// The error body for `fault`, its details merged by [`merge_details`].
    fn error_body(fault: &Fault) -> Value;
    /// Whether the client's request `raw` needs [`Wire::finish`].
    fn unfinished(_raw: &[u8]) -> bool {
        false
    }
    /// Add to a request what this format needs, such as asking for usage.
    fn finish(_v: &mut Map<String, Value>) {}
}

/// Reads a finite JSON request while retaining its original bytes.
pub(crate) fn read_request<W: Wire>(raw: Bytes) -> Result<Conversation, Fault> {
    match serde_json::from_slice::<Value>(&raw) {
        Ok(v) if v.is_object() => {
            let mut chat = W::read(&v);
            chat.model = v["model"].as_str().map(Into::into);
            Ok(chat.came(&Arc::from(W::NAME), raw))
        }
        _ => Err(not_an_object()),
    }
}

fn not_an_object() -> Fault {
    Fault::bad_request("request body is not a JSON object")
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

/// Writes a conversation as the client's bytes, with changes written in, or
/// anew; then what the format adds ([`Wire::finish`]).
struct RequestEncoder<W>(PhantomData<W>);

impl<W: Wire> Encoder<Conversation> for RequestEncoder<W> {
    /// What cannot be written is the client's fault if its own request
    /// cannot be written either; else an edit caused it, a plugin's.
    fn encode(&mut self, chat: &Conversation, out: &mut BytesMut) -> Result<(), Fault> {
        let clients = |said: &&Conversation| request_bytes::<W>(said).is_err();
        let bytes = request_bytes::<W>(chat).map_err(|f| match chat.said().filter(clients) {
            Some(_) => Fault::bad_request(f.message()).with_source(f),
            None => chat.whose(f),
        })?;
        out.extend_from_slice(&bytes);
        Ok(())
    }

    fn end(&mut self, _end: &Result<(), Fault>, _out: &mut BytesMut) -> Result<(), Fault> {
        Ok(())
    }
}

/// `chat` as a request of this format: the client's bytes, with changes
/// written in, or anew; then what the format adds ([`Wire::finish`]).
fn request_bytes<W: Wire>(chat: &Conversation) -> Result<Bytes, Fault> {
    let mut v = match chat.came_in(W::NAME) {
        Some(o) if o.said == *chat && !W::unfinished(&o.bytes) => return Ok(o.bytes.clone()),
        Some(o) if o.said == *chat => object(&o.bytes)?,
        Some(o) => patch_request::<W>(&o.bytes, &o.said, chat)?,
        None => write_request::<W>(chat)?,
    };
    W::finish(&mut v);
    to_bytes(&v)
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

/// Refuses fields in a format other than `W` (see
/// [`Conversation::unsupported_in`]): they cannot be sent as `W`. Parts are
/// checked as they are written.
fn refuse_foreign<W: Wire>(chat: &Conversation) -> Result<(), Fault> {
    match chat.unsupported_in.as_deref() == Some(W::NAME) {
        true => Ok(()),
        false => refuse_leftovers(chat),
    }
}

/// Refuses what only the client's format can send. What a plugin removed
/// from `unsupported`, it chose to lose.
pub(crate) fn refuse_leftovers(chat: &Conversation) -> Result<(), Fault> {
    if let Some(key) = chat.unsupported.keys().next() {
        return Err(untranslatable(&format!("'{key}' in the request")));
    }
    let plain = |m: &Message| !matches!(m.role, Role::Other(_)) && m.unsupported.is_empty();
    match chat.messages.iter().all(plain) {
        true => Ok(()),
        false => Err(untranslatable(
            "messages with fields the kind has no place for",
        )),
    }
}

/// A conversation as a new request, if nothing is lost.
pub(crate) fn write_request<W: Wire>(chat: &Conversation) -> Result<Map<String, Value>, Fault> {
    // Fields in another format are rewritten or refused; parts are checked
    // as they are written.
    let chat = &*crate::tools::adopt(chat, W::NAME)?;
    let mut v = match W::write(chat)? {
        Value::Object(v) => v,
        _ => Map::new(),
    };
    v.extend(chat.unsupported.clone());
    set(&mut v, "model", chat.model.clone().map(Into::into));
    Ok(v)
}

/// The client's request `raw`, which read `before`, with what `chat`
/// changed written in.
pub(crate) fn patch_request<W: Wire>(
    raw: &Bytes,
    before: &Conversation,
    chat: &Conversation,
) -> Result<Map<String, Value>, Fault> {
    refuse_foreign::<W>(chat)?;
    let mut v = object(raw)?;
    // `stream_options` is only valid with a stream.
    if before.stream != chat.stream {
        v.insert("stream".into(), chat.stream.into());
        if !chat.stream {
            v.shift_remove("stream_options");
        }
    }
    if before.model != chat.model {
        set(&mut v, "model", chat.model.clone().map(Into::into));
    }
    W::patch(&mut v, before, chat)?;
    patch_fields(&mut v, &before.unsupported, &chat.unsupported);
    Ok(v)
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

/// Finish a success's head and return whether its replies are relayed as
/// their bytes. One framed otherwise than the client asked is reframed; a
/// head without a content type gets the client's framing.
fn made_head(head: &mut ResponseHead, asks_stream: bool) -> bool {
    let reframe = crate::formats::sse::is_stream(head) != asks_stream;
    if reframe {
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
    !reframe
}

/// A response of format `W` to the call `call`: nothing if it has no body,
/// else SSE events decoded by `stream` for a stream, else one JSON message.
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
            out.push(unary_reply::<W>(status, raw)?);
            Ok(())
        },
    )
}

/// A response of format `W` to the client's request `ask` with `body`, from
/// the provider's head `answer` if relayed: nothing for a bodiless one, else
/// `encoder` of the head, whether it relays the provider's bytes, and what
/// the client [`Asked`].
pub(crate) fn response_encoder(
    head: &mut ResponseHead,
    request: &http::Request<Bytes>,
    encoder: impl FnOnce(&ResponseHead, bool, Asked) -> Box<dyn Encoder<Reply>>,
) -> Option<Box<dyn Encoder<Reply>>> {
    if bodiless(request.method(), head.status) {
        return None;
    }
    let asked = probe(request.body());
    let relayed = made_head(head, asked.stream);
    Some(encoder(head, relayed, asked))
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
    match fault.answer::<Bytes>(W::NAME) {
        Some(words) => words.clone(),
        None => W::error_body(fault).to_string().into(),
    }
}

/// One JSON message, decoded whole: one reply on success, else the
/// provider's error.
pub(crate) fn unary_reply<W: Wire>(status: StatusCode, raw: Bytes) -> Result<Reply, Fault> {
    let v = serde_json::from_slice::<Value>(&raw).ok();
    if let Some(v) = v.as_ref().filter(|v| W::is_response(v)) {
        let mut reply = W::reply(v);
        reply.usage = W::usage(v);
        return Ok(reply.came(&Arc::from(W::NAME), raw));
    }
    Err(
        match crate::formats::sse::failure_json(&raw).filter(says_error) {
            Some(v) => provider_error(status, &v).with_answer(W::NAME, raw),
            None => Fault::upstream("the provider's success is not a response of its format"),
        },
    )
}

/// Writes one message at its end: the provider's with edits put back, or
/// one made from replies. Unless `relayed`, it writes from values. A
/// failure, before any byte, is refused.
pub(crate) struct UnaryEncoder<W> {
    relayed: bool,
    replies: Vec<Reply>,
    wire: PhantomData<W>,
}

impl<W: Wire> UnaryEncoder<W> {
    pub(crate) fn new(relayed: bool) -> Self {
        let (replies, wire) = (Vec::new(), PhantomData);
        Self {
            relayed,
            replies,
            wire,
        }
    }
}

impl<W: Wire> Encoder<Reply> for UnaryEncoder<W> {
    fn encode(&mut self, reply: &Reply, _out: &mut BytesMut) -> Result<(), Fault> {
        self.replies.push(reply.clone());
        Ok(())
    }

    fn end(&mut self, end: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault> {
        end.clone()?;
        let replies = std::mem::take(&mut self.replies);
        let own = replies.iter().find_map(|r| r.came_in(W::NAME));
        let body = match own.filter(|_| self.relayed) {
            Some(o) if replies.len() == 1 && o.said == replies[0] => o.bytes.clone(),
            Some(o) => rebuild_whole::<W>(&o.bytes, &o.said, &merged(&replies))?,
            None => {
                let mut so_far = SoFar::default();
                for reply in &replies {
                    so_far.add(reply)?;
                }
                W::render(&so_far)?.to_string().into()
            }
        };
        out.extend_from_slice(&body);
        Ok(())
    }
}

/// Replies as one message says them: the first start, all content in
/// order, texts of one block joined, the last finish, all usage.
pub(crate) fn merged(replies: &[Reply]) -> Reply {
    let mut all = Reply::default();
    for reply in replies {
        if all.start.is_none() {
            all.start = reply.start.clone();
        }
        all.content.extend(reply.content.iter().cloned());
        if reply.finish.is_some() {
            all.finish = reply.finish.clone();
        }
        all.usage += &reply.usage;
    }
    all.content = join(std::mem::take(&mut all.content));
    all
}

/// A whole response `raw`, which said `said`, with `reply` put back.
pub(crate) fn rebuild_whole<W: Wire>(
    raw: &Bytes,
    said: &Reply,
    reply: &Reply,
) -> Result<Bytes, Fault> {
    let mut v: Value = serde_json::from_slice(raw).unwrap_or_default();
    W::put(&mut v, reply)?;
    if reply.usage != said.usage {
        // The concrete format patches known fields without replacing vendor
        // fields in the usage object.
        W::patch_usage(&mut v, &reply.usage, &said.usage, &reply.usage)?;
    }
    let mut again = W::reply(&v);
    again.usage = W::usage(&v);
    reads_back::<W>(reply, &again)?;
    Ok(v.to_string().into())
}

/// Checks that a message rebuilt with `reply` reads back as it, so an edit
/// it has no place for fails. A start a filter took is not checked.
pub(crate) fn reads_back<W: Wire>(reply: &Reply, again: &Reply) -> Result<(), Fault> {
    let start = reply.start.is_none() || reply.start == again.start;
    let content = |r: &Reply| by_position(&W::in_order(&join(r.content.clone())));
    if start
        && reply.finish == again.finish
        && content(reply) == content(again)
        && same_usage(&reply.usage, &again.usage)
    {
        return Ok(());
    }
    Err(unpatchable("the provider's message has no place for it"))
}

/// `content` with its texts' blocks numbered by position.
fn by_position(content: &[Piece]) -> Vec<Piece> {
    let mut blocks: Vec<u32> = Vec::new();
    let mut numbered = content.to_vec();
    for p in &mut numbered {
        if let Piece::Text { block, .. } = p {
            let at = blocks.iter().position(|b| b == block).unwrap_or_else(|| {
                blocks.push(*block);
                blocks.len() - 1
            });
            *block = u32::try_from(at).unwrap_or(u32::MAX);
        }
    }
    numbered
}

/// `content` with texts of one block joined across whatever came between.
pub(crate) fn join(content: Vec<Piece>) -> Vec<Piece> {
    let mut joined: Vec<Piece> = Vec::with_capacity(content.len());
    for piece in content {
        if let Piece::Text { block, text, .. } = &piece {
            let same = joined.iter_mut().rev().find_map(|had| match had {
                Piece::Text { block: b, text, .. } if b == block => Some(text),
                _ => None,
            });
            if let Some(had) = same {
                had.push_str(text);
                continue;
            }
        }
        joined.push(piece);
    }
    joined
}

/// What a response written from replies has written so far.
#[derive(Debug, Default)]
pub(crate) struct SoFar {
    /// Whether the replies are the provider's own, relayed as their bytes.
    pub(crate) relayed: bool,
    /// The response's id and model, from its first start.
    pub(crate) start: Option<(String, String)>,
    /// Text by content block, in block order, for a whole response.
    pub(crate) text: BTreeMap<u32, String>,
    /// Tool calls, in the order they were made.
    pub(crate) calls: Calls,
    pub(crate) finish: Option<Finish>,
    pub(crate) usage: Usage,
}

impl SoFar {
    /// Note the first start, the last finish and the usage.
    pub(crate) fn note(&mut self, reply: &Reply) {
        if let Some(start) = &reply.start {
            self.start
                .get_or_insert_with(|| (start.id.clone(), start.model.clone()));
        }
        if let Some(finish) = &reply.finish {
            self.finish = Some(finish.clone());
        }
        self.usage += &reply.usage;
    }

    /// Add a reply to a whole response; other content is an `Err`.
    pub(crate) fn add(&mut self, reply: &Reply) -> Result<(), Fault> {
        self.note(reply);
        for piece in &reply.content {
            match piece {
                // Some providers refuse an empty text block.
                Piece::Text { text, .. } if text.is_empty() => {}
                Piece::Text { block, text, .. } => {
                    self.text.entry(*block).or_default().push_str(text)
                }
                Piece::Other { format, raw, .. } => match fragments(format, raw) {
                    Some(all) => {
                        for fragment in all {
                            self.calls.add(fragment)?;
                        }
                    }
                    None => return Err(unwritable_content()),
                },
                _ => return Err(unwritable_content()),
            }
        }
        Ok(())
    }
}

/// Whether `reply` has a finish or content other than empty text.
fn shows(reply: &Reply) -> bool {
    let empty = |p: &Piece| matches!(p, Piece::Text { text, .. } if text.is_empty());
    reply.finish.is_some() || reply.content.iter().any(|p| !empty(p))
}

type ReadEvent<W> = fn(&mut Reading<W>, Bytes, &mut Vec<Reply>) -> Result<(), Fault>;

/// Incremental provider response parsing, shared by streaming and finite reads.
/// Semantic failures retain held usage; wire failures never finalize the parser.
pub(crate) struct ReplyParser<W> {
    frames: crate::formats::sse::Parser,
    reading: Reading<W>,
    event: ReadEvent<W>,
    end: fn(&mut Reading<W>, &mut Vec<Reply>) -> Result<(), Fault>,
    held: Vec<Reply>,
    through: bool,
}

impl<W: Wire> ReplyParser<W> {
    pub(crate) fn new(
        event: ReadEvent<W>,
        end: fn(&mut Reading<W>, &mut Vec<Reply>) -> Result<(), Fault>,
        hold_start: bool,
    ) -> Self {
        Self {
            frames: crate::formats::sse::Parser::new(),
            reading: Reading::new(),
            event,
            end,
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
            .try_for_each(|raw| (self.event)(&mut self.reading, raw, out));
        self.hold(out, from, result.is_err());
        result
    }

    pub(crate) fn finish(&mut self, out: &mut Vec<Reply>) -> Result<(), Fault> {
        let from = out.len();
        let result = self
            .frames
            .finish()
            .into_iter()
            .try_for_each(|raw| (self.event)(&mut self.reading, raw, out))
            .and_then(|()| (self.end)(&mut self.reading, out));
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
            let mut usage = Reply::default();
            for held in self.held.drain(..) {
                usage.usage += &held.usage;
            }
            if !usage.usage.is_empty() {
                out.push(usage);
            }
        }
    }
}

/// An edited reply split around its message: the content before what the
/// message `holds`, the reply that rebuilds it with the start, and the
/// content after. The finish stays inside if `holds_finish` and nothing follows.
pub(crate) fn split_around(
    reply: &Reply,
    holds: impl Fn(&Piece) -> bool,
    holds_finish: bool,
) -> (Reply, Reply, Reply) {
    let mut content = join(reply.content.clone());
    let before = content.iter().take_while(|p| !holds(p)).count();
    let held = before + content[before..].iter().take_while(|p| holds(p)).count();
    let piece = |content: Vec<Piece>| {
        let mut part = Reply::default();
        part.content = content;
        part
    };
    let mut after = piece(content.split_off(held));
    let mut inside = reply.clone();
    inside.content = content.split_off(before);
    if !holds_finish || !after.content.is_empty() {
        after.finish = inside.finish.take();
    }
    (piece(content), inside, after)
}

/// A stream reader of format `W`'s events, each one reply that keeps its
/// bytes, up to its end, after which nothing is read.
pub(crate) struct Reading<W> {
    pub(crate) name: Arc<str>,
    pub(crate) totals: Totals,
    pub(crate) done: bool,
    wire: PhantomData<W>,
}

impl<W: Wire> Reading<W> {
    pub(crate) fn new() -> Self {
        let (name, wire) = (Arc::from(W::NAME), PhantomData);
        Self {
            name,
            totals: Totals::default(),
            done: false,
            wire,
        }
    }

    /// Truncated unless its end came.
    pub(crate) fn ended(&self) -> Result<(), Fault> {
        match self.done {
            true => Ok(()),
            false => Err(Fault::truncated()),
        }
    }
}

/// A conversation's messages in the client's format: a client message (by
/// shared source) as it was, or edited in its form; any other anew.
pub(crate) fn patch_messages(
    format: &str,
    old: &[Message],
    new: &[Message],
    roles: &Names<Role>,
) -> Result<Vec<Value>, Fault> {
    let update = |value: &Value, was: &Message, now: &Message| {
        let mut value = value.clone();
        let fields = value
            .as_object_mut()
            .ok_or_else(|| unpatchable("a message that is not a JSON object was changed"))?;
        if was.role != now.role {
            fields.insert("role".into(), role_of(now, roles)?);
        }
        patch_fields(fields, &was.unsupported, &now.unsupported);
        if was.content != now.content {
            let content = patch_content(format, fields.get("content"), &was.content, &now.content)?;
            fields.insert("content".into(), content);
        }
        Ok(value)
    };
    let one = |m: &Message| match (m.source(), old.iter().find(|o| o.same_source(m))) {
        (Some(source), Some(was)) if was == m => Ok(source.clone()),
        (Some(source), Some(was)) => update(source, was, m),
        _ => write_message(format, m, roles),
    };
    new.iter().map(one).collect()
}

/// Apply a change of unsupported fields from `was` to `now` to `v`.
fn patch_fields(v: &mut Map<String, Value>, was: &Map<String, Value>, now: &Map<String, Value>) {
    if was != now {
        v.retain(|k, _| !was.contains_key(k) || now.contains_key(k));
        v.extend(now.clone());
    }
}

/// A message written anew: role, content and unsupported fields.
pub(crate) fn write_message(
    format: &str,
    m: &Message,
    roles: &Names<Role>,
) -> Result<Value, Fault> {
    let mut v = Map::new();
    v.insert("role".into(), role_of(m, roles)?);
    v.insert("content".into(), write_content(format, &m.content)?);
    v.extend(m.unsupported.clone());
    Ok(v.into())
}

fn role_of(m: &Message, roles: &Names<Role>) -> Result<Value, Fault> {
    let role = match &m.role {
        Role::Other(role) => Some(role.as_str()),
        role => roles.name(role),
    };
    let role = role.map(Value::from);
    role.ok_or_else(|| unpatchable("a message has a role this format has no place for"))
}

/// Content as a string if it is one text, and parts otherwise; a part in
/// a format other than `format` is refused.
pub(crate) fn write_content(format: &str, content: &[Content]) -> Result<Value, Fault> {
    Ok(match content {
        [] => "".into(),
        [Content::Text { text, .. }] => text.as_str().into(),
        parts => parts
            .iter()
            .map(|p| write_part(format, p))
            .collect::<Result<_, _>>()?,
    })
}

/// Content `old`, which read `was`, changed to read `now`, parts keeping
/// their JSON, such as a cache marker, where they can.
pub(crate) fn patch_content(
    format: &str,
    old: Option<&Value>,
    was: &[Content],
    now: &[Content],
) -> Result<Value, Fault> {
    let Some(Value::Array(old)) =
        old.filter(|v| v.as_array().is_some_and(|a| a.len() == was.len()))
    else {
        return write_content(format, now);
    };
    let (n, m) = (was.len(), now.len());
    // Trimming an equal suffix changes LCS tie-breaking when a duplicate
    // appears earlier in `now`: [A] -> [B, A, A] must keep the first A's
    // source, not the last. Match the full sequence while it is affordable.
    // A large, unambiguous one-part edit needs only positional replacement;
    // refuse every other oversized case instead of guessing provenance.
    if (n + 1)
        .checked_mul(m + 1)
        .is_none_or(|cells| cells > 262_144)
    {
        let mut changed = was.iter().zip(now).enumerate().filter(|(_, (a, b))| a != b);
        let Some((at, _)) = changed.next() else {
            return if n == m {
                Ok(old.clone().into())
            } else {
                Err(unpatchable("too many ambiguous content parts to match"))
            };
        };
        let unique = n == m
            && changed.next().is_none()
            && !now
                .iter()
                .enumerate()
                .any(|(i, p)| i != at && p == &was[at])
            && !was
                .iter()
                .enumerate()
                .any(|(i, p)| i != at && p == &now[at]);
        if !unique {
            return Err(unpatchable("too many ambiguous content parts to match"));
        }
        let mut out = old.clone();
        out[at] = part_update(format, &old[at], &was[at], &now[at])?;
        return Ok(out.into());
    }
    let mut out = Vec::with_capacity(m);
    // Parts in the longest common subsequence keep their JSON. Equal-sized
    // gaps are edited part by part; unmatched insertions are written anew.
    let mut kept = vec![vec![0_usize; m + 1]; n + 1];
    for i in (0..n).rev() {
        for j in (0..m).rev() {
            kept[i][j] = match was[i] == now[j] {
                true => kept[i + 1][j + 1] + 1,
                false => kept[i + 1][j].max(kept[i][j + 1]),
            };
        }
    }
    let gap = |out: &mut Vec<Value>,
               from: std::ops::Range<usize>,
               into: std::ops::Range<usize>|
     -> Result<(), Fault> {
        if from.len() == into.len() {
            for (i, j) in from.zip(into) {
                out.push(part_update(format, &old[i], &was[i], &now[j])?);
            }
        } else {
            for j in into {
                out.push(write_part(format, &now[j])?);
            }
        }
        Ok(())
    };
    let (mut i, mut j, mut gap_start) = (0, 0, (0, 0));
    while i < n && j < m {
        if was[i] == now[j] {
            gap(&mut out, gap_start.0..i, gap_start.1..j)?;
            out.push(old[i].clone());
            (i, j) = (i + 1, j + 1);
            gap_start = (i, j);
        } else if kept[i + 1][j] >= kept[i][j + 1] {
            i += 1;
        } else {
            j += 1;
        }
    }
    gap(&mut out, gap_start.0..n, gap_start.1..m)?;
    Ok(out.into())
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

/// A part as `format` has it: text, or a part in that format only.
fn write_part(format: &str, c: &Content) -> Result<Value, Fault> {
    match c {
        Content::Text { text, .. } => Ok(json!({ "type": "text", "text": text })),
        Content::Other { format: f, raw, .. } if **f == *format => Ok(Value::clone(raw)),
        Content::Other { format: f, .. } => Err(untranslatable(&format!("a part in {f}'s shape"))),
        _ => Err(untranslatable("an unsupported kind of part")),
    }
}

/// A part changed: text keeps the part, and any cache marker on it.
fn part_update(format: &str, value: &Value, was: &Content, now: &Content) -> Result<Value, Fault> {
    match (was, now) {
        (Content::Text { .. }, Content::Text { text, .. }) => {
            let mut value = value.clone();
            value["text"] = text.as_str().into();
            Ok(value)
        }
        _ => write_part(format, now),
    }
}

/// Turns a provider's running usage totals into increments.
#[derive(Debug, Default)]
pub(crate) struct Totals(Usage);

/// An explicitly reported zero and an absent counter both add no usage.
pub(crate) fn same_usage(a: &Usage, b: &Usage) -> bool {
    a.iter()
        .chain(b.iter())
        .all(|(unit, _)| a.get(&unit).unwrap_or_default() == b.get(&unit).unwrap_or_default())
}

/// Decode one wire snapshot against the totals already sent to this client.
/// A stream event may report only some units, leaving the others unchanged.
pub(crate) fn usage_step(previous: &Usage, reported: &Usage) -> Usage {
    Totals(previous.clone()).increment(reported)
}

/// A provider can only express its known token counters as integral counts.
pub(crate) fn supported_usage(usage: &Usage, units: &[&str]) -> Result<(), Fault> {
    for (unit, amount) in usage.iter() {
        if (!units.contains(&unit.as_ref()) && amount != 0.0)
            || !amount.is_finite()
            || amount < 0.0
            || amount.fract() != 0.0
            || amount >= u64::MAX as f64
        {
            return Err(unpatchable("this format cannot represent the edited usage"));
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
            if n > seen {
                step.add(unit.clone(), n - seen);
                self.0.add(unit, n - seen);
            }
        }
        step
    }
}

/// A message as a conversation keeps it.
pub(crate) fn read_message(format: &str, m: &Value, roles: &Names<Role>) -> Message {
    let name = m["role"].as_str().unwrap_or_default();
    let role = roles
        .value(name)
        .unwrap_or_else(|| Role::Other(name.into()));
    let fields = m.as_object().into_iter().flatten();
    let unsupported = fields.filter(|(k, v)| *k != "role" && *k != "content" && says(v));
    let unsupported = unsupported.map(|(k, v)| (k.clone(), v.clone())).collect();
    let mut message = Message::new(role, "").with_source(m.clone());
    message.content = read_content(format, &m["content"]);
    message.unsupported = unsupported;
    message
}

/// A text part, even with a cache marker, or other content.
fn read_part(format: &str, p: &Value) -> Content {
    let known = |k: &str| k == "type" || k == "text" || k == "cache_control";
    let fields = p.as_object().into_iter().flatten();
    let more = fields.filter(|(k, _)| !known(k)).any(|(_, v)| says(v));
    match (p["type"].as_str(), p["text"].as_str()) {
        (Some("text"), Some(text)) if !more => Content::from(text),
        _ => Content::other(format, p.clone()),
    }
}

/// Stop sequences, or `None` for none.
pub(crate) fn stop_json(stop: &[String]) -> Option<Value> {
    (!stop.is_empty()).then(|| stop.into())
}

/// A format's names for a conversation type's values; the first of several
/// is written.
pub(crate) struct Names<T: 'static>(pub &'static [(&'static str, T)]);

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
            .unwrap_or_else(|| Finish::Other(reason.to_owned()))
    }

    /// A [`Finish`] as the provider's reason, else a plain stop.
    pub(crate) fn reason(&self, finish: &Finish) -> &'static str {
        let stop = || self.name(&Finish::Stop);
        self.name(finish).or_else(stop).unwrap_or("stop")
    }

    /// Write `finish` at `pointer` if it is there and means otherwise.
    pub(crate) fn rewrite(&self, v: &mut Value, pointer: &str, finish: &Finish) {
        let now = v.pointer(pointer).and_then(Value::as_str);
        if now.is_some_and(|r| self.finish(r) != *finish) {
            replace(v, pointer, self.reason(finish));
        }
    }
}

/// The text of `content`, if it is all text.
pub(crate) fn text_of(content: &[Content]) -> Option<String> {
    content
        .iter()
        .map(|c| match c {
            Content::Text { text, .. } => Some(text.as_str()),
            Content::Other { .. } => None,
            _ => None,
        })
        .collect()
}

/// A plugin's change that cannot be made in the client's format.
pub(crate) fn unpatchable(what: &str) -> Fault {
    Fault::config(format!("a plugin's change cannot be made: {what}"))
}

/// A request carrying what cannot be sent in another format.
pub(crate) fn untranslatable(what: &str) -> Fault {
    Fault::bad_request(format!("{what} cannot be translated to another format yet"))
}

/// A 502 for content the client's format cannot write anew, such as a tool call.
pub(crate) fn unwritable_content() -> Fault {
    let message = "the response holds content that cannot yet be written anew in this format";
    Fault::new("untranslatable", message).with(Status(502))
}

/// A stream event that is not its format's JSON, as other content, so
/// filters read it; one with no data, such as a comment, is no content.
pub(crate) fn unreadable_event(name: &Arc<str>, raw: Bytes) -> Reply {
    match event_data(&raw) {
        Some(data) => Reply::other(name.clone(), data).came(name, raw),
        None => Reply::default().came(name, raw),
    }
}

/// The fault of a provider's error event `v`, whose data goes as it came
/// to a client of this format.
pub(crate) fn error_event(name: &Arc<str>, raw: &[u8], v: &Value) -> Fault {
    let data = Bytes::from(event_data(raw).unwrap_or_default());
    provider_error(StatusCode::OK, v).with_answer(name, data)
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

/// Overwrite the value at `pointer` with `value`, if it is there.
pub(crate) fn replace(v: &mut Value, pointer: &str, value: impl Into<Value>) {
    if let Some(old) = v.pointer_mut(pointer) {
        *old = value.into();
    }
}

/// Write the sampling fields `after` changed from `before`, stop sequences
/// under `stop`.
pub(crate) fn patch_sampling(
    v: &mut Map<String, Value>,
    before: &Conversation,
    after: &Conversation,
    stop: &str,
) {
    if before.temperature != after.temperature {
        set(v, "temperature", after.temperature.map(Into::into));
    }
    if before.top_p != after.top_p {
        set(v, "top_p", after.top_p.map(Into::into));
    }
    if before.stop != after.stop {
        set(v, stop, stop_json(&after.stop));
    }
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
                    Err(Fault::upstream("wire"))
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
                    assert_eq!(
                        usage.usage.get(crate::conversation::units::INPUT_TOKENS),
                        Some(2.0)
                    );
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
                let mut reply = Reply::text(0, "hello");
                reply.finish = Some(Finish::Stop);
                let response = if anthropic {
                    AnthropicMessages.encode_response(&request, &[reply])
                } else {
                    OpenAiChat.encode_response(&request, &[reply])
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
                            assert_eq!(finite.code(), live.code());
                            assert_eq!(finite.message(), live.message());
                        }
                        (finite, live) => panic!("finite/live mismatch: {finite:?} / {live:?}"),
                    }
                }
            }
        }
    }

    #[test]
    fn orphan_tool_arguments_fail_in_unary_and_streaming_responses() {
        let fragments = [
            Piece::other(
                "openai-chat",
                json!([{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{}"}}]}}]),
            ),
            Piece::other(
                "anthropic-messages",
                json!({"type": "input_json_delta", "partial_json": "{}"}),
            ),
        ];
        let ask = http::Request::new(()).into_parts().0;
        for piece in fragments {
            let mut reply = Reply::default();
            reply.content = vec![piece];
            for stream in [false, true] {
                let body: Bytes =
                    json!({"stream": stream, "stream_options": {"include_usage": true}})
                        .to_string()
                        .into();
                let request = http::Request::from_parts(ask.clone(), body);
                assert!(
                    OpenAiChat
                        .encode_response(&request, std::slice::from_ref(&reply))
                        .is_err(),
                    "OpenAI stream={stream}"
                );
                assert!(
                    AnthropicMessages
                        .encode_response(&request, std::slice::from_ref(&reply))
                        .is_err(),
                    "Anthropic stream={stream}"
                );
            }
        }
    }

    #[test]
    fn interleaved_calls_and_repeated_starts_remain_writable() {
        let fragments = [
            json!([{"index":0,"delta":{"tool_calls":[{"index":0,"id":"a","type":"function","function":{"name":"alpha","arguments":"{\"a\":"}}]}}]),
            json!([{"index":0,"delta":{"tool_calls":[{"index":1,"id":"b","type":"function","function":{"name":"beta","arguments":"{\"b\":"}}]}}]),
            json!([{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"1}"}}]}}]),
            json!([{"index":0,"delta":{"tool_calls":[{"index":1,"id":"b","type":"function","function":{"name":"beta","arguments":"2}"}}]}}]),
        ];
        let replies: Vec<_> = fragments
            .into_iter()
            .map(|raw| Reply::other("openai-chat", raw))
            .collect();
        let mut all = SoFar::default();
        for reply in &replies {
            all.add(reply).expect("accumulate tool fragments");
        }
        assert_eq!(all.calls.openai()[0]["function"]["arguments"], "{\"a\":1}");
        assert_eq!(all.calls.openai()[1]["function"]["arguments"], "{\"b\":2}");
        assert_eq!(
            all.calls.anthropic().expect("assembled Anthropic calls")[1]["input"],
            json!({"b":2})
        );
        let ask = http::Request::new(()).into_parts().0;
        for stream in [false, true] {
            let body: Bytes = json!({"stream":stream,"stream_options":{"include_usage":true}})
                .to_string()
                .into();
            let request = http::Request::from_parts(ask.clone(), body);
            assert!(
                OpenAiChat.encode_response(&request, &replies).is_ok(),
                "OpenAI stream={stream}"
            );
            assert!(
                AnthropicMessages
                    .encode_response(&request, &replies)
                    .is_ok(),
                "Anthropic stream={stream}"
            );
        }
    }

    #[test]
    fn duplicate_suffix_does_not_move_a_cache_marker_to_the_last_match() {
        let old = json!([{
            "type": "text", "text": "A", "cache_control": {"type": "ephemeral"}
        }]);
        let was = read_content("anthropic-messages", &old);
        let now = [Content::from("B"), Content::from("A"), Content::from("A")];
        let patched = patch_content("anthropic-messages", Some(&old), &was, &now)
            .expect("patch Anthropic content");
        assert_eq!(patched[0], json!({"type":"text","text":"B"}));
        assert_eq!(patched[1], old[0], "the first A keeps the original marker");
        assert_eq!(patched[2], json!({"type":"text","text":"A"}));
    }

    #[test]
    fn a_large_content_edit_preserves_part_metadata_without_quadratic_matching() {
        let old: Value = (0..10_000)
            .map(|n| {
                json!({
                    "type": "text", "text": n.to_string(), "cache_control": {"type": "ephemeral"}
                })
            })
            .collect();
        let was = read_content("anthropic-messages", &old);
        let mut now = was.clone();
        now[5_000] = Content::from("edited");
        let patched = patch_content("anthropic-messages", Some(&old), &was, &now)
            .expect("patch Anthropic content");
        assert_eq!(
            patched.as_array().expect("patched content array").len(),
            10_000
        );
        assert_eq!(patched[5_000]["text"], "edited");
        assert_eq!(patched[5_000]["cache_control"], old[5_000]["cache_control"]);
        assert_eq!(patched[4_999], old[4_999]);
        assert_eq!(patched[5_001], old[5_001]);
    }

    #[test]
    fn ambiguous_large_content_edits_fail_before_allocating_a_matching_matrix() {
        let old: Value = (0..1_000)
            .map(|n| json!({"type": "text", "text": n.to_string()}))
            .collect();
        let was = read_content("anthropic-messages", &old);
        let now: Vec<_> = was.iter().rev().cloned().collect();
        assert!(patch_content("anthropic-messages", Some(&old), &was, &now).is_err());
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
            let words = (json!(fault.code()), json!(fault.message()));
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
        let fault = Fault::bad_request("m").with(Details(json!([1])));
        let body = OpenAiChat::error_body(&fault);
        assert_eq!(
            body["error"],
            json!({"message": "m", "type": "bad_request", "param": null, "code": "bad_request"})
        );
    }

    #[test]
    fn the_fault_says_message_and_type_the_provider_the_rest() {
        let details = json!({"message": "", "type": "https://x/p", "code": null, "extra": 1});
        let fault = Fault::bad_request("m").with(Details(details));
        let body = OpenAiChat::error_body(&fault);
        // The fault's words are the format's; the provider's `code` stays.
        let kept =
            json!({"message": "m", "type": "bad_request", "param": null, "code": null, "extra": 1});
        assert_eq!(body["error"], kept);
    }

    #[test]
    fn a_plugins_fault_takes_one_shape_on_every_path() {
        let details = json!({"message": "x", "type": 5, "code": 7, "param": 3, "category": "pii"});
        let fault = Fault::new("blocked", "pii")
            .with(Status(403))
            .with(Details(details));
        let body = OpenAiChat::error_body(&fault);
        // OpenAI's `code` and `param` are strings: a number is not kept.
        let kept = json!({"message": "pii", "type": "blocked", "param": null, "code": "blocked", "category": "pii"});
        assert_eq!(body["error"], kept);
        // A string, as OpenAI's own errors give, is kept.
        let details = json!({"param": "messages", "code": "context_length_exceeded"});
        let fault = Fault::bad_request("too long").with(Details(details));
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
        assert_eq!(totals.increment(&total(1.0)).get("out"), Some(1.0));
        assert_eq!(totals.increment(&total(3.0)).get("out"), Some(2.0));
        assert!(totals.increment(&total(3.0)).is_empty());
        assert!(totals.increment(&total(2.0)).is_empty(), "never negative");
    }
}
