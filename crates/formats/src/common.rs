//! What the conversation formats share: unary bodies, request decoding and
//! encoding, usage totals, and small JSON helpers.

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::Arc;

use respan_gateway_core::bytes::{BufMut, Bytes, BytesMut};
use respan_gateway_core::serde_json::{self, Map, Value, json};
use respan_gateway_core::{Decoder, Encoder, Fault};
use respan_gateway_http::http::{self, HeaderValue, StatusCode, header::CONTENT_TYPE};
use respan_gateway_http::{Answer, Ask, bodiless};

use crate::Usage;
use crate::conversation::{Content, Conversation, Finish, Message, Piece, Reply, Role};
use crate::sse::{content_type, event_data};

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
    /// A whole response from the replies of another format, or a plugin.
    fn render(so_far: &SoFar) -> Value;
    /// Put an edited reply back into a whole response of this format.
    fn put(v: &mut Value, reply: &Reply);
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

/// Decodes each JSON request into a conversation that keeps its bytes.
pub(crate) fn request_decoder<W: Wire>() -> Box<dyn Decoder<Conversation>> {
    let name: Arc<str> = Arc::from(W::NAME);
    crate::message_decoder(
        move |raw: Bytes| match serde_json::from_slice::<Value>(&raw) {
            Ok(v) if v.is_object() => {
                let mut chat = W::read(&v);
                chat.model = v["model"].as_str().map(Into::into);
                Ok(chat.came(&name, raw))
            }
            _ => Err(not_an_object()),
        },
    )
}

fn not_an_object() -> Fault {
    Fault::bad_request("request body is not a JSON object")
}

/// Make `head` a JSON `POST` to `path`, adding to a relayed `body` what
/// the format needs ([`Wire::finish`]); a call whose inputs stream fails.
pub(crate) fn post_json<W: Wire>(
    mut head: Ask,
    body: &mut Bytes,
    path: &str,
    streams: bool,
) -> Box<dyn Encoder<Conversation, Ask>> {
    head.method = http::Method::POST;
    crate::set_path(&mut head, path);
    let json = HeaderValue::from_static("application/json");
    head.headers.insert(CONTENT_TYPE, json);
    let mut changed = streams;
    if W::unfinished(body)
        && let Ok(mut v) = object(body)
    {
        W::finish(&mut v);
        *body = to_bytes(&v).unwrap_or_else(|_| body.clone());
        changed = true;
    }
    let head = move |first: Option<&Conversation>| {
        if changed || first.is_some() {
            crate::body_changed(&mut head);
        }
        head
    };
    let encoder = match streams {
        true => crate::whole::unstreamable(W::NAME),
        false => Box::new(RequestEncoder::<W>(PhantomData)),
    };
    crate::headed(head, encoder)
}

/// Writes a conversation as the client's bytes, with changes written in, or
/// anew; then what the format adds ([`Wire::finish`]).
struct RequestEncoder<W>(PhantomData<W>);

impl<W: Wire> Encoder<Conversation, ()> for RequestEncoder<W> {
    fn head(&mut self, _first: Option<&Conversation>) {}

    fn encode(&mut self, chat: &Conversation, out: &mut BytesMut) -> Result<(), Fault> {
        let mut v = match chat.origin(W::NAME) {
            Some(o) if o.said == *chat && !W::unfinished(&o.bytes) => {
                out.extend_from_slice(&o.bytes);
                return Ok(());
            }
            Some(o) if o.said == *chat => object(&o.bytes)?,
            Some(o) => patch_request::<W>(&o.bytes, &o.said, chat)?,
            None => write_request::<W>(chat)?,
        };
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

/// A conversation as a new request, if nothing is lost.
pub(crate) fn write_request<W: Wire>(chat: &Conversation) -> Result<Map<String, Value>, Fault> {
    // What a plugin removed from `unsupported`, it chose to lose.
    if let Some(key) = chat.unsupported.keys().next() {
        return Err(untranslatable(&format!("'{key}' in the request")));
    }
    if !chat.messages.iter().all(Message::is_plain) {
        return Err(untranslatable("messages other than text"));
    }
    let mut v = match W::write(chat)? {
        Value::Object(v) => v,
        _ => Map::new(),
    };
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
    // What a plugin changed among the unsupported fields.
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
fn made_head(head: &mut Answer, asks_stream: bool) -> bool {
    let reframe = crate::sse::is_stream(head) != asks_stream;
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
pub(crate) fn response_decoder<W: Wire>(
    head: &Answer,
    call: &Ask,
    stream: impl FnOnce() -> Box<dyn Decoder<Reply>>,
) -> Box<dyn Decoder<Reply>> {
    if head.status.is_success() && bodiless(&call.method, head.status) {
        return crate::whole::nothing();
    }
    match crate::sse::is_stream(head) {
        true => stream(),
        false => unary_decoder::<W>(head),
    }
}

/// A response of format `W` to the client's request `ask` with `body`, from
/// the provider's head `answer` if relayed: a refusal, nothing for a
/// bodiless one, else `encoder` of the head, whether it relays the
/// provider's bytes, and what the client [`Asked`].
pub(crate) fn response_encoder<W: Wire>(
    answer: Option<&Answer>,
    ask: &Ask,
    body: &Bytes,
    refusal: Option<&Fault>,
    encoder: impl FnOnce(&Answer, bool, Asked) -> Box<dyn Encoder<Reply, ()>>,
) -> Box<dyn Encoder<Reply, Answer>> {
    let mut head = crate::answer_head(answer);
    let body = match refusal {
        Some(fault) => refused::<W>(&mut head, ask, fault),
        None if bodiless(&ask.method, head.status) => crate::whole::no_body(),
        None => {
            let asked = probe(body);
            let relayed = made_head(&mut head, asked.stream);
            encoder(&head, relayed, asked)
        }
    };
    crate::with_head(head, body)
}

/// A refusal in format `W` ([`whole::refusal`](crate::whole::refusal)), its
/// own words as this format's JSON error.
pub(crate) fn refused<W: Wire>(
    head: &mut Answer,
    ask: &Ask,
    fault: &Fault,
) -> Box<dyn Encoder<Reply, ()>> {
    let answered = |head: &mut Answer| crate::default_content_type(head, "application/json");
    crate::whole::refusal(W::NAME, head, ask, fault, answered, |head| {
        head.headers
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        match fault.answer::<Bytes>(W::NAME) {
            Some(words) => words.clone(),
            None => W::error_body(fault).to_string().into(),
        }
    })
}

/// One JSON message, decoded whole: one reply on success, else the
/// provider's error.
fn unary_decoder<W: Wire>(head: &Answer) -> Box<dyn Decoder<Reply>> {
    let (name, status): (Arc<str>, _) = (Arc::from(W::NAME), head.status);
    crate::whole::decoder(W::NAME, head, move |raw, out| {
        let v = serde_json::from_slice::<Value>(&raw).ok();
        if let Some(v) = v.as_ref().filter(|v| W::is_response(v)) {
            let mut reply = W::reply(v);
            reply.usage = W::usage(v);
            out.push(reply.came(&name, raw));
            return Ok(());
        }
        // An error in a success goes as it came to a client of this format.
        Err(match crate::sse::failure_json(&raw).filter(says_error) {
            Some(v) => provider_error(status, &v).with_answer(W::NAME, raw, false),
            None => Fault::upstream("the provider's success is not a response of its format"),
        })
    })
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

impl<W: Wire> Encoder<Reply, ()> for UnaryEncoder<W> {
    fn head(&mut self, _first: Option<&Reply>) {}

    fn encode(&mut self, reply: &Reply, _out: &mut BytesMut) -> Result<(), Fault> {
        self.replies.push(reply.clone());
        Ok(())
    }

    fn end(&mut self, end: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault> {
        end.clone()?;
        let replies = std::mem::take(&mut self.replies);
        let own = replies.iter().find_map(|r| r.origin(W::NAME));
        let body = match own.filter(|_| self.relayed) {
            Some(o) if replies.len() == 1 && o.said == replies[0] => o.bytes.clone(),
            Some(o) => rebuild_whole::<W>(&o.bytes, &o.said, &merged(&replies))?,
            None => {
                let mut so_far = SoFar::default();
                for reply in &replies {
                    so_far.add(reply)?;
                }
                W::render(&so_far).to_string().into()
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
    W::put(&mut v, reply);
    if reply.usage != said.usage && v["usage"].is_object() {
        v["usage"] = W::usage_body(&reply.usage);
    }
    reads_back::<W>(reply, &W::reply(&v))?;
    Ok(v.to_string().into())
}

/// Checks that a message rebuilt with `reply` reads back as it, so an edit
/// it has no place for fails. A start a filter took is not checked.
pub(crate) fn reads_back<W: Wire>(reply: &Reply, again: &Reply) -> Result<(), Fault> {
    let start = reply.start.is_none() || reply.start == again.start;
    let content = |r: &Reply| by_position(&W::in_order(&join(r.content.clone())));
    if start && reply.finish == again.finish && content(reply) == content(again) {
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
        if let Piece::Text { block, text } = &piece {
            let same = joined.iter_mut().rev().find_map(|had| match had {
                Piece::Text { block: b, text } if b == block => Some(text),
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
                Piece::Text { block, text } => self.text.entry(*block).or_default().push_str(text),
                Piece::Other { .. } => return Err(unwritable_content()),
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

/// Wraps a stream's decoder to hold replies before the first content, such
/// as the start, until it comes. A failure first drops them but for their
/// usage, pushed as one reply that is no content, so it can be refused.
pub(crate) fn holding_start(decoder: impl Decoder<Reply>) -> Box<dyn Decoder<Reply>> {
    Box::new(HoldStart {
        decoder,
        held: Vec::new(),
        through: false,
    })
}

struct HoldStart<D> {
    decoder: D,
    held: Vec<Reply>,
    /// Whether content came: later replies pass through.
    through: bool,
}

impl<D> HoldStart<D> {
    /// Keep back what `out` got from `from` on until the first content.
    fn hold(&mut self, out: &mut Vec<Reply>, from: usize, failed: bool) {
        for reply in out.split_off(from) {
            match reply {
                _ if self.through => out.push(reply),
                // Framing passes, unless behind what is held.
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

impl<D: Decoder<Reply>> Decoder<Reply> for HoldStart<D> {
    fn decode(&mut self, bytes: Bytes, out: &mut Vec<Reply>) -> Result<(), Fault> {
        let from = out.len();
        let decoded = self.decoder.decode(bytes, out);
        self.hold(out, from, decoded.is_err());
        decoded
    }

    fn end(&mut self, out: &mut Vec<Reply>) -> Result<(), Fault> {
        let from = out.len();
        let ended = self.decoder.end(out);
        self.hold(out, from, ended.is_err());
        out.append(&mut self.held);
        ended
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
    let piece = |content: Vec<Piece>| Reply {
        content,
        ..Reply::default()
    };
    let mut after = piece(content.split_off(held));
    let mut inside = Reply {
        content: content.split_off(before),
        ..reply.clone()
    };
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
            let content = patch_content(fields.get("content"), &was.content, &now.content)?;
            fields.insert("content".into(), content);
        }
        Ok(value)
    };
    let one = |m: &Message| match (m.source(), old.iter().find(|o| o.same_source(m))) {
        (Some(source), Some(was)) if was == m => Ok(source.clone()),
        (Some(source), Some(was)) => update(source, was, m),
        _ => write_message(m, roles),
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
pub(crate) fn write_message(m: &Message, roles: &Names<Role>) -> Result<Value, Fault> {
    let mut v = Map::new();
    v.insert("role".into(), role_of(m, roles)?);
    v.insert("content".into(), write_content(&m.content));
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

/// Content as a string if it is one text, and parts otherwise.
pub(crate) fn write_content(content: &[Content]) -> Value {
    match content {
        [] => "".into(),
        [Content::Text { text }] => text.as_str().into(),
        parts => parts.iter().map(write_part).collect(),
    }
}

/// Content `old`, which read `was`, changed to read `now`, parts keeping
/// their JSON, such as a cache marker, where they can.
pub(crate) fn patch_content(
    old: Option<&Value>,
    was: &[Content],
    now: &[Content],
) -> Result<Value, Fault> {
    let Some(Value::Array(old)) =
        old.filter(|v| v.as_array().is_some_and(|a| a.len() == was.len()))
    else {
        return Ok(write_content(now));
    };
    // Parts in the longest common subsequence keep their JSON. A gap
    // between them with as many parts as before is edited part by part;
    // otherwise which became which is unknown, so it is written anew.
    let (n, m) = (was.len(), now.len());
    let mut kept = vec![vec![0_usize; m + 1]; n + 1];
    for i in (0..n).rev() {
        for j in (0..m).rev() {
            kept[i][j] = match was[i] == now[j] {
                true => kept[i + 1][j + 1] + 1,
                false => kept[i + 1][j].max(kept[i][j + 1]),
            };
        }
    }
    let mut out = Vec::with_capacity(m);
    let gap = |out: &mut Vec<Value>, from: std::ops::Range<usize>, into: std::ops::Range<usize>| {
        if from.len() == into.len() {
            let pairs = from.zip(into);
            out.extend(pairs.map(|(i, j)| part_update(&old[i], &was[i], &now[j])));
        } else {
            out.extend(into.map(|j| write_part(&now[j])));
        }
    };
    let (mut i, mut j, mut gap_start) = (0, 0, (0, 0));
    while i < n && j < m {
        if was[i] == now[j] {
            gap(&mut out, gap_start.0..i, gap_start.1..j);
            out.push(old[i].clone());
            (i, j) = (i + 1, j + 1);
            gap_start = (i, j);
        } else if kept[i + 1][j] >= kept[i][j + 1] {
            i += 1;
        } else {
            j += 1;
        }
    }
    gap(&mut out, gap_start.0..n, gap_start.1..m);
    Ok(out.into())
}

/// Content as parts: a string is one text part.
pub(crate) fn read_content(v: &Value) -> Vec<Content> {
    match v {
        Value::String(text) => vec![Content::from(text.as_str())],
        Value::Array(parts) => parts.iter().map(read_part).collect(),
        Value::Null => Vec::new(),
        other => vec![Content::other(other.clone())],
    }
}

fn write_part(c: &Content) -> Value {
    match c {
        Content::Text { text } => json!({ "type": "text", "text": text }),
        Content::Other { raw } => Value::clone(raw),
    }
}

/// A part changed: text keeps the part, and any cache marker on it.
fn part_update(value: &Value, was: &Content, now: &Content) -> Value {
    match (was, now) {
        (Content::Text { .. }, Content::Text { text }) => {
            let mut value = value.clone();
            value["text"] = text.as_str().into();
            value
        }
        _ => write_part(now),
    }
}

/// Turns a provider's running usage totals into increments.
#[derive(Debug, Default)]
pub(crate) struct Totals(Usage);

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
pub(crate) fn read_message(m: &Value, roles: &Names<Role>) -> Message {
    let name = m["role"].as_str().unwrap_or_default();
    let role = roles
        .value(name)
        .unwrap_or_else(|| Role::Other(name.into()));
    let fields = m.as_object().into_iter().flatten();
    let unsupported = fields.filter(|(k, v)| *k != "role" && *k != "content" && says(v));
    let unsupported = unsupported.map(|(k, v)| (k.clone(), v.clone())).collect();
    let mut message = Message::new(role, "").with_source(m.clone());
    message.content = read_content(&m["content"]);
    message.unsupported = unsupported;
    message
}

/// A text part, even with a cache marker, or other content.
fn read_part(p: &Value) -> Content {
    let known = |k: &str| k == "type" || k == "text" || k == "cache_control";
    let fields = p.as_object().into_iter().flatten();
    let more = fields.filter(|(k, _)| !known(k)).any(|(_, v)| says(v));
    match (p["type"].as_str(), p["text"].as_str()) {
        (Some("text"), Some(text)) if !more => Content::from(text),
        _ => Content::other(p.clone()),
    }
}

/// Stop sequences, or `None` for none.
pub(crate) fn stop_json(stop: &[String]) -> Option<Value> {
    (!stop.is_empty()).then(|| stop.into())
}

/// A string that says something.
fn said(v: &Value) -> Option<&str> {
    v.as_str().filter(|s| !s.is_empty())
}

/// Whether a provider's JSON message says an error: an `error` that says
/// something, object or string, as compatible servers send.
pub(crate) fn says_error(v: &Value) -> bool {
    let error = &v["error"];
    says(error) && *error != false && *error != "" && error.as_f64() != Some(0.0)
}

/// The fault for a provider's error body in a response of `status`: an
/// error's status, else a 502, with the error's fields as details.
pub(crate) fn provider_error(status: StatusCode, v: &Value) -> Fault {
    // `{"error": {..}}` (OpenAI, Anthropic), the first of a list under
    // `error` or `errors` (Gemini, Cloudflare), or fields at the top.
    let fields = [&v["error"], &v["error"][0], &v["errors"][0]]
        .into_iter()
        .find(|e| e.is_object() && says(e))
        .or_else(|| (v["error"].is_null() && v.is_object()).then_some(v));
    let error = fields.unwrap_or(v);
    // Not an envelope's tag, such as `"type": "error"`, nor a problem
    // type's URI.
    let code = ["type", "error_type", "status", "code"]
        .iter()
        .filter_map(|k| error[*k].as_str())
        .find(|c| !c.is_empty() && *c != "error" && !c.contains(':'))
        .unwrap_or(respan_gateway_core::codes::UPSTREAM);
    // Its `message` (Mistral nests one in `detail[0].msg`), the error as
    // a string, a `detail` or its first `msg` (FastAPI), a problem's
    // `title`, or the body as a string; never empty.
    let message = said(&error["message"])
        .or(said(&v["message"]))
        .or(said(&error["message"]["detail"][0]["msg"]))
        .or(said(&v["error"]))
        .or(said(&v["detail"]))
        .or(said(&v["detail"][0]["msg"]))
        .or(said(&error["title"]))
        .or(said(v));
    let answered = || crate::whole::answered(status);
    let message = message.map_or_else(answered, str::to_owned);
    let to = match status.is_client_error() || status.is_server_error() {
        true => status,
        false => StatusCode::BAD_GATEWAY,
    };
    let fault = Fault::new(to, code.to_owned(), message);
    let Some(fields) = fields else { return fault };
    let mut details = fields.clone();
    // Fields at the top are the error's, not its null `error`.
    if let Some(top) = details.as_object_mut().filter(|_| std::ptr::eq(fields, v)) {
        top.remove("error");
    }
    fault.with_details(details)
}

/// Merge the fault's details into the format's error fields: each is added
/// if missing, and a string or null replaces the format's but for
/// `message` and `type`.
pub(crate) fn merge_details(mut error: Value, fault: &Fault) -> Value {
    let (Some(merged), Some(Value::Object(details))) = (error.as_object_mut(), fault.details())
    else {
        return error;
    };
    for (k, d) in details {
        let says_fault = matches!(k.as_str(), "message" | "type");
        if !merged.contains_key(k) || (!says_fault && (d.is_null() || d.is_string())) {
            merged.insert(k.clone(), d.clone());
        }
    }
    error
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
    Fault::new(
        StatusCode::BAD_GATEWAY,
        "untranslatable",
        "the response holds content that cannot yet be written anew in this format",
    )
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
    provider_error(StatusCode::OK, v).with_answer(name, data, false)
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

    #[test]
    fn only_an_error_that_says_something_is_one() {
        for (error, says) in [
            (json!({}), false),
            (json!(""), false),
            (json!(false), false),
            (Value::Null, false),
            (json!("x"), true),
            (json!([]), false),
            (json!(true), true),
            (json!(0), false),
            (json!(0.0), false),
            (json!({ "message": "x" }), true),
        ] {
            assert_eq!(says_error(&json!({ "error": error })), says, "{error}");
        }
    }

    #[test]
    fn real_provider_errors_keep_their_code_and_words() {
        let answered = "the provider returned 400";
        for (body, code, message) in [
            (
                json!({"error": {"type": "invalid_request_error", "message": "m"}}),
                "invalid_request_error",
                "m",
            ),
            (
                json!({"type": "error", "error": {"type": "overloaded_error", "message": "busy"}}),
                "overloaded_error",
                "busy",
            ),
            (
                json!({"error": "model not found"}),
                "upstream",
                "model not found",
            ),
            (
                json!({"error": "boom", "error_type": "overloaded"}),
                "overloaded",
                "boom",
            ),
            (
                json!({"error": {"code": 429, "status": "RESOURCE_EXHAUSTED", "message": "quota"}}),
                "RESOURCE_EXHAUSTED",
                "quota",
            ),
            (
                json!({"errors": [{"code": "rate_limited", "message": "slow"}], "success": false}),
                "rate_limited",
                "slow",
            ),
            (json!({"detail": "Not found"}), "upstream", "Not found"),
            (
                json!({"detail": [{"msg": "field required"}]}),
                "upstream",
                "field required",
            ),
            (
                json!({"object": "error", "type": "BadRequestError", "message": "bad"}),
                "BadRequestError",
                "bad",
            ),
            (
                json!({"jsonrpc": "2.0", "error": {"code": -32600, "message": "invalid"}}),
                "upstream",
                "invalid",
            ),
            (
                json!({"object": "error", "message": {"detail": [{"msg": "Field required"}]}, "type": "invalid_request_message_error"}),
                "invalid_request_message_error",
                "Field required",
            ),
            (
                json!({"type": "https://example.com/probs/nope", "title": "Nope"}),
                "upstream",
                "Nope",
            ),
            (json!("Unauthorized"), "upstream", "Unauthorized"),
            (json!({"error": {}, "message": "top"}), "upstream", "top"),
            (
                json!({"error": {"code": "x"}, "message": "top"}),
                "x",
                "top",
            ),
            (Value::Null, "upstream", answered),
        ] {
            let fault = provider_error(StatusCode::BAD_REQUEST, &body);
            assert_eq!((fault.code(), fault.message()), (code, message), "{body}");
        }
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
            let openai = crate::OpenAiChat::error_body(&fault);
            let anthropic = crate::AnthropicMessages::error_body(&fault);
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
        let fault = Fault::bad_request("m").with_details(json!([1]));
        let body = crate::OpenAiChat::error_body(&fault);
        assert_eq!(
            body["error"],
            json!({"message": "m", "type": "bad_request", "param": null, "code": "bad_request"})
        );
    }

    #[test]
    fn the_fault_says_message_and_type_the_provider_the_rest() {
        let details = json!({"message": "", "type": "https://x/p", "code": null, "extra": 1});
        let fault = Fault::bad_request("m").with_details(details);
        let body = crate::OpenAiChat::error_body(&fault);
        // The fault's words are the format's; the provider's `code` stays.
        let kept =
            json!({"message": "m", "type": "bad_request", "param": null, "code": null, "extra": 1});
        assert_eq!(body["error"], kept);
    }

    #[test]
    fn a_plugins_fault_takes_one_shape_on_every_path() {
        let details = json!({"message": "x", "type": 5, "code": 7, "param": 3, "category": "pii"});
        let fault = Fault::new(StatusCode::FORBIDDEN, "blocked", "pii").with_details(details);
        let body = crate::OpenAiChat::error_body(&fault);
        // OpenAI's `code` and `param` are strings: a number is not kept.
        let kept = json!({"message": "pii", "type": "blocked", "param": null, "code": "blocked", "category": "pii"});
        assert_eq!(body["error"], kept);
        // A string, as OpenAI's own errors give, is kept.
        let details = json!({"param": "messages", "code": "context_length_exceeded"});
        let fault = Fault::bad_request("too long").with_details(details);
        let error = &crate::OpenAiChat::error_body(&fault)["error"];
        assert_eq!(
            (&error["param"], &error["code"]),
            (&json!("messages"), &json!("context_length_exceeded"))
        );
    }

    #[test]
    fn an_envelopes_tag_is_no_error_code() {
        let code = |v: Value| provider_error(StatusCode::OK, &v).code().to_owned();
        assert_eq!(
            code(json!({"type": "error", "error": "Overloaded"})),
            "upstream"
        );
        assert_eq!(
            code(json!({"error": {"type": "overloaded_error"}})),
            "overloaded_error"
        );
        assert_eq!(
            code(json!({"error": {"status": "RESOURCE_EXHAUSTED", "code": 429}})),
            "RESOURCE_EXHAUSTED"
        );
        let cloudflare =
            json!({"errors": [{"code": "rate_limited", "message": "slow"}], "success": false});
        let fault = provider_error(StatusCode::TOO_MANY_REQUESTS, &cloudflare);
        assert_eq!((fault.code(), fault.message()), ("rate_limited", "slow"));
        assert_eq!(fault.details(), Some(&cloudflare["errors"][0]));
        let listed = provider_error(
            StatusCode::OK,
            &json!({"error": [{"type": "t", "message": "m"}]}),
        );
        assert_eq!((listed.code(), listed.message()), ("t", "m"));
        let message = provider_error(StatusCode::OK, &json!({"error": {"message": ""}}));
        assert_eq!(message.message(), "the provider returned 200");
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
