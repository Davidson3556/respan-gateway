//! What the conversation formats share: unary bodies, request decoding and
//! patching, usage totals, and small JSON helpers.

use respan_gateway_core::{bytes, http, serde_json};

use crate::whole::Whole;
use std::collections::BTreeMap;
use std::marker::PhantomData;

use bytes::{Bytes, BytesMut};
use http::HeaderValue;
use http::StatusCode;
use http::header::CONTENT_TYPE;
use respan_gateway_core::{ClientView, Decoder, Doc, Encoder, Fault, Item, Part, Usage};
use serde_json::{Map, Value, json};

use crate::conversation::{Content, Conversation, Finish, Message, Reply, Role};
use crate::sse::{content_type, event_data};

/// What a conversation format supplies; the plumbing around it is shared.
pub(crate) trait Wire: Send + Sync + 'static {
    /// A JSON request as a conversation, noting what it has no place for.
    fn read(v: &Value) -> Conversation;
    /// A conversation as a request, refusing what this format cannot say.
    fn write(chat: &Conversation) -> Result<Value, Fault>;
    /// Write into the client's request the fields `after` changed from
    /// `before`, its decoding. Everything else keeps its original form.
    fn patch(
        v: &mut Map<String, Value>,
        before: &Conversation,
        after: &Conversation,
    ) -> Result<(), Fault>;

    /// Whether a successful body is a whole response of this format.
    fn is_response(v: &Value) -> bool;
    /// A whole response as events.
    fn replies(v: &Value) -> Vec<Reply>;
    /// The usage totals of a whole response.
    fn usage(v: &Value) -> Usage;
    /// A whole response from the events of another format, or a plugin.
    fn render(so_far: &SoFar) -> Value;
    /// Put edited events back into a whole response of this format.
    fn put(v: &mut Value, events: &[Reply]);
    /// `events` in the order this format reads them back, where the order
    /// means nothing, such as a choice's one text and one other.
    fn in_order(events: &[Reply]) -> Vec<Reply> {
        events.to_vec()
    }
    /// The error body for `fault`, its details merged by [`merge_details`].
    fn error_body(fault: &Fault) -> Value;
}

/// A JSON request as a conversation.
pub(crate) fn decode_request<W: Wire>(body: &Doc) -> Result<Conversation, Fault> {
    let v = body
        .json()
        .filter(|v| v.is_object())
        .ok_or_else(|| Fault::bad_request("request body is not a JSON object"))?;
    Ok(W::read(v))
}

/// A conversation as a body: the client's body patched, or a new body if
/// nothing is lost.
pub(crate) fn encode_request<W: Wire>(
    chat: &Conversation,
    original: Option<(&Doc, &Conversation)>,
) -> Result<Doc, Fault> {
    let Some((v, before)) = original.and_then(|(body, before)| Some((body.json()?, before))) else {
        // What a plugin removed from `unsupported`, it chose to lose.
        if let Some(key) = chat.unsupported.keys().next() {
            return Err(untranslatable(&format!("'{key}' in the request")));
        }
        if !chat.messages.iter().all(Message::is_plain) {
            return Err(untranslatable("messages other than text"));
        }
        return W::write(chat).map(Doc::from);
    };
    let mut v = v.clone();
    let obj = v
        .as_object_mut()
        .ok_or_else(|| Fault::bad_request("request body is not a JSON object"))?;
    // `stream_options` is only valid with a stream.
    if before.stream != chat.stream {
        obj.insert("stream".into(), chat.stream.into());
        if !chat.stream {
            obj.shift_remove("stream_options");
        }
    }
    W::patch(obj, before, chat)?;
    // What a plugin changed among the unsupported fields.
    patch_fields(obj, &before.unsupported, &chat.unsupported);
    Ok(Doc::from(v))
}

/// Pass on the client's end-to-end headers, such as `user-agent`, an app's
/// own, opted-in beta features or credentials, when the client speaks this
/// format ([`ClientView::is_own`]); across formats none pass. A header the
/// request already has is kept; the transport decides whose keys reach the
/// provider. For any format's
/// [`Format::prepare`](respan_gateway_core::Format::prepare); the kernel's
/// header rules apply after it.
///
/// Never passes what changes how the provider reads the request or who
/// sent it: `cookie`, `accept-encoding` (the gateway reads what it
/// relays), `forwarded`, `via`, `x-real-ip`, `x-forwarded-*`, method
/// overrides, `if-*` and `range`.
pub fn pass_headers(req: &mut http::Request<Doc>, client: ClientView<'_>) {
    if !client.is_own() {
        return;
    }
    let set: Vec<_> = req.headers().keys().cloned().collect();
    for (name, value) in &client.head().headers {
        if passes(name.as_str()) && !set.contains(name) {
            req.headers_mut().append(name.clone(), value.clone());
        }
    }
}

/// Whether [`pass_headers`] may pass on the client's header `name`.
fn passes(name: &str) -> bool {
    let denied = matches!(
        name,
        "cookie"
            | "accept-encoding"
            | "forwarded"
            | "via"
            | "x-real-ip"
            | "x-http-method-override"
            | "x-method-override"
            | "range"
    );
    !denied && !name.starts_with("x-forwarded-") && !name.starts_with("if-")
}

/// A JSON `POST` to `path`, as both formats send, with the client's query
/// when the client speaks this format.
pub(crate) fn post_json(
    body: Doc,
    path: &'static str,
    client: ClientView<'_>,
) -> Result<http::Request<Doc>, Fault> {
    let mut req = http::Request::new(body);
    *req.method_mut() = http::Method::POST;
    let query = client.head().uri.query().filter(|_| client.is_own());
    *req.uri_mut() = match query.filter(|q| !q.is_empty()) {
        Some(query) => format!("{path}?{query}").parse().map_err(|e| {
            Fault::bad_request("the request's query cannot be passed on").with_source(e)
        })?,
        None => http::Uri::from_static(path),
    };
    req.headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    Ok(req)
}

/// Finish a response's head and return whether it is still relayed. A
/// reframed one is not, and loses its content type. A head with no content
/// type, unless a relayed failure, gets the client's framing: SSE for a
/// successful stream, otherwise JSON.
pub(crate) fn made_head(
    head: &mut http::response::Parts,
    client: ClientView<'_>,
    relayed: bool,
) -> bool {
    let reframe = relayed && reframed(head, client);
    if reframe {
        head.headers.remove(CONTENT_TYPE);
    }
    // A relayed failure, such as a redirect, is left as it came.
    if content_type(head).is_none() && (!relayed || reframe || head.status.is_success()) {
        let stream = client.body().is_some_and(streams) && head.status.is_success();
        let content_type = match stream {
            true => "text/event-stream",
            false => "application/json",
        };
        head.headers
            .insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    }
    relayed && !reframe
}

/// Whether a success in the client's format is framed otherwise than the
/// client asked, as when a plugin changed `stream`; it is then written from
/// its events. Decoder and encoder both ask it of the provider's head, so
/// they agree.
fn reframed(head: &http::response::Parts, client: ClientView<'_>) -> bool {
    let streamed = crate::sse::is_stream(head);
    client.is_own() && head.status.is_success() && streamed != client.body().is_some_and(streams)
}

/// `decoder`, or for a reframed response, one that drops its framing and
/// the bytes of its items.
pub(crate) fn decoder_for(
    head: &http::response::Parts,
    client: ClientView<'_>,
    decoder: Box<dyn Decoder>,
) -> Box<dyn Decoder> {
    match reframed(head, client) {
        true => Box::new(Reframed(decoder)),
        false => decoder,
    }
}

struct Reframed(Box<dyn Decoder>);

impl Reframed {
    fn remake(from: usize, out: &mut Vec<Part>) {
        for part in out.split_off(from) {
            match part {
                Part::Item(item) => out.push(match item.events::<Reply>() {
                    Ok(events) => Item::new(events.to_vec()).into(),
                    Err(f) => Part::End(Err(f)),
                }),
                Part::Frame(_) => {}
                // Its error bytes are dropped with the framing.
                Part::End(Err(f)) => out.push(Part::End(Err(f.unsaid()))),
                part => out.push(part),
            }
        }
    }
}

impl Decoder for Reframed {
    fn decode(&mut self, bytes: Bytes, out: &mut Vec<Part>) {
        let from = out.len();
        self.0.decode(bytes, out);
        Self::remake(from, out);
    }

    fn finish(&mut self, out: &mut Vec<Part>) {
        let from = out.len();
        self.0.finish(out);
        Self::remake(from, out);
    }
}

/// One JSON message, decoded once whole: its events on success, otherwise
/// the provider's error relayed as it is.
pub(crate) fn unary_decoder<W: Wire>(status: StatusCode) -> Box<dyn Decoder> {
    Box::new(Whole::new(move |raw: Bytes, out: &mut Vec<Part>| {
        let end = if status.is_success() {
            let v = serde_json::from_slice::<Value>(&raw).ok();
            if let Some(v) = v.as_ref().filter(|v| W::is_response(v)) {
                // Usage first, so a body written in another format has it.
                let usage = W::usage(v);
                if !usage.is_empty() {
                    out.push(usage.into());
                }
                out.push(Item::decoded(W::replies(v), raw).into());
                return out.push(Part::End(Ok(())));
            }
            // An error in a success, even one framed as events, or a
            // success that is not a response.
            match crate::sse::failure_json(&raw).filter(says_error) {
                Some(v) => provider_error(status, &v),
                None => Fault::upstream("the provider's success is not a response of its format"),
            }
        } else {
            // A failure is read whole, even one framed as events.
            let v = crate::sse::failure_json(&raw).unwrap_or_default();
            provider_error(status, &v)
        };
        // Relayed as it is.
        out.push(Part::Frame(raw));
        out.push(Part::End(Err(end.said())));
    }))
}

/// Writes one message: the provider's own with any edits put back, a body
/// made from events, or the error.
pub(crate) struct UnaryEncoder<W> {
    ok: bool,
    /// The provider's message, held to the end for what a filter adds.
    own: Option<Relayed>,
    /// The events of another format, or a plugin.
    so_far: SoFar,
    wire: PhantomData<W>,
}

/// The provider's own message in a unary response.
struct Relayed {
    raw: Bytes,
    events: Vec<Reply>,
    edited: bool,
    /// Events a filter released after it.
    added: Vec<Reply>,
}

impl<W: Wire> UnaryEncoder<W> {
    pub(crate) fn new(head: &http::response::Parts) -> Self {
        Self {
            ok: head.status.is_success(),
            own: None,
            so_far: SoFar::default(),
            wire: PhantomData,
        }
    }
}

impl<W: Wire> Encoder for UnaryEncoder<W> {
    fn encode(&mut self, part: &Part, out: &mut BytesMut) -> Result<(), Fault> {
        match part {
            Part::Frame(b) => out.extend_from_slice(b),
            Part::Usage(u) => self.so_far.usage += u,
            Part::Item(item) => match (item.raw(), &mut self.own) {
                (Some(raw), _) => {
                    self.own = Some(Relayed {
                        raw: raw.clone(),
                        events: item.events()?.to_vec(),
                        edited: item.unedited().is_none(),
                        added: Vec::new(),
                    });
                }
                (None, Some(own)) => own.added.extend_from_slice(item.events()?),
                (None, None) => {
                    for event in item.events::<Reply>()? {
                        self.so_far.add(event)?;
                    }
                }
            },
            Part::End(Ok(())) => {
                let body = match self.own.take() {
                    Some(own) if !own.edited && own.added.is_empty() => own.raw,
                    Some(own) => {
                        let events = own.events.into_iter().chain(own.added).collect();
                        rebuild_whole::<W>(&own.raw, &join(events))?
                    }
                    None => W::render(&self.so_far).to_string().into(),
                };
                out.extend_from_slice(&body);
            }
            Part::End(Err(f)) if self.ok => return Err(f.clone()),
            Part::End(Err(f)) => {
                let body = W::error_body(f).to_string();
                out.extend_from_slice(body.as_bytes());
            }
            _ => return Err(unknown_part()),
        }
        Ok(())
    }
}

/// A whole response with its edited events put back.
pub(crate) fn rebuild_whole<W: Wire>(raw: &Bytes, events: &[Reply]) -> Result<Bytes, Fault> {
    let mut v: Value = serde_json::from_slice(raw).unwrap_or_default();
    W::put(&mut v, events);
    // Blocks are numbered by position, which a dropped block changes.
    let again = unless_taken(events, W::replies(&v));
    reads_back(
        &by_position(&W::in_order(events)),
        &by_position(&W::in_order(&again)),
    )?;
    Ok(v.to_string().into())
}

/// `again`, events a message read back as, without their starts if a filter
/// took `events`' start: the message still names itself.
pub(crate) fn unless_taken(events: &[Reply], again: Vec<Reply>) -> Vec<Reply> {
    let is_start = |e: &Reply| matches!(e, Reply::Start { .. });
    match events.iter().any(is_start) {
        true => again,
        false => again.into_iter().filter(|e| !is_start(e)).collect(),
    }
}

/// `events` with their texts numbered in order, as a message's positions
/// number them.
fn by_position(events: &[Reply]) -> Vec<Reply> {
    let mut blocks: Vec<u32> = Vec::new();
    let mut numbered = events.to_vec();
    for e in &mut numbered {
        if let Reply::Text { block, .. } = e {
            let at = blocks.iter().position(|b| b == block).unwrap_or_else(|| {
                blocks.push(*block);
                blocks.len() - 1
            });
            *block = u32::try_from(at).unwrap_or(u32::MAX);
        }
    }
    numbered
}

/// Checks that a message rebuilt with edited events reads back as them. An
/// edit the message has no place for, such as a tool call turned into text,
/// fails the response.
pub(crate) fn reads_back(events: &[Reply], again: &[Reply]) -> Result<(), Fault> {
    if events == again {
        return Ok(());
    }
    Err(unpatchable("the provider's message has no place for it"))
}

/// A Part variant this format does not know.
pub(crate) fn unknown_part() -> Fault {
    Fault::internal("a part this format does not know")
}

/// What a stream format writes itself; [`StreamEncoder`] routes the Parts.
pub(crate) trait StreamWriter: Default + Send + 'static {
    /// A provider's event rebuilt with its edited events, or `None` if it
    /// has nothing left to say.
    fn rebuild(raw: &Bytes, events: &[Reply]) -> Result<Option<Bytes>, Fault>;
    /// Relay the provider's event `raw`. Its end, such as finish and usage,
    /// is held back so events a filter releases at the end go before it.
    fn relay(&mut self, raw: &Bytes, out: &mut BytesMut);
    /// Whether the provider's event `raw` can hold `event`, such as text in
    /// a text event.
    fn holds(raw: &Bytes, event: &Reply) -> bool;
    /// Write text or a finish the provider did not send, in a new stream or
    /// among the provider's events (`so_far.relayed`). Else an `Err`.
    fn write(&mut self, e: &Reply, so_far: &SoFar, out: &mut BytesMut) -> Result<(), Fault>;
    /// Write the end, after what is held. A new stream that succeeded is
    /// opened and closed even if it said nothing; a failure is written in
    /// this format's framing.
    fn end(
        &mut self,
        end: &Result<(), Fault>,
        so_far: &SoFar,
        out: &mut BytesMut,
    ) -> Result<(), Fault>;
}

/// What a response written from events has said so far.
#[derive(Debug, Default)]
pub(crate) struct SoFar {
    /// Whether the events are the provider's own, relayed as its bytes.
    pub(crate) relayed: bool,
    /// The response's id and model, from its first start event.
    pub(crate) start: Option<(String, String)>,
    /// Text by content block, in block order, for a whole response.
    pub(crate) text: BTreeMap<u32, String>,
    pub(crate) finish: Option<Finish>,
    pub(crate) usage: Usage,
}

impl SoFar {
    /// Note the first start and the finish. Stream chunks repeat the
    /// start, some without the model, so only the first counts.
    pub(crate) fn note(&mut self, event: &Reply) {
        match event {
            Reply::Start { id, model } => {
                self.start
                    .get_or_insert_with(|| (id.clone(), model.clone()));
            }
            Reply::Finish(f) => self.finish = Some(f.clone()),
            _ => {}
        }
    }

    /// Add an event to a whole response. Content it cannot write, such as
    /// a tool call, is an `Err`.
    pub(crate) fn add(&mut self, event: &Reply) -> Result<(), Fault> {
        self.note(event);
        match event {
            _ if skips(event) => {}
            Reply::Text { block, text } => self.text.entry(*block).or_default().push_str(text),
            Reply::Finish(_) => {}
            _ => return Err(unwritable_content()),
        }
        Ok(())
    }
}

/// Whether writers leave `event` out: a start, which they place
/// themselves, or an empty text, which some providers refuse as a block.
fn skips(event: &Reply) -> bool {
    match event {
        Reply::Start { .. } => true,
        Reply::Text { text, .. } => text.is_empty(),
        _ => false,
    }
}

/// Writes a stream: the provider's events as they are, or events written
/// from conversation events.
#[derive(Debug, Default)]
pub(crate) struct StreamEncoder<S> {
    so_far: SoFar,
    stream: S,
}

impl<S> StreamEncoder<S> {
    pub(crate) fn new(relayed: bool, stream: S) -> Self {
        let so_far = SoFar {
            relayed,
            ..SoFar::default()
        };
        Self { so_far, stream }
    }
}

impl<S: StreamWriter> Encoder for StreamEncoder<S> {
    fn encode(&mut self, part: &Part, out: &mut BytesMut) -> Result<(), Fault> {
        match part {
            Part::Frame(b) => self.stream.relay(b, out),
            Part::Item(item) => {
                let events = item.events::<Reply>()?;
                let (made, bytes, after) = match (item.unedited(), item.raw()) {
                    (Some(raw), _) => (Vec::new(), Some(raw.clone()), Vec::new()),
                    (None, Some(raw)) => rebuild_after::<S>(raw, events)?,
                    (None, None) => (events.to_vec(), None, Vec::new()),
                };
                for e in events {
                    self.so_far.note(e);
                }
                // Buffered, so an item failing partway writes nothing.
                let mut all = BytesMut::new();
                for e in made.iter().filter(|e| !skips(e)) {
                    self.stream.write(e, &self.so_far, &mut all)?;
                }
                out.extend_from_slice(&all);
                if let Some(bytes) = bytes {
                    self.stream.relay(&bytes, out);
                }
                let mut all = BytesMut::new();
                for e in after.iter().filter(|e| !skips(e)) {
                    self.stream.write(e, &self.so_far, &mut all)?;
                }
                out.extend_from_slice(&all);
            }
            Part::Usage(u) => self.so_far.usage += u,
            Part::End(end) => self.stream.end(end, &self.so_far, out)?,
            _ => return Err(unknown_part()),
        }
        Ok(())
    }
}

/// Split edited `events` around the provider's event `raw`. The run `raw`
/// can hold, texts of one block joined, rebuilds it. Events before that run,
/// such as text released before a finish, and after it, such as a finish
/// added after text, are written anew. Starts stay with `raw`, which names
/// the response.
fn rebuild_after<S: StreamWriter>(raw: &Bytes, events: &[Reply]) -> Result<Rebuilt, Fault> {
    let mut events = join(events.to_vec());
    let starts = events
        .iter()
        .take_while(|e| matches!(e, Reply::Start { .. }))
        .count();
    let before = events[starts..]
        .iter()
        .take_while(|e| !S::holds(raw, e))
        .count();
    let held = events[starts + before..]
        .iter()
        .take_while(|e| S::holds(raw, e))
        .count();
    let after: Vec<Reply> = events.split_off(starts + before + held);
    let before: Vec<Reply> = events.drain(starts..starts + before).collect();
    Ok((before, S::rebuild(raw, &events)?, after))
}

/// Events written before a rebuilt provider event, the event, and events
/// written after it.
type Rebuilt = (Vec<Reply>, Option<Bytes>, Vec<Reply>);

/// `events` as a message holds them: starts first, texts of one block
/// joined across whatever came between, and the finish last.
fn join(events: Vec<Reply>) -> Vec<Reply> {
    let (starts, events): (Vec<Reply>, Vec<Reply>) = events
        .into_iter()
        .partition(|e| matches!(e, Reply::Start { .. }));
    let mut joined: Vec<Reply> = starts;
    for e in events {
        let block = |had: &Reply| match had {
            Reply::Text { block, .. } => Some(*block),
            _ => None,
        };
        let same =
            block(&e).and_then(|b| joined.iter_mut().rev().find(|had| block(had) == Some(b)));
        match (same, e) {
            (Some(Reply::Text { text, .. }), Reply::Text { text: more, .. }) => {
                text.push_str(&more);
            }
            (_, e) => {
                let finish = joined
                    .iter()
                    .position(|had| matches!(had, Reply::Finish(_)));
                joined.insert(finish.unwrap_or(joined.len()), e);
            }
        }
    }
    joined
}

/// A conversation's messages in the client's format. A client message
/// (by shared source) is kept as it was if unchanged and patched in its
/// form if changed; any other is written anew.
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

/// Content `old`, which said `was`, changed to say `now`. Parts keep their
/// JSON, such as a cache marker, where they can.
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
    // between them with as many parts as before is patched part by part;
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

/// Turns a provider's running usage totals into the increments the kernel
/// adds.
#[derive(Debug, Default)]
pub(crate) struct Totals(Usage);

impl Totals {
    /// What `total` adds to the totals seen so far. A total lower than one
    /// seen before adds nothing, so usage only grows.
    pub(crate) fn increment(&mut self, total: &Usage) -> Usage {
        let mut step = Usage::default();
        for (unit, n) in total.iter() {
            let seen = self.0.get(&unit);
            if n > seen {
                step.add(unit.clone(), n - seen);
                self.0.add(unit, n - seen);
            }
        }
        step
    }
}

/// A JSON body's `model`, for any JSON format's
/// [`Format::model`](respan_gateway_core::Format::model).
pub fn json_model(body: &Doc) -> Option<String> {
    body.json()?["model"].as_str().map(Into::into)
}

/// Whether a JSON body asks for a stream.
pub(crate) fn streams(body: &Doc) -> bool {
    body.json().and_then(|v| v["stream"].as_bool()) == Some(true)
}

/// Set a JSON body's `model`, or remove it for `None`, for any JSON
/// format's `prepare`. A body whose `model` is already that keeps its
/// bytes. `Err` with `bad_request` if the body must change and is not a
/// JSON object.
pub fn write_model(body: &mut Doc, model: Option<&str>) -> Result<(), Fault> {
    if body.json().and_then(|v| v["model"].as_str()) != model {
        edit(body, |v| set(v, "model", model.map(Into::into)))?;
    }
    Ok(())
}

/// A JSON body with its fields changed by `f`, in place.
pub(crate) fn edit(body: &mut Doc, f: impl FnOnce(&mut Map<String, Value>)) -> Result<(), Fault> {
    let Some(Value::Object(fields)) = body.json_mut() else {
        return Err(Fault::bad_request("request body is not a JSON object"));
    };
    f(fields);
    Ok(())
}

/// A message as a conversation holds it, with what the kind has no form for
/// kept in place.
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

/// A text part, or other content. A cache marker is only a hint, so a text
/// part with one is still text.
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

/// The fault for a provider's error body in a response of `status`. A
/// failure keeps its status; an error in a success is a 502. The error
/// object becomes the fault's details, so the client gets all its fields.
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
    let to = if status.is_success() {
        StatusCode::BAD_GATEWAY
    } else {
        status
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

/// Merge the fault's details into the format's error fields. `message` and
/// `type` stay the fault's. Any other field is added if missing, and
/// replaces the format's if it is a string or null, as OpenAI's `code` and
/// `param` may be.
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

/// A format's names for the values of a conversation type, such as its
/// roles or finish reasons. Several names may mean one value; the first is
/// the one written.
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

    /// A [`Finish`] as the provider's reason. A reason from another format
    /// that this one lacks is written as a plain stop.
    pub(crate) fn reason(&self, finish: &Finish) -> &'static str {
        let stop = || self.name(&Finish::Stop);
        self.name(finish).or_else(stop).unwrap_or("stop")
    }

    /// Write `finish` at `pointer` if it is there and says otherwise.
    /// Reasons that mean the same, such as a stop sequence and a natural
    /// end, are left as the provider wrote them.
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

/// A request holding what cannot be sent in another format.
pub(crate) fn untranslatable(what: &str) -> Fault {
    Fault::bad_request(format!("{what} cannot be translated to another format yet"))
}

/// A translated or reframed response holding what the client's format
/// cannot write anew, such as a tool call; a 502, as the client is not at
/// fault.
pub(crate) fn unwritable_content() -> Fault {
    Fault::new(
        StatusCode::BAD_GATEWAY,
        "untranslatable",
        "the response holds content that cannot yet be written anew in this format",
    )
}

/// A stream event that is not its format's JSON, as other content, so
/// filters see it rather than it passing unseen. One with no data, such as
/// a comment, is a Frame.
pub(crate) fn unreadable_event(format: &str, raw: Bytes) -> Part {
    match event_data(&raw) {
        Some(data) => Item::decoded([Reply::other(format, data)], raw).into(),
        None => Part::Frame(raw),
    }
}

/// Whether a JSON value says anything: null, an empty array and an empty
/// object do not.
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

/// The fields of `v` not in `known`, except those that say nothing or sit
/// at their `inert` value.
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

/// Write the sampling fields `after` changed from `before`. Both formats
/// spell them alike except the stop sequences, whose key is `stop`.
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
        let answered = "the provider answered 400";
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
                "upstream_error",
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
            (
                json!({"detail": "Not found"}),
                "upstream_error",
                "Not found",
            ),
            (
                json!({"detail": [{"msg": "field required"}]}),
                "upstream_error",
                "field required",
            ),
            (
                json!({"object": "error", "type": "BadRequestError", "message": "bad"}),
                "BadRequestError",
                "bad",
            ),
            (
                json!({"jsonrpc": "2.0", "error": {"code": -32600, "message": "invalid"}}),
                "upstream_error",
                "invalid",
            ),
            (
                json!({"object": "error", "message": {"detail": [{"msg": "Field required"}]}, "type": "invalid_request_message_error"}),
                "invalid_request_message_error",
                "Field required",
            ),
            (
                json!({"type": "https://example.com/probs/nope", "title": "Nope"}),
                "upstream_error",
                "Nope",
            ),
            (json!("Unauthorized"), "upstream_error", "Unauthorized"),
            (
                json!({"error": {}, "message": "top"}),
                "upstream_error",
                "top",
            ),
            (
                json!({"error": {"code": "x"}, "message": "top"}),
                "x",
                "top",
            ),
            (Value::Null, "upstream_error", answered),
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
            "upstream_error"
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
        assert_eq!(message.message(), "the provider answered 200");
    }

    #[test]
    fn usage_totals_become_increments() {
        let mut totals = Totals::default();
        let total = |n| {
            let mut u = Usage::default();
            u.add("out", n);
            u
        };
        assert_eq!(totals.increment(&total(1.0)).get("out"), 1.0);
        assert_eq!(totals.increment(&total(3.0)).get("out"), 2.0);
        assert!(totals.increment(&total(3.0)).is_empty());
        assert!(totals.increment(&total(2.0)).is_empty(), "never negative");
    }
}
